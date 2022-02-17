#!/usr/bin/env python3
from __future__ import annotations

__requires__ = [
    "click >= 8.0",
    "dandischema >= 0.5.1",
    "fscacher",
]

from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from hashlib import md5
import logging
import os
import os.path
from pathlib import Path
import threading
from timeit import timeit
from typing import Dict, Iterable, Optional, Tuple, Union
from argset import argset
import click
from dandischema.digests.zarr import get_checksum
from fscacher import PersistentCache

DIGEST_BLOCK_SIZE = 1 << 16

DEFAULT_THREADS = min(32, (os.cpu_count() or 1) + 4)

CACHE_NAME = "zarr-digest-timings"

log = logging.getLogger()


@dataclass
class ZarrChecksummer(ABC):
    cache: PersistentCache
    threads: int = DEFAULT_THREADS
    cache_files: bool = False

    def __post_init__(self) -> None:
        if self.cache_files:
            self.md5digest = self.cache.memoize_path(self.md5digest)

    @abstractmethod
    def checksum(self, dirpath: Union[str, Path]) -> str:
        ...

    @staticmethod
    def md5digest(filepath: Union[str, Path]) -> str:
        dgst = md5()
        with open(filepath, "rb") as fp:
            while True:
                block = fp.read(DIGEST_BLOCK_SIZE)
                if not block:
                    break
                dgst.update(block)
        return dgst.hexdigest()


@dataclass
class Directory:
    path: str
    children: Dict[str, Union[Directory, File]] = field(default_factory=dict)

    def get_digest(self) -> str:
        files = {}
        dirs = {}
        for n in self.children.values():
            if isinstance(n, Directory):
                dirs[n.path] = n.get_digest()
            else:
                files[n.path] = n.digest
        return get_checksum(files, dirs)


@dataclass
class File:
    path: str
    digest: str


class IterativeChecksummer(ZarrChecksummer):
    @abstractmethod
    def digest_walk(self, dirpath: Union[str, Path]) -> Iterable[Tuple[Path, str]]:
        ...

    def checksum(self, dirpath: Union[str, Path]) -> str:
        root = Directory(path="")
        for p, digest in self.digest_walk(dirpath):
            *dirs, name = p.relative_to(dirpath).parts
            parts = []
            d = root
            for dirname in dirs:
                parts.append(dirname)
                d = d.children.setdefault(dirname, Directory(path="/".join(parts)))
                assert isinstance(d, Directory), f"Path type conflict for {d.name}"
            parts.append(name)
            path = "/".join(parts)
            assert name not in d.children, f"File {path} yielded twice"
            d.children[name] = File(path=path, digest=digest)
        return root.get_digest()


class SyncWalker(IterativeChecksummer):
    def digest_walk(self, dirpath: Union[str, Path]) -> Iterable[Tuple[Path, str]]:
        dirs = deque([Path(dirpath)])
        while dirs:
            for p in dirs.popleft().iterdir():
                if p.is_dir():
                    dirs.append(p)
                else:
                    yield (p, self.md5digest(p))


class ThreadedWalker(IterativeChecksummer):
    def digest_walk(self, dirpath: Union[str, Path]) -> Iterable[Tuple[Path, str]]:
        if not os.path.isdir(dirpath):
            return
        lock = threading.Lock()
        on_input = threading.Condition(lock)
        on_output = threading.Condition(lock)
        tasks = 1
        paths = [dirpath]
        output = []
        threads = self.threads

        def worker():
            nonlocal tasks
            while True:
                with lock:
                    while True:
                        if not tasks:
                            output.append(None)
                            on_output.notify()
                            return
                        if not paths:
                            on_input.wait()
                            continue
                        path = paths.pop()
                        break
                try:
                    for item in os.listdir(path):
                        subpath = os.path.join(path, item)
                        if os.path.isdir(subpath):
                            with lock:
                                tasks += 1
                                paths.append(subpath)
                                on_input.notify()
                        else:
                            digest = self.md5digest(subpath)
                            with lock:
                                output.append((Path(subpath), digest))
                                on_output.notify()
                except OSError:
                    log.exception("Error scanning directory %s", path)
                finally:
                    with lock:
                        tasks -= 1
                        if not tasks:
                            on_input.notify_all()

        workers = [
            threading.Thread(
                target=worker, name=f"fastio.walk {i} {dirpath}", daemon=True
            )
            for i in range(threads)
        ]
        for w in workers:
            w.start()
        while threads or output:
            with lock:
                while not output:
                    on_output.wait()
                item = output.pop()
            if item:
                yield item
            else:
                threads -= 1


class RecursiveChecksummer(ZarrChecksummer):
    def checksum(
        self, dirpath: Union[str, Path], root: Optional[Union[str, Path]] = None
    ) -> str:
        recurse = getattr(self, "recurse", self.checksum)
        files = {}
        dirs = {}
        if root is None:
            root = Path(dirpath)
        for p in Path(dirpath).iterdir():
            key = p.relative_to(root).as_posix()
            if p.is_file():
                files[key] = self.md5digest(p)
            elif any(p.iterdir()):
                dirs[key] = recurse(p, root)
        return get_checksum(files, dirs)


CLASSES = {
    "sync": SyncWalker,
    "fastio": ThreadedWalker,
    "recursive": RecursiveChecksummer,
}


@click.command()
@click.option(
    "-c",
    "--cache",
    "do_cache",
    is_flag=True,
    help="Use fscacher to cache the Zarr directory checksumming routine",
)
@click.option(
    "-C",
    "--cache-files",
    is_flag=True,
    help="Use fscacher to cache digests for individual files",
)
@click.option(
    "--clear-cache/--no-clear-cache",
    default=True,
    help="Clear cache on program startup",
    show_default=True,
)
@click.option("-n", "--number", default=100, show_default=True)
@click.option("-T", "--threads", type=int, default=DEFAULT_THREADS, show_default=True)
@click.argument(
    "dirpath", type=click.Path(exists=True, file_okay=False, path_type=Path)
)
@click.argument("implementation", type=click.Choice(list(CLASSES)))
def main(
    dirpath: Path,
    implementation: str,
    number: int,
    threads: int,
    do_cache: bool,
    cache_files: bool,
    clear_cache: bool,
) -> None:
    if "walk_threads" in argset(PersistentCache):
        kwargs = {"walk_threads": threads}
    else:
        kwargs = {}
    cache = PersistentCache(CACHE_NAME, **kwargs)
    if clear_cache:
        cache.clear()
    summer = CLASSES[implementation](
        cache=cache, threads=threads, cache_files=cache_files
    )
    if number <= 0:
        print(summer.checksum(dirpath))
    else:
        func = summer.checksum
        if do_cache:
            func = cache.memoize_path(func)
            if implementation == "recursive":
                summer.recurse = func
        print(
            timeit(
                "func(dirpath)",
                number=number,
                globals={"func": func, "dirpath": dirpath},
            )
            / number
        )


if __name__ == "__main__":
    main()
