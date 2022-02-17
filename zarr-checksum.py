#!/usr/bin/env python3
from __future__ import annotations

__requires__ = [
    "click >= 8.0",
    "dandischema >= 0.5.1",
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
from typing import Dict, Iterable, Tuple, Union
import click
from dandischema.digests.zarr import get_checksum

DIGEST_BLOCK_SIZE = 1 << 16

DEFAULT_THREADS = min(32, (os.cpu_count() or 1) + 4)

log = logging.getLogger()


@dataclass
class ZarrChecksummer(ABC):
    threads: int = field(default=DEFAULT_THREADS)

    @abstractmethod
    def checksum(self, dirpath: Path) -> str:
        ...


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
    def digest_walk(self, dirpath: Path) -> Iterable[Tuple[Path, str]]:
        ...

    def checksum(self, dirpath: Path) -> str:
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


def md5digest(filepath: Union[str, Path]) -> str:
    dgst = md5()
    with open(filepath, "rb") as fp:
        while True:
            block = fp.read(DIGEST_BLOCK_SIZE)
            if not block:
                break
            dgst.update(block)
    return dgst.hexdigest()


class SyncWalker(IterativeChecksummer):
    def digest_walk(self, dirpath: Path) -> Iterable[Tuple[Path, str]]:
        dirs = deque([dirpath])
        while dirs:
            for p in dirs.popleft().iterdir():
                if p.is_dir():
                    dirs.append(p)
                else:
                    yield (p, md5digest(p))


class ThreadedWalker(IterativeChecksummer):
    def digest_walk(self, dirpath: Path) -> Iterable[Tuple[Path, str]]:
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
                            digest = md5digest(subpath)
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


class ThreadedWalker2(IterativeChecksummer):
    def digest_walk(self, dirpath: Path) -> Iterable[Tuple[Path, str]]:
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
                    if os.path.isdir(path):
                        for item in os.listdir(path):
                            with lock:
                                tasks += 1
                                paths.append(os.path.join(path, item))
                                on_input.notify()
                    else:
                        digest = md5digest(path)
                        with lock:
                            output.append((Path(path), digest))
                            on_output.notify()
                except OSError:
                    log.exception("Error digesting path %s", path)
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


CLASSES = {
    "sync": SyncWalker,
    "threads": ThreadedWalker,
    "threads2": ThreadedWalker2,
}


@click.command()
@click.option("-T", "--threads", type=int, default=DEFAULT_THREADS, show_default=True)
@click.argument(
    "dirpath", type=click.Path(exists=True, file_okay=False, path_type=Path)
)
@click.argument("implementation", type=click.Choice(list(CLASSES)))
def main(dirpath: Path, implementation: str, threads: int) -> None:
    summer = CLASSES[implementation](threads=threads)
    print(summer.checksum(dirpath))


if __name__ == "__main__":
    main()
