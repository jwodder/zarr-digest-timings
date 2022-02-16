#!/usr/bin/env python3
from __future__ import annotations

__requires__ = [
    "click >= 8.0",
    "dandischema >= 0.5.1",
]
from collections import deque
from dataclasses import dataclass, field
from hashlib import md5
from pathlib import Path
from typing import Dict, Iterable, Tuple, Union
import click
from dandischema.digests.zarr import get_checksum

DIGEST_BLOCK_SIZE = 1 << 16


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


def checksum_file_list(dirpath: Path, files: Iterable[Tuple[Path, str]]) -> str:
    root = Directory(path="")
    for p, digest in files:
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


def sync_walk(dirpath: Path) -> Iterable[Tuple[Path, str]]:
    dirs = deque([dirpath])
    while dirs:
        for p in dirs.popleft().iterdir():
            if p.is_dir():
                dirs.append(p)
            else:
                yield (p, md5digest(p))


@click.command()
@click.argument(
    "dirpath", type=click.Path(exists=True, file_okay=False, path_type=Path)
)
def main(dirpath: Path) -> None:
    print(checksum_file_list(dirpath, sync_walk(dirpath)))


if __name__ == "__main__":
    main()
