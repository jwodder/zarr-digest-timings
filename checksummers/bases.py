from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from hashlib import md5
from pathlib import Path
from typing import Dict, Iterable, Tuple, Union
from dandischema.digests.zarr import get_checksum
from fscacher import PersistentCache

DIGEST_BLOCK_SIZE = 1 << 16


@dataclass
class ZarrChecksummer(ABC):
    cache: PersistentCache
    threads: int
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
