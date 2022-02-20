from __future__ import annotations
from hashlib import md5
from pathlib import Path
from typing import Iterable, List, Tuple, TypeVar, Union
import trio
from .bases import DIGEST_BLOCK_SIZE, IterativeChecksummer

T = TypeVar("T")


class TrioWalker2(IterativeChecksummer):
    def digest_walk(self, dirpath: Union[str, Path]) -> Iterable[Tuple[Path, str]]:
        return trio.run(self.async_walk, Path(dirpath))

    async def async_walk(self, dirpath: Path) -> List[Tuple[Path, str]]:
        files = []
        async with trio.open_nursery() as nursery:
            sender, receiver = trio.open_memory_channel(0)
            limit = trio.CapacityLimiter(self.threads)
            nursery.start_soon(self.async_worker, dirpath, nursery, sender, limit)
            async with receiver:
                async for f in receiver:
                    files.append(f)
        return files

    async def async_worker(
        self,
        dirpath: Path,
        nursery: trio.Nursery,
        sender: trio.MemorySendChannel[Tuple[Path, str]],
        limit: trio.CapacityLimiter,
    ) -> None:
        async with sender:
            for p in dirpath.iterdir():
                if p.is_dir():
                    nursery.start_soon(
                        self.async_worker, p, nursery, sender.clone(), limit
                    )
                else:
                    # Limit the number of open files
                    async with limit:
                        dgst = await self.async_md5digest(p)
                    await sender.send((p, dgst))

    @staticmethod
    async def async_md5digest(filepath: Path) -> None:
        dgst = md5()
        async with await trio.open_file(filepath, "rb") as fp:
            while True:
                blob = await fp.read(DIGEST_BLOCK_SIZE)
                if not blob:
                    break
                dgst.update(blob)
        return dgst.hexdigest()
