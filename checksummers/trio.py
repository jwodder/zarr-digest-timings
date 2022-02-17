from __future__ import annotations
from collections import deque
from hashlib import md5
from pathlib import Path
from typing import Iterable, List, Tuple, Union
import trio
from .bases import IterativeChecksummer


class AsyncWalker(IterativeChecksummer):
    def digest_walk(self, dirpath: Union[str, Path]) -> Iterable[Tuple[Path, str]]:
        return trio.run(self.async_walk, Path(dirpath))

    async def async_walk(self, dirpath: Path) -> List[Tuple[Path, str]]:
        files = []
        jobs = deque([dirpath])
        async with trio.open_nursery() as nursery:
            sender, receiver = trio.open_memory_channel(0)
            # We need to limit the number of workers or else we get a "Too many
            # open files" error
            async with sender:
                for _ in range(self.threads):
                    nursery.start_soon(self.async_worker, jobs, sender.clone())
            async with receiver:
                async for f in receiver:
                    files.append(f)
        return files

    async def async_worker(
        self,
        jobs: deque[Path],
        sender: trio.MemorySendChannel[Tuple[Path, str]],
    ) -> None:
        async with sender:
            while jobs:
                # PROBLEM: It is possible for an async worker to check `jobs`
                # while it is empty and other workers are still in the middle
                # of working on a directory, in which case the worker will exit
                # early, likely leading to one worker being left to pick up
                # everyone else's slack.  Solving this would likely involve an
                # async locking queue that keeps track of whether tasks are
                # done.
                for p in jobs.popleft().iterdir():
                    if p.is_dir():
                        jobs.append(p)
                    else:
                        dgst = await self.async_md5digest(p)
                        await sender.send((p, dgst))

    @staticmethod
    async def async_md5digest(filepath: Path) -> None:
        dgst = md5()
        async with await trio.open_file(filepath, "rb") as fp:
            while True:
                blob = await fp.read1()
                if not blob:
                    break
                dgst.update(blob)
        return dgst.hexdigest()
