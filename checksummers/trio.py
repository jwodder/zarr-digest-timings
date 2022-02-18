from __future__ import annotations
from collections import deque
from contextlib import asynccontextmanager
from hashlib import md5
from pathlib import Path
import sys
from typing import (
    AsyncContextManager,
    AsyncIterable,
    AsyncIterator,
    Generic,
    Iterable,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)
import trio
from .bases import DIGEST_BLOCK_SIZE, IterativeChecksummer

T = TypeVar("T")

if sys.version_info >= (3, 10):
    from contextlib import aclosing
else:
    from async_generator import aclosing

    def aiter(obj: AsyncIterable[T]) -> AsyncIterator[T]:
        return obj.__aiter__()


class TrioWalker(IterativeChecksummer):
    def digest_walk(self, dirpath: Union[str, Path]) -> Iterable[Tuple[Path, str]]:
        return trio.run(self.async_walk, Path(dirpath))

    async def async_walk(self, dirpath: Path) -> List[Tuple[Path, str]]:
        files = []
        jobs = AsyncJobStack([dirpath])
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
        jobs: AsyncJobStack[Path],
        sender: trio.MemorySendChannel[Tuple[Path, str]],
    ) -> None:
        async with sender, aclosing(aiter(jobs)) as jobiter:
            async for ctx in jobiter:
                async with ctx as dirpath:
                    for p in dirpath.iterdir():
                        if p.is_dir():
                            await jobs.put(p)
                        else:
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


class AsyncJobStack(Generic[T]):
    def __init__(self, iterable: Optional[Iterable[T]] = None) -> None:
        self._lock = trio.Lock()
        self._cond = trio.Condition(self._lock)
        self._queue: deque[T] = deque()
        self._tasks = 0
        if iterable is not None:
            self._queue.extend(iterable)
            self._tasks += len(self._queue)

    async def __aiter__(self) -> AsyncIterator[AsyncContextManager[T]]:
        while True:
            async with self._lock:
                while True:
                    if not self._tasks:
                        return
                    if not self._queue:
                        await self._cond.wait()
                        continue
                    value = self._queue.pop()
                    break
            yield self._job_ctx(value)

    @asynccontextmanager
    async def _job_ctx(self, value: T) -> AsyncIterator[T]:
        try:
            yield value
        finally:
            async with self._lock:
                self._tasks -= 1
                if self._tasks <= 0:
                    self._cond.notify_all()

    async def put(self, value: T) -> None:
        async with self._lock:
            self._queue.append(value)
            self._tasks += 1
            self._cond.notify()
