# Fails with "Too many open files"
from hashlib import md5
from pathlib import Path
from typing import List, Tuple
import trio


def trio_walk(dirpath: Path) -> List[Tuple[Path, str]]:
    return trio.run(async_walk, dirpath)


async def async_walk(dirpath: Path) -> List[Tuple[Path, str]]:
    files = []
    async with trio.open_nursery() as nursery:
        sender, receiver = trio.open_memory_channel(0)
        nursery.start_soon(async_worker, dirpath, nursery, sender)
        async with receiver:
            async for f in receiver:
                files.append(f)
    return files


async def async_worker(
    dirpath: Path,
    nursery: trio.Nursery,
    sender: trio.MemorySendChannel[Tuple[Path, str]],
) -> None:
    async with sender:
        for p in dirpath.iterdir():
            if p.is_dir():
                nursery.start_soon(async_worker, p, nursery, sender.clone())
            else:
                nursery.start_soon(async_md5digest, p, sender.clone())


async def async_md5digest(
    filepath: Path, sender: trio.MemorySendChannel[Tuple[Path, str]]
) -> None:
    async with sender:
        dgst = md5()
        async with await trio.open_file(filepath, "rb") as fp:
            while True:
                blob = await fp.read1()
                if not blob:
                    break
                dgst.update(blob)
        sender.send((filepath, dgst.hexdigest()))
