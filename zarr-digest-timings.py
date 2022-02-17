#!/usr/bin/env python3
from __future__ import annotations
import os
from pathlib import Path
from timeit import timeit
from argset import argset
from checksummers import CLASSES
import click
from fscacher import PersistentCache

DEFAULT_THREADS = min(32, (os.cpu_count() or 1) + 4)

CACHE_NAME = "zarr-digest-timings"


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
@click.option(
    "-n",
    "--number",
    default=100,
    show_default=True,
    help="Number of times to run the function",
)
@click.option(
    "-T",
    "--threads",
    type=int,
    default=DEFAULT_THREADS,
    show_default=True,
    help="Number of threads to use when walking directory trees",
)
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
