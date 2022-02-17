#!/usr/bin/env python3
from __future__ import annotations
import json
import logging
import os
from pathlib import Path
from timeit import timeit
from typing import Optional
from argset import argset
from checksummers import CLASSES
import click
from fscacher import PersistentCache

DEFAULT_THREADS = min(32, (os.cpu_count() or 1) + 4)

CACHE_NAME = "zarr-digest-timings"

log = logging.getLogger()


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
    "-R",
    "--report",
    type=click.Path(dir_okay=False, writable=True, path_type=Path),
    help="Append a report as a line of JSON to this file",
)
@click.option(
    "-T",
    "--threads",
    type=int,
    default=DEFAULT_THREADS,
    show_default=True,
    help="Number of threads to use when walking directory trees",
)
@click.option(
    "-v",
    "--verbose",
    count=True,
    help=(
        "Log the result & timestamp of each function call.  Repeat option for"
        " more logs."
    ),
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
    report: Optional[Path],
    verbose: int,
) -> None:
    if verbose == 0:
        log_level = logging.WARNING
    elif verbose == 1:
        log_level = logging.INFO
    elif verbose == 2:
        log_level = logging.DEBUG
    elif verbose == 3:
        log_level = 1
    logging.basicConfig(format="%(asctime)s %(message)s", level=log_level)
    if "walk_threads" in argset(PersistentCache):
        kwargs = {"walk_threads": threads}
        threaded_fscacher = True
    else:
        kwargs = {}
        threaded_fscacher = False
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
        stmnt = "func(dirpath)"
        namespace = {"func": func, "dirpath": dirpath}
        if verbose:
            stmnt = f"r = {stmnt}\nlog.info('checksum(%s) = %s', dirpath, r)"
            namespace["log"] = log
        avgtime = timeit(stmnt, number=number, globals=namespace) / number
        print(avgtime)
        if report is not None:
            data = {
                "dirpath": str(dirpath),
                "implementation": implementation,
                "threaded_fscacher": threaded_fscacher,
                "number": number,
                "avgtime": avgtime,
                "threads": threads,
                "caching": do_cache,
                "caching_files": cache_files,
            }
            with report.open("a") as fp:
                print(json.dumps(data), file=fp)


if __name__ == "__main__":
    main()
