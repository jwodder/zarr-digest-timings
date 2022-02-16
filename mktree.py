#!/usr/bin/env python3
import json
import logging
from pathlib import Path
import random
from typing import Optional, TextIO
import click

log = logging.getLogger(__name__)

FILE_BLOCK_SIZE = 65535

try:
    from random import randbytes
except ImportError:

    def randbytes(n: int) -> bytes:
        return bytes(random.choices(list(range(256)), k=n))


def mkfile(path: Path, spec: Optional[dict]) -> None:
    if spec is None:
        log.info("Touching file %s", path)
        path.touch()
        return
    elif "size" in spec:
        size = spec["size"]
    elif "maxsize" in spec:
        size = random.randint(spec.get("minsize", 0), spec["maxsize"])
    else:
        raise ValueError("Filespec must contain 'size' or 'maxsize' key")
    log.info("Creating file %s (size: %d)", path, size)
    with path.open("wb") as fp:
        for _ in range(size // FILE_BLOCK_SIZE):
            fp.write(randbytes(FILE_BLOCK_SIZE))
        if size % FILE_BLOCK_SIZE:
            fp.write(randbytes(size % FILE_BLOCK_SIZE))


def create_tree(root: Path, layout):
    if isinstance(layout, dict):
        for name, sublayout in layout.items():
            p = root / name
            if sublayout is None or (
                isinstance(sublayout, dict)
                and ("size" in sublayout or "maxsize" in sublayout)
            ):
                mkfile(p, sublayout)
            else:
                log.info("Creating directory %s", p)
                p.mkdir()
                create_tree(p, sublayout)
    else:
        if isinstance(layout[-1], dict) or layout[-1] is None:
            filespec = layout[-1]
            layout = layout[:-1]
        else:
            filespec = None
        dirs = [root]
        for i, width in enumerate(layout):
            if i < len(layout) - 1:
                dirs2 = []
                for d in dirs:
                    for x in range(width):
                        d2 = d / f"d{x}"
                        log.info("Creating directory %s", d2)
                        d2.mkdir()
                        dirs2.append(d2)
                dirs = dirs2
            else:
                for d in dirs:
                    for x in range(width):
                        mkfile(d / f"f{x}.dat", filespec)


@click.command()
@click.argument("dirpath", type=click.Path(file_okay=False, path_type=Path))
@click.argument("specfile", type=click.File())
def main(dirpath: Path, specfile: TextIO) -> None:
    logging.basicConfig(
        format="%(asctime)s [%(levelname)-8s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )
    with specfile:
        layout = json.load(specfile)
    log.info("Creating directory %s", dirpath)
    dirpath.mkdir(parents=True, exist_ok=True)
    create_tree(dirpath, layout)


if __name__ == "__main__":
    main()
