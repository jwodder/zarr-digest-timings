from __future__ import annotations
import argparse
from dataclasses import dataclass
from functools import reduce
import json
from operator import add
from typing import Any, Dict, Iterable, List, Union
from checksummers import CLASSES


@dataclass(frozen=True)
class TableID:
    dirpath: str
    threads: int

    def as_rst(self) -> str:
        return f"``{self.dirpath}``, {self.threads} threads"

    def as_markdown(self) -> str:
        return f"`{self.dirpath}`, {self.threads} threads"


@dataclass(frozen=True)
class CellID:
    implementation: str
    threaded_fscacher: bool
    caching: bool
    caching_files: bool


@dataclass
class Average:
    qty: int
    total: float

    @classmethod
    def from_average(cls, qty: int, avg: float) -> Average:
        return cls(qty=qty, total=avg * qty)

    def __add__(self, other: Average) -> Average:
        return type(self)(qty=self.qty + other.qty, total=self.total + other.total)

    def __float__(self) -> float:
        return self.total / self.qty

    def __str__(self) -> str:
        return "{:6g}".format(float(self))


@dataclass
class DualAverage:
    first: Average
    second: Average

    def __add__(self, other: DualAverage) -> DualAverage:
        return type(self)(
            first=self.first + other.first, second=self.second + other.second
        )

    def __str__(self) -> str:
        return f"{self.first} / {self.second}"


def compile_report(
    report: Iterable[Dict[str, Any]]
) -> Dict[TableID, Dict[CellID, Union[Average, DualAverage]]]:
    tables = {}
    for entry in report:
        table_id = TableID(dirpath=entry["dirpath"], threads=entry["threads"])
        cell_id = CellID(
            implementation=entry["implementation"],
            threaded_fscacher=entry["threaded_fscacher"],
            caching=entry["caching"],
            caching_files=entry["caching_files"],
        )
        value = Average.from_average(entry["number"], entry["avgtime"])
        if entry["first_call"] is not None:
            value = DualAverage(
                first=Average(qty=1, total=entry["first_call"]),
                second=Average.from_average(entry["number"], entry["avgtime"]),
            )
        tbl = tables.setdefault(table_id, {})
        if cell_id in tbl:
            assert type(tbl[cell_id]) is type(value)
            tbl[cell_id] += value
        else:
            tbl[cell_id] = value
    return tables


LEFT_SIDE = """\
    +----------------------------------+
    |                                  |
    +==================================+
    | No Caching                       |
    +----------------------------------+
    | Caching Files                    |
    +---------------------+------------+
    | Caching Directories | No Threads |
    +                     +------------+
    |                     | Threads    |
    +---------------------+------------+
    | Caching Both        | No Threads |
    +                     +------------+
    |                     | Threads    |
    +---------------------+------------+
"""

LEFT_COLUMN = [
    "",
    "No Caching",
    "Caching Files",
    "Caching Directories (No Threads)",
    "Caching Directories (Threads)",
    "Caching Both (No Threads)",
    "Caching Both (Threads)",
]

COLWIDTH = 17  # not counting padding
CELLDIV = "-" * (COLWIDTH + 2) + "+"
CELLDIV_HEADER = CELLDIV.replace("-", "=")


def to_cell(s: str) -> str:
    return " " + s.ljust(COLWIDTH) + " |"


def draw_rst(table_id: TableID, columns: List[List[str]]) -> str:
    lines = LEFT_SIDE.splitlines()
    for cells in columns:
        newlines = [CELLDIV]
        for c in cells:
            newlines.append(to_cell(c))
            newlines.append(CELLDIV)
        newlines[2] = CELLDIV_HEADER
        lines = map(add, lines, newlines)
    return f".. table:: {table_id.as_rst()}\n\n" + "\n".join(lines)


def draw_markdown(table_id: TableID, columns: List[List[str]]) -> str:
    columns.insert(0, LEFT_COLUMN)
    lines = []
    for row in zip(*columns):
        lines.append("| " + " | ".join(row) + " |")
    lines.insert(1, "|" + (" --- |" * (len(CLASSES) + 1)))
    return f"### {table_id.as_markdown()}\n\n" + "\n".join(lines)


def extract_columns(tbl: Dict[CellID, Union[Average, DualAverage]]) -> List[List[str]]:
    columns = []
    for implementation in CLASSES.keys():
        cells = [implementation]
        for caching_files in [False, True]:
            values = []
            for threaded in [False, True]:
                key = CellID(
                    implementation=implementation,
                    threaded_fscacher=threaded,
                    caching=False,
                    caching_files=caching_files,
                )
                try:
                    values.append(tbl[key])
                except KeyError:
                    pass
            if values:
                cells.append(str(reduce(add, values)))
            else:
                cells.append("\u2014")
        for caching_files in [False, True]:
            for threaded in [False, True]:
                key = CellID(
                    implementation=implementation,
                    threaded_fscacher=threaded,
                    caching=True,
                    caching_files=caching_files,
                )
                try:
                    cells.append(str(tbl[key]))
                except KeyError:
                    cells.append("\u2014")
        columns.append(cells)
    return columns


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Convert a file of JSON Lines zarr-digest-timings reports to a table"
        ),
    )
    parser.add_argument("-f", "--format", choices=["rst", "md"], default="rst")
    parser.add_argument("-o", "--outfile", type=argparse.FileType("w"), default="-")
    parser.add_argument("report", type=argparse.FileType("r"))
    args = parser.parse_args()
    with args.report:
        report = [json.loads(line) for line in args.report]
    tables = compile_report(report)
    with args.outfile:
        for table_id, tbl in tables.items():
            columns = extract_columns(tbl)
            if args.format == "rst":
                print(draw_rst(table_id, columns), file=args.outfile)
            else:
                print(draw_markdown(table_id, columns), file=args.outfile)
            print(file=args.outfile)
        if args.format == "rst":
            print(".. vim:set nowrap:", file=args.outfile)
        else:
            print("<!-- vim:set nowrap: -->", file=args.outfile)


if __name__ == "__main__":
    main()
