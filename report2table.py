from __future__ import annotations
import argparse
from csv import DictReader
from dataclasses import dataclass, field
from functools import reduce
from operator import add
from pathlib import Path
from textwrap import indent
from typing import Dict, List, Optional, Union
from checksummers import CLASSES, THREADED_CLASSES
from pydantic import BaseModel
from txtble import ASCII_EQ_BORDERS, Txtble


@dataclass(frozen=True, order=True)
class OrderedStr:
    index: int
    value: str

    def __str__(self) -> str:
        return self.value


CACHING_MODES = {
    # caching, caching_files
    (False, False): OrderedStr(0, "No Caching"),
    (False, True): OrderedStr(1, "Caching Files"),
    (True, False): OrderedStr(2, "Caching Directories"),
    (True, True): OrderedStr(3, "Caching Both"),
}

ORDERED_CLASSES = {name: OrderedStr(i, name) for i, name in enumerate(CLASSES)}


class ReportEntry(BaseModel):
    dirpath: str
    implementation: str
    fscacher_version: str
    threaded_fscacher: bool
    caching: bool
    caching_files: bool
    threads: int
    number: int
    first_call: Optional[float]
    avgtime: float

    @property
    def table_id(self) -> TableID:
        return TableID(
            dirpath=self.dirpath, implementation=ORDERED_CLASSES[self.implementation]
        )

    @property
    def caching_mode(self) -> OrderedStr:
        return CACHING_MODES[(self.caching, self.caching_files)]

    def get_version_id(self, fscacher_versions: Dict[str, OrderedStr]) -> VersionID:
        fv = fscacher_versions[self.fscacher_version]
        if self.implementation in THREADED_CLASSES or self.threaded_fscacher:
            return VersionID(fv, self.threads)
        else:
            return VersionID(fv)

    @property
    def value(self) -> Union[Average, DualAverage]:
        value = Average.from_average(self.number, self.avgtime)
        if self.first_call is not None:
            value = DualAverage(
                first=Average(qty=1, total=self.first_call), second=value
            )
        return value


@dataclass(frozen=True, order=True)
class TableID:
    dirpath: str
    implementation: OrderedStr

    def as_rst(self) -> str:
        return f'``{self.dirpath}``, "{self.implementation}" implementation'

    def as_markdown(self) -> str:
        return f'`{self.dirpath}`, "{self.implementation}" implementation'


@dataclass(frozen=True)
class CellID:
    fscacher_version: str
    threaded_fscacher: bool
    caching: bool
    caching_files: bool
    threads: int


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
        return "{:g}".format(float(self))


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


@dataclass(frozen=True, order=True)
class VersionID:
    fscacher_version: OrderedStr
    threads: int = 0

    def __str__(self) -> str:
        s = str(self.fscacher_version)
        if self.threads:
            s += f", {self.threads} threads"
        return s


@dataclass
class Table:
    table_id: TableID
    headers: List[str]
    rows: List[List[str]]

    def as_rst(self) -> str:
        return f".. table:: {self.table_id.as_rst()}\n\n" + indent(
            Txtble(
                headers=self.headers,
                data=self.rows,
                header_border=ASCII_EQ_BORDERS,
                row_border=True,
                padding=1,
            ).show(),
            " " * 4,
        )

    def as_markdown(self) -> str:
        return (
            f"### {self.table_id.as_markdown()}\n\n"
            + self.draw_row(self.headers)
            + "\n"
            + self.draw_row(["---"] * len(self.headers))
            + "\n"
            + "\n".join(map(self.draw_row, self.rows))
        )

    @staticmethod
    def draw_row(row: List[str]) -> str:
        return "| " + " | ".join(row) + " |"


@dataclass
class TableBuilder:
    table_id: TableID
    fscacher_versions: Dict[str, OrderedStr]
    versions: set[VersionID] = field(default_factory=set)
    cells: Dict[VersionID, Dict[OrderedStr, Union[Average, DualAverage]]] = field(
        default_factory=dict
    )

    def add_entry(self, entry: ReportEntry) -> None:
        caching_mode = entry.caching_mode
        version_id = entry.get_version_id(self.fscacher_versions)
        value = entry.value
        row = self.cells.setdefault(version_id, {})
        if caching_mode in row:
            assert type(row[caching_mode]) is type(value)
            row[caching_mode] += value
        else:
            row[caching_mode] = value
        self.versions.add(version_id)

    def compile(self) -> Table:
        columns = sorted(CACHING_MODES.values())
        headers = [""] + [str(c) for c in columns]
        rows = [
            [str(row_id)] + [str(row.get(c, "\u2014")) for c in columns]
            for row_id, row in sorted(self.cells.items())
        ]
        return Table(table_id=self.table_id, headers=headers, rows=rows)


def report2tables(
    report: List[ReportEntry], fscacher_versions: Dict[str, OrderedStr]
) -> List[Table]:
    builders = {}
    for entry in report:
        table_id = entry.table_id
        builders.setdefault(
            table_id, TableBuilder(table_id, fscacher_versions)
        ).add_entry(entry)
    return [b.compile() for _, b in sorted(builders.items())]


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


def get_fscacher_versions() -> Dict[str, OrderedStr]:
    vs = {}
    names = set()
    with Path(__file__).with_name("fscacher-versions.csv").open("r") as fp:
        csv = DictReader(fp)
        for row in csv:
            names.add(row["name"])
            vs[row["version"]] = OrderedStr(len(names), row["name"])
    return vs


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert zarr-digest-timings JSON Lines reports to tables",
    )
    parser.add_argument("-f", "--format", choices=["rst", "md"], default="rst")
    parser.add_argument("-o", "--outfile", type=argparse.FileType("w"), default="-")
    parser.add_argument("-t", "--title")
    parser.add_argument("report", type=argparse.FileType("r"))
    args = parser.parse_args()
    with args.report:
        report = [ReportEntry.parse_raw(line) for line in args.report]
    versions = get_fscacher_versions()
    tables = report2tables(report, versions)
    with args.outfile:
        if args.title:
            print(args.title, file=args.outfile)
            print("=" * len(args.title), file=args.outfile)
            print(file=args.outfile)
        for tbl in tables:
            if args.format == "rst":
                print(tbl.as_rst(), file=args.outfile)
            else:
                print(tbl.as_markdown(), file=args.outfile)
            print(file=args.outfile)
        if args.format == "rst":
            print(".. vim:set nowrap:", file=args.outfile)
        else:
            print("<!-- vim:set nowrap: -->", file=args.outfile)


if __name__ == "__main__":
    main()
