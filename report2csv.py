#!/usr/bin/env python3
import argparse
import csv
import json


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert a file of JSON Lines zarr-digest-timings reports to CSV"
    )
    parser.add_argument("-o", "--outfile", type=argparse.FileType("w"), default="-")
    parser.add_argument("report", type=argparse.FileType("r"))
    args = parser.parse_args()
    with args.outfile:
        out = csv.DictWriter(
            args.outfile,
            [
                "dirpath",
                "implementation",
                "fscacher_version",
                "threaded_fscacher",
                "caching",
                "caching_files",
                "threads",
                "number",
                "first_call",
                "avgtime",
            ],
        )
        out.writeheader()
        with args.report:
            for line in args.report:
                out.writerow(json.loads(line))


if __name__ == "__main__":
    main()
