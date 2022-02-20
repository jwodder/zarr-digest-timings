SHELL = /bin/bash

all : $(addsuffix /README.rst,$(wildcard reports/*))

%/README.rst : %/*.jsonl report2table.py fscacher-versions.csv %/DESCRIPTION
	cat $(@D)/*.jsonl | nox -e report2table -- -t "$$(< $(@D)/DESCRIPTION)" -o $@ -
