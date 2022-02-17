#!/bin/bash
function usage {
    echo "Usage: $0 [-n <number>] [-R <report>] [-T <threads>] [-vvvv] <dirpath>"
}

base_opts=
report=time-all.json

while getopts :hn:R:T:v opt
do
    case "$opt" in
        h) usage
           exit
           ;;
        n) base_opts="$base_opts -n $OPTARG"
           ;;
        R) report="$OPTARG"
           ;;
        T) base_opts="$base_opts -T $OPTARG"
           ;;
        v) base_opts="$base_opts -v"
           ;;
        *) usage >&2
           exit 2
           ;;
    esac
done
shift $((OPTIND-1))

dirpath="$1"
if [ -z "$dirpath" ]
then usage >&2
     exit 2
fi

set -ex

nox -e nothreads -- -R "$report" $base_opts "$dirpath" sync
nox -e nothreads -- -R "$report" $base_opts "$dirpath" fastio
nox -e nothreads -- -R "$report" $base_opts "$dirpath" recursive
nox -e nothreads -- -R "$report" $base_opts "$dirpath" async

for env in threads nothreads
do
    nox -e "$env" -- -R "$report" $base_opts --cache "$dirpath" sync
    nox -e "$env" -- -R "$report" $base_opts --cache "$dirpath" fastio
    nox -e "$env" -- -R "$report" $base_opts --cache "$dirpath" recursive
    nox -e "$env" -- -R "$report" $base_opts --cache "$dirpath" async

    nox -e "$env" -- -R "$report" $base_opts --cache-files "$dirpath" sync
    nox -e "$env" -- -R "$report" $base_opts --cache-files "$dirpath" fastio
    nox -e "$env" -- -R "$report" $base_opts --cache-files "$dirpath" recursive

    nox -e "$env" -- -R "$report" $base_opts --cache --cache-files "$dirpath" sync
    nox -e "$env" -- -R "$report" $base_opts --cache --cache-files "$dirpath" fastio
    nox -e "$env" -- -R "$report" $base_opts --cache --cache-files "$dirpath" recursive
done
