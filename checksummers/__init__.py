from .fastio import FastioWalker
from .oothreads import OOThreadsWalker
from .recursive import RecursiveChecksummer
from .sync import SyncWalker
from .trio import TrioWalker, TrioWalker3

CLASSES = {
    "sync": SyncWalker,
    "fastio": FastioWalker,
    "oothreads": OOThreadsWalker,
    "trio": TrioWalker,
    "trio3": TrioWalker3,
    "recursive": RecursiveChecksummer,
}

# Classes that are affected by the `threads` parameter
THREADED_CLASSES = {"fastio", "oothreads", "trio", "trio3"}
