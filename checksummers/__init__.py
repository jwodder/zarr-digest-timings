from .fastio import FastioWalker
from .oothreads import OOThreadsWalker
from .recursive import RecursiveChecksummer
from .sync import SyncWalker
from .trio2 import TrioWalker2
from .trio import TrioWalker

CLASSES = {
    "sync": SyncWalker,
    "fastio": FastioWalker,
    "oothreads": OOThreadsWalker,
    "trio": TrioWalker,
    "trio2": TrioWalker2,
    "recursive": RecursiveChecksummer,
}

# Classes that are affected by the `threads` parameter
THREADED_CLASSES = {"fastio", "oothreads", "trio", "trio2"}
