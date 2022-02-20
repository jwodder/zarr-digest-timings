from .fastio import FastioWalker
from .oothreads import OOThreadsWalker
from .recursive import RecursiveChecksummer
from .sync import SyncWalker
from .trio import TrioLiveWalker, TrioWalker

CLASSES = {
    "sync": SyncWalker,
    "fastio": FastioWalker,
    "oothreads": OOThreadsWalker,
    "trio": TrioWalker,
    "trio-live": TrioLiveWalker,
    "recursive": RecursiveChecksummer,
}

# Classes that are affected by the `threads` parameter
THREADED_CLASSES = {"fastio", "oothreads", "trio", "trio-live"}
