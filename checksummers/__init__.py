from .fastio import FastioWalker
from .oothreads import OOThreadsWalker
from .recursive import RecursiveChecksummer
from .sync import SyncWalker
from .trio import TrioWalker

CLASSES = {
    "sync": SyncWalker,
    "fastio": FastioWalker,
    "oothreads": OOThreadsWalker,
    "trio": TrioWalker,
    "recursive": RecursiveChecksummer,
}
