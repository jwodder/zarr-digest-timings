from .fastio import FastioWalker
from .recursive import RecursiveChecksummer
from .sync import SyncWalker
from .trio import TrioWalker

CLASSES = {
    "sync": SyncWalker,
    "fastio": FastioWalker,
    "trio": TrioWalker,
    "recursive": RecursiveChecksummer,
}
