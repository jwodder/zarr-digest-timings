from .fastio import ThreadedWalker
from .recursive import RecursiveChecksummer
from .sync import SyncWalker
from .trio import AsyncWalker

CLASSES = {
    "sync": SyncWalker,
    "async": AsyncWalker,
    "fastio": ThreadedWalker,
    "recursive": RecursiveChecksummer,
}
