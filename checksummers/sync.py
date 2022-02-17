from collections import deque
from pathlib import Path
from typing import Iterable, Tuple, Union
from .bases import IterativeChecksummer


class SyncWalker(IterativeChecksummer):
    def digest_walk(self, dirpath: Union[str, Path]) -> Iterable[Tuple[Path, str]]:
        dirs = deque([Path(dirpath)])
        while dirs:
            for p in dirs.popleft().iterdir():
                if p.is_dir():
                    dirs.append(p)
                else:
                    yield (p, self.md5digest(p))
