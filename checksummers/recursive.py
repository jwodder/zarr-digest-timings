from pathlib import Path
from typing import Optional, Union
from dandischema.digests.zarr import get_checksum
from .bases import ZarrChecksummer


class RecursiveChecksummer(ZarrChecksummer):
    def checksum(
        self, dirpath: Union[str, Path], root: Optional[Union[str, Path]] = None
    ) -> str:
        recurse = getattr(self, "recurse", self.checksum)
        files = {}
        dirs = {}
        if root is None:
            root = Path(dirpath)
        for p in Path(dirpath).iterdir():
            key = p.relative_to(root).as_posix()
            if p.is_file():
                files[key] = self.md5digest(p)
            elif any(p.iterdir()):
                dirs[key] = recurse(p, root)
        return get_checksum(files, dirs)
