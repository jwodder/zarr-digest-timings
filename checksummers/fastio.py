import logging
import os.path
from pathlib import Path
import threading
from typing import Iterable, Tuple, Union
from .bases import IterativeChecksummer

log = logging.getLogger(__name__)


class FastioWalker(IterativeChecksummer):
    def digest_walk(self, dirpath: Union[str, Path]) -> Iterable[Tuple[Path, str]]:
        if not os.path.isdir(dirpath):
            return
        lock = threading.Lock()
        on_input = threading.Condition(lock)
        on_output = threading.Condition(lock)
        tasks = 1
        paths = [dirpath]
        output = []
        threads = self.threads

        def worker():
            nonlocal tasks
            while True:
                with lock:
                    while True:
                        if not tasks:
                            output.append(None)
                            on_output.notify()
                            return
                        if not paths:
                            on_input.wait()
                            continue
                        path = paths.pop()
                        break
                try:
                    for item in os.listdir(path):
                        subpath = os.path.join(path, item)
                        if os.path.isdir(subpath):
                            with lock:
                                tasks += 1
                                paths.append(subpath)
                                on_input.notify()
                        else:
                            digest = self.md5digest(subpath)
                            with lock:
                                output.append((Path(subpath), digest))
                                on_output.notify()
                except OSError:
                    log.exception("Error scanning directory %s", path)
                finally:
                    with lock:
                        tasks -= 1
                        if not tasks:
                            on_input.notify_all()

        workers = [
            threading.Thread(
                target=worker, name=f"fastio.walk {i} {dirpath}", daemon=True
            )
            for i in range(threads)
        ]
        for w in workers:
            w.start()
        while threads or output:
            with lock:
                while not output:
                    on_output.wait()
                item = output.pop()
            if item:
                yield item
            else:
                threads -= 1
