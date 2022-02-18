from __future__ import annotations
from collections import deque
from contextlib import contextmanager
import logging
import os.path
from pathlib import Path
from threading import Condition, Lock
from typing import (
    ContextManager,
    Deque,
    Generic,
    Iterable,
    Iterator,
    Optional,
    Tuple,
    TypeVar,
    Union,
)
from interleave import interleave
from .bases import IterativeChecksummer

T = TypeVar("T")

log = logging.getLogger(__name__)


class OOThreadsWalker(IterativeChecksummer):
    def digest_walk(self, dirpath: Union[str, Path]) -> Iterable[Tuple[Path, str]]:
        if not os.path.isdir(dirpath):
            return
        jobs = JobStack([Path(dirpath)])
        with interleave(
            [self.worker(jobs) for _ in range(self.threads)], max_workers=self.threads
        ) as it:
            yield from it

    def worker(self, jobs: JobStack[Path]) -> Iterator[Tuple[Path, str]]:
        for ctx in jobs:
            with ctx as path:
                try:
                    for p in path.iterdir():
                        if p.is_dir():
                            jobs.put(p)
                        else:
                            yield (p, self.md5digest(p))
                except OSError:
                    log.exception("Error scanning directory %s", path)


class JobStack(Generic[T]):
    """
    Synchronized LIFO queue for use by a collection of concurrent workers that
    are both producers and consumers.  Specifically, after the queue is
    initialized with some starting tasks, each worker iterates through the
    queue task by task as available; for each task, the worker operating on it
    adds some number of new tasks to the queue and then marks the current task
    finished.  Once the queue is empty and all tasks have been marked finished,
    the iterators stop yielding values.

    Sample usage by a worker:

    .. code:: python

        for taskctx in job_queue:
            with taskctx as task:
                # Operate on task
                # Call job_queue.put(new_task) some number of times
    """

    def __init__(self, iterable: Optional[Iterable[T]] = None) -> None:
        self._lock = Lock()
        self._cond = Condition(self._lock)
        self._queue: Deque[T] = deque()
        self._tasks = 0
        if iterable is not None:
            self._queue.extend(iterable)
            self._tasks += len(self._queue)

    def __iter__(self) -> Iterator[ContextManager[T]]:
        while True:
            with self._lock:
                while True:
                    if not self._tasks:
                        return
                    if not self._queue:
                        self._cond.wait()
                        continue
                    value = self._queue.pop()
                    break
            yield self._job_ctx(value)

    @contextmanager
    def _job_ctx(self, value: T) -> Iterator[T]:
        try:
            yield value
        finally:
            with self._lock:
                self._tasks -= 1
                if self._tasks <= 0:
                    self._cond.notify_all()

    def put(self, value: T) -> None:
        with self._lock:
            self._queue.append(value)
            self._tasks += 1
            self._cond.notify()
