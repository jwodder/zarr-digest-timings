This repository contains a script, ``zarr-digest-timings.py``, for repeatedly
running various implementations of a Zarr checksum calculation routine with
different types of caching and displaying the average runtime.  The script is
run via nox_, which manages installation of the proper varying dependencies.

.. _nox: https://nox.thea.codes

Python 3.7 or higher is required.

Usage
=====

::

    nox -e <env> -- [<options>] <dirpath> <implementation>

Run a given checksumming function on the given directory a number of times and
print out the average runtime.  If caching is in effect and
``--no-clear-cache`` is not given, an initial function call (populating the
cache) will be timed & reported separately.

Arguments
---------

``<env>``
    The nox environment in which to run the script; can be either
    ``nothreads``, which uses the non-threaded fscacher 0.1.6, or ``threads``,
    which uses the threaded implementation on the `gh-66 branch`_.  (Note that
    neither version of fscacher will have any effect by default unless the
    ``-c`` or ``-C`` option is passed to the script.)

    .. _gh-66 branch: https://github.com/con/fscacher/pull/67

``<dirpath>``
    The path to a directory tree to calculate the Zarr checksum of

``<implementation>``
    The checksumming function to use:

    ``sync``
        Walks the directory tree synchronously and breadth-first, digesting
        files, and constructs an in-memory tree for calculating the Zarr digest

    ``fastio``
        Like ``sync``, but walks the directory tree using `a multithreaded
        walk`__

        __ https://gist.github.com/jart/0a71cde3ca7261f77080a3625a21672b

    ``async``
        Like ``sync``, but walks the directory asynchronously using trio_.  The
        number of workers is controlled by the ``--threads`` option.  This
        implementation is not affected by ``--cache-files``.

        .. _trio: https://github.com/python-trio/trio

    ``recursive``
        Walks & digests the directory tree depth-first using recursion

Options
-------

-c, --cache                     Use fscacher to cache the Zarr directory
                                checksumming routine

-C, --cache-files               Use fscacher to cache digests for individual
                                files

--clear-cache, --no-clear-cache
                                Whether to clear the cache on program startup
                                [default: ``--clear-cache``]

-n INT, --number INT            Set the number of times to call the function
                                (not counting the initial cache-populating
                                call, if any).  As a special case, passing 0
                                will cause the script to simply call the
                                function once and print out the checksum
                                without any timing.  [default: 100]

-R FILE, --report FILE          Append a report of the run, containing the
                                average time and the various input parameters,
                                as a line of JSON to the given file

-T INT, --threads INT           Set the number of threads to use when walking a
                                directory tree.  This affects both the
                                ``fastio`` implementation and the threaded
                                fscacher implementation.  The default value is
                                the number of CPU cores plus 4, to a maximum of
                                32.

-v, --verbose                   Log the result of each function call with a
                                timestamp as it finishes.  Specify this option
                                up to two additional times for more debug
                                logging.


``mktree.py``
=============

::

    python3 mktree.py <dirpath> <specfile>

The ``mktree.py`` script can be used to generate a sample directory tree for
running ``zarr-digest-timings.py`` on.  The directory is generated according to
a *layout specification*, which is a JSON file whose contents take one of the
following forms:

- A list ``lst`` of ``n+1`` integers, possibly with a file object (see below)
  appended — The tree will consist of ``lst[0]`` directories, each of which
  contains ``lst[1]`` sub-directories, each of which contains ``lst[2]``
  sub-subdirectories, and so on, with the directories at level ``n-1``
  consisting of ``lst[n]`` files.  If a file object is supplied, the files will
  be generated according to its specification; otherwise, they will be empty.

- An object mapping path names to layout sub-specifications, file objects, or
  ``null`` — For each key that maps to a layout sub-specification, a
  subdirectory will be created in the directory with that name and layout.  For
  each key that maps to a file object or ``null``, a file will be created in
  the directory with that name and according to that specification (an empty
  file for ``null``\s).

A *file object* is an object specifying the size of a file to create; it can
take the following forms:

- If the object contains a ``"size": INT`` field, the file will be that size.

- Otherwise, the object must contain a ``"maxsize": INT`` field and an optional
  ``"minsize": INT`` field (default value: 0).  The file will be created with a
  random size within the given range, inclusive.

All files are created with random bytes as data.

Some sample layout specifications can be found in the ``layouts/`` directory.


``time-all.sh``
===============

::

    bash time-all.sh [<options>] <dirpath>

The bash script ``time-all.sh`` runs ``zarr-digest-timings.py`` with all
non-redundant configurations against a given directory tree for a given number
of threads, and it generates a JSON Lines report.

Options
-------

-n INT                      Set the number of times to run the checksumming
                            function for each configuration [default: 100]

-R FILE                     Save the report to the given file [default:
                            ``time-all.json``]

-T INT                      Set the number of threads to use when walking a
                            directory tree.  See above for the default.

-v                          Increase the verbosity of
                            ``zarr-digest-timings.py``; can be specified
                            multiple times


``report2table``
================

::

    nox -e report2table -- [<options>] <reportfile>

The ``report2table.py`` script takes a JSON Lines report generated via the
``--report`` option of ``zarr-digest-timings.py`` and renders it as a
reStructuredText or GitHub-Flavored Markdown table.  It should be run via nox
in order to manage its dependencies.

All of the entries in the report should have been generated on the same
machine.  If any entries were generated on different paths or with different
numbers of threads, multiple tables will be produced, one for each path-thread
combination.  If two or more entries were produced by the same configuration,
their times will be combined.

For configurations that make use of caching, the corresponding cell in the
resulting table will consist of two times separated by a slash; the first time
is the runtime of the initial cache-populating call, while the second time is
the average of the other calls.

Options
-------

-f <rst|md>, --format <rst|md>  Specify whether to produce a reStructuredText
                                (``rst``) or Markdown (``md``) table  [default:
                                ``rst``]

-o FILE, --outfile FILE         Output the tables to the specified file
