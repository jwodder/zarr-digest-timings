This repository contains a script, ``zarr-digest-timings.py``, for repeatedly
running various implementations of a Zarr checksum calculation routine with
different types of caching and displaying the average runtime.  The script is
run via nox_, which manages installation of the proper varying dependencies.

.. _nox: https://nox.thea.codes

Usage
=====

::

    nox -e <env> -- [<options>] <dirpath> <implementation>

Run a given checksumming function on the given directory a number of times and
print out the average runtime.

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
        number of workers is controlled by the ``--threads`` option.

        .. _trio: https://github.com/python-trio/trio

    ``recursive``
        Walks the directory tree depth-first using recursion

Options
-------

-c, --cache                     Use fscacher to cache the Zarr directory
                                checksumming routine

-C, --cache-files               Use fscacher to cache digests for individual
                                files

--clear-cache, --no-clear-cache
                                Whether to clear the cache on program startup
                                [default: ``--clear-cache``]

-n INT, --number INT            Set the number of times to run the function.
                                As a special case, passing 0 will cause the
                                script to simply run the function once and
                                print out the checksum without any timing.
                                [default: 100]

-T INT, --threads INT           Set the number of threads to use when walking a
                                directory tree.  This affects both the
                                ``fastio`` implementation and the threaded
                                fscacher implementation.  The default value is
                                the number of CPU cores plus 4, to a maximum of
                                32.


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
