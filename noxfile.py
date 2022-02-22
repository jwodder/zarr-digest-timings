import nox

nox.options.reuse_existing_virtualenvs = True


@nox.session
def nothreads(session):
    """Run zarr-checksum.py using a non-threaded version of fscacher"""
    zarr_checksum(session, "fscacher==0.1.6")


@nox.session
def threads(session):
    """Run zarr-checksum.py using a threaded version of fscacher"""
    zarr_checksum(session, "fscacher @ git+https://github.com/con/fscacher@gh-66")


@nox.session
def xor_bytes(session):
    """
    Run zarr-checksum.py using a version of fscacher that efficiently
    fingerprints directories
    """
    zarr_checksum(session, "fscacher==0.2.0")


@nox.session
def report2table(session):
    """Convert a report file to a table"""
    session.install(
        "dandischema >= 0.5.1",
        "fscacher",
        "interleave",
        "pydantic",
        "trio >= 0.19",
        "txtble ~= 0.12",
    )
    session.run("python", "report2table.py", *session.posargs)


def zarr_checksum(session, fscacher_req):
    session.install(
        "argset ~= 0.1",
        "click >= 8.0",
        "dandischema >= 0.5.1",
        "interleave",
        "trio >= 0.19",
        fscacher_req,
    )
    session.run("python", "zarr-digest-timings.py", *session.posargs)
