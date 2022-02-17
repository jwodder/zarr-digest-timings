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


def zarr_checksum(session, fscacher_req):
    session.install(
        "argset ~= 0.1", "click >= 8.0", "dandischema >= 0.5.1", fscacher_req
    )
    session.run("python", "zarr-checksum.py", *session.posargs)


@nox.session
def mktree(session):
    """Generate a sample tree"""
    session.install("click >= 8.0")
    session.run("python", "mktree.py", *session.posargs)
