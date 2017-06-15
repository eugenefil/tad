# TODO: tests
# two sources: tables
# one source: query
# one source: table
# typed header

import subprocess
import csv
from io import StringIO
import os
import os.path as path
import shutil

import pytest


DBPATH = 'vfpdb/db.dbc'


# add path to diff to PATH
rootdir = pytest.config.rootdir
os.environ['PATH'] = (
    str(rootdir.join('tad')) + ':' + os.environ['PATH']
)


class RunError(subprocess.CalledProcessError):
    """Raised when process returned non-zero exit status."""

    def init(self, returncode, cmd, output=None, stderr=None):
        super().init(returncode, cmd, output=output)
        self.stderr = stderr

    def __str__(self):
        msg = super().__str__()
        return "%s Captured stderr:\n%s" % (msg, self.stderr)


def run(args, input=None):
    """Run process, pipe input to it and return its captured output.

    Output is returned as a tuple (stdout, stderr). If program exits
    with non-zero code, throw RunError exception.
    """
    stdin = None
    if not input is None:
        input = input.encode('utf-8')
        stdin = subprocess.PIPE

    # use binary streams (universal_newlines=False) and encode/decode
    # manually, otherwise \r\n in returned query string fields gets
    # converted to \n, i.e. data gets corrupted
    p = subprocess.Popen(
        args,
        stdin=stdin,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    out, err = [
        s.decode('utf-8')
        for s in p.communicate(input)
    ]

    if p.returncode != 0:
        raise RunError(p.returncode, p.args, output=out, stderr=err)

    return out, err


def diff(db1dir, db2dir, table1, table2=None):
    """Diff two tables and return captured diff parsed with csv."""
    dbname = path.basename(DBPATH)
    cmd = (
        ['tad-diff'] +
        [path.join(dbdir, dbname) for dbdir in [db1dir, db2dir]] +
        [table1] +
        ([table2] if table2 else [])
    )

    out, err = run(cmd)
    return list(csv.reader(StringIO(out)))


@pytest.fixture
def tmpdb(tmpdir):
    """Create two temp databases for diffing.

    Copy test database to temp directory subdirs db1 and db2.

    chdir to temp directory, chdir back on teardown. We have to do this
    to keep paths relative, because adosql is a Windows program, but
    when testing in Cygwin tmpdir will be an absolute unix path, which
    will break adosql.
    """
    origdbdir = path.dirname(DBPATH)
    shutil.copytree(origdbdir, tmpdir.join('db1'))
    shutil.copytree(origdbdir, tmpdir.join('db2'))
    with tmpdir.as_cwd():
        yield None


def test_two_query_sources(tmpdb):
    assert diff(
        'db1',
        'db2',
        'select * from empty',
        'select * from full'
    ) == [
        ['@@', 'id', 'name'],
        ['+++', '1', 'john']
    ]

    # assert diff(
    #     'db1',
    #     'db2',
    #     'empty',
    #     'full'
    # ) == [
    #     ['@@', 'id', 'name'],
    #     ['+++', '1', 'john']
    # ]

    # assert diff(
    #     'db1',
    #     'db2',
    #     'select * from full'
    # ) == [['@@', 'id', 'name']]

    # assert diff('db1', 'db2', 'full') == [['@@', 'id', 'name']]
