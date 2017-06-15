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


def diff(db1dir, db2dir, table1, table2=None, typed_header=False):
    """Diff two tables and return captured diff parsed with csv.

    If typed_header is True, tad-diff must return typed header.
    """
    dbname = path.basename(DBPATH)
    cmd = (
        ['tad-diff'] +
        (['-typed-header'] if typed_header else []) +
        [path.join(dbdir, dbname) for dbdir in [db1dir, db2dir]] +
        [table1] +
        ([table2] if table2 else [])
    )

    out, err = run(cmd)
    return list(csv.reader(StringIO(out)))


@pytest.fixture
def tmpdb(tmpdir):
    """Create two temp databases for diffing.

    Copy test database to temp directory subdirs db1 and db2. Make all
    tables of db1 empty.

    chdir to temp directory, chdir back on teardown. We have to do this
    to keep paths relative, because adosql is a Windows program, but
    when testing in Cygwin tmpdir will be an absolute unix path, which
    will break adosql.
    """
    origdbdir = path.dirname(DBPATH)
    shutil.copytree(origdbdir, tmpdir.join('db1'))
    shutil.copytree(origdbdir, tmpdir.join('db2'))
    with tmpdir.as_cwd():
        # make all tables of db1 empty
        shutil.copy('db1/empty.dbf', 'db1/full.dbf')
        yield None


@pytest.mark.parametrize(
    'testid,table1,table2',
    [
        ('two-queries', 'select * from empty', 'select * from full'),
        ('two-tables', 'empty', 'full'),
        # table `full' is empty in db1
        ('same-query', 'select * from full', None),
        ('same-table', 'full', None)
    ]
)
def test_diff(testid, table1, table2, tmpdb):
    assert diff('db1', 'db2', table1, table2) == [
        ['@@', 'id', 'name'],
        ['+++', '1', 'john']
    ]


def test_output_typed_header(tmpdb):
    assert diff('db1', 'db2', 'empty', 'full', typed_header=True) == [
        ['@@', 'id integer', 'name string'],
        ['+++', '1', 'john']
    ]


def test_input_output_utf8(tmpdb):
    assert diff('db1', 'db2', "select 'хай' as text from full") == [
        ['@@', 'text'],
        ['+++', 'хай']
    ]


def test_input_output_crlf(tmpdb):
    assert diff('db1', 'db2', "select chr(13) + chr(10) as text from full") == [
        ['@@', 'text'],
        ['+++', '\r\n']
    ]
