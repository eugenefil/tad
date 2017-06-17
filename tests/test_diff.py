import csv
from io import StringIO
import os
import os.path as path
import shutil

import pytest

from testutil import run


DBPATH = 'vfpdb/db.dbc'


# add path to diff to PATH
rootdir = pytest.config.rootdir
os.environ['PATH'] = (
    str(rootdir.join('tad')) + ':' + os.environ['PATH']
)


def diff(
        db1dir,
        db2dir,
        table1,
        table2=None,
        typed_header=False,
        key=None
    ):
    """Diff two tables and return captured diff parsed with csv.

    If typed_header is True, tad-diff must return typed header.
  
    If not None, key must be a string of comma-separated column names
    to use as key when comparing.
    """
    dbname = path.basename(DBPATH)
    cmd = (
        ['tad-diff'] +
        (['-typed-header'] if typed_header else []) +
        (['-key', key] if key else []) +
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


def test_diff_with_key(tmpdb):
    assert diff(
        'db2',
        'db2',
        "select 1 id, 'john' name from full",
        "select 2 id, 'john' name from full",
        key='id'
    )[1:] == [
        ['+++', '2.0', 'john'],
        ['---', '1.0', 'john']
    ]


def test_diff_with_multicolumn_key(tmpdb):
    assert diff(
        'db2',
        'db2',
        "select 'john' name, '123' tel, 'dev' job from full",
        "select 'john' name, '456' tel, 'tester' job from full",
        key='name,tel'
    )[1:] == [
        ['+++', 'john', '456', 'tester'],
        ['---', 'john', '123', 'dev']
    ]
