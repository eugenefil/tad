# TODO tests:
# diff2sql dies in the middle producing unfinished but correct input
#   to adosql (must be very rare), adosql could wait for EOF text marker

import os
import os.path as path
import shutil
import csv
from io import StringIO

import pytest

from testutil import run, adosql


DBPATH = 'vfpdb/db.dbc'


# add path to tad-patch to PATH
rootdir = pytest.config.rootdir
os.environ['PATH'] = (
    str(rootdir.join('tad')) + ':' + os.environ['PATH']
)


@pytest.fixture
def tmpdb(tmpdir):
    """Create temp database named db.

    Copy test database to temp directory and chdir to it. chdir back on
    teardown.

    We have to chdir to keep paths relative, because adosql is a Windows
    program, but when testing in Cygwin tmpdir will be a absolute unix
    path, which will break adosql.
    """
    DBDIR = 'db'
    origdbdir, dbname = path.split(DBPATH)
    shutil.copytree(origdbdir, tmpdir.join(DBDIR))
    with tmpdir.as_cwd():
        yield path.join(DBDIR, dbname)


def patch(db, table, diffrows, key=None):
    """Patch database table with diffrows and return its rows.

    If not None, key must be a string of comma-separated column names
    to use as key when building queries.
    """
    f = StringIO()
    csv.writer(f).writerows(diffrows)
    diff = f.getvalue()

    cmd = (
        ['tad-patch'] +
        (['-key', key] if key else []) +
        [db, table]
    )

    run(cmd, input=diff)
    return adosql(db, 'select * from ' + table)


# With tad-patch we don't test that it issues right queries
# (e.g. deletes chosen rows and keeps other intact). That is the job
# of tad-diff2sql. Instead we test that feeding it a diff results in
# database changes, i.e. that tying tad-diff2sql with adosql works.
tests = [
    'basic-patch',
    'full',
    [
        # note: second column misses type, missing types in
        # typed header mode should work ok
        ['@@', 'id integer', 'name'],
        ['---', '1', 'john'],
        ['+++', '2', 'bill']
    ],
    [['2', 'bill']],


    'diff-with-no-changes',
    'empty',
    [['@@', 'id', 'name']],
    [],


    'empty-input', 'empty', [], [],


    'utf8-diff',
    'empty',
    [
        ['@@', 'id integer', 'name string'],
        ['+++', '1', 'Васисуалий']
    ],
    [['1', 'Васисуалий']],


    'diff-with-crlf',
    'empty',
    [
        ['@@', 'id integer', 'name string'],
        ['+++', '1', '\r\n']
    ],
    [['1', '\r\n']]
]
@pytest.mark.parametrize(
    'testid,table,diffrows,resultrows',
    # split tests into 4-tuples
    [tests[i:i + 4] for i in range(0, len(tests), 4)]
)
def test_patch(testid, table, diffrows, resultrows, tmpdb):
    assert patch(tmpdb, table, diffrows)[1:] == resultrows


def test_use_table_key(tmpdb):
    # insert second row with the same key, so that deleting by key
    # deletes all rows, but deleting with all columns as key deletes
    # only one row
    adosql(tmpdb, "insert into full values (1, 'bill')")
    assert patch(
        tmpdb,
        'full',
        [
            ['@@', 'id integer', 'name'],
            ['---', '1', 'john']
        ],
        key='id'
    )[1:] == []
