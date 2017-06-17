# TODO issue warning when target_table is a query

import os
import os.path as path
import shutil

import pytest

from testutil import run, adosql


DBPATH = 'vfpdb/db.dbc'


# add path to diff to PATH
rootdir = pytest.config.rootdir
os.environ['PATH'] = (
    str(rootdir.join('tad')) + ':' + os.environ['PATH']
)


def sync(
        srcdb,
        destdb,
        src_table,
        dest_table=None,
        target_table=None,
        key=None
    ):
    """Sync two database tables and return their rows for comparison.

    If not None, key must be a string of comma-separated column names
    to use as key when diffing and patching.
    """
    cmd = (
        ['tad-sync'] +
        (['-target-table', target_table] if target_table else []) +
        (['-key', key] if key else []) +
        [srcdb, destdb, src_table] +
        ([dest_table] if dest_table else [])
    )
    run(cmd)

    tosql = lambda tbl: tbl if ' ' in tbl else "select * from " + tbl
    return (
        adosql(srcdb, tosql(src_table))[1:],
        adosql(destdb, tosql(dest_table or src_table))[1:]
    )


@pytest.fixture
def tmpdbs(tmpdir):
    """Create src and dest temp databases for syncing.

    Copy test database to temp directory twice: once for src and once
    for dest. Make all tables of dest database empty. Return database
    paths relative to tmpdir in a tuple (srcdb, destdb).

    chdir to temp directory, chdir back on teardown. We have to do this
    to keep paths relative, because adosql is a Windows program, but
    when testing in Cygwin tmpdir will be an absolute unix path, which
    will break adosql.
    """
    origdbdir, dbname = path.split(DBPATH)
    shutil.copytree(origdbdir, tmpdir.join('src'))
    shutil.copytree(origdbdir, tmpdir.join('dest'))
    with tmpdir.join('dest').as_cwd():
        # make all tables of dest db empty
        shutil.copy('empty.dbf', 'full.dbf')
    with tmpdir.as_cwd():
        yield [path.join(dir, dbname) for dir in ['src', 'dest']]


@pytest.mark.parametrize(
    'testid,src_table,dest_table,target_table',
    [
        (
            'two-queries',
            'select * from full',
            'select * from empty',
            'empty'
        ),
        ('two-tables', 'full', 'empty', None),
        ('same-query', 'select * from full', None, 'full'),
        ('same-table', 'full', None, None)
    ]
)        
def test_sync(testid, src_table, dest_table, target_table, tmpdbs):
    srcdb, destdb = tmpdbs
    srcrows, destrows = sync(
        srcdb,
        destdb,
        src_table,
        dest_table,
        target_table
    )
    assert srcrows == destrows == [['1', 'john']]


def test_use_table_key(tmpdbs):
    srcdb, destdb = tmpdbs
    adosql(destdb, [
        "insert into full values (2, 'john')",
        "insert into full values (2, 'bill')"
    ])
    
    srcrows, destrows = sync(
        srcdb,
        destdb,
        "select * from full where name = 'john'",
        target_table='full',
        key='id'
    )
    # Keeping in mind that only two john rows were compared absence of
    # bill in destdb means "delete from full where id = 2" was
    # issued. In turn that means: a) diff was done with id as key
    # (otherwise it would be "update id = 1..."), b) patch was also
    # done with key (otherwise "delete...where id = 2 and name =
    # 'john'" would delete only john).
    assert (
        srcrows ==
        # destrows won't show bill, if he is there, so select all
        adosql(destdb, 'select * from full')[1:] ==
        [['1', 'john']]
    )
