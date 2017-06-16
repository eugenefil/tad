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


def sync(srcdb, destdb, src_table, dest_table=None, target_table=None):
    """Sync two database tables and return their rows for comparison."""
    cmd = (
        ['tad-sync'] +
        (['-t', target_table] if target_table else []) +
        [srcdb, destdb, src_table] +
        ([dest_table] if dest_table else [])
    )
    run(cmd)
    return (
        adosql(srcdb, src_table)[1:],
        adosql(destdb, dest_table)[1:]
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


def test_sync(tmpdbs):
    srcdb, destdb = tmpdbs
    srcrows, destrows = sync(
        srcdb,
        destdb,
        'select * from full',
        'select * from empty',
        target_table = 'empty'
    )
    assert srcrows == destrows == [['1', 'john']]
