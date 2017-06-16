# TODO tests:
# empty diff produces no changes
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


def patch(db, table, diffrows):
    """Patch database table with diffrows and return its rows."""
    f = StringIO()
    csv.writer(f).writerows(diffrows)
    diff = f.getvalue()

    run(['tad-patch', db, table], input=diff)
    return adosql('select * from ' + table, db)


def test_patch(tmpdb):
    assert patch(tmpdb, 'full', [
        # note: second column misses type
        ['@@', 'id integer', 'name'],
        ['---', '1', 'john'],
        ['+++', '2', 'bill']
    ])[1:] == [
        ['2', 'bill']
    ]
