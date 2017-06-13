# TODO: tests
# two updated columns
# different updated columns in different rows
# updated row with --> action tag
# typed header
# empty input
# ignore context rows
# multiple changes
# break on schema row in diff
# detect input format (csv, tsv)
# spaces in column names

import subprocess
import csv
from io import StringIO
import os
import os.path as path

import pytest


# add path to hldiff2sql to PATH
os.environ['PATH'] = (
    path.dirname(path.dirname(__file__)) + ':' + os.environ['PATH']
)


class RunError(subprocess.CalledProcessError):
    """Raised when process returned non-zero exit status."""

    def init(self, returncode, cmd, output=None, stderr=None):
        super().init(returncode, cmd, output=output)
        self.stderr = stderr

    def __str__(self):
        msg = super().__str__()
        return "%s Captured stderr:\n%s" % (msg, self.stderr)


def run(args, input):
    """Run process, pipe input to it and return its captured output.

    Output is returned as a tuple (stdout, stderr). If program exits
    with non-zero code, throw RunError exception.
    """
    # use binary streams (universal_newlines=False) and encode/decode
    # manually, otherwise \r\n in returned query string fields gets
    # converted to \n, i.e. data gets corrupted
    p = subprocess.Popen(
        args,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    out, err = [
        s.decode('utf-8')
        for s in p.communicate(input.encode('utf-8'))
    ]

    if p.returncode != 0:
        raise RunError(p.returncode, p.args, output=out, stderr=err)

    return out, err


def convert(input_rows):
    f = StringIO()
    csv.writer(f).writerows(input_rows)
    out, err = run(['hldiff2sql', 't'], input=f.getvalue())
    return list(csv.reader(StringIO(out), delimiter='\t'))


def test_inserted_row():
    assert convert([
        ['@@', 'id', 'name'],
        ['+++', '1', 'john'],
        ['+++', '2', 'bill']
    ]) == [
        ['insert into t (id, name) values (?, ?)'],
        ['id', 'name'],
        ['1', 'john'],
        ['2', 'bill']
    ]


def test_deleted_row():
    assert convert([
        ['@@', 'id', 'name'],
        ['---', '1', 'john'],
        ['---', '2', 'bill']
    ]) == [
        ['delete from t where id = ? and name = ?'],
        ['id', 'name'],
        ['1', 'john'],
        ['2', 'bill']
    ]


def test_updated_row():
    assert convert([
        ['@@', 'id', 'name', 'age'],
        ['->', '1', 'john->bill', '50->60']
    ]) == [
        ['update t set name = ?, age = ? where id = ? and name = ? and age = ?'],
        ['name', 'age', 'id', 'name', 'age'],
        ['bill', '60', '1', 'john', '50']
    ]
