# TODO: tests
# inserted row
# typed header
# deleted row
# updated row
# empty input
# ignore context rows
# multiple changes
# break on schema row in diff
# detect input format (csv, tsv)

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


def run(input):
    p = subprocess.Popen(
        ['hldiff2sql', 't'],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    out, err = [
        s.decode('utf-8')
        for s in p.communicate(input.encode('utf-8'))
    ]
    return out, err


def convert(input_rows):
    f = StringIO()
    csv.writer(f).writerows(input_rows)
    out, err = run(input=f.getvalue())
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
