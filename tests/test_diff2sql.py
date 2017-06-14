# TODO: tests
# wrong diff format
# spaces in column names

import subprocess
import csv
from io import StringIO
import os
import os.path as path

import pytest


# add path to diff2sql to PATH
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


def convert(input_rows, typed_header=False, delimiter='\t'):
    """Convert diff rows to SQL statements with diff2sql.

    If typed_header is True, tell diff2sql that input has typed
    header.

    Use delimiter as CSV delimiter. Tab is used by default, since this
    allows to parse whole output from diff2sql into a list with csv
    without breaking queries into parts on commas.
    """
    csvargs = {'delimiter': delimiter} if delimiter else {}
    delimiter_arg = ['-t'] if delimiter == '\t' else []

    f = StringIO()
    csv.writer(f, **csvargs).writerows(input_rows)
    input = f.getvalue()
    
    cmd = (
        ['tad-diff2sql'] +
        (['-typed-header'] if typed_header else []) +
        delimiter_arg +
        ['t']
    )
    
    out, err = run(cmd, input)
    return list(csv.reader(StringIO(out), **csvargs))


tests = [
    'insert',
    [
        ['@@', 'id', 'name'],
        ['+++', '1', 'john'],
        ['+++', '2', 'bill']
    ],
    [
        ['insert into t (id, name) values (?, ?)'],
        ['id', 'name'],
        ['1', 'john'],
        ['2', 'bill']
    ],
    
    'delete',
    [
        ['@@', 'id', 'name'],
        ['---', '1', 'john'],
        ['---', '2', 'bill']
    ],
    [
        ['delete from t where id = ? and name = ?'],
        ['id', 'name'],
        ['1', 'john'],
        ['2', 'bill']
    ],

    'update',
    [
        ['@@', 'id', 'name', 'age'],
        ['->', '1', 'john->bill', '50->60']
    ],
    [
        ['update t set name = ?, age = ? where id = ? and name = ? and age = ?'],
        ['name', 'age', 'id', 'name', 'age'],
        ['bill', '60', '1', 'john', '50']
    ],

    'different-kinds-of-changes',
    [
        ['@@', 'id', 'name'],
        ['+++', '1', 'john'],
        ['---', '2', 'bill'],
        ['->', '3', 'sam->pat']
    ],
    [
        ['insert into t (id, name) values (?, ?)'],
        ['id', 'name'],
        ['1', 'john'],
        [],
        ['delete from t where id = ? and name = ?'],
        ['id', 'name'],
        ['2', 'bill'],
        [],
        ['update t set name = ? where id = ? and name = ?'],
        ['name', 'id', 'name'],
        ['pat', '3', 'sam']
    ],

    'empty-input', [], [],
    'empty-diff-input', [['@@', 'id', 'name']], [],

    'update_with_other_action_tag',
    [
        ['@@', 'id', 'tag'],
        ['--->', '1', '->--->-->']
    ],
    [
        ['update t set tag = ? where id = ? and tag = ?'],
        ['tag', 'id', 'tag'],
        ['-->', '1', '->']
    ],

    'ignore-other-action-tags',
    [
        ['@@', 'tag'],
        ['', 'context row'],
        ['...', 'skipped rows'],
        [':', 'reordered row']
    ],
    [],

    'utf-8',
    [
        ['@@', 'name'],
        ['+++', 'Василий']
    ],
    [
        ['insert into t (name) values (?)'],
        ['name'],
        ['Василий']
    ],

    'crlf',
    [
        ['@@', 'text'],
        ['+++', '1\r\n2']
    ],
    [
        ['insert into t (text) values (?)'],
        ['text'],
        ['1\r\n2']
    ]
]
@pytest.mark.parametrize(
    'testid,in_rows,out_rows',
    # split list of tests into triples
    [tests[i:i + 3] for i in range(0, len(tests), 3)]
)
def test_convert(testid, in_rows, out_rows):
    assert convert(in_rows) == out_rows


def test_input_with_typed_header():
    assert convert(
        [
            # note: second column misses type
            ['@@', 'id integer', 'name'],
            ['+++', '1', 'john'],
            ['---', '2', 'bill'],
            ['->', '3', 'sam->pat']
        ],
        typed_header=True
    ) == [
        ['insert into t (id, name) values (?, ?)'],
        ['id integer', 'name'],
        ['1', 'john'],
        [],
        ['delete from t where id = ? and name = ?'],
        ['id integer', 'name'],
        ['2', 'bill'],
        [],
        ['update t set name = ? where id = ? and name = ?'],
        ['name', 'id integer', 'name'],
        ['pat', '3', 'sam']
    ]


def test_break_on_schema_change():
    with pytest.raises(RunError) as excinfo:
        convert([
            ['!', '', '+++'],
            ['@@', 'id', 'name']
        ])
    assert excinfo.value.returncode == 1
    excinfo.match('Error: NotSupported')


def test_csv_input_output():
    assert convert(
        [
            ['@@', 'id', 'name'],
            ['+++', '1', 'john']
        ],
        delimiter=','
    ) == [
        # note: query is broken into parts since we parse all output
        # from diff2sql with csv for simplicity, not just data bits
        ['insert into t (id', ' name) values (?', ' ?)'],
        ['id', 'name'],
        ['1', 'john']
    ]
