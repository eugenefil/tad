# TODO: tests
# wrong diff format
# spaces in column names

import csv
from io import StringIO
import os

import pytest

from testutil import run, RunError


# add path to diff2sql to PATH
rootdir = pytest.config.rootdir
os.environ['PATH'] = (
    str(rootdir.join('tad')) + ':' + os.environ['PATH']
)


def convert(input_rows, typed_header=False, delimiter='\t', key=None):
    """Convert diff rows to SQL statements with diff2sql.

    If typed_header is True, tell diff2sql that input has typed
    header.

    Use delimiter as CSV delimiter. Tab is used by default, since this
    allows to parse whole output from diff2sql into a list with csv
    without breaking queries into parts on commas.

    If not None, key must be a string of comma-separated column names
    to use as key when building queries.
    """
    csvargs = {'delimiter': delimiter} if delimiter else {}
    delimiter_arg = ['-t'] if delimiter == '\t' else []

    f = StringIO()
    csv.writer(f, **csvargs).writerows(input_rows)
    input = f.getvalue()
    
    cmd = (
        ['tad-diff2sql'] +
        (['-typed-header'] if typed_header else []) +
        (['-key', key] if key else []) +
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
    """Test passing diff with typed columns.

    Column names in generated queries must be stripped off types.
    Columns with missing types must not produce error.
    """
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
    excinfo.match('schema changes are not supported')


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


def test_use_table_key():
    """Test using key when building queries.

    INSERT must stay the same, while DELETE and UPDATE must use only key
    fields in the filter.
    """
    assert convert(
        [
            ['@@', 'id integer', 'name'],
            ['+++', '1', 'john'],
            ['---', '2', 'bill'],
            ['->', '3', 'sam->pat']
        ],
        typed_header=True,
        key='id'
    ) == [
        ['insert into t (id, name) values (?, ?)'],
        ['id integer', 'name'],
        ['1', 'john'],
        [],
        ['delete from t where id = ?'],
        ['id integer'],
        ['2'],
        [],
        ['update t set name = ? where id = ?'],
        ['name', 'id integer'],
        ['pat', '3']
    ]


def test_use_multicolumn_table_key():
    assert convert(
        [
            ['@@', 'name', 'tel', 'job'],
            ['---', 'john', '123', 'dev'],
            ['->', 'bill', '345', 'test->sales']
        ],
        typed_header=True,
        key='name,tel'
    ) == [
        ['delete from t where name = ? and tel = ?'],
        ['name', 'tel'],
        ['john', '123'],
        [],
        ['update t set job = ? where name = ? and tel = ?'],
        ['job', 'name', 'tel'],
        ['sales', 'bill', '345']
    ]


def test_group_updates_to_same_columns():
    assert convert(
        [
            ['@@', 'id', 'name', 'tel'],
            ['->', '1', 'john', '123->456'],
            ['->', '2', 'bill->mark', '135'],
            ['->', '3', 'sam', '567->765'],
            ['->', '4', 'stan->jack', '246'],
        ],
        key='id'
    ) == [
        ['update t set tel = ? where id = ?'],
        ['tel', 'id'],
        ['456', '1'],
        ['765', '3'],
        [],
        ['update t set name = ? where id = ?'],
        ['name', 'id'],
        ['mark', '2'],
        ['jack', '4']
    ]
