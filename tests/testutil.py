import subprocess
import csv
from io import StringIO


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


def adosql(
        db,
        sql,
        input_rows=None,
        typed_header=False,
        delimiter=None,
        autocommit=False
    ):
    """Execute sql with adosql and return rows from output csv if any.

    sql is a query to execute.

    If passed, input_rows must a be a list of rows of input values for
    parameterized query. This list is converted to csv and piped to
    adosql right after sql. If row of values is a dict, the parameter
    style is named and no header row is needed. Otherwise it's
    positiotal (question mark) style and first row must be a header.

    sql may be a list of queries. In this case if not None, input_rows
    must be a list of lists of rows one for each parameterized query in
    sql. Parameter styles must be the same for all queries in sql.

    Returned rows are a list of lists including header. If typed_header
    is True, adosql will return typed header: each column will contain
    its type delimited from name by space.

    If not None, use delimiter as CSV delimiter.

    If autocommit is True, adosql executes in autocommit mode.
    """
    csvargs = {'delimiter': delimiter} if delimiter else {}
    delimiter_arg = ['-t'] if delimiter == '\t' else []

    # if sql is a string, make it and input_rows a one-item list
    if hasattr(sql, 'upper'):
        sql = [sql]
        input_rows = [input_rows]
    # if sql is already a list, but input_rows is None, make
    # input_rows a corresponding list of Nones
    else:
        input_rows = input_rows or [None] * len(sql)

    paramstyle_arg = []
    input_chunks = []
    for query, rows in zip(sql, input_rows):
        input_chunk = query + '\n'
        if rows:
            paramstyle_arg = ['-paramstyle']
            f = StringIO()
            row1 = rows[0]
            try:
                colnames = row1.keys()
            except AttributeError:
                paramstyle_arg += ['qmark']
                writer = csv.writer(f, **csvargs)
            else:
                paramstyle_arg += ['named']
                writer = csv.DictWriter(f, colnames, **csvargs)
                writer.writeheader()
            writer.writerows(rows)
            input_chunk += f.getvalue()
            
        input_chunks.append(input_chunk)

    # add empty line separators between parameterized queries
    sep = '\n' if paramstyle_arg else ''
    input = sep.join(input_chunks)

    cmd = (
        ['adosql'] +
        paramstyle_arg +
        (['-typed-header'] if typed_header else []) +
        delimiter_arg +
        (['-autocommit'] if autocommit else []) +
        ['vfp', db]
    )

    out, err = run(cmd, input)
    return list(csv.reader(StringIO(out), **csvargs))
