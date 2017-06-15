import subprocess


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
