"""crash probe: mode routing, parsing and the persistence-promise table.

A probe file's first line selects the verification path: the comment
`# mode: crash` for the crash-consistency gate, `# mode: metis` for the
differential gate. Anything else is rejected - there is no fallback mode.

The rules implemented here are the ones in `plan/crash-probe-spec.md`:
field syntax and limits, the `@pre`/`@seq` sections, the `@cut` marker, and
the promise each synchronisation operation makes.
"""

import os

CRASH = "crash"
METIS = "metis"
MODE_LINES = {"# mode: crash": CRASH, "# mode: metis": METIS}

MAX_IO = 16 * 1024 * 1024          # single write / truncate / fallocate
MAX_XATTR = 4096
MAX_OPS = 4096

# What each synchronisation operation promises the crash-consistency judge.
# A promise is what the gate may require after recovery: `data` means the
# bytes are retrievable and identical, `length` means the recorded length must
# be exactly the length the client had at that point. fdatasync promises data
# only.
PROMISES = {
    "close": ("data", "length"),
    "fsync": ("data", "length"),
    "fdatasync": ("data",),
    "sync": ("data", "length"),
}

METADATA_OPS = ("create_file", "mkdir", "rmdir", "unlink", "rename", "symlink",
                "link", "chmod", "chown_file", "chgrp_file", "setxattr",
                "removexattr")
DATA_OPS = ("write_file", "truncate", "fallocate_file")
SYNC_OPS = tuple(PROMISES)

OPS = METADATA_OPS + DATA_OPS + SYNC_OPS
ARITY = {
    "create_file": 4, "write_file": 5, "truncate": 3, "unlink": 2,
    "mkdir": 3, "rmdir": 2, "rename": 3, "symlink": 3, "link": 3,
    "chmod": 3, "chown_file": 3, "chgrp_file": 3, "setxattr": 6,
    "removexattr": 3, "fallocate_file": 4,
    "close": 2, "fsync": 2, "fdatasync": 2, "sync": 1,
}


class ProbeError(Exception):
    """The probe cannot be executed as written."""


class Op:
    __slots__ = ("name", "args", "index")

    def __init__(self, name, args, index):
        self.name = name
        self.args = args
        self.index = index

    def __repr__(self):
        return "%s(%s)" % (self.name, ", ".join(self.args))

    @property
    def promised(self):
        """The persistence promise this op makes, if any."""
        return PROMISES.get(self.name)


class Probe:
    def __init__(self, mode, pre, seq, cut):
        self.mode = mode
        self.pre = pre
        self.seq = seq
        # the one crash point as (position in seq, name); None only for a
        # metis probe, which never reaches the crash gate
        self.cut = cut


def _octal(text, what, lineno):
    try:
        return int(text, 8)
    except ValueError:
        raise ProbeError("line %d: %s is not octal: %r" % (lineno, what, text))


def _decimal(text, what, lineno):
    try:
        return int(text)
    except ValueError:
        raise ProbeError("line %d: %s is not decimal: %r" % (lineno, what, text))


def _check_path(path, lineno):
    if not path or path.startswith("/") or path in (".", ".."):
        raise ProbeError("line %d: not an FS-relative path: %r" % (lineno, path))
    for part in path.split("/"):
        if part in ("", ".", ".."):
            raise ProbeError("line %d: bad path component in %r"
                             % (lineno, path))


def parse_op(line, lineno):
    fields = [f.strip() for f in line.split(",")]
    name = fields[0]
    if name not in ARITY:
        raise ProbeError("line %d: unknown op %r" % (lineno, name))
    if len(fields) != ARITY[name]:
        raise ProbeError("line %d: %s takes %d fields, got %d"
                         % (lineno, name, ARITY[name], len(fields)))
    args = fields[1:]

    if name in ("create_file", "write_file"):
        flags = args[1]
        if flags.startswith("0"):
            _octal(flags, "flags", lineno)      # octal per the spec
    if name in ("mkdir", "chmod"):
        _octal(args[1], "mode", lineno)
    if name == "create_file":
        _octal(args[2], "mode", lineno)
    if name == "write_file":
        off, length = (_decimal(args[2], "offset", lineno),
                       _decimal(args[3], "length", lineno))
        if length > MAX_IO:
            raise ProbeError("line %d: write of %d exceeds %d"
                             % (lineno, length, MAX_IO))
        if off < 0 or length <= 0:
            raise ProbeError("line %d: bad write range" % lineno)
    if name in ("truncate", "fallocate_file"):
        nums = args[1:] if name == "truncate" else args[1:]
        for text in nums:
            _decimal(text, "size", lineno)
    if name == "setxattr":
        size = _decimal(args[3], "size", lineno)
        if size > MAX_XATTR:
            raise ProbeError("line %d: xattr of %d exceeds %d"
                             % (lineno, size, MAX_XATTR))

    for arg in args:
        if "/" in arg and name not in ("symlink",):
            _check_path(arg, lineno)
    if name == "create_file":
        _check_path(args[0], lineno)
    return Op(name, args, lineno)


def parse(text):
    """-> Probe, or raise ProbeError."""
    raw = text.splitlines()
    if not raw:
        raise ProbeError("missing mode line")
    mode = MODE_LINES.get(raw[0])
    if mode is None:
        # byte-identical first line, same rule as the runner's router and
        # the agent-side schema checker
        raise ProbeError("missing mode line: first line is %r, expected one of "
                         "%s" % (raw[0], ", ".join(MODE_LINES)))

    pre, seq = [], []
    cut = None
    section = None
    created = set()
    for lineno, line in enumerate(raw[1:], start=2):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped == "@pre":
            section = "pre"
            continue
        if stripped == "@seq":
            section = "seq"
            continue
        if stripped.startswith("@cut"):
            # a cut marker belongs to the sequence, right after a promise
            if section != "seq":
                raise ProbeError("line %d: @cut outside @seq" % lineno)
            if not seq or not seq[-1].promised:
                raise ProbeError("line %d: @cut must follow a synchronisation "
                                 "op" % lineno)
            if cut is not None:
                raise ProbeError("line %d: at most one @cut per probe"
                                 % lineno)
            name = stripped[4:].strip() or "cut"
            cut = (len(seq), name)
            continue
        if section is None:
            raise ProbeError("line %d: op before @pre or @seq" % lineno)
        op = parse_op(stripped, lineno)

        if op.name in ("create_file", "mkdir"):
            # parents before children, in both sections: a directory made in
            # @pre is a legitimate parent for a @seq creation
            parent = os.path.dirname(op.args[0])
            if parent and parent not in created:
                raise ProbeError("line %d: parent %r must be created first"
                                 % (lineno, parent))
            created.add(op.args[0])
        (pre if section == "pre" else seq).append(op)

    if not seq:
        raise ProbeError("empty @seq section")
    if mode == CRASH and cut is None:
        raise ProbeError("crash probe has no @cut: mark the one crash point "
                         "with '@cut <name>' after the synchronisation op")
    if len(pre) + len(seq) > MAX_OPS:
        raise ProbeError("probe has %d ops, limit is %d"
                         % (len(pre) + len(seq), MAX_OPS))
    return Probe(mode, pre, seq, cut)


def parse_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return parse(f.read())
