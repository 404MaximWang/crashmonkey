"""What a probe means: the bytes it writes, and the states a healthy system
reaches at each persistence point.

Nothing here runs against the deployment. The probe is declarative, so the
byte a write places at an offset is a function of that operation's identity
(its line in the probe) and the offset, and the state a healthy system must be
in after any persistence point follows from the operations that returned
successfully. That is why the crash gate needs one run and not two: the run
that gets killed is itself the prefix the references are derived from, and its
return record is complete up to the cut, because every operation the cut needs
was executed before the cut.

Two lengths matter and they differ:

  size       the size the running client sees - every write, truncate and
             fallocate in the prefix is reflected in it;
  committed  the length committed so far, i.e. the value the last
             length-committing synchronisation (close / fsync / sync) stored.
             A data-only synchronisation (fdatasync) does not move it.

After a crash the recorded length may legally be either of them: the durable
value the last commit stored, or - when the kill itself flushed the client -
the value the client had. Nothing else.
"""


from judge import State


def byte_at(op_line, offset):
    """The byte written by the operation on `op_line` at `offset`.

    Overlapping writes from different operations disagree, which is what lets
    the judge tell a torn mix from either version.
    """
    return (op_line * 131 + offset * 7 + 11) % 251


def write_image(op_line, offset, length):
    return bytes(byte_at(op_line, offset + i) for i in range(length))


class ModelError(Exception):
    """The run's record cannot support the references the judge needs."""


class Tree:
    """The tree as the running client sees it (no durability, no failures)."""

    def __init__(self):
        self.entries = {}

    def _file(self, path):
        e = self.entries.get(path)
        if e is None:
            e = self.entries[path] = {"kind": "f", "nlink": 1, "size": 0,
                                      "content": bytearray()}
        return e

    def _grow(self, e, size):
        if size > len(e["content"]):
            e["content"].extend(b"\x00" * (size - len(e["content"])))

    def size(self, path):
        e = self.entries.get(path)
        return e["size"] if e else 0

    def paths(self):
        return [p for p, e in self.entries.items() if e["kind"] == "f"]

    def apply(self, op):
        """Fold one successful operation into the client-visible tree."""
        name, a = op.name, op.args
        if name == "create_file":
            self.entries[a[0]] = {"kind": "f", "nlink": 1, "size": 0,
                                  "content": bytearray()}
        elif name == "mkdir":
            self.entries[a[0]] = {"kind": "d", "nlink": 1, "size": 0,
                                  "content": bytearray()}
        elif name == "rmdir":
            self.entries.pop(a[0], None)
        elif name == "unlink":
            # unlink always removes the NAME; the inode survives under its
            # other hard links, with the count decremented
            e = self.entries.pop(a[0], None)
            if e is not None:
                e["nlink"] -= 1
        elif name == "rename":
            e = self.entries.pop(a[0], None)
            if e is not None:
                self.entries[a[1]] = e
        elif name == "link":
            e = self.entries.get(a[0])
            if e is not None:
                e["nlink"] += 1
                self.entries[a[1]] = e
        elif name == "symlink":
            self.entries[a[1]] = {"kind": "l", "nlink": 1, "size": 0,
                                  "content": bytearray()}
        elif name == "write_file":
            e = self._file(a[0])
            off, length = int(a[2]), int(a[3])
            img = write_image(op.index, off, length)
            self._grow(e, off + length)
            e["content"][off:off + length] = img
            e["size"] = max(e["size"], off + length)
        elif name == "truncate":
            e = self._file(a[0])
            size = int(a[1])
            del e["content"][size:]
            self._grow(e, size)
            e["size"] = size
        elif name == "fallocate_file":
            e = self._file(a[0])
            size = int(a[1]) + int(a[2])
            self._grow(e, size)
            e["size"] = max(e["size"], size)

    def snapshot(self, committed):
        """Capture the client-visible state, with the committed length."""
        st = State()
        for path, e in self.entries.items():
            st.records[path] = {"kind": e["kind"], "nlink": e["nlink"],
                                "size": e["size"],
                                "committed": committed.get(path, 0),
                                "content": bytes(e["content"])}
        return st


def expectations(probe, record, k):
    """-> (want, prev) for a cut at the k-th persistence point.

    `record[i]` is the return value of the i-th executed operation (@pre then
    @seq). A failed operation changes nothing, so it is skipped - which is
    also why the model never has to predict an error: the run tells it.
    """
    if k < 1:
        raise ModelError("cut ordinal must be >= 1")
    tree, committed = Tree(), {}
    states = {}
    index = 0

    def step(op):
        nonlocal index
        if index >= len(record):
            raise ModelError("the run stopped before the cut: no outcome "
                             "recorded for %r" % (op,))
        ret = record[index]
        index += 1
        if ret == 0:
            tree.apply(op)
        return ret

    for op in probe.pre:
        step(op)
    states[0] = tree.snapshot(committed)

    points = 0
    for op in probe.seq:
        ret = step(op)
        if not op.promised:
            continue
        if ret != 0:
            raise ModelError("synchronisation %r failed in the run (ret=%d): "
                             "nothing was promised at that point"
                             % (op, ret))
        points += 1
        if "length" in op.promised:
            for path in tree.paths() if op.name == "sync" else [op.args[0]]:
                committed[path] = tree.size(path)
        if points >= k:
            states[points] = tree.snapshot(committed)
            break
        states[points] = tree.snapshot(committed)

    if k not in states:
        raise ModelError("the run did not reach persistence point %d" % k)
    return states[k], states.get(k - 1, states[0])
