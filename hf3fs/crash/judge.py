"""Judging a crash pass against the promises the probe made.

The references come from model.expectations: the state a healthy system has at
the cut (`want`) and at the previous persistence point (`prev`). Nothing here
observes the deployment.

  metadata   the recovered tree must be the tree the prefix built - the
             metadata operations before the cut are committed transactions.
  content    every byte of the client-visible image at the cut must read back
             identical, whether the cut's synchronisation promised the length
             (close / fsync / sync) or only the data (fdatasync).
  length     at a length-committing cut the recorded length must be exactly
             the size the client had; at a data-only cut it must be one of the
             two values that can legally survive - the length the last commit
             stored, or the size the client had when the kill flushed it.
             A third value means a partial length commit.
  no tearing a byte beyond the client-visible image must equal what was there
             before the cut or be absent; anything else is data nobody wrote.
"""


class State:
    """A captured state of the probe tree.

    `records[path]` has `kind`, `nlink`, `content`, and the length the
    situation reports as `size`; references additionally carry `committed`,
    the length the last length-committing synchronisation stored.
    """

    def __init__(self, records=None):
        self.records = records or {}

    def copy(self):
        return State({p: dict(r) for p, r in self.records.items()})

    def files(self):
        return {p: r for p, r in self.records.items() if r["kind"] == "f"}


def judge(want, prev, crash, length_promise):
    """-> list of violation strings (empty means the crash state is legal)."""
    violations = []

    # metadata: same tree, same types, same link counts
    for path in sorted(set(want.records) | set(crash.records)):
        w, c = want.records.get(path), crash.records.get(path)
        if w is None:
            violations.append("entry only in crash: %s" % path)
            continue
        if c is None:
            violations.append("entry missing after crash: %s" % path)
            continue
        if w["kind"] != c["kind"]:
            violations.append("type changed for %s: %s -> %s"
                              % (path, w["kind"], c["kind"]))
        # nlink is compared for files only: a directory's link count is a
        # function of its subtree (2 + subdirectories on POSIX; unmeasured
        # on 3FS), and the entry/type comparison above already pins the tree
        if w["kind"] == "f" and w["nlink"] != c["nlink"]:
            violations.append("nlink changed for %s: %d -> %d"
                              % (path, w["nlink"], c["nlink"]))

    for path, w in sorted(want.files().items()):
        c = crash.records.get(path)
        if c is None or c["kind"] != "f":
            continue                     # already reported above
        wc, cc = w["content"], c["content"]

        # every promised byte must read back identical
        if len(cc) < len(wc):
            violations.append("promised bytes not retrievable in %s: %d of %d"
                              % (path, len(cc), len(wc)))
        elif cc[:len(wc)] != wc:
            off = next(i for i in range(len(wc)) if cc[i] != wc[i])
            violations.append("content differs in %s at offset %d" % (path, off))

        if length_promise:
            if c["size"] != w["size"]:
                violations.append("recorded length is %d in %s, the "
                                  "synchronisation committed %d"
                                  % (c["size"], path, w["size"]))
        elif c["size"] not in (w["committed"], w["size"]):
            violations.append("recorded length is %d in %s, neither the "
                              "committed %d nor the client's %d"
                              % (c["size"], path, w["committed"], w["size"]))

        # nothing else may be readable that was never written
        pc = prev.records.get(path, {}).get("content", b"")
        for i in range(len(wc), len(cc)):
            old = pc[i] if i < len(pc) else None
            if old is None or cc[i] != old:
                violations.append("byte at %s:%d is neither the new nor the "
                                  "previous value" % (path, i))
                break
    return violations
