"""Subjects that execute a probe.

A subject runs the probe once, kills the workload at the chosen persistence
point, recovers the deployment, and reports the recovered state plus the
return value of every operation it executed. The run *is* the reference
prefix: its record covers every operation up to the cut, so the judge derives
what a healthy system would have had from the same record - no second pass,
no observation before the crash.

MockSubject models the deployment as measured:

  * a write lands in the client's buffer and only a synchronisation pushes it;
  * close and fsync push data *and* the recorded length;
  * fdatasync pushes the data but leaves the recorded length behind, so after
    a crash the file reads back correctly while a stat still reports the old
    length;
  * the kill discards the buffer; bytes never pushed are gone, and that is
    legal.

It can inject a fault at the crash point, which is how the self-check proves
the judge catches violations instead of always agreeing.
"""

import os
import stat
import sys

from judge import State
from model import Tree, write_image


class MockSubject:
    def __init__(self, fault=None):
        self.fault = fault
        self.restore_calls = 0

    # -- one run -----------------------------------------------------------

    def _exec(self, op, client, data, length):
        """Execute one operation; return its return value.

        Failures are the ones the probe can hit on a healthy deployment, so
        that the record carries them and the model can skip them.
        """
        name, a = op.name, op.args
        if name in ("create_file", "mkdir"):
            exists = a[0] in client.entries
            excl = name == "mkdir" or int(a[1], 8) & 0o200
            if exists and excl:
                return 17                       # EEXIST
            client.apply(op)
        elif name in ("close", "fsync"):
            e = client.entries.get(a[0])
            if e is not None and e["kind"] == "f":
                data[a[0]] = bytes(e["content"])
                length[a[0]] = e["size"]
        elif name == "fdatasync":
            e = client.entries.get(a[0])
            if e is not None and e["kind"] == "f":
                data[a[0]] = bytes(e["content"])
        elif name == "sync":
            for path in client.paths():
                data[path] = bytes(client.entries[path]["content"])
                length[path] = client.size(path)
        else:
            client.apply(op)
        return 0

    def run(self, probe, points):
        """Run @pre plus @seq up to persistence point `points`, then crash."""
        client, data, length, record = Tree(), {}, {}, []
        seen = 0
        for op in list(probe.pre) + list(probe.seq):
            record.append(self._exec(op, client, data, length))
            if op.promised:
                seen += 1
                if seen == points:
                    break

        st = State()
        for path, e in client.entries.items():
            st.records[path] = {"kind": e["kind"], "nlink": e["nlink"],
                                "size": length.get(path, 0),
                                "content": data.get(path, b"")}
        self._fault(st)
        return st, record

    def _fault(self, st):
        """Damage the recovered state the way a real corruption would."""
        files = sorted(p for p, r in st.records.items() if r["kind"] == "f")
        if not self.fault or not files:
            return
        r = st.records[files[-1]]
        if self.fault == "torn" and r["content"]:
            r["content"] = bytes([(r["content"][0] + 1) % 256]) + r["content"][1:]
        elif self.fault == "drop":
            r["content"] = b""
        elif self.fault == "length_short":
            r["size"] = 0
        elif self.fault == "length_half":
            r["size"] = len(r["content"]) // 2
        elif self.fault == "phantom":
            r["content"] = r["content"] + b"\xff" * 8

    def restore(self):
        """The real subject restarts what it killed; the mock just counts."""
        self.restore_calls += 1


# ---------------------------------------------------------------------------
# Real subject (attach mode): a live 3FS deployment driven through our own
# FUSE client. Client kill class only - killing the deployment's storage or
# cluster daemons is a separate experiment with its own approval
# (crashqa.md section 5).


class ProbeExecutor:
    """Executes probe ops against a live mount; returns 0 or errno per op.

    fds stay open across ops (spec section 5): close/fsync/fdatasync/sync
    and the kill itself are the only events that change persistence, so an
    executor that closed fds between ops would commit data the probe never
    asked to commit. Write content comes from model.write_image, keyed on
    the op's line number - the same function the gate-side model derives
    references from, so executor and judge always agree on the bytes.
    """

    def __init__(self, root):
        self.root = root
        self.fds = {}

    def _full(self, path):
        full = os.path.join(self.root, path)
        if not (full + os.sep).startswith(self.root + os.sep):
            raise ValueError("path escapes root: %s" % path)
        return full

    def _fd(self, path, flags):
        fd = self.fds.get(path)
        if fd is None:
            fd = os.open(self._full(path), flags)
            self.fds[path] = fd
        return fd

    def exec(self, op):
        """-> 0 on success, errno on failure. A failed op changes nothing;
        the model skips it when deriving references."""
        name, a = op.name, op.args
        try:
            if name == "create_file":
                # a previous fd for the path leaks deliberately: closing it
                # would FLUSH (commit) data the probe did not sync
                self.fds[a[0]] = os.open(self._full(a[0]), int(a[1], 8),
                                         int(a[2], 8))
            elif name == "write_file":
                img = write_image(op.index, int(a[2]), int(a[3]))
                if os.pwrite(self._fd(a[0], int(a[1], 8)), img,
                             int(a[2])) != len(img):
                    raise OSError(5, "short write")              # EIO
            elif name == "truncate":
                os.ftruncate(self._fd(a[0], os.O_RDWR), int(a[1]))
            elif name == "fallocate_file":
                os.posix_fallocate(self._fd(a[0], os.O_RDWR),
                                   int(a[1]), int(a[2]))
            elif name == "unlink":
                # an fd held for the path stays open on purpose (spec §5)
                os.unlink(self._full(a[0]))
            elif name == "mkdir":
                os.mkdir(self._full(a[0]), int(a[1], 8))
            elif name == "rmdir":
                os.rmdir(self._full(a[0]))
            elif name == "rename":
                os.rename(self._full(a[0]), self._full(a[1]))
            elif name == "symlink":
                os.symlink(a[0], self._full(a[1]))
            elif name == "link":
                os.link(self._full(a[0]), self._full(a[1]))
            elif name == "chmod":
                os.chmod(self._full(a[0]), int(a[1], 8))
            elif name == "chown_file":
                os.chown(self._full(a[0]), int(a[1]), -1)
            elif name == "chgrp_file":
                os.chown(self._full(a[0]), -1, int(a[1]))
            elif name == "setxattr":
                os.setxattr(self._full(a[0]), a[1], a[2].encode(),
                            int(a[4], 0))
            elif name == "removexattr":
                os.removexattr(self._full(a[0]), a[1])
            elif name == "close":
                fd = self.fds.pop(a[0], None)
                if fd is None:
                    return 9                                      # EBADF
                os.close(fd)
            elif name == "fsync":
                os.fsync(self._fd(a[0], os.O_RDWR))
            elif name == "fdatasync":
                fd = self._fd(a[0], os.O_RDWR)
                if hasattr(os, "fdatasync"):
                    os.fdatasync(fd)
                else:
                    os.fsync(fd)    # host without fdatasync (local mock only)
            elif name == "sync":
                # every dirty byte this client can hold sits behind an open
                # fd; closed files were already committed by close's FLUSH
                for fd in list(self.fds.values()):
                    os.fsync(fd)
            else:
                raise ValueError("unknown op %r" % name)
            return 0
        except OSError as e:
            return e.errno or 5


def _op_extent(op):
    """Highest byte offset the op addresses; bounds the observation read."""
    a = op.args
    if op.name == "write_file":
        return int(a[2]) + int(a[3])
    if op.name == "truncate":
        # truncate-down: bytes past the new length may still be there, and
        # the no-tearing rule wants them compared against the previous image
        return int(a[1])
    if op.name == "fallocate_file":
        return int(a[1]) + int(a[2])
    return 0


def _read_all(path, cap):
    """Up to `cap` bytes; a short or failed read ends the capture. The
    judge, not the observer, decides whether a shortfall is a violation."""
    if cap <= 0:
        return b""
    chunks, total = [], 0
    fd = os.open(path, os.O_RDONLY)
    try:
        while total < cap:
            try:
                b = os.read(fd, min(1 << 20, cap - total))
            except OSError:
                break
            if not b:
                break
            chunks.append(b)
            total += len(b)
    finally:
        os.close(fd)
    return b"".join(chunks)


def observe(root, cap, skip=(".cm-owned",)):
    """The subtree as a fresh client sees it.

    `cap` bounds how far past the recorded length we read: the mount is
    direct-IO and reads are not clamped by it (crashqa.md section 1), so
    the bytes an fdatasync pinned are readable even when the recorded
    length says 0. `skip` excludes the driver's ownership marker, which the
    model cannot know about."""
    recs = {}
    for dirpath, dirnames, filenames in os.walk(root):
        for name in dirnames + filenames:
            full = os.path.join(dirpath, name)
            rel = os.path.relpath(full, root)
            if rel in skip:
                continue
            st = os.lstat(full)
            if stat.S_ISLNK(st.st_mode):
                kind, size, content = "l", 0, b""
                if name in dirnames:
                    dirnames.remove(name)          # never descend a symlink
            elif stat.S_ISDIR(st.st_mode):
                kind, size, content = "d", 0, b""
            else:
                kind, size = "f", st.st_size
                content = _read_all(full, cap)
            recs[rel] = {"kind": kind, "nlink": st.st_nlink,
                         "size": size, "content": content}
    return State(recs)


class AttachCrashSubject:
    """Runs the probe on a live 3FS deployment through our own FUSE client.

    One run: fresh client and a claimed, emptied subtree (AttachDriver);
    execute @pre then @seq recording every return value; at the k-th
    persistence point SIGKILL the client and lazily unmount (a graceful
    stop would flush the very state under judgement, crashqa.md section 2);
    restart the client; observe through it - a fresh client reads the
    recorded length from FoundationDB and the bytes from storage, which is
    exactly the durable state.

    The executor process (us) is never killed and never closes its probe
    fds: a closing writer would send FUSE FLUSH, a full fsync. The stale
    fds reference the killed client's mount and die with the process.
    """

    def __init__(self, cfg):
        # lazy import: mock-only runs never load the driver stack
        sys.path.insert(0, os.path.dirname(os.path.dirname(
            os.path.abspath(__file__))))
        import driver as D
        cfg = dict(cfg)
        cfg["subdir"] = cfg.get("crash_subdir", "work/cmcrash")
        self._driver = D.AttachDriver(cfg)
        self._kill = D.KILL_CLIENT

    def run(self, probe, points):
        d = self._driver
        d.init_cluster()
        root = os.path.join(d.mount, d.testdir)
        executor = ProbeExecutor(root)
        record = []
        extent = 0
        seen = 0
        for op in list(probe.pre) + list(probe.seq):
            ret = executor.exec(op)
            record.append(ret)
            if ret == 0:
                extent = max(extent, _op_extent(op))
            if op.promised:
                seen += 1
                if seen == points:
                    break
        d.kill(self._kill)
        d.recover(self._kill)
        return observe(root, extent), record

    def restore(self):
        """Leave the deployment as found: stop our client and remove our
        subtree (AttachDriver removes it only if its marker says it is
        ours)."""
        self._driver.stop_all()
