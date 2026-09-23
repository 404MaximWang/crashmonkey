"""The crash gate: route a probe, run it once up to the cut, judge, report.

    gate.py --probe P --kill client --subject mock     # no 3FS involved
    gate.py --selfcheck                               # protocol self-test

The subject is pluggable. `--subject mock` uses the in-process model in
subject.py, which reproduces the measured semantics and can inject faults;
`--subject attach --config cfg.json` runs against a live 3FS deployment
through our own FUSE client, holding the shared gate lock (spec section 9)
and restoring the deployment on every exit path, signals included.
"""

import argparse
import json
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import probe as P
from judge import judge
from model import expectations, ModelError
from subject import MockSubject


class InfraFailure(Exception):
    """The run could not be judged (bad probe, subject error)."""


def absolute_ordinal(probe, position):
    """The k-th persistence point of @seq, counted over all of them."""
    k = 0
    for i, op in enumerate(probe.seq):
        if op.promised:
            k += 1
        if i + 1 == position:
            return k
    raise InfraFailure("position %d is not a persistence point" % position)


def run_probe(probe, subject):
    pos, name = probe.cut
    op = probe.seq[pos - 1]
    k = absolute_ordinal(probe, pos)

    crash, record = subject.run(probe, k)
    try:
        want, prev = expectations(probe, record, k)
    except ModelError as e:
        raise InfraFailure(str(e))
    violations = judge(want, prev, crash, "length" in op.promised)
    return {
        "verdict": "crash_state_violation" if violations else "crash_state_ok",
        "cut_name": name,
        "cut_op": op.name,
        "promise": list(op.promised),
        "violations": violations,
        "record": record,
    }


def selfcheck():                                        # noqa: C901
    """The four assertions the spec requires, plus judge sensitivity."""
    bad = []

    def check(name, cond, detail=""):
        if not cond:
            bad.append("%s: %s" % (name, detail))

    # 1. routing
    crash_seq = "# mode: crash\n@seq\nwrite_file, f, 0101, 0, 8\nfsync, f\n@cut\n"
    metis_seq = "# mode: metis\n@seq\nwrite_file, f, 0101, 0, 8\n"
    check("routing crash", P.parse(crash_seq).mode == "crash")
    check("routing metis", P.parse(metis_seq).mode == "metis")
    for text, label in ((crash_seq.split("\n", 1)[1], "missing"),
                        ("bogus\n@seq\nfsync, f\n", "unknown")):
        try:
            P.parse(text)
            bad.append("routing %s: accepted" % label)
        except P.ProbeError as e:
            check("routing %s message" % label, "mode line" in str(e), str(e))

    # 2. the one crash point
    p = P.parse(crash_seq)
    check("cut", p.cut == (2, "cut"), str(p.cut))
    marked = P.parse("# mode: crash\n@seq\nwrite_file, f, 0101, 0, 8\nfsync, f\n"
                     "@cut one\nwrite_file, f, 0101, 8, 8\nfsync, f\n")
    check("named cut", marked.cut == (2, "one"), str(marked.cut))
    for text, label in (
            ("# mode: crash\n@seq\nwrite_file, f, 0101, 0, 8\n@cut bad\n",
             "cut after a non-sync op"),
            ("# mode: crash\n@seq\nwrite_file, f, 0101, 0, 8\nfsync, f\n"
             "@cut one\nwrite_file, f, 0101, 8, 8\nfsync, f\n@cut two\n",
             "a second @cut"),
            ("# mode: crash\n@seq\nwrite_file, f, 0101, 0, 8\nfsync, f\n",
             "no @cut")):
        try:
            P.parse(text)
            bad.append("%s: accepted" % label)
        except P.ProbeError:
            pass

    # a directory created in @pre is a legitimate parent for a @seq creation
    try:
        P.parse("# mode: crash\n@pre\nmkdir, d, 0755\n@seq\n"
                "create_file, d/f, 0101, 0644\nwrite_file, d/f, 01, 0, 8\n"
                "fsync, d/f\n@cut\n")
    except P.ProbeError as e:
        bad.append("parent created in @pre rejected: %s" % e)

    # 3. promise -> judgement
    fsync_probe = P.parse("# mode: crash\n@pre\ncreate_file, f, 0101, 0644\n@seq\n"
                          "write_file, f, 0101, 0, 16\nfsync, f\n@cut\n")
    ok = run_probe(fsync_probe, MockSubject())
    check("fsync clean", ok["verdict"] == "crash_state_ok", str(ok))
    check("fsync promise", ok["promise"] == ["data", "length"], str(ok))
    for fault in ("torn", "drop", "length_short", "length_half", "phantom"):
        r = run_probe(fsync_probe, MockSubject(fault=fault))
        check("fsync caught %s" % fault, r["verdict"] == "crash_state_violation",
              str(r))

    fdatasync_probe = P.parse(
        "# mode: crash\n@pre\ncreate_file, f, 0101, 0644\n@seq\n"
        "write_file, f, 0101, 0, 16\nfdatasync, f\n@cut\n")
    r = run_probe(fdatasync_probe, MockSubject())
    check("fdatasync stale length ok", r["verdict"] == "crash_state_ok", str(r))
    check("fdatasync promise", r["promise"] == ["data"], str(r))
    r = run_probe(fdatasync_probe, MockSubject(fault="drop"))
    check("fdatasync data still promised",
          r["verdict"] == "crash_state_violation", str(r))
    r = run_probe(fdatasync_probe, MockSubject(fault="length_short"))
    check("fdatasync old length legal", r["verdict"] == "crash_state_ok",
          str(r))
    r = run_probe(fdatasync_probe, MockSubject(fault="length_half"))
    check("fdatasync partial length caught",
          r["verdict"] == "crash_state_violation", str(r))

    # a committed truncate must survive, in both directions
    trunc = P.parse("# mode: crash\n@pre\ncreate_file, f, 0101, 0644\n@seq\n"
                    "write_file, f, 0101, 0, 100\nfsync, f\n"
                    "truncate, f, 10\nfsync, f\n@cut t\n")
    r = run_probe(trunc, MockSubject())
    check("truncate clean", r["verdict"] == "crash_state_ok", str(r))
    r = run_probe(trunc, MockSubject(fault="length_short"))
    check("truncate caught length_short",
          r["verdict"] == "crash_state_violation", str(r))

    # an operation that failed changes nothing, and the reference must not
    # require it
    excl = P.parse("# mode: crash\n@pre\ncreate_file, f, 0101, 0644\n@seq\n"
                   "create_file, f, 0201, 0644\nfsync, f\n@cut\n")
    s = MockSubject()
    crash, record = s.run(excl, 1)
    check("failed op recorded", 17 in record, str(record))
    want, prev = expectations(excl, record, 1)
    check("failed op leaves no trace",
          judge(want, prev, crash, True) == [], str(judge(want, prev, crash, True)))
    try:
        expectations(excl, [0, 0, 5], 1)
        bad.append("failed synchronisation accepted")
    except ModelError:
        pass
    try:
        expectations(excl, [0], 1)
        bad.append("truncated record accepted")
    except ModelError:
        pass

    # 4. the restore path runs on the normal path and on failures
    s = MockSubject()
    gate_run(fsync_probe, s)
    check("restore called", s.restore_calls == 1, str(s.restore_calls))

    class Broken(MockSubject):
        def run(self, probe, points):
            raise RuntimeError("subject died")

    s = Broken()
    try:
        gate_run(fsync_probe, s)
    except RuntimeError:
        pass
    check("restore called on failure", s.restore_calls == 1, str(s.restore_calls))

    if bad:
        print("SELFCHECK FAILED")
        for b in bad:
            print("  " + b)
        return 1
    print("selfcheck: routing, cuts, promises, judge sensitivity and restore "
          "path all behave as specified")
    return 0


def _take_gate_lock():
    """Serialize with every other gate that drives this deployment (spec
    section 9): the same flock file as the differential 3FS gate, a bounded
    wait, and exit 97 on timeout - gate-3fs.sh's convention."""
    import fcntl                          # Linux-only, like live subjects
    path = os.environ.get("GATE_LOCK", "/tmp/.gate-3fs.lock")
    wait = int(os.environ.get("GATE_LOCK_WAIT", "7200"))
    fd = open(path, "w")
    deadline = time.time() + wait
    while True:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return fd                     # held until process exit
        except OSError:
            if time.time() >= deadline:
                print("gate lock wait timed out", file=sys.stderr)
                sys.exit(97)
            time.sleep(1)


def _arm_restore_traps(subject):
    """Spec section 9: restoration must cover interruption, not only the
    normal return path."""
    def _trap(signum, _frame):
        try:
            subject.restore()
        except Exception as e:
            print("restore failed on signal %d: %s" % (signum, e),
                  file=sys.stderr)
        sys.exit(3)
    signal.signal(signal.SIGINT, _trap)
    signal.signal(signal.SIGTERM, _trap)


def gate_run(probe, subject):
    """run_probe wrapped in the mandatory restore step."""
    try:
        return run_probe(probe, subject)
    finally:
        subject.restore()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--probe", help="probe file, or - for stdin")
    ap.add_argument("--kill", default="client",
                    choices=("client", "storage", "cluster"))
    ap.add_argument("--subject", choices=("mock", "attach"),
                    help="mock = in-process model; attach = live deployment "
                         "through our own FUSE client (needs --config)")
    ap.add_argument("--config", help="attach-mode deployment config (json)")
    ap.add_argument("--selfcheck", action="store_true")
    args = ap.parse_args()

    if args.selfcheck:
        return selfcheck()
    if not args.probe:
        raise SystemExit("--probe or --selfcheck is required")
    if not args.subject:
        raise SystemExit("--subject mock|attach is required: no live run "
                         "without an explicit subject")

    if args.probe == "-":
        probe = P.parse(sys.stdin.read())
    else:
        probe = P.parse_file(args.probe)
    if probe.mode != P.CRASH:
        raise SystemExit("this is the crash gate; probe mode is %r"
                         % probe.mode)

    live = args.subject == "attach"
    if live:
        if not args.config:
            raise SystemExit("--subject attach needs --config CFG.json")
        from subject import AttachCrashSubject
        with open(args.config) as f:
            subject = AttachCrashSubject(json.load(f), args.kill)
        _take_gate_lock()
        _arm_restore_traps(subject)
    else:
        subject = MockSubject()

    try:
        result = gate_run(probe, subject)
    except InfraFailure as e:
        detail = str(e)
    except Exception as e:
        # a live run that broke in any other way (driver error, restore
        # failure, unreadable mount) could not be judged - never a verdict
        detail = "%s: %s" % (type(e).__name__, e)
    else:
        result["kill"] = args.kill
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["verdict"] == "crash_state_ok" else 42
    print(json.dumps({"verdict": "INFRA_FAILURE", "detail": detail,
                      "kill": args.kill, "probe": args.probe},
                     indent=2, sort_keys=True))
    return 3


if __name__ == "__main__":
    sys.exit(main())
