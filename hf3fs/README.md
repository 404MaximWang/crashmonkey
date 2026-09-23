# hf3fs: CrashMonkey-style crash-consistency testing for 3FS

Port of the B3 approach (bounded black-box crash testing, OSDI'18, Mohan et
al.) from block-device replay to process-kill orchestration.

This file documents how to use the harness. The 3FS semantics it depends on -
persistence points, the judgement rules and what has been verified so far -
are in `~/fm/baseline/plans_bak/crashqa.md`; code-level details are in the
module docstrings of `driver.py` and the modules under `crash/`.

## Layout

- `driver.py` - cluster lifecycle and kill classes. `ThreeFSDriver` brings
  up a private single-machine cluster (shelved: the deployment host has no
  disk headroom for one), `AttachDriver` drives a deployment that is
  already running.
- `crash/` - the crash gate: declarative probes (`probe.py`), a model of what
  a healthy system must show at each persistence point (`model.py`),
  pluggable subjects (`subject.py`: in-process mock, or attach = a live
  deployment through our own FUSE client) and the judge (`judge.py`), driven
  by `gate.py`. One run per probe, no oracle pass: the subject records every
  op result up to the cut and the references are derived from that prefix.
- `examples/` - config templates: `3fs_config.json` for the private-cluster
  driver (shelved with it, see the banner in `driver.py`),
  `attach_config.json` for attach mode.
- `scripts/` - probes and regression suites; each script is listed with its
  purpose in the script index of `crashqa.md`.

## Private cluster (ThreeFSDriver, shelved)

`ThreeFSDriver` is shelved - all experiments run in attach mode - but its
bring-up notes stay here as its reference:

1. Build the 3FS binaries; give `fdbserver` / `fdbcli` paths in the config.
2. Write a config (see `examples/3fs_config.json`). Required: `binary_dir`,
   `test_dir`, `reference_etc` (a proven single-machine deployment's etc
   directory, read read-only as the template), `num_storage`, `rf`,
   `native_rdma`, `cluster_id`, `fdbserver`, `fdbcli`.

### Ports, paths and node ids are derived, never copied

The driver reads the reference deployment's own mgmtd address, FDB address,
storage process count and per-process disk count, then shifts every port by
`port_offset` (default 4000) and assigns its own node ids. A deployment's root
is the parent of `reference_etc`, so `reference_etc=/path/deploy/etc` rewrites
every absolute path in the templates to `test_dir`. Overrides: `reference_root`,
`mgmtd_port`, `fdb_port`, `num_disks`, `mgmtd_node_id`, `meta_node_id`,
`storage_node_id`, `fdb_cluster_name`, `fuse_log_level`.

Two gates run before anything starts. `check_configs()` asserts that no patched
file mentions the reference root, that every address points at one of our
ports, and that all of our ports are free. `check_disk_space()` prints a space
plan derived from the target layout and refuses to start if the `test_dir`
filesystem cannot hold the instance - keep `test_dir` on a filesystem with
room, and note that FoundationDB marks its database unavailable when free space
on that disk falls below 5%, which takes the deployments sharing the disk down
with it.

### attach mode: drive a deployment that is already running

`driver=attach` uses that deployment's own launcher and binary and starts only
our own FUSE client. The storage and cluster kill classes restart what they
kill from the deployment's own pinned configs; each was approved as its own
experiment (crashqa.md section 5; plan6 storage, plan7 cluster) and their
recovery is exercised after every kill. The workload runs
in `subdir` (default `work/cmharness`), a subtree the driver claims with a
marker file; it refuses to empty any non-empty directory that lacks that
marker, so it cannot be pointed at shared space.

### crash gate

```sh
python3 crash/gate.py --selfcheck                # protocol self-test, no 3FS
python3 crash/gate.py --probe crash/examples/basic-fsync.seq --subject mock
python3 crash/gate.py --probe P --subject attach --config attach.json
```

Exit codes: 0 = crash_state_ok, 42 = crash_state_violation, 3 = infra
failure, 97 = the shared gate lock (`GATE_LOCK`, default
`/tmp/.gate-3fs.lock`) timed out. The baseline repo drives this from the
experiment server: `verifier/gate-crash.sh` streams the probe over ssh, holds
the same lock on its own side, and maps these codes into run verdicts. All
three kill classes (client, storage, cluster) are available in attach mode.
  The gate's attach subject claims `crash_subdir` (default
  `work/cmcrash`), a subtree of its own.

### Bring-up notes

- Use the FDB binaries that ship with the reference deployment
  (`fdb/usr/sbin`, `fdb/usr/bin`): 3FS is built against a specific FDB API
  version, and a client library of another major version aborts the process
  while it constructs `FDBContext`. `configure new single ssd` gives a private
  disk-backed database that survives the cluster kill class.
- Start every service with `--cfg <toml>` so it pins its own service config.
  Without it a process pulls the type-level config from mgmtd instead, which
  is one node's values applied to all of them, and processes then collide on
  ports and paths. Restarting a single node needs the same treatment: give it
  `--cfg <deploy>/log/node-<id>.toml`.
- The chain-table CSV header carries one column per replica
  (`ChainId,TargetId,TargetId,...`), one chain per row.
- Memory limits and thread counts are shrunk by default (`shrink_caches`)
  because the harness cluster shares the machine with the reference one; RDMA
  buffer pools are shrunk only on the rxe path. The storage data plane must be
  RDMA (service groups have to be RDMA-typed) - `native_rdma: true` is for real
  hardware, and without an RDMA device the data path fails.
