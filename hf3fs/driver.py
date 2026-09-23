"""Cluster drivers for the 3FS crash-consistency harness.

Three kill classes are supported everywhere:
  client   - kill the FUSE client (simulates client machine power loss)
  storage  - kill one storage_main process (simulates one storage node down)
  cluster  - kill everything including our own fdbserver (full power loss)

Recovery restarts whatever was killed and then polls probe IO on the
mount with retries until it succeeds (reads may return errors while the
cluster is recovering; retry rather than propagate).

ThreeFSDriver brings up a private single-machine cluster whose configs are
copied from a proven reference deployment (cfg["reference_etc"], read-only)
and patched for isolation: own ports, own dirs, own cluster_id, own
FoundationDB instance (disk-backed, so the cluster kill class is meaningful),
periodic_sync disabled (a background length sync would erase the
fdatasync semantics under test), shrunken memory limits and thread counts
so the harness cluster fits alongside the reference cluster (see
shrink_caches), and server.forward_client.force_use_tcp=true so chains with
replication factor > 1 can forward on soft-RoCE (whose in-chain RDMA
read forwarding is unreliable).
"""

import json
import os
import re
import resource
import shutil
import signal
import socket
import subprocess
import time

KILL_CLIENT = "client"
KILL_STORAGE = "storage"
KILL_CLUSTER = "cluster"
KILL_CLASSES = (KILL_CLIENT, KILL_STORAGE, KILL_CLUSTER)

# Directory inside the mount the workload operates on; also the 3FS
# namespace root created at bring-up (see _upload_chains).
TESTDIR = "test"


class DriverError(Exception):
    pass


def _setup_rlimits():
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (1000000, 1000000))
    except (ValueError, OSError):
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))


class BaseDriver:
    """Interface + shared helpers."""

    mount = None
    # Directory inside the mount the workload operates on. Overridable per
    # driver (the attach driver runs inside a deployment whose scratch
    # layout it does not own).
    testdir = TESTDIR

    def init_cluster(self):
        """Wipe data dirs and (re)create the cluster from scratch."""
        raise NotImplementedError

    def stop_all(self):
        raise NotImplementedError

    def wait_ready(self, timeout=180):
        """Poll probe IO on the mount until it succeeds (read errors
        error codes are retried, not propagated)."""
        deadline = time.time() + timeout
        probe_dir = os.path.join(self.mount, self.testdir)
        probe = os.path.join(probe_dir, ".cm_probe")
        while time.time() < deadline:
            try:
                os.makedirs(probe_dir, exist_ok=True)
                with open(probe, "w") as f:
                    f.write("probe")
                with open(probe) as f:
                    f.read()
                os.unlink(probe)
                return
            except OSError:
                time.sleep(1)
        raise DriverError("mount not ready within %ds" % timeout)

    def kill(self, kill_class):
        if kill_class == KILL_CLIENT:
            self._kill_client()
        elif kill_class == KILL_STORAGE:
            self._kill_storage()
        elif kill_class == KILL_CLUSTER:
            self._kill_cluster()
        else:
            raise DriverError("unknown kill class %s" % kill_class)

    def recover(self, kill_class):
        if kill_class == KILL_CLIENT:
            self._recover_client()
        elif kill_class == KILL_STORAGE:
            self._recover_storage()
        elif kill_class == KILL_CLUSTER:
            self._recover_cluster()
        self.wait_ready()

    def _kill_client(self):
        raise NotImplementedError

    def _kill_storage(self):
        raise NotImplementedError

    def _kill_cluster(self):
        raise NotImplementedError

    def _recover_client(self):
        raise NotImplementedError

    def _recover_storage(self):
        raise NotImplementedError

    def _recover_cluster(self):
        raise NotImplementedError


# ---------------------------------------------------------------------------
# DEPRECATED (2026-09-23), shelved: no experiment runs this driver. Every
# experiment goes through AttachDriver on the existing deployment
# (supervisor decision 2026-09-21: the host lacks the disk headroom for a
# second cluster, and a private FoundationDB on the same physical disk can
# take the reference deployment down with it via FDB's 5% free-space
# threshold). The code stays because it is the only place that knows how to
# derive an isolated deployment from a proven reference one, and
# scripts/driver_patch_test.py still guards that derivation. Do not
# instantiate without an explicit un-shelving decision.

class ThreeFSDriver(BaseDriver):
    """DEPRECATED, shelved - see the banner above the class.

    Brings up a private single-machine cluster derived from a proven
    reference deployment. Never verified past preflight: the real bring-up
    (FDB/mgmtd/meta/storage/FUSE) has never been run.
    """

    # Our own node ids; the reference deployment keeps its own (mgmtd 1,
    # meta 100, storage 10001+), which live in a different mgmtd anyway.
    NODE_BASE = {"mgmtd": 501, "meta": 550, "storage": 10501}
    MGMTD_ADDR_RE = re.compile(r"TCP://127\.0\.0\.1:(\d+)")
    LISTEN_PORT_RE = re.compile(r"(listen_port = )(\d+)")
    LOOPBACK_PORT_RE = re.compile(r"127\.0\.0\.1:(\d+)")
    CLUSTER_ID_RE = re.compile(r"cluster_id = '([^']*)'")
    STORAGE_MAIN_RE = re.compile(r"storage(\d+)_main\.toml$")
    TARGET_PATHS_RE = re.compile(r"target_paths = \[(.*?)\]", re.DOTALL)

    def __init__(self, cfg_path):
        with open(cfg_path) as f:
            self.cfg = json.load(f)
        _setup_rlimits()
        self.base = self.cfg["test_dir"].rstrip("/")
        self.mount = os.path.join(self.base, "mnt")
        self.binary = self.cfg["binary_dir"].rstrip("/")
        self.lib_dirs = self.cfg.get("lib_dirs") or [
            self.cfg.get("lib_dir", os.path.join(self.base, "lib"))]
        self.reference_etc = os.path.abspath(
            self.cfg["reference_etc"].rstrip("/"))
        # A deployment's root is the parent of its etc dir (<deploy>/etc ->
        # <deploy>) and every absolute path in its tomls lives under it, so
        # substituting our own base for that root is what keeps the patched
        # files away from the reference deployment's directories. Override
        # with "reference_root" if a deploy keeps etc somewhere else.
        self.reference_root = os.path.abspath(
            self.cfg.get("reference_root") or
            os.path.dirname(self.reference_etc))
        # native_rdma=true targets a real IB/RoCE fabric: skip every rxe
        # soft-RoCE workaround (TCP forcing, buffer pool shrinking)
        self.native_rdma = self.cfg.get("native_rdma", False)
        ref = self._scan_reference()
        # One offset moves the reference's whole port block clear of the
        # cluster that keeps running, preserving the per-process uniqueness
        # the reference relies on. Never copied from the reference: ours.
        self.port_offset = int(self.cfg.get("port_offset", 4000))
        self.mgmtd_port = int(self.cfg.get("mgmtd_port") or
                              ref["mgmtd"] + self.port_offset)
        self.fdb_port = int(self.cfg.get("fdb_port") or
                            ref["fdb"] + self.port_offset)
        self.num_storage = min(int(self.cfg.get("num_storage", 3)),
                               ref["storage"])
        self.rf = min(int(self.cfg.get("rf", self.num_storage)),
                      self.num_storage)
        self.num_disks = int(self.cfg.get("num_disks", ref["disks"]))
        # Chunk size classes the storage targets preallocate: every class in
        # this list costs chunk_size * 1024 bytes per target (measured on the
        # reference deployment: 512KB -> 512M, 4MB -> 4G per target disk).
        # Default keeps only the class the workloads use - the file layout
        # chunk size the cluster is initialised with (512KB).
        self.chunk_sizes = list(self.cfg.get("chunk_size_list") or ["512KB"])
        nb = self.NODE_BASE
        self.node_ids = {
            "mgmtd": int(self.cfg.get("mgmtd_node_id", nb["mgmtd"])),
            "meta": int(self.cfg.get("meta_node_id", nb["meta"])),
            "storage": [int(self.cfg.get("storage_node_id", nb["storage"])) + i
                        for i in range(self.num_storage)]}
        self.cluster_id = self.cfg.get("cluster_id", "cmtest")
        self.data = os.path.join(self.base, "data")
        self.config = os.path.join(self.base, "etc")
        self.log = os.path.join(self.base, "log")
        # the tomls name <deploy>/etc/fdb.cluster, which the root
        # substitution turns into <base>/etc/fdb.cluster: that is where our
        # own cluster file has to live, with our own content
        self.fdb_cluster = os.path.join(self.config, "fdb.cluster")
        self._procs = {}
        self._killed_storage = None
        self.token = self._read_token()

    # -- reference deployment introspection ------------------------------

    def _scan_reference(self):
        """Read the reference deployment's mgmtd port, FDB port, storage
        process count and per-process disk count from its own files."""
        info = {"mgmtd": None, "fdb": None, "storage": 0, "disks": 0}
        for fn in sorted(os.listdir(self.reference_etc)):
            path = os.path.join(self.reference_etc, fn)
            if not fn.endswith(".toml") or not os.path.isfile(path):
                continue
            with open(path) as f:
                text = f.read()
            if self.STORAGE_MAIN_RE.search(fn):
                info["storage"] += 1
                if not info["disks"]:
                    m = self.TARGET_PATHS_RE.search(text)
                    if m:
                        info["disks"] = m.group(1).count('"') // 2
            if info["mgmtd"] is None:
                m = self.MGMTD_ADDR_RE.search(text)
                if m:
                    info["mgmtd"] = int(m.group(1))
        with open(os.path.join(self.reference_etc, "fdb.cluster")) as f:
            m = self.LOOPBACK_PORT_RE.search(f.read())
        if m:
            info["fdb"] = int(m.group(1))
        for key in ("mgmtd", "fdb"):
            if info[key] is None:
                raise DriverError(
                    "cannot read the reference %s port under %s" %
                    (key, self.reference_etc))
        if not info["storage"] or not info["disks"]:
            raise DriverError(
                "no storage*_main.toml with target_paths under %s" %
                self.reference_etc)
        return info

    # -- env / process helpers ------------------------------------------

    def _read_token(self):
        with open(os.path.join(self.reference_etc, "token.txt")) as f:
            return f.read().strip()

    def _env(self):
        env = dict(os.environ)
        # The 3FS binaries are built against a specific FoundationDB client
        # library (FDB_API_VERSION 710, src/fdb/FDB.h:13). A machine usually
        # also has a distribution libfdb_c on the default path, and picking
        # that one up aborts the process while constructing FDBContext, so
        # the library next to the configured fdbserver always comes first -
        # the same thing the reference deployment does through its env.
        fdb_lib = os.path.join(
            os.path.dirname(os.path.dirname(self.cfg.get("fdbserver", ""))),
            "lib")
        libs = list(self.lib_dirs) + [fdb_lib,
            os.path.join(os.path.dirname(self.binary.rstrip("/")),
                         "third_party", "jemalloc", "lib")]
        env["LD_LIBRARY_PATH"] = ":".join(
            [l for l in libs if l and os.path.isdir(l)] +
            [p for p in env.get("LD_LIBRARY_PATH", "").split(":")
             if p]).rstrip(":")
        env["TOKEN"] = self.token
        env["TOKEN_FILE"] = os.path.join(self.config, "token.txt")
        return env

    def _spawn(self, name, argv, stdout_name):
        # start_new_session: a daemon must not die because the terminal the
        # harness was launched from went away (or because the harness
        # process itself exited) - the harness owns their lifetime through
        # stop_all()/reap_own_processes() and nothing else.
        logf = open(os.path.join(self.log, stdout_name), "w")
        self._procs[name] = subprocess.Popen(
            argv, stdout=logf, stderr=subprocess.STDOUT, env=self._env(),
            start_new_session=True)

    def _kill_pid(self, name):
        proc = self._procs.pop(name, None)
        if proc is not None:
            proc.send_signal(signal.SIGKILL)
            proc.wait()

    def _ac(self, *args, check=True):
        """Run admin_cli against OUR mgmtd."""
        r = subprocess.run(
            [os.path.join(self.binary, "admin_cli"),
             "-cfg", os.path.join(self.config, "admin_cli.toml"),
             "--config.mgmtd_client.mgmtd_server_addresses",
             '["TCP://127.0.0.1:%d"]' % self.mgmtd_port,
             "--"] + list(args),
            env=self._env(), capture_output=True, text=True, check=False)
        if check and r.returncode != 0:
            raise DriverError("admin_cli %s failed rc=%d\nstdout:%s\nstderr:%s"
                              % (args[0], r.returncode, r.stdout, r.stderr))
        return r

    def _wait_admin(self, args, needle, timeout, desc):
        deadline = time.time() + timeout
        while time.time() < deadline:
            r = self._ac(*args, check=False)
            if r.returncode == 0 and needle in (r.stdout + r.stderr):
                return
            time.sleep(2)
        raise DriverError("%s: not ready within %ds" % (desc, timeout))

    def _wait_node(self, node, timeout=120):
        deadline = time.time() + timeout
        while time.time() < deadline:
            r = self._ac("list-nodes", check=False)
            if r.returncode == 0:
                for line in r.stdout.splitlines():
                    if line.split()[:1] == [str(node)]:
                        return
            time.sleep(2)
        raise DriverError("node %s not online within %ds" % (node, timeout))

    # -- config generation ----------------------------------------------

    @staticmethod
    def _patch_section(text, section, old, new):
        """Replace `old` with `new` only inside `[section]` ... next `[`."""
        m = re.search(r"(%s.*?)(?=\n\[|\Z)" % re.escape(section), text,
                      re.DOTALL)
        if not m:
            return text
        return text[:m.start()] + m.group(1).replace(old, new) + \
            text[m.end():]

    def _patch_file(self, path):
        with open(path) as f:
            text = f.read()
        # path isolation: everything the reference names under its own root
        # becomes ours (mountpoint, log files, token, target paths)
        text = text.replace(self.reference_root + os.sep, self.base + os.sep)
        text = text.replace("/var/log/3fs", self.log)
        # cluster identity
        text = self.CLUSTER_ID_RE.sub(
            "cluster_id = '%s'" % self.cluster_id, text)
        # ports: shift the reference's whole block by one offset (both the
        # listen ports and the mgmtd addresses that point at them)
        if self.port_offset:
            text = self.LISTEN_PORT_RE.sub(
                lambda m: "%s%d" % (m.group(1),
                                    int(m.group(2)) + self.port_offset), text)
            text = self.LOOPBACK_PORT_RE.sub(
                lambda m: "127.0.0.1:%d" % (int(m.group(1)) +
                                            self.port_offset), text)
        # fdb: point at our private instance
        text = text.replace(self.reference_etc + os.sep + "fdb.cluster",
                            self.fdb_cluster)
        text = text.replace("/etc/foundationdb/fdb.cluster",
                            self.fdb_cluster)
        # storage: force TCP for chain forwarding (rxe soft-RoCE cannot do
        # in-chain RDMA read forwarding on soft-RoCE).
        # Keep [[server.base.groups]] RDMA-typed: the messenger path in
        # StorageClientImpl only uses RDMA endpoints (TCP handshake is served
        # on the same RDMA listener); with --cfg pinning the two procs bind
        # distinct ports, so the earlier ListenFailed (Address already in
        # use) is gone. RDMA buffer pools stay small: rxe is a shared, tiny
        # soft-RoCE device already loaded by the pre-existing cluster.
        if os.path.basename(path).startswith("storage"):
            # chain sync/forwarding uses the forward_client; on rxe
            # soft-RoCE the default RDMA path hangs. The key may sit
            # anywhere in the section body - replace it, or insert it
            # right after the section header if absent. Skipped on native
            # RDMA: forwarding stays on the designed RDMA path.
            if not self.native_rdma:
                for section in ("[server.forward_client]",
                                "[server.client]"):
                    m = re.search(r"(%s\n)(.*?)(?=\n\[|\Z)"
                                  % re.escape(section), text, re.DOTALL)
                    if not m:
                        continue
                    body = m.group(2)
                    if "force_use_tcp" in body:
                        body = body.replace("force_use_tcp = false",
                                            "force_use_tcp = true")
                    else:
                        body = "force_use_tcp = true\n" + body
                    text = text[:m.start(2)] + body + text[m.end(2):]
            # reference storage2_main.toml ships with TCP-typed service
            # groups (a leftover of the previous deploy's experiments); the
            # messenger path in StorageClientImpl requires RDMA endpoints,
            # so pin both procs' groups back to RDMA
            text = re.sub(r"(\[\[server\.base\.groups\]\][^\[]*?"
                          r"network_type = ')[A-Z]+(')",
                          r"\1RDMA\2", text)
            if not self.native_rdma:
                # rxe is a shared, tiny soft-RoCE device already loaded by
                # the pre-existing cluster - keep our RDMA buffer pools small
                text = self._patch_section(
                    text, "[server.buffer_pool]",
                    "big_rdmabuf_count = 8", "big_rdmabuf_count = 1")
                text = self._patch_section(
                    text, "[server.buffer_pool]",
                    "big_rdmabuf_size = '64MB'", "big_rdmabuf_size = '4MB'")
                text = self._patch_section(
                    text, "[server.buffer_pool]",
                    "rdmabuf_count = 256", "rdmabuf_count = 16")
                text = self._patch_section(
                    text, "[server.buffer_pool]",
                    "rdmabuf_size = '4MB'", "rdmabuf_size = '1MB'")
                text = self._patch_section(
                    text, "[server.storage]",
                    "max_concurrent_rdma_reads = 256",
                    "max_concurrent_rdma_reads = 8")
                text = self._patch_section(
                    text, "[server.storage]",
                    "max_concurrent_rdma_writes = 256",
                    "max_concurrent_rdma_writes = 8")
            # one data dir per storage process; the reference config gives
            # every proc two paths (s1/d1 + s2/d1) which collide when two
            # procs run on one machine
            m = self.STORAGE_MAIN_RE.match(os.path.basename(path))
            if m:
                s = m.group(1)
                tdirs = ", ".join('"%s/data/s%s/d%d"' % (self.base, s, d + 1)
                                  for d in range(self.num_disks))
                text = self.TARGET_PATHS_RE.sub(
                    "target_paths = [%s]" % tdirs, text, count=1)
        # shrink memory footprint: a reference cluster and this harness's
        # cluster share the machine, so this applies on every fabric (the
        # RDMA buffer pools above are shrunk separately, only on rxe).
        # Opt out with "shrink_caches": false.
        if self.cfg.get("shrink_caches", True):
            text = text.replace("'8GB'", "'256MB'")
        text = text.replace("max_reserved_chunks = '1GB'",
                            "max_reserved_chunks = '128MB'")
        text = text.replace("num_threads = 32", "num_threads = 8")
        with open(path, "w") as f:
            f.write(text)

    def _patch_fuse_toml(self, path):
        """Disable [periodic_sync] section-scoped."""
        with open(path) as f:
            text = f.read()
        m = re.search(r"(\[periodic_sync\].*?)(enable = true)", text,
                      re.DOTALL)
        if m:
            # only replace the first 'enable = true' after the section head,
            # which belongs to periodic_sync itself
            text = text[:m.start(2)] + "enable = false" + text[m.end(2):]
        with open(path, "w") as f:
            f.write(text)

    def _prepare_configs(self):
        if os.path.isdir(self.config):
            shutil.rmtree(self.config)
        shutil.copytree(self.reference_etc, self.config)
        for fn in os.listdir(self.config):
            path = os.path.join(self.config, fn)
            if not os.path.isfile(path):
                continue
            if fn.endswith(".toml"):
                self._patch_file(path)
            if fn == "hf3fs_fuse_main.toml":
                self._patch_fuse_toml(path)
                # op-level records sit at DBG (OP_LOG_LEVEL, FuseClients.h:31),
                # so a probe that needs to see them raises the category level
                # here; it is the toml that init-cluster pushes to mgmtd.
                lvl = self.cfg.get("fuse_log_level")
                if lvl:
                    with open(path) as f:
                        text = f.read()
                    text = re.sub(r"(?m)^level = '[A-Z]+'",
                                  "level = '%s'" % lvl, text)
                    with open(path, "w") as f:
                        f.write(text)
        # our own FDB descriptor: the tomls were just repointed at this file
        # and a verbatim copy of the reference's would still name the
        # reference deployment's database
        self._write_fdb_cluster()
        # app tomls: node ids of our own cluster
        node_map = {"mgmtd_main_app.toml": self.node_ids["mgmtd"],
                    "meta_main_app.toml": self.node_ids["meta"]}
        for i, node in enumerate(self.node_ids["storage"]):
            node_map["storage%d_main_app.toml" % (i + 1)] = node
        for fn, node in node_map.items():
            path = os.path.join(self.config, fn)
            if not os.path.exists(path):
                continue
            with open(path) as f:
                text = f.read()
            text = re.sub(r"node_id = \d+", "node_id = %d" % node, text)
            with open(path, "w") as f:
                f.write(text)
        # admin_cli authenticates as root via [user_info] (tests/fuse fills
        # it with ${TOKEN} through envsubst; the reference deploy has '')
        ac = os.path.join(self.config, "admin_cli.toml")
        with open(ac) as f:
            text = f.read()
        text = re.sub(r"\[user_info\].*?(?=\n\[|\Z)",
                      "[user_info]\nuid = 0\ngid = 0\ntoken = '%s'\n"
                      % self.token, text, count=1, flags=re.DOTALL)
        with open(ac, "w") as f:
            f.write(text)
        if not self.native_rdma:
            # rxe fallback: admin_cli talks to storage via RDMA by default
            with open(ac) as f:
                text = f.read()
            text = text.replace("force_use_tcp = false",
                                "force_use_tcp = true")
            with open(ac, "w") as f:
                f.write(text)

    @staticmethod
    def _size_bytes(text):
        """'512KB' / '4MB' -> bytes. 3FS sizes are binary multiples."""
        units = {"GIB": 1024 ** 3, "MIB": 1024 ** 2, "KIB": 1024,
                 "GB": 1024 ** 3, "MB": 1024 ** 2, "KB": 1024}
        t = text.strip().upper()
        for suffix, factor in sorted(units.items(), key=lambda kv: -len(kv[0])):
            if t.endswith(suffix):
                return int(float(t[:-len(suffix)]) * factor)
        return int(t)

    # Classes the chunk engine allocates for small writes regardless of what
    # chunk_size_list permits: the reference deployment's disk shows
    # 64/128/256KiB filled even though its list starts at 512KB. Counted as an
    # upper bound so the space gate errs on the safe side.
    ENGINE_IMPLICIT_CLASSES = ("64KB", "128KB", "256KB")

    def planned_disk_bytes(self):
        """Upper bound of what this instance will occupy.

        Measured on the reference deployment: one storage target
        preallocates `chunk_size * 1024` bytes per class that gets used
        (512KB -> 512M, 4MB -> 4G), and its six default classes cost it 51G
        across 6 target paths. Classes listed but never used stay empty
        (16MB/64MB were 4K). Compared with free space *before* starting
        anything: a second six-class instance next to the reference filled a
        548G disk to 99% within a minute, which tripped FoundationDB's
        free-space guard (5%) and took the running reference cluster's
        database down with it.
        """
        # Larger classes stay empty until a workload actually allocates one
        # (the reference's 8MB/16MB/32MB/64MB directories were 4K after a
        # bring-up that only wrote 4MB), so the bound counts what gets
        # allocated in practice. Sanity check: the reference's own list
        # predicts 8.1 GiB per target path -> 48G for 6 paths, and it
        # occupies 51G.
        classes = {s for s in set(self.chunk_sizes) | set(self.ENGINE_IMPLICIT_CLASSES)
                   if self._size_bytes(s) <= 4 * 1024 ** 2}
        per_target = sum(self._size_bytes(s) * 1024 for s in classes)
        return per_target * self.num_storage * self.num_disks

    def actual_disk_bytes(self):
        total = 0
        for root, _dirs, files in os.walk(self.data):
            for fn in files:
                try:
                    total += os.lstat(os.path.join(root, fn)).st_size
                except OSError:
                    pass
        return total

    def check_disk_space(self, margin=1.3, floor=5 * 1024 ** 3):
        """Refuse to start unless the test_dir filesystem can hold us."""
        os.makedirs(self.base, exist_ok=True)
        usage = shutil.disk_usage(self.base)
        need = self.planned_disk_bytes() * margin + floor
        plan = ("space plan: test_dir=%s free=%.1fG need=%.1fG "
                "(expected %.1fG * %.1f + %.0fG), chunk_size_list=%s"
                % (self.base, usage.free / 1e9, need / 1e9,
                   self.planned_disk_bytes() / 1e9, margin, floor / 1e9,
                   self.chunk_sizes))
        if usage.free < need:
            raise DriverError(
                "not enough space on the test_dir filesystem - %s; point "
                "test_dir at a filesystem with room, or shorten "
                "chunk_size_list" % plan)
        return plan

    # -- pre-flight -------------------------------------------------------

    def check_configs(self):
        """Gate: prove the patched configs cannot touch the reference
        deployment, then that every port we will use is free.

        Three families of mistakes have already bitten this driver: a path
        still pointing into the reference root (its mount point, its logs,
        its FDB), a port already taken by the cluster that keeps running,
        and an fdb.cluster naming someone else's database. All three are
        checked here, before anything is started.
        """
        problems = []
        ours = {self.mgmtd_port, self.fdb_port}
        tomls = sorted(fn for fn in os.listdir(self.config)
                       if fn.endswith(".toml"))
        for fn in tomls:
            with open(os.path.join(self.config, fn)) as f:
                text = f.read()
            if self.reference_root in text:
                problems.append("%s still references %s" %
                                (fn, self.reference_root))
            for m in self.LISTEN_PORT_RE.finditer(text):
                ours.add(int(m.group(2)))
        for fn in tomls:
            with open(os.path.join(self.config, fn)) as f:
                text = f.read()
            for m in self.LOOPBACK_PORT_RE.finditer(text):
                if int(m.group(1)) not in ours:
                    problems.append("%s points at an unowned port %s" %
                                    (fn, m.group(1)))
            for m in self.CLUSTER_ID_RE.finditer(text):
                if m.group(1) != self.cluster_id:
                    problems.append("%s keeps cluster_id '%s'" %
                                    (fn, m.group(1)))
            for m in re.finditer(r"clusterFile = '([^']*)'", text):
                if m.group(1) != self.fdb_cluster:
                    problems.append("%s points at %s" % (fn, m.group(1)))
        busy = [p for p in sorted(ours) if not self._port_free(p)]
        if busy:
            problems.append("ports already in use: %s (pick another "
                            "port_offset)" % busy)
        if problems:
            raise DriverError("config self-check failed:\n  " +
                              "\n  ".join(problems))
        return sorted(ours)

    @staticmethod
    def _port_free(port):
        s = socket.socket()
        try:
            s.bind(("0.0.0.0", port))
            return True
        except OSError:
            return False
        finally:
            s.close()

    # -- fdb --------------------------------------------------------------

    def _write_fdb_cluster(self):
        with open(self.fdb_cluster, "w") as f:
            f.write("%s:%s@127.0.0.1:%d\n" %
                    (self.cfg.get("fdb_cluster_name", "cmtest"),
                     self.cluster_id, self.fdb_port))

    def _start_fdb(self, fresh):
        os.makedirs(os.path.join(self.data, "fdb"), exist_ok=True)
        self._write_fdb_cluster()
        self._spawn("fdb",
                    [self.cfg.get("fdbserver", "fdbserver"),
                     "-p", "auto:%d" % self.fdb_port,
                     "-d", os.path.join(self.data, "fdb"),
                     "-L", self.log, "-C", self.fdb_cluster],
                    "fdb.out")
        time.sleep(3)
        if fresh:
            r = subprocess.run(
                [self.cfg.get("fdbcli", "fdbcli"), "-C", self.fdb_cluster,
                 "--exec", "configure new single ssd"],
                env=self._env(), capture_output=True, text=True)
            if r.returncode != 0:
                raise DriverError("fdb configure failed: %s" % r.stderr)
        # wait until the database is accepting transactions
        deadline = time.time() + 60
        while time.time() < deadline:
            r = subprocess.run(
                [self.cfg.get("fdbcli", "fdbcli"), "-C", self.fdb_cluster,
                 "--exec", "status minimal"],
                env=self._env(), capture_output=True, text=True)
            out = (r.stdout + r.stderr).lower()
            if r.returncode == 0 and ("available" in out or "healthy" in out):
                return
            time.sleep(2)
        raise DriverError("fdb not healthy within 60s")

    # -- services ----------------------------------------------------------

    def _start_mgmtd(self):
        self._spawn("mgmtd", [
            os.path.join(self.binary, "mgmtd_main"),
            "--app_cfg",
            os.path.join(self.config, "mgmtd_main_app.toml"),
            "--launcher_cfg",
            os.path.join(self.config, "mgmtd_main_launcher.toml"),
            "--cfg", os.path.join(self.config, "mgmtd_main.toml"),
        ], "mgmtd.out")
        # primary election can take up to ~2min on first bootstrap
        self._wait_admin(("list-nodes",), "PRIMARY_MGMTD", 240, "mgmtd")

    def _start_storage(self, i):
        # --cfg pins the per-process service config; without it all storage
        # procs would pull the single per-type config from mgmtd and collide
        # on listen_port (configMap is keyed by nodeType only)
        self._spawn("storage%d" % i, [
            os.path.join(self.binary, "storage_main"),
            "--app_cfg",
            os.path.join(self.config, "storage%d_main_app.toml" % (i + 1)),
            "--launcher_cfg",
            os.path.join(self.config, "storage%d_main_launcher.toml" % (i + 1)),
            "--cfg",
            os.path.join(self.config, "storage%d_main.toml" % (i + 1)),
        ], "storage%d.out" % i)

    def _start_meta(self):
        self._spawn("meta", [
            os.path.join(self.binary, "meta_main"),
            "--app_cfg", os.path.join(self.config, "meta_main_app.toml"),
            "--launcher_cfg",
            os.path.join(self.config, "meta_main_launcher.toml"),
            "--cfg", os.path.join(self.config, "meta_main.toml"),
        ], "meta.out")

    def _wait_nodes(self):
        ids = [self.node_ids["meta"]] + list(self.node_ids["storage"])
        for node in ids:
            self._wait_node(node)

    def _start_fuse(self):
        self._spawn("fuse", [
            os.path.join(self.binary, "hf3fs_fuse_main"),
            "--launcher_cfg",
            os.path.join(self.config, "hf3fs_fuse_main_launcher.toml"),
        ], "fuse.out")
        deadline = time.time() + 60
        while time.time() < deadline:
            r = subprocess.run(["mountpoint", "-q", self.mount])
            if r.returncode == 0:
                return
            time.sleep(2)
        raise DriverError("fuse not mounted within 60s")

    # -- one-time init (fresh FDB) -----------------------------------------

    def _target_id(self, node, disk):
        return node * 100000 + (disk + 1) * 1000 + 1

    def _init_cluster_state(self):
        # user-add with an empty --token value breaks argparse (the empty
        # argv element shifts positionals: uid gets the name string). The
        # reference deployment runs mgmtd with authenticate=false, so no
        # token is needed at all - omit the flag when token is empty.
        args = ["user-add", "--root", "--admin"]
        if self.token:
            args += ["--token", self.token]
        args += ["0", "root"]
        self._ac(*args)
        if self.token:
            self._ac("user-set-token", "--new", "0")
        self._ac("init-cluster", "--skip-config-check", "1", "524288", "1",
                 "--mgmtd", os.path.join(self.config, "mgmtd_main.toml"),
                 "--meta", os.path.join(self.config, "meta_main.toml"),
                 "--storage",
                 os.path.join(self.config, "storage1_main.toml"),
                 "--fuse",
                 os.path.join(self.config, "hf3fs_fuse_main.toml"))

    def _upload_chains(self):
        # chains: one per disk, chain j = disk j of every storage proc, so
        # killing one storage leaves a survivor on every chain (rf ==
        # num_storage). The CSV header needs one column per replica.
        chains_csv = os.path.join(self.config, "chains.csv")
        table_csv = os.path.join(self.config, "chain-table.csv")
        with open(chains_csv, "w") as fc, open(table_csv, "w") as ft:
            fc.write("ChainId" + ",TargetId" * self.rf + "\n")
            ft.write("ChainId\n")
            for j in range(self.num_disks):
                chain_id = 900200001 + j
                tids = []
                for r in range(self.rf):
                    node = self.node_ids["storage"][r]
                    tid = self._target_id(node, j)
                    self._ac("create-target", "--node-id", str(node),
                             "--disk-index", str(j), "--target-id",
                             str(tid), "--chain-id", str(chain_id),
                             "--chunk-size", *self.chunk_sizes)
                    tids.append(str(tid))
                fc.write("%d,%s\n" % (chain_id, ",".join(tids)))
                ft.write("%d\n" % chain_id)
        self._ac("upload-chains", chains_csv)
        self._ac("upload-chain-table", "1", table_csv,
                 "--desc", "replica-%d" % self.rf)
        self._ac("mkdir", "--perm", "0755", self.testdir)
        uid = str(os.getuid())
        self._ac("set-perm", "--uid", uid, "--gid", uid, self.testdir)

    # -- lifecycle ----------------------------------------------------------

    def init_cluster(self):
        self.stop_all()
        for pid, exe in self.reap_own_processes():
            print("reaped leftover pid %d (%s)" % (pid, exe), flush=True)
        # space first: never wipe a previous run's data, and never start a
        # cluster the filesystem cannot hold (see planned_disk_bytes)
        print(self.check_disk_space(), flush=True)
        shutil.rmtree(self.base, ignore_errors=True)
        for d in (self.config, self.log, self.mount,
                  os.path.join(self.data, "fdb")):
            os.makedirs(d, exist_ok=True)
        for i in range(self.num_storage):
            for j in range(self.num_disks):
                os.makedirs(os.path.join(self.data, "s%d" % (i + 1),
                                         "d%d" % (j + 1)))
        self._prepare_configs()
        self.check_configs()
        self._start_fdb(fresh=True)
        self._init_cluster_state()
        self._start_mgmtd()
        for i in range(self.num_storage):
            self._start_storage(i)
        self._start_meta()
        self._wait_nodes()
        self._upload_chains()
        self._wait_admin(("list-targets",), "SERVING", 120, "targets")
        self._start_fuse()
        self.wait_ready()
        print("actual data footprint %.1fG (predicted upper bound %.1fG, "
              "chunk_size_list=%s)"
              % (self.actual_disk_bytes() / 1e9,
                 self.planned_disk_bytes() / 1e9, self.chunk_sizes),
              flush=True)

    def stop_all(self):
        for name in list(self._procs):
            self._kill_pid(name)
        # -z: see _kill_client.
        subprocess.run(["fusermount3", "-u", "-z", self.mount],
                       capture_output=True, timeout=30)

    def reap_own_processes(self):
        """Kill leftovers of an earlier run of ours.

        The daemons are spawned, not supervised: when a harness process
        dies (a crashed run, a killed shell) its children keep the ports and
        the data directory, and the next run then trips over "listen_port
        already in use" or "Database already exists!". We only ever match a
        command line that names our own base directory *and* one of the 3FS
        binaries we start, so the reference deployment cannot be hit - not
        even by a pattern that appears in someone's shell command.
        """
        if not os.path.isdir("/proc"):
            return []
        binaries = ("fdbserver", "mgmtd_main", "meta_main", "storage_main",
                    "hf3fs_fuse_main")
        mine = {os.getpid(), os.getppid()}
        killed = []
        for entry in os.listdir("/proc"):
            if not entry.isdigit() or int(entry) in mine:
                continue
            pid = int(entry)
            try:
                with open("/proc/%d/cmdline" % pid, "rb") as f:
                    cmd = f.read().decode("utf-8", "replace").replace("\0", " ")
            except OSError:
                continue
            if self.base in cmd and any(b in cmd for b in binaries):
                try:
                    os.kill(pid, signal.SIGKILL)
                    killed.append((pid, cmd.split()[0]))
                except OSError:
                    pass
        if killed:
            time.sleep(1)
        return killed

    # -- kill classes --------------------------------------------------------

    def _kill_client(self):
        self._kill_pid("fuse")
        # SIGKILL first, so the umount cannot ask the daemon to flush (a
        # flush here would commit the data a crash pass exists to judge);
        # -z because the frozen workload still holds fds on the mount -
        # plain -u fails EBUSY and leaks the mount entry under the remount.
        subprocess.run(["fusermount3", "-u", "-z", self.mount],
                       capture_output=True, timeout=30)

    def _kill_storage(self):
        self._killed_storage = 0
        self._kill_pid("storage0")

    def _kill_cluster(self):
        self.stop_all()

    def _recover_client(self):
        self._start_fuse()

    def _recover_storage(self):
        self._start_storage(self._killed_storage)
        # storage recovery is heartbeat-driven; wait_ready() then polls
        # until the cluster serves IO again; poll, do not sleep fixed.

    def _recover_cluster(self):
        self._start_fdb(fresh=False)
        self._start_mgmtd()
        for i in range(self.num_storage):
            self._start_storage(i)
        self._start_meta()
        self._wait_nodes()
        self._start_fuse()


class AttachDriver(BaseDriver):
    """Drive our own FUSE client against a deployment that is already running.

    The harness owns exactly one process here: the client it starts from the
    deployment's own launcher config at the deployment's own mount point. The
    deployment's mgmtd, meta and FoundationDB are only touched by the two
    daemon-killing classes, each approved as its own experiment: storage
    SIGKILLs one storage daemon and restarts it from its pinned config
    (crashqa.md section 5, plan6); cluster SIGKILLs every daemon of this
    deployment - storage, meta, mgmtd and its private FDB - and brings them
    back in dependency order (crashqa.md section 6, plan7). Processes whose
    cmdline does not name this deployment's root are never matched, so the
    host's system FDB and anything foreign are safe by construction.

    The workload runs in a subdirectory of the deployment's scratch space
    (`subdir`), which this driver empties before each pass: that subtree is
    ours, and anything else in the same scratch directory is left alone.
    """

    def __init__(self, cfg):
        self.cfg = cfg
        self.launcher = cfg["launcher"]
        self.binary = cfg["binary"]
        self.mount = self._launcher_field("mountpoint")
        self.testdir = cfg.get("subdir", "work/cmharness")
        self.root = os.path.dirname(
            os.path.dirname(os.path.abspath(self.launcher)))
        self.libs = cfg.get("lib_dirs") or [
            os.path.join(self.root, "fdb", "usr", "lib"),
            os.path.join(os.path.expanduser("~"), ".local", "lib",
                         "x86_64-linux-gnu")]
        self.logdir = cfg.get("log_dir") or os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "logs")
        self._proc = None
        self._storage_down = None
        self._cluster_down = None

    def _launcher_field(self, key):
        pattern = re.compile(r"\s*%s\s*=\s*'([^']*)'" % re.escape(key))
        with open(self.launcher) as f:
            for line in f:
                m = pattern.match(line)
                if m:
                    return m.group(1)
        raise DriverError("cannot read %s from %s" % (key, self.launcher))

    OWNER_MARKER = ".cm-owned"

    def _claim_subtree(self):
        """Empty our workload subtree - but only if it is demonstrably ours.

        init_cluster and stop_all remove <mount>/<subdir>. Pointed at a
        directory that already holds someone else's files, that silently
        deletes them: it happened once by passing subdir="work", which was the
        deployment's whole scratch directory, and two of its files were lost.
        So the driver claims a subtree by creating a marker file, and refuses
        to touch any non-empty directory that does not carry the marker.
        """
        target = os.path.join(self.mount, self.testdir)
        marker = os.path.join(target, self.OWNER_MARKER)
        if os.path.isdir(target):
            entries = set(os.listdir(target))
            if entries and self.OWNER_MARKER not in entries:
                raise DriverError(
                    "%s holds %d entries and is not ours (no %s marker) - "
                    "point subdir at a directory this driver may own"
                    % (target, len(entries), self.OWNER_MARKER))
            shutil.rmtree(target, ignore_errors=True)
        os.makedirs(target, exist_ok=True)
        with open(marker, "w"):
            pass

    def _release_subtree(self):
        """Remove our subtree, and only if the marker says it is ours."""
        target = os.path.join(self.mount, self.testdir)
        if not os.path.isdir(target):
            return
        if self.OWNER_MARKER not in set(os.listdir(target)):
            return                      # not ours: leave it alone
        shutil.rmtree(target, ignore_errors=True)

    def _env(self):
        env = dict(os.environ)
        libs = [p for p in self.libs if p and os.path.isdir(p)]
        if env.get("LD_LIBRARY_PATH"):
            libs.append(env["LD_LIBRARY_PATH"])
        env["LD_LIBRARY_PATH"] = ":".join(libs)
        return env

    def _mounted(self):
        return subprocess.run(["mountpoint", "-q", self.mount],
                              capture_output=True,
                              timeout=15).returncode == 0

    def _wait_mount(self, timeout=90):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self._mounted():
                return True
            time.sleep(2)
        return False

    def _start_client(self):
        os.makedirs(self.logdir, exist_ok=True)
        logf = open(os.path.join(self.logdir, "fuse.out"), "a")
        self._proc = subprocess.Popen(
            [self.binary, "--launcher_cfg", self.launcher], stdout=logf,
            stderr=subprocess.STDOUT, env=self._env(), start_new_session=True)

    def _stop_client(self):
        """Crash the client, never stop it gracefully: a SIGTERM shutdown
        flushes its write buffer and commits the data a crash pass is judging,
        and a lazy unmount is what a crashed client leaves behind."""
        if self._proc is not None and self._proc.poll() is None:
            self._proc.kill()
            try:
                self._proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                pass
        self._proc = None
        subprocess.run(["fusermount3", "-u", "-z", self.mount],
                       capture_output=True, timeout=30)

    # -- BaseDriver interface --------------------------------------------

    def init_cluster(self):
        """Not a bring-up: drop our own client, refuse to touch anyone else's
        mount, start a fresh client and empty our own workload subtree (a
        previous pass's files would make the workloads' O_EXCL creates fail).
        """
        self._stop_client()
        if self._mounted():
            raise DriverError(
                "%s is already mounted and this driver did not create that "
                "mount - refusing to interfere" % self.mount)
        self._start_client()
        if not self._wait_mount():
            raise DriverError("our client did not mount %s" % self.mount)
        self._claim_subtree()
        self.wait_ready()

    # -- storage kill class -------------------------------------------------
    # Killing one storage daemon takes one replica of every chain down; the
    # deployment keeps serving on the remaining replicas. The recipe follows
    # scripts/attach_storage_kill.py, which measured this class on tea3
    # (crashqa.md section 5): SIGKILL only, restart with the node's pinned
    # config, never start a second process for a node that is still alive.

    def _mgmtd_address(self):
        """The deployment mgmtd's loopback address, derived from its own
        config (<deploy>/etc/mgmtd_main.toml), never hardcoded."""
        path = os.path.join(self.root, "etc", "mgmtd_main.toml")
        with open(path) as f:
            for line in f:
                m = re.match(r"\s*listen_port\s*=\s*(\d+)", line)
                if m:
                    return '["TCP://127.0.0.1:%s"]' % m.group(1)
        raise DriverError("cannot read listen_port from %s" % path)

    def _admin_cli(self, *args):
        """Read-only admin_cli query; never raises - the recovery loop must
        stay alive even when mgmtd is busy."""
        cmd = [os.path.join(os.path.dirname(self.binary), "admin_cli"),
               "-cfg", os.path.join(self.root, "etc", "admin_cli.toml"),
               "--config.mgmtd_client.mgmtd_server_addresses",
               self._mgmtd_address(), "--"] + list(args)
        try:
            r = subprocess.run(cmd, capture_output=True, text=True,
                               timeout=30, env=self._env())
            return r.returncode, r.stdout + r.stderr
        except Exception as e:                      # noqa: BLE001
            return -1, "<%s: %s>" % (type(e).__name__, e)

    def _targets_serving(self):
        """True when mgmtd reports every target SERVING."""
        rc, out = self._admin_cli("list-targets")
        lines = [l for l in out.splitlines() if l.split()[:1]
                 and l.split()[0].isdigit()]
        return bool(lines) and all("SERVING" in l for l in lines)

    def _find_procs(self, comm):
        """This deployment's `comm` processes as (pid, cmdline), matched on
        /proc/<pid>/comm plus the deployment root in the cmdline - a bare
        cmdline pattern would match the very shell that runs us, and the
        root check keeps the host's system FDB and any foreign deployment
        out. Zombies have an empty cmdline and never match."""
        found = []
        try:
            entries = os.listdir("/proc")
        except OSError:
            entries = []       # cannot enumerate -> refuse to kill anything
        for entry in entries:
            if not entry.isdigit():
                continue
            pid = int(entry)
            try:
                with open("/proc/%d/comm" % pid) as f:
                    if f.read().strip() != comm:
                        continue
                with open("/proc/%d/cmdline" % pid, "rb") as f:
                    cmdline = f.read().decode("utf-8", "replace") \
                        .replace("\0", " ").strip()
            except OSError:
                continue
            if self.root not in cmdline:
                continue                # not this deployment's process
            found.append((pid, cmdline))
        return sorted(found)

    def _find_storage(self, node=None):
        """This deployment's storage_main processes as (node, pid, cmdline).
        When `node` is given, restrict to that node."""
        found = [(self._node_id_of(cmdline), pid, cmdline)
                 for pid, cmdline in self._find_procs("storage_main")]
        if node is not None:
            found = [f for f in found if f[0] == node]
        return sorted(found)

    @staticmethod
    def _node_id_of(cmdline):
        """The node's id, read from the app config it was started with -
        derived, never hardcoded."""
        for token in cmdline.split():
            if token.endswith("_main_app.toml"):
                with open(token) as f:
                    for line in f:
                        if line.strip().startswith("node_id"):
                            return int(line.split("=")[1].strip())
        raise DriverError("cannot find the node id in: %s" % cmdline)

    def _kill_storage(self):
        """SIGKILL one storage daemon (highest node id), then crash our own
        client: with both writers gone the durable state is frozen, however
        long the restart below takes (a surviving client's periodic_sync
        would commit the very lengths the judge may still allow to lag)."""
        found = self._find_storage()
        if not found:
            raise DriverError("no storage_main of %s is running" % self.root)
        node, pid, cmdline = found[-1]
        pinned = os.path.join(self.root, "log", "node-%d.toml" % node)
        if not os.path.exists(pinned):
            raise DriverError(
                "no pinned config %s to restart pid %d with; killing it "
                "could not be undone" % (pinned, pid))
        restart = cmdline.split()
        if "--cfg" not in restart:
            restart += ["--cfg", pinned]
        try:
            cwd = os.readlink("/proc/%d/cwd" % pid)
        except OSError:
            cwd = "/"
        os.kill(pid, signal.SIGKILL)
        self._storage_down = {"node": node, "restart": restart, "cwd": cwd}
        self._stop_client()

    def _recover_storage(self):
        """Restart the killed daemon and wait for every target to serve
        again. Idempotent: a second call while the node is already alive
        only waits, so the trap/restore path can retry it safely."""
        down = self._storage_down
        if down is None:
            return
        if not self._find_storage(down["node"]):
            os.makedirs(self.logdir, exist_ok=True)
            logf = open(os.path.join(
                self.logdir, "restart-storage%d.out" % down["node"]), "a")
            subprocess.Popen(down["restart"], start_new_session=True,
                             stdout=logf, stderr=subprocess.STDOUT,
                             cwd=down["cwd"], env=self._env())
        deadline = time.time() + 900
        while time.time() < deadline:
            if self._targets_serving():
                self._storage_down = None
                return
            time.sleep(5)
        raise DriverError(
            "storage node %d restarted but not all targets are SERVING "
            "within 900s" % down["node"])

    # -- cluster kill class -------------------------------------------------
    # Full power loss of the deployment: every daemon (storage x3, meta,
    # mgmtd, the private FDB) plus our client. Beyond 3FS's fail-stop model
    # (crashqa.md section 6) - this is attack-surface probing, so the run
    # must be undoable by construction: every restart recipe is captured
    # from the live process BEFORE anything is killed, and the kill refuses
    # to fire while any daemon is missing.

    CLUSTER_KILL_ORDER = ("storage_main", "meta_main", "mgmtd_main",
                          "fdbserver")
    CLUSTER_RECOVER_ORDER = ("fdbserver", "mgmtd_main", "meta_main",
                             "storage_main")

    def _restart_recipe(self, comm, pid, cmdline):
        """The exact command that brings this process back, captured from
        the live process. Storage daemons additionally get their pinned
        per-node config (without it a restarted storage inherits another
        node's ports from mgmtd and dies with RPC::ListenFailed)."""
        restart = cmdline.split()
        node = None
        if comm == "storage_main":
            node = self._node_id_of(cmdline)
            pinned = os.path.join(self.root, "log", "node-%d.toml" % node)
            if not os.path.exists(pinned):
                raise DriverError(
                    "no pinned config %s to restart pid %d with; killing "
                    "it could not be undone" % (pinned, pid))
            if "--cfg" not in restart:
                restart += ["--cfg", pinned]
        try:
            cwd = os.readlink("/proc/%d/cwd" % pid)
        except OSError:
            cwd = "/"
        return {"pid": pid, "node": node, "restart": restart, "cwd": cwd}

    @staticmethod
    def _pid_gone_or_zombie(pid):
        """A SIGKILLed process lingers in /proc as a zombie until reaped;
        a zombie holds no sockets, so gone-or-zombie is when its ports are
        free again."""
        try:
            with open("/proc/%d/cmdline" % pid, "rb") as f:
                return f.read() == b""
        except OSError:
            return True

    def _kill_cluster(self):
        """Crash our client, then SIGKILL every daemon of the deployment.
        The client goes first: with the writer gone before the daemons, no
        FLUSH can launder unpromised state into the durable set, and the
        durable state stays frozen however long recovery takes."""
        recipes = {}
        for comm in self.CLUSTER_KILL_ORDER:
            procs = self._find_procs(comm)
            if not procs:
                raise DriverError(
                    "no %s of %s is running; refusing a cluster kill that "
                    "could not be undone" % (comm, self.root))
            recipes[comm] = [self._restart_recipe(comm, pid, cmdline)
                             for pid, cmdline in procs]
        self._stop_client()
        victims = [r["pid"] for comm in self.CLUSTER_KILL_ORDER
                   for r in recipes[comm]]
        for pid in victims:
            os.kill(pid, signal.SIGKILL)
        # Wait for the victims to die: restarting onto a port whose owner
        # is still dying fails with "address in use".
        deadline = time.time() + 60
        while time.time() < deadline:
            if all(self._pid_gone_or_zombie(pid) for pid in victims):
                break
            time.sleep(1)
        self._cluster_down = recipes

    def _fdb_available(self):
        fdbcli = os.path.join(self.root, "fdb", "usr", "bin", "fdbcli")
        try:
            r = subprocess.run(
                [fdbcli, "-C", os.path.join(self.root, "etc", "fdb.cluster"),
                 "--exec", "status minimal"], capture_output=True, text=True,
                timeout=30, env=self._env())
            out = (r.stdout + r.stderr).lower()
            # "The database is unavailable." contains "available": match the
            # full positive sentence, never the substring.
            return "the database is available" in out
        except Exception:                           # noqa: BLE001
            return False

    def _recover_cluster(self):
        """Bring the deployment back in dependency order: FDB first (mgmtd
        stores its state there), then mgmtd, meta, storage; wait for every
        target to serve again. Idempotent per daemon - but a restarted
        daemon carries a NEW pid, so liveness is checked by identity
        (storage: node id; singleton daemons: any live process of the
        comm), never by the pre-kill pid: comparing pids restarts a live
        daemon, and the duplicate races it on ports until one dies FATAL
        (seen on tea3, 2026-09-23)."""
        down = self._cluster_down
        if down is None:
            return
        os.makedirs(self.logdir, exist_ok=True)
        for comm in self.CLUSTER_RECOVER_ORDER:
            if comm == "storage_main":
                alive = {node for node, _, _ in self._find_storage()}
                todo = [r for r in down[comm] if r["node"] not in alive]
            else:
                todo = [] if self._find_procs(comm) else list(down[comm])
            for r in todo:
                logf = open(os.path.join(
                    self.logdir, "restart-%s-%d.out" % (comm, r["pid"])), "a")
                subprocess.Popen(r["restart"], start_new_session=True,
                                 stdout=logf, stderr=subprocess.STDOUT,
                                 cwd=r["cwd"], env=self._env())
            if comm == "fdbserver":
                # A fresh fdbserver can lose the bind race against its
                # predecessor's dying socket and exit immediately (seen on
                # tea3, 2026-09-23, twice): the wait below therefore
                # restarts it when it is gone, not only waits.
                deadline = time.time() + 420
                while True:
                    if self._fdb_available():
                        break
                    if not self._find_procs(comm):
                        for r in down[comm]:
                            logf = open(os.path.join(
                                self.logdir,
                                "restart-%s-%d.out" % (comm, r["pid"])), "a")
                            subprocess.Popen(
                                r["restart"], start_new_session=True,
                                stdout=logf, stderr=subprocess.STDOUT,
                                cwd=r["cwd"], env=self._env())
                    if time.time() >= deadline:
                        raise DriverError(
                            "the deployment's FDB is not available within "
                            "420s of the restart; leaving the scene as is")
                    time.sleep(5)
            if comm == "mgmtd_main":
                # storage started before mgmtd answers spins on
                # PrimaryMgmtdNotFound; gate it on mgmtd replying
                deadline = time.time() + 120
                while time.time() < deadline:
                    rc, out = self._admin_cli("list-nodes")
                    rows = [l for l in out.splitlines()
                            if l.split()[:1] and l.split()[0].isdigit()]
                    if rc == 0 and rows:
                        break
                    time.sleep(5)
                else:
                    raise DriverError(
                        "mgmtd did not answer list-nodes within 120s of "
                        "the restart; leaving the scene as is")
        deadline = time.time() + 900
        while time.time() < deadline:
            if self._targets_serving():
                self._cluster_down = None
                return
            time.sleep(5)
        raise DriverError(
            "cluster restarted but not all targets are SERVING within 900s")

    # -- BaseDriver interface --------------------------------------------

    def kill(self, kill_class):
        if kill_class == KILL_CLIENT:
            self._stop_client()
        elif kill_class == KILL_STORAGE:
            self._kill_storage()
        elif kill_class == KILL_CLUSTER:
            self._kill_cluster()
        else:
            raise DriverError("unknown kill class %s" % kill_class)

    def recover(self, kill_class):
        if kill_class == KILL_STORAGE:
            self._recover_storage()
        elif kill_class == KILL_CLUSTER:
            self._recover_cluster()
        self._start_client()
        if not self._wait_mount():
            raise DriverError("our client did not remount %s" % self.mount)
        self.wait_ready()

    def stop_all(self):
        """Leave the deployment as found: bring back any daemon we killed
        and have not yet restarted, stop our client, and remove our own
        workload subtree (only if the marker says it is ours)."""
        if self._cluster_down is not None:
            self._recover_cluster()
        if self._storage_down is not None:
            self._recover_storage()
        if self._proc is not None or self._mounted():
            self._release_subtree()
        self._stop_client()


def load_driver(cfg_path):
    with open(cfg_path) as f:
        cfg = json.load(f)
    kind = cfg.get("driver")
    if kind == "3fs":
        return ThreeFSDriver(cfg_path)
    if kind == "attach":
        return AttachDriver(cfg)
    raise DriverError("unknown driver %s" % kind)
