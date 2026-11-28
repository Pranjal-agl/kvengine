#!/usr/bin/env python3
"""End-to-end tests that run real kvengine processes on localhost.

    python3 scripts/e2e/run.py --bin ./kvengine                 # all scenarios
    python3 scripts/e2e/run.py --bin ./kvengine failover reads  # some
    python3 scripts/e2e/run.py --bin ./kvengine --old-bin ./kvengine-v1 migrate

scripts/e2e/run.sh builds both binaries and runs everything. The migrate
scenario needs --old-bin (a v1.0 build) and is skipped without it.
"""

import argparse
import os
import random
import signal
import sys
import threading
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kvcluster import Cluster, KVError, eventually, wait_until  # noqa: E402

SCENARIOS = {}


def scenario(needs_old=False):
    def register(fn):
        fn.needs_old = needs_old
        SCENARIOS[fn.__name__] = fn
        return fn
    return register


def expect_value(node, key, want, timeout=10):
    """Linearizable GET on node must eventually return want."""
    def check():
        with node.client() as c:
            got = c.get(key)
        assert got == want, "%s: GET %r = %r, want %r" % (node.id, key, got, want)
    eventually(check, timeout, "%s to return %r for %r" % (node.id, want, key))


# --- scenarios -------------------------------------------------------------


@scenario()
def failover(binary, old):
    """Kill the leader; the rest elect a new one, keep every acknowledged
    write, and the old leader catches up when it comes back."""
    c = Cluster(binary, ["n0", "n1", "n2"])
    try:
        c.start_all()
        for i in range(20):
            c.put("k%d" % i, "v%d" % i)
        old_leader = c.leader()
        old_leader.kill()
        t0 = time.monotonic()
        new_leader = c.leader(timeout=10)
        assert new_leader.id != old_leader.id
        print("    new leader %s elected %.0f ms after SIGKILL"
              % (new_leader.id, (time.monotonic() - t0) * 1000))
        c.put("after", "failover")
        for n in c.live():
            for i in range(20):
                expect_value(n, "k%d" % i, b"v%d" % i)
        # A follower rejects writes and names the leader.
        follower = next(n for n in c.live() if n.id != new_leader.id)
        with follower.client() as cl:
            try:
                cl.put("x", "y")
                raise AssertionError("follower accepted a write")
            except KVError as e:
                assert "leader" in str(e), e
        old_leader.start(peers=c.peers_of(old_leader.id))
        expect_value(old_leader, "after", b"failover")
    except BaseException:
        c.cleanup(failed=True)
        raise
    c.cleanup()


@scenario()
def binary_values(binary, old):
    """Values with CRLF, NUL and high bytes survive Raft, snapshots and a restart."""
    c = Cluster(binary, ["n0", "n1", "n2"])
    vals = {
        "crlf": b"line1\r\nline2\r\n",
        "nul": b"\x00\x01\x02\x00",
        "high": bytes(range(128, 256)),
        "empty": b"",
        "big": os.urandom(256 * 1024),
    }
    try:
        c.start_all(extra=["--snapshot-every", "3"])
        for k, v in vals.items():
            c.put(k, v)
        for n in c.live():
            for k, v in vals.items():
                expect_value(n, k, v)
        for n in c.nodes.values():
            n.stop()
        c.start_all(extra=["--snapshot-every", "3"])
        for n in c.live():
            for k, v in vals.items():
                expect_value(n, k, v)
    except BaseException:
        c.cleanup(failed=True)
        raise
    c.cleanup()


@scenario()
def snapshot_catchup(binary, old):
    """A node down while the others write and snapshot many times catches up
    through InstallSnapshot when it restarts."""
    c = Cluster(binary, ["n0", "n1", "n2"])
    extra = ["--snapshot-every", "10"]
    try:
        c.start_all(extra=extra)
        c.leader()
        lagger = next(n for n in c.live() if n is not c.leader())
        lagger.stop()
        for i in range(300):
            c.put("k%d" % i, "v%d" % i)
        c.put("k7", "overwritten")
        lagger.start(peers=c.peers_of(lagger.id), extra=extra)
        expect_value(lagger, "k299", b"v299", timeout=20)
        expect_value(lagger, "k7", b"overwritten")
        assert os.path.exists(os.path.join(lagger.data_dir, "raft-snap.json")), \
            "lagging node has no snapshot on disk"
    except BaseException:
        c.cleanup(failed=True)
        raise
    c.cleanup()


@scenario()
def clean_shutdown(binary, old):
    """SIGTERM and SIGINT exit 0 promptly, and the data is all there after."""
    c = Cluster(binary, ["n0", "n1", "n2"])
    try:
        c.start_all()
        for i in range(10):
            c.put("k%d" % i, "v%d" % i)
        for i, n in enumerate(c.nodes.values()):
            sig = signal.SIGINT if i == 0 else signal.SIGTERM
            t0 = time.monotonic()
            code = n.stop(sig)
            assert code == 0, "%s exited %s after %s" % (n.id, code, sig.name)
            print("    %s exited 0 in %.0f ms after %s"
                  % (n.id, (time.monotonic() - t0) * 1000, sig.name))
            with open(os.path.join(c.root, n.id + ".log"), "rb") as f:
                assert b"shutdown complete" in f.read(), "%s: no 'shutdown complete' in log" % n.id
        c.start_all()
        for n in c.live():
            expect_value(n, "k9", b"v9")
    except BaseException:
        c.cleanup(failed=True)
        raise
    c.cleanup()


@scenario()
def follower_reads(binary, old):
    """Linearizable follower reads see acknowledged writes and refuse to answer
    without a leader; --stale-reads answers from local state instead."""
    c = Cluster(binary, ["n0", "n1", "n2"])
    try:
        ids = list(c.nodes)
        for i in ids:
            extra = ["--stale-reads"] if i == "n2" else []
            c[i].start(peers=c.peers_of(i, ids), extra=extra)
        leader = c.leader()
        # Make the leader one of n0/n1 so n2 (stale) and the other are followers.
        for _ in range(10):
            if leader.id != "n2":
                break
            leader.stop()
            leader.start(peers=c.peers_of("n2"), extra=["--stale-reads"])
            leader = c.leader()
        assert leader.id != "n2", "n2 kept winning the election"
        lin = next(c[i] for i in ("n0", "n1") if i != leader.id)
        stale = c["n2"]

        # Read-your-writes on a linearizable follower, every time, no retry.
        with leader.client() as lc, lin.client() as fc:
            for i in range(200):
                lc.put("rw", str(i))
                got = fc.get("rw")
                assert got == str(i).encode(), "linearizable follower read %r after write %d" % (got, i)
        expect_value(stale, "rw", b"199")

        # Freeze the leader. With no leader, a linearizable read must fail;
        # a stale read still answers from local state.
        leader.proc.send_signal(signal.SIGSTOP)
        try:
            with stale.client() as sc:
                assert sc.get("rw") == b"199"
            with lin.client(timeout=15) as fc:
                t0 = time.monotonic()
                try:
                    got = fc.get("rw")
                    # The two followers may elect a new leader first; then the
                    # read is legitimately served and must be current.
                    assert got == b"199", got
                    print("    followers elected a new leader before the read; got current value")
                except KVError as e:
                    took = time.monotonic() - t0
                    print("    linearizable read refused after %.1fs without a leader: %s" % (took, e))
                    # The read timeout is 2s; it used to take 10s (the TCP
                    # transport's timeout and retry) against a frozen leader.
                    assert took < 5, "refusal took %.1fs" % took
        finally:
            leader.proc.send_signal(signal.SIGCONT)
        c.put("rw", "final")
        for n in c.live():
            expect_value(n, "rw", b"final")
    except BaseException:
        c.cleanup(failed=True)
        raise
    c.cleanup()


@scenario()
def membership(binary, old):
    """Join a fourth node, kill the leader, remove it, add a fifth, then
    restart the whole cluster."""
    c = Cluster(binary, ["n0", "n1", "n2"])
    try:
        c.start_all(extra=["--snapshot-every", "50"])
        for i in range(120):
            c.put("k%d" % i, "v%d" % i)

        n3 = c.add("n3")
        n3.start(join=True, extra=["--snapshot-every", "50"])
        with c.leader().client() as lc:
            lc.admin("ADDNODE", "n3", n3.raft_addr)
        expect_value(n3, "k119", b"v119", timeout=20)
        members, _ = c.leader().members()
        assert set(members) == {"n0", "n1", "n2", "n3"}, members

        dead = c.leader()
        dead.kill()
        new = c.leader(timeout=10)
        c.put("after-kill", "1")
        with new.client() as lc:
            lc.admin("REMOVENODE", dead.id)
        eventually(lambda: _assert_members(c.leader(), {"n0", "n1", "n2", "n3"} - {dead.id}),
                   10, "membership without %s" % dead.id)
        del c.nodes[dead.id]

        n4 = c.add("n4")
        n4.start(join=True)
        with c.leader().client() as lc:
            lc.admin("ADDNODE", "n4", n4.raft_addr)
        expect_value(n4, "after-kill", b"1", timeout=20)
        final = set(c.nodes)
        eventually(lambda: _assert_members(c.leader(), final), 10, "final membership")

        # Full restart: membership comes from disk, not from flags, so start
        # every node with --join (no --peers) and they must still find each other.
        for n in c.nodes.values():
            n.stop()
        for n in c.nodes.values():
            n.start(join=True)
        for n in c.live():
            expect_value(n, "k0", b"v0", timeout=20)
            expect_value(n, "after-kill", b"1")
        _assert_members(c.leader(), final)
    except BaseException:
        c.cleanup(failed=True)
        raise
    c.cleanup()


def _assert_members(node, want):
    got, _ = node.members()
    assert set(got) == set(want), "members %s, want %s" % (sorted(got), sorted(want))


@scenario()
def crash_under_load(binary, old):
    """SIGKILL every node in the middle of a write load. After a restart every
    acknowledged write is present."""
    c = Cluster(binary, ["n0", "n1", "n2"])
    acked = {}
    stop = threading.Event()

    def writer(wid):
        seq = 0
        while not stop.is_set():
            try:
                with c.leader(timeout=2).client(timeout=2) as cl:
                    while not stop.is_set():
                        key = "w%d-%d" % (wid, seq)
                        cl.put(key, key)
                        acked[key] = True
                        seq += 1
            except Exception:
                time.sleep(0.02)

    try:
        c.start_all(extra=["--snapshot-every", "200"])
        c.leader()
        threads = [threading.Thread(target=writer, args=(w,), daemon=True) for w in range(8)]
        for t in threads:
            t.start()
        time.sleep(1.5)
        for n in c.nodes.values():
            n.proc.send_signal(signal.SIGKILL)
        stop.set()
        for t in threads:
            t.join(5)
        for n in c.nodes.values():
            n.proc.wait()
            n.log.close()
        keys = list(acked)
        print("    %d writes acknowledged before SIGKILL" % len(keys))
        assert len(keys) > 50, "load generator barely ran"
        c.start_all()
        node = c.leader()
        missing = []
        with node.client() as cl:
            for k in keys:
                if cl.get(k) != k.encode():
                    missing.append(k)
        assert not missing, "%d acknowledged writes lost, e.g. %s" % (len(missing), missing[:5])
    except BaseException:
        stop.set()
        c.cleanup(failed=True)
        raise
    c.cleanup()


@scenario()
def single_node(binary, old):
    """Single-node mode: CLI put/get/delete and the server survive restarts."""
    import subprocess
    import tempfile
    from kvcluster import Client, free_port
    d = tempfile.mkdtemp(prefix="kvengine-e2e-single-")
    wal = os.path.join(d, "data.wal")
    run = lambda *a: subprocess.run([binary, wal] + list(a), capture_output=True, check=True,
                                    env=dict(os.environ, KVENGINE_PPROF="off")).stdout.decode().strip()
    assert run("put", "a", "1") == "OK"
    assert run("get", "a") == "1"
    assert run("delete", "a") == "OK"
    assert run("get", "a") == "(nil)"
    assert run("put", "b", "2") == "OK"
    addr = "127.0.0.1:%d" % free_port()
    for attempt in range(2):
        p = subprocess.Popen([binary, wal, "serve", addr], stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL, env=dict(os.environ, KVENGINE_PPROF="off"))
        try:
            cl = eventually(lambda: Client(addr), 10, "single-node server")
            with cl:
                assert cl.get("b") == b"2"
                if attempt == 0:
                    cl.put("c", b"\r\n\x00")
                else:
                    assert cl.get("c") == b"\r\n\x00"
        finally:
            p.send_signal(signal.SIGTERM)
            assert p.wait(10) == 0
    import shutil
    shutil.rmtree(d)


@scenario(needs_old=True)
def migrate(binary, old):
    """Data written by v1.0 comes back when the whole cluster is stopped and
    restarted on the new binary.

    v1.0 stored keys and values in log entries as JSON strings; the current
    format uses base64. Keys like "user" are valid base64, so a decoder that
    confused the two would read them back as different bytes.

    Rolling upgrades from v1.0 are not supported (see README): a v1.0
    follower would read a new leader's base64 fields as literal strings."""
    c = Cluster(old, ["n0", "n1", "n2"])
    # Stay under 100 entries: v1.0 snapshots at 100 and then panics (its
    # post-snapshot indexing bug), so no working v1.0 cluster has a snapshot.
    # The v1.0 snapshot format is covered by store's unit tests.
    want = {"k%d" % i: "v%d" % i for i in range(80)}
    want.update({"user": "abcd", "QUJD": "base64-looking", "spaces": "a b c"})
    try:
        c.start_all()
        for k, v in want.items():
            c.put(k, v)
        c.put("gone", "x")
        with _any_leader_client(c) as cl:
            cl.delete("gone")
        # v1.0 acknowledged writes before followers applied them; let
        # replication settle before stopping.
        time.sleep(1)
        for n in c.nodes.values():
            n.stop()
        c.start_all(binary=binary)
        for n in c.live():
            for k in ("k0", "k40", "k79", "user", "QUJD", "spaces"):
                expect_value(n, k, want[k].encode(), timeout=20)
            expect_value(n, "gone", None)
        c.put("new", "binary")
        for n in c.live():
            expect_value(n, "new", b"binary")
    except BaseException:
        c.cleanup(failed=True)
        raise
    c.cleanup()


def _any_leader_client(c):
    """A client connected to the node that accepts writes, found by probing
    (old releases have no MEMBERS)."""
    for n in c.live():
        cl = n.client()
        try:
            cl.put("__probe", "1")
            return cl
        except KVError:
            cl.close()
    raise AssertionError("no node accepted a write")


# --- runner ----------------------------------------------------------------


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bin", required=True, help="kvengine binary under test")
    ap.add_argument("--old-bin", help="an older kvengine release, for upgrade scenarios")
    ap.add_argument("scenarios", nargs="*", help="scenarios to run (default: all). One of: " + ", ".join(SCENARIOS))
    args = ap.parse_args()

    names = args.scenarios or list(SCENARIOS)
    unknown = [n for n in names if n not in SCENARIOS]
    if unknown:
        ap.error("unknown scenario(s): %s" % ", ".join(unknown))

    binary = os.path.abspath(args.bin)
    old = os.path.abspath(args.old_bin) if args.old_bin else None
    failed = []
    for name in names:
        fn = SCENARIOS[name]
        if fn.needs_old and not old:
            print("SKIP %s (needs --old-bin)" % name)
            continue
        print("RUN  %s" % name)
        t0 = time.monotonic()
        try:
            fn(binary, old)
            print("PASS %s (%.1fs)" % (name, time.monotonic() - t0))
        except Exception:
            traceback.print_exc()
            print("FAIL %s (%.1fs)" % (name, time.monotonic() - t0))
            failed.append(name)
    if failed:
        print("\n%d failed: %s" % (len(failed), ", ".join(failed)))
        sys.exit(1)
    print("\nall passed")


if __name__ == "__main__":
    main()
