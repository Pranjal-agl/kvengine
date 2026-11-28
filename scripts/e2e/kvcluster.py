"""Helpers for driving real kvengine processes in end-to-end tests.

Standard library only, so the tests run anywhere Python 3.8+ is installed.
"""

import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time


class KVError(Exception):
    """The server answered -ERR."""


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Client:
    """A connection speaking the kvengine text protocol."""

    def __init__(self, addr, timeout=5.0):
        host, port = addr.rsplit(":", 1)
        self.sock = socket.create_connection((host, int(port)), timeout=timeout)
        self.buf = b""

    def close(self):
        self.sock.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _fill(self):
        chunk = self.sock.recv(65536)
        if not chunk:
            raise ConnectionError("server closed the connection")
        self.buf += chunk

    def _line(self):
        while b"\r\n" not in self.buf:
            self._fill()
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _exact(self, n):
        while len(self.buf) < n:
            self._fill()
        data, self.buf = self.buf[:n], self.buf[n:]
        return data

    def _reply(self):
        line = self._line()
        if line.startswith(b"+"):
            return line[1:].decode()
        if line.startswith(b"-"):
            raise KVError(line[1:].decode(errors="replace"))
        if line.startswith(b"$"):
            n = int(line[1:])
            if n < 0:
                return None
            data = self._exact(n)
            if self._exact(2) != b"\r\n":
                raise ValueError("bulk reply not terminated by CRLF")
            return data
        raise ValueError("unexpected reply %r" % line)

    def put(self, key, value):
        if isinstance(key, str):
            key = key.encode()
        if isinstance(value, str):
            value = value.encode()
        self.sock.sendall(b"PUT %s %d\r\n%s\r\n" % (key, len(value), value))
        return self._reply()

    def get(self, key):
        if isinstance(key, str):
            key = key.encode()
        self.sock.sendall(b"GET %s\r\n" % key)
        return self._reply()

    def delete(self, key):
        if isinstance(key, str):
            key = key.encode()
        self.sock.sendall(b"DEL %s\r\n" % key)
        return self._reply()

    def admin(self, *words):
        self.sock.sendall((" ".join(words) + "\r\n").encode())
        return self._reply()


class Node:
    def __init__(self, cluster, node_id):
        self.cluster = cluster
        self.id = node_id
        self.raft_addr = "127.0.0.1:%d" % free_port()
        self.kv_addr = "127.0.0.1:%d" % free_port()
        self.data_dir = os.path.join(cluster.root, node_id)
        self.proc = None
        self.log = None
        self.binary = cluster.binary

    @property
    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def start(self, peers=None, join=False, extra=(), binary=None):
        """Start the node. peers maps id -> raft addr (excluding this node)."""
        assert not self.alive, "%s already running" % self.id
        if binary:
            self.binary = binary
        os.makedirs(self.data_dir, exist_ok=True)
        args = [
            self.binary, "raft",
            "--id", self.id,
            "--raft-addr", self.raft_addr,
            "--kv-addr", self.kv_addr,
            "--data-dir", self.data_dir,
        ]
        if peers:
            args += ["--peers", ",".join("%s=%s" % kv for kv in sorted(peers.items()))]
        if join:
            args.append("--join")
        args += list(extra)
        self.log = open(os.path.join(self.cluster.root, self.id + ".log"), "ab")
        env = dict(os.environ, KVENGINE_PPROF="off")
        self.proc = subprocess.Popen(args, stdout=self.log, stderr=subprocess.STDOUT, env=env)
        wait_until(lambda: self._kv_port_open(), 10, "%s KV port to open" % self.id)

    def _kv_port_open(self):
        if not self.alive:
            raise RuntimeError("%s exited early (code %s); see %s.log"
                               % (self.id, self.proc.returncode, self.id))
        try:
            socket.create_connection(self.kv_addr.rsplit(":", 1), timeout=0.2).close()
            return True
        except OSError:
            return False

    def stop(self, sig=signal.SIGTERM, timeout=15):
        """Stop the node; returns the exit code."""
        if not self.alive:
            return self.proc.returncode if self.proc else None
        self.proc.send_signal(sig)
        try:
            code = self.proc.wait(timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            code = self.proc.wait()
            raise AssertionError("%s did not exit within %ss of %s"
                                 % (self.id, timeout, signal.Signals(sig).name))
        finally:
            self.log.close()
        return code

    def kill(self):
        return self.stop(signal.SIGKILL)

    def client(self, timeout=5.0):
        return Client(self.kv_addr, timeout)

    def members(self):
        """Returns (members dict id->addr, leader id or None) as this node sees them."""
        with self.client() as c:
            text = c.admin("MEMBERS").decode()
        members, leader = {}, None
        for line in text.splitlines():
            parts = line.split()
            if not parts:
                continue
            members[parts[0]] = parts[1]
            if len(parts) > 2 and parts[2] == "leader":
                leader = parts[0]
        return members, leader


class Cluster:
    """A set of kvengine nodes under one temporary directory."""

    def __init__(self, binary, ids=(), keep=False):
        self.binary = os.path.abspath(binary)
        self.root = tempfile.mkdtemp(prefix="kvengine-e2e-")
        self.keep = keep
        self.nodes = {}
        for node_id in ids:
            self.add(node_id)

    def add(self, node_id):
        n = Node(self, node_id)
        self.nodes[node_id] = n
        return n

    def __getitem__(self, node_id):
        return self.nodes[node_id]

    def peers_of(self, node_id, among=None):
        among = among if among is not None else self.nodes.keys()
        return {i: self.nodes[i].raft_addr for i in among if i != node_id}

    def start_all(self, extra=(), binary=None):
        ids = list(self.nodes)
        for i in ids:
            self.nodes[i].start(peers=self.peers_of(i, ids), extra=extra, binary=binary)

    def live(self):
        return [n for n in self.nodes.values() if n.alive]

    def leader(self, timeout=10):
        """Waits for a live node that considers itself leader and returns it."""
        found = {}

        def check():
            for n in self.live():
                try:
                    _, lead = n.members()
                except (OSError, KVError, ConnectionError):
                    continue
                if lead == n.id:
                    found["n"] = n
                    return True
            return False

        wait_until(check, timeout, "a leader to be elected")
        return found["n"]

    def put(self, key, value, timeout=10):
        """Puts through whichever node accepts it (the leader), retrying
        across elections. Doesn't rely on MEMBERS, so it also works against
        old releases that lack it."""
        last = {}

        def attempt():
            for n in self.live():
                try:
                    with n.client() as c:
                        c.put(key, value)
                    return True
                except (OSError, KVError, ConnectionError) as e:
                    last["e"] = e
            return False

        try:
            wait_until(attempt, timeout, "put %r to succeed" % key)
        except AssertionError as e:
            raise AssertionError("%s (last error: %s)" % (e, last.get("e")))

    def stop_all(self):
        for n in self.nodes.values():
            if n.alive:
                try:
                    n.stop()
                except AssertionError:
                    pass

    def cleanup(self, failed=False):
        self.stop_all()
        if failed or self.keep:
            print("    logs and data kept in", self.root)
        else:
            shutil.rmtree(self.root, ignore_errors=True)


def wait_until(cond, timeout, what):
    deadline = time.monotonic() + timeout
    while True:
        if cond():
            return
        if time.monotonic() > deadline:
            raise AssertionError("timed out after %ss waiting for %s" % (timeout, what))
        time.sleep(0.05)


def eventually(fn, timeout=10, what="condition"):
    """Retries fn until it stops raising; returns its result."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            return fn()
        except (AssertionError, OSError, KVError, ConnectionError) as e:
            if time.monotonic() > deadline:
                raise AssertionError("timed out after %ss waiting for %s: %s" % (timeout, what, e))
            time.sleep(0.05)
