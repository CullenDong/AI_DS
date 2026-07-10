"""Redshift connectivity for AI_DS agent tools.

Ported from aws-example (src/bituslabs_ds/etl.py + config.py). Supports optional
SSH bastion tunneling via paramiko and enforces read-only queries.
"""
from __future__ import annotations

import logging
import os
import re
import socket
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Optional, Sequence

import pandas as pd

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


# ---------------- .env loading ----------------
def _maybe_load_dotenv() -> None:
    """Load `.env` from CWD or repo root once at import time."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    repo_root = Path(__file__).resolve().parent.parent
    for p in (Path.cwd() / ".env", repo_root / ".env"):
        if p.exists():
            load_dotenv(dotenv_path=str(p), override=False)
            return


_maybe_load_dotenv()


# ---------------- Config ----------------
REDSHIFT_HOST = os.environ.get(
    "REDSHIFT_HOST",
    "production-redshift-cluster.cwiqzcm13zcn.ap-southeast-1.redshift.amazonaws.com",
)
REDSHIFT_PORT = int(os.environ.get("REDSHIFT_PORT", "5439"))
DEFAULT_BASTION_IP = os.environ.get("REDSHIFT_BASTION_IP", "13.215.212.244")


def get_redshift_user() -> str:
    v = os.environ.get("REDSHIFT_USER")
    if not v:
        raise RuntimeError("REDSHIFT_USER env var required. Add to .env or export.")
    return v


def get_redshift_password() -> str:
    v = os.environ.get("REDSHIFT_PASSWORD")
    if not v:
        raise RuntimeError("REDSHIFT_PASSWORD env var required. Add to .env or export.")
    return v


_BASTION_KEY_FILE: Optional[str] = None


def _get_bastion_key_path() -> Optional[str]:
    """Resolve bastion SSH key from BASTION_KEY_PATH (file) or BASTION_KEY_CONTENT (raw PEM)."""
    global _BASTION_KEY_FILE
    path = os.environ.get("BASTION_KEY_PATH")
    if path:
        path = os.path.expanduser(path)
        if os.path.isfile(path):
            return path
    content = os.environ.get("BASTION_KEY_CONTENT")
    if content:
        if _BASTION_KEY_FILE is None:
            fd, _BASTION_KEY_FILE = tempfile.mkstemp(suffix=".pem")
            os.write(fd, content.encode() if isinstance(content, str) else content)
            os.close(fd)
            os.chmod(_BASTION_KEY_FILE, 0o600)
        return _BASTION_KEY_FILE
    return None


# ---------------- SSH tunnel helpers ----------------
def _shuttle_data(source, destination):
    try:
        while True:
            data = source.recv(1024)
            if not data:
                break
            destination.sendall(data)
    except Exception:
        pass
    finally:
        if hasattr(source, "close"):
            source.close()
        if hasattr(destination, "close"):
            destination.close()


def _forward_tunnel(local_port, remote_host, remote_port, transport, stop_event):
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.settimeout(1.0)
        sock.bind(("127.0.0.1", local_port))
        sock.listen(5)

        while not stop_event.is_set():
            try:
                conn, addr = sock.accept()
            except socket.timeout:
                continue
            chan = transport.open_channel(
                "direct-tcpip", (remote_host, remote_port), (addr[0], addr[1])
            )
            if chan is None:
                conn.close()
                continue
            threading.Thread(target=_shuttle_data, args=(conn, chan), daemon=True).start()
            threading.Thread(target=_shuttle_data, args=(chan, conn), daemon=True).start()
    except Exception as e:
        logger.warning(f"SSH tunnel listener failed on port {local_port}: {e}")
        if sock:
            sock.close()


# ---------------- Redshift backend ----------------
class RedshiftBackend:
    """Read-only Redshift connection with optional SSH bastion tunnel."""

    WRITE_KEYWORDS = re.compile(
        r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|ALTER|TRUNCATE)\b", re.IGNORECASE
    )

    def __init__(
        self,
        database: str,
        host: str = REDSHIFT_HOST,
        port: int = REDSHIFT_PORT,
        user: Optional[str] = None,
        password: Optional[str] = None,
        bastion_ip: Optional[str] = None,
        bastion_user: str = "ubuntu",
        local_port: int = 5433,
    ):
        self.conn = None
        self.ssh = None
        self.tunnel_thread = None
        self.host = host
        self.database = database
        self.user = user or get_redshift_user()
        self.password = password or get_redshift_password()
        self.port = port
        self.bastion_ip = bastion_ip
        self.bastion_user = bastion_user
        self.local_port = local_port
        self._stop_tunnel = threading.Event()

    def _check_query(self, query: str) -> None:
        query_lines = [
            line for line in query.splitlines()
            if not line.lstrip().startswith("--") and not line.lstrip().startswith("#")
        ]
        stripped = "\n".join(query_lines)
        if self.WRITE_KEYWORDS.search(stripped):
            raise RuntimeError("RedshiftBackend is read-only. Write queries are not allowed.")

    def connect(self):
        import redshift_connector

        if self.conn is not None:
            return self.conn

        if self.bastion_ip:
            import paramiko

            ssh_pkey = _get_bastion_key_path()
            if not ssh_pkey:
                raise RuntimeError(
                    "Bastion key required when using bastion_ip. Set BASTION_KEY_PATH or "
                    "BASTION_KEY_CONTENT. Omit bastion_ip for direct connect."
                )
            logger.info(f"Establishing SSH tunnel via {self.bastion_ip}")
            self.ssh = paramiko.SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self._stop_tunnel.clear()
            self.ssh.connect(
                hostname=self.bastion_ip,
                username=self.bastion_user,
                key_filename=ssh_pkey,
                timeout=10,
            )
            self.tunnel_thread = threading.Thread(
                target=_forward_tunnel,
                args=(self.local_port, self.host, self.port, self.ssh.get_transport(), self._stop_tunnel),
                daemon=True,
            )
            self.tunnel_thread.start()
            time.sleep(1)
            host, port = "127.0.0.1", self.local_port
        else:
            host, port = self.host, self.port

        logger.info(f"Connecting to Redshift host={host} port={port} db={self.database}")
        self.conn = redshift_connector.connect(
            host=host,
            port=port,
            database=self.database,
            user=self.user,
            password=self.password,
            ssl=True,
        )
        return self.conn

    def execute(self, query: str, params: Optional[Sequence[Any]] = None) -> Sequence[Any]:
        self._check_query(query)
        conn = self.connect()
        cursor = conn.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall() if cursor.description else []
        finally:
            cursor.close()

    def query_to_df(self, query: str) -> pd.DataFrame:
        self._check_query(query)
        conn = self.connect()
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            return cursor.fetch_dataframe()
        finally:
            cursor.close()

    def close(self) -> None:
        self._stop_tunnel.set()
        if self.conn:
            self.conn.close()
            self.conn = None
        if self.ssh:
            self.ssh.close()
            self.ssh = None
        time.sleep(1)


# ---------------- Cached backend for agent tool reuse ----------------
_CACHED_BACKEND: Optional[RedshiftBackend] = None
_CACHED_KEY: Optional[tuple] = None


def _get_backend(database: str, bastion_ip: Optional[str]) -> RedshiftBackend:
    """Return a cached backend for the given (database, bastion_ip). Reconnects on change."""
    global _CACHED_BACKEND, _CACHED_KEY
    key = (database, bastion_ip)
    if _CACHED_BACKEND is not None and _CACHED_KEY == key:
        return _CACHED_BACKEND
    if _CACHED_BACKEND is not None:
        _CACHED_BACKEND.close()
    _CACHED_BACKEND = RedshiftBackend(database=database, bastion_ip=bastion_ip)
    _CACHED_KEY = key
    return _CACHED_BACKEND
