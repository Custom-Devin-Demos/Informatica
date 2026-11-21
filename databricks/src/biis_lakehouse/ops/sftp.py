"""Replacement for the 7 ``Transfer Scripts/*`` ksh scripts (``sftp sa-cdirect@m1csv301 ... put``)."""
from __future__ import annotations

import io
import posixpath
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional, Protocol

from biis_lakehouse.context import RunContext
from biis_lakehouse.ops.notify import send_email
from biis_lakehouse.ops.secret_scope import get_secret


@dataclass(frozen=True)
class TransferProfile:
    script: str  # original script under Transfer Scripts/
    area: str  # local directory: <landing_root>/out/<area> (was /data/BIISINT/data/int/out/<area>)
    remote_dir: str  # `cd` target on the SFTP server


PROFILES = {
    "afps_transfer": TransferProfile("afps_transfer", "CPM", "/opt/app/jail/sa-afps/outbound"),
    "cdc_transfer": TransferProfile("cdc_transfer", "CPM", "/opt/app/jail/sa-cdcusr/outbound"),
    "fda_transfer": TransferProfile("fda_transfer", "CPM", "/opt/app/jail/sa-fdausr2/outbound"),
    "nih_cpm_transfer": TransferProfile("nih_cpm_transfer", "CPM", "/opt/app/jail/sa-nihbiisu/outbound"),
    "nih_les_transfer": TransferProfile("nih_les_transfer", "LES", "/opt/app/jail/sa-nihbiisu/outbound"),
    "nih_transfer_les": TransferProfile("nih_transfer_les", "LES", "/opt/app/jail/sa-nihbiisu/outbound"),
    "oig_transfer": TransferProfile("oig_transfer", "CPM", "/opt/app/jail/sa-oig/outbound"),
}


class SftpClient(Protocol):
    def put(self, localpath: str, remotepath: str): ...

    def close(self) -> None: ...


ClientFactory = Callable[[RunContext], SftpClient]


class _ParamikoClient:
    def __init__(self, ssh, sftp):
        self._ssh, self._sftp = ssh, sftp

    def put(self, localpath: str, remotepath: str):
        return self._sftp.put(localpath, remotepath, confirm=True)

    def close(self) -> None:
        self._sftp.close()
        self._ssh.close()


def _load_private_key(paramiko, text: str, passphrase: Optional[str]):
    errors = []
    for cls in (paramiko.Ed25519Key, paramiko.ECDSAKey, paramiko.RSAKey):
        try:
            return cls.from_private_key(io.StringIO(text), password=passphrase)
        except paramiko.SSHException as e:  # wrong key type, try next
            errors.append(f"{cls.__name__}: {e}")
    raise ValueError("unsupported SFTP private key: " + "; ".join(errors))


def paramiko_client(ctx: RunContext) -> SftpClient:
    """Key-based SFTP with strict host-key checking; key + known_hosts line from secret scope ``biis``."""
    import paramiko

    host = ctx.param("SFTP_HOST")
    port = int(ctx.params.get("SFTP_PORT") or 22)
    passphrase = None
    if ctx.params.get("SFTP_KEY_HAS_PASSPHRASE", "").lower() == "true":
        passphrase = get_secret(ctx, "sftp_private_key_passphrase")
    pkey = _load_private_key(paramiko, get_secret(ctx, "sftp_private_key"), passphrase)

    ssh = paramiko.SSHClient()
    for line in get_secret(ctx, "sftp_known_hosts").splitlines():
        entry = paramiko.hostkeys.HostKeyEntry.from_line(line)
        if entry is not None:
            for name in entry.hostnames:
                ssh.get_host_keys().add(name, entry.key.get_name(), entry.key)
    ssh.set_missing_host_key_policy(paramiko.RejectPolicy())
    ssh.connect(host, port=port, username=ctx.param("SFTP_USER"), pkey=pkey, look_for_keys=False, allow_agent=False)
    return _ParamikoClient(ssh, ssh.open_sftp())


_client_factory: ClientFactory = paramiko_client


def set_client_factory(factory: Optional[ClientFactory]) -> None:
    global _client_factory
    _client_factory = factory or paramiko_client


def transfer(ctx: RunContext, script: str, filename: str, client_factory: Optional[ClientFactory] = None) -> str:
    """``<script>.sh <filename>``: put ``<landing>/out/<area>/<filename>`` into the profile's remote outbound dir.

    The ksh scripts mailed "Aborting: File not found" and exited 0; here a missing file raises so the job task
    fails and the job's failure notification fires.
    """
    profile = PROFILES[script]
    if not filename:
        raise ValueError(f"Usage: {script}.sh <filename>")
    local = ctx.path("out", profile.area, filename)
    if not Path(local).exists():
        raise FileNotFoundError(f"Aborting:File {local} not found!")
    remote = posixpath.join(profile.remote_dir, filename)
    client = (client_factory or _client_factory)(ctx)
    try:
        client.put(local, remote)
    finally:
        client.close()
    host = ctx.params.get("SFTP_HOST", "")
    send_email(
        ctx,
        ctx.params.get("TRANSFER_EMAIL_LIST", ""),
        f"File {local} transfered to {host} server sucessfully",
        f"Transfer of {local} to {host}:{remote} completed.",
    )
    return remote
