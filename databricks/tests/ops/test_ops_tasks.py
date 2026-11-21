from pathlib import Path

import pytest

from biis_lakehouse.context import RunContext
from biis_lakehouse.ops import notify, sftp, tasks
from biis_lakehouse.runner import load_tasks


class FakeSftp:
    def __init__(self):
        self.puts, self.closed = [], False

    def put(self, localpath, remotepath):
        self.puts.append((localpath, remotepath, Path(localpath).read_text()))

    def close(self):
        self.closed = True


class FakeSmtp:
    sent = []

    def __init__(self, host, port):
        self.host = host

    def send_message(self, msg):
        FakeSmtp.sent.append(msg)

    def quit(self):
        pass


@pytest.fixture
def fakes():
    client = FakeSftp()
    sftp.set_client_factory(lambda ctx: client)
    FakeSmtp.sent = []
    notify.set_smtp_factory(FakeSmtp)
    yield client
    sftp.set_client_factory(None)
    notify.set_smtp_factory(None)


def mkctx(tmp_path, **params):
    base = {"SMTP_HOST": "smtp.test", "SFTP_HOST": "sftp.test", "TRANSFER_EMAIL_LIST": "ops@example.com"}
    return RunContext(spark=None, landing_root=str(tmp_path), params={**base, **params})


def write(path: Path, text: str = "x") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


def test_registered_in_runner():
    assert load_tasks("ops") is tasks.TASKS
    assert len({c.task_key for c in tasks.SESSION_COMMANDS}) == len(tasks.SESSION_COMMANDS)


def test_comptime_post_archives_with_pay_period(tmp_path):
    write(tmp_path / "in/COMPTIME/u0827d01.txt", "comp")
    ctx = mkctx(tmp_path, COMPTIME_filename="u0827d01.txt", WF_PP_YEAR_NUM="201907")
    tasks.TASKS["s_COMPTIME_Build_Message_Counters__post"](ctx)
    assert (tmp_path / "archive/COMPTIME/u0827d01_P201907.txt").read_text() == "comp"
    assert not (tmp_path / "in/COMPTIME/u0827d01.txt").exists()


def test_cpm_cdc_pre_concat_and_transfer(tmp_path, fakes):
    out = tmp_path / "out/CPM"
    write(out / "cdc.out", "stale")
    ctx = mkctx(tmp_path, CPM_CDC_HDR_filename="cdc.hdr", CPM_CDC_DAT_filename="cdc.dat",
                CPM_CDC_out_filename="cdc.out", WF_PP_YEAR_NUM="201907")
    tasks.TASKS["s_CPM_CDC_Concatenate_Files__pre"](ctx)
    assert not (out / "cdc.out").exists()
    write(out / "cdc.hdr", "H\n")
    write(out / "cdc.dat", "D\n")
    tasks.TASKS["s_CPM_CDC_Concatenate_Files__post"](ctx)
    assert (out / "cdc.out").read_text() == "H\nD\n"
    tasks.TASKS["s_CPM_CDC_Build_Message__post"](ctx)
    assert fakes.puts == [(str(out / "CDC.CPM.PP201907"), "/opt/app/jail/sa-cdcusr/outbound/CDC.CPM.PP201907", "H\nD\n")]
    assert fakes.closed
    assert FakeSmtp.sent[-1]["To"] == "ops@example.com"


def test_afps_post_full_sequence(tmp_path, fakes):
    write(tmp_path / "out/CPM/afps.out", "afps")
    write(tmp_path / "in/CPM/gprecon.txt", "gp")
    write(tmp_path / "in/CPM/ddform.txt", "dd")
    write(tmp_path / "out/CPM/crossfoot_report.txt", "CROSSFOOT")
    write(tmp_path / "out/CPM/cpm_afps_counters.txt", "COUNTERS")
    ctx = mkctx(tmp_path, CPM_AFPS_out_filename="afps.out", CPM_GPRecon_filename="gprecon.txt",
                CPM_DDForm_filename="ddform.txt", WF_PP_NUM="07", WF_PP_END_YEAR_LAST_DIGIT="9",
                WF_PP_YEAR_NUM="201907", WF_CPM_EMAIL_LIST="a@example.com b@example.com",
                WF_SUBJECT="AFPS done", WF_MESSAGE="msg", FAIL_TASK_ON_COMMAND_ERROR="true")
    tasks.TASKS["s_CPM_AFPS_1000_Send_Report__post"](ctx)
    assert [r for _, r, _ in fakes.puts] == [
        "/opt/app/jail/sa-afps/outbound/AFPS_FLAT_PP07.txt",
        "/opt/app/jail/sa-afps/outbound/P3306D04.GPRECON.P907",
        "/opt/app/jail/sa-afps/outbound/P6615X01.P907",
    ]
    assert (tmp_path / "archive/CPM/p3306d04_P201907.txt").read_text() == "gp"
    assert (tmp_path / "archive/CPM/p6615x01_P201907.txt").read_text() == "dd"
    report = FakeSmtp.sent[-1]
    assert report["To"] == "a@example.com, b@example.com" and report["Subject"] == "AFPS done"
    assert "CROSSFOOT" in report.get_content() and "COUNTERS" in report.get_content()


def test_command_failure_semantics(tmp_path, fakes):
    """'Fail task if any command fails = NO': later commands still run and the task succeeds unless configured."""
    write(tmp_path / "in/PSEUDO/backup.txt", "b")
    ctx = mkctx(tmp_path, PSEUDOSSN_file="missing.txt", PSEUDOSSN_Backup_file="backup.txt", WF_PP_YEAR_NUM="201907")
    tasks.TASKS["s_Pseudossn_Counters__post"](ctx)
    assert (tmp_path / "archive/PSEUDO/bkup.pseudssn.P201907.txt").read_text() == "b"

    ctx.params["FAIL_TASK_ON_COMMAND_ERROR"] = "true"
    with pytest.raises(RuntimeError, match="missing.txt"):
        tasks.TASKS["s_Pseudossn_Counters__post"](ctx)


def test_transfer_missing_file_and_usage(tmp_path, fakes):
    ctx = mkctx(tmp_path, FILENAME="nope.txt")
    with pytest.raises(FileNotFoundError, match="Aborting"):
        tasks.TASKS["oig_transfer"](ctx)
    with pytest.raises(ValueError, match="Usage"):
        sftp.transfer(ctx, "oig_transfer", "")
    assert fakes.puts == []


@pytest.mark.parametrize("script,area,remote", [
    ("afps_transfer", "CPM", "/opt/app/jail/sa-afps/outbound"),
    ("cdc_transfer", "CPM", "/opt/app/jail/sa-cdcusr/outbound"),
    ("fda_transfer", "CPM", "/opt/app/jail/sa-fdausr2/outbound"),
    ("nih_cpm_transfer", "CPM", "/opt/app/jail/sa-nihbiisu/outbound"),
    ("nih_les_transfer", "LES", "/opt/app/jail/sa-nihbiisu/outbound"),
    ("nih_transfer_les", "LES", "/opt/app/jail/sa-nihbiisu/outbound"),
    ("oig_transfer", "CPM", "/opt/app/jail/sa-oig/outbound"),
])
def test_standalone_transfer_tasks(tmp_path, fakes, script, area, remote):
    write(tmp_path / "out" / area / "F.TXT", "data")
    tasks.TASKS[script](mkctx(tmp_path, FILENAME="F.TXT"))
    assert fakes.puts == [(str(tmp_path / "out" / area / "F.TXT"), f"{remote}/F.TXT", "data")]


def test_transfer_profiles_match_scripts():
    root = Path(__file__).resolve().parents[3] / "Transfer Scripts"
    for script, profile in sftp.PROFILES.items():
        text = (root / script).read_text()
        assert f"cd {profile.remote_dir}\n" in text
        assert f"/data/BIISINT/data/int/out/{profile.area}/$1" in text


def test_workflow_email_task(tmp_path, fakes):
    ctx = mkctx(tmp_path, WF_LES_EMAIL_LIST="les@example.com", WF_SUBJECT="LES PP07", WF_MESSAGE="rows=10")
    tasks.TASKS["email_LES_Complete"](ctx)
    msg = FakeSmtp.sent[-1]
    assert (msg["To"], msg["Subject"], msg.get_content().strip()) == ("les@example.com", "LES PP07", "rows=10")


def test_email_without_smtp_only_logs(tmp_path, capsys):
    ctx = RunContext(spark=None, landing_root=str(tmp_path), params={"WF_SUBJECT": "s", "WF_MESSAGE": "m",
                                                                     "WF_PSEUDOSSN_EMAIL_LIST": "x@example.com"})
    tasks.TASKS["Email_Pseudossn"](ctx)
    assert "email not sent" in capsys.readouterr().out


def test_paramiko_client_uses_secret_scope_key(tmp_path, monkeypatch):
    paramiko = pytest.importorskip("paramiko")
    from io import StringIO

    from biis_lakehouse.ops import secret_scope

    key = paramiko.RSAKey.generate(1024)
    buf = StringIO()
    key.write_private_key(buf)
    host_key = paramiko.RSAKey.generate(1024)
    secrets = {"sftp_private_key": buf.getvalue(),
               "sftp_known_hosts": f"sftp.test {host_key.get_name()} {host_key.get_base64()}"}
    asked = []
    secret_scope.set_secret_getter(lambda ctx, scope, k: asked.append((scope, k)) or secrets[k])
    connected = {}

    class FakeSSH:
        def __init__(self):
            self.keys = paramiko.HostKeys()

        def get_host_keys(self):
            return self.keys

        def set_missing_host_key_policy(self, policy):
            connected["policy"] = type(policy).__name__

        def connect(self, host, **kw):
            connected.update(host=host, **kw)

        def open_sftp(self):
            return FakeSftp()

        def close(self):
            pass

    monkeypatch.setattr(paramiko, "SSHClient", FakeSSH)
    try:
        client = sftp.paramiko_client(mkctx(tmp_path, SFTP_USER="sa-cdirect"))
    finally:
        secret_scope.set_secret_getter(None)
    assert ("biis", "sftp_private_key") in asked
    assert connected["host"] == "sftp.test" and connected["username"] == "sa-cdirect"
    assert connected["pkey"].get_base64() == key.get_base64()
    assert connected["policy"] == "RejectPolicy" and connected["look_for_keys"] is False
    client.close()
