"""``biis-run --workflow ops --task <key>``: PowerCenter session pre/post commands and Email tasks.

Each :class:`SessionCommands` reproduces one session's ``Pre-session command`` or ``Post-session success command``
component, step for step in ``EXECORDER``. ``xml_command`` keeps the original command for traceability (checked
against the XML in ``tests/ops``). Paths map ``$Param_Root_Directory/data/int/<in|out>/<AREA>`` to
``ctx.path("<in|out>", AREA)`` and ``$Param_Root_Directory/data/archive/<AREA>`` to ``ctx.path("archive", AREA)``.
Templates use PowerCenter parameter names without the ``$$`` / ``$Param_`` prefix, e.g. ``{WF_PP_YEAR_NUM}``.
"""
from __future__ import annotations

import string
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from biis_lakehouse.context import RunContext, Task
from biis_lakehouse.ops import files, notify, sftp


def render(ctx: RunContext, template: str) -> str:
    out = []
    for literal, field, _spec, _conv in string.Formatter().parse(template):
        out.append(literal)
        if field is not None:
            out.append(ctx.param(field))
    return "".join(out)


def _path(ctx: RunContext, parts: tuple[str, ...]) -> str:
    return ctx.path(*(render(ctx, p) for p in parts))


@dataclass(frozen=True)
class Step:
    xml_command: str

    def run(self, ctx: RunContext) -> None:  # pragma: no cover - abstract
        raise NotImplementedError


@dataclass(frozen=True)
class Move(Step):
    src: tuple[str, ...]
    dst: tuple[str, ...]

    def run(self, ctx):
        files.move(_path(ctx, self.src), _path(ctx, self.dst))


@dataclass(frozen=True)
class Copy(Step):
    src: tuple[str, ...]
    dst: tuple[str, ...]

    def run(self, ctx):
        files.copy(_path(ctx, self.src), _path(ctx, self.dst))


@dataclass(frozen=True)
class MoveGlob(Step):
    src_dir: tuple[str, ...]
    pattern: str
    dst_dir: tuple[str, ...]

    def run(self, ctx):
        files.move_glob(_path(ctx, self.src_dir), self.pattern, _path(ctx, self.dst_dir))


@dataclass(frozen=True)
class Concat(Step):
    dir: tuple[str, ...]
    srcs: tuple[str, ...]
    dst: str

    def run(self, ctx):
        d = _path(ctx, self.dir)
        files.concat([str(Path(d, render(ctx, s))) for s in self.srcs], str(Path(d, render(ctx, self.dst))))


@dataclass(frozen=True)
class RemoveFile(Step):
    dir: tuple[str, ...]
    filename: str

    def run(self, ctx):
        files.remove_file(_path(ctx, self.dir), render(ctx, self.filename))


@dataclass(frozen=True)
class ArchiveFiles(Step):
    input_dir: tuple[str, ...]
    dest_dir: tuple[str, ...]
    pay_period: str

    def run(self, ctx):
        files.archive_files(_path(ctx, self.input_dir), _path(ctx, self.dest_dir), render(ctx, self.pay_period))


@dataclass(frozen=True)
class Transfer(Step):
    script: str
    filename: str

    def run(self, ctx):
        sftp.transfer(ctx, self.script, render(ctx, self.filename))


@dataclass(frozen=True)
class Email(Step):
    """Session ``Success Email`` / workflow ``Email`` task. ``attachments`` (``%a<file>``) are appended to the body."""

    to: str
    subject: str
    text: str = ""
    attachments: tuple[tuple[str, ...], ...] = ()

    def run(self, ctx):
        body = [render(ctx, self.text)] if self.text else []
        for parts in self.attachments:
            body.append(Path(_path(ctx, parts)).read_text())
        notify.send_email(ctx, render(ctx, self.to), render(ctx, self.subject), "\n".join(body))


@dataclass(frozen=True)
class SessionCommands:
    workflow: str
    session: str
    phase: str  # "pre" (Pre-session command) or "post" (Post-session success command)
    steps: tuple[Step, ...]

    @property
    def task_key(self) -> str:
        return f"{self.session}__{self.phase}"

    def run(self, ctx: RunContext) -> Optional[dict]:
        """Run every step. Like PowerCenter with ``Fail task if any command fails = NO`` (every command component in
        the XML), a failing step is reported and the next one still runs; ``FAIL_TASK_ON_COMMAND_ERROR=true`` fails
        the task after all steps ran instead."""
        errors = []
        for step in self.steps:
            try:
                step.run(ctx)
            except Exception as e:  # noqa: BLE001 - mirror PowerCenter command semantics
                errors.append(f"{step.xml_command}: {type(e).__name__}: {e}")
                print(f"[biis ops] {self.task_key} command failed: {errors[-1]}", file=sys.stderr)
        if errors and ctx.params.get("FAIL_TASK_ON_COMMAND_ERROR", "false").lower() == "true":
            raise RuntimeError(f"{self.task_key}: {len(errors)} command(s) failed:\n" + "\n".join(errors))
        return None


R = "$Param_Root_Directory/data"
IN_CPM, OUT_CPM, ARCH_CPM = ("in", "CPM"), ("out", "CPM"), ("archive", "CPM")
_RM = "$Param_ControlDir/scripts/remove_file.sh $Param_Root_Directory/data/int/out/CPM $Param_{0}"


def _rm_out_cpm(param: str) -> RemoveFile:
    return RemoveFile(_RM.format(param), OUT_CPM, "{%s}" % param)


def _mv_in_to_archive(area: str, param: str, archived: str) -> Move:
    cmd = f"mv {R}/int/in/{area}/$Param_{param}  {R}/archive/{area}/{archived.replace('{', '$$').replace('}', '')}"
    return Move(cmd, ("in", area, "{%s}" % param), ("archive", area, archived))


SESSION_COMMANDS: tuple[SessionCommands, ...] = (
    # --- wf_COMPTIME
    SessionCommands("wf_COMPTIME", "s_COMPTIME_Build_Message_Counters", "post", (
        _mv_in_to_archive("COMPTIME", "COMPTIME_filename", "u0827d01_P{WF_PP_YEAR_NUM}.txt"),
    )),
    # --- wf_CPM
    SessionCommands("wf_CPM", "s_CPM_Send_Counts", "post", (
        _mv_in_to_archive("CPM", "CPM_Biweekly_MER_filename", "p6722d01_P{WF_PP_YEAR_NUM}.txt"),
        _mv_in_to_archive("CPM", "CPM_PAD_filename", "p6331x01_P{WF_PP_YEAR_NUM}.txt"),
        _mv_in_to_archive("CPM", "CPM_Department_of_Energy_filename", "p6310de2_P{WF_PP_YEAR_NUM}.txt"),
        _mv_in_to_archive("CPM", "CPM_Year_To_Date_filename", "p6791x01_P{WF_PP_YEAR_NUM}.txt"),
        Email("Success Email", "{WF_CPM_EMAIL_LIST}", "{WF_SUBJECT}", attachments=(OUT_CPM + ("cpm_message_file.txt",),)),
    )),
    # --- wf_CPM_AFPS
    SessionCommands("wf_CPM_AFPS", "s_CPM_AFPS_0200_Debridge_To_FEEDER_FLAT", "pre", (
        _rm_out_cpm("CPM_AFPS_out_filename"),
    )),
    SessionCommands("wf_CPM_AFPS", "s_CPM_AFPS_1000_Send_Report", "post", (
        Copy(f"cp {R}/int/out/CPM/$Param_CPM_AFPS_out_filename {R}/int/out/CPM/AFPS_FLAT_PP$$WF_PP_NUM.txt",
             OUT_CPM + ("{CPM_AFPS_out_filename}",), OUT_CPM + ("AFPS_FLAT_PP{WF_PP_NUM}.txt",)),
        Copy(f"cp {R}/int/in/CPM/$Param_CPM_GPRecon_filename "
             f"{R}/int/out/CPM/P3306D04.GPRECON.P$$WF_PP_END_YEAR_LAST_DIGIT$$WF_PP_NUM",
             IN_CPM + ("{CPM_GPRecon_filename}",), OUT_CPM + ("P3306D04.GPRECON.P{WF_PP_END_YEAR_LAST_DIGIT}{WF_PP_NUM}",)),
        Transfer("/home/sa-infoadm/scripts/afps_transfer.sh AFPS_FLAT_PP$$WF_PP_NUM.txt",
                 "afps_transfer", "AFPS_FLAT_PP{WF_PP_NUM}.txt"),
        Transfer("/home/sa-infoadm/scripts/afps_transfer.sh P3306D04.GPRECON.P$$WF_PP_END_YEAR_LAST_DIGIT$$WF_PP_NUM",
                 "afps_transfer", "P3306D04.GPRECON.P{WF_PP_END_YEAR_LAST_DIGIT}{WF_PP_NUM}"),
        Move(f"mv {R}/int/in/CPM/$Param_CPM_GPRecon_filename {R}/archive/CPM/p3306d04_P$$WF_PP_YEAR_NUM.txt",
             IN_CPM + ("{CPM_GPRecon_filename}",), ARCH_CPM + ("p3306d04_P{WF_PP_YEAR_NUM}.txt",)),
        Copy(f"cp {R}/int/in/CPM/$Param_CPM_DDForm_filename "
             f"{R}/int/out/CPM/P6615X01.P$$WF_PP_END_YEAR_LAST_DIGIT$$WF_PP_NUM",
             IN_CPM + ("{CPM_DDForm_filename}",), OUT_CPM + ("P6615X01.P{WF_PP_END_YEAR_LAST_DIGIT}{WF_PP_NUM}",)),
        Transfer("/home/sa-infoadm/scripts/afps_transfer.sh P6615X01.P$$WF_PP_END_YEAR_LAST_DIGIT$$WF_PP_NUM",
                 "afps_transfer", "P6615X01.P{WF_PP_END_YEAR_LAST_DIGIT}{WF_PP_NUM}"),
        Move(f"mv {R}/int/in/CPM/$Param_CPM_DDForm_filename {R}/archive/CPM/p6615x01_P$$WF_PP_YEAR_NUM.txt",
             IN_CPM + ("{CPM_DDForm_filename}",), ARCH_CPM + ("p6615x01_P{WF_PP_YEAR_NUM}.txt",)),
        # $PMTargetFileDir for the CPM folder is $Param_Root_Directory/data/int/out/CPM.
        Email("Success Email", "{WF_CPM_EMAIL_LIST}", "{WF_SUBJECT}", "{WF_MESSAGE}",
              (OUT_CPM + ("crossfoot_report.txt",), OUT_CPM + ("cpm_afps_counters.txt",))),
    )),
    # --- wf_CPM_CDC
    SessionCommands("wf_CPM_CDC", "s_CPM_CDC_Load_CPM_CDC_Header_File", "pre", (_rm_out_cpm("CPM_CDC_HDR_filename"),)),
    SessionCommands("wf_CPM_CDC", "s_CPM_CDC_Load_CPM_CDC_Data_File", "pre", (_rm_out_cpm("CPM_CDC_DAT_filename"),)),
    SessionCommands("wf_CPM_CDC", "s_CPM_CDC_Concatenate_Files", "pre", (_rm_out_cpm("CPM_CDC_out_filename"),)),
    SessionCommands("wf_CPM_CDC", "s_CPM_CDC_Concatenate_Files", "post", (
        Concat(f"cd {R}/int/out/CPM; cat $Param_CPM_CDC_HDR_filename $Param_CPM_CDC_DAT_filename > $Param_CPM_CDC_out_filename",
               OUT_CPM, ("{CPM_CDC_HDR_filename}", "{CPM_CDC_DAT_filename}"), "{CPM_CDC_out_filename}"),
    )),
    SessionCommands("wf_CPM_CDC", "s_CPM_CDC_Build_Message", "post", (
        Copy(f"cd {R}/int/out/CPM; cp $Param_CPM_CDC_out_filename CDC.CPM.PP$$WF_PP_YEAR_NUM",
             OUT_CPM + ("{CPM_CDC_out_filename}",), OUT_CPM + ("CDC.CPM.PP{WF_PP_YEAR_NUM}",)),
        Transfer("/home/sa-infoadm/scripts/cdc_transfer.sh CDC.CPM.PP$$WF_PP_YEAR_NUM", "cdc_transfer", "CDC.CPM.PP{WF_PP_YEAR_NUM}"),
    )),
    # --- wf_CPM_NIH
    SessionCommands("wf_CPM_NIH", "s_CPM_NIH_Load_CPM_NIH_Header_File", "pre", (_rm_out_cpm("CPM_NIH_HDR_filename"),)),
    SessionCommands("wf_CPM_NIH", "s_CPM_NIH_Load_CPM_NIH_Data_File", "pre", (_rm_out_cpm("CPM_NIH_DAT_filename"),)),
    SessionCommands("wf_CPM_NIH", "s_CPM_NIH_Concatenate_Files", "pre", (_rm_out_cpm("CPM_NIH_out_filename"),)),
    SessionCommands("wf_CPM_NIH", "s_CPM_NIH_Concatenate_Files", "post", (
        Concat(f"cd {R}/int/out/CPM; cat $Param_CPM_NIH_HDR_filename $Param_CPM_NIH_DAT_filename > $Param_CPM_NIH_out_filename",
               OUT_CPM, ("{CPM_NIH_HDR_filename}", "{CPM_NIH_DAT_filename}"), "{CPM_NIH_out_filename}"),
    )),
    SessionCommands("wf_CPM_NIH", "s_CPM_NIH_Build_Message", "post", (
        Copy(f"cd {R}/int/out/CPM; cp $Param_CPM_NIH_out_filename NIHPAYMASTER.PP$$WF_PP_YEAR_NUM",
             OUT_CPM + ("{CPM_NIH_out_filename}",), OUT_CPM + ("NIHPAYMASTER.PP{WF_PP_YEAR_NUM}",)),
        Transfer("/home/sa-infoadm/scripts/nih_cpm_transfer.sh NIHPAYMASTER.PP$$WF_PP_YEAR_NUM",
                 "nih_cpm_transfer", "NIHPAYMASTER.PP{WF_PP_YEAR_NUM}"),
    )),
    # --- wf_CPM_OIG
    SessionCommands("wf_CPM_OIG", "s_CPM_OIG_Load_CPM_OIG_File", "pre", (_rm_out_cpm("CPM_OIG_out_filename"),)),
    SessionCommands("wf_CPM_OIG", "s_CPM_OIG_Build_Message", "post", (
        Copy(f"cd {R}/int/out/CPM; cp $Param_CPM_OIG_out_filename OIG.CPM.PP$$WF_PP_YEAR_NUM.TXT",
             OUT_CPM + ("{CPM_OIG_out_filename}",), OUT_CPM + ("OIG.CPM.PP{WF_PP_YEAR_NUM}.TXT",)),
        Transfer("/home/sa-infoadm/scripts/oig_transfer.sh OIG.CPM.PP$$WF_PP_YEAR_NUM.TXT",
                 "oig_transfer", "OIG.CPM.PP{WF_PP_YEAR_NUM}.TXT"),
    )),
    # --- wf_FDA_Leave
    SessionCommands("wf_FDA_Leave", "s_1100_PM_FDA_Send_Email", "post", (
        Copy(f"cd {R}/int/out/CPM; cp $Param_FDA_12200_out_file FDA.TATRAN.PP$$WF_PP_NUM.TXT",
             OUT_CPM + ("{FDA_12200_out_file}",), OUT_CPM + ("FDA.TATRAN.PP{WF_PP_NUM}.TXT",)),
        Transfer("/home/sa-infoadm/scripts/fda_transfer.sh FDA.TATRAN.PP$$WF_PP_NUM.TXT", "fda_transfer", "FDA.TATRAN.PP{WF_PP_NUM}.TXT"),
        MoveGlob(f"cd {R}/int/in/CPM; mv tatran* {R}/archive/CPM/", IN_CPM, "tatran*", ARCH_CPM),
        Email("Success Email", "{WF_CPM_EMAIL_LIST}", "{WF_SUBJECT}", attachments=(OUT_CPM + ("fda_extract_message_file.txt",),)),
    )),
    # --- wf_LES
    SessionCommands("wf_LES", "s_LES_Build_Message_Counters", "post", (
        Copy(f"cd {R}/int/out/LES; cp $Param_LES_NIH_emp_detail NIHLES.P$$WF_PP_YEAR_NUM",
             ("out", "LES", "{LES_NIH_emp_detail}"), ("out", "LES", "NIHLES.P{WF_PP_YEAR_NUM}")),
        Transfer("/home/sa-infoadm/scripts/nih_les_transfer.sh NIHLES.P$$WF_PP_YEAR_NUM", "nih_les_transfer", "NIHLES.P{WF_PP_YEAR_NUM}"),
        ArchiveFiles(f"$Param_ControlDir/scripts/archive_files.sh {R}/int/in/LES {R}/archive/LES $$WF_PP_YEAR_NUM",
                     ("in", "LES"), ("archive", "LES"), "{WF_PP_YEAR_NUM}"),
    )),
    # --- wf_Pseudossn
    SessionCommands("wf_Pseudossn", "s_Pseudossn_Counters", "post", (
        _mv_in_to_archive("PSEUDO", "PSEUDOSSN_file", "PSEUDO_P{WF_PP_YEAR_NUM}.txt"),
        _mv_in_to_archive("PSEUDO", "PSEUDOSSN_Backup_file", "bkup.pseudssn.P{WF_PP_YEAR_NUM}.txt"),
    )),
)

# Workflow-level Email tasks (TASKTYPE="Email"): run as the last job task, like the PowerCenter link chain.
EMAIL_TASKS: dict[str, tuple[str, Email]] = {
    name: (wf, Email("Email task", to, "{WF_SUBJECT}", "{WF_MESSAGE}"))
    for name, wf, to in (
        ("email_COMPTIME_Complete", "wf_COMPTIME", "{WF_COMPTIME_EMAIL_LIST}"),
        ("email_CPM_CDC", "wf_CPM_CDC", "{WF_CPM_EMAIL_LIST}"),
        ("email_CPM_NIH", "wf_CPM_NIH", "{WF_CPM_EMAIL_LIST}"),
        ("email_CPM_OIG", "wf_CPM_OIG", "{WF_CPM_EMAIL_LIST}"),
        ("email_LES_Complete", "wf_LES", "{WF_LES_EMAIL_LIST}"),
        ("Email_Pay_Calendar", "wf_Pay_Calendar", "{WF_PAY_CALENDAR_EMAIL_LIST}"),
        ("Email_Pseudossn", "wf_Pseudossn", "{WF_PSEUDOSSN_EMAIL_LIST}"),
    )
}


def _email_task(email: Email) -> Task:
    def run(ctx: RunContext) -> None:
        email.run(ctx)

    return run


def _transfer_task(script: str) -> Task:
    """Standalone ``<script>.sh <FILENAME>`` for manual re-sends: ``--param FILENAME=...``."""

    def run(ctx: RunContext) -> None:
        sftp.transfer(ctx, script, ctx.param("FILENAME"))

    return run


def _archive_files(ctx: RunContext) -> None:
    """Standalone ``archive_files.sh <in> <dest> <pp>`` with landing-relative ``--param INPUT_DIR=in/LES`` etc."""
    files.archive_files(ctx.path(ctx.param("INPUT_DIR")), ctx.path(ctx.param("FILE_DEST")), ctx.param("PAY_PERIOD"))


def _remove_file(ctx: RunContext) -> None:
    files.remove_file(ctx.path(ctx.param("INPUT_DIR")), ctx.param("FILENAME"))


TASKS: dict[str, Task] = {
    **{c.task_key: c.run for c in SESSION_COMMANDS},
    **{name: _email_task(email) for name, (_wf, email) in EMAIL_TASKS.items()},
    **{script: _transfer_task(script) for script in sftp.PROFILES},
    "archive_files": _archive_files,
    "remove_file": _remove_file,
}
