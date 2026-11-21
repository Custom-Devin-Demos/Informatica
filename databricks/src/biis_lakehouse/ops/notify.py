"""Replacement for PowerCenter Email tasks / session emails and the ``mailx`` calls in the shell scripts."""
from __future__ import annotations

import re
import smtplib
from email.message import EmailMessage
from typing import Callable, Optional

from biis_lakehouse.context import RunContext
from biis_lakehouse.ops.secret_scope import get_secret

SmtpFactory = Callable[[str, int], "smtplib.SMTP"]
_smtp_factory: SmtpFactory = smtplib.SMTP


def set_smtp_factory(factory: Optional[SmtpFactory]) -> None:
    global _smtp_factory
    _smtp_factory = factory or smtplib.SMTP


def recipients(value: str) -> list[str]:
    """PowerCenter / mailx address lists are space, comma or semicolon separated."""
    return [a for a in re.split(r"[\s,;]+", value or "") if a]


def send_email(ctx: RunContext, to: str, subject: str, body: str) -> bool:
    """Send via ``SMTP_HOST`` (bundle variable). Returns False (and prints) when SMTP is not configured or no recipients."""
    addrs = recipients(to)
    host = ctx.params.get("SMTP_HOST", "")
    if not addrs or not host:
        print(f"[biis ops] email not sent (smtp_host={host!r}, to={addrs}): {subject}\n{body}")
        return False
    msg = EmailMessage()
    msg["From"] = ctx.params.get("SMTP_SENDER") or "biis-lakehouse@localhost"
    msg["To"] = ", ".join(addrs)
    msg["Subject"] = subject
    msg.set_content(body)
    smtp = _smtp_factory(host, int(ctx.params.get("SMTP_PORT") or 25))
    try:
        if ctx.params.get("SMTP_USER"):
            smtp.starttls()
            smtp.login(ctx.params["SMTP_USER"], get_secret(ctx, "smtp_password"))
        smtp.send_message(msg)
    finally:
        smtp.quit()
    return True
