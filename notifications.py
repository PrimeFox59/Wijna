"""Notifications & email utilities extracted from app.py for modularity.
Lightweight imports only; heavy modules lazy-loaded inside functions.
"""
from __future__ import annotations
import time
import streamlit as st
from typing import Dict, List, Tuple

# Static default role map & create access can be imported or overridden by main app if needed.
NOTIF_ROLE_MAP: dict[tuple[str, str], list[str]] = {
    ("inventory", "create"): ["finance"],
    ("inventory", "finance_review"): ["director"],
    ("inventory", "director_approved"): ["finance"],
    ("inventory", "director_reject"): ["finance"],
    ("surat_masuk", "draft"): ["director"],
    ("surat_masuk", "director_approved"): ["finance"],
    ("surat_keluar", "draft"): ["director"],
    ("surat_keluar", "final_upload"): ["finance"],
    ("cuti", "submit"): ["finance"],
    ("cuti", "finance_review"): ["director"],
    ("cuti", "director_approved"): ["finance"],
    ("cuti", "director_reject"): ["finance"],
    ("auth", "login"): ["superuser"],
    ("auth", "logout"): ["superuser"],
    ("users", "register"): ["superuser"],
    ("cash_advance", "create"): ["finance"],
    ("cash_advance", "finance_review"): ["director"],
    ("cash_advance", "director_approval"): ["finance"],
    ("pmr", "upload"): ["finance"],
    ("pmr", "finance_review"): ["director"],
    ("pmr", "director_approval"): ["finance"],
    ("delegasi", "create"): ["director"],
    ("delegasi", "update"): ["director"],
    ("flex", "create"): ["finance"],
    ("flex", "finance_review"): ["director"],
    ("flex", "director_approval"): ["finance"],
    ("mobil", "create"): ["finance"],
    ("mobil", "update"): ["finance", "director"],
    ("mobil", "delete"): ["finance"],
    ("calendar", "add_holiday"): ["director"],
    ("notulen", "upload"): ["director"],
    ("notulen", "director_approval"): ["staff", "finance"],
    ("sop", "upload"): ["director"],
    ("sop", "director_approval"): ["staff", "finance"],
    ("mou", "due_soon"): ["director", "finance"],
}

ADMIN_EMAIL_RECIPIENT = ""

# Public hooks will be bound/overridden by app.
_get_emails_by_role_hook = None
_is_superuser_auto_enabled_hook = None


def configure_hooks(get_emails_by_role, is_superuser_auto_enabled):
    global _get_emails_by_role_hook, _is_superuser_auto_enabled_hook
    _get_emails_by_role_hook = get_emails_by_role
    _is_superuser_auto_enabled_hook = is_superuser_auto_enabled


def send_notification_email(recipient_email: str, subject: str, body: str) -> bool:
    try:
        import smtplib  # lazy
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        creds = st.secrets.get("email_credentials", {})
        sender_email = (creds.get("username") or "").strip()
        sender_password = (creds.get("app_password") or "").strip()
        if not sender_email or not sender_password:
            return False
        msg = MIMEMultipart(); msg["From"] = sender_email; msg["To"] = recipient_email; msg["Subject"] = subject
        msg.attach(MIMEText(body or "", "plain"))
        for attempt in range(2):
            try:
                if attempt == 0:
                    server = smtplib.SMTP("smtp.gmail.com", 587, timeout=20)
                    server.ehlo(); server.starttls(); server.login(sender_email, sender_password)
                else:
                    server = smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=20)
                    server.login(sender_email, sender_password)
                server.send_message(msg); server.quit(); return True
            except Exception:
                if attempt == 1: raise
                time.sleep(1.0)
        return False
    except Exception:
        return False


def send_notification_bulk(recipients: list[str], subject: str, body: str) -> tuple[int, int]:
    try:
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        recipients = sorted({r.strip() for r in recipients if r and r.strip()})
        total = len(recipients)
        if not total: return 0, 0
        creds = st.secrets.get("email_credentials", {})
        sender_email = (creds.get("username") or "").strip()
        sender_password = (creds.get("app_password") or "").strip()
        if not sender_email or not sender_password: return 0, total
        def build(to_addr: str):
            m = MIMEMultipart(); m["From"] = sender_email; m["To"] = to_addr; m["Subject"] = subject; m.attach(MIMEText(body or "", "plain")); return m
        sent = 0
        try:
            server = smtplib.SMTP("smtp.gmail.com", 587, timeout=20)
            server.ehlo(); server.starttls(); server.login(sender_email, sender_password)
            for rcpt in recipients:
                try: server.sendmail(sender_email, [rcpt], build(rcpt).as_string()); sent += 1
                except Exception: pass
            server.quit(); return sent, total
        except Exception:
            try:
                server = smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=20)
                server.login(sender_email, sender_password)
                for rcpt in recipients:
                    try: server.sendmail(sender_email, [rcpt], build(rcpt).as_string()); sent += 1
                    except Exception: pass
                server.quit(); return sent, total
            except Exception:
                return 0, total
    except Exception:
        return 0, 0


def _notify_roles_internal(roles: list[str], subject: str, body: str):
    emails: set[str] = set()
    if not _get_emails_by_role_hook:
        return
    for r in {x.strip().lower() for x in roles if x}:
        try:
            for e in _get_emails_by_role_hook(r):
                if e: emails.add(e.strip())
        except Exception:
            continue
    if _is_superuser_auto_enabled_hook and _is_superuser_auto_enabled_hook():
        try:
            for su in _get_emails_by_role_hook('superuser'):
                if su: emails.add(su.strip())
        except Exception:
            pass
    admin_email = (ADMIN_EMAIL_RECIPIENT or '').strip()
    if admin_email:
        emails.add(admin_email)
    if not emails:
        return
    send_notification_bulk(sorted(emails), subject, body)
    st.toast(f"Notifikasi terkirim ke {len(emails)} penerima.")


def notify_event(module: str, action: str, subject: str, body: str, roles: list[str] | None = None,
                 dynamic_map: dict[tuple[str,str], list[str]] | None = None):
    try:
        if roles is not None:
            target = roles
        else:
            dyn = dynamic_map or {}
            target = dyn.get((module.lower(), action.lower())) or NOTIF_ROLE_MAP.get((module, action), [])
        if not target: return
        _notify_roles_internal(target, subject, body)
    except Exception:
        pass
