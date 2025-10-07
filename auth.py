"""Authentication utilities extracted from app.py"""
from __future__ import annotations
import streamlit as st
import pandas as pd
from passlib.context import CryptContext
from datetime import datetime
from typing import Tuple

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

USERS_SHEET_NAME = "users"

# Hooks to be configured by main app
_get_spreadsheet_hook = None
_notify_event_hook = None
_audit_log_hook = None
_now_wib_iso_hook = None


def configure_hooks(get_spreadsheet, notify_event, audit_log, now_wib_iso):
    global _get_spreadsheet_hook, _notify_event_hook, _audit_log_hook, _now_wib_iso_hook
    _get_spreadsheet_hook = get_spreadsheet
    _notify_event_hook = notify_event
    _audit_log_hook = audit_log
    _now_wib_iso_hook = now_wib_iso


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return pwd_context.verify(plain, hashed)
    except Exception:
        return False


def _load_users_df():
    ss = _get_spreadsheet_hook()
    ws = ss.worksheet(USERS_SHEET_NAME)
    # Pull only needed columns to reduce quota/time.
    headers = ws.row_values(1)
    needed = ['email','username','password_hash','role','active','full_name']
    present = [h for h in headers if h in needed]
    # If some needed columns missing, fallback to full fetch to preserve compatibility.
    if len(present) < 3:  # likely legacy format
        records = ws.get_all_records()
        return ws, pd.DataFrame(records)
    # Build A1 ranges for each column and batch fetch (gspread doesn't have batch get easily, so fetch full row set once and then filter columns)
    # Simpler: get_all_values then reconstruct only needed columns (still less parsing vs pandas entire conversion?)
    values = ws.get_all_values()
    if not values:
        return ws, pd.DataFrame(columns=present)
    header_row = values[0]
    col_index_map = {h:i for i,h in enumerate(header_row)}
    rows = []
    for r in values[1:]:
        row_obj = {}
        for h in present:
            idx = col_index_map.get(h)
            if idx is not None and idx < len(r):
                row_obj[h] = r[idx]
        rows.append(row_obj)
    return ws, pd.DataFrame(rows)


def login_user(email: str, password: str) -> Tuple[bool, str, dict | None]:
    try:
        ws, df = _load_users_df()
        email_col = 'email' if 'email' in df.columns else ('username' if 'username' in df.columns else None)
        if not email_col:
            return False, 'Kolom email/username tidak ditemukan.', None
        row = df[df[email_col].astype(str).str.lower() == email.lower()]
        if row.empty:
            return False, 'Email/Username tidak ditemukan.', None
        hashed = row.iloc[0].get('password_hash')
        if not hashed or not verify_password(password, hashed):
            return False, 'Password salah.', None
        user_obj = {
            'email': row.iloc[0].get('email', row.iloc[0].get('username')),
            'full_name': row.iloc[0].get('full_name', ''),
            'role': str(row.iloc[0].get('role', 'user')).lower() or 'user',
            'active': int(row.iloc[0].get('active', 1)) if str(row.iloc[0].get('active', 1)).isdigit() else 1,
        }
        if not user_obj['active']:
            return False, 'Akun dinonaktifkan.', None
        if _notify_event_hook:
            try: _notify_event_hook('auth','login','Notifikasi: User Login', f"User '{user_obj['email']}' login.")
            except Exception: pass
        if _audit_log_hook:
            try: _audit_log_hook('auth','login', target=user_obj.get('email',''))
            except Exception: pass
        return True, 'Login berhasil.', user_obj
    except Exception as e:
        return False, f'Gagal login: {e}', None


def register_user(email: str, full_name: str, password: str) -> Tuple[bool, str]:
    try:
        ss = _get_spreadsheet_hook(); ws = ss.worksheet(USERS_SHEET_NAME)
        df = pd.DataFrame(ws.get_all_records())
        email_lower = email.lower()
        email_col = 'email' if 'email' in df.columns else ('username' if 'username' in df.columns else None)
        if email_col and (df[email_col].astype(str).str.lower() == email_lower).any():
            return False, 'Email/Username sudah terdaftar.'
        hashed = hash_password(password)
        now = _now_wib_iso_hook() if _now_wib_iso_hook else datetime.utcnow().isoformat()
        headers = ws.row_values(1)
        row_values = []
        for h in headers:
            if h == 'email': row_values.append(email)
            elif h == 'username': row_values.append(email)
            elif h == 'password_hash': row_values.append(hashed)
            elif h == 'full_name': row_values.append(full_name)
            elif h == 'role': row_values.append('user')
            elif h == 'created_at': row_values.append(now)
            elif h == 'active': row_values.append(1)
            else: row_values.append('')
        ws.append_row(row_values)
        if _notify_event_hook:
            try: _notify_event_hook('users','register','Notifikasi: User Baru', f"User baru '{email}' mendaftar.")
            except Exception: pass
        if _audit_log_hook:
            try: _audit_log_hook('users','register', target=email)
            except Exception: pass
        return True, 'Registrasi berhasil.'
    except Exception as e:
        return False, f'Gagal registrasi: {e}'
