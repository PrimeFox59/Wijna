import streamlit as st
import pandas as pd
import gspread
from passlib.context import CryptContext
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import io
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
import uuid
from datetime import datetime, date, timedelta
import time

# --- 1. KONFIGURASI APLIKASI ---
# PENTING: Pastikan ID ini berasal dari folder di dalam SHARED DRIVE
GDRIVE_FOLDER_ID = "1CxYo2ZGu8jweKjmEws41nT3cexJju5_1" 
USERS_SHEET_NAME = "users"
CUTI_SHEET_NAME = "cuti"
AUDIT_SHEET_NAME = "audit_log"
INVENTORY_SHEET_NAME = "inventory"
SURAT_MASUK_SHEET_NAME = "surat_masuk"
SURAT_KELUAR_SHEET_NAME = "surat_keluar"
SPREADSHEET_URL = st.secrets["connections"]["gsheets"]["spreadsheet"]
ADMIN_EMAIL_RECIPIENT = "primetroyxs@gmail.com"  # Email tujuan notifikasi
st.set_page_config(page_title="Secure App", page_icon="🔐", layout="centered")


# --- 2. FUNGSI KONEKSI & AUTENTIKASI ---
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

@st.cache_resource
def get_credentials():
    """Membuat object credentials dari secrets."""
    creds_dict = st.secrets["connections"]["gsheets"]
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=[
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
    )
    return creds

@st.cache_resource
def get_gsheets_client():
    """Membuat client untuk gspread menggunakan credentials."""
    creds = get_credentials()
    client = gspread.authorize(creds)
    return client

@st.cache_resource
def get_gdrive_service():
    """Membuat service untuk Google Drive API menggunakan credentials."""
    creds = get_credentials()
    service = build('drive', 'v3', credentials=creds)
    return service


@st.cache_resource
def get_spreadsheet():
    """Cache object Spreadsheet untuk menghindari open_by_url berulang."""
    client = get_gsheets_client()
    return client.open_by_url(SPREADSHEET_URL)


@st.cache_data(ttl=60, show_spinner=False, max_entries=64)
def _cached_get_all_records(sheet_name: str, expected_headers: list | None = None):
    """Ambil seluruh records dari sebuah sheet dengan cache dan retry ringan.
    - ttl 60s untuk mengurangi beban read
    - expected_headers bila disediakan akan memaksa mapping kolom
    """
    ws = get_spreadsheet().worksheet(sheet_name)
    # Retry ringan untuk 429/5xx
    for i in range(3):
        try:
            if expected_headers is not None:
                return ws.get_all_records(expected_headers=expected_headers)
            return ws.get_all_records()
        except gspread.exceptions.APIError as e:
            msg = str(e)
            if any(code in msg for code in ["429", "500", "503"]):
                time.sleep(1.5 * (i + 1))
                continue
            raise
    # Fallback terakhir tanpa mapping
    return ws.get_all_records()


def _invalidate_data_cache():
    """Invalidasi cache data sheet (dipanggil setelah operasi tulis)."""
    try:
        st.cache_data.clear()
    except Exception:
        pass


# --- 3. FUNGSI HELPER & UTILITAS ---
def hash_password(password: str):
    """Mengubah password plain text menjadi hash."""
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str):
    """Memverifikasi password dengan hash yang tersimpan."""
    return pwd_context.verify(plain_password, hashed_password)

def send_notification_email(recipient_email, subject, body):
    """Mengirim email notifikasi menggunakan kredensial dari st.secrets."""
    try:
        sender_email = st.secrets["email_credentials"]["username"]
        sender_password = st.secrets["email_credentials"]["app_password"]

        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = recipient_email
        message["Subject"] = subject
        message.attach(MIMEText(body, "plain"))

        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(message)
        server.quit()
        st.toast(f"📧 Notifikasi email terkirim ke {recipient_email}")
        return True
    except Exception as e:
        st.toast(f" Gagal mengirim email: {e}")
        return False

def initialize_users_sheet():
    """Memastikan sheet 'users' ada dan berisi user default 'admin'."""
    try:
        client = get_gsheets_client()
        spreadsheet = client.open_by_url(SPREADSHEET_URL)
        
        try:
            worksheet = spreadsheet.worksheet(USERS_SHEET_NAME)
            df = pd.DataFrame(worksheet.get_all_records())
        except gspread.WorksheetNotFound:
            st.info(f"Sheet '{USERS_SHEET_NAME}' tidak ditemukan. Membuat sheet baru...")
            worksheet = spreadsheet.add_worksheet(title=USERS_SHEET_NAME, rows="100", cols="2")
            headers = ["username", "password_hash"]
            worksheet.append_row(headers)
            st.success(f"Sheet '{USERS_SHEET_NAME}' berhasil dibuat.")
            df = pd.DataFrame(columns=headers)

        if df.empty or 'admin' not in df['username'].values:
            st.info("User default 'admin' tidak ditemukan. Membuat user...")
            hashed_admin_pass = hash_password('admin')
            worksheet.append_row(['admin', hashed_admin_pass])
            st.success("User default 'admin' dengan password 'admin' berhasil ditambahkan.")
    except Exception as e:
        st.error(f"Gagal inisialisasi Google Sheet: {e}")


def ensure_sheet_with_headers(spreadsheet, title: str, headers: list[str]):
    """Ensure a worksheet exists and its header row is valid and unique.
    - If worksheet doesn't exist: create it and set exact headers.
    - If header row is empty or contains duplicates/whitespace variants: replace with canonical headers.
    - If some required headers are missing: append them to the end (keeping existing columns).
    """
    try:
        ws = spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=title, rows="1000", cols=str(max(10, len(headers) + 2)))
        ws.update("A1", [headers])
        return ws

    # Ensure headers present and unique
    try:
        current = ws.row_values(1)
        if not current:
            ws.update("A1", [headers])
            return ws

        # Normalize for duplicate detection
        curr_norm = [str(h).strip() for h in current]
        has_duplicates = len(curr_norm) != len(set(curr_norm))

        if has_duplicates:
            # If duplicates exist (e.g., ['active', 'active']), reset to canonical headers
            ws.update("A1", [headers])
            return ws

        # Append any missing required headers (avoid case/whitespace issues)
        present = set(curr_norm)
        missing = [h for h in headers if h.strip() not in present]
        if missing:
            new_headers = curr_norm + missing
            ws.update("A1", [new_headers])
        return ws
    except Exception:
        # If header fetch fails for any reason, set headers
        ws.update("A1", [headers])
        return ws


def ensure_core_sheets():
    """Initialize required worksheets once; minimize reads to avoid quotas."""
    if st.session_state.get("_core_sheets_ok"):
        return
    try:
        spreadsheet = get_spreadsheet()

        # Users sheet: create or validate headers only (no full read)
        users_headers = ["email", "password_hash", "full_name", "role", "created_at", "active"]
        users_ws = ensure_sheet_with_headers(spreadsheet, USERS_SHEET_NAME, users_headers)

        # Lightweight check for at least one data row; only read a tiny range
        try:
            data_row2 = users_ws.row_values(2)
        except Exception:
            data_row2 = []
        if not data_row2:
            # Append a default superuser only if sheet is empty
            for i in range(3):
                try:
                    users_ws.append_row(["admin@local", hash_password("admin"), "Admin", "superuser", datetime.utcnow().isoformat(), 1])
                    break
                except gspread.exceptions.APIError as e:
                    if "429" in str(e):
                        time.sleep(1.2 * (i + 1))
                        continue
                    raise

        # Cuti sheet
        cuti_headers = [
            "id", "nama", "tgl_mulai", "tgl_selesai", "durasi",
            "kuota_tahunan", "cuti_terpakai", "sisa_kuota", "status",
            "finance_note", "finance_approved", "director_note", "director_approved",
            "alasan", "created_at"
        ]
        ensure_sheet_with_headers(spreadsheet, CUTI_SHEET_NAME, cuti_headers)

        # Audit log sheet
        audit_headers = ["timestamp", "actor", "module", "action", "target", "details"]
        ensure_sheet_with_headers(spreadsheet, AUDIT_SHEET_NAME, audit_headers)

        # Inventory sheet
        inv_headers = [
            "id", "name", "location", "status", "pic", "updated_at",
            "finance_note", "finance_approved", "director_note", "director_approved",
            "file_id", "file_name", "file_link", "loan_info"
        ]
        ensure_sheet_with_headers(spreadsheet, INVENTORY_SHEET_NAME, inv_headers)

        # Surat Masuk sheet
        sm_headers = [
            "id", "nomor", "tanggal", "pengirim", "perihal",
            "file_id", "file_name", "file_link",
            "status", "follow_up",
            "director_approved", "rekap",
            "created_at", "submitted_by"
        ]
        ensure_sheet_with_headers(spreadsheet, SURAT_MASUK_SHEET_NAME, sm_headers)

        # Surat Keluar sheet
        sk_headers = [
            "id", "nomor", "tanggal", "ditujukan", "perihal", "pengirim",
            "status", "follow_up", "director_note", "director_approved",
            "draft_file_id", "draft_name", "draft_link",
            "final_file_id", "final_name", "final_link",
            "created_at", "updated_at", "submitted_by"
        ]
        ensure_sheet_with_headers(spreadsheet, SURAT_KELUAR_SHEET_NAME, sk_headers)

        st.session_state["_core_sheets_ok"] = True
    except Exception as e:
        st.error(f"Gagal memastikan sheet inti tersedia: {e}")


# --- 4. MANAJEMEN SESSION STATE ---
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'username' not in st.session_state:
    st.session_state.username = ""
if 'user' not in st.session_state:
    st.session_state.user = None


# --- 5. TAMPILAN HALAMAN (UI) ---
def show_login_page():
    """Menampilkan halaman login dan registrasi."""
    st.header("🔐 Secure App Login")
    
    with st.sidebar:
        st.subheader("Pilih Aksi")
        action = st.radio(" ", ["Login", "Register"])

    try:
        client = get_gsheets_client()
        spreadsheet = client.open_by_url(SPREADSHEET_URL)
        worksheet = spreadsheet.worksheet(USERS_SHEET_NAME)
    except Exception as e:
        st.error(f"Tidak dapat terhubung ke Google Sheet. Pastikan file dibagikan dan URL benar. Error: {e}")
        st.stop()

    if action == "Login":
        st.subheader("Login")
        with st.form("login_form"):
            username = st.text_input("Username").lower()
            password = st.text_input("Password", type="password")
            login_button = st.form_submit_button("Login")

            if login_button:
                if not username or not password:
                    st.warning("Username dan Password tidak boleh kosong.")
                    return

                users_df = pd.DataFrame(worksheet.get_all_records())
                user_data = users_df[users_df["username"] == username]

                if not user_data.empty:
                    stored_hash = user_data.iloc[0]["password_hash"]
                    if verify_password(password, stored_hash):
                        
                        # Kirim notifikasi email saat LOGIN
                        email_subject = "Notifikasi: User Login"
                        email_body = f"User '{username}' telah berhasil LOGIN ke aplikasi Anda."
                        send_notification_email(ADMIN_EMAIL_RECIPIENT, email_subject, email_body)
                        
                        st.session_state.logged_in = True
                        st.session_state.username = username
                        st.rerun()
                    else:
                        st.error("Username atau Password salah.")
                else:
                    st.error("Username atau Password salah.")

    elif action == "Register":
        st.subheader("Buat Akun Baru")
        with st.form("register_form"):
            new_username = st.text_input("Username Baru").lower()
            new_password = st.text_input("Password Baru", type="password")
            confirm_password = st.text_input("Konfirmasi Password", type="password")
            register_button = st.form_submit_button("Register")

            if register_button:
                if not new_username or not new_password or not confirm_password:
                    st.warning("Semua field harus diisi.")
                    return
                if new_password != confirm_password:
                    st.error("Password tidak cocok.")
                    return
                
                users_df = pd.DataFrame(worksheet.get_all_records())
                if new_username in users_df["username"].values:
                    st.error("Username sudah terdaftar. Silakan pilih yang lain.")
                else:
                    hashed_pass = hash_password(new_password)
                    worksheet.append_row([new_username, hashed_pass])
                    st.success("Registrasi berhasil! Silakan login.")

                    # Kirim notifikasi email saat REGISTRASI
                    email_subject = "Notifikasi: User Baru Telah Mendaftar"
                    email_body = f"User baru dengan username '{new_username}' telah berhasil mendaftar di aplikasi Anda."
                    send_notification_email(ADMIN_EMAIL_RECIPIENT, email_subject, email_body)

def show_main_app():
    """Menampilkan aplikasi utama setelah user berhasil login."""
    st.sidebar.success(f"Login sebagai: **{st.session_state.username}**")
    if st.sidebar.button("Logout"):
        
        # Kirim notifikasi email saat LOGOUT
        email_subject = "Notifikasi: User Logout"
        email_body = f"User '{st.session_state.username}' telah LOGOUT dari aplikasi Anda."
        send_notification_email(ADMIN_EMAIL_RECIPIENT, email_subject, email_body)
        
        st.session_state.logged_in = False
        st.session_state.username = ""
        st.rerun()

    st.title("📂 File Management with Google Drive")

    st.header("⬆️ Upload File Baru")
    uploaded_file = st.file_uploader("Pilih file untuk diupload ke Google Drive", type=None)
    
    if uploaded_file is not None:
        if st.button(f"Upload '{uploaded_file.name}'"):
            with st.spinner("Mengupload file..."):
                try:
                    drive_service = get_gdrive_service()
                    file_metadata = {'name': uploaded_file.name, 'parents': [GDRIVE_FOLDER_ID]}
                    file_buffer = io.BytesIO(uploaded_file.getvalue())
                    media = MediaIoBaseUpload(file_buffer, mimetype=uploaded_file.type, resumable=True)
                    
                    file = drive_service.files().create(
                        body=file_metadata,
                        media_body=media,
                        fields='id',
                        supportsAllDrives=True
                    ).execute()
                    st.success(f"✅ File '{uploaded_file.name}' berhasil diupload!")
                except Exception as e:
                    st.error(f"Gagal mengupload file: {e}")

    st.header("📋 Daftar File di Drive")
    if st.button("Refresh Daftar File"):
        # force cache clear
        try:
            st.cache_data.clear()
        except Exception:
            pass
        st.rerun()
        
    try:
        @st.cache_data(ttl=60, show_spinner=False)
        def _list_drive_files(folder_id: str):
            service = get_gdrive_service()
            query = f"'{folder_id}' in parents and trashed=false"
            for i in range(3):
                try:
                    results = service.files().list(
                        q=query,
                        pageSize=100,
                        fields="nextPageToken, files(id, name)",
                        supportsAllDrives=True,
                        includeItemsFromAllDrives=True
                    ).execute()
                    return results.get('files', [])
                except Exception as e:
                    if any(code in str(e) for code in ["429", "500", "503"]):
                        time.sleep(1.2 * (i + 1))
                        continue
                    raise
            return []
        with st.spinner("Memuat daftar file dari Google Drive..."):
            items = _list_drive_files(GDRIVE_FOLDER_ID)

        if not items:
            st.info("📂 Folder ini masih kosong atau ID salah/belum di-share.")
        else:
            st.write(f"Ditemukan {len(items)} file:")
            for item in items:
                col1, col2 = st.columns([4, 1])
                with col1:
                    st.write(f"📄 **{item['name']}**")
                with col2:
                    def download_file_from_drive(file_id):
                        service = get_gdrive_service()
                        for i in range(3):
                            try:
                                request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
                                fh = io.BytesIO()
                                fh.write(request.execute())
                                fh.seek(0)
                                return fh.getvalue()
                            except Exception as e:
                                if any(code in str(e) for code in ["429", "500", "503"]):
                                    time.sleep(1.0 * (i + 1))
                                    continue
                                raise
                        return b""

                    file_data = download_file_from_drive(item['id'])
                    st.download_button(
                        label="Download",
                        data=file_data,
                        file_name=item['name'],
                        key=f"dl_{item['id']}"
                    )
    except Exception as e:
        st.error(f"Gagal memuat daftar file: {e}")


# --- 6. LOGIKA UTAMA APLIKASI ---




def get_current_user():
    return st.session_state.get("user")


def set_current_user(user_obj):
    st.session_state.user = user_obj


def logout():
    user = get_current_user()
    if user:
        try:
            send_notification_email(ADMIN_EMAIL_RECIPIENT, "Notifikasi: User Logout", f"User '{user.get('email')}' telah LOGOUT dari aplikasi Anda.")
        except Exception:
            pass
    st.session_state.user = None
    st.session_state.logged_in = False
    st.session_state.username = ""


def _load_users_df():
    spreadsheet = get_spreadsheet()
    ws = spreadsheet.worksheet(USERS_SHEET_NAME)
    users_headers = ["email", "password_hash", "full_name", "role", "created_at", "active"]
    try:
        records = _cached_get_all_records(USERS_SHEET_NAME, users_headers)
    except Exception:
        records = ws.get_all_records()
    df = pd.DataFrame(records)
    return ws, df


def login_user(email: str, password: str):
    try:
        ws, df = _load_users_df()
        email_col = "email" if "email" in df.columns else ("username" if "username" in df.columns else None)
        if email_col is None:
            return False, "Sheet users tidak memiliki kolom email/username."
        row = df[df[email_col].astype(str).str.lower() == email.lower()]
        if row.empty:
            return False, "Email/Username tidak ditemukan."
        hashed = row.iloc[0].get("password_hash")
        if not hashed:
            return False, "Akun tidak memiliki password. Hubungi admin."
        if not verify_password(password, hashed):
            return False, "Password salah."
        # Build user object
        user_obj = {
            "email": row.iloc[0].get("email", row.iloc[0].get("username")),
            "full_name": row.iloc[0].get("full_name", ""),
            "role": str(row.iloc[0].get("role", "user")).lower() or "user",
            "active": int(row.iloc[0].get("active", 1)) if str(row.iloc[0].get("active", 1)).isdigit() else 1,
        }
        if not user_obj["active"]:
            return False, "Akun dinonaktifkan."
        set_current_user(user_obj)
        st.session_state.logged_in = True
        st.session_state.username = user_obj["email"]
        try:
            send_notification_email(ADMIN_EMAIL_RECIPIENT, "Notifikasi: User Login", f"User '{user_obj['email']}' telah berhasil LOGIN.")
        except Exception:
            pass
        return True, "Login berhasil."
    except Exception as e:
        return False, f"Gagal login: {e}"


def register_user(email: str, full_name: str, password: str):
    try:
        spreadsheet = get_spreadsheet()
        ws = spreadsheet.worksheet(USERS_SHEET_NAME)
        users_headers = ["email", "password_hash", "full_name", "role", "created_at", "active"]
        try:
            df = pd.DataFrame(_cached_get_all_records(USERS_SHEET_NAME, users_headers))
        except Exception:
            df = pd.DataFrame(ws.get_all_records())
        email_lower = email.lower()
        # Check existing
        email_col = "email" if "email" in df.columns else ("username" if "username" in df.columns else None)
        if email_col and (df[email_col].astype(str).str.lower() == email_lower).any():
            return False, "Email/Username sudah terdaftar."
        hashed = hash_password(password)
        now = datetime.utcnow().isoformat()
        # Adapt to sheet schema
        headers = ws.row_values(1)
        row_values = []
        for h in headers:
            if h == "email":
                row_values.append(email)
            elif h == "username":
                row_values.append(email)
            elif h == "password_hash":
                row_values.append(hashed)
            elif h == "full_name":
                row_values.append(full_name)
            elif h == "role":
                row_values.append("user")
            elif h == "created_at":
                row_values.append(now)
            elif h == "active":
                row_values.append(1)
            else:
                row_values.append("")
        # Retry and cache invalidation
        for i in range(3):
            try:
                ws.append_row(row_values)
                _invalidate_data_cache()
                break
            except gspread.exceptions.APIError as e:
                if "429" in str(e):
                    time.sleep(1.2 * (i + 1))
                    continue
                raise
        try:
            send_notification_email(ADMIN_EMAIL_RECIPIENT, "Notifikasi: User Baru", f"User baru '{email}' telah mendaftar.")
        except Exception:
            pass
        return True, "Registrasi berhasil."
    except Exception as e:
        return False, f"Gagal registrasi: {e}"


def auth_sidebar():
    user = get_current_user()
    if user:
        st.sidebar.info(f"Masuk sebagai: {user.get('full_name') or user.get('email')} ({user.get('role')})")


def _get_emails_by_role(role: str) -> list[str]:
    """Kembalikan list email untuk user dengan role tertentu dan active=1 (jika ada)."""
    try:
        _, df = _load_users_df()
        if df is None or df.empty:
            return []
        role_mask = df.get('role', pd.Series(dtype=str)).astype(str).str.lower() == str(role).lower()
        if 'active' in df.columns:
            active_mask = pd.to_numeric(df['active'], errors='coerce').fillna(0).astype(int) == 1
            role_mask = role_mask & active_mask
        email_col = 'email' if 'email' in df.columns else ('username' if 'username' in df.columns else None)
        if not email_col:
            return []
        emails = (
            df.loc[role_mask, email_col]
            .dropna()
            .astype(str)
            .str.strip()
            .unique()
            .tolist()
        )
        return emails
    except Exception:
        return []


def _notify_role(role: str, subject: str, body: str):
    """Kirim email ke semua user dengan role tersebut. Non-blocking per kegagalan satu alamat."""
    emails = _get_emails_by_role(role)
    sent = 0
    for e in emails:
        try:
            if send_notification_email(e, subject, body):
                sent += 1
        except Exception:
            pass
    if emails:
        st.toast(f"Notifikasi terkirim ke {sent}/{len(emails)} {role}")


def require_login():
    user = get_current_user()
    if not user:
        st.stop()
    return user


def gen_id(prefix: str):
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _get_ws(name: str):
    spreadsheet = get_spreadsheet()
    return spreadsheet.worksheet(name)


def audit_log(module: str, action: str, target: str = "", details: str = ""):
    try:
        ws = _get_ws(AUDIT_SHEET_NAME)
        actor = (get_current_user() or {}).get("email", "guest")
        data = [datetime.utcnow().isoformat(), actor, module, action, target, details]
        for i in range(3):
            try:
                ws.append_row(data)
                _invalidate_data_cache()
                break
            except gspread.exceptions.APIError as e:
                if "429" in str(e):
                    time.sleep(1.2 * (i + 1))
                    continue
                raise
    except Exception:
        # Non-blocking
        pass


# --- Helpers: Users sheet operations ---
def _find_user_row(ws, email_or_username: str) -> int | None:
    """Find row index (1-based) by matching email or username columns (case-insensitive). Returns None if not found."""
    headers = ws.row_values(1)
    email_col_idx = (headers.index('email') + 1) if 'email' in headers else None
    user_col_idx = (headers.index('username') + 1) if 'username' in headers else None
    # Prefer email search
    try_cols = [email_col_idx, user_col_idx]
    for col_idx in try_cols:
        if not col_idx:
            continue
        try:
            cell = ws.find(email_or_username, in_column=col_idx, case_sensitive=False)
            if cell:
                return cell.row
        except Exception:
            continue
    return None


def _users_update_row(ws, row_idx: int, updates: dict):
    """Update specified columns for a given row with retries; clear cache after."""
    headers = ws.row_values(1)
    for k, v in updates.items():
        if k not in headers:
            continue
        a1 = gspread.utils.rowcol_to_a1(row_idx, headers.index(k) + 1)
        for i in range(3):
            try:
                ws.update(a1, [[v]])
                break
            except gspread.exceptions.APIError as e:
                if '429' in str(e):
                    time.sleep(1.0 * (i + 1))
                    continue
                raise
    _invalidate_data_cache()


def _users_delete_row(ws, row_idx: int):
    for i in range(3):
        try:
            ws.delete_rows(row_idx)
            _invalidate_data_cache()
            break
        except gspread.exceptions.APIError as e:
            if '429' in str(e):
                time.sleep(1.2 * (i + 1))
                continue
            raise


def dashboard():
    st.title("🏠 Dashboard")
    st.write("Selamat datang di WIJNA Manajemen System.")


def inventory_module():
    user = require_login()
    st.markdown("# 📦 Inventory")

    # Helpers for inventory sheet
    def _inv_ws():
        return _get_ws(INVENTORY_SHEET_NAME)

    def _inv_read_df():
        ws = _inv_ws()
        inv_headers = [
            "id", "name", "location", "status", "pic", "updated_at",
            "finance_note", "finance_approved", "director_note", "director_approved",
            "file_id", "file_name", "file_link", "loan_info"
        ]
        try:
            df = pd.DataFrame(_cached_get_all_records(INVENTORY_SHEET_NAME, inv_headers))
        except Exception:
            df = pd.DataFrame(ws.get_all_records())
        # Ensure all expected columns exist to avoid KeyError in filters
        for h in inv_headers:
            if h not in df.columns:
                # defaults: numeric approvals -> 0, others empty string
                df[h] = 0 if h in ("finance_approved", "director_approved") else ""
        return ws, df

    def _inv_append(row: dict):
        ws = _inv_ws()
        headers = ws.row_values(1)
        values = [row.get(h, "") for h in headers]
        for i in range(3):
            try:
                ws.append_row(values)
                _invalidate_data_cache()
                break
            except gspread.exceptions.APIError as e:
                if "429" in str(e):
                    time.sleep(1.2 * (i + 1))
                    continue
                raise

    def _inv_update_by_id(iid: str, updates: dict):
        ws = _inv_ws()
        headers = ws.row_values(1)
        id_cell = ws.find(iid)
        if not id_cell:
            raise ValueError("ID tidak ditemukan")
        row_idx = id_cell.row
        for k, v in updates.items():
            if k not in headers:
                continue
            col_idx = headers.index(k) + 1
            a1 = gspread.utils.rowcol_to_a1(row_idx, col_idx)
            # Retry updates
            for i in range(3):
                try:
                    ws.update(a1, [[v]])
                    break
                except gspread.exceptions.APIError as e:
                    if "429" in str(e):
                        time.sleep(1.0 * (i + 1))
                        continue
                    raise
        _invalidate_data_cache()

    def format_datetime_wib(ts: str) -> str:
        try:
            dt = datetime.fromisoformat(str(ts).replace("Z", ""))
            dt_wib = dt + timedelta(hours=7)
            return dt_wib.strftime("%Y-%m-%d %H:%M") + " WIB"
        except Exception:
            return str(ts)

    def upload_file_to_drive(file) -> tuple[str, str, str]:
        """Upload to Drive and return (file_id, file_name, web_view_link)."""
        if not file:
            return "", "", ""
        try:
            service = get_gdrive_service()
            file_metadata = {
                'name': file.name,
                'parents': [GDRIVE_FOLDER_ID]
            }
            media = MediaIoBaseUpload(io.BytesIO(file.getvalue()), mimetype=file.type or 'application/octet-stream', resumable=True)
            resp = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, webViewLink',
                supportsAllDrives=True
            ).execute()
            # Set permission anyone with link can view (optional, comment if not desired)
            try:
                service.permissions().create(fileId=resp['id'], body={
                    'role': 'reader', 'type': 'anyone'
                }, supportsAllDrives=True).execute()
            except Exception:
                pass
            return resp.get('id', ''), file.name, resp.get('webViewLink', '')
        except Exception as e:
            st.warning(f"Gagal upload lampiran ke Drive: {e}")
            return "", "", ""

    def drive_download_button(file_id: str, file_name: str, key: str):
        if not file_id or not file_name:
            return
        try:
            service = get_gdrive_service()
            request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
            fh = io.BytesIO()
            fh.write(request.execute())
            fh.seek(0)
            st.download_button(
                label=f"⬇️ Download {file_name}",
                data=fh.getvalue(),
                file_name=file_name,
                mime="application/octet-stream",
                key=key
            )
        except Exception as e:
            st.caption(f"Tidak dapat download lampiran: {e}")

    # Prepare monthly rekap
    _, df_all = _inv_read_df()
    this_month = date.today().strftime("%Y-%m")
    df_month = df_all.copy()
    if not df_month.empty and 'updated_at' in df_month.columns:
        df_month = df_month[df_month['updated_at'].astype(str).str[:7] == this_month]

    tab_labels = []
    tab_contents = []

    # Staff tab: tambah barang
    if user.get("role") in ["staff", "superuser"]:
        tab_labels.append("➕ Tambah Barang")

        def staff_tab():
            with st.form("inv_add"):
                name = st.text_input("Nama Barang")
                keterangan_opsi = st.selectbox("Keterangan Tambahan", ["", "dijual", "rusak"], index=0)
                loc = st.text_input("Tempat Barang")
                status = st.selectbox("Status", ["Tersedia","Dipinjam","Rusak","Dijual"])
                f = st.file_uploader("Lampiran (opsional)")
                submitted = st.form_submit_button("Simpan (draft)")
                if submitted:
                    if not name:
                        st.warning("Nama barang wajib diisi.")
                    else:
                        full_nama = name if not keterangan_opsi else f"{name} ({keterangan_opsi})"
                        iid = gen_id("inv")
                        now = datetime.utcnow().isoformat()
                        file_id, file_name, file_link = upload_file_to_drive(f) if f else ("", "", "")
                        # PIC adalah user penginput
                        pic = (user.get("full_name") or user.get("email") or "").strip()
                        _inv_append({
                            "id": iid,
                            "name": full_nama,
                            "location": loc,
                            "status": status,
                            "pic": pic,
                            "updated_at": now,
                            "finance_note": "",
                            "finance_approved": 0,
                            "director_note": "",
                            "director_approved": 0,
                            "file_id": file_id,
                            "file_name": file_name,
                            "file_link": file_link,
                            "loan_info": ""
                        })
                        try:
                            audit_log("inventory", "create", target=iid, details=f"{full_nama} @ {loc} status={status}")
                        except Exception:
                            pass
                        # Notify Finance users
                        try:
                            _notify_role("finance", "[WIJNA] Draft Inventaris Baru",
                                         f"Item inventaris baru menunggu review Finance.\n\nNama: {full_nama}\nID: {iid}\nLokasi: {loc}\nStatus: {status}\nPIC: {pic}")
                        except Exception:
                            pass
                        st.success("Item disimpan sebagai draft. Menunggu review Finance.")
                        st.rerun()

        tab_contents.append(staff_tab)

    # Finance review tab
    if user.get("role") in ["finance", "superuser"]:
        tab_labels.append("💰 Review Finance")

        def finance_tab():
            st.info("Approve item yang sudah diinput staf.")
            _, df = _inv_read_df()
            if not df.empty:
                df['finance_approved'] = pd.to_numeric(df.get('finance_approved'), errors='coerce').fillna(0).astype(int)
            rows = df[df['finance_approved'] == 0]
            for idx, r in rows.iterrows():
                st.markdown(f"""
<div style='border:1.5px solid #b3d1ff; border-radius:10px; padding:1.2em 1em; margin-bottom:1.5em; background:#f8fbff;'>
<b>📦 {r.get('name')}</b> <span style='color:#888;'>(ID: {r.get('id')})</span><br>
<b>Lokasi:</b> {r.get('location')}<br>
<b>Status:</b> {r.get('status')}<br>
<b>Penanggung Jawab:</b> {r.get('pic','')}<br>
<b>Terakhir Update:</b> {format_datetime_wib(r.get('updated_at',''))}<br>
""", unsafe_allow_html=True)
                file_id = r.get('file_id')
                file_name = r.get('file_name')
                if file_id and file_name:
                    drive_download_button(file_id, file_name, key=f"dl_fin_{r.get('id')}_{idx}")
                st.markdown("**Catatan Finance:**")
                note = st.text_area("Tulis catatan atau alasan jika perlu", value=r.get("finance_note") or "", key=f"fin_note_{r.get('id')}_{idx}")
                colf1, colf2 = st.columns([1,2])
                with colf1:
                    if st.button("🔎 Review", key=f"ap_fin_{r.get('id')}_{idx}"):
                        try:
                            _inv_update_by_id(r.get('id'), {"finance_note": note, "finance_approved": 1})
                            try:
                                audit_log("inventory", "finance_review", target=r.get('id'), details=str(note))
                            except Exception:
                                pass
                            # Notify Directors
                            try:
                                _notify_role("director", "[WIJNA] Inventaris Menunggu Approval Director",
                                             f"Item inventaris telah direview Finance dan menunggu Approval Director.\n\nNama: {r.get('name')}\nID: {r.get('id')}\nLokasi: {r.get('location')}\nStatus: {r.get('status')}\nPIC: {r.get('pic','')}")
                            except Exception:
                                pass
                            st.success("Finance reviewed. Menunggu persetujuan Director.")
                        except Exception as e:
                            st.error(f"Gagal menyimpan: {e}")
                        st.rerun()
                with colf2:
                    st.caption("Klik Review jika sudah sesuai. Catatan akan tersimpan di database.")

        tab_contents.append(finance_tab)

    # Director approval tab
    if user.get("role") in ["director", "superuser"]:
        tab_labels.append("✅ Approval Director")

        def director_tab():
            st.info("Approve/Tolak item yang sudah di-approve Finance.")
            _, df = _inv_read_df()
            if not df.empty:
                df['finance_approved'] = pd.to_numeric(df.get('finance_approved'), errors='coerce').fillna(0).astype(int)
                df['director_approved'] = pd.to_numeric(df.get('director_approved'), errors='coerce').fillna(0).astype(int)
            rows = df[(df['finance_approved'] == 1) & (df['director_approved'] == 0)]
            for idx, r in rows.iterrows():
                with st.expander(f"[Menunggu Approval Director] {r.get('name')} ({r.get('id')})"):
                    st.markdown(f"""
                    <div style='background:#f8fafc;border-radius:12px;padding:1.2em 1.5em 1em 1.5em;margin-bottom:1em;'>
                        <b>Nama:</b> {r.get('name')}<br>
                        <b>ID:</b> {r.get('id')}<br>
                        <b>Lokasi:</b> {r.get('location')}<br>
                        <b>Status:</b> <span style='color:#2563eb;font-weight:600'>{r.get('status')}</span><br>
                        <b>PIC:</b> {r.get('pic','')}<br>
                        <b>Update Terakhir:</b> {format_datetime_wib(r.get('updated_at',''))}<br>
                    </div>
                    """, unsafe_allow_html=True)
                    if r.get('file_id') and r.get('file_name'):
                        drive_download_button(r.get('file_id'), r.get('file_name'), key=f"dl_dir_{r.get('id')}_{idx}")
                    st.markdown("<b>Catatan Director</b>", unsafe_allow_html=True)
                    note2 = st.text_area("", value=r.get("director_note") or "", key=f"dir_note_{r.get('id')}_{idx}", placeholder="Tulis catatan atau alasan jika perlu...", height=80)
                    colA, colB = st.columns([1,1])
                    with colA:
                        if st.button("✅ Approve", key=f"ap_dir_{r.get('id')}_{idx}"):
                            try:
                                _inv_update_by_id(r.get('id'), {"director_note": note2, "director_approved": 1})
                                try:
                                    audit_log("inventory", "director_approval", target=r.get('id'), details=f"approve=1; note={note2}")
                                except Exception:
                                    pass
                                st.success("Item telah di-approve Director.")
                            except Exception as e:
                                st.error(f"Gagal menyimpan: {e}")
                            st.rerun()
                    with colB:
                        if st.button("❌ Tolak", key=f"reject_dir_{r.get('id')}_{idx}"):
                            try:
                                _inv_update_by_id(r.get('id'), {"director_note": note2, "director_approved": -1})
                                try:
                                    audit_log("inventory", "director_approval", target=r.get('id'), details=f"approve=0; note={note2}")
                                except Exception:
                                    pass
                                st.success("Item ditolak Director.")
                            except Exception as e:
                                st.error(f"Gagal menyimpan: {e}")
                            st.rerun()

        tab_contents.append(director_tab)

    # Daftar Inventaris + Pinjam
    tab_labels.append("📦 Daftar Inventaris")

    def data_tab():
        st.subheader("Daftar Inventaris & Pinjam Barang")
        left_col, right_col = st.columns([2, 1])
        with left_col:
            filter_col1, filter_col2, filter_col3 = st.columns([2,2,2])
            with filter_col1:
                filter_nama = st.text_input("Filter Nama Barang", "")
            with filter_col2:
                filter_lokasi = st.text_input("Filter Lokasi", "")
            with filter_col3:
                filter_status = st.selectbox("Filter Status", ["Semua", "Tersedia", "Dipinjam", "Rusak", "Dijual"], index=0)

            _, df = _inv_read_df()
            if not df.empty and 'updated_at' in df.columns:
                df['updated_at'] = df['updated_at'].apply(format_datetime_wib)

            filtered_df = df.copy()
            if filter_nama:
                filtered_df = filtered_df[filtered_df.get('name','').astype(str).str.contains(filter_nama, case=False, na=False)]
            if filter_lokasi:
                filtered_df = filtered_df[filtered_df.get('location','').astype(str).str.contains(filter_lokasi, case=False, na=False)]
            if filter_status != "Semua":
                filtered_df = filtered_df[filtered_df.get('status','') == filter_status]

            if filtered_df.empty:
                st.info("Tidak ada data inventaris sesuai filter.")
            else:
                # Tampilkan hanya kolom: id, name, location, status, pic, updated_at
                cols_show = ["id", "name", "location", "status", "pic", "updated_at"]
                show_df = filtered_df.reindex(columns=cols_show)
                st.dataframe(show_df, use_container_width=True)

                lampiran_list = [
                    f"{row.get('name')} - {row.get('file_name')}" for _, row in filtered_df.iterrows()
                    if row.get('file_id') and row.get('file_name')
                ]
                lampiran_dict = {
                    f"{row.get('name')} - {row.get('file_name')}": (row.get('file_name'), row.get('file_id'))
                    for _, row in filtered_df.iterrows()
                    if row.get('file_id') and row.get('file_name')
                }
                if lampiran_list:
                    selected = st.selectbox("Pilih lampiran untuk diunduh:", lampiran_list)
                    if selected:
                        file_name, file_id = lampiran_dict[selected]
                        drive_download_button(file_id, file_name, key=f"dl_sel_{hash(selected)}")
                else:
                    st.info("Tidak ada lampiran yang tersedia untuk diunduh.")

        with right_col:
            st.markdown("### 📋 Pinjam Barang")
            _, df2 = _inv_read_df()
            filtered_df2 = df2.copy()
            if filter_nama:
                filtered_df2 = filtered_df2[filtered_df2.get('name','').astype(str).str.contains(filter_nama, case=False, na=False)]
            if filter_lokasi:
                filtered_df2 = filtered_df2[filtered_df2.get('location','').astype(str).str.contains(filter_lokasi, case=False, na=False)]
            if filter_status != "Semua":
                filtered_df2 = filtered_df2[filtered_df2.get('status','') == filter_status]

            for idx, row in filtered_df2.iterrows():
                if row.get('status') != "Dipinjam":
                    with st.expander(f"Pinjam: {row.get('name')} ({row.get('id')})"):
                        keperluan = st.text_input(f"Keperluan pinjam untuk {row.get('name')}", key=f"keperluan_{row.get('id')}")
                        tgl_kembali = st.date_input("Tanggal Kembali", key=f"tglkembali_{row.get('id')}", min_value=date.today())
                        ajukan = st.button("Ajukan Pinjam", key=f"ajukan_{row.get('id')}")
                        if ajukan:
                            try:
                                # Simpan detail pinjam terpisah agar tidak menimpa PIC penginput
                                info_pic = f"{user.get('email')}|{keperluan}|{tgl_kembali}|0|0"
                                _inv_update_by_id(row.get('id'), {
                                    "loan_info": info_pic,
                                    "finance_approved": 0,
                                    "director_approved": 0,
                                    "updated_at": datetime.utcnow().isoformat()
                                })
                                try:
                                    audit_log("inventory", "loan_request", target=row.get('id'), details=f"keperluan={keperluan}; kembali={tgl_kembali}")
                                except Exception:
                                    pass
                                st.success("Pengajuan pinjam barang berhasil. Menunggu ACC Finance & Director.")
                            except Exception as e:
                                st.error(f"Gagal mengajukan pinjam: {e}")

    # Build tab labels and contents
    # Ensure display names
    for i, lbl in enumerate(tab_labels):
        if lbl.lower().startswith("tambah barang") or lbl.lower().startswith("➕ tambah barang"):
            tab_labels[i] = "➕ Tambah Barang"
        elif lbl.lower().startswith("review finance") or lbl.lower().startswith("💰 review finance"):
            tab_labels[i] = "💰 Review Finance"
        elif lbl.lower().startswith("approval director") or lbl.lower().startswith("✅ approval director"):
            tab_labels[i] = "✅ Approval Director"
        elif lbl.lower().startswith("daftar inventaris") or lbl.lower().startswith("📦 daftar inventaris"):
            tab_labels[i] = "📦 Daftar Inventaris"

    # Ensure Daftar Inventaris tab exists once and last
    tab_contents = [tab for tab in tab_contents if tab.__name__ != "data_tab"]
    tab_contents.append(data_tab)
    if "📦 Daftar Inventaris" not in tab_labels:
        tab_labels.append("📦 Daftar Inventaris")

    # Render tabs
    selected = st.tabs(tab_labels)
    for i, tab_func in enumerate(tab_contents):
        with selected[i]:
            tab_func()


def surat_masuk_module():
    user = require_login()
    st.header("📥 Surat Masuk")

    # Helpers for Surat Masuk sheet
    def _sm_headers():
        return [
            "id", "nomor", "tanggal", "pengirim", "perihal",
            "file_id", "file_name", "file_link",
            "status", "follow_up",
            "director_approved", "rekap",
            "created_at", "submitted_by"
        ]

    def _sm_ws():
        # Try to get the worksheet; if missing, create with headers
        try:
            return _get_ws(SURAT_MASUK_SHEET_NAME)
        except gspread.WorksheetNotFound:
            spreadsheet = get_spreadsheet()
            return ensure_sheet_with_headers(spreadsheet, SURAT_MASUK_SHEET_NAME, _sm_headers())

    def _sm_read_df():
        ws = _sm_ws()
        headers = _sm_headers()
        try:
            df = pd.DataFrame(_cached_get_all_records(SURAT_MASUK_SHEET_NAME, headers))
        except Exception:
            df = pd.DataFrame(ws.get_all_records())
        for h in headers:
            if h not in df.columns:
                df[h] = 0 if h in ("director_approved", "rekap") else ""
        # Normalize numeric flags
        df["director_approved"] = pd.to_numeric(df.get("director_approved"), errors="coerce").fillna(0).astype(int)
        df["rekap"] = pd.to_numeric(df.get("rekap"), errors="coerce").fillna(0).astype(int)
        return ws, df

    def _sm_append(row: dict):
        ws = _sm_ws()
        headers = ws.row_values(1)
        values = [row.get(h, "") for h in headers]
        for i in range(3):
            try:
                ws.append_row(values)
                _invalidate_data_cache()
                break
            except gspread.exceptions.APIError as e:
                if "429" in str(e):
                    time.sleep(1.2 * (i + 1))
                    continue
                raise

    def _sm_update_by_id(sid: str, updates: dict):
        ws = _sm_ws()
        headers = ws.row_values(1)
        id_cell = ws.find(sid)
        if not id_cell:
            raise ValueError("ID tidak ditemukan")
        row_idx = id_cell.row
        for k, v in updates.items():
            if k not in headers:
                continue
            a1 = gspread.utils.rowcol_to_a1(row_idx, headers.index(k) + 1)
            for i in range(3):
                try:
                    ws.update(a1, [[v]])
                    break
                except gspread.exceptions.APIError as e:
                    if "429" in str(e):
                        time.sleep(1.0 * (i + 1))
                        continue
                    raise
        _invalidate_data_cache()

    # Drive helpers
    def _upload_surat_to_drive(file) -> tuple[str, str, str]:
        if not file:
            return "", "", ""
        try:
            service = get_gdrive_service()
            file_metadata = {"name": file.name, "parents": [GDRIVE_FOLDER_ID]}
            media = MediaIoBaseUpload(io.BytesIO(file.getvalue()), mimetype=file.type or "application/octet-stream", resumable=True)
            resp = service.files().create(
                body=file_metadata,
                media_body=media,
                fields="id, webViewLink",
                supportsAllDrives=True,
            ).execute()
            try:
                service.permissions().create(
                    fileId=resp["id"],
                    body={"role": "reader", "type": "anyone"},
                    supportsAllDrives=True,
                ).execute()
            except Exception:
                pass
            return resp.get("id", ""), file.name, resp.get("webViewLink", "")
        except Exception as e:
            st.warning(f"Gagal upload surat ke Drive: {e}")
            return "", "", ""

    def _download_button(file_id: str, file_name: str, key: str):
        if not file_id or not file_name:
            return
        try:
            service = get_gdrive_service()
            request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
            fh = io.BytesIO()
            fh.write(request.execute())
            fh.seek(0)
            st.download_button(
                label=f"⬇️ Download {file_name}",
                data=fh.getvalue(),
                file_name=file_name,
                mime="application/octet-stream",
                key=key,
            )
        except Exception as e:
            st.caption(f"Tidak dapat download surat: {e}")

    # Authorization: who can input/approve
    role = str(user.get("role", "")).lower()
    allowed_submit = role in ["staff", "staf", "finance", "director", "superuser"]

    tab1, tab2, tab3 = st.tabs([
        "📝 Input Draft Surat Masuk",
        "✅ Approval",
        "📋 Daftar & Rekap Surat Masuk",
    ])

    # Tab 1: Input
    with tab1:
        st.markdown("### Input Draft Surat Masuk")
        if not allowed_submit:
            st.warning("Anda tidak memiliki akses untuk input Surat Masuk.")
        else:
            with st.form("form_surat_masuk", clear_on_submit=True):
                nomor = st.text_input("Nomor Surat")
                pengirim = st.text_input("Pengirim")
                tanggal = st.date_input("Tanggal Surat", value=date.today())
                perihal = st.text_input("Perihal")
                file_upload = st.file_uploader("Upload File Surat (wajib)", type=None)
                status = st.selectbox(
                    "Status",
                    [
                        "Diusulkan dibahas ke rapat rutin",
                        "Langsung dilegasikan ke salah satu user",
                        "Selesai",
                    ],
                    index=0,
                )
                follow_up = st.text_area("Tindak Lanjut (Follow Up)")
                submitted = st.form_submit_button("Catat Surat Masuk")

                if submitted:
                    if not file_upload:
                        st.error("File surat wajib diupload.")
                    else:
                        fid, fname, flink = _upload_surat_to_drive(file_upload)
                        if not fid:
                            st.error("Gagal mengupload file ke Drive.")
                        else:
                            sid = gen_id("sm")
                            now = datetime.utcnow().isoformat()
                            _sm_append(
                                {
                                    "id": sid,
                                    "nomor": nomor,
                                    "tanggal": tanggal.isoformat(),
                                    "pengirim": pengirim,
                                    "perihal": perihal,
                                    "file_id": fid,
                                    "file_name": fname,
                                    "file_link": flink,
                                    "status": status,
                                    "follow_up": follow_up,
                                    "director_approved": 0,
                                    "rekap": 0,
                                    "created_at": now,
                                    "submitted_by": user.get("email"),
                                }
                            )
                            audit_log(
                                "surat_masuk",
                                "create",
                                target=sid,
                                details=f"{nomor} - {perihal} ({pengirim})",
                            )
                            st.success("Surat masuk berhasil dicatat.")
                            st.rerun()

    # Tab 2: Approval (Director/Superuser)
    with tab2:
        st.markdown("### Approval Surat Masuk")
        if role in ["director", "superuser"]:
            _, df = _sm_read_df()
            for idx, row in df.sort_values(by="tanggal", ascending=False).iterrows():
                if int(row.get("director_approved", 0)) == 0:
                    with st.expander(f"{row.get('nomor','')} | {row.get('perihal','')} | {row.get('tanggal','')}"):
                        st.write(f"Pengirim: {row.get('pengirim','')}")
                        st.write(f"Status: {row.get('status','')}")
                        st.write(f"Follow Up: {row.get('follow_up','')}")
                        if row.get("file_link"):
                            st.markdown(f"[Link Surat]({row.get('file_link')})")
                        if row.get("file_id") and row.get("file_name"):
                            _download_button(row.get("file_id"), row.get("file_name"), key=f"dl_sm_{row.get('id')}_{idx}")
                        colA, colB = st.columns(2)
                        with colA:
                            if st.button("Approve Surat Masuk", key=f"approve_{row.get('id')}"):
                                _sm_update_by_id(row.get("id"), {"director_approved": 1})
                                audit_log("surat_masuk", "director_approval", target=row.get("id"), details="approve=1")
                                st.success("Surat masuk di-approve Director.")
                                st.rerun()
                        with colB:
                            if st.button("Reject Surat Masuk", key=f"reject_{row.get('id')}"):
                                _sm_update_by_id(row.get("id"), {"director_approved": -1})
                                audit_log("surat_masuk", "director_approval", target=row.get("id"), details="approve=0")
                                st.warning("Surat masuk ditolak Director.")
                                st.rerun()
                elif int(row.get("director_approved", 0)) == 1:
                    st.success(
                        f"Sudah di-approve Director: {row.get('nomor','')} | {row.get('perihal','')} | {row.get('tanggal','')}"
                    )
                elif int(row.get("director_approved", 0)) == -1:
                    st.error(
                        f"Surat masuk ditolak Director: {row.get('nomor','')} | {row.get('perihal','')} | {row.get('tanggal','')}"
                    )
        else:
            st.info("Hanya Director atau Superuser yang dapat meng-approve surat masuk.")

    # Tab 3: Daftar & Rekap
    with tab3:
        st.markdown("### Daftar & Rekap Surat Masuk")
        _, df = _sm_read_df()
        if not df.empty:
            df_show = df.copy()
            # Indeks otomatis
            df_show = df_show.sort_values(by="tanggal", ascending=False).reset_index(drop=True)
            df_show["indeks"] = [f"SM-{i+1:04d}" for i in range(len(df_show))]
            show_cols = [
                c
                for c in ["indeks", "nomor", "tanggal", "pengirim", "perihal", "file_name"]
                if c in df_show.columns
            ]
        else:
            df_show = df
            show_cols = [c for c in ["nomor", "tanggal", "pengirim", "perihal", "file_name"] if c in df.columns]

        # Rekap only
        rekap_df = df[df.get("rekap", 0) == 1].copy() if not df.empty else df.copy()
        if not rekap_df.empty:
            # Build download links column via buttons below table
            st.dataframe(rekap_df[show_cols], use_container_width=True, hide_index=True)
            # Optional: quick download selector
            files = [
                f"{r.get('nomor','')}: {r.get('file_name','')}" for _, r in rekap_df.iterrows() if r.get("file_id") and r.get("file_name")
            ]
            mapping = {
                f"{r.get('nomor','')}: {r.get('file_name','')}": (r.get("file_id"), r.get("file_name"))
                for _, r in rekap_df.iterrows()
                if r.get("file_id") and r.get("file_name")
            }
            if files:
                pick = st.selectbox("Pilih surat untuk diunduh:", files, key="rekap_pick")
                if pick:
                    fid, fname = mapping.get(pick, ("", ""))
                    _download_button(fid, fname, key=f"dl_rekap_{hash(pick)}")
        else:
            st.info("Belum ada surat masuk yang direkap.")

        # Add to rekap (Director/Superuser only)
        if role in ["director", "superuser"] and not df.empty:
            st.markdown("#### Masukan ke Daftar Rekap Surat")
            for idx, row in df.sort_values(by="tanggal", ascending=False).iterrows():
                if int(row.get("rekap", 0)) == 0 and int(row.get("director_approved", 0)) == 1:
                    if st.button("Masukan ke Daftar Rekap Surat", key=f"rekap_{row.get('id')}"):
                        _sm_update_by_id(row.get("id"), {"rekap": 1})
                        audit_log("surat_masuk", "rekap_add", target=row.get("id"))
                        st.success("Surat masuk dimasukan ke rekap.")
                        st.rerun()


def surat_keluar_module():
    user = require_login()
    st.header("📤 Surat Keluar")

    # Headers and worksheet helpers
    def _sk_headers():
        return [
            "id", "nomor", "tanggal", "ditujukan", "perihal", "pengirim",
            "status", "follow_up", "director_note", "director_approved",
            "draft_file_id", "draft_name", "draft_link",
            "final_file_id", "final_name", "final_link",
            "created_at", "updated_at", "submitted_by"
        ]

    def _sk_ws():
        try:
            return _get_ws(SURAT_KELUAR_SHEET_NAME)
        except gspread.WorksheetNotFound:
            spreadsheet = get_spreadsheet()
            return ensure_sheet_with_headers(spreadsheet, SURAT_KELUAR_SHEET_NAME, _sk_headers())

    def _sk_read_df():
        ws = _sk_ws()
        headers = _sk_headers()
        try:
            df = pd.DataFrame(_cached_get_all_records(SURAT_KELUAR_SHEET_NAME, headers))
        except Exception:
            df = pd.DataFrame(ws.get_all_records())
        # Ensure columns
        for h in headers:
            if h not in df.columns:
                df[h] = 0 if h in ("director_approved",) else ""
        df["director_approved"] = pd.to_numeric(df.get("director_approved"), errors="coerce").fillna(0).astype(int)
        return ws, df

    def _sk_append(row: dict):
        ws = _sk_ws()
        headers = ws.row_values(1)
        values = [row.get(h, "") for h in headers]
        for i in range(3):
            try:
                ws.append_row(values)
                _invalidate_data_cache()
                break
            except gspread.exceptions.APIError as e:
                if "429" in str(e):
                    time.sleep(1.2 * (i + 1))
                    continue
                raise

    def _sk_update_by_id(sid: str, updates: dict):
        ws = _sk_ws()
        headers = ws.row_values(1)
        id_cell = ws.find(sid)
        if not id_cell:
            raise ValueError("ID tidak ditemukan")
        row_idx = id_cell.row
        for k, v in updates.items():
            if k not in headers:
                continue
            a1 = gspread.utils.rowcol_to_a1(row_idx, headers.index(k) + 1)
            for i in range(3):
                try:
                    ws.update(a1, [[v]])
                    break
                except gspread.exceptions.APIError as e:
                    if "429" in str(e):
                        time.sleep(1.0 * (i + 1))
                        continue
                    raise
        _invalidate_data_cache()

    # Drive helpers
    def _upload_to_drive(file) -> tuple[str, str, str]:
        if not file:
            return "", "", ""
        try:
            service = get_gdrive_service()
            file_metadata = {"name": file.name, "parents": [GDRIVE_FOLDER_ID]}
            media = MediaIoBaseUpload(io.BytesIO(file.getvalue()), mimetype=file.type or "application/octet-stream", resumable=True)
            resp = service.files().create(
                body=file_metadata,
                media_body=media,
                fields="id, webViewLink",
                supportsAllDrives=True,
            ).execute()
            try:
                service.permissions().create(
                    fileId=resp["id"],
                    body={"role": "reader", "type": "anyone"},
                    supportsAllDrives=True,
                ).execute()
            except Exception:
                pass
            return resp.get("id", ""), file.name, resp.get("webViewLink", "")
        except Exception as e:
            st.warning(f"Gagal upload ke Drive: {e}")
            return "", "", ""

    def _download_button(file_id: str, file_name: str, key: str):
        if not file_id or not file_name:
            return
        try:
            service = get_gdrive_service()
            request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
            fh = io.BytesIO()
            fh.write(request.execute())
            fh.seek(0)
            st.download_button(
                label=f"⬇️ Download {file_name}",
                data=fh.getvalue(),
                file_name=file_name,
                mime="application/octet-stream",
                key=key,
            )
        except Exception as e:
            st.caption(f"Tidak dapat download: {e}")

    tab1, tab2, tab3 = st.tabs([
        "📝 Input Draft Surat Keluar",
        "✅ Approval",
        "📋 Daftar & Rekap Surat Keluar",
    ])

    # Tab 1: Input Draft
    with tab1:
        st.markdown("### Input Draft Surat Keluar (Staf)")
        # Pilih jenis draft di luar form agar perubahan memicu rerun dan input muncul dinamis
        st.session_state.setdefault("sk_draft_type", "Upload File")
        st.radio(
            "Jenis Draft Surat",
            ["Upload File", "Link URL"],
            horizontal=True,
            key="sk_draft_type",
        )
        with st.form("sk_add", clear_on_submit=True):
            col1, col2 = st.columns(2)
            with col1:
                nomor = st.text_input("Nomor Surat")
                tanggal = st.date_input("Tanggal", value=date.today())
            with col2:
                ditujukan = st.text_input("Ditujukan Kepada")
                perihal = st.text_input("Perihal")
            # Gunakan pilihan dari radio di luar form agar dinamis
            draft_type = st.session_state.get("sk_draft_type", "Upload File")
            draft_file = None
            draft_link = None
            if draft_type == "Upload File":
                draft_file = st.file_uploader(
                    "Upload Draft Surat (PDF/DOC)",
                    type=["pdf", "doc", "docx"],
                    key="sk_draft_file",
                )
            else:
                draft_link = st.text_input(
                    "Link Draft Surat (Google Drive, dll)",
                    key="sk_draft_link",
                )
            follow_up = st.text_area("Tindak Lanjut (opsional)")
            submit = st.form_submit_button("💾 Simpan Draft Surat Keluar")
            if submit:
                if draft_type == "Upload File" and not draft_file:
                    st.error("File draft surat wajib diupload.")
                elif draft_type == "Link URL" and not draft_link:
                    st.error("Link draft surat wajib diisi.")
                else:
                    sid = gen_id("sk")
                    now = datetime.utcnow().isoformat()
                    draft_file_id, draft_name, draft_web = "", "", ""
                    if draft_type == "Upload File":
                        draft_file_id, draft_name, draft_web = _upload_to_drive(draft_file)
                        if not draft_file_id:
                            st.error("Gagal mengupload draft ke Drive.")
                            st.stop()
                    _sk_append({
                        "id": sid,
                        "nomor": nomor,
                        "tanggal": tanggal.isoformat(),
                        "ditujukan": ditujukan,
                        "perihal": perihal,
                        "pengirim": user.get("full_name") or user.get("email"),
                        "status": "Draft",
                        "follow_up": follow_up,
                        "director_note": "",
                        "director_approved": 0,
                        "draft_file_id": draft_file_id,
                        "draft_name": draft_name,
                        "draft_link": draft_link or draft_web,
                        "final_file_id": "",
                        "final_name": "",
                        "final_link": "",
                        "created_at": now,
                        "updated_at": now,
                        "submitted_by": user.get("email"),
                    })
                    audit_log("surat_keluar", "create", target=sid, details=f"{nomor}-{perihal}; draft={'file:'+draft_name if draft_name else 'url:'+str(draft_link)}")
                    st.success("✅ Surat keluar (draft) tersimpan.")
                    st.rerun()

    # Tab 2: Approval Director
    with tab2:
        st.markdown("### Approval Surat Keluar (Director)")
        role = str(user.get("role", "")).lower()
        if role in ["director", "superuser"]:
            _, df = _sk_read_df()
            for idx, row in df.sort_values(by="tanggal", ascending=False).iterrows():
                with st.expander(f"{row.get('nomor','')} | {row.get('perihal','')} | {row.get('tanggal','')} | Status: {row.get('status','')}"):
                    st.write(f"Ditujukan: {row.get('ditujukan','')}")
                    st.write(f"Pengirim: {row.get('pengirim','')}")
                    st.write(f"Follow Up: {row.get('follow_up','')}")
                    if row.get("draft_file_id") and row.get("draft_name"):
                        st.markdown(f"**Draft Surat (file):** {row.get('draft_name')}")
                        _download_button(row.get("draft_file_id"), row.get("draft_name"), key=f"dl_sk_draft_{row.get('id')}_{idx}")
                    elif row.get("draft_link"):
                        st.markdown(f"**Draft Surat (link):** [Lihat Draft]({row.get('draft_link')})")
                    note = st.text_area("Catatan Director", value=str(row.get("director_note","")), key=f"sk_note_{row.get('id')}")
                    final = st.file_uploader("Upload File Final (wajib untuk status resmi)", key=f"sk_final_{row.get('id')}")
                    colA, colB = st.columns(2)
                    with colA:
                        approve = st.button("✅ Approve & Upload Final", key=f"sk_approve_{row.get('id')}")
                    with colB:
                        disapprove = st.button("❌ Disapprove (Revisi ke Draft)", key=f"sk_disapprove_{row.get('id')}")
                    if approve:
                        if not final:
                            st.error("File final wajib diupload agar surat keluar tercatat resmi.")
                        else:
                            fid, fname, flink = _upload_to_drive(final)
                            if not fid:
                                st.error("Gagal mengupload final ke Drive.")
                            else:
                                _sk_update_by_id(row.get("id"), {
                                    "final_file_id": fid,
                                    "final_name": fname,
                                    "final_link": flink,
                                    "director_note": note,
                                    "director_approved": 1,
                                    "status": "Final",
                                    "updated_at": datetime.utcnow().isoformat(),
                                })
                                audit_log("surat_keluar", "director_approval", target=row.get("id"), details=f"final={fname}; note={note}")
                                st.success("Final uploaded & approved.")
                                st.rerun()
                    if disapprove:
                        _sk_update_by_id(row.get("id"), {
                            "status": "Draft",
                            "director_note": note,
                            "director_approved": 0,
                            "updated_at": datetime.utcnow().isoformat(),
                        })
                        audit_log("surat_keluar", "director_disapprove", target=row.get("id"), details=f"note={note}")
                        st.warning("Surat dikembalikan ke draft untuk direvisi.")
                        st.rerun()
        else:
            st.info("Hanya Director yang dapat meng-approve dan upload file final.")

    # Tab 3: Daftar & Rekap
    with tab3:
        st.markdown("### Daftar & Rekap Surat Keluar")
        _, df = _sk_read_df()
        if not df.empty:
            df = df.copy().sort_values(by="tanggal", ascending=False).reset_index(drop=True)
            # Kode referensi tampilan
            df["indeks"] = [f"SK-{i+1:04d}" for i in range(len(df))]
            st.dataframe(df[[c for c in ["indeks","nomor","tanggal","ditujukan","perihal","pengirim","status","follow_up","final_name"] if c in df.columns]], use_container_width=True, hide_index=True)
            st.markdown("#### Download File Final Surat Keluar")
            for idx, row in df.iterrows():
                if row.get("final_file_id") and row.get("final_name"):
                    st.write(f"{row.get('nomor','')} | {row.get('perihal','')} | {row.get('tanggal','')}")
                    _download_button(row.get("final_file_id"), row.get("final_name"), key=f"dl_sk_final_{row.get('id')}_{idx}")
        else:
            st.info("Belum ada surat keluar.")

        # Rekap Bulanan
        st.markdown("#### 📊 Rekap Bulanan Surat Keluar")
        this_month = date.today().strftime("%Y-%m")
        df_month = pd.DataFrame()
        if not df.empty:
            df_month = df[df["tanggal"].astype(str).str[:7] == this_month]
        st.write(f"Total surat keluar bulan ini: **{len(df_month)}**")
        if not df_month.empty:
            approved = df_month[df_month["director_approved"] == 1]
            draft = df_month[df_month["status"].astype(str).str.lower() == "draft"]
            percent_final = (len(approved) / len(df_month)) * 100 if len(df_month) > 0 else 0
            st.info(f"Approved: {len(approved)} | Masih Draft: {len(draft)} | % Finalisasi: {percent_final:.1f}%")
            # Export Excel/CSV
            export_cols = [c for c in ["nomor","tanggal","ditujukan","perihal","pengirim","status","follow_up","final_name","director_approved"] if c in df_month.columns]
            xbuf = io.BytesIO()
            try:
                df_month[export_cols].to_excel(xbuf, index=False, engine="openpyxl")
            except Exception:
                # Fallback without engine name if not available in env
                df_month[export_cols].to_excel(xbuf, index=False)
            xbuf.seek(0)
            st.download_button("⬇️ Download Rekap Bulanan (Excel)", xbuf, file_name=f"rekap_suratkeluar_{this_month}.xlsx")
            st.download_button("⬇️ Download Rekap Bulanan (CSV)", df_month[export_cols].to_csv(index=False), file_name=f"rekap_suratkeluar_{this_month}.csv")


def mou_module():
    st.header("🤝 MoU")
    st.info("Module coming soon (Sheets + Drive)")


def cash_advance_module():
    st.header("💸 Cash Advance")
    st.info("Module coming soon (Sheets + Drive)")


def pmr_module():
    st.header("📑 PMR")
    st.info("Module coming soon (Sheets + Drive)")


def flex_module():
    st.header("⏰ Flex Time")
    st.info("Module coming soon (Sheets + Drive)")


def delegasi_module():
    st.header("📝 Delegasi")
    st.info("Module coming soon (Sheets + Drive)")


def kalender_pemakaian_mobil_kantor():
    st.header("🚗 Mobil Kantor")
    st.info("Module coming soon (Sheets + Drive)")


def calendar_module():
    st.header("📅 Kalender Bersama")
    st.info("Module coming soon (Sheets + Drive)")


def sop_module():
    st.header("📚 SOP")
    st.info("Module coming soon (Sheets + Drive)")


def notulen_module():
    st.header("🗒️ Notulen")
    st.info("Module coming soon (Sheets + Drive)")


def user_setting_module():
    user = require_login()
    st.header("⚙️ User Setting")
    ws, df = _load_users_df()
    email_col = 'email' if 'email' in df.columns else ('username' if 'username' in df.columns else None)
    if not email_col:
        st.error("Sheet users tidak memiliki kolom email/username.")
        return

    current_identifier = user.get('email')
    row = df[df[email_col].astype(str).str.lower() == str(current_identifier).lower()]
    if row.empty:
        st.warning("Data user Anda tidak ditemukan di database.")

    tab1, tab2 = st.tabs(["👤 Profil", "🔒 Ganti Password"])
    with tab1:
        st.subheader("Update Profil")
        full_name = st.text_input("Nama Lengkap", value=user.get('full_name', ''))
        if st.button("Simpan Profil"):
            try:
                row_idx = _find_user_row(ws, current_identifier)
                if row_idx:
                    _users_update_row(ws, row_idx, {"full_name": full_name})
                    set_current_user({**user, "full_name": full_name})
                    audit_log("users", "update_profile", target=current_identifier, details=f"full_name -> {full_name}")
                    st.success("Profil berhasil diperbarui.")
                else:
                    st.error("Gagal menemukan baris user di sheet.")
            except Exception as e:
                st.error(f"Gagal menyimpan profil: {e}")

    with tab2:
        st.subheader("Ganti Password")
        old_pw = st.text_input("Password Lama", type="password")
        new_pw = st.text_input("Password Baru", type="password")
        new_pw2 = st.text_input("Ulangi Password Baru", type="password")
        if st.button("Simpan Password"):
            if not (old_pw and new_pw and new_pw2):
                st.warning("Semua field password wajib diisi.")
            elif new_pw != new_pw2:
                st.error("Ulangi password baru tidak sama.")
            else:
                try:
                    # verify
                    hashed = row.iloc[0].get('password_hash') if not row.empty else None
                    if not hashed or not verify_password(old_pw, hashed):
                        st.error("Password lama salah.")
                    else:
                        row_idx = _find_user_row(ws, current_identifier)
                        if not row_idx:
                            st.error("Gagal menemukan baris user di sheet.")
                        else:
                            _users_update_row(ws, row_idx, {"password_hash": hash_password(new_pw)})
                            audit_log("users", "change_password", target=current_identifier)
                            st.success("Password berhasil diganti.")
                except Exception as e:
                    st.error(f"Gagal mengganti password: {e}")

    # Director admin panel
    st.markdown("---")
    if str(user.get('role', '')).lower() in ["director", "superuser"]:
        st.subheader("🛠️ Admin Pengguna (Director)")
        # Simple grid
        df_show = df.copy()
        cols_wanted = [c for c in [email_col, 'full_name', 'role', 'active', 'created_at'] if c in df_show.columns]
        st.dataframe(df_show[cols_wanted], use_container_width=True)

        st.markdown("### Edit User")
        sel_user = st.selectbox("Pilih user", df[email_col].astype(str).tolist())
        if sel_user:
            target_row = df[df[email_col].astype(str) == sel_user]
            cur_role = str(target_row.get('role', pd.Series(['user'])).iloc[0]) if not target_row.empty else 'user'
            cur_active = int(pd.to_numeric(target_row.get('active', pd.Series([1])), errors='coerce').fillna(1).iloc[0]) if not target_row.empty else 1
            new_role = st.selectbox("Role", ["user", "staff", "finance", "director", "superuser"], index=["user","staff","finance","director","superuser"].index(cur_role) if cur_role in ["user","staff","finance","director","superuser"] else 0)
            new_active = st.checkbox("Aktif", value=bool(cur_active))
            colA, colB, colC = st.columns(3)
            with colA:
                if st.button("Simpan Perubahan"):
                    try:
                        row_idx = _find_user_row(ws, sel_user)
                        if not row_idx:
                            st.error("User tidak ditemukan di sheet.")
                        else:
                            _users_update_row(ws, row_idx, {"role": new_role, "active": 1 if new_active else 0})
                            audit_log("users", "admin_update", target=sel_user, details=f"role={new_role}; active={int(new_active)}")
                            st.success("Perubahan disimpan.")
                            st.rerun()
                    except Exception as e:
                        st.error(f"Gagal menyimpan: {e}")
            with colB:
                if st.button("Nonaktifkan"):
                    try:
                        row_idx = _find_user_row(ws, sel_user)
                        if row_idx:
                            _users_update_row(ws, row_idx, {"active": 0})
                            audit_log("users", "admin_deactivate", target=sel_user)
                            st.success("User dinonaktifkan.")
                            st.rerun()
                    except Exception as e:
                        st.error(f"Gagal: {e}")
            with colC:
                if st.button("Hapus User", type="primary"):
                    if sel_user.lower() == str(current_identifier).lower():
                        st.error("Tidak dapat menghapus akun Anda sendiri.")
                    else:
                        try:
                            row_idx = _find_user_row(ws, sel_user)
                            if row_idx:
                                _users_delete_row(ws, row_idx)
                                audit_log("users", "admin_delete", target=sel_user)
                                st.success("User dihapus.")
                                st.rerun()
                        except Exception as e:
                            st.error(f"Gagal menghapus: {e}")
    else:
        st.info("Hubungi Director jika perlu perubahan role atau manajemen akun lain.")


def audit_trail_module():
    st.header("🕵️ Audit Trail")
    try:
        ws = _get_ws(AUDIT_SHEET_NAME)
        audit_headers = ["timestamp", "actor", "module", "action", "target", "details"]
        try:
            df = pd.DataFrame(ws.get_all_records(expected_headers=audit_headers))
        except Exception:
            df = pd.DataFrame(ws.get_all_records())
        st.dataframe(df, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Gagal memuat audit trail: {e}")


def superuser_panel():
    st.header("🔑 Superuser Panel")
    st.info("Module coming soon (Sheets + Drive)")


def _cuti_read_df():
    ws = _get_ws(CUTI_SHEET_NAME)
    cuti_headers = [
        "id", "nama", "tgl_mulai", "tgl_selesai", "durasi",
        "kuota_tahunan", "cuti_terpakai", "sisa_kuota", "status",
        "finance_note", "finance_approved", "director_note", "director_approved",
        "alasan", "created_at"
    ]
    try:
        df = pd.DataFrame(_cached_get_all_records(CUTI_SHEET_NAME, cuti_headers))
    except Exception:
        df = pd.DataFrame(ws.get_all_records())
    # Ensure all expected columns exist
    for h in cuti_headers:
        if h not in df.columns:
            df[h] = 0 if h in ("finance_approved", "director_approved", "durasi", "kuota_tahunan", "cuti_terpakai", "sisa_kuota") else ""
    return ws, df


def _cuti_append(row_dict: dict):
    ws = _get_ws(CUTI_SHEET_NAME)
    headers = ws.row_values(1)
    values = [row_dict.get(h, "") for h in headers]
    for i in range(3):
        try:
            ws.append_row(values)
            _invalidate_data_cache()
            break
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                time.sleep(1.2 * (i + 1))
                continue
            raise


def _cuti_update_by_id(cid: str, updates: dict):
    ws = _get_ws(CUTI_SHEET_NAME)
    headers = ws.row_values(1)
    id_cell = ws.find(cid)
    if not id_cell:
        raise ValueError("ID tidak ditemukan")
    row_idx = id_cell.row
    # Update each cell individually (simpler, avoids API mismatch)
    for k, v in updates.items():
        if k not in headers:
            continue
        col_idx = headers.index(k) + 1
        a1 = gspread.utils.rowcol_to_a1(row_idx, col_idx)
        for i in range(3):
            try:
                ws.update(a1, [[v]])
                break
            except gspread.exceptions.APIError as e:
                if "429" in str(e):
                    time.sleep(1.0 * (i + 1))
                    continue
                raise
    _invalidate_data_cache()


def main():
    ensure_core_sheets()
    # --- Sidebar Logo ---
    user = get_current_user()
    if not user:
        # --- Full page login/register, no sidebar ---
        st.markdown("""
            <style>
            [data-testid="stSidebar"], .stSidebar {display: none !important;}
            .center-login {
                max-width: 400px;
                margin: 5% auto 0 auto;
                background: #fff;
                border-radius: 12px;
                box-shadow: 0 2px 16px rgba(80,140,255,0.10);
                padding: 2.5rem 2.5rem 2rem 2.5rem;
            }
            .center-login h2 {text-align:center; color:#2563eb; margin-bottom:1.5rem;}
            .center-login .stTextInput>div>input, .center-login .stTextInput>div>textarea {
                border-radius: 6px; border: 1px solid #b3d1ff;
            }
            .center-login .stButton>button {
                width: 100%; margin-top: 1rem; font-weight: 600;
            }
            .center-login .stTabs {
                margin-bottom: 1.5rem;
            }
            </style>
        """, unsafe_allow_html=True)
        st.markdown('<div class="center-login">', unsafe_allow_html=True)
        st.image(os.path.join(os.path.dirname(__file__), "logo.png"), width=160)
        st.markdown("<h2>WIJNA Manajemen System</h2>", unsafe_allow_html=True)
        tabs = st.tabs(["Login", "Register"])
        with tabs[0]:
            email = st.text_input("Email", key="login_email", placeholder="Masukkan email Anda")
            pwd = st.text_input("Password", type="password", key="login_pw", placeholder="Masukkan password Anda")
            if st.button("Login", key="login_btn"):
                if not email or not pwd:
                    st.warning("Email dan password wajib diisi.")
                else:
                    ok, msg = login_user(email.strip().lower(), pwd)
                    if ok:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)
        with tabs[1]:
            r_email = st.text_input("Email (register)", key="r_email2")
            r_name = st.text_input("Nama lengkap", key="r_name2")
            r_pw = st.text_input("Password", type="password", key="r_pw2")
            if st.button("Register", key="register_btn"):
                if not (r_email and r_name and r_pw):
                    st.error("Lengkapi semua field.")
                else:
                    ok, msg = register_user(r_email.strip().lower(), r_name.strip(), r_pw)
                    if ok:
                        st.success(msg)
                    else:
                        st.error(msg)
        st.markdown('</div>', unsafe_allow_html=True)
        st.stop()

    # --- Sidebar/menu for logged in user ---
    logo_path = os.path.join(os.path.dirname(__file__), "logo.png")
    st.sidebar.image(logo_path, use_container_width=True)
    st.sidebar.markdown("<h2 style='text-align:center;margin-bottom:0.5em;'>WIJNA Manajemen System</h2>", unsafe_allow_html=True)
    auth_sidebar()

    menu = [
        ("Dashboard", "🏠 Dashboard"),
        ("Inventory", "📦 Inventory"),
        ("Surat Masuk", "📥 Surat Masuk"),
        ("Surat Keluar", "📤 Surat Keluar"),
        ("MoU", "🤝 MoU"),
        ("Cash Advance", "💸 Cash Advance"),
        ("PMR", "📑 PMR"),
        ("Cuti", "🌴 Cuti"),
        ("Flex Time", "⏰ Flex Time"),
        ("Delegasi", "📝 Delegasi"),
        ("Mobil Kantor", "🚗 Mobil Kantor"),
        ("Kalender Bersama", "📅 Kalender Bersama"),
        ("SOP", "📚 SOP"),
        ("Notulen", "🗒️ Notulen"),
        ("User Setting", "⚙️ User Setting"),
        ("Audit Trail", "🕵️ Audit Trail"),
        ("Superuser Panel", "🔑 Superuser Panel")
    ]
    if "page" not in st.session_state:
        st.session_state["page"] = "Dashboard"

    # CSS agar tombol navigasi seragam dan rapi
    st.sidebar.markdown(
        """
        <style>
        .wijna-nav-btn > button {
            width: 100% !important;
            min-height: 42px !important;
            font-size: 1.05rem !important;
            margin-bottom: 6px !important;
            border-radius: 6px !important;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    st.sidebar.markdown("---")
    st.sidebar.markdown("### Navigasi Modul")
    nav_cols = st.sidebar.columns(2)
    for idx, (key, label) in enumerate(menu):
        col = nav_cols[idx % 2]
        with col:
            btn = st.button(label, key=f"nav_{key}", help=key, use_container_width=True)
            if btn:
                st.session_state["page"] = key
                st.rerun()

    # --- Logout button at the very bottom ---
    if user:
        st.sidebar.markdown("<div style='height:32px'></div>", unsafe_allow_html=True)
        if st.sidebar.button("Logout", key="sidebar_logout", use_container_width=True):
            logout()
            st.rerun()

    choice = st.session_state["page"]

    # route
    if choice == "Dashboard":
        if user:
            dashboard()
        else:
            st.title("Selamat datang — silakan login/register di sidebar")
    elif choice == "Inventory":
        inventory_module()
    elif choice == "Surat Masuk":
        surat_masuk_module()
    elif choice == "Surat Keluar":
        surat_keluar_module()
    elif choice == "MoU":
        mou_module()
    elif choice == "Cash Advance":
        cash_advance_module()
    elif choice == "Cuti":
            user = require_login()
            st.header("🌴 Pengajuan & Approval Cuti")
            st.markdown("<div style='color:#2563eb;font-size:1.1rem;margin-bottom:1.2em'>Kelola pengajuan cuti, review finance, dan approval director secara terintegrasi.</div>", unsafe_allow_html=True)
            tab1, tab2, tab3 = st.tabs(["📝 Ajukan Cuti", "💰 Review Finance", "✅ Approval Director & Rekap"])
            # Tab 1: Ajukan Cuti
            with tab1:
                st.markdown("### 📝 Ajukan Cuti")
                nama = user.get("full_name") or user.get("email")
                tgl_mulai = st.date_input("Tanggal Mulai", value=date.today())
                tgl_selesai = st.date_input("Tanggal Selesai", value=date.today())
                alasan = st.text_area("Alasan Cuti")
                durasi = (tgl_selesai - tgl_mulai).days + 1 if tgl_selesai >= tgl_mulai else 0
                # Ambil info kuota terakhir pengguna
                _, df_cuti_all = _cuti_read_df()
                df_user = df_cuti_all[df_cuti_all.get("nama", pd.Series()).astype(str) == str(nama)]
                if not df_user.empty:
                    last = df_user.sort_values(by="tgl_mulai", ascending=False).iloc[0]
                    kuota_tahunan = int(last.get("kuota_tahunan", 12)) if str(last.get("kuota_tahunan", "")).strip() else 12
                    cuti_terpakai = int(last.get("cuti_terpakai", 0)) if str(last.get("cuti_terpakai", "")).strip() else 0
                else:
                    kuota_tahunan = 12
                    cuti_terpakai = 0
                sisa_kuota = kuota_tahunan - cuti_terpakai
                st.info(f"Sisa kuota cuti: {sisa_kuota} hari dari {kuota_tahunan} hari")
                st.write(f"Durasi cuti diajukan: {durasi} hari")
                if durasi > 0 and sisa_kuota < durasi:
                    st.error("Sisa kuota tidak cukup, pengajuan cuti otomatis ditolak.")
                if st.button("Ajukan Cuti"):
                    if not alasan or durasi <= 0:
                        st.warning("Lengkapi data dan pastikan tanggal benar.")
                    elif sisa_kuota < durasi:
                        st.error("Sisa kuota tidak cukup, pengajuan cuti ditolak.")
                    else:
                        cid = gen_id("cuti")
                        now = datetime.utcnow().isoformat()
                        _cuti_append({
                            "id": cid,
                            "nama": nama,
                            "tgl_mulai": tgl_mulai.isoformat(),
                            "tgl_selesai": tgl_selesai.isoformat(),
                            "durasi": int(durasi),
                            "kuota_tahunan": int(kuota_tahunan),
                            "cuti_terpakai": int(cuti_terpakai),
                            "sisa_kuota": int(sisa_kuota),
                            "status": "Menunggu Review Finance",
                            "finance_note": "",
                            "finance_approved": 0,
                            "director_note": "",
                            "director_approved": 0,
                            "alasan": alasan,
                            "created_at": now
                        })
                        st.success("Pengajuan cuti berhasil diajukan.")
                        try:
                            audit_log("cuti", "create", target=cid, details=f"{nama} ajukan cuti {tgl_mulai} s/d {tgl_selesai} ({durasi} hari)")
                        except Exception:
                            pass
                        st.rerun()
            # Tab 2: Review Finance
            with tab2:
                st.markdown("### Review & Approval Finance")
                if user.get("role") in ["finance", "superuser"]:
                    _, df = _cuti_read_df()
                    # Normalize types
                    if "finance_approved" in df.columns:
                        df["finance_approved"] = pd.to_numeric(df["finance_approved"], errors="coerce").fillna(0).astype(int)
                    pending = df[df.get("finance_approved", 0) == 0]
                    for idx, row in pending.sort_values(by="tgl_mulai", ascending=False).iterrows():
                        with st.expander(f"{row.get('nama')} | {row.get('tgl_mulai')} s/d {row.get('tgl_selesai')}"):
                            st.write(f"Durasi: {row.get('durasi')} hari, Sisa kuota: {row.get('sisa_kuota')} hari")
                            st.write(f"Alasan: {row.get('alasan', '')}")
                            note = st.text_area("Catatan Finance", value=row.get("finance_note") or "", key=f"fin_note_{row.get('id')}")
                            approve = st.checkbox("Approve", value=bool(int(row.get("finance_approved", 0))), key=f"fin_appr_{row.get('id')}")
                            if st.button("Simpan Review", key=f"fin_save_{row.get('id')}"):
                                status = "Menunggu Approval Director" if approve else "Ditolak Finance"
                                try:
                                    _cuti_update_by_id(row.get('id'), {
                                        "finance_note": note,
                                        "finance_approved": int(bool(approve)),
                                        "status": status
                                    })
                                    st.success("Review Finance disimpan.")
                                    try:
                                        audit_log("cuti", "finance_review", target=row.get('id'), details=f"approve={bool(approve)}; status={status}")
                                    except Exception:
                                        pass
                                except Exception as e:
                                    st.error(f"Gagal menyimpan: {e}")
                                st.rerun()
                else:
                    st.info("Hanya Finance/Superuser yang dapat review di sini.")
            # Tab 3: Approval Director & Rekap
            with tab3:
                st.markdown("### Approval Director & Rekap Cuti")
                if user.get("role") in ["director", "superuser"]:
                    _, df = _cuti_read_df()
                    df["finance_approved"] = pd.to_numeric(df.get("finance_approved", 0), errors="coerce").fillna(0).astype(int)
                    df["director_approved"] = pd.to_numeric(df.get("director_approved", 0), errors="coerce").fillna(0).astype(int)
                    for idx, row in df[df["finance_approved"] == 1].sort_values(by="tgl_mulai", ascending=False).iterrows():
                        with st.expander(f"{row.get('nama')} | {row.get('tgl_mulai')} s/d {row.get('tgl_selesai')}"):
                            st.write(f"Durasi: {row.get('durasi')} hari, Sisa kuota: {row.get('sisa_kuota')} hari")
                            st.write(f"Alasan: {row.get('alasan', '')}")
                            note = st.text_area("Catatan Director", value=row.get("director_note") or "", key=f"dir_note_{row.get('id')}")
                            approve = st.checkbox("Approve", value=bool(int(row.get("director_approved", 0))), key=f"dir_appr_{row.get('id')}")
                            if st.button("Simpan Approval", key=f"dir_save_{row.get('id')}"):
                                try:
                                    if approve:
                                        dur = int(pd.to_numeric(row.get("durasi", 0), errors="coerce") or 0)
                                        terpakai = int(pd.to_numeric(row.get("cuti_terpakai", 0), errors="coerce") or 0)
                                        kuota = int(pd.to_numeric(row.get("kuota_tahunan", 12), errors="coerce") or 12)
                                        baru_terpakai = terpakai + dur
                                        sisa = kuota - baru_terpakai
                                        _cuti_update_by_id(row.get('id'), {
                                            "director_note": note,
                                            "director_approved": 1,
                                            "status": "Disetujui Director",
                                            "cuti_terpakai": baru_terpakai,
                                            "sisa_kuota": sisa
                                        })
                                    else:
                                        _cuti_update_by_id(row.get('id'), {
                                            "director_note": note,
                                            "director_approved": 0,
                                            "status": "Ditolak Director"
                                        })
                                    st.success("Approval Director disimpan.")
                                    try:
                                        audit_log("cuti", "director_approval", target=row.get('id'), details=f"approve={bool(approve)}")
                                    except Exception:
                                        pass
                                except Exception as e:
                                    st.error(f"Gagal menyimpan: {e}")
                                st.rerun()
                # Rekap semua pengajuan cuti
                st.markdown("#### Rekap Pengajuan Cuti")
                try:
                    _, df_all = _cuti_read_df()
                    st.dataframe(df_all.sort_values(by="tgl_mulai", ascending=False), use_container_width=True, hide_index=True)
                except Exception as e:
                    st.error(f"Gagal memuat data cuti: {e}")
    elif choice == "PMR":
        pmr_module()
    elif choice == "Flex Time":
        flex_module()
    elif choice == "Delegasi":
        delegasi_module()
    elif choice == "Mobil Kantor":
        kalender_pemakaian_mobil_kantor()
    elif choice == "Kalender Bersama":
        calendar_module()
    elif choice == "SOP":
        sop_module()
    elif choice == "Notulen":
        notulen_module()
    elif choice == "User Setting":
        user_setting_module()
    elif choice == "Audit Trail":
        audit_trail_module()
    elif choice == "Superuser Panel":
        superuser_panel()

if __name__ == "__main__":
    main()
