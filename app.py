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
import threading

# --- 1. KONFIGURASI APLIKASI ---
# PENTING: Pastikan ID ini berasal dari folder di dalam SHARED DRIVE
GDRIVE_FOLDER_ID = "1CxYo2ZGu8jweKjmEws41nT3cexJju5_1" 
USERS_SHEET_NAME = "users"
CUTI_SHEET_NAME = "cuti"
AUDIT_SHEET_NAME = "audit_log"
INVENTORY_SHEET_NAME = "inventory"
SURAT_MASUK_SHEET_NAME = "surat_masuk"
SURAT_KELUAR_SHEET_NAME = "surat_keluar"
CONFIG_SHEET_NAME = "config"
SPREADSHEET_URL = st.secrets["connections"]["gsheets"]["spreadsheet"]
# ADMIN_EMAIL_RECIPIENT sekarang dikosongkan; seluruh notifikasi dikendalikan oleh
# pemetaan per modul & aksi melalui NOTIF_ROLE_MAP di bawah. Jika ingin fallback
# khusus (misal selalu kirim ke tim IT), isi dengan alamat distribution list.
ADMIN_EMAIL_RECIPIENT = ""
ALLOWED_ROLES = ["user", "staff", "finance", "director", "superuser"]

# Pemetaan default notifikasi: (module, action) -> daftar role penerima utama.
# Catatan: superuser selalu otomatis ditambahkan oleh helper.
# Tambahkan / ubah sesuai kebutuhan bisnis Anda.
NOTIF_ROLE_MAP: dict[tuple[str, str], list[str]] = {
    ("inventory", "create"): ["finance"],
    ("inventory", "finance_review"): ["director"],
    ("inventory", "director_approved"): ["finance"],  # misal informasikan balik ke finance
    ("inventory", "director_reject"): ["finance"],
    ("surat_masuk", "draft"): ["director"],
    ("surat_masuk", "director_approved"): ["finance"],
    ("surat_keluar", "draft"): ["director"],
    ("surat_keluar", "final_upload"): ["finance"],
    ("cuti", "submit"): ["finance"],
    ("cuti", "finance_review"): ["director"],
    ("cuti", "director_approved"): ["finance"],
    ("cuti", "director_reject"): ["finance"],
    # Auth events
    ("auth", "login"): ["superuser"],
    ("auth", "logout"): ["superuser"],
    ("users", "register"): ["superuser"],
}

@st.cache_data(ttl=60, show_spinner=False)
def load_config_notif_map() -> dict[tuple[str, str], list[str]]:
    """Load dynamic notification role mapping from config sheet.
    Sheet schema: module | action | roles | active | updated_at | updated_by
    - roles: comma-separated roles, e.g. "finance,director"
    - active: 1/0 or TRUE/FALSE; only active==1 considered
    Returns dict with (module, action) => [roles]
    """
    mapping: dict[tuple[str, str], list[str]] = {}
    try:
        ws = _get_ws(CONFIG_SHEET_NAME)
        records = ws.get_all_records()
        for rec in records:
            try:
                mod = str(rec.get("module", "")).strip().lower()
                act = str(rec.get("action", "")).strip().lower()
                roles_raw = str(rec.get("roles", "")).strip()
                active_val = str(rec.get("active", "1")).strip().lower()
                if not mod or not act or not roles_raw:
                    continue
                if active_val not in ("1", "true", "yes", "y"):  # treat others as inactive
                    continue
                roles_list = [r.strip().lower() for r in roles_raw.split(',') if r.strip()]
                if not roles_list:
                    continue
                mapping[(mod, act)] = roles_list
            except Exception:
                continue
    except Exception:
        return {}
    return mapping

def notify_event(module: str, action: str, subject: str, body: str, roles: list[str] | None = None):
    """Kirim notifikasi email berbasis module & action.
    - roles: override manual; bila None akan lookup NOTIF_ROLE_MAP.
    - superuser + ADMIN_EMAIL_RECIPIENT ditambahkan otomatis oleh _notify_roles.
    - Diam (silent) jika tidak ada peran terpetakan.
    """
    try:
        if roles is not None:
            target_roles = roles
        else:
            # First try dynamic config sheet
            dyn_map = load_config_notif_map()
            target_roles = dyn_map.get((module.lower(), action.lower()))
            if not target_roles:
                # fallback to static default
                target_roles = NOTIF_ROLE_MAP.get((module, action), [])
        if not target_roles or not isinstance(target_roles, list):
            return
        _notify_roles(list(set(target_roles)), subject, body)
    except Exception:
        pass

def _load_settings_row() -> dict:
    """Read special settings row from config sheet where module='__settings__'. Returns dict."""
    try:
        ws = _get_ws(CONFIG_SHEET_NAME)
        records = ws.get_all_records()
        for rec in records:
            if str(rec.get("module", "")).strip().lower() == "__settings__":
                return rec
    except Exception:
        return {}
    return {}

@st.cache_data(ttl=60, show_spinner=False)
def is_superuser_auto_enabled() -> bool:
    """Check settings to decide whether superuser auto inclusion is active. Default True if not set."""
    row = _load_settings_row()
    val = str(row.get("superuser_auto", "1")) if row else "1"
    return val.strip().lower() in ("1", "true", "yes", "y")
ICON_PATH = os.path.join(os.path.dirname(__file__), "icon.png")
# Use centered layout on login screen; switch to wide after user logs in.
_layout_mode = "wide" if st.session_state.get("user") else "centered"
st.set_page_config(page_title="WIJNA Management System", page_icon=ICON_PATH, layout=_layout_mode)

# --- Compatibility Helpers (Deprecation safe) ---
def safe_dataframe(df, *, index=True, height=None, key=None, use_container: bool = True, **kwargs):
    """Wrapper untuk transisi dari use_container_width -> width='stretch'.
    Param:
      - use_container: jika True (default) akan pakai width='stretch'.
      - kwargs lain diteruskan ke st.dataframe.
    """
    try:
        if use_container:
            return st.dataframe(df, width='stretch', height=height, hide_index=not index, key=key, **kwargs)
        else:
            return st.dataframe(df, height=height, hide_index=not index, key=key, **kwargs)
    except TypeError:
        # Versi Streamlit lama belum dukung width argumen baru
        return st.dataframe(df, height=height, hide_index=not index, key=key, **kwargs)

def safe_image(image, *, caption=None, clamp=False, channels="RGB", output_format="auto", use_container: bool = True, **kwargs):
    """Wrapper image untuk hindari width=None invalid di versi baru.
    - Jika use_container True, kita biarkan Streamlit autosize tanpa mengirim width=None eksplisit.
    - Jika ada argumen width=None, diabaikan.
    """
    if 'width' in kwargs and kwargs['width'] is None:
        kwargs.pop('width')
    try:
        return st.sidebar.image(image, caption=caption, clamp=clamp, channels=channels, output_format=output_format, **kwargs)
    except Exception:
        try:
            return st.image(image, caption=caption, clamp=clamp, channels=channels, output_format=output_format, **kwargs)
        except Exception:
            pass

# Ensure the browser tab title is exactly as desired on some Streamlit versions that append '• Streamlit'.
def _enforce_page_title():
    try:
        import streamlit.components.v1 as components
        components.html(
            "<script>window.parent.document.title = 'WIJNA Management System';</script>",
            height=0,
        )
    except Exception:
        # Non-blocking; fall back to set_page_config title
        pass

_enforce_page_title()


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
        creds = st.secrets.get("email_credentials", {})
        sender_email = (creds.get("username") or "").strip()
        sender_password = (creds.get("app_password") or "").strip()
        if not sender_email or not sender_password:
            st.toast("Konfigurasi email tidak lengkap (username/app_password).")
            return False

        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = recipient_email
        message["Subject"] = subject
        message.attach(MIMEText(body or "", "plain"))

        # Coba TLS 587 kemudian fallback ke SSL 465 jika gagal
        for attempt in range(2):
            try:
                if attempt == 0:
                    server = smtplib.SMTP("smtp.gmail.com", 587, timeout=20)
                    server.ehlo()
                    server.starttls()
                    server.login(sender_email, sender_password)
                else:
                    server = smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=20)
                    server.login(sender_email, sender_password)
                server.send_message(message)
                server.quit()
                st.toast(f"📧 Notifikasi email terkirim ke {recipient_email}")
                return True
            except Exception as inner_e:
                if attempt == 1:
                    raise inner_e
                # tunggu sebentar lalu coba SSL
                try:
                    time.sleep(1.0)
                except Exception:
                    pass
        return False
    except Exception as e:
        st.toast(f"Gagal mengirim email: {e}")
        return False

def send_notification_bulk(recipients: list[str], subject: str, body: str) -> tuple[int, int]:
    """Kirim email ke banyak penerima dalam satu sesi SMTP untuk performa lebih baik.
    Return: (jumlah_terkirim, total_penerima)
    Catatan: Fungsi ini tidak menampilkan toast per penerima untuk menghindari lag.
    """
    try:
        # Normalisasi dan dedupe
        recipients = sorted({(e or "").strip() for e in recipients if (e or "").strip()})
        total = len(recipients)
        if total == 0:
            return 0, 0

        creds = st.secrets.get("email_credentials", {})
        sender_email = (creds.get("username") or "").strip()
        sender_password = (creds.get("app_password") or "").strip()
        if not sender_email or not sender_password:
            return 0, total

        def build_msg(to_addr: str):
            msg = MIMEMultipart()
            msg["From"] = sender_email
            msg["To"] = to_addr or "Undisclosed recipients"
            msg["Subject"] = subject
            msg.attach(MIMEText(body or "", "plain"))
            return msg

        sent = 0
        # 1) Coba TLS 587 dahulu
        try:
            server = smtplib.SMTP("smtp.gmail.com", 587, timeout=20)
            try:
                server.ehlo()
                server.starttls()
                server.login(sender_email, sender_password)
                for rcpt in recipients:
                    try:
                        server.sendmail(sender_email, [rcpt], build_msg(rcpt).as_string())
                        sent += 1
                    except Exception:
                        # lanjut ke penerima berikutnya
                        pass
            finally:
                try:
                    server.quit()
                except Exception:
                    pass
            return sent, total
        except Exception:
            # 2) Fallback SSL 465
            try:
                server = smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=20)
                try:
                    server.login(sender_email, sender_password)
                    for rcpt in recipients:
                        try:
                            server.sendmail(sender_email, [rcpt], build_msg(rcpt).as_string())
                            sent += 1
                        except Exception:
                            pass
                finally:
                    try:
                        server.quit()
                    except Exception:
                        pass
                return sent, total
            except Exception:
                return 0, total
    except Exception:
        return 0, 0

def _send_async(func, *args, **kwargs):
    """Jalankan fungsi di background thread agar UI tidak menunggu I/O jaringan."""
    try:
        t = threading.Thread(target=func, args=args, kwargs=kwargs, daemon=True)
        t.start()
    except Exception:
        try:
            func(*args, **kwargs)
        except Exception:
            pass

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

        # Config sheet for dynamic notification mappings
        config_headers = ["module", "action", "roles", "active", "updated_at", "updated_by"]
        ensure_sheet_with_headers(spreadsheet, CONFIG_SHEET_NAME, config_headers)

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
                        
                        # Kirim notifikasi ke seluruh SUPERUSER saat LOGIN
                        email_subject = "Notifikasi: User Login"
                        email_body = f"User '{username}' telah berhasil LOGIN ke aplikasi Anda."
                        try:
                            notify_event("auth", "login", email_subject, email_body)
                        except Exception:
                            pass
                        
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

                    # Kirim notifikasi ke seluruh SUPERUSER saat REGISTRASI
                    email_subject = "Notifikasi: User Baru Telah Mendaftar"
                    email_body = f"User baru dengan username '{new_username}' telah berhasil mendaftar di aplikasi Anda."
                    try:
                        notify_event("users", "register", email_subject, email_body)
                    except Exception:
                        pass

def show_main_app():
    """Menampilkan aplikasi utama setelah user berhasil login."""
    st.sidebar.success(f"Login sebagai: **{st.session_state.username}**")
    if st.sidebar.button("Logout"):
        
        # Kirim notifikasi ke seluruh SUPERUSER saat LOGOUT
        email_subject = "Notifikasi: User Logout"
        email_body = f"User '{st.session_state.username}' telah LOGOUT dari aplikasi Anda."
        try:
            notify_event("auth", "logout", email_subject, email_body)
        except Exception:
            pass
        
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
                    try:
                        audit_log("drive", "upload", target=file.get('id', ''), details=f"name={uploaded_file.name}; type={uploaded_file.type}")
                    except Exception:
                        pass
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
            notify_event("auth", "logout", "Notifikasi: User Logout", f"User '{user.get('email')}' telah LOGOUT dari aplikasi Anda.")
        except Exception:
            pass
        # Audit logout event
        try:
            audit_log("auth", "logout", target=user.get("email", ""))
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
            notify_event("auth", "login", "Notifikasi: User Login", f"User '{user_obj['email']}' telah berhasil LOGIN.")
        except Exception:
            pass
        # Audit login event
        try:
            audit_log("auth", "login", target=user_obj.get("email", ""))
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
            notify_event("users", "register", "Notifikasi: User Baru", f"User baru '{email}' telah mendaftar.")
        except Exception:
            pass
        # Audit register
        try:
            audit_log("users", "register", target=email)
        except Exception:
            pass
        return True, "Registrasi berhasil."
    except Exception as e:
        return False, f"Gagal registrasi: {e}"


def auth_sidebar():
    user = get_current_user()
    if user:
                full_name = (user.get('full_name') or '').strip()
                email = (user.get('email') or '').strip()
                role = str(user.get('role', 'user')).strip().lower() or 'user'
                role_label = 'Superuser' if role == 'superuser' else role.title()

                # Small profile card in the sidebar
                st.sidebar.markdown(
                        f"""
                        <div style="background:#EEF2FF;border:1px solid #c7d2fe;border-radius:10px;padding:12px 12px;margin:8px 0 14px 0;">
                            <div style="font-weight:700;color:#1f2937;display:flex;align-items:center;gap:8px;">
                                <span>👤</span>
                                <span>{full_name or email}</span>
                            </div>
                            {f'<div style="font-size:12px;color:#4b5563;margin-left:24px;">{email}</div>' if full_name and email else ''}
                            <div style="margin-top:8px;margin-left:24px;">
                                <span style="background:#e0e7ff;color:#3730a3;padding:2px 10px;border-radius:999px;font-size:12px;font-weight:600;">{role_label}</span>
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                )


def _get_emails_by_role(role: str) -> list[str]:
    """Kembalikan list email untuk user dengan role tertentu dan active=1 (jika ada)."""
    try:
        _, df = _load_users_df()
        if df is None or df.empty:
            return []
        role_mask = df.get('role', pd.Series(dtype=str)).astype(str).str.lower() == str(role).lower()
        # Interpret kolom active lebih fleksibel: terima 1/"1"/true/yes/y/aktif/active
        if 'active' in df.columns:
            active_col = df['active'].astype(str).str.strip().str.lower()
            truthy_values = {"1", "true", "yes", "y", "aktif", "active"}
            # Jika kolom numeric (0/1) tetap akan cocok dgn "1"
            active_mask = active_col.isin(truthy_values)
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
    """Kirim email ke semua user dengan role tersebut (async + bulk)."""
    emails = _get_emails_by_role(role)
    # Tambahkan superuser dan admin (fallback)
    superusers = []
    try:
        superusers = _get_emails_by_role("superuser")
    except Exception:
        pass
    admin_email = (ADMIN_EMAIL_RECIPIENT or "").strip() if ADMIN_EMAIL_RECIPIENT else ""
    pool = set(emails or []) | set(superusers or []) | ({admin_email} if admin_email else set())
    recipients = sorted(e for e in pool if e)
    if recipients:
        _send_async(send_notification_bulk, recipients, subject, body)
        st.toast(f"Mengirim notifikasi ke {len(recipients)} penerima ({role})…")
    else:
        st.toast(f"Tidak ada penerima untuk role {role}")


def _notify_roles(roles: list[str], subject: str, body: str):
    """Kirim email ke gabungan beberapa role (dedupe), selalu menyertakan superuser. Async + bulk."""
    unique_roles = {*(r.strip().lower() for r in roles if r)}
    if is_superuser_auto_enabled():
        unique_roles.add("superuser")
    all_emails: set[str] = set()  # Initialize a set to store unique emails
    for r in unique_roles:
        try:
            for e in _get_emails_by_role(r):
                if e:
                    all_emails.add(str(e).strip())
        except Exception:
            continue
    # Add admin fallback if available
    admin_email = (ADMIN_EMAIL_RECIPIENT or "").strip() if ADMIN_EMAIL_RECIPIENT else ""
    if admin_email:
        all_emails.add(admin_email)
    recipients = sorted(all_emails)
    if recipients:
        _send_async(send_notification_bulk, recipients, subject, body)
        st.toast(f"Mengirim notifikasi ke {len(recipients)} penerima ({', '.join(sorted(unique_roles))})…")
    else:
        st.toast("Tidak ada penerima notifikasi untuk roles: " + ", ".join(sorted(unique_roles)))


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
    st.write("Selamat datang di WIJNA Management System.")


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
            # Audit upload attachment
            try:
                audit_log("inventory", "upload_attachment", target=resp.get('id', ''), details=f"name={file.name}; type={file.type}")
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
                            notify_event("inventory", "create", "[WIJNA] Draft Inventaris Baru",
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
            if df.empty:
                st.info("Tidak ada item untuk direview.")
                return
            df['finance_approved'] = pd.to_numeric(df.get('finance_approved'), errors='coerce').fillna(0).astype(int)
            pending = df[df['finance_approved'] == 0].copy()
            if pending.empty:
                st.info("Tidak ada item pending untuk Finance.")
                return
            options = [f"{r['name']} (ID: {r['id']})" for _, r in pending.iterrows()]
            mapping = {f"{r['name']} (ID: {r['id']})": r['id'] for _, r in pending.iterrows()}
            pick = st.selectbox("Pilih item untuk direview:", options, key="fin_pick_inv")
            if not pick:
                return
            sel_id = mapping[pick]
            r = pending[pending['id'] == sel_id].iloc[0]
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
                drive_download_button(file_id, file_name, key=f"dl_fin_{r.get('id')}")
            note = st.text_area("Catatan Finance (opsional)", value=r.get("finance_note") or "", key=f"fin_note_{r.get('id')}")
            if st.button("🔎 Simpan Review Finance", key=f"ap_fin_{r.get('id')}"):
                try:
                    _inv_update_by_id(r.get('id'), {"finance_note": note, "finance_approved": 1})
                    try:
                        audit_log("inventory", "finance_review", target=r.get('id'), details=str(note))
                    except Exception:
                        pass
                    try:
                        notify_event("inventory", "finance_review", "[WIJNA] Inventaris Menunggu Approval Director",
                                      f"Item inventaris telah direview Finance dan menunggu Approval Director.\n\nNama: {r.get('name')}\nID: {r.get('id')}\nLokasi: {r.get('location')}\nStatus: {r.get('status')}\nPIC: {r.get('pic','')}")
                    except Exception:
                        pass
                    st.success("Finance reviewed. Menunggu persetujuan Director.")
                except Exception as e:
                    st.error(f"Gagal menyimpan: {e}")
                st.rerun()

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
            rows = df[(df['finance_approved'] == 1) & (df['director_approved'] == 0)].copy()
            if rows.empty:
                st.info("Tidak ada item yang menunggu Approval Director.")
                return
            options = [f"{r['name']} (ID: {r['id']})" for _, r in rows.iterrows()]
            mapping = {f"{r['name']} (ID: {r['id']})": r['id'] for _, r in rows.iterrows()}
            pick = st.selectbox("Pilih item untuk disetujui/ditolak:", options, key="dir_pick_inv")
            if not pick:
                return
            sel_id = mapping[pick]
            r = rows[rows['id'] == sel_id].iloc[0]
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
                drive_download_button(r.get('file_id'), r.get('file_name'), key=f"dl_dir_{r.get('id')}")
            note2 = st.text_area("Catatan Director", value=r.get("director_note") or "", key=f"dir_note_{r.get('id')}", height=80)
            colA, colB = st.columns([1,1])
            with colA:
                if st.button("✅ Approve", key=f"ap_dir_{r.get('id')}"):
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
                if st.button("❌ Tolak", key=f"reject_dir_{r.get('id')}"):
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
                st.dataframe(show_df, width='stretch')

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

            # Pilih satu item saja agar UI ringan
            available = filtered_df2[filtered_df2.get('status','') != "Dipinjam"].copy()
            if available.empty:
                st.info("Tidak ada barang yang bisa dipinjam sesuai filter.")
            else:
                opts = [f"{r['name']} (ID: {r['id']})" for _, r in available.iterrows()]
                map_id = {f"{r['name']} (ID: {r['id']})": r['id'] for _, r in available.iterrows()}
                choose = st.selectbox("Pilih barang untuk dipinjam:", opts, key="pinjam_pick")
                if choose:
                    rid = map_id[choose]
                    row = available[available['id'] == rid].iloc[0]
                    keperluan = st.text_input("Keperluan pinjam", key=f"keperluan_{rid}")
                    tgl_kembali = st.date_input("Tanggal Kembali", key=f"tglkembali_{rid}", min_value=date.today())
                    if st.button("Ajukan Pinjam", key=f"ajukan_{rid}"):
                        try:
                            info_pic = f"{user.get('email')}|{keperluan}|{tgl_kembali}|0|0"
                            _inv_update_by_id(rid, {
                                "loan_info": info_pic,
                                "finance_approved": 0,
                                "director_approved": 0,
                                "updated_at": datetime.utcnow().isoformat()
                            })
                            try:
                                audit_log("inventory", "loan_request", target=rid, details=f"keperluan={keperluan}; kembali={tgl_kembali}")
                            except Exception:
                                pass
                            try:
                                notify_event("inventory", "loan_request", "[WIJNA] Permohonan Pinjam Barang",
                                             f"Permohonan pinjam barang menunggu review Finance.\n\nBarang: {row.get('name')}\nID: {row.get('id')}\nLokasi: {row.get('location')}\nPemohon: {user.get('email')}\nKeperluan: {keperluan}\nRencana kembali: {tgl_kembali}")
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
            try:
                audit_log("surat_masuk", "upload", target=resp.get("id", ""), details=f"name={file.name}; type={file.type}")
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
                            # Notifikasi: Ada surat masuk baru perlu review/approve oleh Director
                            try:
                                notify_event("surat_masuk", "draft", "[WIJNA] Surat Masuk Baru",
                                             f"Surat Masuk baru menunggu review/approve.\n\nNomor: {nomor}\nPerihal: {perihal}\nPengirim: {pengirim}\nTanggal: {tanggal}")
                            except Exception:
                                pass
                            st.success("Surat masuk berhasil dicatat.")
                            st.rerun()

    # Tab 2: Approval (Director/Superuser)
    with tab2:
        st.markdown("### Approval Surat Masuk")
        if role in ["director", "superuser"]:
            _, df = _sm_read_df()
            pending = df[pd.to_numeric(df.get("director_approved", 0), errors="coerce").fillna(0).astype(int) == 0]
            if pending.empty:
                st.info("Tidak ada surat masuk yang menunggu approval.")
            else:
                opts = [f"{r.get('nomor','')} | {r.get('perihal','')} | {r.get('tanggal','')}" for _, r in pending.sort_values(by="tanggal", ascending=False).iterrows()]
                map_id = {f"{r.get('nomor','')} | {r.get('perihal','')} | {r.get('tanggal','')}": r.get('id') for _, r in pending.iterrows()}
                pick = st.selectbox("Pilih surat untuk approval:", opts, key="sm_pick")
                if pick:
                    row = pending[pending['id'] == map_id[pick]].iloc[0]
                    st.write(f"Pengirim: {row.get('pengirim','')}")
                    st.write(f"Status: {row.get('status','')}")
                    st.write(f"Follow Up: {row.get('follow_up','')}")
                    if row.get("file_link"):
                        st.markdown(f"[Link Surat]({row.get('file_link')})")
                    if row.get("file_id") and row.get("file_name"):
                        _download_button(row.get("file_id"), row.get("file_name"), key=f"dl_sm_{row.get('id')}")
                    colA, colB = st.columns(2)
                    with colA:
                        if st.button("Approve Surat Masuk", key=f"approve_{row.get('id')}"):
                            _sm_update_by_id(row.get("id"), {"director_approved": 1})
                            audit_log("surat_masuk", "director_approval", target=row.get("id"), details="approve=1")
                            try:
                                submitter = str(row.get("submitted_by",""))
                                if submitter:
                                    send_notification_email(submitter, "[WIJNA] Surat Masuk Disetujui",
                                                            f"Surat Masuk {row.get('nomor','')} telah disetujui Director.")
                            except Exception:
                                pass
                            st.success("Surat masuk di-approve Director.")
                            st.rerun()
                    with colB:
                        if st.button("Reject Surat Masuk", key=f"reject_{row.get('id')}"):
                            _sm_update_by_id(row.get("id"), {"director_approved": -1})
                            audit_log("surat_masuk", "director_approval", target=row.get("id"), details="approve=0")
                            try:
                                submitter = str(row.get("submitted_by",""))
                                if submitter:
                                    send_notification_email(submitter, "[WIJNA] Surat Masuk Ditolak",
                                                            f"Surat Masuk {row.get('nomor','')} ditolak Director.")
                            except Exception:
                                pass
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
            st.dataframe(rekap_df[show_cols], width='stretch', hide_index=True)
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
            try:
                audit_log("surat_keluar", "upload", target=resp.get("id", ""), details=f"name={file.name}; type={file.type}")
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
                    # Notifikasi: Ada draft Surat Keluar yang perlu direview/approve
                    try:
                        notify_event("surat_keluar", "draft", "[WIJNA] Draft Surat Keluar Baru",
                                     f"Draft Surat Keluar menunggu review/approve.\n\nNomor: {nomor}\nPerihal: {perihal}\nDitujukan: {ditujukan}\nDibuat oleh: {user.get('full_name') or user.get('email')}\nTanggal: {tanggal}")
                    except Exception:
                        pass
                    st.success("✅ Surat keluar (draft) tersimpan.")
                    st.rerun()

    # Tab 2: Approval Director
    with tab2:
        st.markdown("### Approval Surat Keluar (Director)")
        role = str(user.get("role", "")).lower()
        if role in ["director", "superuser"]:
            _, df = _sk_read_df()
            df = df.copy().sort_values(by="tanggal", ascending=False)
            opts = [f"{r.get('nomor','')} | {r.get('perihal','')} | {r.get('tanggal','')} | {r.get('status','')}" for _, r in df.iterrows()]
            map_id = {f"{r.get('nomor','')} | {r.get('perihal','')} | {r.get('tanggal','')} | {r.get('status','')}": r.get('id') for _, r in df.iterrows()}
            pick = st.selectbox("Pilih surat untuk diproses:", opts, key="sk_pick")
            if pick:
                row = df[df['id'] == map_id[pick]].iloc[0]
                st.write(f"Ditujukan: {row.get('ditujukan','')}")
                st.write(f"Pengirim: {row.get('pengirim','')}")
                st.write(f"Follow Up: {row.get('follow_up','')}")
                if row.get("draft_file_id") and row.get("draft_name"):
                    st.markdown(f"**Draft Surat (file):** {row.get('draft_name')}")
                    _download_button(row.get("draft_file_id"), row.get("draft_name"), key=f"dl_sk_draft_{row.get('id')}")
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
                            try:
                                audit_log("surat_keluar", "final_upload", target=fid, details=f"name={fname}")
                            except Exception:
                                pass
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
            st.dataframe(df[[c for c in ["indeks","nomor","tanggal","ditujukan","perihal","pengirim","status","follow_up","final_name"] if c in df.columns]], width='stretch', hide_index=True)
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
        df_show = df.copy()
        cols_wanted = [c for c in [email_col, 'full_name', 'role', 'active', 'created_at'] if c in df_show.columns]
        st.dataframe(df_show[cols_wanted], width='stretch')
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
        # Ensure columns
        for h in audit_headers:
            if h not in df.columns:
                df[h] = ""
        # Filters
        st.markdown("### Filter")
        use_date = st.checkbox("Filter berdasarkan tanggal")
        c1, c2, c3 = st.columns(3)
        with c1:
            start_date = st.date_input("Dari tanggal", value=date.today()) if use_date else None
        with c2:
            end_date = st.date_input("Sampai tanggal", value=date.today()) if use_date else None
        with c3:
            actor = st.text_input("Aktor (email)")
        c4, c5 = st.columns(2)
        with c4:
            module = st.selectbox("Module", options=["Semua"] + sorted([m for m in df["module"].astype(str).unique() if m]))
        with c5:
            action = st.selectbox("Action", options=["Semua"] + sorted([a for a in df["action"].astype(str).unique() if a]))
        q = st.text_input("Cari (target / details)")

        fdf = df.copy()
        # Date filter expects ISO timestamps
        if use_date and start_date:
            fdf = fdf[pd.to_datetime(fdf["timestamp"], errors="coerce") >= pd.to_datetime(start_date)]
        if use_date and end_date:
            fdf = fdf[pd.to_datetime(fdf["timestamp"], errors="coerce") <= pd.to_datetime(end_date) + pd.Timedelta(days=1)]
        if actor:
            fdf = fdf[fdf["actor"].astype(str).str.contains(actor, case=False, na=False)]
        if module and module != "Semua":
            fdf = fdf[fdf["module"].astype(str) == module]
        if action and action != "Semua":
            fdf = fdf[fdf["action"].astype(str) == action]
        if q:
            mask = (
                fdf["target"].astype(str).str.contains(q, case=False, na=False)
                | fdf["details"].astype(str).str.contains(q, case=False, na=False)
            )
            fdf = fdf[mask]

        # Sort latest first
        try:
            fdf = fdf.sort_values(by="timestamp", ascending=False)
        except Exception:
            pass

        st.dataframe(fdf, width='stretch', hide_index=True)

        # Export filtered
        if not fdf.empty:
            csv_data = fdf.to_csv(index=False).encode("utf-8")
            st.download_button("⬇️ Download CSV (filtered)", csv_data, file_name="audit_filtered.csv", mime="text/csv")
    except Exception as e:
        st.error(f"Gagal memuat audit trail: {e}")


def superuser_panel():
    user = require_login()
    role = str(user.get("role", "")).lower()
    if role != "superuser":
        st.error("Hanya superuser yang dapat mengakses panel ini.")
        return
    st.header("🔑 Superuser Panel - Notifikasi Dinamis")
    st.markdown(
        """
        Kelola pemetaan notifikasi berbasis (module, action) ke daftar peran (roles) yang akan menerima email.
        Data disimpan di sheet `config` dengan kolom: module, action, roles, active, updated_at, updated_by.
        Catatan:
        - roles ditulis dipisahkan koma, contoh: ``finance,director``.
        - hanya baris dengan active=1 (atau TRUE/yes) yang dipakai.
        - superuser tetap otomatis ditambahkan saat pengiriman email.
        - Mapping statis di kode menjadi fallback jika (module, action) belum ada di sini.
        """,
        unsafe_allow_html=True,
    )

    # Utility: refresh dynamic cache
    def _clear_config_cache():
        try:
            load_config_notif_map.clear()  # type: ignore[attr-defined]
        except Exception:
            try:
                st.cache_data.clear()
            except Exception:
                pass

    # Fetch sheet
    try:
        ws = _get_ws(CONFIG_SHEET_NAME)
        records = ws.get_all_records()
    except Exception as e:
        st.error(f"Gagal membaca sheet config: {e}")
        return

    # Normalize display dataframe
    df_cfg = pd.DataFrame(records)
    base_cols = ["module", "action", "roles", "active", "updated_at", "updated_by"]
    for c in base_cols:
        if c not in df_cfg.columns:
            df_cfg[c] = ""
    if not df_cfg.empty:
        df_cfg['active'] = df_cfg['active'].astype(str)

    with st.expander("📋 Lihat Mapping Saat Ini", expanded=True):
        if df_cfg.empty:
            st.info("Belum ada data di config sheet.")
        else:
            st.dataframe(df_cfg[base_cols], width='stretch')
            st.caption(f"Total entri: {len(df_cfg)}")

    st.markdown("---")
    st.subheader("➕ Tambah / Update Mapping")
    # Derive existing modules and actions for convenience
    existing_modules = sorted(set(str(m).strip().lower() for m in df_cfg.get('module', []) if m and not str(m).startswith('__')))
    existing_actions = sorted(set(str(a).strip().lower() for a in df_cfg.get('action', []) if a))
    default_modules = sorted({m for (m, _) in NOTIF_ROLE_MAP.keys()})
    module_options = sorted(set(existing_modules) | set(default_modules)) + ["(Custom)"]
    # Action list akan difilter berdasarkan module terpilih
    # Kumpulkan mapping static per module
    static_actions_by_module: dict[str, set[str]] = {}
    for (m, a) in NOTIF_ROLE_MAP.keys():
        static_actions_by_module.setdefault(m, set()).add(a)
    # Kumpulkan mapping dynamic per module
    dynamic_actions_by_module: dict[str, set[str]] = {}
    if not df_cfg.empty:
        for _, row in df_cfg.iterrows():
            m = str(row.get('module','')).strip().lower()
            a = str(row.get('action','')).strip().lower()
            if m and a and not m.startswith('__'):
                dynamic_actions_by_module.setdefault(m, set()).add(a)
    # Fungsi helper untuk dapat semua actions suatu module
    def _actions_for_module(mod: str) -> list[str]:
        acts = set()
        acts |= static_actions_by_module.get(mod, set())
        acts |= dynamic_actions_by_module.get(mod, set())
        return sorted(acts)

    # Pemilihan module diletakkan di luar form agar perubahan langsung memicu rerun dan memfilter daftar action
    sel_mod = st.selectbox(
        "Pilih Module",
        module_options,
        key="cfg_sel_mod",
        help="Pilih module. Daftar action otomatis difilter sesuai module. Gunakan (Custom) untuk module baru."
    )
    # Reset action selection jika module berubah
    if 'cfg_last_mod' not in st.session_state or st.session_state.cfg_last_mod != sel_mod:
        st.session_state.cfg_last_mod = sel_mod
        if 'cfg_sel_act' in st.session_state:
            del st.session_state['cfg_sel_act']
    # Tentukan module input final
    if sel_mod == "(Custom)":
        mod_in = st.text_input("Module Baru", key="cfg_mod_custom", placeholder="misal: cash_advance").strip().lower()
        # kumpulkan semua action unik sebagai referensi (tidak dipakai langsung kecuali user ingin lihat)
        all_actions_flat = sorted({a for s in static_actions_by_module.values() for a in s} | {a for s in dynamic_actions_by_module.values() for a in s})
        filtered_actions = all_actions_flat
    else:
        mod_in = sel_mod.strip().lower()
        filtered_actions = _actions_for_module(mod_in)
    if not filtered_actions:
        filtered_actions = []
    filtered_actions_with_custom = filtered_actions + ["(Custom)"]

    with st.form("cfg_add_update", clear_on_submit=False):
        c1, c2 = st.columns(2)
        with c1:
            sel_act = st.selectbox(
                "Pilih Action",
                filtered_actions_with_custom,
                key="cfg_sel_act",
                help="Action difilter oleh module. Pilih (Custom) untuk menambah action baru."
            )
            if sel_act == "(Custom)":
                act_in = st.text_input("Action Baru", key="cfg_act_custom", placeholder="misal: submitted").strip().lower()
            else:
                act_in = sel_act.strip().lower()
        with c2:
            roles_multi = st.multiselect("Pilih Roles", ALLOWED_ROLES, default=[r for r in ["finance"] if r in ALLOWED_ROLES])
            active_in = st.checkbox("Active", value=True)
        submitted = st.form_submit_button("Simpan / Update Mapping")
        if submitted:
            mod = mod_in.strip().lower()
            act = act_in.strip().lower()
            valid_roles = [r for r in roles_multi if r in ALLOWED_ROLES]
            if not mod or not act or not valid_roles:
                st.warning("Module, Action, dan minimal satu Role valid wajib diisi.")
            else:
                roles_clean = ",".join(sorted(set(valid_roles)))
                headers = ws.row_values(1)
                try:
                    module_col = headers.index('module') + 1
                    action_col = headers.index('action') + 1
                except ValueError:
                    st.error("Kolom module/action tidak ditemukan di sheet config.")
                    st.stop()
                target_row = None
                try:
                    all_vals = ws.get_all_values()
                    for ridx, row_vals in enumerate(all_vals[1:], start=2):
                        try:
                            if str(row_vals[module_col-1]).strip().lower() == mod and str(row_vals[action_col-1]).strip().lower() == act:
                                target_row = ridx
                                break
                        except Exception:
                            continue
                except Exception:
                    pass
                updated_at = datetime.utcnow().isoformat()
                updated_by = (get_current_user() or {}).get('email', 'system')
                row_payload = {
                    'module': mod,
                    'action': act,
                    'roles': roles_clean,
                    'active': '1' if active_in else '0',
                    'updated_at': updated_at,
                    'updated_by': updated_by
                }
                if target_row:
                    for k, v in row_payload.items():
                        if k in headers:
                            a1 = gspread.utils.rowcol_to_a1(target_row, headers.index(k) + 1)
                            for i in range(3):
                                try:
                                    ws.update(a1, v)
                                    break
                                except gspread.exceptions.APIError as e:
                                    if '429' in str(e):
                                        time.sleep(1 + i)
                                        continue
                                    raise
                    st.success(f"Mapping diperbarui: {mod} / {act}")
                    try:
                        audit_log("config", "update", target=f"{mod}:{act}", details=roles_clean)
                    except Exception:
                        pass
                else:
                    values = [row_payload.get(h, "") for h in headers]
                    for i in range(3):
                        try:
                            ws.append_row(values)
                            break
                        except gspread.exceptions.APIError as e:
                            if '429' in str(e):
                                time.sleep(1 + i)
                                continue
                            raise
                    st.success(f"Mapping ditambahkan: {mod} / {act}")
                    try:
                        audit_log("config", "add", target=f"{mod}:{act}", details=roles_clean)
                    except Exception:
                        pass
                _clear_config_cache()
                st.rerun()

    st.markdown("---")
    st.subheader("🗑️ Hapus Mapping")
    if df_cfg.empty:
        st.info("Tidak ada mapping untuk dihapus.")
    else:
        # Build label list
        df_cfg['label'] = df_cfg.apply(lambda r: f"{r['module']} | {r['action']} -> {r['roles']} (active={r['active']})", axis=1)
        choice = st.selectbox("Pilih mapping", df_cfg['label'])
        if choice:
            sel_row = df_cfg[df_cfg['label'] == choice].iloc[0]
            if st.button("Hapus Mapping Terpilih", type="primary"):
                mod = str(sel_row.get('module'))
                act = str(sel_row.get('action'))
                # find row index again
                all_vals = ws.get_all_values()
                headers = all_vals[0]
                module_idx = headers.index('module') if 'module' in headers else None
                action_idx = headers.index('action') if 'action' in headers else None
                del_row = None
                if module_idx is not None and action_idx is not None:
                    for ridx, row_vals in enumerate(all_vals[1:], start=2):
                        try:
                            if str(row_vals[module_idx]).strip().lower() == mod and str(row_vals[action_idx]).strip().lower() == act:
                                del_row = ridx
                                break
                        except Exception:
                            continue
                if del_row:
                    for i in range(3):
                        try:
                            ws.delete_rows(del_row)
                            break
                        except gspread.exceptions.APIError as e:
                            if '429' in str(e):
                                time.sleep(1 + i)
                                continue
                            raise
                    st.success(f"Mapping dihapus: {mod} / {act}")
                    try:
                        audit_log("config", "delete", target=f"{mod}:{act}")
                    except Exception:
                        pass
                    _clear_config_cache()
                    st.rerun()
                else:
                    st.warning("Gagal menemukan baris mapping untuk dihapus.")

    st.markdown("---")
    if st.button("🔄 Refresh Mapping Cache"):
        _clear_config_cache()
        st.success("Cache dynamic mapping dibersihkan.")

    st.markdown("---")
    st.subheader("⚙️ Pengaturan Tambahan")
    # Superuser auto toggle
    cur_auto = is_superuser_auto_enabled()
    new_auto = st.checkbox("Sertakan superuser otomatis pada semua notifikasi", value=cur_auto, help="Jika dimatikan, superuser hanya menerima notifikasi jika termasuk dalam roles mapping.")
    if new_auto != cur_auto:
        # Write/update settings row
        try:
            headers = ws.row_values(1)
            # ensure settings headers contain superuser_auto column (optional)
            if 'superuser_auto' not in headers:
                headers.append('superuser_auto')
                ws.update('A1', [headers])
            all_vals = ws.get_all_values()
            module_idx = headers.index('module') if 'module' in headers else None
            found_row = None
            if module_idx is not None:
                for ridx, row_vals in enumerate(all_vals[1:], start=2):
                    if len(row_vals) > module_idx and str(row_vals[module_idx]).strip().lower() == '__settings__':
                        found_row = ridx
                        break
            updated_at = datetime.utcnow().isoformat()
            updater = (get_current_user() or {}).get('email', 'system')
            settings_payload = {
                'module': '__settings__',
                'action': 'global',
                'roles': '',
                'active': '1',
                'updated_at': updated_at,
                'updated_by': updater,
                'superuser_auto': '1' if new_auto else '0'
            }
            # Align headers again (in case added)
            headers = ws.row_values(1)
            if found_row:
                # update columns
                for k, v in settings_payload.items():
                    if k in headers:
                        a1 = gspread.utils.rowcol_to_a1(found_row, headers.index(k) + 1)
                        for i in range(3):
                            try:
                                ws.update(a1, v)
                                break
                            except gspread.exceptions.APIError as e:
                                if '429' in str(e):
                                    time.sleep(1 + i)
                                    continue
                                raise
            else:
                row_vals = [settings_payload.get(h, '') for h in headers]
                for i in range(3):
                    try:
                        ws.append_row(row_vals)
                        break
                    except gspread.exceptions.APIError as e:
                        if '429' in str(e):
                            time.sleep(1 + i)
                            continue
                        raise
            try:
                audit_log('config', 'settings_update', target='__settings__', details=f"superuser_auto={int(new_auto)}")
            except Exception:
                pass
            # Clear caches
            try:
                is_superuser_auto_enabled.clear()  # type: ignore[attr-defined]
            except Exception:
                pass
            _clear_config_cache()
            st.success("Pengaturan superuser_auto diperbarui.")
        except Exception as e:
            st.error(f"Gagal menyimpan pengaturan: {e}")

    st.markdown("---")
    st.subheader("🧪 Test Kirim Email Dummy")
    with st.form("test_email_form"):
        test_subject = st.text_input("Subject", value="[TEST] Notifikasi Dummy")
        test_body = st.text_area("Body", value="Ini hanya email percobaan.")
        # Pilih mapping yang ada
        active_map = load_config_notif_map()
        map_labels = [f"{m}:{a}" for (m, a) in sorted(active_map.keys())]
        use_mapping = st.selectbox("Gunakan Mapping (opsional)", ["(Manual Roles)"] + map_labels)
        manual_roles = []
        if use_mapping == "(Manual Roles)":
            manual_roles = st.multiselect("Manual Roles", ALLOWED_ROLES, default=["finance"])
        send_btn = st.form_submit_button("Kirim Email Uji")
        if send_btn:
            try:
                if use_mapping != "(Manual Roles)":
                    # parse mapping
                    parts = use_mapping.split(":", 1)
                    if len(parts) == 2:
                        m_sel, a_sel = parts[0], parts[1]
                        roles = active_map.get((m_sel, a_sel), [])
                        if not roles:
                            st.warning("Mapping tidak memiliki roles aktif.")
                        else:
                            notify_event(m_sel, a_sel, test_subject, test_body, roles=None)  # dynamic lookup
                            # Cek secara manual siapa saja penerima (diagnostik) dengan memanggil fungsi internal
                            dyn_roles = load_config_notif_map().get((m_sel, a_sel), []) or []
                            preview_roles = set(dyn_roles)
                            if is_superuser_auto_enabled():
                                preview_roles.add("superuser")
                            # Ambil email tiap role
                            preview_emails = []
                            for r in preview_roles:
                                try:
                                    preview_emails.extend(_get_emails_by_role(r))
                                except Exception:
                                    pass
                            preview_emails = sorted({e for e in preview_emails if e})
                            if preview_emails:
                                st.success(f"Email test berdasarkan mapping dikirim ke {len(preview_emails)} penerima.")
                                with st.expander("Detail Penerima", expanded=False):
                                    st.write(preview_emails)
                            else:
                                st.error("Mapping valid tetapi tidak ditemukan email penerima (cek sheet users & kolom active).")
                else:
                    valid_roles = [r for r in manual_roles if r in ALLOWED_ROLES]
                    if not valid_roles:
                        st.warning("Pilih minimal satu role valid untuk manual.")
                    else:
                        notify_event("test", "manual", test_subject, test_body, roles=valid_roles)
                        # Diagnostik manual
                        diag_roles = set(valid_roles)
                        if is_superuser_auto_enabled():
                            diag_roles.add("superuser")
                        diag_emails = []
                        for r in diag_roles:
                            try:
                                diag_emails.extend(_get_emails_by_role(r))
                            except Exception:
                                pass
                        diag_emails = sorted({e for e in diag_emails if e})
                        if diag_emails:
                            st.success(f"Email test manual dikirim ke {len(diag_emails)} penerima.")
                            with st.expander("Detail Penerima", expanded=False):
                                st.write(diag_emails)
                        else:
                            st.error("Tidak ada email ditemukan untuk roles yang dipilih. Pastikan user, role, dan active benar.")
            except Exception as e:
                st.error(f"Gagal mengirim email test: {e}")

    # Panel Diagnostik Role -> Emails
    st.markdown("---")
    with st.expander("🩺 Diagnostik Role & Penerima", expanded=False):
        st.caption("Gunakan panel ini untuk melihat email yang terdeteksi per role sesuai isi sheet users saat ini.")
        roles_check = st.multiselect("Pilih roles untuk cek", ALLOWED_ROLES, default=["finance","director","superuser"])
        if st.button("Cek Penerima", key="btn_diag_roles"):
            rows = []
            for r in roles_check:
                try:
                    emails = _get_emails_by_role(r)
                except Exception:
                    emails = []
                rows.append({"role": r, "jumlah": len(emails), "emails": ", ".join(emails)})
            if rows:
                st.dataframe(pd.DataFrame(rows), width='stretch')
            else:
                st.info("Tidak ada data untuk roles dipilih.")


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
        # Centered header using 3 columns; place content in the middle column
        col1, col2, col3 = st.columns(3)
        with col2:
            st.image(os.path.join(os.path.dirname(__file__), "logo.png"), width=500)
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
    # Gunakan helper agar tidak memicu StreamlitInvalidWidthError (hindari width=None eksplisit)
    safe_image(logo_path)
    st.sidebar.markdown("<h2 style='text-align:center;margin-bottom:0.5em;'>WIJNA Management System</h2>", unsafe_allow_html=True)
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
        /* Uniform sidebar navigation buttons */
        .wijna-nav-btn > button {
            width:100% !important;
            height:10px !important;
            min-height:42px !important;
            display:flex !important;
            align-items:center !important;
            justify-content:center !important;
            font-size:0.95rem !important;
            font-weight:600 !important;
            padding:0 10px !important;
            margin:0 0 3px 0 !important;
            border-radius:6px !important;
            line-height:1.0 !important;
            white-space:normal !important;
        }
        .wijna-nav-btn.active-nav > button {
            background:#2563eb !important;
            color:#ffffff !important;
            border:1px solid #1d4ed8 !important;
            box-shadow:0 0 0 1px rgba(37,99,235,0.35) inset;
        }
        .wijna-nav-btn > button:hover {
            border:1px solid #2563eb !important;
        }
        /* Compact sidebar heading & separator */
        .sidebar-section-title { 
            font-size:0.78rem; 
            font-weight:700; 
            letter-spacing:0.5px; 
            color:#475569; 
            text-transform:uppercase; 
            margin:4px 0 6px 0 !important;
        }
        .sidebar-thin-sep { 
            height:1px; 
            background:linear-gradient(90deg,#cbd5e1,#f1f5f9); 
            margin:6px 0 4px 0; 
            border-radius:1px;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    # Compact separator & heading
    st.sidebar.markdown('<div class="sidebar-thin-sep"></div><div class="sidebar-section-title">Navigasi Modul</div>', unsafe_allow_html=True)
    nav_cols = st.sidebar.columns(2)
    for idx, (key, label) in enumerate(menu):
        col = nav_cols[idx % 2]
        with col:
            active_class = 'active-nav' if st.session_state.get("page") == key else ''
            st.markdown(f'<div class="wijna-nav-btn {active_class}">', unsafe_allow_html=True)
            clicked = st.button(label, key=f"nav_{key}", help=key, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
            if clicked:
                st.session_state["page"] = key
                st.rerun()

    # --- Logout button at the very bottom ---
    if user:
        st.sidebar.markdown("<div style='height:32px'></div>", unsafe_allow_html=True)
    if st.sidebar.button("Logout", key="sidebar_logout"): 
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
                        # Notifikasi ke Finance + Superuser untuk review
                        try:
                            notify_event("cuti", "submit", "[WIJNA] Pengajuan Cuti Baru",
                                         f"Pengajuan cuti baru menunggu review Finance.\n\nNama: {nama}\nPeriode: {tgl_mulai} s/d {tgl_selesai}\nDurasi: {durasi} hari\nAlasan: {alasan}")
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
                                    # Jika approve, beritahu Director + Superuser
                                        if approve:
                                            try:
                                                notify_event("cuti", "finance_review", "[WIJNA] Cuti Menunggu Approval Director",
                                                             f"Pengajuan cuti menunggu approval Director.\n\nNama: {row.get('nama')}\nPeriode: {row.get('tgl_mulai')} s/d {row.get('tgl_selesai')}\nDurasi: {row.get('durasi')} hari")
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
                                    # Notifikasi ke pemohon terkait keputusan Director (jika ada kolom submitted_by gunakan itu)
                                    try:
                                        # Gunakan 'submitted_by' jika ada, fallback skip
                                        if 'submitted_by' in row.index:
                                            sb = str(row.get('submitted_by',''))
                                            if sb:
                                                send_notification_email(sb, f"[WIJNA] Pengajuan Cuti {'Disetujui' if approve else 'Ditolak'}",
                                                                        f"Pengajuan cuti Anda {'disetujui' if approve else 'ditolak'} oleh Director.")
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

