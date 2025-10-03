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
from datetime import datetime, date

# --- 1. KONFIGURASI APLIKASI ---
# PENTING: Pastikan ID ini berasal dari folder di dalam SHARED DRIVE
GDRIVE_FOLDER_ID = "1CxYo2ZGu8jweKjmEws41nT3cexJju5_1" 
USERS_SHEET_NAME = "users"
CUTI_SHEET_NAME = "cuti"
AUDIT_SHEET_NAME = "audit_log"
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
    """Initialize required worksheets: users, cuti, audit_log, and fix duplicate headers if any."""
    try:
        client = get_gsheets_client()
        spreadsheet = client.open_by_url(SPREADSHEET_URL)

        # Users sheet: support both legacy (username/password_hash) and new schema
        users_headers = ["email", "password_hash", "full_name", "role", "created_at", "active"]
        users_ws = ensure_sheet_with_headers(spreadsheet, USERS_SHEET_NAME, users_headers)

        # Ensure at least an admin exists — use expected_headers to avoid duplicate header error during read
        try:
            df_users = pd.DataFrame(users_ws.get_all_records(expected_headers=users_headers))
        except Exception:
            # Fallback read without mapping
            df_users = pd.DataFrame(users_ws.get_all_records())

        if df_users.empty:
            users_ws.append_row(["admin@local", hash_password("admin"), "Admin", "superuser", datetime.utcnow().isoformat(), 1])
        else:
            # If there is no superuser, ensure one exists
            has_superuser = False
            if "role" in df_users.columns:
                has_superuser = (df_users["role"].astype(str).str.lower() == "superuser").any()
            if not has_superuser:
                users_ws.append_row(["admin@local", hash_password("admin"), "Admin", "superuser", datetime.utcnow().isoformat(), 1])

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
        st.rerun()
        
    try:
        with st.spinner("Memuat daftar file dari Google Drive..."):
            drive_service = get_gdrive_service()
            query = f"'{GDRIVE_FOLDER_ID}' in parents and trashed=false"
            results = drive_service.files().list(
                q=query,
                pageSize=100,
                fields="nextPageToken, files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()
            items = results.get('files', [])

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
                        request = drive_service.files().get_media(fileId=file_id, supportsAllDrives=True)
                        fh = io.BytesIO()
                        fh.write(request.execute())
                        fh.seek(0)
                        return fh.getvalue()

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
    client = get_gsheets_client()
    spreadsheet = client.open_by_url(SPREADSHEET_URL)
    ws = spreadsheet.worksheet(USERS_SHEET_NAME)
    users_headers = ["email", "password_hash", "full_name", "role", "created_at", "active"]
    try:
        df = pd.DataFrame(ws.get_all_records(expected_headers=users_headers))
    except Exception:
        df = pd.DataFrame(ws.get_all_records())
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
        client = get_gsheets_client()
        spreadsheet = client.open_by_url(SPREADSHEET_URL)
        ws = spreadsheet.worksheet(USERS_SHEET_NAME)
        users_headers = ["email", "password_hash", "full_name", "role", "created_at", "active"]
        try:
            df = pd.DataFrame(ws.get_all_records(expected_headers=users_headers))
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
        ws.append_row(row_values)
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


def require_login():
    user = get_current_user()
    if not user:
        st.stop()
    return user


def gen_id(prefix: str):
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _get_ws(name: str):
    client = get_gsheets_client()
    spreadsheet = client.open_by_url(SPREADSHEET_URL)
    return spreadsheet.worksheet(name)


def audit_log(module: str, action: str, target: str = "", details: str = ""):
    try:
        ws = _get_ws(AUDIT_SHEET_NAME)
        actor = (get_current_user() or {}).get("email", "guest")
        ws.append_row([datetime.utcnow().isoformat(), actor, module, action, target, details])
    except Exception:
        # Non-blocking
        pass


def dashboard():
    st.title("🏠 Dashboard")
    st.write("Selamat datang di WIJNA Manajemen System.")


def inventory_module():
    st.header("📦 Inventory")
    st.info("Module coming soon (Sheets + Drive)")


def surat_masuk_module():
    st.header("📥 Surat Masuk")
    st.info("Module coming soon (Sheets + Drive)")


def surat_keluar_module():
    st.header("📤 Surat Keluar")
    st.info("Module coming soon (Sheets + Drive)")


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
    st.header("⚙️ User Setting")
    st.info("Module coming soon (Sheets + Drive)")


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
        df = pd.DataFrame(ws.get_all_records(expected_headers=cuti_headers))
    except Exception:
        df = pd.DataFrame(ws.get_all_records())
    return ws, df


def _cuti_append(row_dict: dict):
    ws = _get_ws(CUTI_SHEET_NAME)
    headers = ws.row_values(1)
    values = [row_dict.get(h, "") for h in headers]
    ws.append_row(values)


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
        ws.update(a1, [[v]])


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
    ensure_core_sheets()
    main()
