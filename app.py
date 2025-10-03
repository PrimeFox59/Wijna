import streamlit as st
import pandas as pd
import gspread
from passlib.context import CryptContext
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import io

# --- KONFIGURASI ---
GDRIVE_FOLDER_ID = "1CxYo2ZGu8jweKjmEws41nT3cexJju5_1"
USERS_SHEET_NAME = "users"
SPREADSHEET_URL = st.secrets["connections"]["gsheets"]["spreadsheet"]
st.set_page_config(page_title="Secure App", page_icon="🔐", layout="centered")

# --- KONEKSI & AUTENTIKASI (VERSI PERBAIKAN) ---
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

@st.cache_resource
def get_credentials():
    """Membuat object credentials dari secrets. Ini akan digunakan oleh kedua service."""
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
    creds = get_credentials() # <-- Ambil credentials langsung
    service = build('drive', 'v3', credentials=creds) # <-- Gunakan credentials langsung
    return service

# --- FUNGSI HELPER (Tidak ada perubahan) ---
def hash_password(password: str):
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str):
    return pwd_context.verify(plain_password, hashed_password)

def initialize_users_sheet():
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

# --- INISIALISASI SESSION STATE (Tidak ada perubahan) ---
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'username' not in st.session_state:
    st.session_state.username = ""

# --- Tampilan LOGIN / REGISTER (Tidak ada perubahan) ---
def show_login_page():
    st.header("🔐 Secure App Login")
    
    with st.sidebar:
        st.subheader("Pilih Aksi")
        action = st.radio(" ", ["Login", "Register"])

    client = get_gsheets_client()
    spreadsheet = client.open_by_url(SPREADSHEET_URL)
    worksheet = spreadsheet.worksheet(USERS_SHEET_NAME)

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

# --- Tampilan APLIKASI UTAMA (Tidak ada perubahan) ---
def show_main_app():
    st.sidebar.success(f"Login sebagai: **{st.session_state.username}**")
    if st.sidebar.button("Logout"):
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
                    file_metadata = {
                        'name': uploaded_file.name,
                        'parents': [GDRIVE_FOLDER_ID]
                    }
                    file_buffer = io.BytesIO(uploaded_file.getvalue())
                    media = MediaIoBaseUpload(file_buffer, mimetype=uploaded_file.type, resumable=True)
                    
                    file = drive_service.files().create(body=file_metadata,
                                                        media_body=media,
                                                        fields='id').execute()
                    st.success(f"✅ File '{uploaded_file.name}' berhasil diupload! (ID: {file.get('id')})")
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
                fields="nextPageToken, files(id, name)"
            ).execute()
            items = results.get('files', [])

        if not items:
            st.info("📂 Folder ini masih kosong.")
        else:
            st.write(f"Ditemukan {len(items)} file:")
            for item in items:
                col1, col2 = st.columns([4, 1])
                with col1:
                    st.write(f"📄 **{item['name']}**")
                with col2:
                    @st.cache_data(ttl=300)
                    def download_file_from_drive(file_id):
                        request = drive_service.files().get_media(fileId=file_id)
                        downloader = io.BytesIO(request.execute())
                        return downloader.getvalue()

                    file_data = download_file_from_drive(item['id'])
                    st.download_button(
                        label="Download",
                        data=file_data,
                        file_name=item['name'],
                        key=f"dl_{item['id']}"
                    )
    except Exception as e:
        st.error(f"Gagal memuat daftar file: {e}")

# --- MAIN LOGIC (Tidak ada perubahan) ---
if __name__ == "__main__":
    initialize_users_sheet()
    
    if not st.session_state.logged_in:
        show_login_page()
    else:
        show_main_app()
