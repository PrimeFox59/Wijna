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
