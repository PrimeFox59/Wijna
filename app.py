import streamlit as st
import os
import re
from datetime import datetime, date, timedelta
import base64
import pandas as pd
import uuid
import json
import io
from typing import Dict, Any, List, Optional


st.set_page_config(
    page_title="WIJNA Manajemen System",
    page_icon="icon.png", 
    layout="wide"
)

# --- GOOGLE SHEETS CONNECTION & SETUP ---
def get_gsheet_connection():
    try:
        creds = st.secrets["connections"]["gsheets"]
        gc = gspread.service_account_from_dict(creds)
        sh = gc.open_by_key(st.secrets["connections"]["gsheets"]["spreadsheet"].split('/')[-2])
        return sh
    except Exception as e:
        st.error(f"Gagal terhubung ke Google Sheets. Pastikan file secrets.toml sudah benar dan API Google Sheets/Drive telah diaktifkan: {e}")
        st.stop()
    return None

sh = get_gsheet_connection()

# --- UTILITY FUNCTIONS ---
def get_worksheet(sheet_name):
    try:
        return sh.worksheet(sheet_name)
    except WorksheetNotFound:
        return None

def check_and_create_worksheets():
    """Checks for required worksheets and creates them with headers if they don't exist."""
    required_worksheets = {
        "users": ["id","email","full_name","role","password_hash","status","created_at","last_login"],
        "sop": ["id","judul","file_name","file_id","file_url","tanggal_upload","tanggal_terbit","director_approved","memo","board_note"],
        "notulen": ["id","judul","file_name","file_id","file_url","tanggal_upload","uploaded_by","deadline","director_note","director_approved","tanggal_rapat"],
        "surat_masuk": ["id","indeks","nomor","pengirim","tanggal","perihal","file_name","file_id","file_url","status","follow_up","rekap","director_approved"],
        "surat_keluar": ["id","indeks","nomor","tanggal","ditujukan","perihal","lampiran_name","lampiran_id","lampiran_url","draft_name","draft_id","draft_url","status","follow_up","pengirim","director_note","director_approved","final_name","final_id","final_url"],
        "mou": ["id","nomor","nama","pihak","jenis","tgl_mulai","tgl_selesai","file_name","file_id","file_url","board_note","board_approved","director_note","director_approved","final_name","final_id","final_url"],
        "cash_advance": ["id","divisi","items_json","totals","tanggal","finance_note","finance_approved","director_note","director_approved"],
        "pmr": ["id","nama","file1_name","file1_id","file1_url","file2_name","file2_id","file2_url","bulan","finance_note","finance_approved","director_note","director_approved","tanggal_submit"],
        "cuti": ["id","nama","tgl_mulai","tgl_selesai","durasi","kuota_tahunan","cuti_terpakai","sisa_kuota","status","finance_note","finance_approved","director_note","director_approved"],
        "flex": ["id","nama","tanggal","jam_mulai","jam_selesai","alasan","catatan_finance","approval_finance","catatan_director","approval_director","finance_note","finance_approved","director_note","director_approved"],
        "delegasi": ["id","judul","deskripsi","pic","tgl_mulai","tgl_selesai","file_name","file_id","file_url","status","tanggal_update"],
        "mobil": ["id","nama_pengguna","divisi","tgl_mulai","tgl_selesai","tujuan","kendaraan","driver","status","finance_note"],
        "calendar": ["id","jenis","judul","nama_divisi","tgl_mulai","tgl_selesai","deskripsi","file_name","file_id","file_url","is_holiday","sumber","ditetapkan_oleh","tanggal_penetapan"],
        "public_holidays": ["tahun","tanggal","nama","keterangan","ditetapkan_oleh","tanggal_penetapan"],
        "inventory": ["id","nama_barang","kode_barang","qty","harga","updated_at","finance_approved","director_approved","divisi"],
    }

    existing_worksheets = [ws.title for ws in sh.worksheets()]
    
    for ws_name, headers in required_worksheets.items():
        if ws_name not in existing_worksheets:
            st.warning(f"Worksheet '{ws_name}' tidak ditemukan. Membuat sekarang...")
            new_ws = sh.add_worksheet(title=ws_name, rows="1000", cols="20")
            new_ws.append_row(headers)
            st.success(f"Worksheet '{ws_name}' berhasil dibuat dengan header.")


