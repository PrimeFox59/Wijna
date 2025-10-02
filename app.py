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

# Google APIs
try:
    import gspread
    from gspread.exceptions import WorksheetNotFound
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseUpload
except Exception:
    gspread = None
    WorksheetNotFound = None
    Credentials = None
    build = None
    MediaIoBaseUpload = None

# ------------------------------------------------------------------
# Google Sheets / Drive Adapter (non-breaking, mirrors SQLite <-> Sheets)
# ------------------------------------------------------------------

# Configuration from Streamlit Secrets
_secrets = st.secrets if hasattr(st, "secrets") else {}
_conn_secrets = _secrets.get("connections", {}).get("gsheets", {}) if isinstance(_secrets, dict) else {}
# Prefer connections.gsheets.spreadsheet, but support common fallbacks (top-level or env vars)
GSHEETS_SPREADSHEET_URL = (
    (_conn_secrets or {}).get("spreadsheet")
    or (_secrets.get("spreadsheet") if isinstance(_secrets, dict) else None)
    or (_secrets.get("gsheets_spreadsheet") if isinstance(_secrets, dict) else None)
    or os.environ.get("GSHEETS_SPREADSHEET")
    or os.environ.get("SPREADSHEET_URL")
    or os.environ.get("SPREADSHEET_KEY")
)
# Folder ID provided by user; can be overridden in secrets as gdrive_folder_id
DRIVE_FOLDER_ID = _secrets.get("gdrive_folder_id", "1CxYo2ZGu8jweKjmEws41nT3cexJju5_1")

def _extract_spreadsheet_key(url_or_key: Optional[str]) -> Optional[str]:
    """Accepts a full URL or a key; returns spreadsheet key or None."""
    if not url_or_key:
        return None
    s = str(url_or_key)
    # If it's a URL, try robust extractions
    if "/spreadsheets/d/" in s:
        try:
            return s.split("/spreadsheets/d/")[1].split("/")[0]
        except Exception:
            pass
    if "http" in s and "/" in s:
        try:
            return s.strip("/").split("/")[-2]
        except Exception:
            pass
    # Otherwise assume it's already a key
    return s

def _google_creds():
    if Credentials is None:
        return None
    # Primary source: connections.gsheets (Streamlit Cloud pattern)
    info = dict(_conn_secrets or {})
    # Fallback: service account json at root of secrets
    if not info or "client_email" not in info:
        if isinstance(_secrets, dict) and _secrets.get("type") == "service_account" and _secrets.get("client_email"):
            info = dict(_secrets)
    # streamlit stores private_key with \n escapes; google lib needs actual newlines
    if info.get("private_key"):
        info["private_key"] = info["private_key"].replace("\\n", "\n")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/drive.file",
    ]
    try:
        return Credentials.from_service_account_info(info, scopes=scopes)
    except Exception:
        return None

@st.cache_resource(show_spinner=False)
def _gs_client_and_sheet():
    """Return (gspread_client, spreadsheet) or (None, None) if not configured.
    Tries opening by key (preferred) or URL, and creates a new spreadsheet if not found.
    """
    if gspread is None:
        return None, None
    creds = _google_creds()
    gc = None
    # Prefer google-auth credentials, fallback to gspread's service_account_from_dict
    if creds is not None:
        try:
            gc = gspread.authorize(creds)
        except Exception:
            gc = None
    if gc is None:
        try:
            gc = gspread.service_account_from_dict(_conn_secrets or {})
        except Exception:
            return None, None
    # If a spreadsheet URL/key is configured, use THAT exact sheet only.
    # If we cannot open it (e.g., not shared with the service account), return (gc, None)
    if GSHEETS_SPREADSHEET_URL:
        sh = None
        key = _extract_spreadsheet_key(GSHEETS_SPREADSHEET_URL)
        try:
            if key:
                sh = gc.open_by_key(key)
        except Exception:
            sh = None
        if sh is None:
            try:
                sh = gc.open_by_url(GSHEETS_SPREADSHEET_URL)
            except Exception:
                sh = None
        return gc, sh
    # No spreadsheet configured: create or re-use a default one owned by the service account
    try:
        # Try to find an existing spreadsheet named WIJNA Backend
        # Note: gspread has no direct search; we simply create if missing
        sh = gc.create("WIJNA Backend")
        return gc, sh
    except Exception:
        return gc, None

@st.cache_resource(show_spinner=False)
def _drive_service():
    if build is None:
        return None
    creds = _google_creds()
    if not creds:
        return None
    try:
        return build("drive", "v3", credentials=creds)
    except Exception:
        return None

def _ensure_worksheet(name: str, columns: List[str]) -> Optional[Any]:
    """Ensure a worksheet exists with at least provided columns as header."""
    gc, sh = _gs_client_and_sheet()
    if not sh:
        return None
    try:
        try:
            ws = sh.worksheet(name)
        except Exception:
            ws = sh.add_worksheet(title=name, rows=1000, cols=max(10, len(columns)))
        # Ensure headers
        existing = ws.row_values(1)
        if not existing:
            ws.update("1:1", [columns])
        else:
            # Merge columns, preserve order of existing then add missing at end
            merged = existing[:]
            for c in columns:
                if c not in merged:
                    merged.append(c)
            if merged != existing:
                ws.update("1:1", [merged])
        return ws
    except Exception:
        return None

# --- Compatibility helpers based on user's reference code ---
def get_gsheet_connection():
    """Open the Google Spreadsheet defined in secrets; stop the app with a helpful error if it fails."""
    gc, sh = _gs_client_and_sheet()
    if sh:
        return sh
    # If we reach here, provide a clear error and stop like the reference code
    try:
        raise RuntimeError("Gagal terhubung ke Google Sheets. Pastikan secrets.toml benar dan API Sheets/Drive aktif.")
    except Exception as e:
        st.error(f"Gagal terhubung ke Google Sheets. Pastikan file secrets.toml sudah benar dan API Google Sheets/Drive telah diaktifkan: {e}")
        st.stop()
    return None

def get_worksheet(sheet_name: str):
    """Return a worksheet by name, or None if not found (no exception)."""
    try:
        sh = get_gsheet_connection()
        return sh.worksheet(sheet_name)
    except Exception:
        return None

def check_and_create_worksheets():
    """Checks for required worksheets and creates them with headers if they don't exist.
    Meniru gaya referensi: definisikan dict sheet->headers, lalu pastikan tiap sheet ada beserta kolomnya.
    """
    # Validate config
    if not GSHEETS_SPREADSHEET_URL:
        st.info("Google Sheets belum dikonfigurasi. Set 'connections.gsheets.spreadsheet' di secrets, atau variabel GSHEETS_SPREADSHEET.")
        return
    # Try connection first to surface clear errors
    gc, sh = _gs_client_and_sheet()
    if not sh:
        sa_email = None
        try:
            creds = _google_creds()
            if creds and hasattr(creds, "service_account_email"):
                sa_email = creds.service_account_email
            elif isinstance(_conn_secrets, dict):
                sa_email = _conn_secrets.get("client_email")
        except Exception:
            pass
        msg = "Tidak bisa membuka Spreadsheet. Pastikan link/key benar dan di-share ke Service Account."
        if sa_email:
            msg += f" Share edit ke: {sa_email}"
        st.error(msg)
        return
    # Definisikan worksheet yang dibutuhkan sesuai skema app
    required_worksheets: Dict[str, List[str]] = {
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
    # Pastikan tiap worksheet ada dan header lengkap
    for name, headers in required_worksheets.items():
        _ensure_worksheet(name, headers)

def ensure_default_superuser():
    """Buat akun superuser default jika tabel users kosong atau belum ada superuser."""
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM users")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM users WHERE role='superuser'")
        has_su = cur.fetchone()[0] > 0
        if total == 0 or not has_su:
            now = datetime.utcnow().isoformat()
            pw_hash = hash_password("superpassword")
            # Insert minimal fields; id will auto-generate per table schema
            cur.execute(
                "INSERT INTO users (email, full_name, role, password_hash, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("superuser@local", "Superuser", "superuser", pw_hash, "active", now),
            )
            conn.commit()
    except Exception:
        pass

def _ws_headers(ws) -> List[str]:
    try:
        return ws.row_values(1)
    except Exception:
        return []

def _serialize_row_for_sheet(row: Dict[str, Any]) -> Dict[str, str]:
    out = {}
    for k, v in row.items():
        if isinstance(v, (bytes, bytearray)):
            out[k] = ""  # don't store blobs in sheet
        elif v is None:
            out[k] = ""
        elif isinstance(v, (dict, list)):
            out[k] = json.dumps(v, ensure_ascii=False)
        else:
            out[k] = str(v)
    return out

def _sheet_upsert(table: str, row: Dict[str, Any], key: str = "id"):
    ws = _ensure_worksheet(table, list(row.keys()))
    if not ws:
        return
    headers = _ws_headers(ws)
    key_idx = headers.index(key) + 1 if key in headers else None
    if not key_idx:
        # add key to header
        headers.append(key)
        ws.update("1:1", [headers])
        key_idx = len(headers)
    row_s = _serialize_row_for_sheet(row)
    # Ensure all header keys exist
    for h in headers:
        if h not in row_s:
            row_s[h] = ""
    try:
        # Find existing row by key
        col_vals = ws.col_values(key_idx)
        try:
            found = col_vals.index(str(row.get(key))) + 1  # 1-based
        except ValueError:
            found = None
        values = [row_s.get(h, "") for h in headers]
        if found and found != 1:
            ws.update(f"{found}:{found}", [values])
        else:
            ws.append_row(values, value_input_option="USER_ENTERED")
    except Exception:
        pass

def _sheet_delete(table: str, key_val: str, key: str = "id"):
    ws = _ensure_worksheet(table, [key])
    if not ws:
        return
    headers = _ws_headers(ws)
    if key not in headers:
        return
    key_idx = headers.index(key) + 1
    try:
        col_vals = ws.col_values(key_idx)
        idx = col_vals.index(str(key_val)) + 1
        if idx > 1:
            ws.delete_rows(idx)
    except Exception:
        pass

def _sync_sqlite_row_to_sheet(table: str, row_id: Optional[str]):
    if not row_id:
        return
    try:
        # Read full row from SQLite and push to Sheets
        st.session_state["__audit_disabled"] = True
        conn = get_db()
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table})")
        cols = [r[1] for r in cur.fetchall()]
        cur.execute(f"SELECT * FROM {table} WHERE id=?", (row_id,))
        r = cur.fetchone()
        if not r:
            return
        row_dict = {c: r[c] for c in cols if c in r.keys()}
        # Mirror: don't store blobs
        for c in list(row_dict.keys()):
            if isinstance(row_dict[c], (bytes, bytearray)):
                row_dict[c] = ""
        _sheet_upsert(table, row_dict, key="id")
    except Exception:
        pass
    finally:
        st.session_state["__audit_disabled"] = False

def _drive_upload(file_bytes: bytes, filename: str) -> Dict[str, str]:
    svc = _drive_service()
    res = {"file_id": "", "webViewLink": "", "webContentLink": ""}
    if not svc:
        return res
    media = MediaIoBaseUpload(io.BytesIO(file_bytes), mimetype="application/octet-stream", resumable=False)
    meta = {"name": filename}
    if DRIVE_FOLDER_ID:
        meta["parents"] = [DRIVE_FOLDER_ID]
    try:
        f = svc.files().create(body=meta, media_body=media, fields="id, name, webViewLink, webContentLink").execute()
        file_id = f.get("id")
        # Make it readable by anyone with link
        try:
            svc.permissions().create(fileId=file_id, body={"type": "anyone", "role": "reader"}).execute()
        except Exception:
            pass
        # Re-fetch to get links
        f2 = svc.files().get(fileId=file_id, fields="id, webViewLink, webContentLink").execute()
        res = {
            "file_id": file_id or "",
            "webViewLink": f2.get("webViewLink", ""),
            "webContentLink": f2.get("webContentLink", f"https://drive.google.com/uc?id={file_id}&export=download") if file_id else "",
        }
        return res
    except Exception:
        return res

def _drive_find_file_by_name(filename: str) -> Dict[str, str]:
    svc = _drive_service()
    if not svc or not filename:
        return {}
    try:
        # Search in folder if provided; newest first
        safe_name = filename.replace("'", "\'")
        q_parts = [f"name = '{safe_name}'", "trashed = false"]
        if DRIVE_FOLDER_ID:
            q_parts.append(f"'{DRIVE_FOLDER_ID}' in parents")
        q = " and ".join(q_parts)
        resp = svc.files().list(q=q, fields="files(id, name, webViewLink, webContentLink)", orderBy="modifiedTime desc", pageSize=1).execute()
        files = resp.get("files", [])
        if not files:
            return {}
        f = files[0]
        fid = f.get("id")
        # Ensure public readable
        try:
            svc.permissions().create(fileId=fid, body={"type": "anyone", "role": "reader"}).execute()
        except Exception:
            pass
        return {
            "file_id": fid,
            "webViewLink": f.get("webViewLink", ""),
            "webContentLink": f.get("webContentLink", f"https://drive.google.com/uc?id={fid}&export=download") if fid else "",
        }
    except Exception:
        return {}

def ensure_gsheets_backend():
    """Prepare worksheets with minimal columns used by the app so syncing doesn't fail."""
    table_columns: Dict[str, List[str]] = {
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
        "inventory": ["id","nama_barang","kode_barang","qty","harga","updated_at","finance_approved","director_approved","divisi"]
    }
    for t, cols in table_columns.items():
        _ensure_worksheet(t, cols)

def sop_module():
    user = require_login()
    st.header("📚 Kebijakan & SOP")
    conn = get_db()
    cur = conn.cursor()

    # Introspeksi kolom untuk fleksibilitas skema
    cur.execute("PRAGMA table_info(sop)")
    sop_cols = [row[1] for row in cur.fetchall()]
    sop_date_col = "tanggal_terbit" if "tanggal_terbit" in sop_cols else ("tanggal_upload" if "tanggal_upload" in sop_cols else None)

    tab_upload, tab_daftar, tab_approve = st.tabs(["🆕 Upload SOP", "📋 Daftar & Rekap", "✅ Approval Director"])

    # --- Tab 1: Upload SOP ---
    with tab_upload:
        st.subheader("Upload SOP / Kebijakan")
        with st.form("sop_add", clear_on_submit=True):
            judul = st.text_input("Judul Kebijakan / SOP")
            tgl = st.date_input("Tanggal Terbit" if sop_date_col == "tanggal_terbit" else "Tanggal", value=date.today())
            f = st.file_uploader("Upload File SOP (PDF/DOC)")
            submit = st.form_submit_button("💾 Simpan")
            if submit:
                if not judul or not f:
                    st.warning("Judul dan file wajib diisi.")
                else:
                    sid = gen_id("sop")
                    blob, fname, _ = upload_file_and_store(f)
                    meta = get_last_upload_meta()
                    cols = ["id", "judul", "file_name"]
                    vals = [sid, judul, fname]
                    # Store Drive metadata
                    for k_sql, v in (("file_id", meta.get("file_id")), ("file_url", meta.get("file_url"))):
                        try:
                            cur.execute(f"ALTER TABLE sop ADD COLUMN {k_sql} TEXT")
                        except Exception:
                            pass
                        cols.append(k_sql); vals.append(v)
                    if sop_date_col == "tanggal_terbit":
                        cols.append("tanggal_terbit"); vals.append(tgl.isoformat())
                    elif sop_date_col == "tanggal_upload":
                        cols.append("tanggal_upload"); vals.append(datetime.utcnow().isoformat())
                    if "director_approved" in sop_cols:
                        cols.append("director_approved"); vals.append(0)
                    for opt in ("memo", "board_note"):
                        if opt in sop_cols:
                            cols.append(opt); vals.append("")
                    placeholders = ", ".join(["?" for _ in cols])
                    cur.execute(f"INSERT INTO sop ({', '.join(cols)}) VALUES ({placeholders})", vals)
                    conn.commit()
                    try:
                        audit_log("sop", "upload", target=sid, details=f"{judul}; file={fname}")
                    except Exception:
                        pass
                    st.success("SOP berhasil diupload. Menunggu approval Director.")

    # --- Tab 2: Daftar & Rekap ---
    with tab_daftar:
        st.subheader("Daftar SOP")
        # Build SELECT dinamis
        cur.execute("PRAGMA table_info(sop)")
        sop_cols = [row[1] for row in cur.fetchall()]
        sop_date_col = "tanggal_terbit" if "tanggal_terbit" in sop_cols else ("tanggal_upload" if "tanggal_upload" in sop_cols else None)
        select_cols = ["id", "judul"]
        if sop_date_col: select_cols.append(sop_date_col)
        if "director_approved" in sop_cols: select_cols.append("director_approved")
        if "file_name" in sop_cols: select_cols.append("file_name")
        df = pd.read_sql_query(f"SELECT {', '.join(select_cols)} FROM sop ORDER BY " + (sop_date_col or "id") + " DESC", conn)

        # Filter UI
        col1, col2, col3 = st.columns([2,2,2])
        with col1:
            q = st.text_input("Cari Judul", "")
        with col2:
            status_opt = ["Semua", "Approved", "Belum"] if "director_approved" in df.columns else ["Semua"]
            status_sel = st.selectbox("Status", status_opt)
        with col3:
            if sop_date_col and not df.empty:
                min_d = pd.to_datetime(df[sop_date_col]).min().date()
                max_d = pd.to_datetime(df[sop_date_col]).max().date()
                dr = st.date_input("Rentang Tanggal", value=(min_d, max_d))
            else:
                dr = None

        dff = df.copy()
        if q:
            dff = dff[dff["judul"].astype(str).str.contains(q, case=False, na=False)]
        if status_sel != "Semua" and "director_approved" in dff.columns:
            dff = dff[dff["director_approved"] == (1 if status_sel == "Approved" else 0)]
        if dr and isinstance(dr, (list, tuple)) and len(dr) == 2 and sop_date_col and sop_date_col in dff.columns:
            s, e = dr
            dff = dff[(pd.to_datetime(dff[sop_date_col]) >= pd.to_datetime(s)) & (pd.to_datetime(dff[sop_date_col]) <= pd.to_datetime(e))]

        # Tampilan tabel ramah pengguna
        if not dff.empty:
            show = dff.copy()
            if "director_approved" in show.columns:
                show["Status"] = show["director_approved"].map({1: "✅ Approved", 0: "🕒 Proses"})
            cols_show = [c for c in ["judul", sop_date_col, "file_name", "Status"] if (c and c in show.columns)]
            st.dataframe(show[cols_show], use_container_width=True)
            # Download CSV
            st.download_button("⬇️ Download CSV", data=show[cols_show].to_csv(index=False).encode("utf-8"), file_name="daftar_sop.csv")
            # Download file per item (opsional pilih)
            if "id" in show.columns and "file_name" in show.columns:
                opsi = {f"{r['judul']} — {r.get(sop_date_col, '')} ({r['file_name'] or '-'})": r['id'] for _, r in show.iterrows()}
                if opsi:
                    pilih = st.selectbox("Unduh file SOP", [""] + list(opsi.keys()))
                    if pilih:
                        sid = opsi[pilih]
                        row = pd.read_sql_query("SELECT file_name, file_url FROM sop WHERE id=?", conn, params=(sid,)).iloc[0]
                        url = row.get("file_url") if "file_url" in row.index else None
                        if row["file_name"] and url:
                            st.markdown(f"<a href='{url}' target='_blank'>⬇️ Download {row['file_name']}</a>", unsafe_allow_html=True)
        else:
            st.info("Belum ada SOP.")

        # Rekap Bulanan SOP
        st.markdown("#### 📅 Rekap Bulanan SOP (Otomatis)")
        this_month = date.today().strftime("%Y-%m")
        if not dff.empty and sop_date_col and sop_date_col in dff.columns:
            df_month = dff[dff[sop_date_col].astype(str).str[:7] == this_month]
        else:
            df_month = pd.DataFrame()
        st.write(f"Total SOP/Kebijakan bulan ini: {len(df_month)}")

    # --- Tab 3: Approval Director ---
    with tab_approve:
        if user["role"] not in ["director", "superuser"]:
            st.info("Hanya Director/Superuser yang dapat meng-approve.")
        elif "director_approved" not in sop_cols:
            st.info("Kolom director_approved belum tersedia pada tabel SOP. Approval tidak bisa dilakukan.")
        else:
            df_pend = pd.read_sql_query(
                f"SELECT id, judul" + (f", {sop_date_col}" if sop_date_col else "") + ", file_name FROM sop WHERE director_approved=0 ORDER BY " + (sop_date_col or "id") + " DESC",
                conn
            )
            if df_pend.empty:
                st.success("Tidak ada item menunggu approval.")
            else:
                for _, row in df_pend.iterrows():
                    title = f"{row['judul']}" + (f" | {row[sop_date_col]}" if sop_date_col and row.get(sop_date_col) else "")
                    with st.expander(title):
                        st.write(f"File: {row.get('file_name') or '-'}")
                        note = st.text_area("Catatan Director (opsional)", key=f"sop_note_{row['id']}")
                        if st.button("✅ Approve", key=f"sop_approve_{row['id']}"):
                            if "memo" in sop_cols:
                                cur.execute("UPDATE sop SET director_approved=1, memo=? WHERE id=?", (note, row['id']))
                            else:
                                cur.execute("UPDATE sop SET director_approved=1 WHERE id=?", (row['id'],))
                            conn.commit()
                            try:
                                audit_log("sop", "director_approval", target=row['id'], details=f"note={note}")
                            except Exception:
                                pass
                            st.success("SOP approved.")
                            st.rerun()
    # Force create default superuser if not exists (guaranteed on first run)
    cur.execute("SELECT COUNT(*) as c FROM users")
    c = cur.fetchone()["c"] if cur.description else 0
    if c == 0:
        pw = hash_password("superpassword")
        now = datetime.utcnow().isoformat()
        cur.execute("INSERT INTO users (email, full_name, role, password_hash, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    ("superuser@local", "Superuser", "superuser", pw, "active", now))
        conn.commit()
    # Surat Masuk
    cur.execute("""
    CREATE TABLE IF NOT EXISTS surat_masuk (
        id TEXT PRIMARY KEY,
        indeks TEXT,
        nomor TEXT,
        pengirim TEXT,
        tanggal TEXT,
        perihal TEXT,
        file_blob BLOB,
        file_name TEXT,
        status TEXT,
        follow_up TEXT
    )
    """)
    # Surat Keluar
    cur.execute("""
    CREATE TABLE IF NOT EXISTS surat_keluar (
        id TEXT PRIMARY KEY,
        indeks TEXT,
        nomor TEXT,
        tanggal TEXT,
        ditujukan TEXT,
        perihal TEXT,
        lampiran_blob BLOB,
        lampiran_name TEXT,
        pengirim TEXT,
        draft_blob BLOB,
        draft_name TEXT,
        status TEXT,
        follow_up TEXT,
        director_note TEXT,
        director_approved INTEGER DEFAULT 0,
        final_blob BLOB,
        final_name TEXT
    )
    """)
    # MoU
    cur.execute("""
    CREATE TABLE IF NOT EXISTS mou (
        id TEXT PRIMARY KEY,
        nomor TEXT,
        nama TEXT,
        pihak TEXT,
        jenis TEXT,
        tgl_mulai TEXT,
        tgl_selesai TEXT,
        divisi TEXT,
        file_blob BLOB,
        file_name TEXT,
        board_note TEXT,
        board_approved INTEGER DEFAULT 0,
        director_note TEXT,
        director_approved INTEGER DEFAULT 0,
        final_blob BLOB,
        final_name TEXT
    )
    """)
    # Cash Advance
    cur.execute("""
    CREATE TABLE IF NOT EXISTS cash_advance (
        id TEXT PRIMARY KEY,
        divisi TEXT,
        items_json TEXT,
        totals REAL,
        tanggal TEXT,
        finance_note TEXT,
        finance_approved INTEGER DEFAULT 0,
        director_note TEXT,
        director_approved INTEGER DEFAULT 0
    )
    """)
    # PMR
    cur.execute("""
    CREATE TABLE IF NOT EXISTS pmr (
        id TEXT PRIMARY KEY,
        nama TEXT,
        file1_blob BLOB,
        file1_name TEXT,
        file2_blob BLOB,
        file2_name TEXT,
        bulan TEXT,
        finance_note TEXT,
        finance_approved INTEGER DEFAULT 0,
        director_note TEXT,
        director_approved INTEGER DEFAULT 0,
        tanggal_submit TEXT
    )
    """)
    # Cuti
    cur.execute("""
    CREATE TABLE IF NOT EXISTS cuti (
        id TEXT PRIMARY KEY,
        nama TEXT,
        tgl_mulai TEXT,
        tgl_selesai TEXT,
        durasi INTEGER,
        kuota_tahunan INTEGER,
        cuti_terpakai INTEGER,
        sisa_kuota INTEGER,
        status TEXT,
        finance_note TEXT,
        finance_approved INTEGER DEFAULT 0,
        director_note TEXT,
        director_approved INTEGER DEFAULT 0
    )
    """)
    # Flex
    cur.execute("""
    CREATE TABLE IF NOT EXISTS flex (
        id TEXT PRIMARY KEY,
        nama TEXT,
        tanggal TEXT,
        jam_mulai TEXT,
        jam_selesai TEXT,
        finance_note TEXT,
        finance_approved INTEGER DEFAULT 0,
        director_note TEXT,
        director_approved INTEGER DEFAULT 0
    )
    """)
    # Delegasi
    cur.execute("""
    CREATE TABLE IF NOT EXISTS delegasi (
        id TEXT PRIMARY KEY,
        judul TEXT,
        deskripsi TEXT,
        pic TEXT,
        tgl_mulai TEXT,
        tgl_selesai TEXT,
        file_blob BLOB,
        file_name TEXT,
        status TEXT,
        tanggal_update TEXT
    )
    """)
    # Mobil
    cur.execute("""
    CREATE TABLE IF NOT EXISTS mobil (
        id TEXT PRIMARY KEY,
        nama_pengguna TEXT,
        divisi TEXT,
        tgl_mulai TEXT,
        tgl_selesai TEXT,
        tujuan TEXT,
        kendaraan TEXT,
        driver TEXT,
        status TEXT,
        finance_note TEXT
    )
    """)
    # Calendar
    cur.execute("""
    CREATE TABLE IF NOT EXISTS calendar (
        id TEXT PRIMARY KEY,
        jenis TEXT,
        judul TEXT,
        nama_divisi TEXT,
        tgl_mulai TEXT,
        tgl_selesai TEXT,
        deskripsi TEXT,
        file_blob BLOB,
        file_name TEXT,
        is_holiday INTEGER DEFAULT 0,
        sumber TEXT,
        ditetapkan_oleh TEXT,
        tanggal_penetapan TEXT
    )
    """)
    # Public Holidays
    cur.execute("""
    CREATE TABLE IF NOT EXISTS public_holidays (
        tahun INTEGER,
        tanggal TEXT,
        nama TEXT,
        keterangan TEXT,
        ditetapkan_oleh TEXT,
        tanggal_penetapan TEXT
    )
    """)
    # SOP
    cur.execute("""
    CREATE TABLE IF NOT EXISTS sop (
        id TEXT PRIMARY KEY,
        judul TEXT,
        file_blob BLOB,
        file_name TEXT,
        tanggal_upload TEXT
    )
    """)
    # Notulen
    cur.execute("""
    CREATE TABLE IF NOT EXISTS notulen (
        id TEXT PRIMARY KEY,
        judul TEXT,
        file_blob BLOB,
        file_name TEXT,
        tanggal_upload TEXT
    )
    """)
    # File Log
    cur.execute("""
    CREATE TABLE IF NOT EXISTS file_log (
        id TEXT PRIMARY KEY,
        modul TEXT,
        file_name TEXT,
        versi INTEGER,
        deleted_by TEXT,
        tanggal_hapus TEXT,
        alasan TEXT
    )
    """)
    # Migration: ensure optional columns exist for richer audit
    try:
        cur.execute("PRAGMA table_info(file_log)")
        fl_cols = [row[1] for row in cur.fetchall()]
        # add uploaded_by
        if "uploaded_by" not in fl_cols:
            cur.execute("ALTER TABLE file_log ADD COLUMN uploaded_by TEXT")
        # add tanggal_upload
        if "tanggal_upload" not in fl_cols:
            cur.execute("ALTER TABLE file_log ADD COLUMN tanggal_upload TEXT")
        # add action
        if "action" not in fl_cols:
            cur.execute("ALTER TABLE file_log ADD COLUMN action TEXT")
    except Exception:
        pass
    conn.commit()
    conn.close()

# Use in-memory SQLite as a fast cache while Google Sheets is the source of truth
DB_PATH = ":memory:"
SALT = "office_ops_salt_v1"

# --- Password hashing utility ---
def hash_password(password: str) -> str:
    import hashlib
    salted = (password + SALT).encode('utf-8')
    return hashlib.sha256(salted).hexdigest()

icon_path = os.path.join(os.path.dirname(__file__), "icon.png")
if os.path.exists(icon_path):
    st.set_page_config(page_title="WIJNA Manajemen System", page_icon=icon_path, layout="wide")
else:
    st.set_page_config(page_title="WIJNA Manajemen System", page_icon="📊", layout="wide")
# --- Global CSS for modern look ---
st.markdown(
    """
    <style>
    .main .block-container {padding-top: 2rem;}
    .stButton>button, .stDownloadButton>button {
        background: linear-gradient(90deg, #4f8cff 0%, #38c6ff 100%);
        color: white; border: none; border-radius: 6px; font-weight: 600;
        box-shadow: 0 2px 8px rgba(80,140,255,0.08);
        margin-bottom: 6px;
    }
    .stButton>button:hover, .stDownloadButton>button:hover {
        background: linear-gradient(90deg, #38c6ff 0%, #4f8cff 100%);
        color: #fff;
    }
    .stDataFrame, .stTable {background: #f8fbff; border-radius: 8px;}
    .stExpanderHeader {font-weight: 700; color: #2a5d9f;}
    .stTextInput>div>input, .stTextArea>div>textarea {
        border-radius: 6px; border: 1px solid #b3d1ff;
    }
    .stFileUploader>div>div {background: #eaf4ff; border-radius: 6px;}
    .stMetric {background: #eaf4ff; border-radius: 8px;}
    </style>
    """,
    unsafe_allow_html=True
)
# Pastikan pemanggilan st.markdown(table_html, unsafe_allow_html=True) dilakukan di bagian yang tepat pada kode Daftar Inventaris

# -------------------------
# Utilities
# -------------------------
# --- Database connection utility --- 
def format_datetime_wib(dtstr):
    try:
        dt_utc = datetime.fromisoformat(dtstr)
        dt_wib = dt_utc + timedelta(hours=7)
        return dt_wib.strftime('%d-%m-%Y %H:%M') + ' WIB'
    except Exception:
        return dtstr
class _Row:
    def __init__(self, cols: List[str], values: List[Any]):
        self._cols = cols
        self._vals = values
        self._map = {c: (values[i] if i < len(values) else None) for i, c in enumerate(cols)}
    def __getitem__(self, key):
        if isinstance(key, int):
            return self._vals[key]
        return self._map.get(key)
    def keys(self):
        return self._cols
    def get(self, k, d=None):
        return self._map.get(k, d)

class SheetsDB:
    def __init__(self):
        self.gc, self.sh = _gs_client_and_sheet()
    def worksheet(self, table: str):
        return _ensure_worksheet(table, [])
    def headers(self, table: str) -> List[str]:
        ws = self.worksheet(table)
        return ws.row_values(1) if ws else []
    def all_records(self, table: str) -> List[Dict[str, Any]]:
        ws = self.worksheet(table)
        if not ws:
            return []
        values = ws.get_all_values()
        if not values:
            return []
        headers = [h.strip() for h in (values[0] or [])]
        recs = []
        for row in values[1:]:
            rec = {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}
            recs.append(rec)
        return recs
    def upsert(self, table: str, data: Dict[str, Any], key: str = "id"):
        if key not in data or not data[key]:
            data[key] = gen_id(table[:3] if len(table) >= 3 else table)
        _sheet_upsert(table, data, key=key)
        return data[key]
    def delete(self, table: str, key_val: Any, key: str = "id"):
        _sheet_delete(table, str(key_val), key=key)

def _parse_simple_where(where: str, params: List[Any]):
    tokens = re.split(r"\s+(and|or)\s+", where, flags=re.IGNORECASE)
    clauses = []
    ops = []
    i = 0
    for t in tokens:
        tl = t.strip().lower()
        if tl in ("and", "or"):
            ops.append(tl)
        elif tl:
            clauses.append(t)
    # Build evaluators
    p_idx = 0
    evals = []
    for c in clauses:
        cl = c.strip()
        m = re.match(r"substr\((\w+),\s*1,\s*7\)\s*=\s*\?", cl, flags=re.IGNORECASE)
        if m:
            col = m.group(1)
            val = str(params[p_idx]); p_idx += 1
            evals.append(lambda row, col=col, val=val: str(row.get(col, ""))[:7] == val)
            continue
        m = re.match(r"(\w+)\s+like\s+\?", cl, flags=re.IGNORECASE)
        if m:
            col = m.group(1)
            pat = str(params[p_idx]); p_idx += 1
            needle = pat.replace('%','').lower()
            evals.append(lambda row, col=col, needle=needle: needle in str(row.get(col, "")).lower())
            continue
        m = re.match(r"date\((\w+)\)\s*<=\s*date\('\s*now\s*','\+(\d+) day'\)", cl, flags=re.IGNORECASE)
        if m:
            col = m.group(1); days = int(m.group(2))
            from datetime import datetime, timedelta
            bound = datetime.utcnow() + timedelta(days=days)
            evals.append(lambda row, col=col, bound=bound: pd.to_datetime(row.get(col), errors='coerce') <= bound)
            continue
        m = re.match(r"(\w+)\s*=\s*\?", cl)
        if m:
            col = m.group(1)
            val = params[p_idx]; p_idx += 1
            evals.append(lambda row, col=col, val=val: str(row.get(col, "")) == str(val))
            continue
        m = re.match(r"(\w+)\s*=\s*([0-9\-]+)", cl)
        if m:
            col = m.group(1); val = m.group(2)
            evals.append(lambda row, col=col, val=val: str(row.get(col, "")) == val)
            continue
    def predicate(row):
        if not evals:
            return True
        res = evals[0](row)
        idx = 1
        for op in ops:
            if idx >= len(evals):
                break
            if op == 'and':
                res = res and evals[idx](row)
            else:
                res = res or evals[idx](row)
            idx += 1
        return res
    return predicate

class SheetsCursor:
    def __init__(self, sdb: SheetsDB):
        self._sdb = sdb
        self._results: List[_Row] = []
        self.description = None
    def execute(self, sql: str, params: Any = ()): 
        sql = sql.strip()
        low = sql.lower()
        # DDL: ALTER ADD COLUMN
        if low.startswith("alter table") and " add column " in low:
            parts = low.split()
            tbl = parts[2]
            col = low.split(" add column ",1)[1].strip().split()[0].strip('`"[]')
            _ensure_worksheet(tbl, [col])
            self._results = []
            return self
        # DDL: CREATE TABLE IF NOT EXISTS tbl (...)
        if low.startswith("create table") and " if not exists " in low:
            after = low.split(" if not exists ",1)[1]
            tbl = after.split("(",1)[0].strip().split()[0]
            cols_part = sql[sql.index("(")+1: sql.rindex(")")]
            cols = []
            for seg in cols_part.split(","):
                name = seg.strip().split()[0].strip('`"[]')
                if name and name.upper() not in {"PRIMARY","FOREIGN","UNIQUE","CONSTRAINT"}:
                    cols.append(name)
            _ensure_worksheet(tbl, cols)
            self._results = []
            return self
        # PRAGMA table_info(table)
        if low.startswith("pragma table_info("):
            tbl = low.split("pragma table_info(",1)[1].split(")",1)[0]
            headers = self._sdb.headers(tbl)
            # emulate sqlite schema rows
            rows = []
            for idx, h in enumerate(headers):
                rows.append(_Row(["cid","name","type","notnull","dflt_value","pk"], [idx, h, "TEXT", 0, None, 0]))
            self._results = rows
            self.description = [("name",)]
            return self
        # SELECT ... FROM ... [WHERE ...] [ORDER BY ...]
        if low.startswith("select"):
            # COUNT(*) support
            m = re.match(r"select\s+(.+?)\s+from\s+(\w+)(?:\s+where\s+(.+?))?(?:\s+order\s+by\s+(.+))?$", low)
            if m:
                cols_sel, table, where, order = m.groups()
                recs = self._sdb.all_records(table)
                if where:
                    pred = _parse_simple_where(where, list(params) if isinstance(params, (list, tuple)) else [])
                    recs = [r for r in recs if pred(r)]
                if cols_sel.strip().startswith("count(*)"):
                    alias = 'c' if ' as c' in cols_sel else 'count'
                    self._results = [_Row([alias], [len(recs)])]
                    self.description = [(alias,)]
                    return self
                # columns
                cols = [c.strip() for c in cols_sel.split(',')]
                if cols == ['*']:
                    cols = list(recs[0].keys()) if recs else []
                # order
                if order:
                    ord_tokens = order.strip().split()
                    ord_col = ord_tokens[0]
                    desc = any(t.upper()=="DESC" for t in ord_tokens[1:])
                    recs.sort(key=lambda r: str(r.get(ord_col, "")), reverse=desc)
                rows = [_Row(cols, [r.get(c, "") for c in cols]) for r in recs]
                self._results = rows
                self.description = [(c,) for c in cols]
                return self
        # INSERT INTO table (cols) VALUES (?,...)
        if low.startswith("insert into"):
            m = re.match(r"insert\s+into\s+(\w+)\s*\(([^)]+)\)\s*values\s*\(([^)]+)\)", low)
            if m:
                table = m.group(1)
                cols = [c.strip().strip('`"') for c in m.group(2).split(',')]
                vals = list(params) if isinstance(params, (list, tuple)) else []
                data = {cols[i]: (vals[i] if i < len(vals) else "") for i in range(len(cols))}
                new_id = self._sdb.upsert(table, data, key="id" if "id" in cols else cols[0])
                self._results = []
                return self
        # UPDATE table SET col=?, ... WHERE id=?
        if low.startswith("update"):
            m = re.match(r"update\s+(\w+)\s+set\s+(.+?)\s+where\s+(.+)$", low)
            if m:
                table = m.group(1)
                set_part = m.group(2)
                where_part = m.group(3)
                assigns = [p.strip() for p in set_part.split(',')]
                cols = [a.split('=')[0].strip() for a in assigns]
                vals = list(params)
                # Assume last param is key for id=? style
                key = 'id'
                key_val = vals[-1] if vals else None
                updates = {cols[i]: vals[i] for i in range(len(cols))}
                # Read sheet rows and update
                ws = self._sdb.worksheet(table)
                headers = self._sdb.headers(table)
                if key in headers:
                    key_idx = headers.index(key) + 1
                    col_vals = ws.col_values(key_idx)
                    try:
                        row_idx = col_vals.index(str(key_val)) + 1
                    except ValueError:
                        row_idx = None
                    if row_idx and row_idx > 1:
                        for c, v in updates.items():
                            if c in headers:
                                col_i = headers.index(c) + 1
                                ws.update_cell(row_idx, col_i, str(v) if v is not None else "")
                self._results = []
                return self
        # DELETE FROM table WHERE id=?
        if low.startswith("delete from"):
            m = re.match(r"delete\s+from\s+(\w+)\s+where\s+(.+)$", low)
            if m:
                table = m.group(1)
                # assume id=?
                key_val = params[-1] if isinstance(params, (list, tuple)) and params else None
                self._sdb.delete(table, key_val, key='id')
                self._results = []
                return self
        # Fallback: no-op
        self._results = []
        return self
    def executemany(self, sql, seq_of_params):
        for p in seq_of_params:
            self.execute(sql, p)
        return self
    def fetchone(self):
        return self._results[0] if self._results else None
    def fetchall(self):
        return self._results

class SheetsConnection:
    def __init__(self):
        self._sdb = SheetsDB()
    def cursor(self):
        return SheetsCursor(self._sdb)
    def commit(self):
        return None
    def close(self):
        return None

def get_db():
    # Return a Sheets-backed connection shim compatible with existing code paths
    return SheetsConnection()

def _create_sqlite_table_from_headers(cur, table: str, headers: List[str]):
    """Create a SQLite table with given headers if it does not exist."""
    if not headers:
        return
    # Basic affinity: TEXT for all to keep it simple and compatible
    cols_sql = ", ".join([f"{h} TEXT" for h in headers])
    try:
        # Ensure the worksheet with headers exists
        _ensure_worksheet(table, headers)
    except Exception:
        pass
def ensure_db():
    """Bootstrap in-memory cache from Google Sheets and ensure worksheets/headers exist.
    - Ensures required worksheets exist with headers
    - Builds in-memory SQLite tables mirroring Sheets headers
    - Hydrates data from Sheets into the in-memory cache
    - Ensures a default superuser exists in both cache and Sheets
    """
    # Ensure Sheets are available and headers exist
    check_and_create_worksheets()
    # Build cache schema from available worksheets and hydrate
    gc, sh = _gs_client_and_sheet()
    conn = get_db()
    cur = conn.cursor()
    if sh:
        try:
            worksheets = sh.worksheets()
        except Exception:
            worksheets = []
        for ws in worksheets:
            try:
                headers = [h.strip() for h in (ws.row_values(1) or []) if h and h.strip()]
                if not headers:
                    continue
                _create_sqlite_table_from_headers(cur, ws.title, headers)
                # Clear then insert current data
                try:
                    values = ws.get_all_values()
                except Exception:
                    values = []
                if values and len(values) > 1:
                    hdr = values[0]
                    data_rows = values[1:]
                    # Align to headers present in SQLite (which mirrors hdr)
                    common = [c for c in hdr]
                    placeholders = ",".join(["?" for _ in common])
                    for row in data_rows:
                        rec = {hdr[i]: (row[i] if i < len(row) else "") for i in range(len(hdr))}
                        params = [str(rec.get(c, "")) for c in common]
                        try:
                            cur.execute(f"INSERT INTO {ws.title} ({', '.join(common)}) VALUES ({placeholders})", params)
                        except Exception:
                            pass
            except Exception:
                pass
        try:
            conn.commit()
        except Exception:
            pass
    # Ensure a default superuser exists (in cache and mirrored to Sheets via audit hook)
    try:
        ensure_default_superuser()
    except Exception:
        pass
def log_file_delete(modul, file_name, deleted_by, alasan=None):
    conn = get_db()
    cur = conn.cursor()
    log_id = gen_id("log")
    now = datetime.utcnow().isoformat()
    cur.execute("INSERT INTO file_log (id, modul, file_name, versi, deleted_by, tanggal_hapus, alasan) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (log_id, modul, file_name, 1, deleted_by, now, alasan or ""))
    conn.commit()
    
def audit_log(modul: str, action: str, target=None, details=None, actor=None):
    """Write an audit log entry to file_log.
    - modul: name of module (e.g., 'auth', 'cuti', 'delegasi')
    - action: verb (e.g., 'login', 'logout', 'create', 'update', 'delete', 'approve', 'review')
    - target: entity id/name being acted on
    - details: optional human-readable info
    - actor: email/name of actor; defaults to current user's email if available
    """
    try:
        # prevent recursive logging
        st.session_state["__audit_disabled"] = True
        # Use the same shared connection (audit is disabled so get_db returns raw)
        conn = get_db()
        cur = conn.cursor()
        now = datetime.utcnow().isoformat()
        # Resolve actor from session if not provided
        if not actor:
            u = st.session_state.get("user")
            actor = (u.get("email") or u.get("full_name")) if u else "-"
        # Check available columns
        cur.execute("PRAGMA table_info(file_log)")
        cols = {row[1] for row in cur.fetchall()}
        # Build insert dynamically
        data = {
            "id": gen_id("log"),
            "modul": modul,
            "file_name": target or "",
            "versi": 1,
            "uploaded_by": actor,
            "tanggal_upload": now,
            "alasan": (details or ""),
            "action": action,
        }
        insert_cols = [c for c in ["id","modul","file_name","versi",
                                    "uploaded_by" if "uploaded_by" in cols else None,
                                    "tanggal_upload" if "tanggal_upload" in cols else None,
                                    "alasan",
                                    "action" if "action" in cols else None] if c]
        placeholders = ", ".join(["?" for _ in insert_cols])
        cur.execute(f"INSERT INTO file_log ({', '.join(insert_cols)}) VALUES ({placeholders})",
                    [data[c] for c in insert_cols])
        conn.commit()
    except Exception:
        pass
    finally:
        try:
            del st.session_state["__audit_disabled"]
        except Exception:
            pass


def to_blob(file_bytes: bytes) -> bytes:
    # store base64 bytes (text) to BLOB, so we keep as bytes
    return base64.b64encode(file_bytes)

def from_blob(blob: bytes) -> bytes:
    if blob is None:
        return None
    try:
        return base64.b64decode(blob)
    except Exception:
        return blob

def gen_id(prefix="id"):
    return f"{prefix}_{uuid.uuid4().hex[:12]}"

# -------------------------
# Auth & Session
# -------------------------
def register_user(email, full_name, password):
    conn = get_db()
    cur = conn.cursor()
    pw = hash_password(password)
    now = datetime.utcnow().isoformat()
    try:
        cur.execute("INSERT INTO users (email, full_name, role, password_hash, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (email, full_name, "staff", pw, "pending", now))
        conn.commit()
        return True, "Registered — menunggu approval superuser."
    except Exception:
        return False, "Email sudah terdaftar."

def login_user(email, password):
    conn = get_db()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat()
    cur.execute("SELECT * FROM users WHERE email = ?", (email,))
    row = cur.fetchone()
    if not row:
        alasan = "User tidak ditemukan."
        audit_log("auth", "login_failed", target=email, details=alasan, actor=email)
        return False, alasan
    if row["status"] != "active":
        alasan = f"User status: {row['status']}. Tidak bisa login."
        audit_log("auth", "login_failed", target=email, details=alasan, actor=email)
        return False, alasan
    if row["password_hash"] == hash_password(password):
        # set session
        st.session_state["user"] = {"id": row["id"], "email": row["email"], "role": row["role"], "full_name": row["full_name"]}
        cur.execute("UPDATE users SET last_login = ? WHERE id = ?", (now, row["id"]))
        audit_log("auth", "login", target=email, details="Login sukses.", actor=email)
        conn.commit()
        return True, "Login sukses."
    else:
        alasan = "Password salah."
        audit_log("auth", "login_failed", target=email, details=alasan, actor=email)
        return False, alasan

def logout():
    # capture actor before clearing session
    actor_email = None
    if "user" in st.session_state and st.session_state["user"]:
        actor_email = st.session_state["user"].get("email") or st.session_state["user"].get("full_name")
    audit_log("auth", "logout", target=actor_email or "-", details="Logout", actor=actor_email)
    for k in ("user",):
        if k in st.session_state:
            del st.session_state[k]

def get_current_user():
    return st.session_state.get("user")

# -------------------------
# Simple decorators / checks
# -------------------------
def require_login():
    user = get_current_user()
    if not user:
        st.warning("Silakan login dulu.")
        st.stop()
    return user

def require_role(roles):
    user = require_login()
    if user["role"] not in roles:
        st.error(f"Akses ditolak. Diperlukan role: {roles}")
        st.stop()
    return user

# -------------------------
# UI Components: Authentication
# -------------------------
def auth_sidebar():
    st.sidebar.title("Authentication")
    user = get_current_user()
    if user:
        # Ambil info user lebih lengkap dari DB
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT email, full_name, role, status, last_login FROM users WHERE id = ?", (user["id"],))
        u = cur.fetchone()
        if u:
            st.sidebar.markdown("""
                <b>Email:</b> {email}<br>
                <b>Role:</b> {role}<br>
                <b>Status:</b> {status}<br>
                <b>Last Login:</b> {last_login}
                </div>
            """.format(
                full_name=u["full_name"],
                email=u["email"],
                role=u["role"],
                status=u["status"],
                last_login=u["last_login"] or "-"
            ), unsafe_allow_html=True)
        else:
            st.sidebar.write(f"Logged in: **{user['full_name']}** ({user['role']})")
    else:
        tabs = st.sidebar.tabs(["Login", "Register"])
        with tabs[0]:
            st.subheader("Login")
            with st.form("login_form_sidebar"):
                email_login = st.text_input("Email", key="login_email")
                pwd_login = st.text_input("Password", type="password", key="login_pwd")
                submit_login = st.form_submit_button("Login")
                if submit_login:
                    ok, msg = login_user(email_login.strip().lower(), pwd_login)
                    if ok:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)
        with tabs[1]:
            st.subheader("Register")
            with st.form("register_form_sidebar"):
                email_reg = st.text_input("Email", key="register_email")
                name_reg = st.text_input("Nama Lengkap", key="register_name")
                pwd_reg = st.text_input("Password", type="password", key="register_pwd")
                submit_reg = st.form_submit_button("Register")
                if submit_reg:
                    if not (email_reg and name_reg and pwd_reg):
                        st.error("Lengkapi semua field.")
                    else:
                        ok, msg = register_user(email_reg.strip().lower(), name_reg.strip(), pwd_reg)
                        if ok:
                            st.success(msg)
                        else:
                            st.error(msg)

# -------------------------
# Admin (superuser) panel
# -------------------------
def superuser_panel():
    require_role(["superuser"])
    st.header("Superuser — Manajemen User & Approval")
    st.subheader("Semua user")
    conn = get_db()
    cur = conn.cursor()
    df = pd.read_sql_query("SELECT id,email,full_name,role,status,last_login,created_at FROM users", conn)
    st.dataframe(df)

    tab1, tab2 = st.tabs(["🕒 User Baru Menunggu Approval", "👥 Semua User"])

    with tab1:
        st.subheader("User baru menunggu approval")
        conn1 = get_db()
        cur1 = conn1.cursor()
        cur1.execute("SELECT id,email,full_name,role,status,created_at FROM users WHERE status = 'pending'")
        pendings = cur1.fetchall()
        if pendings:
            for idx, p in enumerate(pendings):
                with st.expander(f"{p['full_name']} — {p['email']}"):
                    st.markdown(f'''
<div style="background:#f8fbff;border-radius:10px;padding:1.2em 1.5em 1em 1.5em;margin-bottom:1em;box-shadow:0 2px 8px rgba(80,140,255,0.07);">
<b>Nama:</b> {p['full_name']}<br>
<b>Email:</b> {p['email']}<br>
<b>Role:</b> <span style="color:#2563eb;font-weight:600">{p['role']}</span><br>
<b>Status:</b> <span style="color:#eab308;font-weight:600">{p['status']}</span><br>
<b>Tanggal Daftar:</b> {p['created_at'][:10]}<br>
<b>User ID:</b> <code>{p['id']}</code>
</div>
''', unsafe_allow_html=True)
                    # Action buttons for this user
                    col1, col2, col3 = st.columns([1,1,2])
                    if 'superuser_panel_action' not in st.session_state:
                        st.session_state['superuser_panel_action'] = {}
                    with col1:
                        if st.button("Approve", key=f"approve_{p['id']}_card_{idx}"):
                            conn_btn = get_db()
                            cur_btn = conn_btn.cursor()
                            cur_btn.execute("UPDATE users SET status='active' WHERE id = ?", (p['id'],))
                            conn_btn.commit()
                            conn_btn.close()
                            st.success("User diapprove.")
                            st.rerun()
                    with col2:
                        if st.button("Reject", key=f"reject_{p['id']}_card_{idx}"):
                            conn_btn = get_db()
                            cur_btn = conn_btn.cursor()
                            cur_btn.execute("UPDATE users SET status='inactive' WHERE id = ?", (p['id'],))
                            conn_btn.commit()
                            conn_btn.close()
                            st.info("User di-set inactive.")
                            st.rerun()
                    with col3:
                        new_role = st.selectbox("Set role", ["staff","finance","director","superuser"], index=["staff","finance","director","superuser"].index(p['role']), key=f"role_{p['id']}_card_{idx}")
                        if st.button("Update Role", key=f"setrole_{p['id']}_card_{idx}"):
                            conn_btn = get_db()
                            cur_btn = conn_btn.cursor()
                            cur_btn.execute("UPDATE users SET role=? WHERE id=?", (new_role, p['id']))
                            conn_btn.commit()
                            conn_btn.close()
                            st.success("Role diupdate.")
                            st.rerun()
        else:
            st.info("Tidak ada user pending.")
        conn1.close()

    with tab2:
        st.subheader("Semua user")
        conn2 = get_db()
        df2 = pd.read_sql_query("SELECT id,email,full_name,role,status,last_login,created_at FROM users", conn2)
        st.dataframe(df2, use_container_width=True)
        conn2.close()
    # (No form here, only one form with key 'admin_change' exists below)

    # Only one form, outside the tabs
    st.markdown("---")
    st.subheader("Aksi User Management")
    # Fetch all users for dropdown
    conn3 = get_db()
    cur3 = conn3.cursor()
    cur3.execute("SELECT id, email, full_name, role, status FROM users ORDER BY created_at DESC")
    user_rows = cur3.fetchall()
    conn3.close()
    user_options = [f"{row['id']} | {row['email']} | {row['full_name']} | {row['role']} | {row['status']}" for row in user_rows]
    user_id_map = {f"{row['id']} | {row['email']} | {row['full_name']} | {row['role']} | {row['status']}": row['id'] for row in user_rows}
    with st.form("admin_change"):
        selected_user = st.selectbox("Pilih user untuk edit", user_options, key="admin_user_select")
        uid = user_id_map[selected_user] if selected_user else ""
        newrole = st.selectbox("Pilih role baru", ["staff","finance","director","superuser"])
        newpw = st.text_input("Set new password (kosong = tidak diganti)", type="password")
        action = st.selectbox("Aksi", ["Update User", "Approve", "Reject"])
        submit = st.form_submit_button("Apply")
        if submit:
            conn3 = get_db()
            cur3 = conn3.cursor()
            if not uid:
                st.error("Masukkan user id.")
            else:
                if action == "Update User":
                    if newpw:
                        cur3.execute("UPDATE users SET role=?, password_hash=? WHERE id=?", (newrole, hash_password(newpw), uid))
                    else:
                        cur3.execute("UPDATE users SET role=? WHERE id=?", (newrole, uid))
                    conn3.commit()
                    st.success("User updated.")
                elif action == "Approve":
                    cur3.execute("UPDATE users SET status='active' WHERE id = ?", (uid,))
                    conn3.commit()
                    st.success("User diapprove.")
                elif action == "Reject":
                    cur3.execute("UPDATE users SET status='inactive' WHERE id = ?", (uid,))
                    conn3.commit()
                    st.info("User di-set inactive.")
            conn3.close()

        # (No form here, only one form with key 'admin_change' exists below)

# -------------------------
# Common helpers for modules
# -------------------------
def upload_file_and_store(file_uploader_obj):
    """Upload to Google Drive and return (blob,name,size).
    For Drive metadata, call get_last_upload_meta() after this.
    Blob is empty to avoid DB bloat; use file_url for downloads.
    """
    uploaded = file_uploader_obj
    if uploaded is None:
        return None, None, None
    raw = uploaded.read()
    name = uploaded.name
    # Upload to Drive
    drive_info = _drive_upload(raw, name)
    file_id = drive_info.get("file_id")
    file_url = drive_info.get("webContentLink") or drive_info.get("webViewLink")
    # Audit trail log upload
    try:
        user = get_current_user()
        conn = get_db()
        cur = conn.cursor()
        log_id = gen_id("log")
        now = datetime.utcnow().isoformat()
        cur.execute("INSERT INTO file_log (id, modul, file_name, versi, uploaded_by, tanggal_upload, action) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (log_id, "upload", name, 1, (user.get("full_name") if user else "-"), now, "upload"))
        conn.commit()
        conn.close()
    except Exception:
        pass
    # Stash meta for caller retrieval
    st.session_state["__last_upload_meta"] = {"file_id": file_id, "file_url": file_url}
    # Return empty blob to avoid DB bloat; callers should store name + id/url
    return b"", name, len(raw)

def get_last_upload_meta() -> Dict[str, str]:
    return st.session_state.get("__last_upload_meta", {})

def show_file_download(blob, filename, file_url: str = ""):
    # Prefer Drive link if available
    if file_url:
        st.markdown(f"<a href='{file_url}' target='_blank'>Download {filename}</a>", unsafe_allow_html=True)
    else:
        data = from_blob(blob)
        if data:
            b64 = base64.b64encode(data).decode()
            href = f'<a href="data:application/octet-stream;base64,{b64}" download="{filename}">Download {filename}</a>'
            st.markdown(href, unsafe_allow_html=True)
        else:
            # Try to find in Drive by filename
            info = _drive_find_file_by_name(filename)
            link = info.get("webContentLink") or info.get("webViewLink")
            if link:
                st.markdown(f"<a href='{link}' target='_blank'>Download {filename}</a>", unsafe_allow_html=True)

def _initial_full_sync_to_sheets():
    # Sync essential tables to Sheets if not synced yet
    if st.session_state.get("__synced_once"):
        return
    try:
        # Use the shared in-memory connection
        conn = get_db()
        cur = conn.cursor()
        # Enumerate tables we care about
        tables = [
            "users","sop","notulen","surat_masuk","surat_keluar","mou","cash_advance","pmr","cuti","flex","delegasi","mobil","calendar","public_holidays","inventory"
        ]
        for t in tables:
            try:
                df = pd.read_sql_query(f"SELECT * FROM {t}", conn)
            except Exception:
                continue
            if df is None or df.empty:
                continue
            cols = list(df.columns)
            _ensure_worksheet(t, cols)
            for _, r in df.iterrows():
                row = {c: r[c] for c in cols}
                for c in list(row.keys()):
                    if isinstance(row[c], (bytes, bytearray)):
                        row[c] = ""
                _sheet_upsert(t, row, key="id" if "id" in cols else cols[0])
        st.session_state["__synced_once"] = True
    except Exception:
        pass

def _hydrate_sqlite_from_sheets():
    gc, sh = _gs_client_and_sheet()
    if not sh:
        return
    try:
        conn = get_db()
        cur = conn.cursor()
        # Worksheets present
        try:
            worksheets = sh.worksheets()
        except Exception:
            worksheets = []
        for ws in worksheets:
            table = ws.title
            # Create table if missing using headers
            try:
                values = ws.get_all_values()
            except Exception:
                values = []
            headers = [h.strip() for h in (values[0] if values else [])]
            if not headers:
                continue
            _create_sqlite_table_from_headers(cur, table, headers)
            # Only insert if empty
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                if cur.fetchone()[0] > 0:
                    continue
            except Exception:
                continue
            if not values or len(values) <= 1:
                continue
            placeholders = ",".join(["?" for _ in headers])
            sql = f"INSERT INTO {table} ({', '.join(headers)}) VALUES ({placeholders})"
            for row in values[1:]:
                params = [str(row[i]) if i < len(row) else "" for i in range(len(headers))]
                try:
                    cur.execute(sql, params)
                except Exception:
                    pass
        conn.commit()
    except Exception:
        pass

# -------------------------
# Modules Implementation (concise)
# -------------------------
def inventory_module():
    # Prepare monthly rekap at the top
    user = require_login()
    conn = get_db()
    cur = conn.cursor()
    this_month = date.today().strftime("%Y-%m")
    df_month = pd.read_sql_query(f"SELECT * FROM inventory WHERE substr(updated_at,1,7)=?", conn, params=(this_month,))
    # --- UI with Tabs (always show tabs, even if df_month is empty) ---
    tab_labels = []
    tab_contents = []
    if user["role"] in ["staff", "superuser"]:
        st.markdown("# 📦 Inventory")
        tab_labels.append("➕ Tambah Barang")
        def staff_tab():
            with st.form("inv_add"):
                name = st.text_input("Nama Barang")
                keterangan_opsi = st.selectbox("Keterangan Tambahan", ["", "dijual", "rusak"], index=0)
                loc = st.text_input("Tempat Barang")
                status = st.selectbox("Status", ["Tersedia","Dipinjam","Rusak","Dijual"])
                # PIC dihapus
                f = st.file_uploader("Lampiran (opsional)")
                submitted = st.form_submit_button("Simpan (draft)")
                if submitted:
                    if not name:
                        st.warning("Nama barang wajib diisi.")
                    else:
                        full_nama = name
                        if keterangan_opsi:
                            full_nama += f" ({keterangan_opsi})"
                        iid = gen_id("inv")
                        now = datetime.utcnow().isoformat()
                        blob, fname, _ = upload_file_and_store(f) if f else (None, None, None)
                        # PIC dihapus, set kosong
                        pic = ""
                        cur.execute("""INSERT INTO inventory (id,name,location,status,pic,updated_at,finance_note,finance_approved,director_note,director_approved,file_blob,file_name)
                                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                                    (iid, full_nama, loc, status, pic, now, '', 0, '', 0, blob, fname))
                        # Store Drive metadata if available
                        if f:
                            meta = get_last_upload_meta()
                            try:
                                cur.execute("ALTER TABLE inventory ADD COLUMN file_id TEXT")
                            except Exception:
                                pass
                            try:
                                cur.execute("ALTER TABLE inventory ADD COLUMN file_url TEXT")
                            except Exception:
                                pass
                            try:
                                cur.execute("UPDATE inventory SET file_id=?, file_url=? WHERE id=?", (meta.get("file_id"), meta.get("file_url"), iid))
                            except Exception:
                                pass
                        conn.commit()
                        try:
                            audit_log("inventory", "create", target=iid, details=f"{full_nama} @ {loc} status={status}")
                        except Exception:
                            pass
                        st.success("Item disimpan sebagai draft. Menunggu review Finance.")
        tab_contents.append(staff_tab)
    if user["role"] in ["finance", "superuser"]:
        tab_labels.append("💰 Review Finance")
        def finance_tab():
            st.info("Approve item yang sudah diinput staf.")
            cur.execute("SELECT * FROM inventory WHERE finance_approved=0")
            rows = cur.fetchall()
            for idx, r in enumerate(rows):
                with st.container():
                    st.markdown(f"""
<div style='border:1.5px solid #b3d1ff; border-radius:10px; padding:1.2em 1em; margin-bottom:1.5em; background:#f8fbff;'>
<b>📦 {r['name']}</b> <span style='color:#888;'>(ID: {r['id']})</span><br>
<b>Lokasi:</b> {r['location']}<br>
<b>Status:</b> {r['status']}<br>
<b>Penanggung Jawab:</b> {r['pic']}<br>
<b>Terakhir Update:</b> {r['updated_at']}<br>
""", unsafe_allow_html=True)
                    # Download file jika ada, but check for missing keys
                    file_blob = r['file_blob'] if 'file_blob' in r.keys() else None
                    file_name = r['file_name'] if 'file_name' in r.keys() else None
                    if file_blob and file_name:
                        show_file_download(file_blob, file_name)
                    st.markdown("**Catatan Finance:**")
                    note = st.text_area("Tulis catatan atau alasan jika perlu", value=r["finance_note"] or "", key=f"fin_note_{r['id']}_finance_{idx}")
                    colf1, colf2 = st.columns([1,2])
                    with colf1:
                        if st.button("🔎 Review", key=f"ap_fin_{r['id']}_finance_{idx}"):
                            cur.execute("UPDATE inventory SET finance_note=?, finance_approved=1 WHERE id=?", (note, r["id"]))
                            conn.commit()
                            try:
                                audit_log("inventory", "finance_review", target=r["id"], details=note)
                            except Exception:
                                pass
                            st.success("Finance reviewed. Menunggu persetujuan Director.")
                    with colf2:
                        st.caption("Klik Review jika sudah sesuai. Catatan akan tersimpan di database.")
        tab_contents.append(finance_tab)
    if user["role"] in ["director", "superuser"]:
        tab_labels.append("✅ Approval Director")
        def director_tab():
            st.info("Approve/Tolak item yang sudah di-approve Finance.")
            cur.execute("SELECT * FROM inventory WHERE finance_approved=1 AND director_approved=0")
            rows = cur.fetchall()
            for idx, r in enumerate(rows):
                updated_str = format_datetime_wib(r['updated_at'])
                with st.expander(f"[Menunggu Approval Director] {r['name']} ({r['id']})"):
                    st.markdown(f"""
                    <div style='background:#f8fafc;border-radius:12px;padding:1.2em 1.5em 1em 1.5em;margin-bottom:1em;'>
                        <b>Nama:</b> {r['name']}<br>
                        <b>ID:</b> {r['id']}<br>
                        <b>Lokasi:</b> {r['location']}<br>
                        <b>Status:</b> <span style='color:#2563eb;font-weight:600'>{r['status']}</span><br>
                        <b>PIC:</b> {r['pic']}<br>
                        <b>Update Terakhir:</b> {updated_str}<br>
                    </div>
                    """, unsafe_allow_html=True)
                    # Download file jika ada
                    if r['file_blob'] and r['file_name']:
                        show_file_download(r['file_blob'], r['file_name'])
                    st.markdown("<b>Catatan Director</b>", unsafe_allow_html=True)
                    note2 = st.text_area("", value=r["director_note"] or "", key=f"dir_note_{r['id']}_director_{idx}", placeholder="Tulis catatan atau alasan jika perlu...", height=80)
                    colA, colB = st.columns([1,1])
                    with colA:
                        if st.button("✅ Approve", key=f"ap_dir_{r['id']}_director_{idx}"):
                            cur.execute("UPDATE inventory SET director_note=?, director_approved=1 WHERE id=?", (note2, r["id"]))
                            conn.commit()
                            try:
                                audit_log("inventory", "director_approval", target=r["id"], details=f"approve=1; note={note2}")
                            except Exception:
                                pass
                            st.success("Item telah di-approve Director.")
                    with colB:
                        if st.button("❌ Tolak", key=f"reject_dir_{r['id']}_director_{idx}"):
                            cur.execute("UPDATE inventory SET director_note=?, director_approved=-1 WHERE id=?", (note2, r["id"]))
                            conn.commit()
                            try:
                                audit_log("inventory", "director_approval", target=r["id"], details=f"approve=0; note={note2}")
                            except Exception:
                                pass
        tab_contents.append(director_tab)

    # Tab Daftar Inventaris dan Pinjam Barang (selalu tambahkan labelnya)
    tab_labels.append("📦 Daftar Inventaris")
    def data_tab():
        st.subheader("Daftar Inventaris & Pinjam Barang")
        left_col, right_col = st.columns([2, 1])
        # --- Kiri: Daftar Inventaris ---
        with left_col:
            filter_col1, filter_col2, filter_col3 = st.columns([2,2,2])
            with filter_col1:
                filter_nama = st.text_input("Filter Nama Barang", "")
            with filter_col2:
                filter_lokasi = st.text_input("Filter Lokasi", "")
            with filter_col3:
                filter_status = st.selectbox("Filter Status", ["Semua", "Tersedia", "Dipinjam", "Rusak", "Dijual"], index=0)

            df = pd.read_sql_query("SELECT id, name, location, status, pic, updated_at, file_name, file_blob FROM inventory ORDER BY updated_at DESC", conn)
            if not df.empty and 'updated_at' in df.columns:
                df['updated_at'] = df['updated_at'].apply(format_datetime_wib)

            filtered_df = df.copy()
            if filter_nama:
                filtered_df = filtered_df[filtered_df['name'].str.contains(filter_nama, case=False, na=False)]
            if filter_lokasi:
                filtered_df = filtered_df[filtered_df['location'].str.contains(filter_lokasi, case=False, na=False)]
            if filter_status != "Semua":
                filtered_df = filtered_df[filtered_df['status'] == filter_status]

            if filtered_df.empty:
                st.info("Tidak ada data inventaris sesuai filter.")
            else:
                show_df = filtered_df.drop(columns=["file_blob"], errors="ignore")
                st.dataframe(show_df, use_container_width=True)

                lampiran_list = [
                    f"{row['name']} - {row['file_name']}" for idx, row in filtered_df.iterrows()
                    if row['file_blob'] is not None and row['file_name']
                ]
                lampiran_dict = {
                    f"{row['name']} - {row['file_name']}": (row['file_name'], row['file_blob'])
                    for idx, row in filtered_df.iterrows()
                    if row['file_blob'] is not None and row['file_name']
                }
                if lampiran_list:
                    selected = st.selectbox("Pilih lampiran untuk diunduh:", lampiran_list)
                    if selected:
                        file_name, file_blob = lampiran_dict[selected]
                        st.download_button(
                            label=f"⬇️ Download {file_name}",
                            data=file_blob,
                            file_name=file_name,
                            mime="application/octet-stream"
                        )
                else:
                    st.info("Tidak ada lampiran yang tersedia untuk diunduh.")

        # --- Kanan: Pinjam Barang ---
        with right_col:
            st.markdown("### 📋 Pinjam Barang")
            # Filter pinjam barang mengikuti filter kiri
            pinjam_df = filtered_df.copy()
            for idx, row in pinjam_df.iterrows():
                if row['status'] != "Dipinjam":
                    with st.expander(f"Pinjam: {row['name']} ({row['id']})"):
                        keperluan = st.text_input(f"Keperluan pinjam untuk {row['name']}", key=f"keperluan_{row['id']}")
                        tgl_kembali = st.date_input(f"Tanggal Kembali", key=f"tglkembali_{row['id']}", min_value=date.today())
                        ajukan = st.button(f"Ajukan Pinjam", key=f"ajukan_{row['id']}")
                        if ajukan:
                            # Simpan pengajuan: status tetap, approval direset, info pengajuan di pic
                            cur.execute("UPDATE inventory SET pic=?, finance_approved=0, director_approved=0, updated_at=? WHERE id=?", (f"{user['id']}|{keperluan}|{tgl_kembali}|0|0", datetime.utcnow().isoformat(), row['id']))
                            conn.commit()
                            try:
                                audit_log("inventory", "loan_request", target=row['id'], details=f"keperluan={keperluan}; kembali={tgl_kembali}")
                            except Exception:
                                pass
                            st.success("Pengajuan pinjam barang berhasil. Menunggu ACC Finance & Director.")

    # Tab Pinjam Barang terpisah (definisi dan penambahan tab di dalam scope agar variabel filter_* tersedia)


    # Update tab_labels dan tab_contents
    # Ganti label tab dengan icon/emoji yang lebih menarik
    for i, lbl in enumerate(tab_labels):
        if lbl.lower().startswith("tambah barang") or lbl.lower().startswith("➕ tambah barang"):
            tab_labels[i] = "➕ Tambah Barang"
        elif lbl.lower().startswith("review finance") or lbl.lower().startswith("💰 review finance"):
            tab_labels[i] = "💰 Review Finance"
        elif lbl.lower().startswith("approval director") or lbl.lower().startswith("✅ approval director"):
            tab_labels[i] = "✅ Approval Director"
        elif lbl.lower().startswith("daftar inventaris") or lbl.lower().startswith("📦 daftar inventaris"):
            tab_labels[i] = "📦 Daftar Inventaris"
    tab_contents[:] = [tab for tab in tab_contents if tab.__name__ != "download_tab"]
    # Pastikan urutan tab: ... , 📦 Daftar Inventaris
    # Hapus data_tab jika sudah ada agar tidak dobel
    tab_contents = [tab for tab in tab_contents if tab.__name__ != "data_tab"]
    # Tambahkan sesuai urutan label
    for lbl in tab_labels:
        if lbl == "📦 Daftar Inventaris":
            tab_contents.append(data_tab)

    # Sinkronisasi jumlah tab_labels dan tab_contents
    if len(tab_labels) > len(tab_contents):
        tab_labels = tab_labels[:len(tab_contents)]
    elif len(tab_contents) > len(tab_labels):
        tab_contents = tab_contents[:len(tab_labels)]

    # Render the tabs dan panggil fungsi sesuai urutan
    selected = st.tabs(tab_labels)
    for i, tab_func in enumerate(tab_contents):
        with selected[i]:
            tab_func()

def surat_masuk_module():
    st.header("📥 Surat Masuk")
    user = get_current_user()
    # Allow both 'staff' and 'staf' for compatibility
    allowed_roles = ["staff", "staf", "finance", "director", "superuser"]
    if not user or user["role"] not in allowed_roles:
        st.warning("Anda tidak memiliki akses untuk input Surat Masuk.")
        return

    tab1, tab2, tab3 = st.tabs([
        "📝 Input Draft Surat Masuk",
        "✅ Approval",
        "📋 Daftar & Rekap Surat Masuk"
    ])

    with tab1:
        st.markdown("### Input Draft Surat Masuk")
        with st.form("form_surat_masuk", clear_on_submit=True):
            nomor = st.text_input("Nomor Surat")
            pengirim = st.text_input("Pengirim")
            tanggal = st.date_input("Tanggal Surat", value=date.today())
            perihal = st.text_input("Perihal")
            file_upload = st.file_uploader("Upload File Surat (wajib)", type=None)
            status = st.selectbox("Status", ["Diusulkan dibahas ke rapat rutin", "Langsung dilegasikan ke salah satu user", "Selesai"], index=0)
            follow_up = st.text_area("Tindak Lanjut (Follow Up)")
            submitted = st.form_submit_button("Catat Surat Masuk")

            if submitted:
                if not file_upload:
                    st.error("File surat wajib diupload.")
                else:
                    conn = get_db()
                    cur = conn.cursor()
                    # Ensure optional columns exist
                    try:
                        cur.execute("ALTER TABLE surat_masuk ADD COLUMN director_approved INTEGER DEFAULT 0")
                    except Exception:
                        pass
                    try:
                        cur.execute("ALTER TABLE surat_masuk ADD COLUMN rekap INTEGER DEFAULT 0")
                    except Exception:
                        pass
                    try:
                        cur.execute("ALTER TABLE surat_masuk ADD COLUMN file_id TEXT")
                    except Exception:
                        pass
                    try:
                        cur.execute("ALTER TABLE surat_masuk ADD COLUMN file_url TEXT")
                    except Exception:
                        pass
                    # Upload ke Drive dan siapkan metadata
                    blob, file_name, _ = upload_file_and_store(file_upload)
                    meta = get_last_upload_meta()
                    file_id = meta.get("file_id")
                    file_url = meta.get("file_url")
                    # Simpan data ke DB (indeks otomatis di rekap)
                    sid = str(uuid.uuid4())
                    cur.execute(
                        """
                        INSERT INTO surat_masuk (id, nomor, tanggal, pengirim, perihal, file_blob, file_name, status, follow_up)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            sid,
                            nomor,
                            tanggal.isoformat(),
                            pengirim,
                            perihal,
                            blob,
                            file_name,
                            status,
                            follow_up,
                        ),
                    )
                    # Simpan metadata Drive jika ada
                    if file_id or file_url:
                        try:
                            cur.execute("UPDATE surat_masuk SET file_id=?, file_url=? WHERE id=?", (file_id, file_url, sid))
                        except Exception:
                            pass
                    conn.commit()
                    try:
                        audit_log("surat_masuk", "create", target=sid, details=f"{nomor} - {perihal} ({pengirim})")
                    except Exception:
                        pass
                    st.success("Surat masuk berhasil dicatat.")

    with tab2:
        st.markdown("### Approval Surat Masuk")
        if user["role"] in ["director", "superuser"]:
            conn = get_db()
            cur = conn.cursor()
            df = pd.read_sql_query("SELECT id, nomor, tanggal, pengirim, perihal, file_name, file_url, status, follow_up, director_approved, rekap FROM surat_masuk ORDER BY tanggal DESC", conn)
            for idx, row in df.iterrows():
                if row.get("director_approved", 0) == 0:
                    with st.expander(f"{row['nomor']} | {row['perihal']} | {row['tanggal']}"):
                        st.write(f"Pengirim: {row['pengirim']}")
                        st.write(f"Status: {row['status']}")
                        st.write(f"Follow Up: {row['follow_up']}")
                        if row.get('file_url'):
                            st.markdown(f"[⬇️ Download/Preview Surat]({row['file_url']})")
                        elif row.get('file_name'):
                            # Fallback: tampilkan tombol download dari DB/Drive by name
                            cur.execute("SELECT file_blob, file_name FROM surat_masuk WHERE id= ?", (row['id'],))
                            f = cur.fetchone()
                            show_file_download(f['file_blob'] if f else None, row.get('file_name',''))
                        colA, colB = st.columns(2)
                        with colA:
                            if st.button("Approve Surat Masuk", key=f"approve_{row['id']}"):
                                cur.execute("UPDATE surat_masuk SET director_approved=1 WHERE id= ?", (row['id'],))
                                conn.commit()
                                try:
                                    audit_log("surat_masuk", "director_approval", target=row['id'], details="approve=1")
                                except Exception:
                                    pass
                                st.success("Surat masuk di-approve Director.")
                                st.rerun()
                        with colB:
                            if st.button("Reject Surat Masuk", key=f"reject_{row['id']}"):
                                cur.execute("UPDATE surat_masuk SET director_approved=-1 WHERE id= ?", (row['id'],))
                                conn.commit()
                                try:
                                    audit_log("surat_masuk", "director_approval", target=row['id'], details="approve=0")
                                except Exception:
                                    pass
                                st.warning("Surat masuk ditolak Director.")
                                st.rerun()
                elif row.get("director_approved", 0) == 1:
                    st.success(f"Sudah di-approve Director: {row['nomor']} | {row['perihal']} | {row['tanggal']}")
                elif row.get("director_approved", 0) == -1:
                    st.error(f"Surat masuk ditolak Director: {row['nomor']} | {row['perihal']} | {row['tanggal']}")
        else:
            st.info("Hanya Director atau Superuser yang dapat meng-approve surat masuk.")
    # Tab Rekap Surat Masuk
    with tab3:
        st.markdown("### Daftar & Rekap Surat Masuk")
        conn = get_db()
        cur = conn.cursor()
        df = pd.read_sql_query("SELECT id, nomor, tanggal, pengirim, perihal, file_name, file_url, rekap, director_approved FROM surat_masuk ORDER BY tanggal DESC", conn)
        # Indeks otomatis
        if not df.empty:
            df = df.copy()
            df['indeks'] = [f"SM-{i+1:04d}" for i in range(len(df))]
            show_cols = ["indeks","nomor","tanggal","pengirim","perihal","file_name"]
        else:
            show_cols = ["nomor","tanggal","pengirim","perihal","file_name"]

        # Tabel rekap dengan tombol download di kolom, styled modern UI
        rekap_df = df[df['rekap']==1].copy() if 'rekap' in df.columns else df.copy()
        if not rekap_df.empty:
            download_links = []
            for idx, row in rekap_df.iterrows():
                if row.get('file_url'):
                    href = f"<a class='rekap-download-btn' href='{row['file_url']}' target='_blank'><span style='font-size:1.1em;'>⬇️</span> Download</a>"
                else:
                    href = '<span style="color:#bbb">-</span>'
                download_links.append(href)
            rekap_df = rekap_df.reset_index(drop=True)
            rekap_df['Download'] = download_links
            # Custom styled HTML table
            table_html = rekap_df[show_cols + ["Download"]].to_html(escape=False, index=False, classes="rekap-table")
            st.markdown('''
<style>
.rekap-table {
    border-collapse: separate;
    border-spacing: 0;
    width: 100%;
    font-size: 1.05em;
    background: #f8fbff;
    border-radius: 12px;
    overflow: hidden;
    box-shadow: 0 2px 8px rgba(80,140,255,0.07);
}
.rekap-table th {
    background: linear-gradient(90deg, #4f8cff 0%, #38c6ff 100%);
    color: #fff;
    font-weight: 700;
    text-align: center;
    padding: 10px 8px;
    border: none;
}
.rekap-table td {
    text-align: center;
    padding: 8px 6px;
    border-bottom: 1px solid #e3eaff;
    background: #fff;
    font-size: 1em;
}
.rekap-table tr:last-child td {
    border-bottom: none;
}
.rekap-download-btn {
    display: inline-block;
    background: linear-gradient(90deg, #38c6ff 0%, #4f8cff 100%);
    color: #fff !important;
    padding: 4px 16px;
    border-radius: 6px;
    font-weight: 600;
    text-decoration: none;
    box-shadow: 0 1px 4px rgba(80,140,255,0.10);
    transition: background 0.2s;
}
.rekap-download-btn:hover {
    background: linear-gradient(90deg, #4f8cff 0%, #38c6ff 100%);
    color: #fff !important;
}
</style>
''', unsafe_allow_html=True)
            st.markdown(table_html, unsafe_allow_html=True)
        else:
            st.info("Belum ada surat masuk yang direkap.")
        # Approval Director: masukan ke rekap
        if user["role"] in ["director", "superuser"]:
            for idx, row in df.iterrows():
                if row.get("rekap", 0) == 0 and row.get("director_approved", 0) == 1:
                    if st.button("Masukan ke Daftar Rekap Surat", key=f"rekap_{row['id']}"):
                        cur.execute("UPDATE surat_masuk SET rekap=1 WHERE id= ?", (row['id'],))
                        conn.commit()
                        try:
                            audit_log("surat_masuk", "rekap_add", target=row['id'])
                        except Exception:
                            pass
                        st.success("Surat masuk dimasukan ke rekap.")
                        st.rerun()

def surat_keluar_module():
    conn = get_db()
    cur = conn.cursor()
    user = require_login()

    st.markdown("# 📤 Surat Keluar")
    tab1, tab2, tab3 = st.tabs([
        "📝 Input Draft Surat Keluar",
        "✅ Approval",
        "📋 Daftar & Rekap Surat Keluar"
    ])

    # --- Tab 1: Input Draft oleh Staf ---
    with tab1:
        st.markdown("### Input Draft Surat Keluar (Staf)")
        with st.form("sk_add", clear_on_submit=True):
            col1, col2 = st.columns(2)
            with col1:
                nomor = st.text_input("Nomor Surat")
                tanggal = st.date_input("Tanggal", value=date.today())
            with col2:
                ditujukan = st.text_input("Ditujukan Kepada")
                perihal = st.text_input("Perihal")
            draft_type = st.radio("Jenis Draft Surat", ["Upload File", "Link URL"], horizontal=True)
            draft_blob, draft_name, draft_url = None, None, None
            if draft_type == "Upload File":
                draft = st.file_uploader("Upload Draft Surat (PDF/DOC)")
            else:
                draft = None
                draft_url = st.text_input("Link Draft Surat (Google Drive, dll)")
            follow_up = st.text_area("Tindak Lanjut (opsional)")
            submit = st.form_submit_button("💾 Simpan Draft Surat Keluar")
            if submit:
                if draft_type == "Upload File" and not draft:
                    st.error("File draft surat wajib diupload.")
                elif draft_type == "Link URL" and not draft_url:
                    st.error("Link draft surat wajib diisi.")
                else:
                    sid = gen_id("sk")
                    draft_file_id, draft_file_url = None, None
                    if draft_type == "Upload File":
                        draft_blob, draft_name, _ = upload_file_and_store(draft)
                        meta = get_last_upload_meta(); draft_file_id = meta.get("file_id"); draft_file_url = meta.get("file_url")
                    # Ensure columns for URLs/IDs exist
                    try:
                        cur.execute("ALTER TABLE surat_keluar ADD COLUMN draft_id TEXT")
                    except Exception:
                        pass
                    try:
                        cur.execute("ALTER TABLE surat_keluar ADD COLUMN draft_url TEXT")
                    except Exception:
                        pass
                    cur.execute("""INSERT INTO surat_keluar (id,indeks,nomor,tanggal,ditujukan,perihal,pengirim,draft_blob,draft_name,status,follow_up, draft_url, draft_id)
                                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (sid, '', nomor, tanggal.isoformat(), ditujukan, perihal, user['full_name'], draft_blob, draft_name, "Draft", follow_up, draft_url or draft_file_url, draft_file_id))
                    conn.commit()
                    try:
                        det = f"draft_file={draft_name}" if draft_name else f"draft_url={draft_url}"
                        audit_log("surat_keluar", "create", target=sid, details=f"{nomor}-{perihal}; {det}")
                    except Exception:
                        pass
                    st.success("✅ Surat keluar (draft) tersimpan.")

    # --- Tab 2: Approval Director ---
    with tab2:
        st.markdown("### Approval Surat Keluar (Director)")
        if user["role"] in ["director","superuser"]:
            df = pd.read_sql_query("SELECT id,indeks,nomor,tanggal,ditujukan,perihal,pengirim,status,follow_up, director_approved, final_name, draft_blob, draft_name, draft_url, final_blob, final_url FROM surat_keluar ORDER BY tanggal DESC", conn)
            for idx, row in df.iterrows():
                with st.expander(f"{row['nomor']} | {row['perihal']} | {row['tanggal']} | Status: {row['status']}"):
                    st.write(f"Ditujukan: {row['ditujukan']}")
                    st.write(f"Pengirim: {row['pengirim']}")
                    st.write(f"Follow Up: {row['follow_up']}")
                    # Preview/download draft
                    if row['draft_blob'] and row['draft_name']:
                        st.markdown(f"**Draft Surat (file):** {row['draft_name']}")
                        show_file_download(row['draft_blob'], row['draft_name'], row.get('draft_url',''))
                    elif row.get('draft_url'):
                        st.markdown(f"**Draft Surat (link):** [Lihat Draft]({row['draft_url']})")
                    # Catatan dan upload final
                    note = st.text_area("Catatan Director", value="", key=f"note_{row['id']}")
                    final = st.file_uploader("Upload File Final (wajib untuk status resmi)", key=f"final_{row['id']}")
                    colA, colB = st.columns(2)
                    with colA:
                        approve = st.button("✅ Approve & Upload Final", key=f"approve_{row['id']}")
                    with colB:
                        disapprove = st.button("❌ Disapprove (Revisi ke Draft)", key=f"disapprove_{row['id']}")
                    if approve:
                        if not final:
                            st.error("File final wajib diupload agar surat keluar tercatat resmi.")
                        else:
                            blob, fname, _ = upload_file_and_store(final)
                            meta = get_last_upload_meta(); fid = meta.get("file_id"); furl = meta.get("file_url")
                            # Ensure columns
                            try:
                                cur.execute("ALTER TABLE surat_keluar ADD COLUMN final_id TEXT")
                            except Exception:
                                pass
                            try:
                                cur.execute("ALTER TABLE surat_keluar ADD COLUMN final_url TEXT")
                            except Exception:
                                pass
                            cur.execute("UPDATE surat_keluar SET final_blob=?, final_name=?, final_id=?, final_url=?, director_note=?, director_approved=1, status='Final' WHERE id=?",
                                        (blob, fname, fid, furl, note, row['id']))
                            conn.commit()
                            try:
                                audit_log("surat_keluar", "director_approval", target=row['id'], details=f"final={fname}; note={note}")
                            except Exception:
                                pass
                            st.success("Final uploaded & approved.")
                            st.rerun()
                    if disapprove:
                        cur.execute("UPDATE surat_keluar SET status='Draft', director_note=?, director_approved=0 WHERE id=?", (note, row['id']))
                        conn.commit()
                        try:
                            audit_log("surat_keluar", "director_disapprove", target=row['id'], details=f"note={note}")
                        except Exception:
                            pass
                        st.warning("Surat dikembalikan ke draft untuk direvisi.")
                        st.rerun()
        else:
            st.info("Hanya Director yang dapat meng-approve dan upload file final.")

    # --- Tab 3: Daftar & Rekap Surat Keluar ---
    with tab3:
        st.markdown("### Daftar & Rekap Surat Keluar")
        df = pd.read_sql_query("SELECT id,indeks,nomor,tanggal,ditujukan,perihal,pengirim,status,follow_up, director_approved, final_name, draft_name, draft_url, final_blob, final_url FROM surat_keluar ORDER BY tanggal DESC", conn)
        # Indeks otomatis: urutan
        if not df.empty:
            df = df.copy()
            df['indeks'] = [f"SK-{i+1:04d}" for i in range(len(df))]
        # Kolom file final dapat diunduh
        def file_final_link(row):
            if row.get('final_url'):
                st.markdown(f"<a href='{row['final_url']}' target='_blank'>Download {row.get('final_name','')}</a>", unsafe_allow_html=True)
            elif row['final_blob'] and row['final_name']:
                show_file_download(row['final_blob'], row['final_name'])
        st.dataframe(df[["indeks","nomor","tanggal","ditujukan","perihal","pengirim","status","follow_up","final_name"]], use_container_width=True, hide_index=True)
        st.markdown("#### Download File Final Surat Keluar")
        for idx, row in df.iterrows():
            if row.get('final_url') or (row['final_blob'] and row['final_name']):
                st.write(f"{row['nomor']} | {row['perihal']} | {row['tanggal']}")
                show_file_download(row.get('final_blob'), row.get('final_name',''), row.get('final_url',''))
        # Rekap Bulanan
        st.markdown("#### 📊 Rekap Bulanan Surat Keluar")
        this_month = date.today().strftime("%Y-%m")
        df_month = pd.DataFrame()
        if not df.empty:
            df_month = df[df['tanggal'].str[:7] == this_month]
    st.write(f"Total surat keluar bulan ini: **{len(df_month)}**")
    if not df_month.empty:
            approved = df_month[df_month['director_approved']==1]
            draft = df_month[df_month['status'].str.lower() == 'draft']
            percent_final = (len(approved)/len(df_month))*100 if len(df_month) > 0 else 0
            st.info(f"Approved: {len(approved)} | Masih Draft: {len(draft)} | % Finalisasi: {percent_final:.1f}%")
            # Perbaiki export Excel
            import io
            excel_buffer = io.BytesIO()
            df_month.to_excel(excel_buffer, index=False, engine='openpyxl')
            excel_buffer.seek(0)
            st.download_button("⬇️ Download Rekap Bulanan (Excel)", excel_buffer, file_name=f"rekap_suratkeluar_{this_month}.xlsx")
            st.download_button("⬇️ Download Rekap Bulanan (CSV)", df_month.to_csv(index=False), file_name=f"rekap_suratkeluar_{this_month}.csv")

def mou_module():
    user = require_login()
    st.header("🤝 MoU")
    conn = get_db()
    cur = conn.cursor()
    tab1, tab2, tab4 = st.tabs([
        "📝 Input Draft MoU",
        "👥 Review Board",
        "📋 Daftar & Rekap MoU"
    ])

    # --- Tab 1: Input Draft MoU ---
    with tab1:
        st.markdown("### Input Draft MoU (Staf)")
        jenis_options = [
            "Programmatic MoU",
            "Funding MoU / Grant Agreement",
            "Strategic Partnership MoU",
            "Capacity Building MoU",
            "Secondment MoU",
            "Internship/Volunteer MoU",
            "Advocacy MoU",
            "Operational MoU",
            "Research & Development MoU",
            "MoU Advokasi Kebijakan Publik",
            "MoU Operasional",
        ]
        with st.form("mou_add", clear_on_submit=True):
            nomor = st.text_input("Nomor MoU")
            nama = st.text_input("Nama MoU")
            pihak = st.text_input("Pihak Terlibat")
            jenis = st.selectbox("Jenis MoU", jenis_options)
            tgl_mulai = st.date_input("Tgl Mulai", value=date.today())
            tgl_selesai = st.date_input("Tgl Selesai", value=date.today()+timedelta(days=365))
            f = st.file_uploader("File Draft MoU (wajib)")
            submit = st.form_submit_button("Simpan Draft MoU")
            if submit:
                if not f:
                    st.error("File draft MoU wajib diupload.")
                elif tgl_selesai < tgl_mulai:
                    st.error("Tanggal selesai tidak boleh sebelum tanggal mulai.")
                else:
                    mid = gen_id("mou")
                    blob, fname, _ = upload_file_and_store(f)
                    meta = get_last_upload_meta()
                    # ensure Drive metadata columns exist
                    try:
                        cur.execute("ALTER TABLE mou ADD COLUMN file_id TEXT")
                    except Exception:
                        pass
                    try:
                        cur.execute("ALTER TABLE mou ADD COLUMN file_url TEXT")
                    except Exception:
                        pass
                    try:
                        cur.execute("ALTER TABLE mou ADD COLUMN final_id TEXT")
                    except Exception:
                        pass
                    try:
                        cur.execute("ALTER TABLE mou ADD COLUMN final_url TEXT")
                    except Exception:
                        pass
                    cur.execute("""INSERT INTO mou (id,nomor,nama,pihak,jenis,tgl_mulai,tgl_selesai,file_blob,file_name,file_id,file_url,board_note,board_approved,final_blob,final_name,final_id,final_url)
                                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (mid, nomor, nama, pihak, jenis, tgl_mulai.isoformat(), tgl_selesai.isoformat(), blob, fname, meta.get("file_id"), meta.get("file_url"), "", 0, None, None, None, None))
                    conn.commit()
                    try:
                        audit_log("mou", "create", target=mid, details=f"{nomor} - {nama} ({jenis})")
                    except Exception:
                        pass
                    st.success("MoU tersimpan (draft).")

    # --- Tab 2: Review Board (opsional) ---
    with tab2:
        st.markdown("### Review Board (Opsional)")
        if user["role"] in ["board","superuser"]:
            df = pd.read_sql_query("SELECT id, nomor, nama, pihak, jenis, tgl_mulai, tgl_selesai, board_note, board_approved FROM mou ORDER BY tgl_selesai ASC", conn)
            for idx, row in df.iterrows():
                with st.expander(f"{row['nomor']} | {row['nama']} | {row['tgl_mulai']} - {row['tgl_selesai']}"):
                    st.write(f"Pihak: {row['pihak']}")
                    st.write(f"Jenis: {row['jenis']}")
                    st.write(f"Catatan Board: {row['board_note']}")
                    note = st.text_area("Catatan Board", value=row['board_note'], key=f"board_note_{row['id']}")
                    approve = st.checkbox("Approve Board", value=bool(row['board_approved']), key=f"board_approved_{row['id']}")
                    if st.button("Simpan Review Board", key=f"save_board_{row['id']}"):
                        cur.execute("UPDATE mou SET board_note=?, board_approved=? WHERE id=?", (note, int(approve), row['id']))
                        conn.commit()
                        st.success("Review Board disimpan.")
                        conn.commit()
                        st.success("Review Board disimpan.")
                        try:
                            audit_log("mou", "board_review", target=row['id'], details=f"approve={bool(approve)}; note={note}")
                        except Exception:
                            pass
                        st.rerun()
        else:
            st.info("Hanya Board yang dapat review di sini.")



    # --- Tab 4: Daftar & Rekap MoU ---
    with tab4:
        st.markdown("### Daftar & Rekap MoU")
        df = pd.read_sql_query("SELECT id, nomor, nama, pihak, jenis, tgl_mulai, tgl_selesai, file_name, file_url, board_approved FROM mou ORDER BY tgl_selesai ASC", conn)
        # Status aktif (berdasarkan rentang tanggal)
        today = pd.to_datetime(date.today())
        if not df.empty:
            df = df.copy()
            df['status_aktif'] = [
                'Aktif' if pd.to_datetime(row['tgl_mulai']) <= today <= pd.to_datetime(row['tgl_selesai']) else 'Tidak Aktif'
                for _, row in df.iterrows()
            ]
        else:
            df['status_aktif'] = []

        # Siapkan dataframe untuk tampilan
        show_df = df[["id", "nomor", "nama", "pihak", "jenis", "tgl_mulai", "tgl_selesai", "file_name", "board_approved", "status_aktif"]].copy() if not df.empty else pd.DataFrame(columns=["id","nomor","nama","pihak","jenis","tgl_mulai","tgl_selesai","file_name","board_approved","status_aktif"])
        if not show_df.empty:
            show_df['Board Approved'] = show_df['board_approved'].map({0: '❌', 1: '✅'})
            show_df = show_df.rename(columns={
                "id": "ID",
                "nomor": "Nomor",
                "nama": "Nama",
                "pihak": "Pihak",
                "jenis": "Jenis",
                "tgl_mulai": "Tgl Mulai",
                "tgl_selesai": "Tgl Selesai",
                "file_name": "File",
                "status_aktif": "Status Aktif",
            })

        left, right = st.columns([3,2])
        with left:
            st.subheader("📋 Daftar MoU")
            cols_order = ["ID","Nomor","Nama","Pihak","Jenis","Tgl Mulai","Tgl Selesai","File","Board Approved","Status Aktif"]
            disp = show_df[cols_order] if not show_df.empty else show_df
            st.dataframe(disp, use_container_width=True, hide_index=True)

        with right:
            st.subheader("⬇️ Download File")
            if not df.empty:
                # Hanya tampilkan opsi yang memiliki file
                opt_map = {f"{r['nomor']} | {r['nama']} — {r['file_name']}": r['id'] for _, r in df.iterrows() if r.get('file_name')}
                pilihan = st.selectbox("Pilih MoU", [""] + list(opt_map.keys()))
                if pilihan:
                    mid = opt_map[pilihan]
                    row = pd.read_sql_query("SELECT file_name, file_url FROM mou WHERE id=?", conn, params=(mid,))
                    if not row.empty and row.iloc[0].get("file_url"):
                        st.markdown(f"<a href='{row.iloc[0]['file_url']}' target='_blank'>Download {row.iloc[0]['file_name']}</a>", unsafe_allow_html=True)
                    elif not row.empty and row.iloc[0].get("file_name"):
                        # Fallback: cari berdasarkan nama file di Drive
                        show_file_download(None, row.iloc[0]["file_name"])    
                    else:
                        st.info("File tidak tersedia untuk MoU terpilih.")
            else:
                st.info("Belum ada data MoU.")

        # Rekap Bulanan MoU (opsional, ringkas)
        st.markdown("#### 📅 Rekap Bulanan MoU (Otomatis)")
        this_month = date.today().strftime("%Y-%m")
        if not df.empty:
            df_month = df[(df['tgl_mulai'].astype(str).str[:7] == this_month) | (df['tgl_selesai'].astype(str).str[:7] == this_month)]
        else:
            df_month = pd.DataFrame()
        st.write(f"Total MoU terkait bulan ini: {len(df_month)}")
        if not df_month.empty:
            by_jenis = df_month['jenis'].value_counts()
            st.write("Rekap per Jenis:")
            st.dataframe(by_jenis)

def cash_advance_module():
    user = require_login()
    st.header("💸 Cash Advance")
    conn = get_db()
    cur = conn.cursor()
    tab1, tab2, tab3, tab4 = st.tabs([
        "📝 Input Staf",
        "💰 Review Finance",
        "✅ Approval Director",
        "📋 Daftar & Rekap"
    ])

    # --- Tab 1: Input Staf ---
    with tab1:
        st.markdown("### Pengajuan Cash Advance (Staf)")
        # --- Real-time total calculation ---
        if 'ca_nominals' not in st.session_state:
            st.session_state['ca_nominals'] = [0.0]*10
        items = []
        nama_program = st.text_input("Nama Program")
        import re
        def format_ribuan(val):
            if val is None or val == "":
                return ""
            val_str = str(val)
            val_str = re.sub(r'[^\d]', '', val_str)
            if not val_str:
                return ""
            return f"{int(val_str):,}".replace(",", ".")

        for i in range(1, 11):
            col1, col2 = st.columns([3,2])
            with col1:
                item = st.text_input(f"Item {i}", key=f"ca_item_{i}")
            with col2:
                key_nom = f"ca_nom_{i}"
                val = st.session_state.get(key_nom, "")
                val_disp = format_ribuan(val)
                nominal_str = st.text_input(f"Nominal {i}", value=val_disp, key=key_nom)
                # Remove non-digit and update session state
                clean_nom = re.sub(r'[^\d]', '', nominal_str)
                st.session_state['ca_nominals'][i-1] = float(clean_nom) if clean_nom else 0.0
            if item:
                items.append({"item": item, "nominal": float(st.session_state['ca_nominals'][i-1])})
        total = sum(st.session_state['ca_nominals'])
        tanggal = st.date_input("Tanggal", value=date.today())
        # Format total as Rp with thousand separator
        def format_rp(val):
            return f"Rp. {val:,.0f}".replace(",", ".")
        st.info(f"Total: {format_rp(total)}")
        if st.button("Ajukan Cash Advance"):
            if not nama_program or not items:
                st.error("Nama Program dan minimal 1 item wajib diisi.")
            else:
                cid = gen_id("ca")
                cur.execute("INSERT INTO cash_advance (id,divisi,items_json,totals,tanggal,finance_note,finance_approved,director_note,director_approved) VALUES (?,?,?,?,?,?,?,?,?)",
                            (cid, nama_program, json.dumps(items), total, tanggal.isoformat(), "", 0, "", 0))
                conn.commit()
                try:
                    audit_log("cash_advance", "create", target=cid, details=f"divisi={nama_program}; total={total}")
                except Exception:
                    pass
                st.success("Cash advance diajukan.")
                st.session_state['ca_nominals'] = [0.0]*10

    # --- Tab 2: Review Finance ---
    with tab2:
        st.markdown("### Review & Approval Finance")
        if user["role"] in ["finance", "superuser"]:
            df = pd.read_sql_query("SELECT id, divisi, items_json, totals, tanggal, finance_note, finance_approved FROM cash_advance ORDER BY tanggal DESC", conn)
            for idx, row in df.iterrows():
                with st.expander(f"{row['divisi']} | {row['tanggal']} | Total: {format_rp(row['totals'])}"):
                    items = json.loads(row['items_json']) if row['items_json'] else []
                    # Format nominal columns as Rp
                    if items:
                        df_items = pd.DataFrame(items)
                        if 'nominal' in df_items.columns:
                            df_items['nominal'] = df_items['nominal'].apply(format_rp)
                        st.write(df_items)
                    st.write(f"Catatan: {row['finance_note']}")
                    note = st.text_area("Catatan Finance", value=row['finance_note'], key=f"fin_note_{row['id']}")
                    # Opsi upload ToR jika diminta
                    tor_file = st.file_uploader("Upload File ToR (jika diminta)", key=f"tor_{row['id']}")
                    # Opsi: Ajukan ke Director atau Kembalikan ke User
                    colA, colB = st.columns(2)
                    with colA:
                        approve = st.button("Ajukan ke Director", key=f"ajukan_dir_{row['id']}")
                    with colB:
                        return_user = st.button("Kembalikan ke User", key=f"kembali_user_{row['id']}")
                    if approve:
                        # Simpan ToR jika ada
                        if tor_file:
                            tor_blob, tor_name, _ = upload_file_and_store(tor_file)
                            cur.execute("UPDATE cash_advance SET finance_note=?, finance_approved=1 WHERE id=?", (note + "\n[ToR diupload: " + tor_name + "]", row['id']))
                        else:
                            cur.execute("UPDATE cash_advance SET finance_note=?, finance_approved=1 WHERE id=?", (note, row['id']))
                        conn.commit()
                        try:
                            tor_info = f"; ToR={tor_file.name}" if tor_file else ""
                            audit_log("cash_advance", "finance_review", target=row['id'], details=f"approve=1; note={note}{tor_info}")
                        except Exception:
                            pass
                        st.success("Diajukan ke Director.")
                        st.rerun()
                    if return_user:
                        cur.execute("UPDATE cash_advance SET finance_note=?, finance_approved=0 WHERE id=?", (note + "\n[Perlu revisi oleh user]", row['id']))
                        conn.commit()
                        try:
                            audit_log("cash_advance", "finance_review", target=row['id'], details=f"approve=0; note={note}")
                        except Exception:
                            pass
                        st.warning("Dikembalikan ke user peminta.")
                        st.rerun()
        else:
            st.info("Hanya Finance yang dapat review di sini.")

    # --- Tab 3: Approval Director ---
    with tab3:
        st.markdown("### Approval Director Cash Advance")
        if user["role"] in ["director", "superuser"]:
            df = pd.read_sql_query("SELECT id, divisi, items_json, totals, tanggal, finance_approved, director_note, director_approved FROM cash_advance ORDER BY tanggal DESC", conn)
            for idx, row in df.iterrows():
                with st.expander(f"{row['divisi']} | {row['tanggal']} | Total: Rp {row['totals']:,.0f}"):
                    items = json.loads(row['items_json']) if row['items_json'] else []
                    st.write(pd.DataFrame(items))
                    st.write(f"Finance Approved: {'Ya' if row['finance_approved'] else 'Belum'}")
                    st.write(f"Catatan Director: {row['director_note']}")
                    note = st.text_area("Catatan Director", value=row['director_note'], key=f"dir_note_{row['id']}")
                    approve = st.checkbox("Approve Director", value=bool(row['director_approved']), key=f"dir_approved_{row['id']}")
                    if st.button("Simpan Approval Director", key=f"save_dir_{row['id']}"):
                        cur.execute("UPDATE cash_advance SET director_note=?, director_approved=? WHERE id=?", (note, int(approve), row['id']))
                        conn.commit()
                        try:
                            audit_log("cash_advance", "director_approval", target=row['id'], details=f"approve={bool(approve)}; note={note}")
                        except Exception:
                            pass
                        st.success("Approval Director disimpan.")
                        st.rerun()
        else:
            st.info("Hanya Director yang dapat approve di sini.")

    # --- Tab 4: Daftar & Rekap ---
    with tab4:
        st.markdown("### Daftar & Rekap Cash Advance")
        df = pd.read_sql_query("SELECT id, divisi, items_json, totals, tanggal, finance_approved, director_approved FROM cash_advance ORDER BY tanggal DESC", conn)
        df['status'] = df.apply(lambda x: 'Cair' if x['finance_approved'] and x['director_approved'] else 'Proses', axis=1)
        # --- FILTER UI ---
        with st.container():
            col_div, col_status, col_tgl = st.columns([2,2,3])
            with col_div:
                divisi_list = ["Semua"] + sorted(df["divisi"].dropna().unique().tolist())
                filter_divisi = st.selectbox("Filter Divisi", divisi_list)
            with col_status:
                status_list = ["Semua", "Cair", "Proses"]
                filter_status = st.selectbox("Status", status_list)
            with col_tgl:
                min_tgl = df["tanggal"].min() if not df.empty else date.today()
                max_tgl = df["tanggal"].max() if not df.empty else date.today()
                filter_tgl = st.date_input("Tanggal", value=(min_tgl, max_tgl) if min_tgl and max_tgl else (date.today(), date.today()))

        dff = df.copy()
        if filter_divisi != "Semua":
            dff = dff[dff["divisi"] == filter_divisi]
        if filter_status != "Semua":
            dff = dff[dff["status"] == filter_status]
        if filter_tgl and isinstance(filter_tgl, tuple) and len(filter_tgl) == 2:
            tgl_start, tgl_end = filter_tgl
            dff = dff[(pd.to_datetime(dff["tanggal"]) >= pd.to_datetime(tgl_start)) & (pd.to_datetime(dff["tanggal"]) <= pd.to_datetime(tgl_end))]

        st.dataframe(dff)
        # Rekap Bulanan
        st.markdown("#### Rekap Bulanan Cash Advance (Otomatis)")
        this_month = date.today().strftime("%Y-%m")
        df_month = dff[dff['tanggal'].str[:7] == this_month] if not dff.empty else pd.DataFrame()
        st.write(f"Jumlah pengajuan bulan ini: {len(df_month)}")
        if not df_month.empty:
            by_div = df_month.groupby('divisi').agg({'id':'count','totals':'sum'}).rename(columns={'id':'jumlah_pengajuan','totals':'total_nominal'})
            st.write("Rekap per Divisi:")
            st.dataframe(by_div)
            approved = df_month[(df_month['finance_approved']==1) & (df_month['director_approved']==1)]
            pending = df_month[(df_month['finance_approved']==0) | (df_month['director_approved']==0)]
            st.write(f"Approved (Cair): {len(approved)} | Pending: {len(pending)}")

def pmr_module():
    user = require_login()
    st.header("📑 PMR")
    conn = get_db()
    cur = conn.cursor()
    tab_upload, tab_finance, tab_director, tab_rekap = st.tabs([
        "📝 Upload Laporan Bulanan (Staf)",
        "💰 Review & Approval Finance",
        "✅ Approval Director PMR",
        "📋 Daftar & Rekap PMR"
    ])

    with tab_upload:
        st.markdown("### Upload Laporan Bulanan (Staf)")
        with st.form("pmr_add", clear_on_submit=True):
            nama = st.text_input("Nama Pegawai")
            bulan = st.selectbox("Bulan (YYYY-MM)", options=[f"{y}-{m:02d}" for y in range(date.today().year-1, date.today().year+2) for m in range(1,13)])
            f1 = st.file_uploader("File Laporan 1 (wajib)")
            f2 = st.file_uploader("File Laporan 2 (opsional)")
            duplicate = False
            if nama and bulan:
                cek = cur.execute("SELECT COUNT(*) FROM pmr WHERE nama=? AND bulan=?", (nama, bulan)).fetchone()[0]
                if cek > 0:
                    duplicate = True
            submit = st.form_submit_button("Submit")
            if submit:
                if not nama or not bulan:
                    st.error("Nama dan Bulan wajib diisi.")
                elif duplicate:
                    st.error("Sudah ada laporan bulan ini untuk nama tersebut.")
                elif not f1:
                    st.error("Minimal 1 file wajib diupload.")
                else:
                    pid = gen_id("pmr")
                    b1, n1, _ = upload_file_and_store(f1)
                    meta1 = get_last_upload_meta()
                    if f2:
                        b2, n2, _ = upload_file_and_store(f2)
                        meta2 = get_last_upload_meta()
                    else:
                        b2, n2 = None, None
                        meta2 = {}
                    now = datetime.utcnow().isoformat()
                    # Ensure Drive metadata columns
                    for col in ("file1_id","file1_url","file2_id","file2_url"):
                        try:
                            cur.execute(f"ALTER TABLE pmr ADD COLUMN {col} TEXT")
                        except Exception:
                            pass
                    cur.execute("""INSERT INTO pmr (id,nama,file1_blob,file1_name,file1_id,file1_url,file2_blob,file2_name,file2_id,file2_url,bulan,finance_note,finance_approved,director_note,director_approved,tanggal_submit)
                                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                                (pid, nama, b1, n1, meta1.get("file_id"), meta1.get("file_url"), b2, n2, meta2.get("file_id"), meta2.get("file_url"), bulan, "", 0, "", 0, now))
                    conn.commit()
                    try:
                        audit_log("pmr", "upload", target=pid, details=f"{nama} {bulan}; file1={n1}; file2={n2 or '-'}")
                    except Exception:
                        pass
                    st.success("Laporan bulanan berhasil diupload.")

    with tab_finance:
        st.markdown("### Review & Approval Finance")
        st.caption("Finance melakukan review, memberi catatan, dan approval. Hanya Finance/Superuser yang dapat mengakses.")
        if user["role"] in ["finance", "superuser"]:
            df_fin = pd.read_sql_query("SELECT id, nama, bulan, file1_name, file2_name, finance_note, finance_approved FROM pmr ORDER BY tanggal_submit DESC", conn)
            for idx, row in df_fin.iterrows():
                with st.expander(f"{row['nama']} | {row['bulan']}"):
                    st.write(f"File 1: {row['file1_name']}")
                    if row['file2_name']:
                        st.write(f"File 2: {row['file2_name']}")
                    st.write(f"Catatan: {row['finance_note']}")
                    note = st.text_area("Catatan Finance", value=row['finance_note'], key=f"fin_note_{row['id']}")
                    colA, colB = st.columns(2)
                    with colA:
                        ajukan = st.button("Ajukan ke Director", key=f"ajukan_dir_{row['id']}")
                    with colB:
                        kembalikan = st.button("Kembalikan ke User", key=f"kembali_user_{row['id']}")
                    if ajukan:
                        cur.execute("UPDATE pmr SET finance_note=?, finance_approved=1 WHERE id=?", (note, row['id']))
                        conn.commit()
                        try:
                            audit_log("pmr", "finance_review", target=row['id'], details=f"approve=1; note={note}")
                        except Exception:
                            pass
                        st.success("Diajukan ke Director.")
                        st.rerun()
                    if kembalikan:
                        cur.execute("UPDATE pmr SET finance_note=?, finance_approved=0 WHERE id=?", (note+"\n[Perlu revisi oleh user]", row['id']))
                        conn.commit()
                        try:
                            audit_log("pmr", "finance_review", target=row['id'], details=f"approve=0; note={note}")
                        except Exception:
                            pass
                        st.warning("Dikembalikan ke user peminta.")
                        st.rerun()
        else:
            st.info("Hanya Finance yang dapat review di sini.")

    with tab_director:
        st.markdown("### Approval Director PMR")
        st.caption("Director melakukan approval akhir dan memberi catatan. Hanya Director/Superuser yang dapat mengakses.")
        if user["role"] in ["director", "superuser"]:
            df_dir = pd.read_sql_query("SELECT id, nama, bulan, file1_name, file2_name, director_note, director_approved, finance_approved FROM pmr ORDER BY tanggal_submit DESC", conn)
            for idx, row in df_dir.iterrows():
                with st.expander(f"{row['nama']} | {row['bulan']}"):
                    st.write(f"File 1: {row['file1_name']}")
                    if row['file2_name']:
                        st.write(f"File 2: {row['file2_name']}")
                    st.write(f"Finance Approved: {'Ya' if row['finance_approved'] else 'Belum'}")
                    st.write(f"Catatan: {row['director_note']}")
                    note = st.text_area("Catatan Director", value=row['director_note'], key=f"dir_note_{row['id']}")
                    approve = st.checkbox("Approve Director", value=bool(row['director_approved']), key=f"dir_approved_{row['id']}")
                    if st.button("Simpan Approval Director", key=f"save_dir_{row['id']}"):
                        cur.execute("UPDATE pmr SET director_note=?, director_approved=? WHERE id=?", (note, int(approve), row['id']))
                        conn.commit()
                        try:
                            audit_log("pmr", "director_approval", target=row['id'], details=f"approve={bool(approve)}; note={note}")
                        except Exception:
                            pass
                        st.success("Approval Director disimpan.")
                        st.rerun()
        else:
            st.info("Hanya Director yang dapat approve di sini.")

    with tab_rekap:
        if user["role"] in ["finance", "director", "superuser"]:
            st.markdown("### Daftar & Rekap PMR")
            df = pd.read_sql_query("SELECT id, nama, bulan, tanggal_submit, finance_approved, director_approved, file1_name, file1_url, file2_name, file2_url FROM pmr ORDER BY tanggal_submit DESC", conn)
            bulan_list = sorted(df['bulan'].unique(), reverse=True) if not df.empty else []
            filter_bulan = st.selectbox("Pilih Bulan", bulan_list, index=0 if bulan_list else None, key="rekap_bulan")
            if filter_bulan:
                df_month = df[df['bulan'] == filter_bulan]
            else:
                df_month = pd.DataFrame()
            st.write(f"Total laporan bulan ini: {len(df_month)}")
            if not df_month.empty:
                df_month_disp = df_month.copy()
                df_month_disp['Status'] = df_month_disp.apply(lambda x: 'Approved' if x['finance_approved'] and x['director_approved'] else ('Proses Finance' if not x['finance_approved'] else 'Proses Director'), axis=1)
                def make_download_link(row, idx):
                    name_col = f"file{idx}_name"; url_col = f"file{idx}_url"
                    name = row.get(name_col)
                    url = row.get(url_col)
                    if name and url:
                        return f"<a href='{url}' target='_blank'>{name}</a>"
                    elif name:
                        # fallback: try by filename in Drive
                        return f"<a href='{_drive_find_file_by_name(name).get('webViewLink','')}' target='_blank'>{name}</a>" if _drive_find_file_by_name(name).get('webViewLink') else name
                    return "-"
                df_month_disp['Download File 1'] = df_month_disp.apply(lambda r: make_download_link(r, 1), axis=1)
                df_month_disp['Download File 2'] = df_month_disp.apply(lambda r: make_download_link(r, 2), axis=1)
                show_cols = ["nama","bulan","tanggal_submit","Status","Download File 1","Download File 2"]
                st.markdown(df_month_disp[show_cols].to_html(escape=False, index=False), unsafe_allow_html=True)
        else:
            st.info("Hanya Finance/Director yang dapat melihat rekap PMR.")

 

def delegasi_module():
    user = require_login()
    st.header("🗂️ Delegasi Tugas & Monitoring")
    st.markdown("<div style='color:#2563eb;font-size:1.1rem;margin-bottom:1.2em'>Alur: Pemberi tugas membuat → PIC update status/upload bukti → Director monitor → Sinkron kalender & peringatan tenggat.</div>", unsafe_allow_html=True)
    conn = get_db()
    cur = conn.cursor()
    tab1, tab2, tab3, tab4 = st.tabs(["🆕 Buat Tugas", "📝 Update Status/Bukti", "👀 Monitoring Director", "📅 Rekap & Filter"])

    # Tab 1: Buat Tugas
    with tab1:
        st.markdown("### 🆕 Buat Tugas Baru (Pemberi Tugas)")
        with st.form("del_add"):
            judul = st.text_input("Judul Tugas")
            deskripsi = st.text_area("Deskripsi")
            pic = st.text_input("Penanggung Jawab (PIC)")
            tgl_mulai = st.date_input("Tgl Mulai", value=date.today())
            tgl_selesai = st.date_input("Tgl Selesai", value=date.today())
            if st.form_submit_button("Buat Tugas"):
                if not (judul and deskripsi and pic):
                    st.warning("Semua field wajib diisi.")
                elif tgl_selesai < tgl_mulai:
                    st.warning("Tanggal selesai tidak boleh sebelum mulai.")
                else:
                    did = gen_id("del")
                    now = datetime.utcnow().isoformat()
                    cur.execute("INSERT INTO delegasi (id,judul,deskripsi,pic,tgl_mulai,tgl_selesai,status,tanggal_update) VALUES (?,?,?,?,?,?,?,?)",
                        (did, judul, deskripsi, pic, tgl_mulai.isoformat(), tgl_selesai.isoformat(), "Belum Selesai", now))
                    conn.commit()
                    try:
                        audit_log("delegasi", "create", target=did, details=f"{judul} -> {pic} {tgl_mulai}..{tgl_selesai}")
                    except Exception:
                        pass
                    st.success("Tugas berhasil dibuat.")

    # Tab 2: Update Status & Upload Bukti (PIC)
    with tab2:
        st.markdown("### 📝 PIC Update Status & Upload Bukti")
        tugas_pic = pd.read_sql_query("SELECT * FROM delegasi WHERE pic=? ORDER BY tgl_selesai ASC", conn, params=(user["full_name"],))
        filter_status = st.selectbox("Filter Status", ["Semua", "Belum Selesai", "Proses", "Selesai"], key="filter_status_pic")
        if filter_status != "Semua":
            tugas_pic = tugas_pic[tugas_pic["status"] == filter_status]
        for idx, row in tugas_pic.iterrows():
            with st.expander(f"{row['judul']} | {row['tgl_mulai']} s/d {row['tgl_selesai']} | Status: {row['status']}"):
                st.write(f"Deskripsi: {row['deskripsi']}")
                st.write(f"Tenggat: {row['tgl_mulai']} s/d {row['tgl_selesai']}")
                status = st.selectbox("Status", ["Belum Selesai", "Proses", "Selesai"], index=["Belum Selesai", "Proses", "Selesai"].index(row["status"]), key=f"status_{row['id']}")
                file_bukti = st.file_uploader("Upload Bukti (wajib jika selesai)", key=f"bukti_{row['id']}")
                if st.button("Update Status", key=f"update_{row['id']}"):
                    if status == "Selesai" and not file_bukti:
                        st.error("Status 'Selesai' wajib upload file dokumentasi!")
                    else:
                        blob, fname, _ = upload_file_and_store(file_bukti) if file_bukti else (None, None, None)
                        meta = get_last_upload_meta() if file_bukti else {}
                        # Ensure columns for Drive metadata
                        for col in ("file_id","file_url"):
                            try:
                                cur.execute(f"ALTER TABLE delegasi ADD COLUMN {col} TEXT")
                            except Exception:
                                pass
                        now = datetime.utcnow().isoformat()
                        if status == "Selesai":
                            cur.execute("UPDATE delegasi SET status=?, file_blob=?, file_name=?, file_id=?, file_url=?, tanggal_update=? WHERE id=?",
                                (status, blob, fname, meta.get("file_id"), meta.get("file_url"), now, row["id"]))
                        else:
                            cur.execute("UPDATE delegasi SET status=?, tanggal_update=? WHERE id=?",
                                (status, now, row["id"]))
                        conn.commit()
                        try:
                            det = f"status={status}" + (f"; bukti={fname}" if fname else "")
                            audit_log("delegasi", "update", target=row['id'], details=det)
                        except Exception:
                            pass
                        st.success("Status tugas diperbarui.")

    # Tab 3: Monitoring Director
    with tab3:
        st.markdown("### 👀 Monitoring Director")
        if user["role"] in ["director", "superuser"]:
            df_all = pd.read_sql_query("SELECT id,judul,deskripsi,pic,tgl_mulai,tgl_selesai,status,file_name,file_url,tanggal_update FROM delegasi ORDER BY tgl_selesai ASC", conn)
            filter_status = st.selectbox("Filter Status", ["Semua", "Belum Selesai", "Proses", "Selesai"], key="filter_status_dir")
            if filter_status != "Semua":
                df_all = df_all[df_all["status"] == filter_status]
            st.dataframe(df_all)
            for idx, row in df_all.iterrows():
                with st.expander(f"{row['judul']} | {row['pic']} | Status: {row['status']}"):
                    st.write(f"Deskripsi: {row['deskripsi']}")
                    st.write(f"Tenggat: {row['tgl_mulai']} s/d {row['tgl_selesai']}")
                    st.write(f"Update terakhir: {row['tanggal_update']}")
                    if row["status"] == "Selesai" and row["file_name"]:
                        if row.get("file_url"):
                            st.markdown(f"<a href='{row['file_url']}' target='_blank'>Download {row['file_name']}</a>", unsafe_allow_html=True)
                        else:
                            show_file_download(row.get("file_blob"), row["file_name"])
                    st.write(f"Status: {row['status']}")

    # Tab 4: Rekap Bulanan & Statistik
    with tab4:
        st.markdown("### 📅 Rekap Bulanan Delegasi & Filter")
        df = pd.read_sql_query("SELECT id,judul,pic,tgl_mulai,tgl_selesai,status,tanggal_update FROM delegasi ORDER BY tgl_selesai ASC", conn)
        filter_status = st.selectbox("Filter Status", ["Semua", "Belum Selesai", "Proses", "Selesai"], key="filter_status_rekap")
        if filter_status != "Semua":
            df = df[df["status"] == filter_status]
        this_month = date.today().strftime("%Y-%m")
        df_month = df[df['tgl_mulai'].str[:7] == this_month] if not df.empty else pd.DataFrame()
        if not df_month.empty:
            st.download_button("Download Rekap Bulanan (Excel)", df_month.to_excel(index=False, engine='openpyxl'), file_name=f"rekap_delegasi_{this_month}.xlsx")
            st.download_button("Download Rekap Bulanan (CSV)", df_month.to_csv(index=False), file_name=f"rekap_delegasi_{this_month}.csv")
            by_pic = df_month['pic'].value_counts().head(5)
            st.write("Top 5 PIC:")
            st.dataframe(by_pic)
            status_count = df_month['status'].value_counts().to_dict()
            st.write("Status:", status_count)
        st.write(f"Total tugas bulan ini: {len(df_month)}")
        # Preview warna tenggat
        st.subheader("⏰ Status Tenggat (preview warna)")
        rows = pd.read_sql_query("SELECT id,judul,pic,tgl_mulai,tgl_selesai,status FROM delegasi", conn)
        def color_for_deadline(end_str):
            end = datetime.fromisoformat(end_str).date()
            today = date.today()
            d = (end - today).days
            if d < 0:
                return "Merah"
            if d <= 3:
                return "Oranye"
            if d <= 7:
                return "Kuning"
            return "Hijau"
        rows["color"] = rows["tgl_selesai"].apply(color_for_deadline)
        st.dataframe(rows)
def flex_module():
    user = require_login()
    st.header("⏰ Flex Time")
    conn = get_db()
    cur = conn.cursor()
    # Cek kolom tabel flex
    cur.execute("PRAGMA table_info(flex)")
    flex_cols = [row[1] for row in cur.fetchall()]
    required_cols = ["alasan","catatan_finance","approval_finance","catatan_director","approval_director"]
    missing = [c for c in required_cols if c not in flex_cols]
    if missing:
        st.error(f"Struktur tabel flex belum sesuai. Kolom berikut belum ada: {', '.join(missing)}.\n\nSilakan backup data, drop tabel flex, lalu jalankan ulang aplikasi agar tabel otomatis dibuat ulang.")
        return
    tabs = st.tabs([
        "📝 Input Staf",
        "💰 Review Finance",
        "✅ Approval Director",
        "📋 Daftar Flex"
    ])

    # --- Tab 1: Input Staf ---
    with tabs[0]:
        st.subheader(":bust_in_silhouette: Ajukan Flex Time")
        with st.form("flex_add_form"):
            nama = st.text_input("Nama")
            tanggal = st.date_input("Tanggal")
            jam_mulai = st.time_input("Jam Mulai")
            jam_selesai = st.time_input("Jam Selesai")
            alasan = st.text_area("Alasan")
            submit = st.form_submit_button("Ajukan")
            if submit:
                # Validasi jam tidak overlap
                q = "SELECT * FROM flex WHERE tanggal=? AND ((jam_mulai <= ? AND jam_selesai > ?) OR (jam_mulai < ? AND jam_selesai >= ?) OR (jam_mulai >= ? AND jam_selesai <= ?)) AND approval_director=1"
                params = (tanggal.isoformat(), jam_mulai.isoformat(), jam_mulai.isoformat(), jam_selesai.isoformat(), jam_selesai.isoformat(), jam_mulai.isoformat(), jam_selesai.isoformat())
                overlap = pd.read_sql_query(q, conn, params=params)
                if not nama or not alasan:
                    st.warning("Nama dan alasan wajib diisi.")
                elif jam_mulai >= jam_selesai:
                    st.warning("Jam selesai harus setelah jam mulai.")
                elif not overlap.empty:
                    st.error("Jam flex time bentrok/overlap dengan pengajuan lain yang sudah disetujui.")
                else:
                    fid = gen_id("flex")
                    cur.execute("INSERT INTO flex (id, nama, tanggal, jam_mulai, jam_selesai, alasan, catatan_finance, approval_finance, catatan_director, approval_director) VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (fid, nama, tanggal.isoformat(), jam_mulai.isoformat(), jam_selesai.isoformat(), alasan, '', 0, '', 0))
                    conn.commit()
                    try:
                        audit_log("flex", "create", target=fid, details=f"{nama} {tanggal} {jam_mulai}-{jam_selesai}; alasan={alasan}")
                    except Exception:
                        pass
                    st.success("Flex time diajukan.")

    # --- Tab 2: Review Finance ---
    with tabs[1]:
        st.subheader(":money_with_wings: Review Finance")
        df_fin = pd.read_sql_query("SELECT * FROM flex WHERE approval_finance=0 ORDER BY tanggal DESC", conn)
        if df_fin.empty:
            st.info("Tidak ada pengajuan flex time yang perlu direview.")
        else:
            for idx, row in df_fin.iterrows():
                with st.expander(f"{row['nama']} | {row['tanggal']} | {row['jam_mulai']} - {row['jam_selesai']}"):
                    st.write(f"Alasan: {row['alasan']}")
                    catatan = st.text_area("Catatan Finance", value=row['catatan_finance'] or "", key=f"catatan_fin_{row['id']}")
                    approve = st.button("Approve", key=f"approve_fin_{row['id']}")
                    reject = st.button("Tolak", key=f"reject_fin_{row['id']}")
                    if approve or reject:
                        cur.execute("UPDATE flex SET catatan_finance=?, approval_finance=? WHERE id=?", (catatan, 1 if approve else -1, row['id']))
                        conn.commit()
                        try:
                            audit_log("flex", "finance_review", target=row['id'], details=f"approve={1 if approve else 0}; note={catatan}")
                        except Exception:
                            pass
                        st.success("Status review finance diperbarui.")
                        st.experimental_rerun()

    # --- Tab 3: Approval Director ---
    with tabs[2]:
        st.subheader("👨‍💼 Approval Director")
        df_dir = pd.read_sql_query("SELECT * FROM flex WHERE approval_finance=1 AND approval_director=0 ORDER BY tanggal DESC", conn)
        if df_dir.empty:
            st.info("Tidak ada pengajuan flex time yang menunggu approval director.")
        else:
            for idx, row in df_dir.iterrows():
                with st.expander(f"{row['nama']} | {row['tanggal']} | {row['jam_mulai']} - {row['jam_selesai']}"):
                    st.write(f"Alasan: {row['alasan']}")
                    st.write(f"Catatan Finance: {row['catatan_finance']}")
                    catatan = st.text_area("Catatan Director", value=row['catatan_director'] or "", key=f"catatan_dir_{row['id']}")
                    approve = st.button("Approve", key=f"approve_dir_{row['id']}")
                    reject = st.button("Tolak", key=f"reject_dir_{row['id']}")
                    if approve or reject:
                        cur.execute("UPDATE flex SET catatan_director=?, approval_director=? WHERE id=?", (catatan, 1 if approve else -1, row['id']))
                        conn.commit()
                        try:
                            audit_log("flex", "director_approval", target=row['id'], details=f"approve={1 if approve else 0}; note={catatan}")
                        except Exception:
                            pass
                        st.success("Status approval director diperbarui.")
                        st.experimental_rerun()

    # --- Tab 4: Daftar Flex ---
    with tabs[3]:
        st.subheader(":clipboard: Daftar Flex Time")
        df = pd.read_sql_query("SELECT * FROM flex ORDER BY tanggal DESC, jam_mulai ASC", conn)
        if df.empty:
            st.info("Belum ada data flex time.")
        else:
            df['status'] = df.apply(lambda r: '✅ Disetujui' if r['approval_director']==1 else ('❌ Ditolak' if r['approval_finance']==-1 or r['approval_director']==-1 else ('🕒 Proses')), axis=1)
            df['jam_mulai'] = df['jam_mulai'].str[:5]
            df['jam_selesai'] = df['jam_selesai'].str[:5]
            st.dataframe(df[['nama','tanggal','jam_mulai','jam_selesai','alasan','catatan_finance','catatan_director','status']], use_container_width=True)
        # Rekap Bulanan
        st.markdown("#### Rekap Bulanan Flex Time (Otomatis)")
        this_month = date.today().strftime("%Y-%m")
        df_month = df[df['tanggal'].str[:7] == this_month] if not df.empty else pd.DataFrame()
        st.write(f"Total pengajuan flex bulan ini: {len(df_month)}")
        if not df_month.empty:
            by_pegawai = df_month.groupby('nama').agg({'id':'count'}).rename(columns={'id':'jumlah_pengajuan'})
            st.write("Rekap per Pegawai:")
            st.dataframe(by_pegawai)

# Modul Mobil Kantor
def kalender_pemakaian_mobil_kantor():
    user = require_login()
    st.header("🚗 Kalender & Booking Mobil Kantor")
    st.markdown("<div style='color:#2563eb;font-size:1.1rem;margin-bottom:1.2em'>Input/edit/hapus hanya oleh Finance, view oleh semua user, cek bentrok jadwal, sinkron ke Kalender Bersama.</div>", unsafe_allow_html=True)
    conn = get_db()
    cur = conn.cursor()
    tab1, tab2, tab3 = st.tabs(["📝 Input/Edit/Hapus (Finance)", "📋 Daftar Booking & Filter", "📅 Rekap Bulanan & Bentrok"])

    # Tab 1: Input/Edit/Hapus (Finance)
    with tab1:
        st.markdown("### 📝 Input/Edit/Hapus Jadwal Mobil (Finance)")
        if user["role"] in ["finance", "superuser"]:
            with st.form("form_mobil"):  
                nama_pengguna = st.text_input("Nama")
                divisi = st.text_input("Divisi")
                tgl_mulai = st.date_input("Tgl Mulai", value=date.today())
                tgl_selesai = st.date_input("Tgl Selesai", value=date.today())
                tujuan = st.text_input("Tujuan")
                kendaraan = st.text_input("Kendaraan")
                driver = st.text_input("Driver")
                status = st.selectbox("Status", ["Menunggu Approve", "Disetujui", "Ditolak"])
                finance_note = st.text_area("Catatan")
                submitted = st.form_submit_button("Simpan Jadwal Mobil")
                if submitted:
                    mid = gen_id("mobil")
                    cur.execute("""
                        INSERT INTO mobil (id, nama_pengguna, divisi, tgl_mulai, tgl_selesai, tujuan, kendaraan, driver, status, finance_note)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        mid, nama_pengguna, divisi, tgl_mulai.isoformat(), tgl_selesai.isoformat(), tujuan, kendaraan, driver, status, finance_note
                    ))
                    conn.commit()
                    try:
                        audit_log("mobil", "create", target=mid, details=f"{kendaraan} {tgl_mulai}..{tgl_selesai} {tujuan}")
                    except Exception:
                        pass
                    st.success("Jadwal mobil berhasil disimpan.")
            # Edit/hapus jadwal
            df_edit = pd.read_sql_query("SELECT * FROM mobil ORDER BY tgl_mulai ASC", conn)
            st.markdown("#### Edit/Hapus Jadwal Mobil")
            for idx, row in df_edit.iterrows():
                with st.expander(f"{row['nama_pengguna']} | {row['tgl_mulai']} s/d {row['tgl_selesai']} | {row['kendaraan']} | Status: {row['status']}"):
                    st.write(f"Tujuan: {row['tujuan']}, Driver: {row['driver']}, Catatan: {row['finance_note']}")
                    if st.button("Hapus Jadwal", key=f"hapus_mobil_{row['id']}"):
                        cur.execute("DELETE FROM mobil WHERE id=?", (row["id"],))
                        conn.commit()
                        try:
                            audit_log("mobil", "delete", target=row['id'], details=f"kendaraan={row['kendaraan']}; tujuan={row['tujuan']}")
                        except Exception:
                            pass
                        st.success("Jadwal dihapus.")
                        st.experimental_rerun()

    # Tab 2: Daftar Booking & Filter (semua user)
    with tab2:
        st.markdown("### 📋 Daftar Booking Mobil & Filter")
        df = pd.read_sql_query("SELECT id,nama_pengguna,divisi,tgl_mulai,tgl_selesai,tujuan,kendaraan,driver,status FROM mobil ORDER BY tgl_mulai ASC", conn)
        filter_status = st.selectbox("Filter Status", ["Semua", "Menunggu Approve", "Disetujui", "Ditolak"], key="filter_status_mobil")
        filter_kendaraan = st.text_input("Filter Kendaraan (opsional)", "", key="filter_kendaraan_mobil")
        if filter_status != "Semua":
            df = df[df["status"] == filter_status]
        if filter_kendaraan:
            df = df[df["kendaraan"].str.contains(filter_kendaraan, case=False, na=False)]
        st.dataframe(df)

    # Tab 3: Rekap Bulanan & Bentrok
    with tab3:
        st.markdown("### 📅 Rekap Bulanan Mobil Kantor & Cek Bentrok")
        df = pd.read_sql_query("SELECT * FROM mobil ORDER BY tgl_mulai ASC", conn)
        this_month = date.today().strftime("%Y-%m")
        df_month = df[df['tgl_mulai'].str[:7] == this_month] if not df.empty else pd.DataFrame()
        st.write(f"Total booking bulan ini: {len(df_month)}")
        if not df_month.empty:
            by_kendaraan = df_month['kendaraan'].value_counts()
            st.write("Top Kendaraan Dipakai:")
            st.dataframe(by_kendaraan)
        # Cek bentrok jadwal mobil kantor (kendaraan sama, tanggal overlap)
        st.markdown("#### 🚨 Cek Bentrok Jadwal Mobil Kantor")
        if not df.empty:
            df_sorted = df.sort_values(["kendaraan", "tgl_mulai"])
            overlaps = []
            for kendaraan, group in df_sorted.groupby("kendaraan"):
                prev_end = None
                for idx, row in group.iterrows():
                    start = pd.to_datetime(row["tgl_mulai"])
                    end = pd.to_datetime(row["tgl_selesai"])
                    if prev_end and start <= prev_end:
                        overlaps.append((kendaraan, row["tgl_mulai"], row["tgl_selesai"]))
                    prev_end = max(prev_end, end) if prev_end else end
            if overlaps:
                st.warning(f"Terdapat overlap jadwal Mobil Kantor untuk kendaraan yang sama: {overlaps}")
            else:
                st.success("Tidak ada bentrok jadwal mobil kantor bulan ini.")

def calendar_module():

    user = require_login()
    st.header("📅 Kalender Bersama (Auto Integrasi)")
    conn = get_db()
    cur = conn.cursor()

    tab1, tab2 = st.tabs(["➕ Tambah Libur Nasional", "📆 Kalender & Rekap"])

    # Tab 1: Tambah Libur Nasional (khusus Director/Superuser)
    with tab1:
        st.subheader("Tambah Libur Nasional (Director)")
        if user["role"] in ["director", "superuser"]:
            with st.form("add_libur_nasional"):
                judul = st.text_input("Judul Libur Nasional")
                tgl_mulai = st.date_input("Tgl Mulai")
                tgl_selesai = st.date_input("Tgl Selesai")
                sumber = st.text_input("Sumber / Dasar Penetapan (opsional)")
                if st.form_submit_button("Tambah Libur Nasional"):
                    cid = gen_id("cal")
                    now = datetime.utcnow().isoformat()
                    cur.execute("INSERT INTO calendar (id,jenis,judul,nama_divisi,tgl_mulai,tgl_selesai,deskripsi,file_blob,file_name,is_holiday,sumber,ditetapkan_oleh,tanggal_penetapan) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (cid, "Libur Nasional", judul, "-", tgl_mulai.isoformat(), tgl_selesai.isoformat(), sumber, None, None, 1, sumber, user["full_name"], now))
                    cur.execute("INSERT INTO public_holidays (tahun,tanggal,nama,keterangan,ditetapkan_oleh,tanggal_penetapan) VALUES (?,?,?,?,?,?)",
                        (tgl_mulai.year, tgl_mulai.isoformat(), judul, sumber or "", user["full_name"], now))
                    conn.commit()
                    try:
                        audit_log("calendar", "add_holiday", target=cid, details=f"{judul} {tgl_mulai}..{tgl_selesai}")
                    except Exception:
                        pass
                    st.success("Libur Nasional ditambahkan.")
        else:
            st.info("Hanya Director yang bisa menambah Libur Nasional.")

    # Tab 2: Kalender Gabungan & Rekap
    with tab2:
        # --- AUTO INTEGRASI EVENT ---
        df_cuti = pd.read_sql_query("SELECT nama as judul, 'Cuti' as jenis, nama as nama_divisi, tgl_mulai, tgl_selesai FROM cuti WHERE director_approved=1", conn)
        df_flex = pd.read_sql_query("SELECT nama as judul, 'Flex Time' as jenis, nama as nama_divisi, tanggal as tgl_mulai, tanggal as tgl_selesai FROM flex WHERE director_approved=1", conn)
        df_delegasi = pd.read_sql_query("SELECT judul, 'Delegasi' as jenis, pic as nama_divisi, tgl_mulai, tgl_selesai FROM delegasi", conn)
        df_rapat = pd.read_sql_query("SELECT judul, jenis, nama_divisi, tgl_mulai, tgl_selesai FROM calendar WHERE jenis='Rapat'", conn)
        df_mobil = pd.read_sql_query("SELECT tujuan as judul, 'Mobil Kantor' as jenis, kendaraan as nama_divisi, tgl_mulai, tgl_selesai, kendaraan FROM mobil WHERE status='Disetujui'", conn)
        df_libur = pd.read_sql_query("SELECT judul, jenis, nama_divisi, tgl_mulai, tgl_selesai FROM calendar WHERE is_holiday=1", conn)

        # Gabungkan semua event
        df_all = pd.concat([
            df_cuti,
            df_flex,
            df_delegasi,
            df_rapat,
            df_mobil.drop(columns=["kendaraan"], errors="ignore"),
            df_libur
        ], ignore_index=True)

        # Cek overlap mobil kantor (tidak boleh overlap untuk kendaraan yang sama)
        if not df_mobil.empty:
            df_mobil_sorted = df_mobil.sort_values(["kendaraan", "tgl_mulai"])
            overlaps = []
            for kendaraan, group in df_mobil_sorted.groupby("kendaraan"):
                prev_end = None
                for idx, row in group.iterrows():
                    start = pd.to_datetime(row["tgl_mulai"])
                    end = pd.to_datetime(row["tgl_selesai"])
                    if prev_end and start <= prev_end:
                        overlaps.append((kendaraan, row["tgl_mulai"], row["tgl_selesai"]))
                    prev_end = max(prev_end, end) if prev_end else end
            if overlaps:
                st.warning(f"Terdapat overlap jadwal Mobil Kantor untuk kendaraan yang sama: {overlaps}")

        # -------------------------
        # Filter UI
        # -------------------------
        st.subheader("🔎 Filter Kalender")
        if not df_all.empty:
            # Siapkan kolom bantu untuk filter tanggal overlap
            df_all = df_all.copy()
            df_all["tgl_mulai_dt"] = pd.to_datetime(df_all["tgl_mulai"], errors="coerce")
            df_all["tgl_selesai_dt"] = pd.to_datetime(df_all["tgl_selesai"], errors="coerce")

            min_date = df_all["tgl_mulai_dt"].min()
            max_date = df_all["tgl_selesai_dt"].max()
            # Default rentang: bulan ini jika ada, fallback ke min/max
            today = date.today()
            month_start = today.replace(day=1)
            next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
            month_end = next_month - timedelta(days=1)
            default_start = month_start if pd.notna(min_date) else today
            default_end = month_end if pd.notna(max_date) else today

            col_a, col_b = st.columns([2, 2])
            with col_a:
                jenis_options = sorted([x for x in df_all["jenis"].dropna().unique().tolist()])
                jenis_selected = st.multiselect("Jenis Event", jenis_options, default=jenis_options)
            with col_b:
                date_range = st.date_input("Rentang Tanggal (overlap)", value=(default_start, default_end))

            col_c, col_d = st.columns([2, 2])
            with col_c:
                filter_div = st.text_input("Filter Divisi (nama_divisi)", "")
            with col_d:
                filter_judul = st.text_input("Cari Judul", "")

            # Terapkan filter
            dff = df_all[(df_all["jenis"].isin(jenis_selected))] if jenis_selected else df_all.copy()
            if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
                start_d, end_d = date_range
                if start_d and end_d:
                    # Event overlap jika mulai <= end_d dan selesai >= start_d
                    dff = dff[(dff["tgl_mulai_dt"] <= pd.to_datetime(end_d)) & (dff["tgl_selesai_dt"] >= pd.to_datetime(start_d))]
            if filter_div:
                dff = dff[dff["nama_divisi"].astype(str).str.contains(filter_div, case=False, na=False)]
            if filter_judul:
                dff = dff[dff["judul"].astype(str).str.contains(filter_judul, case=False, na=False)]

            # Tampilkan hasil
            st.subheader("📆 Tampilkan Kalender (Gabungan) — Hasil Filter")
            if not dff.empty:
                dff = dff.sort_values("tgl_mulai")
                show_cols = ["judul", "jenis", "nama_divisi", "tgl_mulai", "tgl_selesai"]
                st.dataframe(dff[show_cols], use_container_width=True)
                # Download CSV hasil filter
                csv_bytes = dff[show_cols].to_csv(index=False).encode("utf-8")
                st.download_button("⬇️ Download CSV (Hasil Filter)", data=csv_bytes, file_name=f"kalender_gabungan_filtered_{today.isoformat()}.csv", mime="text/csv")
            else:
                st.info("Tidak ada event sesuai filter.")

            # Rekap Bulanan Kalender (berdasarkan hasil filter)
            st.markdown("#### 📅 Rekap Bulanan Kalender (Otomatis) — Berdasar Filter")
            this_month = date.today().strftime("%Y-%m")
            df_month = dff[dff['tgl_mulai'].astype(str).str[:7] == this_month] if not dff.empty else pd.DataFrame()
            st.write(f"Total event bulan ini: {len(df_month)}")
            if not df_month.empty:
                by_jenis = df_month['jenis'].value_counts()
                st.write("Rekap per Jenis Event:")
                st.dataframe(by_jenis)
        else:
            st.info("Belum ada event pada kalender.")

 

def notulen_module():
    user = require_login()
    st.header("🗒️ Notulen Rapat Rutin")
    conn = get_db()
    cur = conn.cursor()
    # Introspeksi awal kolom dan tanggal utama
    cur.execute("PRAGMA table_info(notulen)")
    nt_cols = [row[1] for row in cur.fetchall()]
    nt_date_col = "tanggal_rapat" if "tanggal_rapat" in nt_cols else ("tanggal_upload" if "tanggal_upload" in nt_cols else None)

    tab_upload, tab_list, tab_rekap = st.tabs(["🆕 Upload Notulen", "📋 Daftar Notulen", "📅 Rekap Bulanan Notulen"])

    # --- Tab 1: Upload ---
    with tab_upload:
        st.subheader("🆕 Upload Notulen (staf upload, Director approve final)")
        with st.form("not_add", clear_on_submit=True):
            judul = st.text_input("Judul Rapat")
            if nt_date_col == "tanggal_rapat":
                tgl = st.date_input("Tanggal Rapat", value=date.today())
            else:
                tgl = None
                st.caption("Tanggal upload akan dicatat otomatis.")
            f = st.file_uploader("File Notulen (PDF/DOC/IMG)")
            follow_up = st.text_area("Catatan Follow Up (opsional)") if "follow_up" in nt_cols else None
            deadline = st.date_input("Deadline / Tindak Lanjut", value=date.today()) if "deadline" in nt_cols else None
            submit = st.form_submit_button("💾 Upload Notulen")
            if submit:
                if not judul or not f:
                    st.warning("Judul dan file wajib diisi.")
                else:
                    nid = gen_id("not")
                    blob, fname, _ = upload_file_and_store(f)
                    meta = get_last_upload_meta()
                    # ensure columns for Drive metadata
                    try:
                        cur.execute("ALTER TABLE notulen ADD COLUMN file_id TEXT")
                    except Exception:
                        pass
                    try:
                        cur.execute("ALTER TABLE notulen ADD COLUMN file_url TEXT")
                    except Exception:
                        pass
                    cols = ["id", "judul", "file_blob", "file_name", "file_id", "file_url"]
                    vals = [nid, judul, blob, fname, meta.get("file_id"), meta.get("file_url")]
                    if nt_date_col == "tanggal_rapat":
                        cols.append("tanggal_rapat"); vals.append(tgl.isoformat())
                    elif nt_date_col == "tanggal_upload":
                        cols.append("tanggal_upload"); vals.append(datetime.utcnow().isoformat())
                    if "follow_up" in nt_cols:
                        cols.append("follow_up"); vals.append(follow_up or "")
                    if "deadline" in nt_cols and deadline:
                        cols.append("deadline"); vals.append(deadline.isoformat())
                    if "uploaded_by" in nt_cols:
                        cols.append("uploaded_by"); vals.append(user.get("full_name") or user.get("email"))
                    if "director_note" in nt_cols:
                        cols.append("director_note"); vals.append("")
                    if "director_approved" in nt_cols:
                        cols.append("director_approved"); vals.append(0)
                    placeholders = ", ".join(["?" for _ in cols])
                    cur.execute(f"INSERT INTO notulen ({', '.join(cols)}) VALUES ({placeholders})", vals)
                    conn.commit()
                    try:
                        audit_log("notulen", "upload", target=nid, details=f"{judul} {tgl or ''}; file={fname}")
                    except Exception:
                        pass
                    st.success("Notulen berhasil diupload. Menunggu approval Director.")

    # --- Tab 2: Daftar ---
    with tab_list:
        st.subheader("📋 Daftar Notulen")
        # Build SELECT dinamis
        cur.execute("PRAGMA table_info(notulen)")
        nt_cols = [row[1] for row in cur.fetchall()]
        nt_date_col = "tanggal_rapat" if "tanggal_rapat" in nt_cols else ("tanggal_upload" if "tanggal_upload" in nt_cols else None)
        select_cols = ["id", "judul"]
        if nt_date_col: select_cols.append(nt_date_col)
        for extra in ["uploaded_by", "deadline", "director_approved", "file_name"]:
            if extra in nt_cols:
                select_cols.append(extra)
        order_clause = f" ORDER BY {nt_date_col} DESC" if nt_date_col else " ORDER BY id DESC"
        df = pd.read_sql_query(f"SELECT {', '.join(select_cols)} FROM notulen" + order_clause, conn)

        # Filter UI
        c1, c2, c3 = st.columns([2,2,3])
        with c1:
            q = st.text_input("Cari Judul", "")
        with c2:
            status_sel = st.selectbox("Status", ["Semua","Approved","Belum"]) if "director_approved" in df.columns else "Semua"
        with c3:
            if nt_date_col and not df.empty:
                min_d = pd.to_datetime(df[nt_date_col]).min().date()
                max_d = pd.to_datetime(df[nt_date_col]).max().date()
                dr = st.date_input("Rentang Tanggal", value=(min_d, max_d))
            else:
                dr = None

        dff = df.copy()
        if q:
            dff = dff[dff["judul"].astype(str).str.contains(q, case=False, na=False)]
        if status_sel != "Semua" and "director_approved" in dff.columns:
            dff = dff[dff["director_approved"] == (1 if status_sel == "Approved" else 0)]
        if dr and isinstance(dr, (list, tuple)) and len(dr) == 2 and nt_date_col and nt_date_col in dff.columns:
            s, e = dr
            dff = dff[(pd.to_datetime(dff[nt_date_col]) >= pd.to_datetime(s)) & (pd.to_datetime(dff[nt_date_col]) <= pd.to_datetime(e))]

        if not dff.empty:
            show = dff.copy()
            if "director_approved" in show.columns:
                show["Status"] = show["director_approved"].map({1: "✅ Approved", 0: "🕒 Proses"})
            cols_show = ["judul"]
            if nt_date_col: cols_show.append(nt_date_col)
            for c in ["uploaded_by", "deadline", "file_name", "Status"]:
                if c in show.columns:
                    cols_show.append(c)
            st.dataframe(show[cols_show], use_container_width=True)

            # Download file terpilih
            if "id" in show.columns and "file_name" in show.columns:
                opsi = {f"{r['judul']} — {r.get(nt_date_col, '')}" + (f" ({r['file_name']})" if r.get('file_name') else ""): r['id'] for _, r in show.iterrows()}
                if opsi:
                    pilih = st.selectbox("Pilih notulen untuk diunduh", [""] + list(opsi.keys()))
                    if pilih:
                        nid = opsi[pilih]
                        row = pd.read_sql_query("SELECT file_blob, file_name, file_url FROM notulen WHERE id=?", conn, params=(nid,)).iloc[0]
                        show_file_download(row.get("file_blob"), row.get("file_name"), row.get("file_url"))

            # Approval Director inline
            if user["role"] in ["director", "superuser"] and "director_approved" in nt_cols:
                st.markdown("#### ✅ Approval Director (Pending)")
                pend = pd.read_sql_query(
                    f"SELECT id, judul" + (f", {nt_date_col}" if nt_date_col else "") + ", file_name, director_note FROM notulen WHERE director_approved=0" + (f" ORDER BY {nt_date_col} DESC" if nt_date_col else ""),
                    conn
                )
                if pend.empty:
                    st.info("Tidak ada notulen menunggu approval.")
                else:
                    for _, r in pend.iterrows():
                        title = f"{r['judul']}" + (f" | {r[nt_date_col]}" if nt_date_col and r.get(nt_date_col) else "")
                        with st.expander(title):
                            st.write(f"File: {r.get('file_name') or '-'}")
                            rr = pd.read_sql_query("SELECT file_blob, file_name, file_url FROM notulen WHERE id=?", conn, params=(r['id'],))
                            if not rr.empty:
                                rr0 = rr.iloc[0]
                                show_file_download(rr0.get("file_blob"), rr0.get("file_name"), rr0.get("file_url"))
                            note = st.text_area("Catatan Director (opsional)", value=r.get("director_note") or "", key=f"nt_note_{r['id']}")
                            if st.button("Approve Notulen", key=f"nt_approve_{r['id']}"):
                                if "director_note" in nt_cols:
                                    cur.execute("UPDATE notulen SET director_approved=1, director_note=? WHERE id=?", (note, r['id']))
                                else:
                                    cur.execute("UPDATE notulen SET director_approved=1 WHERE id=?", (r['id'],))
                                conn.commit()
                                try:
                                    audit_log("notulen", "director_approval", target=r['id'], details=f"note={note}")
                                except Exception:
                                    pass
                                st.success("Notulen approved.")
                                st.rerun()
        else:
            st.info("Belum ada notulen.")

    # --- Tab 3: Rekap ---
    with tab_rekap:
        st.subheader("📅 Rekap Bulanan Notulen (Otomatis)")
        cur.execute("PRAGMA table_info(notulen)")
        nt_cols = [row[1] for row in cur.fetchall()]
        nt_date_col = "tanggal_rapat" if "tanggal_rapat" in nt_cols else ("tanggal_upload" if "tanggal_upload" in nt_cols else None)
        select_cols = ["id", "judul"]
        if nt_date_col: select_cols.append(nt_date_col)
        df_all = pd.read_sql_query(f"SELECT {', '.join(select_cols)} FROM notulen", conn)
        this_month = date.today().strftime("%Y-%m")
        if not df_all.empty and (nt_date_col and nt_date_col in df_all.columns):
            df_month = df_all[df_all[nt_date_col].astype(str).str[:7] == this_month]
        else:
            df_month = pd.DataFrame()
        st.write(f"Total notulen bulan ini: {len(df_month)}")
        if not df_month.empty:
            if nt_date_col:
                st.dataframe(df_month[["judul", nt_date_col]], use_container_width=True)
            else:
                st.dataframe(df_month[["judul"]], use_container_width=True)
            try:
                st.download_button("⬇️ Download Rekap Bulanan (CSV)", df_month.to_csv(index=False).encode("utf-8"), file_name=f"rekap_notulen_{this_month}.csv")
            except Exception:
                pass

# -------------------------
# User Setting Module
# -------------------------
def user_setting_module():
    user = require_login()
    st.header("⚙️ User Setting")
    conn = get_db()
    cur = conn.cursor()

    # Ambil data user terkini dari DB
    cur.execute("SELECT id, email, full_name, role, status, last_login FROM users WHERE id = ?", (user["id"],))
    me = cur.fetchone()
    if not me:
        st.error("User tidak ditemukan di database.")
        return

        # Kartu ringkas profil (rapi & ringkas)
        email = me["email"] or "-"
        nama = me["full_name"] or "-"
        role = me["role"] or "-"
        status = me["status"] or "-"
        st.markdown(
                f"""
                <div style='background:#f8fafc;border:1px solid #e5efff;border-radius:12px;padding:12px 16px;margin-bottom:12px;'>
                    <div style='display:flex;gap:24px;flex-wrap:wrap;'>
                        <div style='min-width:260px'>
                            <div style='color:#64748b;font-size:12px;margin-bottom:4px'>Email</div>
                            <div style='font-weight:600;font-size:16px'>{email}</div>
                        </div>
                        <div style='min-width:220px'>
                            <div style='color:#64748b;font-size:12px;margin-bottom:4px'>Nama</div>
                            <div style='font-weight:600;font-size:16px'>{nama}</div>
                        </div>
                        <div style='min-width:160px'>
                            <div style='color:#64748b;font-size:12px;margin-bottom:4px'>Role</div>
                            <div style='font-weight:600;font-size:16px;text-transform:capitalize'>{role}</div>
                        </div>
                        <div style='min-width:140px'>
                            <div style='color:#64748b;font-size:12px;margin-bottom:4px'>Status</div>
                            <div style='font-weight:600;font-size:16px;text-transform:capitalize'>{status}</div>
                        </div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
        )

    tab_profile, tab_admin = st.tabs(["👤 Profil Saya", "🔐 Admin (Director)"])

    # --- Tab 1: Profil Saya ---
    with tab_profile:
        st.subheader("Ubah Profil")
        with st.form("change_name_form"):
            new_name = st.text_input("Nama Lengkap", value=me["full_name"] or "")
            save_name = st.form_submit_button("Simpan Nama")
            if save_name:
                if not new_name.strip():
                    st.warning("Nama tidak boleh kosong.")
                else:
                    cur.execute("UPDATE users SET full_name=? WHERE id=?", (new_name.strip(), me["id"]))
                    conn.commit()
                    # Update session juga
                    st.session_state["user"]["full_name"] = new_name.strip()
                    try:
                        audit_log("user_setting", "update_profile", target=me["email"], details=f"Ubah nama menjadi '{new_name.strip()}'")
                    except Exception:
                        pass
                    st.success("Nama berhasil diperbarui.")

        st.markdown("---")
        st.subheader("Ubah Password")
        with st.form("change_password_form"):
            old_pw = st.text_input("Password Lama", type="password")
            new_pw = st.text_input("Password Baru", type="password")
            new_pw2 = st.text_input("Ulangi Password Baru", type="password")
            change_pw = st.form_submit_button("Ganti Password")
            if change_pw:
                # Validasi
                if not old_pw or not new_pw or not new_pw2:
                    st.warning("Semua field password wajib diisi.")
                elif new_pw != new_pw2:
                    st.warning("Konfirmasi password baru tidak cocok.")
                elif len(new_pw) < 6:
                    st.warning("Password baru minimal 6 karakter.")
                else:
                    # Cek password lama
                    cur.execute("SELECT password_hash FROM users WHERE id=?", (me["id"],))
                    row = cur.fetchone()
                    if not row or row["password_hash"] != hash_password(old_pw):
                        st.error("Password lama tidak sesuai.")
                    else:
                        cur.execute("UPDATE users SET password_hash=? WHERE id=?", (hash_password(new_pw), me["id"]))
                        conn.commit()
                        try:
                            audit_log("user_setting", "change_password", target=me["email"], details="Ganti password sendiri")
                        except Exception:
                            pass
                        st.success("Password berhasil diganti.")

    # --- Tab 2: Admin (Director) ---
    with tab_admin:
        st.subheader("Admin User (Khusus Director/Superuser)")
        if (st.session_state.get("user", {}).get("role") not in ["director", "superuser"]):
            st.info("Hanya Director/Superuser yang dapat mengakses menu ini.")
        else:
            # Cek kolom untuk sorting
            cur.execute("PRAGMA table_info(users)")
            user_cols = {r[1] for r in cur.fetchall()}
            order_col = "created_at" if "created_at" in user_cols else "email"

            # Daftar user ringkas & filter
            with st.expander("Cari & Pilih User", expanded=True):
                keyword = st.text_input("Cari user (email/nama)", value="")
                if keyword:
                    dfu = pd.read_sql_query(
                        f"SELECT id,email,full_name,role,status,last_login FROM users WHERE email LIKE ? OR full_name LIKE ? ORDER BY {order_col} DESC",
                        conn, params=(f"%{keyword}%", f"%{keyword}%")
                    )
                else:
                    dfu = pd.read_sql_query(f"SELECT id,email,full_name,role,status,last_login FROM users ORDER BY {order_col} DESC", conn)
                st.dataframe(dfu, use_container_width=True, hide_index=True)

                # Pilih user target
                options = [f"{r['id']} | {r['email']} | {r['full_name']} | {r['role']} | {r['status']}" for _, r in dfu.iterrows()] if not dfu.empty else []
                selected = st.selectbox("Pilih user", ["-"] + options)
                target_id = None
                if selected and selected != "-":
                    target_id = selected.split(" | ")[0]

            if target_id:
                cur.execute("SELECT id,email,full_name,role,status FROM users WHERE id=?", (target_id,))
                target = cur.fetchone()
                if not target:
                    st.error("User target tidak ditemukan.")
                else:
                    st.markdown(f"**Target:** {target['email']} · {target['full_name']} · {target['role']} · {target['status']}")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown("##### Reset Password User")
                        with st.form("admin_reset_pw"):
                            npw = st.text_input("Password Baru", type="password")
                            npw2 = st.text_input("Ulangi Password Baru", type="password")
                            do_reset = st.form_submit_button("Reset Password")
                            if do_reset:
                                if len(npw) < 6:
                                    st.warning("Password minimal 6 karakter.")
                                elif npw != npw2:
                                    st.warning("Konfirmasi password tidak cocok.")
                                else:
                                    cur.execute("UPDATE users SET password_hash=? WHERE id=?", (hash_password(npw), target["id"]))
                                    conn.commit()
                                    try:
                                        audit_log("user_setting", "reset_password", target=target["email"], details=f"Oleh {me['email']}")
                                    except Exception:
                                        pass
                                    st.success("Password user berhasil direset.")
                    with col2:
                        st.markdown("##### Hapus User")
                        st.caption("Aksi permanen. Tidak dapat dibatalkan.")
                        with st.form("admin_delete_user"):
                            confirm_mail = st.text_input("Ketik email user untuk konfirmasi")
                            confirm_yes = st.checkbox("Saya yakin ingin menghapus user ini")
                            do_delete = st.form_submit_button("Hapus User", disabled=(target["id"] == me["id"]))
                            if do_delete:
                                if target["id"] == me["id"]:
                                    st.error("Tidak dapat menghapus akun sendiri.")
                                elif target["role"] == "superuser" and me["role"] != "superuser":
                                    st.error("Hanya Superuser yang boleh menghapus Superuser.")
                                elif confirm_mail.strip().lower() != (target["email"] or "").lower():
                                    st.error("Konfirmasi email tidak cocok.")
                                elif not confirm_yes:
                                    st.error("Centang konfirmasi terlebih dahulu.")
                                else:
                                    cur.execute("DELETE FROM users WHERE id=?", (target["id"],))
                                    conn.commit()
                                    try:
                                        audit_log("user_setting", "delete_user", target=target["email"], details=f"Oleh {me['email']}")
                                    except Exception:
                                        pass
                                    st.success("User berhasil dihapus. Silakan refresh daftar di atas.")

# -------------------------
# Dashboard
# -------------------------
def dashboard():
    user = require_login()
    st.title("🏠 Dashboard WIJNA")
    conn = get_db()
    cur = conn.cursor()

    # Notification Approval (count items needing approval)
    q_pending = 0
    services = [
        ("inventory","finance_approved=0 OR director_approved=0"),
        ("cash_advance","finance_approved=0 OR director_approved=0"),
        ("pmr","finance_approved=0 OR director_approved=0"),
        ("cuti","finance_approved=0 OR director_approved=0"),
        ("surat_keluar","director_approved=0"),
        ("mou","director_approved=0"),
        ("sop","director_approved=0"),
        ("notulen","director_approved=0"),
    ]
    notif_items = []
    import sqlite3
    for table, cond in services:
        try:
            cur.execute(f"SELECT COUNT(*) as c FROM {table} WHERE {cond}")
            c = cur.fetchone()["c"]
            if c > 0:
                notif_items.append((table, c))
                q_pending += c
        except sqlite3.OperationalError as e:
            st.error(f"Tabel '{table}' belum memiliki kolom yang diperlukan: {e}")
            continue

    # --- Stat Cards Section (Always Show at Top) ---
    st.markdown("""
<style>
.stat-card {
    background: #fff;
    border-radius: 16px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.07);
    padding: 1.5rem;
    transition: box-shadow 0.2s;
    margin-bottom: 0.5rem;
}
.stat-card:hover {
                box-shadow: 0 6px 18px rgba(0,0,0,0.13);
        }
        .stat-flex {
                display: flex;
                align-items: center;
                justify-content: space-between;
        }
        .stat-label {
                font-size: 1rem;
                color: #666;
                font-weight: 500;
                margin-bottom: 0.2rem;
        }
        .stat-value {
                font-size: 2.2rem;
                font-weight: bold;
                margin-bottom: 0.1rem;
        }
        .stat-delta {
                font-size: 1rem;
                color: #888;
        }
        .stat-iconbox {
                width: 48px; height: 48px;
                border-radius: 12px;
                display: flex; align-items: center; justify-content: center;
        }
        .stat-iconbox.orange { background: #fff7ed; }
        .stat-iconbox.blue { background: #e6f0fa; }
        .stat-iconbox.purple { background: #f3e8ff; }
        .stat-icon.orange { color: #fb923c; }
        .stat-icon.blue { color: #2563eb; }
        .stat-icon.purple { color: #a21caf; }
        .stat-value.orange { color: #fb923c; }
        .stat-value.blue { color: #2563eb; }
        .stat-value.purple { color: #a21caf; }
        </style>
        """, unsafe_allow_html=True)
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(f"""
        <div class=\"stat-card\"> 
            <div class=\"stat-flex\"> 
                <div> 
                    <div class=\"stat-label\">Approval Menunggu</div> 
                    <div class=\"stat-value orange\">{q_pending}</div> 
                    <div class=\"stat-delta\">Menunggu persetujuan</div> 
                </div> 
                <div class=\"stat-iconbox orange\"> 
                    <svg class=\"stat-icon orange\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\">
                                        <circle cx=\"12\" cy=\"12\" r=\"10\"/>
                                        <polyline points=\"12,6 12,12 16,14\"/>
                                    </svg>
                                </div>
                            </div>
                        </div>
        """, unsafe_allow_html=True)
    cur.execute("SELECT COUNT(*) as c FROM surat_masuk WHERE status='Belum Dibahas'")
    sm_unprocessed = cur.fetchone()["c"]
    with col2:
        st.markdown(f"""
        <div class=\"stat-card\"> 
            <div class=\"stat-flex\"> 
                <div> 
                    <div class=\"stat-label\">Surat Belum Dibahas</div> 
                    <div class=\"stat-value blue\">{sm_unprocessed}</div> 
                    <div class=\"stat-delta\">Belum diproses</div> 
                </div> 
                <div class=\"stat-iconbox blue\"> 
                    <svg class=\"stat-icon blue\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\">
                                        <rect x=\"4\" y=\"4\" width=\"16\" height=\"16\" rx=\"4\"/>
                                        <path d=\"M9 9h6v6H9z\"/>
                                    </svg>
                                </div>
                            </div>
                        </div>
        """, unsafe_allow_html=True)
    cur.execute("SELECT COUNT(*) as c FROM mou WHERE date(tgl_selesai) <= date('now','+7 day')")
    mou_due7 = cur.fetchone()["c"]
    with col3:
        st.markdown(f"""
        <div class=\"stat-card\"> 
            <div class=\"stat-flex\"> 
                <div> 
                    <div class=\"stat-label\">MoU ≤ 7 hari jatuh tempo</div> 
                    <div class=\"stat-value purple\">{mou_due7}</div> 
                    <div class=\"stat-delta\">Segera ditindaklanjuti</div> 
                </div> 
                <div class=\"stat-iconbox purple\"> 
                    <svg class=\"stat-icon purple\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\">
                                        <path d=\"M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z\"/>
                                        <polyline points=\"14,2 14,8 20,8\"/>
                                        <line x1=\"16\" y1=\"13\" x2=\"8\" y2=\"13\"/>
                                        <line x1=\"16\" y1=\"17\" x2=\"8\" y2=\"17\"/>
                                    </svg>
                                </div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)

    st.markdown("""
    <style>
    .wijna-section-card {
        background: #f8fafc;
        border-radius: 14px;
        box-shadow: 0 1px 4px rgba(0,0,0,0.04);
        padding: 1.2rem 1.5rem 1.2rem 1.5rem;
        margin-bottom: 1.2rem;
    }
    .wijna-section-title {
        font-size: 1.25rem;
        font-weight: 600;
        color: #2563eb;
        margin-bottom: 0.5rem;
    }
    .wijna-section-desc {
        color: #64748b;
        font-size: 1rem;
        margin-bottom: 0.7rem;
    }
    </style>
    """, unsafe_allow_html=True)

    # --- GRID 2x2 RINGKASAN ---
    grid1, grid2 = st.columns(2)
    with grid1:
        st.markdown('<div class="wijna-section-card">', unsafe_allow_html=True)
        st.markdown('<div class="wijna-section-title">🌴 Cuti & Flex — Ringkasan</div>', unsafe_allow_html=True)
        st.markdown('<div class="wijna-section-desc">Rekap pengajuan cuti dan total durasi per pegawai.</div>', unsafe_allow_html=True)
        df_cuti = pd.read_sql_query("SELECT nama, COUNT(*) as total_pengajuan, SUM(durasi) as total_durasi FROM cuti GROUP BY nama", conn)
        for col in df_cuti.columns:
            if 'tanggal' in col or 'tgl' in col:
                df_cuti[col] = df_cuti[col].apply(format_datetime_wib)
        st.dataframe(df_cuti, use_container_width=True, hide_index=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with grid2:
        st.markdown('<div class="wijna-section-card">', unsafe_allow_html=True)
        st.markdown('<div class="wijna-section-title">🗂️ Delegasi Tugas — Aktif</div>', unsafe_allow_html=True)
        st.markdown('<div class="wijna-section-desc">Daftar delegasi tugas yang sedang berjalan.</div>', unsafe_allow_html=True)
        df_del = pd.read_sql_query("SELECT id,judul,pic,tgl_mulai,tgl_selesai,status FROM delegasi ORDER BY tgl_selesai ASC LIMIT 10", conn)
        for col in df_del.columns:
            if 'tanggal' in col or 'tgl' in col:
                df_del[col] = df_del[col].apply(format_datetime_wib)
        st.dataframe(df_del, use_container_width=True, hide_index=True)
        st.markdown('</div>', unsafe_allow_html=True)

    grid3, grid4 = st.columns(2)
    with grid3:
        st.markdown('<div class="wijna-section-card">', unsafe_allow_html=True)
        st.markdown('<div class="wijna-section-title">📅 Kalender Bersama (30 Hari)</div>', unsafe_allow_html=True)
        st.markdown('<div class="wijna-section-desc">Event & hari libur 30 hari ke depan (cuti, flex, delegasi, rapat, mobil kantor, libur nasional).</div>', unsafe_allow_html=True)
        today = date.today()
        end_30 = today + timedelta(days=30)
        df_cuti = pd.read_sql_query("SELECT nama as judul, 'Cuti' as jenis, nama as nama_divisi, tgl_mulai, tgl_selesai FROM cuti WHERE director_approved=1", conn)
        df_flex = pd.read_sql_query("SELECT nama as judul, 'Flex Time' as jenis, nama as nama_divisi, tanggal as tgl_mulai, tanggal as tgl_selesai FROM flex WHERE director_approved=1", conn)
        df_delegasi = pd.read_sql_query("SELECT judul, 'Delegasi' as jenis, pic as nama_divisi, tgl_mulai, tgl_selesai FROM delegasi", conn)
        df_rapat = pd.read_sql_query("SELECT judul, jenis, nama_divisi, tgl_mulai, tgl_selesai FROM calendar WHERE jenis='Rapat'", conn)
        df_mobil = pd.read_sql_query("SELECT tujuan as judul, 'Mobil Kantor' as jenis, kendaraan as nama_divisi, tgl_mulai, tgl_selesai FROM mobil WHERE status='Disetujui'", conn)
        df_libur = pd.read_sql_query("SELECT judul, jenis, nama_divisi, tgl_mulai, tgl_selesai FROM calendar WHERE is_holiday=1", conn)
        df_all = pd.concat([
            df_cuti, df_flex, df_delegasi, df_rapat, df_mobil, df_libur
        ], ignore_index=True)
        if not df_all.empty:
            df_all['tgl_mulai'] = pd.to_datetime(df_all['tgl_mulai'])
            df_all['tgl_selesai'] = pd.to_datetime(df_all['tgl_selesai'])
            mask = (df_all['tgl_selesai'] >= pd.to_datetime(today)) & (df_all['tgl_mulai'] <= pd.to_datetime(end_30))
            df_30 = df_all.loc[mask].copy()
            df_30 = df_30.sort_values('tgl_mulai')
            if not df_30.empty:
                def label_color(jenis):
                    color_map = {
                        'Cuti': '#f59e42',
                        'Flex Time': '#38bdf8',
                        'Delegasi': '#a78bfa',
                        'Rapat': '#f472b6',
                        'Mobil Kantor': '#34d399',
                        'Libur Nasional': '#ef4444',
                    }
                    return f"<span style='background:{color_map.get(jenis,'#ddd')};color:#fff;padding:2px 10px;border-radius:8px;font-size:0.95em'>{jenis}</span>"
                st.markdown("<ul style='padding-left:1.2em'>", unsafe_allow_html=True)
                for _, row in df_30.iterrows():
                    jenis_lbl = label_color(row['jenis'])
                    tgl = row['tgl_mulai'].strftime('%d-%m-%Y')
                    tgl2 = row['tgl_selesai'].strftime('%d-%m-%Y')
                    tgl_str = tgl if tgl == tgl2 else f"{tgl} s/d {tgl2}"
                    st.markdown(f"<li><b>{row['judul']}</b> {jenis_lbl} <span style='color:#2563eb'>({tgl_str})</span></li>", unsafe_allow_html=True)
                st.markdown("</ul>", unsafe_allow_html=True)
            else:
                st.info("Tidak ada event 30 hari ke depan.")
        else:
            st.info("Tidak ada event 30 hari ke depan.")
        st.markdown('</div>', unsafe_allow_html=True)

    with grid4:
        st.markdown('<div class="wijna-section-card">', unsafe_allow_html=True)
        st.markdown('<div class="wijna-section-title">📊 Rekap Bulanan (Ringkasan)</div>', unsafe_allow_html=True)
        st.markdown('<div class="wijna-section-desc">Rekap otomatis tersedia di tabel <b>rekap_monthly_*</b> (trigger scheduler belum diimplementasikan).</div>', unsafe_allow_html=True)
        df_ca_rekap = pd.read_sql_query("SELECT * FROM rekap_monthly_cashadvance LIMIT 10", conn)
        for col in df_ca_rekap.columns:
            if 'tanggal' in col or 'tgl' in col or 'updated' in col:
                df_ca_rekap[col] = df_ca_rekap[col].apply(format_datetime_wib)
        st.write("Cash Advance Rekap (sample)")
        st.dataframe(df_ca_rekap, use_container_width=True, hide_index=True)
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="wijna-section-card">', unsafe_allow_html=True)

# -------------------------
# Main app flow
# -------------------------
def main():
    ensure_db()
    # Ensure a default superuser exists (once)
    try:
        ensure_default_superuser()
    except Exception:
        pass
    # Prepare Google Sheets / Drive backend if configured
    try:
        check_and_create_worksheets()
    except Exception:
        pass
    try:
        _hydrate_sqlite_from_sheets()
    except Exception:
        pass
    try:
        _initial_full_sync_to_sheets()
    except Exception:
        pass
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
        _login_logo = os.path.join(os.path.dirname(__file__), "logo.png")
        if os.path.exists(_login_logo):
            st.image(_login_logo, width=160)
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
    if os.path.exists(logo_path):
        st.sidebar.image(logo_path, use_container_width=True)
    else:
        st.sidebar.markdown("## WIJNA")
    # Backend status
    if GSHEETS_SPREADSHEET_URL:
        st.sidebar.caption("Backend: Google Sheets ✓")
    if DRIVE_FOLDER_ID:
        st.sidebar.caption(f"Files: Google Drive folder {DRIVE_FOLDER_ID}")
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
    elif choice == "PMR":
        pmr_module()
    elif choice == "Cuti":
        user = require_login()
        st.header("🌴 Pengajuan & Approval Cuti")
        st.markdown("<div style='color:#2563eb;font-size:1.1rem;margin-bottom:1.2em'>Kelola pengajuan cuti, review finance, dan approval director secara terintegrasi.</div>", unsafe_allow_html=True)
        conn = get_db()
        cur = conn.cursor()
        tab1, tab2, tab3 = st.tabs(["📝 Ajukan Cuti", "💰 Review Finance", "✅ Approval Director & Rekap"])
        # Tab 1: Ajukan Cuti
        with tab1:
            st.markdown("### 📝 Ajukan Cuti")
            nama = user["full_name"]
            tgl_mulai = st.date_input("Tanggal Mulai", value=date.today())
            tgl_selesai = st.date_input("Tanggal Selesai", value=date.today())
            alasan = st.text_area("Alasan Cuti")
            durasi = (tgl_selesai - tgl_mulai).days + 1 if tgl_selesai >= tgl_mulai else 0
            cur.execute("SELECT kuota_tahunan, cuti_terpakai FROM cuti WHERE nama=? ORDER BY tgl_mulai DESC LIMIT 1", (nama,))
            row = cur.fetchone()
            kuota_tahunan = row["kuota_tahunan"] if row else 12
            cuti_terpakai = row["cuti_terpakai"] if row else 0
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
                    cur.execute("""
                        INSERT INTO cuti (id, nama, tgl_mulai, tgl_selesai, durasi, kuota_tahunan, cuti_terpakai, sisa_kuota, status, finance_note, finance_approved, director_note, director_approved)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '', 0, '', 0)
                    """, (cid, nama, tgl_mulai.isoformat(), tgl_selesai.isoformat(), durasi, kuota_tahunan, cuti_terpakai, sisa_kuota, "Menunggu Review Finance"))
                    conn.commit()
                    st.success("Pengajuan cuti berhasil diajukan.")
                    # Audit trail
                    try:
                        audit_log("cuti", "create", target=cid, details=f"{nama} ajukan cuti {tgl_mulai} s/d {tgl_selesai} ({durasi} hari)")
                    except Exception:
                        pass
        # Tab 2: Review Finance
        with tab2:
            st.markdown("### Review & Approval Finance")
            if user["role"] in ["finance", "superuser"]:
                df = pd.read_sql_query("SELECT * FROM cuti WHERE finance_approved=0 ORDER BY tgl_mulai DESC", conn)
                for idx, row in df.iterrows():
                    with st.expander(f"{row['nama']} | {row['tgl_mulai']} s/d {row['tgl_selesai']}"):
                        st.write(f"Durasi: {row['durasi']} hari, Sisa kuota: {row['sisa_kuota']} hari")
                        st.write(f"Alasan: {row['status']}")
                        note = st.text_area("Catatan Finance", value=row["finance_note"] or "", key=f"fin_note_{row['id']}")
                        approve = st.checkbox("Approve", value=bool(row["finance_approved"]), key=f"fin_appr_{row['id']}")
                        if st.button("Simpan Review", key=f"fin_save_{row['id']}"):
                            status = "Menunggu Approval Director" if approve else "Ditolak Finance"
                            cur.execute("UPDATE cuti SET finance_note=?, finance_approved=?, status=? WHERE id=?", (note, int(approve), status, row["id"]))
                            conn.commit()
                            st.success("Review Finance disimpan.")
                            # Audit trail
                            try:
                                audit_log("cuti", "finance_review", target=row["id"], details=f"approve={bool(approve)}; status={status}")
                            except Exception:
                                pass
                            st.rerun()
            else:
                st.info("Hanya Finance/Superuser yang dapat review di sini.")
        # Tab 3: Approval Director & Rekap
        with tab3:
            st.markdown("### Approval Director & Rekap Cuti")
            if user["role"] in ["director", "superuser"]:
                df = pd.read_sql_query("SELECT * FROM cuti WHERE finance_approved=1 ORDER BY tgl_mulai DESC", conn)
                for idx, row in df.iterrows():
                    with st.expander(f"{row['nama']} | {row['tgl_mulai']} s/d {row['tgl_selesai']}"):
                        st.write(f"Durasi: {row['durasi']} hari, Sisa kuota: {row['sisa_kuota']} hari")
                        st.write(f"Alasan: {row['status']}")
                        note = st.text_area("Catatan Director", value=row["director_note"] or "", key=f"dir_note_{row['id']}")
                        approve = st.checkbox("Approve", value=bool(row["director_approved"]), key=f"dir_appr_{row['id']}")
                        if st.button("Simpan Approval", key=f"dir_save_{row['id']}"):
                            if approve:
                                cur.execute("SELECT cuti_terpakai, durasi, kuota_tahunan FROM cuti WHERE id=?", (row["id"],))
                                r = cur.fetchone()
                                baru_terpakai = (r["cuti_terpakai"] or 0) + (r["durasi"] or 0)
                                sisa = (r["kuota_tahunan"] or 12) - baru_terpakai
                                cur.execute("UPDATE cuti SET director_note=?, director_approved=?, status=?, cuti_terpakai=?, sisa_kuota=? WHERE id=?",
                                    (note, int(approve), "Disetujui Director", baru_terpakai, sisa, row["id"]))
                            else:
                                cur.execute("UPDATE cuti SET director_note=?, director_approved=?, status=? WHERE id=?",
                                    (note, int(approve), "Ditolak Director", row["id"]))
                            conn.commit()
                            st.success("Approval Director disimpan.")
                            # Audit trail
                            try:
                                audit_log("cuti", "director_approval", target=row["id"], details=f"approve={bool(approve)}")
                            except Exception:
                                pass
                            st.rerun()
            # Rekap semua pengajuan cuti
            st.markdown("#### Rekap Pengajuan Cuti")
            df = pd.read_sql_query("SELECT * FROM cuti ORDER BY tgl_mulai DESC", conn)
            st.dataframe(df, use_container_width=True, hide_index=True)
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
def audit_trail_module():
    user = require_login()
    st.header("🕵️ Audit Trail / Log Aktivitas")
    conn = get_db()
    cur = conn.cursor()
    # Filter
    st.markdown("#### Filter Audit Trail")
    # Cek kolom di file_log
    cur.execute("PRAGMA table_info(file_log)")
    cols = [row[1] for row in cur.fetchall()]
    # Build user list
    user_fields = []
    if "uploaded_by" in cols:
        user_fields.append("uploaded_by")
    if "deleted_by" in cols:
        user_fields.append("deleted_by")
    user_fields.append("file_name")
    user_union = " UNION ".join([f"SELECT DISTINCT {f} as user FROM file_log" for f in user_fields])
    users = [r[0] for r in cur.execute(user_union).fetchall() if r[0]]
    actions = ["All", "Upload", "Delete", "Login", "Other"]
    selected_user = st.selectbox("User", ["All"] + users)
    selected_action = st.selectbox("Action", actions)
    date_min = st.date_input("Dari tanggal", value=date.today() - timedelta(days=30))
    date_max = st.date_input("Sampai tanggal", value=date.today())
    # Query
    query = "SELECT * FROM file_log WHERE 1=1"
    params = []
    if selected_user != "All":
        user_cond = []
        if "uploaded_by" in cols:
            user_cond.append("uploaded_by=?")
            params.append(selected_user)
        if "deleted_by" in cols:
            user_cond.append("deleted_by=?")
            params.append(selected_user)
        user_cond.append("file_name=?")
        params.append(selected_user)
        query += " AND (" + " OR ".join(user_cond) + ")"
    if selected_action != "All":
        if selected_action == "Upload" and "uploaded_by" in cols:
            query += " AND uploaded_by IS NOT NULL AND uploaded_by != ''"
        elif selected_action == "Delete" and "deleted_by" in cols:
            query += " AND deleted_by IS NOT NULL AND deleted_by != ''"
        elif selected_action == "Login":
            query += " AND modul='auth'"
        else:
            query += " AND modul NOT IN ('auth')"
    # Tentukan kolom tanggal utama untuk filter
    date_cols = [c for c in ["tanggal_upload", "tanggal_hapus"] if c in cols]
    date_col = None
    if date_cols:
        # Gunakan COALESCE jika dua-duanya ada
        if len(date_cols) == 2:
            date_expr = f"COALESCE({date_cols[0]}, {date_cols[1]}, '')"
        else:
            date_expr = date_cols[0]
        date_col = date_expr
    else:
        # Fallback: cari kolom string lain (misal alasan, id, versi)
        fallback = [c for c in ["alasan", "id", "versi"] if c in cols]
        if fallback:
            date_col = fallback[0]
        else:
            date_col = None
    if date_col:
        query += f" AND ({date_col} >= ? AND {date_col} <= ?)"
        params += [date_min.isoformat(), (date_max + timedelta(days=1)).isoformat()]
    df = pd.read_sql_query(query, conn, params=params)
    # Tampilkan hasil
    if not df.empty:
        # Tambah kolom tanggal utama untuk sort/tampil
        if set(["tanggal_upload","tanggal_hapus"]).issubset(set(df.columns)):
            df["tanggal_utama"] = df["tanggal_upload"].fillna("")
            df.loc[df["tanggal_utama"]=="", "tanggal_utama"] = df["tanggal_hapus"].fillna("")
        elif "tanggal_upload" in df.columns:
            df["tanggal_utama"] = df["tanggal_upload"]
        elif "tanggal_hapus" in df.columns:
            df["tanggal_utama"] = df["tanggal_hapus"]
        else:
            df["tanggal_utama"] = ""
        # Sort terbaru dulu
        try:
            df = df.sort_values(by="tanggal_utama", ascending=False)
        except Exception:
            pass
        st.dataframe(df, use_container_width=True)
        # Download
        try:
            st.download_button("Download CSV", df.to_csv(index=False).encode("utf-8"), file_name="audit_trail.csv")
        except Exception:
            pass
    else:
        st.info("Belum ada log yang tercatat untuk filter yang dipilih.")
# jangan dihapus
if __name__ == "__main__":
    ensure_db()
    main()
