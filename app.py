import streamlit as st
import os
import sqlite3
from sqlite3 import Connection
import sqlite3
import pandas as pd
from datetime import datetime, date, timedelta
import io
import base64
import pandas as pd
import uuid
import json
from typing import Optional, Tuple, Dict, List
import smtplib
from email.mime.text import MIMEText
try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
    _GDRIVE_AVAILABLE = True
except Exception:
    _GDRIVE_AVAILABLE = False

# NOTE: Skema tabel akan dibuat di fungsi ensure_db() / inisialisasi terpusat.
# Blok CREATE TABLE yang sebelumnya ada di bagian atas telah dipindahkan agar tidak
# menyebabkan NameError (variabel cur belum didefinisikan) dan agar lebih terstruktur.

DB_PATH = "office_ops.db"
GDRIVE_DEFAULT_FOLDER_ID = os.environ.get("DUNYIM_GDRIVE_FOLDER_ID", "1CxYo2ZGu8jweKjmEws41nT3cexJju5_1")
SALT = "office_ops_salt_v1"

# --- Password hashing utility ---
def hash_password(password: str) -> str:
    import hashlib
    salted = (password + SALT).encode('utf-8')
    return hashlib.sha256(salted).hexdigest()

icon_path = os.path.join(os.path.dirname(__file__), "icon.png")
st.set_page_config(page_title="WIJNA Manajemen System", page_icon=icon_path, layout="wide")
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
        dt = datetime.fromisoformat(dtstr)
        # Assume stored timestamps are already WIB-naive
        return dt.strftime('%d-%m-%Y %H:%M') + ' WIB'
    except Exception:
        return dtstr

# Timezone helpers: WIB (GMT+7)
def now_wib() -> datetime:
    """Return current time in WIB (UTC+7) as a naive datetime."""
    return datetime.utcnow() + timedelta(hours=7)

def now_wib_iso() -> str:
    """ISO8601 string of WIB time (no microseconds)."""
    return now_wib().replace(microsecond=0).isoformat()

def format_date_wib(d: Optional[str]) -> str:
    """Format a date or datetime string to dd-mm-yyyy in WIB.
    Accepts ISO date (YYYY-MM-DD) or ISO datetime, returns 'dd-mm-yyyy'.
    """
    if not d:
        return ""
    try:
        if len(d) >= 10 and d[4] == '-' and d[7] == '-':
            # Looks like yyyy-mm-dd or yyyy-mm-ddTHH:MM:SS
            if 'T' in d:
                dt = datetime.fromisoformat(d.split('Z')[0].replace('Z',''))
                # Assume stored as WIB-naive
                return dt.strftime('%d-%m-%Y')
            # else:  # Removed due to missing corresponding if or code block
                pass
                y, m, dd = d[:4], d[5:7], d[8:10]
                return f"{dd}-{m}-{y}"
    except Exception:
        pass
    return str(d)

class _AuditCursor:
    def __init__(self, outer_conn, inner_cursor):
        self._outer_conn = outer_conn
        self._c = inner_cursor
    def __getattr__(self, name):
        return getattr(self._c, name)
    def execute(self, sql, params=()):
        result = self._c.execute(sql, params)
        try:
            self._maybe_log(sql, params)
        except Exception:
            pass
        return result
    def executemany(self, sql, seq_of_params):
        result = self._c.executemany(sql, seq_of_params)
        try:
            for p in seq_of_params:
                self._maybe_log(sql, p)
        except Exception:
            pass
        return result
    def _maybe_log(self, sql, params):
        # Guard or non-DML: skip
        if st.session_state.get("__audit_disabled"):
            return
        sql_l = (sql or "").strip().lower()
        op = None
        table = None
        target_id = None
        if sql_l.startswith("insert into"):
            op = "create"
            try:
                # parse table and col list
                after_into = sql_l.split("insert into",1)[1].strip()
                table = after_into.split("(",1)[0].strip().split()[0]
                if "(" in after_into:
                    cols_part = after_into.split("(",1)[1].split(")",1)[0]
                    cols = [c.strip().strip('`"') for c in cols_part.split(",")]
                    if "id" in cols:
                        idx = cols.index("id")
                        if isinstance(params, (list, tuple)) and len(params) > idx:
                            target_id = params[idx]
            except Exception:
                pass
        elif sql_l.startswith("update"):
            op = "update"
            try:
                table = sql_l.split()[1]
                if (" where " in sql_l) and (" id = ?" in sql_l or " id=?" in sql_l):
                    # assume last param is id
                    if isinstance(params, (list, tuple)) and len(params) >= 1:
                        target_id = params[-1]
            except Exception:
                pass
        elif sql_l.startswith("delete from"):
            op = "delete"
            try:
                table = sql_l.split()[2]
                # assume last param is id
                if isinstance(params, (list, tuple)) and len(params) >= 1:
                    target_id = params[-1]
            except Exception:
                pass
        # Only log DML and skip logging file_log table itself
        if op and table and table != "file_log":
            try:
                audit_log(table, op, target=str(target_id) if target_id is not None else None, details=(sql[:180] + ("..." if len(sql) > 180 else "")))
            except Exception:
                pass

class _AuditConnection:
    def __init__(self, inner_conn: sqlite3.Connection):
        self._conn = inner_conn
        self.row_factory = inner_conn.row_factory
    def __getattr__(self, name):
        return getattr(self._conn, name)
    def cursor(self, *args, **kwargs):
        return _AuditCursor(self, self._conn.cursor(*args, **kwargs))

def sop_module():
    user = require_login()
    st.header("📚 Kebijakan & SOP")
    conn = get_db()
    cur = conn.cursor()

    # Introspeksi kolom untuk fleksibilitas skema
    cur.execute("PRAGMA table_info(sop)")
    sop_cols = [row[1] for row in cur.fetchall()]
    sop_date_col = "tanggal_terbit" if "tanggal_terbit" in sop_cols else ("tanggal_upload" if "tanggal_upload" in sop_cols else None)

    tab_upload, tab_daftar, tab_approve, tab_board = st.tabs(["🆕 Upload SOP", "📋 Daftar & Rekap", "✅ Approval Director", "👥 Review Board"])

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
                    pass
                    sid = gen_id("sop")
                    blob, fname, _ = upload_file_and_store(f)
                    cols = ["id", "judul", "file_blob", "file_name"]
                    vals = [sid, judul, blob, fname]
                    if sop_date_col == "tanggal_terbit":
                        cols.append("tanggal_terbit"); vals.append(tgl.isoformat())
                    elif sop_date_col == "tanggal_upload":
                        cols.append("tanggal_upload"); vals.append(now_wib_iso())
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
                        notify_review_request("sop", title=judul, entity_id=sid, recipients_roles=("director",))
                    except Exception:
                        pass
                    st.success("SOP berhasil diupload. Menunggu approval Director.")
                _sop_title = st.text_input("Judul SOP/Kebijakan", key="sop_title_stub")
                _sop_submit = st.form_submit_button("Simpan (stub)")
                if _sop_submit:
                    st.info("Form SOP sedang disederhanakan; gunakan modul SOP pada versi lengkap untuk upload.")

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
            st.dataframe(show[cols_show], width='stretch')
            # Download file per item (opsional pilih)

        # --- Tab 4: Review Board (Opsional) ---
        with tab_board:
            if user["role"] in ["board", "superuser"]:
                try:
                    dfb = pd.read_sql_query("SELECT id, judul, tanggal_upload, board_note FROM sop ORDER BY COALESCE(tanggal_upload, id) DESC", conn)
                except Exception:
                    dfb = pd.DataFrame()
                if dfb.empty:
                    st.info("Belum ada SOP untuk direview.")
                else:
                    for _, row in dfb.iterrows():
                        st.markdown(f"**{row['judul']}**")
                        cur_note = row.get('board_note') or ""
                        note = st.text_area("Catatan Board", value=cur_note, key=f"sop_board_note_{row['id']}")
                        if st.button("Simpan Catatan", key=f"sop_board_save_{row['id']}"):
                            try:
                                cur.execute("UPDATE sop SET board_note=? WHERE id=?", (note, row['id']))
                                conn.commit()
                                st.success("Catatan Board disimpan.")
                                try:
                                    audit_log("sop", "board_review", target=row['id'], details=f"note={note}")
                                except Exception:
                                    pass
                            except Exception as e:
                                st.error(f"Gagal simpan: {e}")
            else:
                st.info("Hanya Board yang dapat review di sini.")
            if "id" in show.columns and "file_name" in show.columns:
                opsi = {f"{r['judul']} — {r.get(sop_date_col, '')} ({r['file_name'] or '-'})": r['id'] for _, r in show.iterrows()}
                if opsi:
                    pilih = st.selectbox("Unduh file SOP", [""] + list(opsi.keys()))
                    if pilih:
                        sid = opsi[pilih]
                        row = pd.read_sql_query("SELECT file_blob, file_name FROM sop WHERE id=?", conn, params=(sid,)).iloc[0]
                        if row["file_blob"] is not None and row["file_name"]:
                            st.download_button("⬇️ Download File", data=row["file_blob"], file_name=row["file_name"], mime="application/octet-stream")
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

def get_db() -> sqlite3.Connection:
    db_path = DB_PATH if os.path.isabs(DB_PATH) else os.path.join(os.path.dirname(__file__), DB_PATH)
    conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    conn.row_factory = sqlite3.Row
    # If audit is disabled (e.g., during migrations or within audit_log), return raw connection
    if st.session_state.get("__audit_disabled"):
        return conn
    return _AuditConnection(conn)
def ensure_db():
    """Ensure minimum required tables/columns exist so modules load safely.
    This lightweight bootstrap focuses on Users, Calendar, SOP, Notulen, and File Log.
    """
    try:
        conn = get_db()
        cur = conn.cursor()
        # Users
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(8)))),
                email TEXT UNIQUE NOT NULL,
                full_name TEXT,
                role TEXT,
                password_hash TEXT,
                status TEXT,
                created_at TEXT,
                last_login TEXT
            )
            """
        )
        # Seed default superuser if table empty
        cur.execute("SELECT COUNT(*) FROM users")
        count_users = cur.fetchone()[0]
        if count_users == 0:
            try:
                pw = hash_password("zzz")
                now = now_wib_iso()
                cur.execute("INSERT INTO users (email, full_name, role, password_hash, status, created_at) VALUES (?,?,?,?,?,?)",
                            ("admin", "Prime", "superuser", pw, "active", now))
                cur.execute("INSERT INTO users (email, full_name, role, password_hash, status, created_at) VALUES (?,?,?,?,?,?)",
                            ("admin2", "Finance", "Finance", pw, "active", now))
                cur.execute("INSERT INTO users (email, full_name, role, password_hash, status, created_at) VALUES (?,?,?,?,?,?)",
                            ("admin3", "director", "director", pw, "active", now))
                conn.commit()
            except Exception:
                pass
        # Calendar tables
        cur.execute(
            """
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
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public_holidays (
                tahun INTEGER,
                tanggal TEXT,
                nama TEXT,
                keterangan TEXT,
                ditetapkan_oleh TEXT,
                tanggal_penetapan TEXT
            )
            """
        )
        # SOP and Notulen (minimal compatible schemas)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sop (
                id TEXT PRIMARY KEY,
                judul TEXT,
                file_blob BLOB,
                file_name TEXT,
                tanggal_upload TEXT,
                director_approved INTEGER DEFAULT 0
            )
            """
        )
        # Ensure SOP has board_note column for Board reviewer notes
        try:
            cur.execute("PRAGMA table_info(sop)")
            sop_cols_existing = {row[1] for row in cur.fetchall()}
            if "board_note" not in sop_cols_existing:
                cur.execute("ALTER TABLE sop ADD COLUMN board_note TEXT")
        except Exception:
            pass
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS notulen (
                id TEXT PRIMARY KEY,
                judul TEXT,
                file_blob BLOB,
                file_name TEXT,
                tanggal_upload TEXT,
                uploaded_by TEXT,
                deadline TEXT,
                director_note TEXT,
                director_approved INTEGER DEFAULT 0
            )
            """
        )
        # Ensure Notulen has board_note column for Board reviewer notes
        try:
            cur.execute("PRAGMA table_info(notulen)")
            nt_cols_existing = {row[1] for row in cur.fetchall()}
            if "board_note" not in nt_cols_existing:
                cur.execute("ALTER TABLE notulen ADD COLUMN board_note TEXT")
        except Exception:
            pass
        # File Log for audit
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS file_log (
                id TEXT PRIMARY KEY,
                modul TEXT,
                file_name TEXT,
                versi INTEGER,
                deleted_by TEXT,
                tanggal_hapus TEXT,
                alasan TEXT
            )
            """
        )
        # Additional domain tables (moved from top-level into bootstrap)
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
            follow_up TEXT,
            rekap INTEGER DEFAULT 0,
            director_approved INTEGER DEFAULT 0
        )
        """)
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
        # Migration: ensure new optional column draft_url exists (for link-based drafts)
        try:
            cur.execute("PRAGMA table_info(surat_keluar)")
            sk_cols = {row[1] for row in cur.fetchall()}
            if "draft_url" not in sk_cols:
                cur.execute("ALTER TABLE surat_keluar ADD COLUMN draft_url TEXT")
        except Exception:
            pass
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
        # Migration: track requester for cash advance
        try:
            cur.execute("ALTER TABLE cash_advance ADD COLUMN requested_by TEXT")
        except Exception:
            pass
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
        # Cuti table (fix malformed DDL and ensure required columns exist)
        cur.execute(
            """
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
            """
        )
        # Migration: track creator for MoU (moved outside of Cuti DDL)
        try:
            cur.execute("ALTER TABLE mou ADD COLUMN created_by TEXT")
        except Exception:
            pass
        cur.execute("""
        CREATE TABLE IF NOT EXISTS flex (
            id TEXT PRIMARY KEY,
            nama TEXT,
            tanggal TEXT,
            jam_mulai TEXT,
            jam_selesai TEXT,
            alasan TEXT,
            catatan_finance TEXT,
            approval_finance INTEGER DEFAULT 0,
            catatan_director TEXT,
            approval_director INTEGER DEFAULT 0
        )
        """)
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
        # Ensure workflow columns for Delegasi exist
        try:
            cur.execute("PRAGMA table_info(delegasi)")
            _del_cols = {row[1] for row in cur.fetchall()}
            if "created_by" not in _del_cols:
                cur.execute("ALTER TABLE delegasi ADD COLUMN created_by TEXT")
            if "review_status" not in _del_cols:
                cur.execute("ALTER TABLE delegasi ADD COLUMN review_status TEXT")
            if "review_note" not in _del_cols:
                cur.execute("ALTER TABLE delegasi ADD COLUMN review_note TEXT")
            if "review_time" not in _del_cols:
                cur.execute("ALTER TABLE delegasi ADD COLUMN review_time TEXT")
            if "reviewed_by" not in _del_cols:
                cur.execute("ALTER TABLE delegasi ADD COLUMN reviewed_by TEXT")
        except Exception:
            pass
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
        # Inventory table (missing previously)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS inventory (
            id TEXT PRIMARY KEY,
            name TEXT,
            location TEXT,
            status TEXT,
            pic TEXT,
            updated_at TEXT,
            finance_note TEXT,
            finance_approved INTEGER DEFAULT 0,
            director_note TEXT,
            director_approved INTEGER DEFAULT 0,
            file_blob BLOB,
            file_name TEXT
        )
        """)
        # Optional requester column for inventory (when loan requests reuse pic field already, so this is optional)
        try:
            cur.execute("ALTER TABLE inventory ADD COLUMN requested_by TEXT")
        except Exception:
            pass
        # Rekap bulanan cash advance (aggregated summary), one row per bulan (YYYY-MM)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS rekap_monthly_cashadvance (
            bulan TEXT PRIMARY KEY,
            total_pengajuan INTEGER DEFAULT 0,
            total_nominal REAL DEFAULT 0,
            total_cair INTEGER DEFAULT 0,
            total_nominal_cair REAL DEFAULT 0,
            updated_at TEXT
        )
        """)
        cur.execute("PRAGMA table_info(file_log)")
        fl_cols = {row[1] for row in cur.fetchall()}
        if "uploaded_by" not in fl_cols:
            cur.execute("ALTER TABLE file_log ADD COLUMN uploaded_by TEXT")
        if "tanggal_upload" not in fl_cols:
            cur.execute("ALTER TABLE file_log ADD COLUMN tanggal_upload TEXT")
        if "action" not in fl_cols:
            cur.execute("ALTER TABLE file_log ADD COLUMN action TEXT")
        # --- Dunyim Security tables (idempotent) ---
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS backup_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name TEXT,
                drive_file_id TEXT,
                status TEXT,
                message TEXT,
                backup_time TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS record_notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                note TEXT,
                created_by TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_email TEXT,
                action TEXT,
                details TEXT,
                timestamp TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS email_notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type TEXT,
                entity_id TEXT,
                kind TEXT,
                tag TEXT,
                recipients TEXT,
                sent_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        # Seed default settings
        try:
            cur.execute("INSERT OR IGNORE INTO app_settings (key, value) VALUES ('auto_restore_enabled','true')")
            cur.execute("INSERT OR IGNORE INTO app_settings (key, value) VALUES ('scheduled_backup_enabled','false')")
            cur.execute("INSERT OR IGNORE INTO app_settings (key, value) VALUES ('enable_email_notifications','false')")
            cur.execute("INSERT OR IGNORE INTO app_settings (key, value) VALUES ('pmr_notify_enabled','true')")
            cur.execute("INSERT OR IGNORE INTO app_settings (key, value) VALUES ('delegasi_notify_enabled','true')")
            cur.execute("INSERT OR IGNORE INTO app_settings (key, value) VALUES ('delegasi_deadline_autoshift','false')")
            if GDRIVE_DEFAULT_FOLDER_ID:
                cur.execute("INSERT OR IGNORE INTO app_settings (key, value) VALUES ('gdrive_folder_id', ?)", (GDRIVE_DEFAULT_FOLDER_ID,))
        except Exception:
            pass
        conn.commit()
    except Exception:
        pass
def log_file_delete(modul, file_name, deleted_by, alasan=None):
    conn = get_db()
    cur = conn.cursor()
    log_id = gen_id("log")
    now = now_wib_iso()
    cur.execute("INSERT INTO file_log (id, modul, file_name, versi, deleted_by, tanggal_hapus, alasan) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (log_id, modul, file_name, 1, deleted_by, now, alasan or ""))
    conn.commit()
def generate_cashadvance_monthly_rekap():
    """Aggregate data from cash_advance into rekap_monthly_cashadvance for the current month.
    - bulan format: YYYY-MM
    - total_pengajuan: count rows bulan tsb
    - total_nominal: sum totals
    - total_cair: count approved (finance_approved=1 AND director_approved=1)
    - total_nominal_cair: sum totals for approved
    """
    try:
        conn = get_db()
        cur = conn.cursor()
        bulan = date.today().strftime("%Y-%m")
        # Filter baris sesuai bulan pada kolom tanggal (assuming stored as ISO date)
        cur.execute("SELECT COUNT(*), COALESCE(SUM(totals),0) FROM cash_advance WHERE substr(tanggal,1,7)=?", (bulan,))
        row_all = cur.fetchone()
        total_pengajuan = row_all[0] if row_all else 0
        total_nominal = row_all[1] if row_all else 0.0
        cur.execute("""
            SELECT COUNT(*), COALESCE(SUM(totals),0) FROM cash_advance
            WHERE substr(tanggal,1,7)=? AND finance_approved=1 AND director_approved=1
        """, (bulan,))
        row_cair = cur.fetchone()
        total_cair = row_cair[0] if row_cair else 0
        total_nominal_cair = row_cair[1] if row_cair else 0.0
        now = now_wib_iso()
        # Upsert (SQLite 3.24+ supports ON CONFLICT DO UPDATE)
        cur.execute("""
            INSERT INTO rekap_monthly_cashadvance (bulan,total_pengajuan,total_nominal,total_cair,total_nominal_cair,updated_at)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(bulan) DO UPDATE SET
              total_pengajuan=excluded.total_pengajuan,
              total_nominal=excluded.total_nominal,
              total_cair=excluded.total_cair,
              total_nominal_cair=excluded.total_nominal_cair,
              updated_at=excluded.updated_at
        """, (bulan, total_pengajuan, total_nominal, total_cair, total_nominal_cair, now))
        conn.commit()
        try:
            audit_log("cash_advance", "rekap_generate", target=bulan, details=f"pengajuan={total_pengajuan}; cair={total_cair}")
        except Exception:
            pass
    except Exception:
        pass
    
def audit_log(modul: str, action: str, target=None, details=None, actor=None):
    """Write a simplified activity record into audit_logs.
    - modul: logical module name (e.g., 'auth', 'cuti', 'delegasi')
    - action: verb (e.g., 'login', 'logout', 'create', 'update', 'delete', 'approve', 'review')
    - target: optional entity id/name
    - details: optional additional information
    - actor: user email/name; if None, inferred from session
    """
    try:
        # prevent recursive logging
        st.session_state["__audit_disabled"] = True
        db_path = DB_PATH if os.path.isabs(DB_PATH) else os.path.join(os.path.dirname(__file__), DB_PATH)
        conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        cur = conn.cursor()
        now = now_wib_iso()
        # Resolve actor from session if not provided
        if not actor:
            u = st.session_state.get("user")
            actor = (u.get("email") or u.get("full_name")) if u else "-"
        # Compose detail text
        parts = []
        if modul:
            parts.append(f"[{modul}]")
        if target:
            parts.append(str(target))
        if details:
            parts.append(str(details))
        detail_text = " ".join([p for p in parts if p]).strip() or "-"
        # Insert into simplified audit table
        cur.execute(
            """
            INSERT INTO audit_logs (user_email, action, details, timestamp)
            VALUES (?, ?, ?, ?)
            """,
            (actor or "-", action or "-", detail_text, now),
        )
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
    now = now_wib_iso()
    try:
        cur.execute("INSERT INTO users (email, full_name, role, password_hash, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (email, full_name, "staff", pw, "active", now))
        conn.commit()
        return True, "Registered — akun langsung aktif."
    except sqlite3.IntegrityError:
        return False, "Email sudah terdaftar."

def login_user(email, password):
    conn = get_db()
    cur = conn.cursor()
    now = now_wib_iso()
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
        # Commit before calling audit_log to prevent cross-connection SQLite lock
        conn.commit()
        audit_log("auth", "login", target=email, details="Login sukses.", actor=email)
        # Defer backup to after rerun to avoid blocking login button
        try:
            st.session_state["__post_login_backup"] = True
        except Exception:
            pass
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
    # Best-effort backup on logout
    try:
        if _drive_available():
            folder_id = _setting_get('gdrive_folder_id', GDRIVE_DEFAULT_FOLDER_ID) or GDRIVE_DEFAULT_FOLDER_ID
            if folder_id:
                service = _build_drive()
                _backup_db_now(service, folder_id)
    except Exception:
        pass
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
    # Superuser override: can access everything
    if user.get("role") == "superuser":
        return user
    # Normalize roles input (allow string or list)
    if isinstance(roles, str):
        roles = [roles]
    if user["role"] not in roles:
        st.error(f"Akses ditolak. Diperlukan role: {roles}")
        st.stop()
    return user

def has_role(roles) -> bool:
    """Convenience checker: returns True if current user is superuser or in roles."""
    u = get_current_user()
    if not u:
        return False
    if u.get("role") == "superuser":
        return True
    if isinstance(roles, str):
        roles = [roles]
    return u.get("role") in (roles or [])

# -------------------------
# Role hierarchy helpers
# -------------------------
# Order: staff < finance < director < superuser
ROLE_HIERARCHY = {
    "staff": 1,
    "board": 1,
    "finance": 2,
    "director": 3,
    "superuser": 4,
}

def role_rank(role: Optional[str]) -> int:
    if not role:
        return 0
    return ROLE_HIERARCHY.get(role, 0)

def has_min_role(min_role: str) -> bool:
    u = get_current_user()
    if not u:
        return False
    # superuser always passes
    if u.get("role") == "superuser":
        return True
    return role_rank(u.get("role")) >= role_rank(min_role)

def require_min_role(min_role: str):
    u = require_login()
    if u.get("role") == "superuser":
        return u
    if role_rank(u.get("role")) < role_rank(min_role):
        st.error(f"Akses ditolak. Minimal role: {min_role}")
        st.stop()
    return u

# -------------------------
# UI Components: Authentication
# -------------------------
def auth_sidebar():
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
                </div>
            """.format(
                full_name=u["full_name"],
                email=u["email"],
                role=u["role"],
                status=u["status"],
            ), unsafe_allow_html=True)
            # Peringatan penting agar user melakukan logout setelah selesai
            st.sidebar.warning("saat selesai menggunakan apps harap **logout**, menghindari potensi unsaved data saat apps autosleep")
        else:
            st.sidebar.write(f"Logged in: **{user['full_name']}** ({user['role']})")
            # Peringatan penting agar user melakukan logout setelah selesai
            st.sidebar.warning("saat selesai menggunakan apps harap **logout**, menghindari potensi unsaved data saat apps autosleep")
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
# Common helpers for modules
# -------------------------
def upload_file_and_store(file_uploader_obj):
    uploaded = file_uploader_obj
    if uploaded is None:
        return None, None, None
    raw = uploaded.read()
    blob = to_blob(raw)
    name = uploaded.name
    # Audit trail log upload
    try:
        user = get_current_user()
        conn = get_db()
        cur = conn.cursor()
        log_id = gen_id("log")
        now = now_wib_iso()
        cur.execute("INSERT INTO file_log (id, modul, file_name, versi, uploaded_by, tanggal_upload) VALUES (?, ?, ?, ?, ?, ?)",
            (log_id, "upload", name, 1, user["full_name"] if user else "-", now))
        conn.commit()
        conn.close()
    except Exception:
        pass
    return blob, name, len(raw)

def show_file_download(blob, filename):
    data = from_blob(blob)
    if data:
        b64 = base64.b64encode(data).decode()
        href = f'<a href="data:application/octet-stream;base64,{b64}" download="{filename}">Download {filename}</a>'
        st.markdown(href, unsafe_allow_html=True)

# -------------------------
# Modules Implementation (concise)
# -------------------------
# --- Dunyim helpers ---
def _setting_get(key: str, default: Optional[str] = None) -> Optional[str]:
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT value FROM app_settings WHERE key=?", (key,))
        row = cur.fetchone()
        return row[0] if row else default
    except Exception:
        return default

def _setting_set(key: str, value: str) -> None:
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("INSERT INTO app_settings (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))
        conn.commit()
    except Exception:
        pass

def _drive_available() -> bool:
    return bool(_GDRIVE_AVAILABLE)

def _build_drive():
    if not _GDRIVE_AVAILABLE:
        raise RuntimeError("Google API packages not installed.")
    try:
        creds_info = st.secrets["service_account"]
    except Exception:
        raise RuntimeError("Secrets service_account tidak tersedia.")
    scopes = ["https://www.googleapis.com/auth/drive"]
    creds = service_account.Credentials.from_service_account_info(dict(creds_info), scopes=scopes)
    return build("drive","v3", credentials=creds)

def _drive_list(service, folder_id: str):
    res = []
    token = None
    q = f"'{folder_id}' in parents and trashed=false"
    while True:
        resp = service.files().list(q=q, spaces="drive", fields="nextPageToken, files(id,name,mimeType,modifiedTime,size)", pageToken=token, supportsAllDrives=True, includeItemsFromAllDrives=True, pageSize=200).execute()
        res.extend(resp.get("files", []))
        token = resp.get("nextPageToken")
        if not token:
            break
    return res

def _drive_upload_or_replace(service, folder_id: str, name: str, data: bytes, mimetype: str = "application/octet-stream") -> Optional[str]:
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mimetype, resumable=True)
    try:
        q = f"name='{name}' and '{folder_id}' in parents and trashed=false"
        resp = service.files().list(q=q, spaces='drive', fields='files(id)', supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        existing = resp.get('files', [])
        if existing:
            fid = existing[0]['id']
            service.files().update(fileId=fid, media_body=media, supportsAllDrives=True).execute()
            return fid
        else:
            meta = {"name": name, "parents": [folder_id]}
            created = service.files().create(body=meta, media_body=media, fields='id', supportsAllDrives=True).execute()
            return created.get('id')
    except Exception:
        return None

def _drive_download(service, fid: str) -> bytes:
    req = service.files().get_media(fileId=fid)
    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = dl.next_chunk()
    buf.seek(0)
    return buf.read()

def _drive_delete(service, fid: str) -> None:
    service.files().delete(fileId=fid, supportsAllDrives=True).execute()

def _bytes_fmt(n: int) -> str:
    try:
        n = int(n)
    except Exception:
        return "-"
    units = ["B","KB","MB","GB","TB"]
    size = float(n)
    for u in units:
        if size < 1024 or u == units[-1]:
            return (f"{int(size)} {u}" if u == "B" else f"{size:.2f} {u}")
        size /= 1024

# --- Email helpers (Dunyim) ---
def _email_enabled() -> bool:
    try:
        if _setting_get('enable_email_notifications', 'false') != 'true':
            return False
        creds = st.secrets.get('email_credentials')
        if not creds:
            return False
        if not creds.get('username') or not creds.get('app_password'):
            return False
        return True
    except Exception:
        return False

def _smtp_settings() -> Tuple[Optional[str], Optional[str]]:
    try:
        creds = st.secrets.get('email_credentials')
        return creds.get('username'), creds.get('app_password')
    except Exception:
        return None, None

def _send_email(recipients: List[str], subject: str, body: str) -> bool:
    if not recipients:
        return False
    try:
        username, app_password = _smtp_settings()
        if not username or not app_password:
            return False
        msg = MIMEText(body, _charset='utf-8')
        msg['Subject'] = subject
        msg['From'] = username
        msg['To'] = ", ".join(recipients)
        import smtplib
        with smtplib.SMTP('smtp.gmail.com', 587, timeout=15) as server:
            server.ehlo()
            server.starttls()
            server.login(username, app_password)
            server.sendmail(username, recipients, msg.as_string())
        return True
    except Exception:
        return False

# --- Notification toggles helpers ---
def _bool_from_str(val: Optional[str], default: bool = True) -> bool:
    if val is None:
        return default
    v = str(val).strip().lower()
    return v in ("1", "true", "yes", "on")

def _notif_toggle_key(entity_type: str, kind: str) -> str:
    safe_entity = (entity_type or "").strip().lower().replace(" ", "_")
    safe_kind = (kind or "decision").strip().lower().replace(" ", "_")
    return f"notify_{safe_entity}_{safe_kind}_enabled"

def _notif_toggle_enabled(entity_type: str, kind: str, default: bool = True) -> bool:
    try:
        key = _notif_toggle_key(entity_type, kind)
        val = _setting_get(key)
        return _bool_from_str(val, default)
    except Exception:
        return default

def _notif_already_sent(entity_type: str, entity_id: str, kind: str, tag: str) -> bool:
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("SELECT 1 FROM email_notifications WHERE entity_type=? AND entity_id=? AND kind=? AND tag=? LIMIT 1",
                    (entity_type, entity_id, kind, tag))
        return cur.fetchone() is not None
    except Exception:
        return False

def _mark_notif_sent(entity_type: str, entity_id: str, kind: str, tag: str, recipients: List[str]) -> None:
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("INSERT INTO email_notifications (entity_type, entity_id, kind, tag, recipients) VALUES (?,?,?,?,?)",
                    (entity_type, entity_id, kind, tag, ",".join(recipients)))
        conn.commit()
    except Exception:
        pass

def _get_director_emails() -> List[str]:
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("SELECT email FROM users WHERE status='active' AND role IN ('director','superuser')")
        rows = cur.fetchall()
        return [r['email'] if isinstance(r, dict) else (r[0] if r else None) for r in rows if (r and (r['email'] if isinstance(r, dict) else r[0]))]
    except Exception:
        return []

def _get_user_email_by_name(full_name: str) -> Optional[str]:
    if not full_name:
        return None

def _get_board_emails() -> List[str]:
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("SELECT email FROM users WHERE status='active' AND role='board'")
        rows = cur.fetchall() or []
        emails: List[str] = []
        for r in rows:
            e = r['email'] if isinstance(r, dict) else (r[0] if r else None)
            if e and '@' in e:
                emails.append(str(e).strip().lower())
        return sorted(set(emails))
    except Exception:
        return []
    try:
        conn = get_db(); cur = conn.cursor()
        # case-insensitive match on full_name
        cur.execute("SELECT email FROM users WHERE lower(full_name)=lower(?) LIMIT 1", (full_name.strip(),))
        row = cur.fetchone()
        if not row:
            return None
        return row['email'] if isinstance(row, dict) else row[0]
    except Exception:
        return None

def _resolve_user_email_by_id_or_name(user_ref: Optional[str]) -> Optional[str]:
    """Resolve a user email from a stored reference: supports user id, email, or full name."""
    if not user_ref:
        return None
    try:
        ur = str(user_ref).strip()
        if '@' in ur:
            return ur
        conn = get_db(); cur = conn.cursor()
        cur.execute("SELECT email FROM users WHERE id=?", (ur,))
        row = cur.fetchone()
        if row:
            return row['email'] if isinstance(row, dict) else row[0]
        return _get_user_email_by_name(ur)
    except Exception:
        return None

# --- Notification helpers for review/approval ---
def _get_finance_emails() -> List[str]:
    try:
        conn = get_db()
        cur = conn.cursor()
        rows = cur.execute("SELECT email FROM users WHERE lower(role)='finance' AND status='active'").fetchall()
        emails = []
        for r in rows:
            if isinstance(r, dict):
                e = r.get('email')
            else:
                e = r[0] if r else None
            if e and '@' in e:
                emails.append(e)
        return sorted(set(emails))
    except Exception:
        return []

def notify_review_request(entity_type: str, title: str, entity_id: Optional[str] = None,
                          recipients_roles: Tuple[str, ...] = ("finance", "director"),
                          recipients_extra: Optional[List[str]] = None) -> None:
    """Send an immediate email notification about a new review/approval request.
    - entity_type: short module key, e.g., 'cash_advance', 'cuti', 'pmr', 'sop', 'notulen', 'surat_masuk', 'surat_keluar', 'inventory'
    - title: brief display title (e.g., judul/perihal/nama pengaju)
    - entity_id: optional id for dedup tagging
    - recipients_roles: which roles to notify ('finance', 'director')
    - recipients_extra: extra email addresses
    """
    try:
        # Check per-event toggle (request stage)
        if not _notif_toggle_enabled(entity_type, "request", True):
            return
        if not _email_enabled():
            return
        # Build recipient list by roles
        recips: List[str] = []
        roles = [r.strip().lower() for r in (recipients_roles or ())]
        if "finance" in roles:
            recips.extend(_get_finance_emails())
        if "director" in roles:
            recips.extend(_get_director_emails())
        if "board" in roles:
            recips.extend(_get_board_emails())
        if recipients_extra:
            recips.extend([e for e in recipients_extra if e and '@' in e])
        # Deduplicate
        recips = sorted({e.lower() for e in recips})
        if not recips:
            return
        # De-duplication tag
        tag = f"{entity_type}:{entity_id or title}"
        if _notif_already_sent(entity_type, entity_id or '-', 'review-request', tag):
            return
        # Compose email
        subj = f"[WIJNA] Permintaan review: {entity_type.replace('_',' ').title()} — {title}"
        ts = format_datetime_wib(now_wib_iso())
        body = (
            f"Permintaan review/approval baru untuk modul: {entity_type}.\n"
            f"Judul/Perihal: {title}\n"
            f"Waktu: {ts}\n\n"
            f"Silakan buka aplikasi WIJNA untuk meninjau dan mengambil tindakan."
        )
        if _send_email(recips, subj, body):
            _mark_notif_sent(entity_type, entity_id or '-', 'review-request', tag, recips)
    except Exception:
        # best effort only
        pass

def notify_decision(entity_type: str, title: str, decision: str, entity_id: Optional[str] = None,
                    recipients_roles: Optional[Tuple[str, ...]] = None,
                    recipients_users: Optional[List[str]] = None,
                    tag_suffix: str = "",
                    decision_note: Optional[str] = None,
                    acted_by_role: Optional[str] = None,
                    decision_kind: Optional[str] = None) -> None:
    """Generic notifier for decisions (approve/reject/reviewed).
    - decision_note: optional note to include; if absent and entity_id provided, try to fetch from DB based on module and acted_by_role.
    - acted_by_role: one of 'finance','director','board' to determine toggle kind; if omitted, inferred from decision_kind or recipients_roles.
    - decision_kind: override toggle kind, e.g., 'finance_decision','director_decision','board_decision'.
    """
    try:
        if not _email_enabled():
            return
        # Determine kind for toggle & dedup
        kind = (decision_kind or "").strip().lower()
        if not kind:
            role = (acted_by_role or "").strip().lower()
            if role in ("finance", "director", "board"):
                kind = f"{role}_decision"
            elif recipients_roles and len(recipients_roles) == 1 and recipients_roles[0] in ("finance","director","board"):
                kind = f"{recipients_roles[0]}_decision"
            else:
                kind = "decision"

        # Gate by per-event toggle
        if not _notif_toggle_enabled(entity_type, kind, True):
            return

        recipients: List[str] = []
        if recipients_roles:
            roles = [r.strip().lower() for r in recipients_roles]
            if "director" in roles:
                recipients += _get_director_emails()
            if "finance" in roles:
                recipients += _get_finance_emails()
            if "board" in roles:
                recipients += _get_board_emails()
        if recipients_users:
            recipients += [e for e in recipients_users if e and '@' in e]
        recipients = sorted(set([e.lower() for e in recipients if e]))
        if not recipients:
            return

        # Try resolve decision note if not provided
        note = (decision_note or "").strip()
        if not note and entity_id:
            try:
                conn = get_db(); cur = conn.cursor()
                et = (entity_type or "").strip().lower()
                if et == "inventory":
                    col = "director_note" if "director" in kind else ("finance_note" if "finance" in kind else None)
                    if col:
                        cur.execute(f"SELECT {col} FROM inventory WHERE id=?", (entity_id,))
                        r = cur.fetchone(); note = (r[col] if r and r[col] else "")
                elif et == "cuti":
                    col = "director_note" if "director" in kind else ("finance_note" if "finance" in kind else None)
                    if col:
                        cur.execute(f"SELECT {col} FROM cuti WHERE id=?", (entity_id,))
                        r = cur.fetchone(); note = (r[col] if r and r[col] else "")
                elif et == "cash_advance":
                    col = "director_note" if "director" in kind else ("finance_note" if "finance" in kind else None)
                    if col:
                        cur.execute(f"SELECT {col} FROM cash_advance WHERE id=?", (entity_id,))
                        r = cur.fetchone(); note = (r[col] if r and r[col] else "")
                elif et == "flex":
                    col = "catatan_director" if "director" in kind else ("catatan_finance" if "finance" in kind else None)
                    if col:
                        cur.execute(f"SELECT {col} FROM flex WHERE id=?", (entity_id,))
                        r = cur.fetchone(); note = (r[col] if r and r[col] else "")
                elif et == "mou":
                    col = "director_note" if "director" in kind else ("board_note" if "board" in kind else None)
                    if col:
                        cur.execute(f"SELECT {col} FROM mou WHERE id=?", (entity_id,))
                        r = cur.fetchone(); note = (r[col] if r and r[col] else "")
            except Exception:
                pass

        # Subject & body
        decision_label = decision.replace('_',' ').title()
        subj = f"[WIJNA] {entity_type.replace('_',' ').title()} — {decision_label} — {title}"
        ts = format_datetime_wib(now_wib_iso())
        actor = get_current_user() or {}
        actor_name = actor.get('full_name') or actor.get('email') or '-'
        actor_role = (acted_by_role or actor.get('role') or '').title()
        lines = [
            f"Keputusan: {decision_label}",
            f"Modul: {entity_type}",
            f"Judul/Referensi: {title}",
            f"Waktu: {ts}",
            f"Oleh: {actor_name} ({actor_role or '-'})",
        ]
        if note:
            lines.append("")
            lines.append("Catatan Keputusan:")
            lines.append(note)
        lines.append("")
        lines.append("Notifikasi otomatis WIJNA.")
        body = "\n".join(lines)

        # Dedup key uses the computed kind
        tag = f"{entity_type}:{decision}:{entity_id or title}:{tag_suffix or '-'}"
        if _notif_already_sent(entity_type, entity_id or '-', kind, tag):
            return
        if _send_email(recipients, subj, body):
            _mark_notif_sent(entity_type, entity_id or '-', kind, tag, recipients)
    except Exception:
        pass

# --- Helpers: Public Holiday utilities & working days ---
def _get_all_active_emails() -> List[str]:
    """Return all active user emails (deduped, lowercase)."""
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("SELECT email FROM users WHERE status='active' AND email IS NOT NULL AND email<>''")
        rows = cur.fetchall() or []
        emails: List[str] = []
        for r in rows:
            e = r['email'] if isinstance(r, dict) else (r[0] if r else None)
            if e and '@' in e:
                emails.append(str(e).strip().lower())
        return sorted(set(emails))
    except Exception:
        return []

def _list_public_holidays_between(d1: date, d2: date) -> List[date]:
    """List all public holiday dates between inclusive d1..d2 using calendar.is_holiday=1 ranges.
    Falls back to public_holidays single-day entries if available.
    """
    if d2 < d1:
        d1, d2 = d2, d1
    out: List[date] = []
    try:
        conn = get_db(); cur = conn.cursor()
        # Ranged holidays from calendar table
        try:
            q = "SELECT tgl_mulai, tgl_selesai FROM calendar WHERE is_holiday=1 AND NOT (date(tgl_selesai) < date(?) OR date(tgl_mulai) > date(?))"
            rows = cur.execute(q, (d1.isoformat(), d2.isoformat())).fetchall() or []
            for r in rows:
                try:
                    s = pd.to_datetime(r['tgl_mulai'] if isinstance(r, dict) else r[0]).date()
                    e = pd.to_datetime(r['tgl_selesai'] if isinstance(r, dict) else r[1]).date()
                except Exception:
                    continue
                if e < s:
                    s, e = e, s
                cur_d = s
                while cur_d <= e:
                    out.append(cur_d)
                    cur_d += timedelta(days=1)
        except Exception:
            pass
        # Single-day fallback from public_holidays
        try:
            q2 = "SELECT tanggal FROM public_holidays WHERE date(tanggal) BETWEEN date(?) AND date(?)"
            rows2 = cur.execute(q2, (d1.isoformat(), d2.isoformat())).fetchall() or []
            for r in rows2:
                try:
                    out.append(pd.to_datetime(r['tanggal'] if isinstance(r, dict) else r[0]).date())
                except Exception:
                    continue
        except Exception:
            pass
    except Exception:
        return out
    # Dedup
    return sorted(set(out))

def _is_public_holiday(d: date) -> bool:
    try:
        holidays = _list_public_holidays_between(d, d)
        return len(holidays) > 0
    except Exception:
        return False

def _next_working_day(d: date) -> date:
    """Return the next date >= d that is not a public holiday (weekends still allowed unless managed as holiday)."""
    cur = d
    for _ in range(366):
        if not _is_public_holiday(cur):
            return cur
        cur = cur + timedelta(days=1)
    return d

def _count_days_excluding_holidays(d1: date, d2: date) -> int:
    """Inclusive day count excluding any days that are public holidays."""
    if d2 < d1:
        d1, d2 = d2, d1
    holidays = set(_list_public_holidays_between(d1, d2))
    count = 0
    cur = d1
    while cur <= d2:
        if cur not in holidays:
            count += 1
        cur += timedelta(days=1)
    return count

def run_automations_for_dashboard() -> None:
    """Lightweight email automations for Dashboard entry.
    - PMR lateness (> day 5): email to staff without PMR this month (cc Directors)
    - Delegasi reminders: ≤3 days to deadline (PIC), overdue (PIC + Directors)
    """
    try:
        if not _email_enabled():
            return
        today = date.today()
        this_month = today.strftime('%Y-%m')
        directors = _get_director_emails()
        conn = get_db(); cur = conn.cursor()

        # 1) PMR lateness
        if int(today.day) > 5 and (_setting_get('pmr_notify_enabled', 'true') == 'true'):
            try:
                # Active users to check (exclude superuser)
                cur.execute("SELECT id, full_name, email, role FROM users WHERE status='active' AND role <> 'superuser'")
                users_all = cur.fetchall() or []
                # Submitted PMR names this month
                pmr_df = pd.read_sql_query("SELECT DISTINCT nama FROM pmr WHERE substr(bulan,1,7)=?", conn._conn if hasattr(conn,'_conn') else conn, params=(this_month,))
                submitted = set([] if pmr_df is None or pmr_df.empty else [str(x).strip().lower() for x in pmr_df['nama'].tolist()])
                for u in users_all:
                    uname = (u['full_name'] if isinstance(u, dict) else u[1])
                    uid = (u['id'] if isinstance(u, dict) else u[0])
                    umail = (u['email'] if isinstance(u, dict) else u[2])
                    if not uname:
                        continue
                    if uname.strip().lower() in submitted:
                        continue
                    tag = f"pmr-{this_month}"
                    if _notif_already_sent('pmr_missing', str(uid), 'late', tag):
                        continue
                    recips = []
                    if umail: recips.append(umail)
                    for d in directors:
                        if d and d not in recips: recips.append(d)
                    if not recips:
                        continue
                    subj = f"[WIJNA] PMR {this_month} belum diunggah"
                    body = (
                        f"Halo {uname},\n\n"
                        f"Sistem mendeteksi hingga tanggal {today.day:02d} bahwa PMR untuk bulan {this_month} belum diunggah.\n"
                        f"Mohon segera upload PMR melalui modul PMR di aplikasi WIJNA.\n\n"
                        f"Terima kasih.\n"
                    )
                    if _send_email(recips, subj, body):
                        _mark_notif_sent('pmr_missing', str(uid), 'late', tag, recips)
            except Exception:
                pass

        # 2) Delegasi reminders
        if _setting_get('delegasi_notify_enabled', 'true') == 'true':
            try:
                df = pd.read_sql_query("SELECT id, judul, pic, tgl_selesai, status FROM delegasi", conn._conn if hasattr(conn,'_conn') else conn)
            except Exception:
                df = pd.DataFrame(columns=['id','judul','pic','tgl_selesai','status'])
            if not df.empty:
                for _, r in df.iterrows():
                    status = str(r.get('status','') or '').strip().lower()
                    if status in ('selesai','done'):
                        continue
                    try:
                        due = pd.to_datetime(r['tgl_selesai']).date()
                    except Exception:
                        continue
                    days_left = (due - today).days
                    pic_name = str(r.get('pic','') or '').strip()
                    pic_email = _get_user_email_by_name(pic_name) if pic_name else None
                    if days_left < 0:
                        # Overdue
                        tag = f"delegasi-{r['id']}-overdue"
                        if not _notif_already_sent('delegasi', str(r['id']), 'overdue', tag):
                            recips = []
                            if pic_email: recips.append(pic_email)
                            for d in directors:
                                if d and d not in recips: recips.append(d)
                            if recips:
                                subj = f"[WIJNA] Delegasi lewat tenggat: {r['judul']}"
                                body = (
                                    f"Tugas '{r['judul']}' (PIC: {pic_name}) telah lewat tenggat (due {due.isoformat()}).\n"
                                    f"Mohon segera ditindaklanjuti dan update status di modul Delegasi.\n"
                                )
                                if _send_email(recips, subj, body):
                                    _mark_notif_sent('delegasi', str(r['id']), 'overdue', tag, recips)
                    elif 0 <= days_left <= 3:
                        # Reminder window
                        tag = f"delegasi-{r['id']}-rem-{days_left}"
                        if not _notif_already_sent('delegasi', str(r['id']), 'reminder', tag):
                            recips = [pic_email] if pic_email else []
                            if recips:
                                subj = f"[WIJNA] Reminder {days_left} hari — {r['judul']}"
                                body = (
                                    f"Halo {pic_name},\n\n"
                                    f"Tugas '{r['judul']}' akan jatuh tempo pada {due.isoformat()} (sisa {days_left} hari).\n"
                                    f"Mohon pastikan progres dan update status di modul Delegasi.\n"
                                )
                                if _send_email(recips, subj, body):
                                    _mark_notif_sent('delegasi', str(r['id']), 'reminder', tag, recips)
    except Exception:
        # Never break dashboard rendering due to notifier
        pass

def _folder_usage_quick(service, folder_id: str) -> Dict:
    total = 0
    unknown = 0
    files = _drive_list(service, folder_id)
    for f in files:
        sz = f.get('size')
        if sz is not None:
            try:
                total += int(sz)
            except Exception:
                unknown += 1
        else:
            unknown += 1
    return {"total_bytes": total, "file_count": len(files), "unknown_size_count": unknown}

def _backup_db_now(service, folder_id: str) -> Tuple[bool, str]:
    if not os.path.exists(DB_PATH):
        return False, f"DB '{DB_PATH}' tidak ditemukan"
    base_name = _setting_get('auto_backup_filename', 'auto_backup.sqlite') or 'auto_backup.sqlite'
    try:
        db_size = os.path.getsize(DB_PATH)
    except Exception:
        db_size = 0
    cap = int(_setting_get('project_capacity_bytes', 2*1024*1024*1024) or 2*1024*1024*1024)
    usage = _folder_usage_quick(service, folder_id)
    used = int(usage.get('total_bytes', 0))
    # allow overwrite if same name exists
    try:
        q = f"name='{base_name}' and '{folder_id}' in parents and trashed=false"
        resp = service.files().list(q=q, spaces='drive', fields='files(id,size)', supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        existing = resp.get('files', [])
    except Exception:
        existing = []
    if not existing:
        if used >= cap:
            return False, "Kapasitas penuh."
        if used + db_size > cap:
            return False, "Ukuran backup melebihi kapasitas."
    try:
        with open(DB_PATH,'rb') as f:
            data = f.read()
        fid = _drive_upload_or_replace(service, folder_id, base_name, data, mimetype='application/x-sqlite3')
        conn = get_db(); cur = conn.cursor()
        if fid:
            cur.execute("INSERT INTO backup_log (file_name, drive_file_id, status, message) VALUES (?,?,?,?)", (base_name, fid, 'SUCCESS', 'auto'))
            conn.commit()
            return True, f"Backup sukses (ID: {fid})"
        else:
            cur.execute("INSERT INTO backup_log (file_name, drive_file_id, status, message) VALUES (?,?,?,?)", (base_name, None, 'FAILED', 'upload gagal'))
            conn.commit()
            return False, "Upload gagal"
    except Exception as e:
        try:
            conn = get_db(); cur = conn.cursor()
            cur.execute("INSERT INTO backup_log (file_name, drive_file_id, status, message) VALUES (?,?,?,?)", (base_name, None, 'FAILED', str(e)))
            conn.commit()
        except Exception:
            pass
        return False, f"Error: {e}"

# --- Scheduled backup slots & auto-restore-on-wake ---
DEFAULT_SCHEDULE_SLOTS = [
    {"start": 6,  "end": 12, "name": "slot_morning"},
    {"start": 12, "end": 18, "name": "slot_afternoon"},
    {"start": 18, "end": 23, "name": "slot_evening"},
    {"start": 23, "end": 6,  "name": "slot_night"},  # wrap
]

def _validate_slot_struct(slots) -> bool:
    if not isinstance(slots, list) or not slots:
        return False
    names = set()
    for s in slots:
        if not isinstance(s, dict):
            return False
        if 'start' not in s or 'end' not in s or 'name' not in s:
            return False
        try:
            st_h = int(s['start']); en_h = int(s['end'])
        except Exception:
            return False
        if not (0 <= st_h <= 23 and 0 <= en_h <= 23):
            return False
        if st_h == en_h:
            return False
        nm = str(s['name']).strip()
        if not nm or nm in names:
            return False
        names.add(nm)
    return True

def get_schedule_slots():
    raw = _setting_get('scheduled_backup_slots_json')
    if raw:
        try:
            import json as _json
            slots = _json.loads(raw)
            if _validate_slot_struct(slots):
                # normalize
                return [{"start": int(s['start']), "end": int(s['end']), "name": str(s['name']).strip()} for s in slots]
        except Exception:
            pass
    return DEFAULT_SCHEDULE_SLOTS

def determine_slot(now_local: datetime) -> str:
    h = now_local.hour
    for s in get_schedule_slots():
        st_h = int(s['start']); en_h = int(s['end'])
        if st_h < en_h:
            if st_h <= h < en_h:
                return s['name']
        else:  # wrap
            if h >= st_h or h < en_h:
                return s['name']
    return 'slot_unknown'

def check_scheduled_backup(service, folder_id: str) -> Tuple[bool, str]:
    enabled = _setting_get('scheduled_backup_enabled', 'false') == 'true'
    if not enabled:
        return False, 'Scheduled backup disabled'
    base_name = _setting_get('scheduled_backup_filename', 'scheduled_backup.sqlite') or 'scheduled_backup.sqlite'
    now_local = now_wib()
    slot = determine_slot(now_local)
    if slot == 'slot_unknown':
        return False, 'Outside defined slots'
    # Tag harian berdasarkan WIB
    today_tag = now_local.date().isoformat()
    last_slot_done = _setting_get('scheduled_backup_last_slot')
    last_slot_date = _setting_get('scheduled_backup_last_date')
    if last_slot_done == slot and last_slot_date == today_tag:
        return False, 'Slot already backed up'
    # capacity and overwrite guards
    if not os.path.exists(DB_PATH):
        return False, 'DB missing'
    try:
        with open(DB_PATH,'rb') as f:
            data = f.read()
    except Exception as e:
        return False, f'Cannot read DB: {e}'
    try:
        usage = _folder_usage_quick(service, folder_id)
        used_now = int(usage.get('total_bytes', 0))
    except Exception:
        used_now = 0
    cap = int(_setting_get('project_capacity_bytes', 2*1024*1024*1024) or 2*1024*1024*1024)
    try:
        q = f"name='{base_name}' and '{folder_id}' in parents and trashed=false"
        resp = service.files().list(q=q, spaces='drive', fields='files(id,size)', supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        existing = resp.get('files', [])
    except Exception:
        existing = []
    if not existing:
        if used_now >= cap:
            return False, 'Capacity reached'
        if used_now + len(data) > cap:
            return False, 'Backup exceeds capacity'
    fid = _drive_upload_or_replace(service, folder_id, base_name, data, mimetype='application/x-sqlite3')
    if fid:
        _setting_set('scheduled_backup_last_slot', slot)
        _setting_set('scheduled_backup_last_date', today_tag)
        try:
            conn = get_db(); cur = conn.cursor()
            cur.execute("INSERT INTO backup_log (file_name, drive_file_id, status, message) VALUES (?,?,?,?)", (base_name, fid, 'SUCCESS', f'scheduled {slot}'))
            conn.commit()
        except Exception:
            pass
        return True, f'Scheduled backup OK ({slot}) -> {base_name}'
    else:
        try:
            conn = get_db(); cur = conn.cursor()
            cur.execute("INSERT INTO backup_log (file_name, drive_file_id, status, message) VALUES (?,?,?,?)", (base_name, None, 'FAILED', f'scheduled {slot} upload error'))
            conn.commit()
        except Exception:
            pass
        return False, 'Upload failed'

def _is_probably_fresh_seed_db() -> bool:
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM users"); user_cnt = cur.fetchone()[0]
        if user_cnt > 3:  # WIJNA seeds up to 3 users in ensure_db
            return False
        cur.execute("SELECT COUNT(*) FROM backup_log"); bkup_cnt = cur.fetchone()[0]
        if bkup_cnt > 0:
            return False
        return True
    except Exception:
        return False

def _pick_latest_drive_backup_file(service, folder_id: str):
    try:
        files = _drive_list(service, folder_id)
    except Exception:
        return None
    if not files:
        return None
    candidates = [f for f in files if f.get('name','').endswith(('.sqlite','.db'))]
    if not candidates:
        return None
    try:
        candidates.sort(key=lambda x: x.get('modifiedTime',''), reverse=True)
    except Exception:
        pass
    return candidates[0]

def attempt_auto_restore_if_seed(service, folder_id: str) -> Tuple[bool, str]:
    if _setting_get('auto_restore_enabled','true') != 'true':
        return False, 'Auto-restore disabled'
    if not _is_probably_fresh_seed_db():
        return False, 'DB not fresh'
    latest = _pick_latest_drive_backup_file(service, folder_id)
    if not latest:
        return False, 'No backup found'
    fid = latest.get('id'); fname = latest.get('name')
    try:
        data = _drive_download(service, fid)
        if not data.startswith(b'SQLite format 3\x00'):
            return False, 'Invalid sqlite header'
        with open(DB_PATH,'wb') as f:
            f.write(data)
        _setting_set('auto_restore_last_file', fname)
        _setting_set('auto_restore_last_time', now_wib_iso())
        return True, f'Restored from {fname}'
    except Exception as e:
        return False, f'Restore failed: {e}'

def dunyim_security_module():
    user = require_login()
    st.header("🛡️ Dunyim Security System")
    if not _drive_available():
        st.error("Paket Google API belum terpasang. Tambahkan 'google-api-python-client' dan 'google-auth' di requirements.")
        return
    folder_id = _setting_get('gdrive_folder_id', GDRIVE_DEFAULT_FOLDER_ID) or GDRIVE_DEFAULT_FOLDER_ID
    # Settings only visible to superuser
    if (user or {}).get("role") == "superuser":
        with st.expander("⚙️ Pengaturan", expanded=not bool(folder_id)):
            fld = st.text_input("Folder ID Google Drive", value=folder_id)
            cap = st.number_input("Kapasitas (bytes)", min_value=0, value=int(_setting_get('project_capacity_bytes', 2*1024*1024*1024) or 2*1024*1024*1024))
            colA, colB = st.columns(2)
            with colA:
                sched_enabled = st.checkbox("Aktifkan Scheduled Backup", value=(_setting_get('scheduled_backup_enabled','false')=='true'))
            with colB:
                sched_name = st.text_input("Nama file jadwal (overwrite)", value=_setting_get('scheduled_backup_filename','scheduled_backup.sqlite') or 'scheduled_backup.sqlite')
            if st.button("Simpan Pengaturan"):
                if fld:
                    _setting_set('gdrive_folder_id', fld)
                _setting_set('project_capacity_bytes', str(cap))
                _setting_set('scheduled_backup_enabled', 'true' if sched_enabled else 'false')
                _setting_set('scheduled_backup_filename', sched_name.strip() or 'scheduled_backup.sqlite')
                st.success("Pengaturan disimpan.")
                st.rerun()
    if not folder_id:
        st.info("Masukkan Folder ID terlebih dahulu.")
        return
    try:
        service = _build_drive()
    except Exception as e:
        st.error(str(e)); return
    tabs = st.tabs(["List","Upload","Download","Delete","Sync DB","Audit Log","Record","Drive Usage"])
    # List
    with tabs[0]:
        try:
            files = _drive_list(service, folder_id)
        except Exception as e:
            st.error(f"Gagal list: {e}"); files=[]
        if not files:
            st.info("Folder kosong.")
        else:
            df = pd.DataFrame(files)
            if 'size' in df.columns:
                df['size'] = df['size'].fillna(0).astype(int).apply(_bytes_fmt)
            st.dataframe(df[['name','id','mimeType','modifiedTime'] + (['size'] if 'size' in df.columns else [])], use_container_width=True, hide_index=True)
        c1, c2 = st.columns(2)
        with c1:
            if st.button("🚀 Backup DB Sekarang"):
                ok, msg = _backup_db_now(service, folder_id)
                st.success(msg) if ok else st.error(msg)
        with c2:
            if st.button("⏱️ Paksa Backup Slot Saat Ini"):
                ok, msg = check_scheduled_backup(service, folder_id)
                if ok:
                    st.success(msg)
                else:
                    st.info(msg)
    # Upload
    with tabs[1]:
        f = st.file_uploader("Pilih file untuk upload")
        if f and st.button("Upload"):
            data = f.read()
            usage = _folder_usage_quick(service, folder_id)
            cap = int(_setting_get('project_capacity_bytes', 2*1024*1024*1024) or 2*1024*1024*1024)
            if usage['total_bytes'] + len(data) > cap:
                st.error("Melebihi kapasitas.")
            else:
                fid = _drive_upload_or_replace(service, folder_id, f.name, data, mimetype=f.type or 'application/octet-stream')
                st.success(f"Uploaded (ID: {fid})") if fid else st.error("Gagal upload")
    # Download
    with tabs[2]:
        files = _drive_list(service, folder_id)
        if not files:
            st.info("Folder kosong.")
        else:
            mp = {x['name']: x['id'] for x in files}
            sel = st.selectbox("File", list(mp.keys()))
            if st.button("Download"):
                try:
                    data = _drive_download(service, mp[sel])
                    st.download_button("Klik untuk download", data=data, file_name=sel)
                except Exception as e:
                    st.error(f"Gagal download: {e}")
    # Delete
    with tabs[3]:
        files = _drive_list(service, folder_id)
        if not files:
            st.info("Folder kosong.")
        else:
            mp = {x['name']: x['id'] for x in files}
            sel = st.selectbox("Pilih file", list(mp.keys()))
            if st.button("Hapus"):
                try:
                    _drive_delete(service, mp[sel])
                    st.success("Terhapus."); st.rerun()
                except Exception as e:
                    st.error(f"Gagal hapus: {e}")
    # Sync DB
    with tabs[4]:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("### ⬆️ Upload & Replace DB Lokal")
            up = st.file_uploader("File .sqlite/.db", type=["sqlite","db"])
            if up and st.button("Replace DB"):
                data = up.read()
                if not data.startswith(b"SQLite format 3\x00"):
                    st.error("File bukan SQLite valid.")
                else:
                    ts = now_wib().strftime('%Y%m%d_%H%M%S')
                    if os.path.exists(DB_PATH):
                        try:
                            with open(DB_PATH,'rb') as a, open(f"local_backup_before_replace_{ts}.sqlite",'wb') as b:
                                b.write(a.read())
                            st.info("Backup lokal lama tersimpan.")
                        except Exception as e:
                            st.warning(f"Backup lokal gagal: {e}")
                    with open(DB_PATH,'wb') as f2:
                        f2.write(data)
                    st.success("DB lokal diganti.")
        with col2:
            st.markdown("### ⬇️ Restore dari Drive")
            files = _drive_list(service, folder_id)
            dbs = [f for f in files if f.get('name','').endswith(('.sqlite','.db'))]
            if not dbs:
                st.info("Tidak ada file DB di Drive.")
            else:
                try:
                    dbs.sort(key=lambda x: x.get('modifiedTime',''), reverse=True)
                except Exception:
                    pass
                mp = {x['name']: x['id'] for x in dbs}
                sel = st.selectbox("Pilih file DB", list(mp.keys()))
                if st.button("Restore DB Lokal"):
                    try:
                        data = _drive_download(service, mp[sel])
                        if not data.startswith(b"SQLite format 3\x00"):
                            st.error("Bukan SQLite valid.")
                        else:
                            ts = now_wib().strftime('%Y%m%d_%H%M%S')
                            if os.path.exists(DB_PATH):
                                try:
                                    with open(DB_PATH,'rb') as a, open(f"local_backup_before_restore_{ts}.sqlite",'wb') as b:
                                        b.write(a.read())
                                    st.info("Backup lokal lama tersimpan.")
                                except Exception as e:
                                    st.warning(f"Backup lokal gagal: {e}")
                            with open(DB_PATH,'wb') as f2:
                                f2.write(data)
                            st.success("DB berhasil direstore. Reload halaman.")
                    except Exception as e:
                        st.error(f"Gagal restore: {e}")
    # Audit Log
    with tabs[5]:
        try:
            conn = get_db(); df = pd.read_sql_query("SELECT * FROM backup_log ORDER BY id DESC LIMIT 20", conn)
        except Exception:
            df = pd.DataFrame()
        st.subheader("Riwayat Backup")
        if df.empty:
            st.info("Belum ada log backup.")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)
    # Record
    with tabs[6]:
        st.subheader("Catatan Manual")
        with st.form("note_form"):
            t = st.text_input("Catatan baru")
            s = st.form_submit_button("Simpan")
            if s and t.strip():
                try:
                    conn = get_db(); cur = conn.cursor()
                    cur.execute("INSERT INTO record_notes (note, created_by) VALUES (?,?)", (t.strip(), user['email']))
                    conn.commit(); st.success("Tersimpan."); st.rerun()
                except Exception as e:
                    st.error(f"Gagal menyimpan: {e}")
        try:
            conn = get_db(); notes = pd.read_sql_query("SELECT * FROM record_notes ORDER BY id DESC LIMIT 50", conn)
        except Exception:
            notes = pd.DataFrame()
        if notes.empty:
            st.info("Belum ada catatan.")
        else:
            st.dataframe(notes[['id','note','created_by','created_at']], use_container_width=True, hide_index=True)
    # Drive Usage
    with tabs[7]:
        st.subheader("Penggunaan Drive")
        try:
            usage = _folder_usage_quick(service, folder_id)
        except Exception as e:
            st.error(f"Gagal menghitung penggunaan: {e}")
            usage = {"total_bytes":0,"file_count":0,"unknown_size_count":0}
        cap = int(_setting_get('project_capacity_bytes', 2*1024*1024*1024) or 2*1024*1024*1024)
        used = int(usage.get('total_bytes',0))
        st.metric("Used", _bytes_fmt(used))
        st.metric("Capacity", _bytes_fmt(cap))
        pct = (used/cap*100.0) if cap>0 else 0.0
        st.progress(min(pct/100.0, 1.0))
def inventory_module():
    # Prepare monthly rekap at the top
    user = require_login()
    conn = get_db()
    cur = conn.cursor()
    this_month = date.today().strftime("%Y-%m")
    # Safeguard: if table missing (first migration), create it and continue
    try:
        df_month = pd.read_sql_query("SELECT * FROM inventory WHERE substr(updated_at,1,7)=?", conn, params=(this_month,))
    except Exception:
        try:
            cur.execute("SELECT 1 FROM inventory LIMIT 1")
            df_month = pd.DataFrame()
        except Exception:
            # create table on the fly (should already exist via ensure_db, but fallback)
            cur.execute("""CREATE TABLE IF NOT EXISTS inventory (
                id TEXT PRIMARY KEY,
                name TEXT,
                location TEXT,
                status TEXT,
                pic TEXT,
                updated_at TEXT,
                finance_note TEXT,
                finance_approved INTEGER DEFAULT 0,
                director_note TEXT,
                director_approved INTEGER DEFAULT 0,
                file_blob BLOB,
                file_name TEXT
            )""")
            conn.commit()
            df_month = pd.DataFrame()
    # --- UI with Tabs: Selalu tampilkan SEMUA tab; hak akses diatur di dalam masing-masing tab ---
    st.markdown("# 📦 Inventory")

    tab_labels = [
        "➕ Tambah Barang",
        "💰 Review Finance",
        "✅ Approval Director",
        "📦 Daftar Inventaris",
    ]

    # Tab 1: Tambah Barang (aksi hanya untuk Staff/Superuser)
    def staff_tab():
        allowed = user["role"] in ["staff", "superuser"]
        if not allowed:
            st.info("Hanya Staff atau Superuser yang dapat menambah barang. Tab ini ditampilkan untuk transparansi alur kerja.")
            return
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
                    now = now_wib_iso()
                    blob, fname, _ = upload_file_and_store(f) if f else (None, None, None)
                    # PIC dihapus, set kosong
                    pic = ""
                    cur.execute("""INSERT INTO inventory (id,name,location,status,pic,updated_at,finance_note,finance_approved,director_note,director_approved,file_blob,file_name)
                               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                                (iid, full_nama, loc, status, pic, now, '', 0, '', 0, blob, fname))
                    conn.commit()
                    try:
                        audit_log("inventory", "create", target=iid, details=f"{full_nama} @ {loc} status={status}")
                    except Exception:
                        pass
                    # Notify Finance + Director immediately on draft creation
                    try:
                        notify_review_request("inventory", title=f"{full_nama} — {loc}", entity_id=iid, recipients_roles=("finance","director"))
                    except Exception:
                        pass
                    st.success("Item disimpan sebagai draft. Menunggu review Finance.")

    # Tab 2: Review Finance (aksi hanya untuk Finance/Superuser; lainnya read-only)
    def finance_tab():
        allowed = user["role"] in ["finance", "superuser"]
        if not allowed:
            st.info("Hanya Finance atau Superuser yang dapat melakukan review. Anda dapat melihat daftar yang menunggu review.")
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
                note = st.text_area(
                    "Tulis catatan atau alasan jika perlu",
                    value=r["finance_note"] or "",
                    key=f"fin_note_{r['id']}_finance_{idx}",
                    disabled=not allowed,
                )
                colf1, colf2 = st.columns([1,2])
                with colf1:
                    if allowed and st.button("🔎 Review", key=f"ap_fin_{r['id']}_finance_{idx}"):
                        cur.execute("UPDATE inventory SET finance_note=?, finance_approved=1 WHERE id=?", (note, r["id"]))
                        conn.commit()
                        try:
                            audit_log("inventory", "finance_review", target=r["id"], details=note)
                        except Exception:
                            pass
                        # Notify Director + requester (if resolvable from PIC)
                        try:
                            requester_email = None
                            # PIC may store requester id or name; try resolve
                            requester_email = _resolve_user_email_by_id_or_name(r.get('pic')) if isinstance(r, sqlite3.Row) else None
                            notify_decision(
                                "inventory",
                                title=r['name'],
                                decision="finance_reviewed",
                                entity_id=r['id'],
                                recipients_roles=("director",),
                                recipients_users=[requester_email] if requester_email else None,
                                tag_suffix="finance",
                                decision_note=note,
                                acted_by_role="finance",
                                decision_kind="finance_decision",
                            )
                        except Exception:
                            pass
                        st.success("Finance reviewed. Menunggu persetujuan Director.")
                with colf2:
                    st.caption("Klik Review jika sudah sesuai. Catatan akan tersimpan di database.")
        # Section: items waiting for Director approval with resend option
        st.markdown("---")
        st.subheader("Menunggu Approval Director" + (" (Kirim Ulang Notifikasi)" if allowed else ""))
        cur.execute("SELECT * FROM inventory WHERE finance_approved=1 AND director_approved=0 ORDER BY updated_at DESC")
        waiting = cur.fetchall()
        if not waiting:
            st.caption("Tidak ada item yang menunggu.")
        else:
            for jdx, w in enumerate(waiting):
                col1, col2, col3 = st.columns([3,3,2])
                with col1:
                    st.write(f"{w['name']} ({w['id']})")
                with col2:
                    st.write(f"PIC: {w['pic']}")
                with col3:
                    if allowed and st.button("Kirim Ulang Notifikasi", key=f"resend_dir_{w['id']}"):
                        try:
                            requester_email = _resolve_user_email_by_id_or_name(w.get('pic')) if isinstance(w, sqlite3.Row) else None
                            notify_decision(
                                "inventory",
                                title=w['name'],
                                decision="finance_reviewed",
                                entity_id=w['id'],
                                recipients_roles=("director",),
                                recipients_users=[requester_email] if requester_email else None,
                                tag_suffix="finance-resend",
                                acted_by_role="finance",
                                decision_kind="finance_decision",
                            )
                            st.success("Notifikasi dikirim.")
                        except Exception:
                            st.warning("Gagal mengirim notifikasi.")

    # Tab 3: Approval Director (aksi hanya untuk Director/Superuser; lainnya read-only)
    def director_tab():
        allowed = user["role"] in ["director", "superuser"]
        if not allowed:
            st.info("Hanya Director atau Superuser yang dapat memberikan persetujuan. Anda dapat melihat daftar yang menunggu persetujuan.")
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
                note2 = st.text_area(
                    "",
                    value=r["director_note"] or "",
                    key=f"dir_note_{r['id']}_director_{idx}",
                    placeholder="Tulis catatan atau alasan jika perlu...",
                    height=80,
                    disabled=not allowed,
                )
                colA, colB = st.columns([1,1])
                with colA:
                    if allowed and st.button("✅ Approve", key=f"ap_dir_{r['id']}_director_{idx}"):
                        cur.execute("UPDATE inventory SET director_note=?, director_approved=1 WHERE id=?", (note2, r["id"]))
                        conn.commit()
                        try:
                            audit_log("inventory", "director_approval", target=r["id"], details=f"approve=1; note={note2}")
                        except Exception:
                            pass
                        # Notify requester + Finance
                        try:
                            requester_email = _resolve_user_email_by_id_or_name(r.get('pic')) if isinstance(r, sqlite3.Row) else None
                            notify_decision(
                                "inventory",
                                title=r['name'],
                                decision="director_approved",
                                entity_id=r['id'],
                                recipients_roles=("finance",),
                                recipients_users=[requester_email] if requester_email else None,
                                tag_suffix="director",
                                decision_note=note2,
                                acted_by_role="director",
                                decision_kind="director_decision",
                            )
                        except Exception:
                            pass
                        st.success("Item telah di-approve Director.")
                with colB:
                    if allowed and st.button("❌ Tolak", key=f"reject_dir_{r['id']}_director_{idx}"):
                        cur.execute("UPDATE inventory SET director_note=?, director_approved=-1 WHERE id=?", (note2, r["id"]))
                        conn.commit()
                        try:
                            audit_log("inventory", "director_approval", target=r["id"], details=f"approve=0; note={note2}")
                        except Exception:
                            pass
                        try:
                            requester_email = _resolve_user_email_by_id_or_name(r.get('pic')) if isinstance(r, sqlite3.Row) else None
                            notify_decision(
                                "inventory",
                                title=r['name'],
                                decision="director_rejected",
                                entity_id=r['id'],
                                recipients_roles=("finance",),
                                recipients_users=[requester_email] if requester_email else None,
                                tag_suffix="director",
                                decision_note=note2,
                                acted_by_role="director",
                                decision_kind="director_decision",
                            )
                        except Exception:
                            pass

    # Tab 4: Daftar Inventaris (tetap tanpa batasan)
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
                st.dataframe(show_df, width='stretch')

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
                                notify_review_request("inventory", title=f"Pinjam {row['name']} oleh {user['full_name']}", entity_id=row['id'], recipients_roles=("finance","director"))
                            except Exception:
                                pass
                            st.success("Pengajuan pinjam barang berhasil. Menunggu ACC Finance & Director.")

    # Render tabs dalam urutan tetap dan jalankan fungsi masing-masing
    tab_contents = [staff_tab, finance_tab, director_tab, data_tab]
    selected = st.tabs(tab_labels)
    for i, tab_func in enumerate(tab_contents):
        with selected[i]:
            tab_func()

def surat_masuk_module():
    st.header("📥 Surat Masuk")
    user = get_current_user()
    allowed_roles = ["staff", "finance", "director", "superuser"]
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
                    file_blob = file_upload.read()
                    file_name = file_upload.name
                    # Simpan data ke DB (indeks otomatis di rekap)
                    sid = str(uuid.uuid4())
                    cur.execute("""
                        INSERT INTO surat_masuk (id, nomor, tanggal, pengirim, perihal, file_blob, file_name, status, follow_up)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        sid,
                        nomor,
                        tanggal.isoformat(),
                        pengirim,
                        perihal,
                        file_blob,
                        file_name,
                        status,
                        follow_up
                    ))
                    conn.commit()
                    try:
                        audit_log("surat_masuk", "create", target=sid, details=f"{nomor} - {perihal} ({pengirim})")
                        notify_review_request("surat_masuk", title=f"{nomor} — {perihal}", entity_id=sid, recipients_roles=("director",))
                    except Exception:
                        pass
                    st.success("Surat masuk berhasil dicatat.")

    with tab2:
        st.markdown("### Approval Surat Masuk")
        if user["role"] in ["director", "superuser"]:
            conn = get_db()
            cur = conn.cursor()
            df = pd.read_sql_query("SELECT id, nomor, tanggal, pengirim, perihal, file_name, status, follow_up, director_approved, rekap FROM surat_masuk ORDER BY tanggal DESC", conn)
            for idx, row in df.iterrows():
                if row.get("director_approved", 0) == 0:
                    with st.expander(f"{row['nomor']} | {row['perihal']} | {row['tanggal']}"):
                        st.write(f"Pengirim: {row['pengirim']}")
                        st.write(f"Status: {row['status']}")
                        st.write(f"Follow Up: {row['follow_up']}")
                        if row['file_name']:
                            if str(row['file_name']).startswith('http'):
                                st.markdown(f"[Link Surat]({row['file_name']})")
                            else:
                                lihat = st.button(f"Lihat File Surat", key=f"dl_{row['id']}")
                                if lihat:
                                    try:
                                        audit_log("surat_masuk", "view_file", target=row['id'], details=row['file_name'])
                                    except Exception:
                                        pass
                                    cur.execute("SELECT file_blob, file_name FROM surat_masuk WHERE id= ?", (row['id'],))
                                    f = cur.fetchone()
                                    if f and f['file_blob']:
                                        st.download_button("Lihat File Surat", data=f['file_blob'], file_name=f['file_name'])
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
        df = pd.read_sql_query("SELECT id, nomor, tanggal, pengirim, perihal, file_name, rekap, director_approved FROM surat_masuk ORDER BY tanggal DESC", conn)
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
                cur.execute("SELECT file_blob FROM surat_masuk WHERE id=?", (row['id'],))
                f = cur.fetchone()
                if f and f['file_blob']:
                    import base64
                    b64 = base64.b64encode(f['file_blob']).decode()
                    href = f'<a class="rekap-download-btn" href="data:application/octet-stream;base64,{b64}" download="{row["file_name"]}"><span style="font-size:1.1em;">⬇️</span> Download</a>'
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

    # --- Tab 1: Input Draft oleh Staff ---
    with tab1:
        st.markdown("### Input Draft Surat Keluar (Staff)")
        draft_type = st.radio("Jenis Draft Surat", ["Upload File", "Link URL"], horizontal=True, key="draft_type_sk")
        with st.form("sk_add", clear_on_submit=True):
            col1, col2 = st.columns(2)
            with col1:
                nomor = st.text_input("Nomor Surat")
                tanggal = st.date_input("Tanggal", value=date.today())
            with col2:
                ditujukan = st.text_input("Ditujukan Kepada")
                perihal = st.text_input("Perihal")
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
                    if draft_type == "Upload File":
                        draft_blob, draft_name, _ = upload_file_and_store(draft)
                    cur.execute("""INSERT INTO surat_keluar (id,indeks,nomor,tanggal,ditujukan,perihal,pengirim,draft_blob,draft_name,status,follow_up, draft_url)
                                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (sid, '', nomor, tanggal.isoformat(), ditujukan, perihal, user['full_name'], draft_blob, draft_name, "Draft", follow_up, draft_url))
                    conn.commit()
                    try:
                        det = f"draft_file={draft_name}" if draft_name else f"draft_url={draft_url}"
                        audit_log("surat_keluar", "create", target=sid, details=f"{nomor}-{perihal}; {det}")
                        notify_review_request("surat_keluar", title=f"Draft {nomor} — {perihal}", entity_id=sid, recipients_roles=("director",))
                    except Exception:
                        pass
                    st.success("✅ Surat keluar (draft) tersimpan.")

    # --- Tab 2: Approval Director ---
    with tab2:
        st.markdown("### Approval Surat Keluar (Director)")
        if user["role"] in ["director","superuser"]:
            df = pd.read_sql_query("SELECT id,indeks,nomor,tanggal,ditujukan,perihal,pengirim,status,follow_up, director_approved, final_name, draft_blob, draft_name, draft_url FROM surat_keluar ORDER BY tanggal DESC", conn)
            for idx, row in df.iterrows():
                with st.expander(f"{row['nomor']} | {row['perihal']} | {row['tanggal']} | Status: {row['status']}"):
                    st.write(f"Ditujukan: {row['ditujukan']}")
                    st.write(f"Pengirim: {row['pengirim']}")
                    st.write(f"Follow Up: {row['follow_up']}")
                    # Preview/download draft
                    if row['draft_blob'] and row['draft_name']:
                        st.markdown(f"**Draft Surat (file):** {row['draft_name']}")
                        show_file_download(row['draft_blob'], row['draft_name'])
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
                            cur.execute("UPDATE surat_keluar SET final_blob=?, final_name=?, director_note=?, director_approved=1, status='Final' WHERE id=?",
                                        (blob, fname, note, row['id']))
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
        df = pd.read_sql_query("SELECT id,indeks,nomor,tanggal,ditujukan,perihal,pengirim,status,follow_up, director_approved, final_name, draft_name, draft_url, final_blob FROM surat_keluar ORDER BY tanggal DESC", conn)
        # Indeks otomatis: urutan
        if not df.empty:
            df = df.copy()
            df['indeks'] = [f"SK-{i+1:04d}" for i in range(len(df))]
        # Tabel utama
        st.dataframe(df[["indeks","nomor","tanggal","ditujukan","perihal","pengirim","status","follow_up","final_name"]], width='stretch', hide_index=True)

        # Download File Final
        st.markdown("#### Download File Final Surat Keluar")
        if not df.empty:
            for idx, row in df.iterrows():
                if row['final_blob'] and row['final_name']:
                    st.write(f"{row['nomor']} | {row['perihal']} | {row['tanggal']}")
                    show_file_download(row['final_blob'], row['final_name'])

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
            wrote_excel = False
            # Try openpyxl first, fall back to xlsxwriter, then fallback CSV only
            for engine_name in ["openpyxl", "xlsxwriter"]:
                if wrote_excel:
                    break
                try:
                    df_month.to_excel(excel_buffer, index=False, engine=engine_name)
                    excel_buffer.seek(0)
                    st.download_button(
                        "⬇️ Download Rekap Bulanan (Excel)",
                        excel_buffer,
                        file_name=f"rekap_suratkeluar_{this_month}.xlsx"
                    )
                    wrote_excel = True
                except Exception:
                    excel_buffer = io.BytesIO()  # reset buffer for next attempt
                    continue
            if not wrote_excel:
                st.info("Library openpyxl/xlsxwriter tidak tersedia. Export Excel dinonaktifkan.")

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
        st.markdown("### Input Draft MoU (Staff)")
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
                    created_by = (user.get('id') if isinstance(user, dict) else None)
                    cur.execute("""INSERT INTO mou (id,nomor,nama,pihak,jenis,tgl_mulai,tgl_selesai,file_blob,file_name,board_note,board_approved,final_blob,final_name,created_by)
                                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (mid, nomor, nama, pihak, jenis, tgl_mulai.isoformat(), tgl_selesai.isoformat(), blob, fname, "", 0, None, None, created_by))
                    conn.commit()
                    try:
                        audit_log("mou", "create", target=mid, details=f"{nomor} - {nama} ({jenis})")
                    except Exception:
                        pass
                    # Notify Board and Director for new draft
                    try:
                        title = f"{nomor} — {nama}"
                        notify_review_request("mou", title=title, entity_id=mid, recipients_roles=("board","director"))
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
                        # Notify Director + creator/applicant
                        try:
                            creator_email = None
                            try:
                                r2 = pd.read_sql_query("SELECT created_by, nomor, nama FROM mou WHERE id=?", conn, params=(row['id'],))
                                if not r2.empty:
                                    creator_email = _resolve_user_email_by_id_or_name(r2.iloc[0].get('created_by'))
                                    title = f"{r2.iloc[0]['nomor']} — {r2.iloc[0]['nama']}"
                                else:
                                    title = f"{row['nomor']} — {row['nama']}"
                            except Exception:
                                title = f"{row['nomor']} — {row['nama']}"
                            decision = "board_approved" if approve else "board_rejected"
                            notify_decision("mou", title=title, decision=decision, entity_id=row['id'],
                                            recipients_roles=("director",), recipients_users=[creator_email] if creator_email else None,
                                            tag_suffix="board")
                        except Exception:
                            pass
                        st.rerun()
        else:
            st.info("Hanya Board yang dapat review di sini.")



    # --- Tab 4: Daftar & Rekap MoU ---
    with tab4:
        st.markdown("### Daftar & Rekap MoU")
        df = pd.read_sql_query("SELECT id, nomor, nama, pihak, jenis, tgl_mulai, tgl_selesai, file_name, board_approved FROM mou ORDER BY tgl_selesai ASC", conn)
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
            st.dataframe(disp, width='stretch', hide_index=True)

        with right:
            st.subheader("⬇️ Download File")
            if not df.empty:
                # Hanya tampilkan opsi yang memiliki file
                opt_map = {f"{r['nomor']} | {r['nama']} — {r['file_name']}": r['id'] for _, r in df.iterrows() if r.get('file_name')}
                pilihan = st.selectbox("Pilih MoU", [""] + list(opt_map.keys()))
                if pilihan:
                    mid = opt_map[pilihan]
                    row = pd.read_sql_query("SELECT file_blob, file_name FROM mou WHERE id=?", conn, params=(mid,))
                    if not row.empty and row.iloc[0]["file_blob"] is not None and row.iloc[0]["file_name"]:
                        data_bytes = from_blob(row.iloc[0]["file_blob"])  # decode if base64
                        st.download_button(
                            label=f"Download {row.iloc[0]['file_name']}",
                            data=data_bytes,
                            file_name=row.iloc[0]['file_name'],
                            mime="application/octet-stream"
                        )
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
        "📝 Input Staff",
        "💰 Review Finance",
        "✅ Approval Director",
        "📋 Daftar & Rekap"
    ])

    # --- Tab 1: Input Staf ---
    with tab1:
        st.markdown("### Pengajuan Cash Advance (Staff)")
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
                requester_id = (user.get('id') if isinstance(user, dict) else None)
                try:
                    cur.execute("INSERT INTO cash_advance (id,divisi,items_json,totals,tanggal,finance_note,finance_approved,director_note,director_approved,requested_by) VALUES (?,?,?,?,?,?,?,?,?,?)",
                                (cid, nama_program, json.dumps(items), total, tanggal.isoformat(), "", 0, "", 0, requester_id))
                except Exception:
                    # fallback if column doesn't exist yet
                    cur.execute("INSERT INTO cash_advance (id,divisi,items_json,totals,tanggal,finance_note,finance_approved,director_note,director_approved) VALUES (?,?,?,?,?,?,?,?,?)",
                                (cid, nama_program, json.dumps(items), total, tanggal.isoformat(), "", 0, "", 0))
                conn.commit()
                try:
                    audit_log("cash_advance", "create", target=cid, details=f"divisi={nama_program}; total={total}")
                except Exception:
                    pass
                try:
                    notify_review_request("cash_advance", title=f"{nama_program} — {format_rp(total)}", entity_id=cid, recipients_roles=("finance","director"))
                except Exception:
                    pass
                st.success("Cash advance diajukan.")
                st.session_state['ca_nominals'] = [0.0]*10

    # --- Tab 2: Review Finance ---
    with tab2:
        st.markdown("### Review & Approval Finance")
        if user["role"] in ["finance", "superuser"]:
            df = pd.read_sql_query("SELECT id, divisi, items_json, totals, tanggal, finance_note, finance_approved, COALESCE(requested_by,'') as requested_by FROM cash_advance ORDER BY tanggal DESC", conn)
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
                        # Notify Director + applicant
                        try:
                            applicant_email = _resolve_user_email_by_id_or_name(row.get('requested_by')) if isinstance(row, dict) else None
                            decision = "finance_approved"
                            title_txt = f"{row['divisi']} — {format_rp(row['totals'])}"
                            notify_decision("cash_advance", title=title_txt, decision=decision, entity_id=row['id'],
                                            recipients_roles=("director",), recipients_users=[applicant_email] if applicant_email else None,
                                            tag_suffix="finance")
                        except Exception:
                            pass
                        st.rerun()
                    if return_user:
                        cur.execute("UPDATE cash_advance SET finance_note=?, finance_approved=0 WHERE id=?", (note + "\n[Perlu revisi oleh user]", row['id']))
                        conn.commit()
                        try:
                            audit_log("cash_advance", "finance_review", target=row['id'], details=f"approve=0; note={note}")
                        except Exception:
                            pass
                        st.warning("Dikembalikan ke user peminta.")
                        # Notify Director + applicant of rejection
                        try:
                            applicant_email = _resolve_user_email_by_id_or_name(row.get('requested_by')) if isinstance(row, dict) else None
                            decision = "finance_rejected"
                            title_txt = f"{row['divisi']} — {format_rp(row['totals'])}"
                            notify_decision("cash_advance", title=title_txt, decision=decision, entity_id=row['id'],
                                            recipients_roles=("director",), recipients_users=[applicant_email] if applicant_email else None,
                                            tag_suffix="finance")
                        except Exception:
                            pass
                        st.rerun()
        else:
            st.info("Hanya Finance yang dapat review di sini.")

    # --- Tab 3: Approval Director ---
    with tab3:
        st.markdown("### Approval Director Cash Advance")
        if user["role"] in ["director", "superuser"]:
            df = pd.read_sql_query("SELECT id, divisi, items_json, totals, tanggal, finance_approved, director_note, director_approved, COALESCE(requested_by,'') as requested_by FROM cash_advance ORDER BY tanggal DESC", conn)
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
                        # Notify applicant + Finance
                        try:
                            applicant_email = _resolve_user_email_by_id_or_name(row.get('requested_by')) if isinstance(row, dict) else None
                            decision = "director_approved" if approve else "director_rejected"
                            title_txt = f"{row['divisi']} — {format_rp(row['totals'])}"
                            notify_decision("cash_advance", title=title_txt, decision=decision, entity_id=row['id'],
                                            recipients_roles=("finance",), recipients_users=[applicant_email] if applicant_email else None,
                                            tag_suffix="director")
                        except Exception:
                            pass
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
        "📝 Upload Laporan Bulanan (Staff)",
        "💰 Review & Approval Finance",
        "✅ Approval Director PMR",
        "📋 Daftar & Rekap PMR"
    ])

    with tab_upload:
        st.markdown("### Upload Laporan Bulanan (Staff)")
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
                    if f2:
                        b2, n2, _ = upload_file_and_store(f2)
                    else:
                        b2, n2 = None, None
                    now = now_wib_iso()
                    cur.execute("""INSERT INTO pmr (id,nama,file1_blob,file1_name,file2_blob,file2_name,bulan,finance_note,finance_approved,director_note,director_approved,tanggal_submit)
                                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                                (pid, nama, b1, n1, b2, n2, bulan, "", 0, "", 0, now))
                    conn.commit()
                    try:
                        audit_log("pmr", "upload", target=pid, details=f"{nama} {bulan}; file1={n1}; file2={n2 or '-'}")
                    except Exception:
                        pass
                    try:
                        notify_review_request("pmr", title=f"{nama} — {bulan}", entity_id=pid, recipients_roles=("finance","director"))
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
            df = pd.read_sql_query("SELECT id, nama, bulan, tanggal_submit, finance_approved, director_approved, file1_name, file2_name FROM pmr ORDER BY tanggal_submit DESC", conn)
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
                def make_download_link(row, col):
                    if row[col]:
                        return f'<a href="/download_pmr/{row["id"]}/{col}" target="_blank">{row[col]}</a>'
                    return "-"
                df_month_disp['Download File 1'] = df_month_disp.apply(lambda r: make_download_link(r, 'file1_name'), axis=1)
                df_month_disp['Download File 2'] = df_month_disp.apply(lambda r: make_download_link(r, 'file2_name'), axis=1)
                show_cols = ["nama","bulan","tanggal_submit","Status","Download File 1","Download File 2"]
                st.markdown(df_month_disp[show_cols].to_html(escape=False, index=False), unsafe_allow_html=True)
        else:
            st.info("Hanya Finance/Director yang dapat melihat rekap PMR.")

 

def delegasi_module():
    user = require_login()
    st.header("🗂️ Delegasi Tugas & Monitoring")
    conn = get_db()
    cur = conn.cursor()
    tab1, tab2, tab3, tab4 = st.tabs(["🆕 Buat Tugas", "📝 Update Status/Bukti", "👀 Monitoring Director", "📅 Rekap & Filter"])

    # Tab 1: Buat Tugas
    with tab1:
        st.markdown("### 🆕 Buat Tugas Baru (Pemberi Tugas)")
        with st.form("del_add"):
            judul = st.text_input("Judul Tugas")
            deskripsi = st.text_area("Deskripsi")
            # PIC dropdown from active users
            try:
                cur.execute("SELECT id, full_name, email, role, status FROM users WHERE status='active' ORDER BY COALESCE(NULLIF(full_name,''), email)")
                _users = cur.fetchall() or []
            except Exception:
                _users = []
            pic_value = None
            if _users:
                # Build safe (label, value) options to avoid serializing Row objects
                _opts = []
                for u in _users:
                    try:
                        fn = u["full_name"] if isinstance(u, dict) else u[1]
                        em = u["email"] if isinstance(u, dict) else u[2]
                    except Exception:
                        fn = u[1] if len(u) > 1 else None
                        em = u[2] if len(u) > 2 else None
                    label = f"{fn} ({em})" if fn else (em or "-")
                    value = fn or em or ""
                    if value:
                        _opts.append((label, value))
                labels = [o[0] for o in _opts]
                selected_label = st.selectbox("Penanggung Jawab (PIC)", options=labels, key="del_pic_select")
                # Map back to value
                pic_value = dict(_opts).get(selected_label)
            else:
                st.info("Daftar user aktif kosong, masukkan PIC secara manual.")
                pic_value = st.text_input("Penanggung Jawab (PIC)")
            tgl_mulai = st.date_input("Tgl Mulai", value=date.today())
            tgl_selesai = st.date_input("Tgl Selesai", value=date.today())
            # Holiday-aware hints
            try:
                if _is_public_holiday(tgl_mulai):
                    st.info("Catatan: Tgl Mulai jatuh pada Libur Nasional.")
                if _is_public_holiday(tgl_selesai):
                    st.warning("Peringatan: Tgl Selesai jatuh pada Libur Nasional.")
            except Exception:
                pass
            if st.form_submit_button("Buat Tugas"):
                if not (judul and deskripsi and pic_value):
                    st.warning("Semua field wajib diisi.")
                elif tgl_selesai < tgl_mulai:
                    st.warning("Tanggal selesai tidak boleh sebelum mulai.")
                else:
                    # Optional auto-shift deadline to next working day
                    try:
                        auto_shift = (_setting_get('delegasi_deadline_autoshift', 'false') == 'true')
                    except Exception:
                        auto_shift = False
                    adj_selesai = tgl_selesai
                    try:
                        if auto_shift and _is_public_holiday(adj_selesai):
                            adj_selesai = _next_working_day(adj_selesai)
                            st.info(f"Tgl Selesai otomatis digeser ke hari kerja berikutnya: {adj_selesai.isoformat()}")
                    except Exception:
                        pass
                    did = gen_id("del")
                    now = now_wib_iso()
                    created_by = user.get('email') or user.get('full_name')
                    try:
                        cur.execute("PRAGMA table_info(delegasi)")
                        _del_cols = {row[1] for row in cur.fetchall()}
                    except Exception:
                        _del_cols = set()
                    if {"created_by","review_status"}.issubset(_del_cols):
                        cur.execute("INSERT INTO delegasi (id,judul,deskripsi,pic,tgl_mulai,tgl_selesai,status,tanggal_update,created_by,review_status) VALUES (?,?,?,?,?,?,?,?,?,?)",
                                    (did, judul, deskripsi, pic_value, tgl_mulai.isoformat(), adj_selesai.isoformat(), "Belum Selesai", now, created_by, "Pending"))
                    else:
                        cur.execute("INSERT INTO delegasi (id,judul,deskripsi,pic,tgl_mulai,tgl_selesai,status,tanggal_update) VALUES (?,?,?,?,?,?,?,?)",
                                    (did, judul, deskripsi, pic_value, tgl_mulai.isoformat(), adj_selesai.isoformat(), "Belum Selesai", now))
                    conn.commit()
                    try:
                        audit_log("delegasi", "create", target=did, details=f"{judul} -> {pic_value} {tgl_mulai}..{adj_selesai}")
                    except Exception:
                        pass
                    try:
                        notify_review_request("delegasi", title=f"{judul} → {pic_value}", entity_id=did, recipients_roles=("director",))
                    except Exception:
                        pass
                    st.success("Tugas berhasil dibuat.")

    # Tab 2: Update Status & Upload Bukti (PIC)
    with tab2:
        st.markdown("### 📝 PIC Update Status & Upload Bukti")
        # Match tasks where PIC equals current user's full_name or email (fallback)
        _u_name = user.get("full_name") if isinstance(user, dict) else None
        _u_mail = user.get("email") if isinstance(user, dict) else None
        if _u_name and _u_mail:
            _sql = "SELECT * FROM delegasi WHERE pic IN (?, ?) ORDER BY tgl_selesai ASC"
            tugas_pic = pd.read_sql_query(_sql, conn, params=(_u_name, _u_mail))
        elif _u_name or _u_mail:
            _val = _u_name or _u_mail
            _sql = "SELECT * FROM delegasi WHERE pic=? ORDER BY tgl_selesai ASC"
            tugas_pic = pd.read_sql_query(_sql, conn, params=(_val,))
        else:
            tugas_pic = pd.read_sql_query("SELECT * FROM delegasi ORDER BY tgl_selesai ASC", conn)
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
                        now = now_wib_iso()
                        if status == "Selesai":
                            cur.execute("UPDATE delegasi SET status=?, file_blob=?, file_name=?, tanggal_update=? WHERE id=?",
                                (status, blob, fname, now, row["id"]))
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

                # Approval/Reject by Pemberi Tugas (creator)
                # fetch creator lazily
                try:
                    _created_by = row['created_by'] if 'created_by' in row.index else None
                except Exception:
                    _created_by = None
                if not _created_by:
                    r2 = cur.execute("SELECT created_by FROM delegasi WHERE id=?", (row['id'],)).fetchone()
                    _created_by = r2[0] if r2 and len(r2) > 0 else None
                if _created_by and (user.get('email') == _created_by or user.get('full_name') == _created_by):
                    st.markdown("---")
                    st.write("Approval oleh Pemberi Tugas")
                    note = st.text_area("Catatan (wajib saat reject)", key=f"rev_note_{row['id']}")
                    col1, col2 = st.columns(2)
                    with col1:
                        approve_btn = st.button("Approve", key=f"approve_{row['id']}")
                    with col2:
                        reject_btn = st.button("Reject", key=f"reject_{row['id']}")
                    if approve_btn or reject_btn:
                        now = now_wib_iso()
                        reviewer = user.get('email') or user.get('full_name')
                        new_stat = "Approved" if approve_btn else "Rejected"
                        if new_stat == "Rejected" and not (note and note.strip()):
                            st.error("Catatan wajib saat reject.")
                        else:
                            try:
                                cur.execute("PRAGMA table_info(delegasi)")
                                _del_cols = {row[1] for row in cur.fetchall()}
                            except Exception:
                                _del_cols = set()
                            if {"review_status","review_note","review_time","reviewed_by"}.issubset(_del_cols):
                                cur.execute("UPDATE delegasi SET review_status=?, review_note=?, review_time=?, reviewed_by=? WHERE id=?",
                                            (new_stat, note or '', now, reviewer, row['id']))
                            else:
                                cur.execute("UPDATE delegasi SET status=?, tanggal_update=? WHERE id=?",
                                            (row['status'], now, row['id']))
                            conn.commit()
                            try:
                                audit_log('delegasi','review', target=row['id'], details=f"{new_stat}; note={note or ''}", actor=reviewer)
                            except Exception:
                                pass
                            st.success(f"Tugas di-{new_stat.lower()} oleh pemberi tugas.")
                            if new_stat == "Rejected":
                                # Notify PIC to rework/upload again
                                try:
                                    pic_name = row['pic']
                                    recips = []
                                    if pic_name and '@' in str(pic_name):
                                        recips = [pic_name]
                                    else:
                                        em = _get_user_email_by_name(str(pic_name))
                                        if em:
                                            recips = [em]
                                    if recips:
                                        _send_email(recips, f"[WIJNA] Delegasi ditolak: {row['judul']}", f"Delegasi '{row['judul']}' ditolak.\nCatatan: {note or '-'}\nSilakan perbaiki dan upload bukti kembali.")
                                except Exception:
                                    pass

    # Tab 3: Monitoring Director
    with tab3:
        st.markdown("### 👀 Monitoring Director")
        if user["role"] in ["director", "superuser"]:
            df_all = pd.read_sql_query("SELECT id,judul,deskripsi,pic,tgl_mulai,tgl_selesai,status,file_name,tanggal_update FROM delegasi ORDER BY tgl_selesai ASC", conn)
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
                        st.download_button("Download Bukti", data=row["file_blob"], file_name=row["file_name"])
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
            # Robust Excel export with fallback engines (openpyxl -> xlsxwriter) else CSV only
            try:
                bio = io.BytesIO()
                success = False
                for eng in ["openpyxl", "xlsxwriter"]:
                    if success:
                        break
                    try:
                        df_month.to_excel(bio, index=False, engine=eng)
                        bio.seek(0)
                        st.download_button("Download Rekap Bulanan (Excel)", bio, file_name=f"rekap_delegasi_{this_month}.xlsx")
                        success = True
                    except Exception:
                        bio = io.BytesIO()
                        continue
                if not success:
                    st.info("Engine Excel (openpyxl/xlsxwriter) tidak tersedia. Export Excel dinonaktifkan.")
            except Exception:
                st.warning("Gagal membuat file Excel.")
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
        "📝 Input Staff",
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
                    # Notify Finance + Director new request
                    try:
                        notify_review_request("flex", title=f"{nama} • {tanggal} {jam_mulai}-{jam_selesai}", entity_id=fid, recipients_roles=("finance","director"))
                    except Exception:
                        pass
                    st.success("Flex time diajukan.")

    # --- Tab 2: Review Finance ---
    with tabs[1]:
        st.subheader(":money_with_wings: Review Finance")
        allowed_finance = user["role"] in ["finance", "superuser"]
        df_fin = pd.read_sql_query("SELECT * FROM flex WHERE approval_finance=0 ORDER BY tanggal DESC", conn)
        if df_fin.empty:
            st.info("Tidak ada pengajuan flex time yang perlu direview.")
        else:
            for idx, row in df_fin.iterrows():
                with st.expander(f"{row['nama']} | {row['tanggal']} | {row['jam_mulai']} - {row['jam_selesai']}"):
                    st.write(f"Alasan: {row['alasan']}")
                    catatan = st.text_area(
                        "Catatan Finance",
                        value=row['catatan_finance'] or "",
                        key=f"catatan_fin_{row['id']}",
                        disabled=not allowed_finance,
                    )
                    if allowed_finance:
                        approve = st.button("Approve", key=f"approve_fin_{row['id']}")
                        reject = st.button("Tolak", key=f"reject_fin_{row['id']}")
                        if approve or reject:
                            cur.execute("UPDATE flex SET catatan_finance=?, approval_finance=? WHERE id=?", (catatan, 1 if approve else -1, row['id']))
                            conn.commit()
                            try:
                                audit_log("flex", "finance_review", target=row['id'], details=f"approve={1 if approve else 0}; note={catatan}")
                            except Exception:
                                pass
                            # Notify Director + applicant
                            try:
                                applicant_email = _get_user_email_by_name(row['nama'])
                                decision = "finance_approved" if approve else "finance_rejected"
                                notify_decision("flex", title=f"{row['nama']} • {row['tanggal']} {row['jam_mulai']}-{row['jam_selesai']}", decision=decision,
                                                entity_id=row['id'], recipients_roles=("director",),
                                                recipients_users=[applicant_email] if applicant_email else None, tag_suffix="finance")
                            except Exception:
                                pass
                            st.success("Status review finance diperbarui.")
                            st.rerun()
                    else:
                        st.info("Hanya Finance/Superuser yang dapat melakukan review di tab ini.")

    # --- Tab 3: Approval Director ---
    with tabs[2]:
        st.subheader("👨‍💼 Approval Director")
        allowed_dir = user["role"] in ["director", "superuser"]
        df_dir = pd.read_sql_query("SELECT * FROM flex WHERE approval_finance=1 AND approval_director=0 ORDER BY tanggal DESC", conn)
        if df_dir.empty:
            st.info("Tidak ada pengajuan flex time yang menunggu approval director.")
        else:
            for idx, row in df_dir.iterrows():
                with st.expander(f"{row['nama']} | {row['tanggal']} | {row['jam_mulai']} - {row['jam_selesai']}"):
                    st.write(f"Alasan: {row['alasan']}")
                    st.write(f"Catatan Finance: {row['catatan_finance']}")
                    catatan = st.text_area(
                        "Catatan Director",
                        value=row['catatan_director'] or "",
                        key=f"catatan_dir_{row['id']}",
                        disabled=not allowed_dir,
                    )
                    if allowed_dir:
                        approve = st.button("Approve", key=f"approve_dir_{row['id']}")
                        reject = st.button("Tolak", key=f"reject_dir_{row['id']}")
                        if approve or reject:
                            cur.execute("UPDATE flex SET catatan_director=?, approval_director=? WHERE id=?", (catatan, 1 if approve else -1, row['id']))
                            conn.commit()
                            try:
                                audit_log("flex", "director_approval", target=row['id'], details=f"approve={1 if approve else 0}; note={catatan}")
                            except Exception:
                                pass
                            # Notify applicant + Finance
                            try:
                                applicant_email = _get_user_email_by_name(row['nama'])
                                decision = "director_approved" if approve else "director_rejected"
                                notify_decision("flex", title=f"{row['nama']} • {row['tanggal']} {row['jam_mulai']}-{row['jam_selesai']}", decision=decision,
                                                entity_id=row['id'], recipients_roles=("finance",),
                                                recipients_users=[applicant_email] if applicant_email else None, tag_suffix="director")
                            except Exception:
                                pass
                            st.success("Status approval director diperbarui.")
                            st.rerun()
                    else:
                        st.info("Hanya Director/Superuser yang dapat memberikan approval di tab ini.")

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
            st.dataframe(df[['nama','tanggal','jam_mulai','jam_selesai','alasan','catatan_finance','catatan_director','status']], width='stretch')
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
                        st.rerun()

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
                    now = now_wib_iso()
                    cur.execute("INSERT INTO calendar (id,jenis,judul,nama_divisi,tgl_mulai,tgl_selesai,deskripsi,file_blob,file_name,is_holiday,sumber,ditetapkan_oleh,tanggal_penetapan) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (cid, "Libur Nasional", judul, "-", tgl_mulai.isoformat(), tgl_selesai.isoformat(), sumber, None, None, 1, sumber, user["full_name"], now))
                    cur.execute("INSERT INTO public_holidays (tahun,tanggal,nama,keterangan,ditetapkan_oleh,tanggal_penetapan) VALUES (?,?,?,?,?,?)",
                        (tgl_mulai.year, tgl_mulai.isoformat(), judul, sumber or "", user["full_name"], now))
                    conn.commit()
                    try:
                        audit_log("calendar", "add_holiday", target=cid, details=f"{judul} {tgl_mulai}..{tgl_selesai}")
                    except Exception:
                        pass
                    # Notify all staff about new public holiday
                    try:
                        if _email_enabled():
                            all_staff = _get_all_active_emails()
                            if all_staff:
                                tag = f"holiday:{cid}:{tgl_mulai.isoformat()}-{tgl_selesai.isoformat()}"
                                if not _notif_already_sent('calendar', cid, 'new_holiday', tag):
                                    subj = f"[WIJNA] Libur Nasional ditetapkan: {judul}"
                                    body = (
                                        f"Libur Nasional baru telah ditambahkan.\n"
                                        f"Nama: {judul}\n"
                                        f"Tanggal: {tgl_mulai.isoformat()} s/d {tgl_selesai.isoformat()}\n"
                                        f"Sumber: {sumber or '-'}\n"
                                        f"Ditetapkan oleh: {user.get('full_name','-')} pada {format_datetime_wib(now)}\n"
                                    )
                                    if _send_email(all_staff, subj, body):
                                        _mark_notif_sent('calendar', cid, 'new_holiday', tag, all_staff)
                    except Exception:
                        pass
                    st.success("Libur Nasional ditambahkan.")
        else:
            st.info("Hanya Director yang bisa menambah Libur Nasional.")

    # Tab 2: Kalender Gabungan & Rekap
    with tab2:
        # Use raw sqlite connection for pandas compatibility
        raw_conn = conn._conn if hasattr(conn, "_conn") else conn
        # --- AUTO INTEGRASI EVENT ---
        df_cuti = pd.read_sql_query("SELECT nama as judul, 'Cuti' as jenis, nama as nama_divisi, tgl_mulai, tgl_selesai FROM cuti WHERE director_approved=1", raw_conn)
        # Flex table uses approval_director (not director_approved). Fallback if column missing.
        try:
            df_flex = pd.read_sql_query("SELECT nama as judul, 'Flex Time' as jenis, nama as nama_divisi, tanggal as tgl_mulai, tanggal as tgl_selesai FROM flex WHERE approval_director=1", raw_conn)
        except Exception:
            # fallback without filter
            try:
                df_flex = pd.read_sql_query("SELECT nama as judul, 'Flex Time' as jenis, nama as nama_divisi, tanggal as tgl_mulai, tanggal as tgl_selesai FROM flex", raw_conn)
            except Exception:
                df_flex = pd.DataFrame(columns=["judul","jenis","nama_divisi","tgl_mulai","tgl_selesai"])
        df_delegasi = pd.read_sql_query("SELECT judul, 'Delegasi' as jenis, pic as nama_divisi, tgl_mulai, tgl_selesai FROM delegasi", raw_conn)
        df_rapat = pd.read_sql_query("SELECT judul, jenis, nama_divisi, tgl_mulai, tgl_selesai FROM calendar WHERE jenis='Rapat'", raw_conn)
        df_mobil = pd.read_sql_query("SELECT tujuan as judul, 'Mobil Kantor' as jenis, kendaraan as nama_divisi, tgl_mulai, tgl_selesai, kendaraan FROM mobil WHERE status='Disetujui'", raw_conn)
        df_libur = pd.read_sql_query("SELECT judul, jenis, nama_divisi, tgl_mulai, tgl_selesai FROM calendar WHERE is_holiday=1", raw_conn)

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
                st.dataframe(dff[show_cols], width='stretch')
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

    tab_upload, tab_list, tab_rekap, tab_board = st.tabs(["🆕 Upload Notulen", "📋 Daftar Notulen", "📅 Rekap Bulanan Notulen", "👥 Review Board"]) 

    # --- Tab 1: Upload ---
    with tab_upload:
        st.subheader("🆕 Upload Notulen (staff upload, Director approve final)")
        with st.form("not_add", clear_on_submit=True):
            judul = st.text_input("Judul Rapat")
            if nt_date_col == "tanggal_rapat":
                tgl = st.date_input("Tanggal Rapat", value=date.today())
            else:
                tgl = None
                st.caption("Tanggal upload akan dicatat otomatis.")
            f = st.file_uploader("File Notulen")
            follow_up = st.text_area("Catatan Follow Up (opsional)") if "follow_up" in nt_cols else None
            deadline = st.date_input("Deadline / Tindak Lanjut", value=date.today()) if "deadline" in nt_cols else None
            submit = st.form_submit_button("💾 Upload Notulen")
            if submit:
                if not judul or not f:
                    st.warning("Judul dan file wajib diisi.")
                else:
                    nid = gen_id("not")
                    blob, fname, _ = upload_file_and_store(f)
                    cols = ["id", "judul", "file_blob", "file_name"]
                    vals = [nid, judul, blob, fname]
                    if nt_date_col == "tanggal_rapat":
                        cols.append("tanggal_rapat"); vals.append(tgl.isoformat())
                    elif nt_date_col == "tanggal_upload":
                        cols.append("tanggal_upload"); vals.append(now_wib_iso())
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
                    try:
                        notify_review_request("notulen", title=judul, entity_id=nid, recipients_roles=("director",))
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
            st.dataframe(show[cols_show], width='stretch')

            # Download file terpilih
            if "id" in show.columns and "file_name" in show.columns:
                opsi = {f"{r['judul']} — {r.get(nt_date_col, '')}" + (f" ({r['file_name']})" if r.get('file_name') else ""): r['id'] for _, r in show.iterrows()}
                if opsi:
                    pilih = st.selectbox("Pilih notulen untuk diunduh", [""] + list(opsi.keys()))
                    if pilih:
                        nid = opsi[pilih]
                        row = pd.read_sql_query("SELECT file_blob, file_name FROM notulen WHERE id=?", conn, params=(nid,)).iloc[0]
                        if row["file_blob"] is not None and row["file_name"]:
                            show_file_download(row["file_blob"], row["file_name"])

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
                            rr = pd.read_sql_query("SELECT file_blob, file_name FROM notulen WHERE id=?", conn, params=(r['id'],))
                            if not rr.empty and rr.iloc[0]["file_blob"] is not None and rr.iloc[0]["file_name"]:
                                show_file_download(rr.iloc[0]["file_blob"], rr.iloc[0]["file_name"])
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
    
    # --- Tab 4: Review Board (Opsional) ---
    with tab_board:
        if user["role"] in ["board", "superuser"]:
            try:
                df_nb = pd.read_sql_query("SELECT id, judul, " + (nt_date_col if nt_date_col else "'') AS tanggal") + ", board_note FROM notulen ORDER BY " + (nt_date_col if nt_date_col else "id") + " DESC", conn)
            except Exception:
                try:
                    df_nb = pd.read_sql_query("SELECT id, judul, board_note FROM notulen ORDER BY id DESC", conn)
                except Exception:
                    df_nb = pd.DataFrame()
            if df_nb.empty:
                st.info("Belum ada notulen untuk direview.")
            else:
                for _, row in df_nb.iterrows():
                    st.markdown(f"**{row['judul']}**")
                    cur_note = row.get('board_note') or ""
                    note = st.text_area("Catatan Board", value=cur_note, key=f"notulen_board_note_{row['id']}")
                    if st.button("Simpan Catatan", key=f"notulen_board_save_{row['id']}"):
                        try:
                            cur.execute("UPDATE notulen SET board_note=? WHERE id=?", (note, row['id']))
                            conn.commit()
                            st.success("Catatan Board disimpan.")
                            try:
                                audit_log("notulen", "board_review", target=row['id'], details=f"note={note}")
                            except Exception:
                                pass
                        except Exception as e:
                            st.error(f"Gagal simpan: {e}")
        else:
            st.info("Hanya Board yang dapat review di sini.")
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
                st.dataframe(df_month[["judul", nt_date_col]], width='stretch')
            else:
                st.dataframe(df_month[["judul"]], width='stretch')
            # CSV export removed as requested

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

    tab_profile, tab_admin, tab1, tab2 = st.tabs(["👤 Profil Saya", "🔐 Admin (Director)","🕒 User Baru Menunggu Approval", "👥 Semua User"])

    # --- Tab 1: Profil Saya ---
    with tab_profile:
        st.subheader("Ubah Identitas (Email & Nama)")
        with st.form("change_identity_form"):
            new_email = st.text_input("Email", value=me["email"] or "").strip()
            new_name = st.text_input("Nama Lengkap", value=me["full_name"] or "").strip()
            save_identity = st.form_submit_button("Simpan Profil")
            if save_identity:
                # Validasi sederhana
                if not new_email or "@" not in new_email:
                    st.warning("Masukkan email yang valid.")
                elif not new_name:
                    st.warning("Nama tidak boleh kosong.")
                else:
                    try:
                        # Cek duplikasi email bila berubah
                        if (new_email.lower() != (me["email"] or "").lower()):
                            cur.execute("SELECT 1 FROM users WHERE lower(email)=lower(?) AND id<>?", (new_email, me["id"]))
                            if cur.fetchone():
                                st.error("Email sudah digunakan oleh akun lain.")
                                raise RuntimeError("email_in_use")
                        # Lakukan update
                        cur.execute("UPDATE users SET email=?, full_name=? WHERE id=?", (new_email, new_name, me["id"]))
                        conn.commit()
                        # Update session
                        st.session_state["user"]["email"] = new_email
                        st.session_state["user"]["full_name"] = new_name
                        try:
                            old_email = me["email"]
                            old_name = me["full_name"]
                            details = f"email: {old_email} → {new_email}; nama: {old_name} → {new_name}"
                            audit_log("user_setting", "update_identity", target=old_email or new_email, details=details)
                        except Exception:
                            pass
                        st.success("Identitas berhasil diperbarui.")
                    except RuntimeError:
                        pass

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
        # Gunakan role hierarchy helper: superuser otomatis lolos
        if not has_min_role("director"):
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
                st.dataframe(dfu, width='stretch', hide_index=True)

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

            # Email notification settings (Director & Superuser) in expanders
            st.markdown("---")
            with st.expander("Email Notifikasi (Dunyim) — Director/Superuser", expanded=False):
                enabled_global = (_setting_get('enable_email_notifications','false') == 'true')
                enabled_pmr = (_setting_get('pmr_notify_enabled','true') == 'true')
                enabled_delegasi = (_setting_get('delegasi_notify_enabled','true') == 'true')
                autoshift = (_setting_get('delegasi_deadline_autoshift','false') == 'true')
                colA, colB = st.columns(2)
                with colA:
                    ng = st.toggle("Aktifkan notifikasi email", value=enabled_global)
                with colB:
                    st.caption("Kredensial di secrets: [email_credentials] username/app_password")
                colC, colD = st.columns(2)
                with colC:
                    np = st.toggle("PMR: Tegur otomatis jika terlambat (> tgl 5)", value=enabled_pmr)
                with colD:
                    nd = st.toggle("Delegasi: Pengingat ≤3 hari & Lewat tenggat", value=enabled_delegasi)
                colE, colF = st.columns(2)
                with colE:
                    na = st.toggle("Delegasi: Auto-shift deadline jika jatuh pada Libur Nasional", value=autoshift)
                with colF:
                    st.caption("Jika aktif, sistem akan memundurkan tenggat ke hari kerja berikutnya.")

                st.markdown("---")
                st.markdown("#### Toggle Notifikasi per Event")
                st.caption("Aktif/nonaktifkan pengiriman email untuk tiap modul dan tahap event.")
                modules = [
                    ("inventory", "Inventory"),
                    ("cuti", "Cuti"),
                    ("cash_advance", "Cash Advance"),
                    ("flex", "Flex Time"),
                    ("mou", "MoU"),
                ]
                events = [
                    ("request", "Permintaan Review (Submit)"),
                    ("finance_decision", "Keputusan Finance"),
                    ("director_decision", "Keputusan Director"),
                    ("board_decision", "Keputusan Board (khusus MoU/SOP/Notulen)"),
                ]
                # Render a grid of toggles
                for ent_key, ent_label in modules:
                    st.markdown(f"**{ent_label}**")
                    c1, c2, c3, c4 = st.columns(4)
                    cols = [c1, c2, c3, c4]
                    vals = []
                    for idx, (evt_key, evt_label) in enumerate(events):
                        with cols[idx]:
                            cur_val = _notif_toggle_enabled(ent_key, evt_key, True)
                            vals.append(st.toggle(evt_label, value=cur_val, key=f"tgl_{ent_key}_{evt_key}"))
                    # Save for this module
                    if st.button(f"Simpan Toggle {ent_label}", key=f"save_toggles_{ent_key}"):
                        for (evt_key, _), v in zip(events, vals):
                            _setting_set(_notif_toggle_key(ent_key, evt_key), 'true' if v else 'false')
                        st.success(f"Toggle notifikasi {ent_label} disimpan.")
                if st.button("Simpan Pengaturan Email", key="save_email_notif"):
                    _setting_set('enable_email_notifications', 'true' if ng else 'false')
                    _setting_set('pmr_notify_enabled', 'true' if np else 'false')
                    _setting_set('delegasi_notify_enabled', 'true' if nd else 'false')
                    _setting_set('delegasi_deadline_autoshift', 'true' if na else 'false')
                    st.success("Pengaturan disimpan.")

            with st.expander("🔔 Kirim Email Uji Coba", expanded=False):
                st.caption("Gunakan ini untuk menguji apakah konfigurasi SMTP dan pengiriman email berjalan.")
                with st.form("test_email_form"):
                    default_to = me["email"] or ""
                    test_to = st.text_input("Kirim ke (email)", value=default_to)
                    subject = st.text_input("Subjek", value="[WIJNA] Test Notifikasi Email")
                    body = st.text_area("Isi Pesan", value="Ini adalah email uji dari WIJNA Manajemen System.")
                    submit_test = st.form_submit_button("Kirim Email Tes")
                    if submit_test:
                        if not test_to or "@" not in test_to:
                            st.warning("Masukkan alamat email tujuan yang valid.")
                        else:
                            ok = False
                            try:
                                ok = _send_email([test_to], subject.strip(), body)
                            except Exception:
                                ok = False
                            if ok:
                                st.success(f"Email uji berhasil dikirim ke {test_to}.")
                            else:
                                st.error("Gagal mengirim email uji. Periksa kembali kredensial di secrets dan koneksi internet.")
                                usern, apppw = _smtp_settings()
                                if not usern or not apppw:
                                    st.info("Hint: Pastikan secrets.email_credentials.username dan app_password terisi.")

            # Aksi User Management moved inside Admin tab
            st.markdown("---")
            st.subheader("Aksi User Management")
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
                newrole = st.selectbox("Pilih role baru", ["staff","board","finance","director","superuser"])
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
    
    with tab1:
        st.subheader("User baru menunggu approval")
        if not has_min_role("director, superuser"):
            st.info("Hanya Director/Superuser yang dapat mengakses menu ini.")
            return
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
                        role_options = ["staff","board","finance","director","superuser"]
                        # Normalize existing role to options
                        current_role = p['role'] if p['role'] in role_options else "staff"
                        new_role = st.selectbox("Set role", role_options, index=role_options.index(current_role), key=f"role_{p['id']}_card_{idx}")
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
    
    # (Aksi User Management dipindahkan ke dalam tab Admin)

    with tab2:
        st.subheader("Semua user")
        if not has_min_role("director, superuser"):
            st.info("Hanya Director/Superuser yang dapat mengakses menu ini.")
            return
        conn2 = get_db()
        df2 = pd.read_sql_query("SELECT id,email,full_name,role,status,last_login,created_at FROM users", conn2)
        st.dataframe(df2, width='stretch')
        conn2.close()
    # (No form here, only one form with key 'admin_change' exists below)

    
# -------------------------
# Dashboard
# -------------------------
def dashboard():
    user = require_login()
    st.title("🏠 Dashboard WIJNA")
    conn = get_db()
    cur = conn.cursor()
    # Raw connection (hindari warning pandas karena wrapper _AuditConnection)
    raw_conn = conn._conn if hasattr(conn, '_conn') else conn

    # Run lightweight automations (email notifications) once per session render
    try:
        run_automations_for_dashboard()
    except Exception:
        pass


    # --------------------------------------------------
    # UTIL: CSS & helper
    # --------------------------------------------------
    st.markdown("""
    <style>
    .dash-grid {margin-bottom:1rem;}
    .stat-card {background:#ffffff;border-radius:16px;padding:1.1rem 1.2rem;box-shadow:0 2px 6px rgba(0,0,0,0.06);position:relative;overflow:hidden;}
    .stat-card:before {content:"";position:absolute;right:-30px;top:-30px;width:120px;height:120px;background:linear-gradient(135deg,#e0f2fe,#bfdbfe);border-radius:50%;opacity:.35;}
    .stat-label {font-size:.85rem;letter-spacing:.5px;text-transform:uppercase;color:#64748b;font-weight:600;}
    .stat-value {font-size:2.1rem;font-weight:700;margin-top:.15rem;margin-bottom:.25rem;line-height:1;}
    .stat-foot {font-size:.75rem;color:#94a3b8;font-weight:500;}
    .wijna-section-card {background:#f8fafc;border:1px solid #e2e8f0;border-radius:14px;padding:1.05rem 1.1rem 1rem 1.1rem;margin-bottom:1.05rem;}
    .wijna-section-title {font-size:1.05rem;font-weight:650;color:#1d4ed8;margin:0 0 .35rem 0;display:flex;align-items:center;gap:.45rem;}
    .wijna-section-desc {color:#64748b;font-size:.78rem;margin:-.15rem 0 .55rem 0;}
    .mini-table thead tr th {background:#eff6ff!important;color:#1e3a8a!important;font-size:.7rem!important;}
    .mini-table tbody tr td {font-size:.72rem!important;padding:.25rem .4rem!important;}
    .empty-hint {font-size:.7rem;color:#94a3b8;font-style:italic;}
    </style>
    """, unsafe_allow_html=True)

    # --------------------------------------------------
    # TOP METRICS
    # --------------------------------------------------
    services = [
        ("inventory","finance_approved=0 OR director_approved=0"),
        ("cash_advance","finance_approved=0 OR director_approved=0"),
        ("pmr","finance_approved=0 OR director_approved=0"),
        ("cuti","finance_approved=0 OR director_approved=0"),
        ("surat_keluar","director_approved=0"),
        ("mou","director_approved=0"),
        ("sop","director_approved=0"),
        ("notulen","director_approved=0"),
        ("flex","approval_finance=0 OR approval_director=0"),
    ]
    total_pending = 0
    import sqlite3
    for table, cond in services:
        try:
            cur.execute(f"SELECT COUNT(*) as c FROM {table} WHERE {cond}")
            total_pending += cur.fetchone()["c"]
        except sqlite3.OperationalError:
            continue
    cur.execute("SELECT COUNT(*) as c FROM surat_masuk WHERE status='Belum Dibahas'")
    surat_blm = cur.fetchone()["c"]
    # Use localtime so comparisons align better with WIB date
    cur.execute("SELECT COUNT(*) as c FROM mou WHERE date(tgl_selesai) <= date('now','localtime','+7 day')")
    mou_due7 = cur.fetchone()["c"]
    # Delegasi aktif (tidak selesai)
    try:
        delegasi_aktif = pd.read_sql_query(
            "SELECT COUNT(*) c FROM delegasi WHERE lower(status) NOT IN ('selesai','done')",
            raw_conn
        ).iloc[0]['c']
    except Exception:
        delegasi_aktif = 0
    colA, colB, colC, colD = st.columns(4)
    colA.markdown(f"""<div class='stat-card'><div class='stat-label'>Approval Pending</div><div class='stat-value' style='color:#f97316'>{total_pending}</div><div class='stat-foot'>Perlu tindakan</div></div>""", unsafe_allow_html=True)
    colB.markdown(f"""<div class='stat-card'><div class='stat-label'>Surat Belum Dibahas</div><div class='stat-value' style='color:#2563eb'>{surat_blm}</div><div class='stat-foot'>Status awal</div></div>""", unsafe_allow_html=True)
    colC.markdown(f"""<div class='stat-card'><div class='stat-label'>MoU ≤ 7 Hari</div><div class='stat-value' style='color:#9333ea'>{mou_due7}</div><div class='stat-foot'>Segera follow-up</div></div>""", unsafe_allow_html=True)
    colD.markdown(f"""<div class='stat-card'><div class='stat-label'>Delegasi Aktif</div><div class='stat-value' style='color:#059669'>{delegasi_aktif}</div><div class='stat-foot'>Belum selesai</div></div>""", unsafe_allow_html=True)


    # --------------------------------------------------
    # ROW 1: Approval | Status Surat+MoU | Rekap Multi Modul
    # --------------------------------------------------
    c1, c2, c3 = st.columns([1.05, 1.05, 1.1])
    with c1:
        st.markdown("<div class='wijna-section-card'>", unsafe_allow_html=True)
        st.markdown("<div class='wijna-section-title'>🛎️ Approval Menunggu</div>", unsafe_allow_html=True)
        st.markdown("<div class='wijna-section-desc'>Limit 5 per modul / ringkas.</div>", unsafe_allow_html=True)
        pending_specs = [
            ("inventory", "SELECT id,name as info,status FROM inventory WHERE (finance_approved=0 OR director_approved=0) LIMIT 5"),
            ("cash_advance", "SELECT id,divisi as info, totals as status FROM cash_advance WHERE (finance_approved=0 OR director_approved=0) LIMIT 5"),
            ("pmr", "SELECT id,nama as info, bulan as status FROM pmr WHERE (finance_approved=0 OR director_approved=0) LIMIT 5"),
            ("cuti", "SELECT id,nama as info,status FROM cuti WHERE director_approved=0 LIMIT 5"),
            ("surat_keluar", "SELECT id,perihal as info,status FROM surat_keluar WHERE director_approved=0 LIMIT 5"),
            ("mou", "SELECT id,nama as info,tgl_selesai as status FROM mou WHERE director_approved=0 LIMIT 5"),
            ("sop", "SELECT id,judul as info,'pending' as status FROM sop WHERE director_approved=0 LIMIT 5"),
            ("notulen", "SELECT id,judul as info,'pending' as status FROM notulen WHERE director_approved=0 LIMIT 5"),
            ("flex", "SELECT id,nama as info, CASE WHEN approval_finance=0 THEN 'Finance' ELSE 'Director' END as status FROM flex WHERE (approval_finance=0 OR approval_director=0) LIMIT 5"),
        ]
        rows = []
        for modul, q in pending_specs:
            try:
                dtemp = pd.read_sql_query(q, raw_conn)
                if not dtemp.empty:
                    for _, r in dtemp.iterrows():
                        rows.append({"Modul": modul, "Info": r.get("info"), "Status": r.get("status")})
            except Exception:
                continue
        if rows:
            dfp = pd.DataFrame(rows)
            st.dataframe(dfp, width='stretch', hide_index=True)
        else:
            st.markdown("<div class='empty-hint'>Tidak ada.</div>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
    with c2:
        st.markdown("<div class='wijna-section-card'>", unsafe_allow_html=True)
        st.markdown("<div class='wijna-section-title'>📄 Surat & MoU</div>", unsafe_allow_html=True)
        st.markdown("<div class='wijna-section-desc'>Surat belum dibahas & MoU ≤30 hari jatuh tempo.</div>", unsafe_allow_html=True)
        try:
            df_surat_pending = pd.read_sql_query("SELECT nomor, perihal, tanggal FROM surat_masuk WHERE status='Belum Dibahas' ORDER BY tanggal DESC LIMIT 6", raw_conn)
        except Exception:
            df_surat_pending = pd.DataFrame()
        try:
            df_mou_due = pd.read_sql_query("SELECT nomor, nama, tgl_selesai FROM mou WHERE date(tgl_selesai) BETWEEN date('now') AND date('now','+30 day') ORDER BY tgl_selesai ASC LIMIT 6", raw_conn)
        except Exception:
            df_mou_due = pd.DataFrame()
        left, right = st.columns(2)
        with left:
            st.caption("Surat Belum Dibahas")
            if df_surat_pending.empty:
                st.markdown("<div class='empty-hint'>-</div>", unsafe_allow_html=True)
            else:
                st.dataframe(df_surat_pending, width='stretch', hide_index=True)
        with right:
            st.caption("MoU Due")
            if df_mou_due.empty:
                st.markdown("<div class='empty-hint'>-</div>", unsafe_allow_html=True)
            else:
                st.dataframe(df_mou_due, width='stretch', hide_index=True)
        st.markdown("</div>", unsafe_allow_html=True)
    with c3:
        st.markdown("<div class='wijna-section-card'>", unsafe_allow_html=True)
        st.markdown("<div class='wijna-section-title'>📊 Rekap Bulanan Multi-Modul</div>", unsafe_allow_html=True)
        this_month = date.today().strftime('%Y-%m')
        rekap_rows = []
        def safe_read(query, params=None):
            try:
                return pd.read_sql_query(query, raw_conn, params=params)
            except Exception:
                return pd.DataFrame()
        df_ca = safe_read("SELECT totals, finance_approved, director_approved, tanggal FROM cash_advance WHERE substr(tanggal,1,7)=?", (this_month,))
        if not df_ca.empty:
            rekap_rows.append({"Modul":"Cash Advance","Jumlah":len(df_ca),"Selesai":len(df_ca[(df_ca.finance_approved==1)&(df_ca.director_approved==1)]),"Nominal":float(df_ca.totals.sum())})
        df_pmr = safe_read("SELECT finance_approved,director_approved, bulan FROM pmr WHERE substr(bulan,1,7)=?", (this_month,))
        if not df_pmr.empty:
            rekap_rows.append({"Modul":"PMR","Jumlah":len(df_pmr),"Selesai":len(df_pmr[(df_pmr.finance_approved==1)&(df_pmr.director_approved==1)]),"Nominal":"-"})
        inv_cnt_df = safe_read("SELECT COUNT(*) c FROM inventory WHERE substr(updated_at,1,7)=?", (this_month,))
        if not inv_cnt_df.empty:
            rekap_rows.append({"Modul":"Inventory Updated","Jumlah":int(inv_cnt_df.iloc[0]['c']),"Selesai":"-","Nominal":"-"})
        sm_df = safe_read("SELECT status,tanggal FROM surat_masuk WHERE substr(tanggal,1,7)=?", (this_month,))
        if not sm_df.empty:
            selesai_sm = (sm_df.status.str.lower()!='belum dibahas').sum()
            rekap_rows.append({"Modul":"Surat Masuk","Jumlah":len(sm_df),"Selesai":int(selesai_sm),"Nominal":"-"})
        sk_df = safe_read("SELECT status,tanggal FROM surat_keluar WHERE substr(tanggal,1,7)=?", (this_month,))
        if not sk_df.empty:
            final_cnt = (sk_df.status.str.lower()=="final").sum()
            rekap_rows.append({"Modul":"Surat Keluar","Jumlah":len(sk_df),"Selesai":int(final_cnt),"Nominal":"-"})
        mou_df = safe_read("SELECT id,tgl_mulai,tgl_selesai FROM mou WHERE substr(tgl_mulai,1,7)=? OR substr(tgl_selesai,1,7)=?", (this_month,this_month))
        if not mou_df.empty:
            rekap_rows.append({"Modul":"MoU","Jumlah":len(mou_df),"Selesai":"-","Nominal":"-"})
        if rekap_rows:
            dr = pd.DataFrame(rekap_rows)
            if 'Nominal' in dr.columns:
                dr['Nominal'] = dr['Nominal'].apply(lambda v: (f"Rp {v:,.0f}".replace(",",".")) if isinstance(v,(int,float)) else v)
            # Normalisasi kolom 'Selesai' agar tidak campur int/string (hindari ArrowTypeError)
            if 'Selesai' in dr.columns:
                dr['Selesai'] = dr['Selesai'].apply(lambda v: '-' if (isinstance(v,str) and v.strip()== '-') else str(v))
            st.dataframe(dr, width='stretch', hide_index=True)
        else:
            st.markdown("<div class='empty-hint'>Tidak ada data bulan ini.</div>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    # --------------------------------------------------
    # ROW 2: Cuti & Flex | Delegasi Aktif | Kalender 30 Hari | Rekap (Cash Advance historis)
    # (split menjadi dua baris agar tidak terlalu lebar / over-scroll)
    # --------------------------------------------------
    c4, c5 = st.columns([1.1, 1])
    with c4:
        st.markdown("<div class='wijna-section-card'>", unsafe_allow_html=True)
        st.markdown("<div class='wijna-section-title'>🌴 Cuti & ⏰ Flex</div>", unsafe_allow_html=True)
        st.markdown("<div class='wijna-section-desc'>Ringkas total cuti per nama & flex disetujui terbaru.</div>", unsafe_allow_html=True)
        try:
            df_cuti = pd.read_sql_query("SELECT nama, COUNT(*) total_pengajuan, SUM(durasi) total_durasi FROM cuti GROUP BY nama ORDER BY total_durasi DESC", raw_conn)
        except Exception:
            df_cuti = pd.DataFrame(columns=["nama","total_pengajuan","total_durasi"])
        if df_cuti.empty:
            st.markdown("<div class='empty-hint'>Belum ada data cuti.</div>", unsafe_allow_html=True)
        else:
            st.dataframe(df_cuti, width='stretch', hide_index=True)

        # Sisa Kuota per Pegawai (ambil baris terakhir per nama)
        st.caption("Sisa Kuota per Pegawai (berdasarkan pengajuan terakhir)")
        try:
            df_sisa = pd.read_sql_query(
                """
                SELECT c1.nama, c1.kuota_tahunan, c1.cuti_terpakai, c1.sisa_kuota
                FROM cuti c1
                JOIN (
                    SELECT nama, MAX(tgl_mulai) AS last_tgl
                    FROM cuti
                    GROUP BY nama
                ) last ON last.nama = c1.nama AND last.last_tgl = c1.tgl_mulai
                ORDER BY c1.sisa_kuota ASC
                """,
                raw_conn,
            )
        except Exception:
            df_sisa = pd.DataFrame(columns=["nama","kuota_tahunan","cuti_terpakai","sisa_kuota"])
        if df_sisa.empty:
            st.markdown("<div class='empty-hint'>Belum ada data sisa kuota.</div>", unsafe_allow_html=True)
        else:
            st.dataframe(df_sisa, width='stretch', hide_index=True)
        try:
            df_flex_ok = pd.read_sql_query("SELECT nama, tanggal, jam_mulai, jam_selesai FROM flex WHERE approval_finance=1 AND approval_director=1 ORDER BY tanggal DESC LIMIT 8", raw_conn)
        except Exception:
            df_flex_ok = pd.DataFrame()
        st.caption("Flex Disetujui (8 terbaru)")
        if df_flex_ok.empty:
            st.markdown("<div class='empty-hint'>-</div>", unsafe_allow_html=True)
        else:
            st.dataframe(df_flex_ok, width='stretch', hide_index=True)
        st.markdown("</div>", unsafe_allow_html=True)
    with c5:
        st.markdown("<div class='wijna-section-card'>", unsafe_allow_html=True)
        st.markdown("<div class='wijna-section-title'>🗂️ Delegasi Aktif</div>", unsafe_allow_html=True)
        st.markdown("<div class='wijna-section-desc'>10 tugas mendekati/berjalan (urut tgl selesai).</div>", unsafe_allow_html=True)
        try:
            df_del = pd.read_sql_query("SELECT judul, pic, tgl_mulai, tgl_selesai, status FROM delegasi ORDER BY tgl_selesai ASC LIMIT 10", raw_conn)
        except Exception:
            df_del = pd.DataFrame()
        if df_del.empty:
            st.markdown("<div class='empty-hint'>Tidak ada.</div>", unsafe_allow_html=True)
        else:
            st.dataframe(df_del, width='stretch', hide_index=True)
        st.markdown("</div>", unsafe_allow_html=True)

    c6, c7 = st.columns([1.1, 1])
    with c6:
        st.markdown("<div class='wijna-section-card'>", unsafe_allow_html=True)
        st.markdown("<div class='wijna-section-title'>📅 Kalender 30 Hari</div>", unsafe_allow_html=True)
        st.markdown("<div class='wijna-section-desc'>Event lintas modul (cuti, flex, delegasi, rapat, mobil, libur).</div>", unsafe_allow_html=True)
        today = date.today(); end_30 = today + timedelta(days=30)
        def safe_df(q):
            try: return pd.read_sql_query(q, raw_conn)
            except Exception: return pd.DataFrame(columns=["judul","jenis","nama_divisi","tgl_mulai","tgl_selesai"])
        df_cuti = safe_df("SELECT nama as judul, 'Cuti' as jenis, nama as nama_divisi, tgl_mulai, tgl_selesai FROM cuti WHERE director_approved=1")
        df_flex = safe_df("SELECT nama as judul, 'Flex Time' as jenis, nama as nama_divisi, tanggal as tgl_mulai, tanggal as tgl_selesai FROM flex WHERE approval_director=1")
        # Sertakan status untuk delegasi agar bisa diwarnai selesai/overdue/dll
        df_delegasi = safe_df("SELECT judul, 'Delegasi' as jenis, pic as nama_divisi, tgl_mulai, tgl_selesai, status FROM delegasi")
        # Tambahkan MoU untuk pewarnaan jatuh tempo
        df_mou = safe_df("SELECT nama as judul, 'MoU' as jenis, divisi as nama_divisi, tgl_mulai, tgl_selesai FROM mou")
        df_rapat = safe_df("SELECT judul, jenis, nama_divisi, tgl_mulai, tgl_selesai FROM calendar WHERE jenis='Rapat'")
        df_mobil = safe_df("SELECT tujuan as judul, 'Mobil Kantor' as jenis, kendaraan as nama_divisi, tgl_mulai, tgl_selesai FROM mobil WHERE status='Disetujui'")
        df_libur = safe_df("SELECT judul, jenis, nama_divisi, tgl_mulai, tgl_selesai FROM calendar WHERE is_holiday=1")
        df_all = pd.concat([df_cuti, df_flex, df_delegasi, df_mou, df_rapat, df_mobil, df_libur], ignore_index=True)
        if df_all.empty:
            st.markdown("<div class='empty-hint'>Tidak ada event.</div>", unsafe_allow_html=True)
        else:
            for c in ("tgl_mulai","tgl_selesai"):
                df_all[c] = pd.to_datetime(df_all[c])
            mask = (df_all['tgl_selesai'] >= pd.to_datetime(today)) & (df_all['tgl_mulai'] <= pd.to_datetime(end_30))
            df_30 = df_all.loc[mask].sort_values('tgl_mulai')
            if df_30.empty:
                st.markdown("<div class='empty-hint'>Tidak ada event periode ini.</div>", unsafe_allow_html=True)
            else:
                # Legend sesuai permintaan
                st.markdown(
                    """
                    <div style='font-size:.72rem;margin-bottom:.35rem;display:flex;flex-wrap:wrap;gap:.5rem 1rem;'>
                        <span><span style='display:inline-block;width:10px;height:10px;background:#ef4444;border-radius:2px;margin-right:6px'></span>Merah: overdue/kritikal</span>
                        <span><span style='display:inline-block;width:10px;height:10px;background:#facc15;border-radius:2px;margin-right:6px'></span>Kuning: ≤7 hari ke due</span>
                        <span><span style='display:inline-block;width:10px;height:10px;background:#fb923c;border-radius:2px;margin-right:6px'></span>Oranye: ≤3 hari (peringatan)</span>
                        <span><span style='display:inline-block;width:10px;height:10px;background:#3b82f6;border-radius:2px;margin-right:6px'></span>Biru: Cuti</span>
                        <span><span style='display:inline-block;width:10px;height:10px;background:#a855f7;border-radius:2px;margin-right:6px'></span>Ungu: Flex</span>
                        <span><span style='display:inline-block;width:10px;height:10px;background:#22c55e;border-radius:2px;margin-right:6px'></span>Hijau: Tugas selesai</span>
                        <span><span style='display:inline-block;width:10px;height:10px;background:#111827;border-radius:2px;margin-right:6px'></span>Abu/Hitam: Mobil kantor</span>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

                def _badge_color(row) -> str:
                    j = (row.get('jenis') or '').strip()
                    # Default gray for unspecified
                    default = '#64748b'
                    today_dt = pd.to_datetime(date.today())
                    # Delegasi: selesai -> hijau; overdue -> merah; <=3 hari -> oranye; <=7 hari -> kuning
                    if j == 'Delegasi':
                        status = str(row.get('status') or '').strip().lower()
                        if status in ('selesai','done','completed','finish','finished'):
                            return '#22c55e'  # hijau
                        try:
                            end = pd.to_datetime(row.get('tgl_selesai'))
                            if pd.isna(end):
                                return default
                            if end.date() < today_dt.date():
                                return '#ef4444'  # merah overdue
                            days = (end.date() - today_dt.date()).days
                            if days <= 3:
                                return '#fb923c'  # oranye ≤3
                            if days <= 7:
                                return '#facc15'  # kuning ≤7
                        except Exception:
                            return default
                        return default
                    # MoU: overdue -> merah; ≤7 hari -> kuning
                    if j == 'MoU':
                        try:
                            end = pd.to_datetime(row.get('tgl_selesai'))
                            if pd.isna(end):
                                return default
                            if end.date() < today_dt.date():
                                return '#ef4444'
                            days = (end.date() - today_dt.date()).days
                            if days <= 7:
                                return '#facc15'
                        except Exception:
                            return default
                        return default
                    # Cuti: biru
                    if j == 'Cuti':
                        return '#3b82f6'
                    # Flex: ungu
                    if j == 'Flex Time':
                        return '#a855f7'
                    # Mobil kantor: abu-abu/hitam
                    if j == 'Mobil Kantor':
                        return '#111827'
                    # Rapat/Libur dan lainnya
                    return default

                st.markdown("<ul style='padding-left:1.05em;margin:0;'>", unsafe_allow_html=True)
                for _, r in df_30.iterrows():
                    t1 = r['tgl_mulai'].strftime('%d - %m - %Y')
                    t2 = r['tgl_selesai'].strftime('%d - %m - %Y')
                    rng = t1 if t1==t2 else f"{t1} / {t2}"
                    badge_color = _badge_color(r)
                    badge = f"<span style='background:{badge_color};color:#fff;padding:2px 8px;border-radius:6px;font-size:.63rem'>{r['jenis']}</span>"
                    st.markdown(f"<li style='margin-bottom:2px;font-size:.72rem'><b>{r['judul']}</b> {badge} <span style='color:#2563eb'>({rng})</span></li>", unsafe_allow_html=True)
                st.markdown("</ul>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
    with c7:
        st.markdown("<div class='wijna-section-card'>", unsafe_allow_html=True)
        st.markdown("<div class='wijna-section-title'>🧾 Rekap Cash Advance (Histori)</div>", unsafe_allow_html=True)
        st.markdown("<div class='wijna-section-desc'>Sumber tabel rekap_monthly_cashadvance (maks 12 terakhir).</div>", unsafe_allow_html=True)
        try:
            df_hist = pd.read_sql_query("SELECT * FROM rekap_monthly_cashadvance ORDER BY bulan DESC LIMIT 12", raw_conn)
        except Exception:
            df_hist = pd.DataFrame()
        if df_hist.empty:
            st.markdown("<div class='empty-hint'>Belum ada data / belum digenerate.</div>", unsafe_allow_html=True)
        else:
            for col in ["total_nominal","total_nominal_cair"]:
                if col in df_hist.columns:
                    df_hist[col] = df_hist[col].apply(lambda v: f"Rp {v:,.0f}".replace(",","."))
            st.dataframe(df_hist, width='stretch', hide_index=True)
        st.markdown("</div>", unsafe_allow_html=True)

def audit_trail_module():
    user = require_login()
    st.header("🕵️ Audit Trail / Log Aktivitas")
    conn = get_db()
    cur = conn.cursor()
    # Simple filters for Activity view (audit_logs)
    st.markdown("#### Activity")
    c1, c2 = st.columns(2)
    with c1:
        date_min = st.date_input("Dari tanggal", value=date.today() - timedelta(days=7))
    with c2:
        date_max = st.date_input("Sampai tanggal", value=date.today())
    q = st.text_input("Cari (Nama/Action/Detail)", "")

    # Load from audit_logs with basic range filter
    try:
        params: List = []
        # Join to users to show Full Name before email in Nama User column
        query = (
            "SELECT "
            "CASE WHEN u.full_name IS NOT NULL AND TRIM(u.full_name) <> '' "
            "THEN u.full_name || ' (' || a.user_email || ')' "
            "ELSE a.user_email END AS nama_user, "
            "a.timestamp AS tanggal, a.action, a.details "
            "FROM audit_logs a "
            "LEFT JOIN users u ON lower(u.email) = lower(a.user_email) "
            "WHERE 1=1"
        )
        if date_min:
            query += " AND date(a.timestamp) >= date(?)"
            params.append(date_min.isoformat())
        if date_max:
            query += " AND date(a.timestamp) <= date(?)"
            params.append(date_max.isoformat())
        query += " ORDER BY a.timestamp DESC"
        df = pd.read_sql_query(query, conn, params=params)
    except Exception:
        df = pd.DataFrame(columns=["nama_user","tanggal","action","details"])

    # Quick search filter
    if q and not df.empty:
        ql = q.lower()
        def _has(x):
            try:
                return ql in str(x).lower()
            except Exception:
                return False
        mask = df.apply(lambda r: _has(r.get("nama_user")) or _has(r.get("action")) or _has(r.get("details")), axis=1)
        df = df[mask]

    # Present concise columns with nicer headers
    if not df.empty:
        df_present = df.rename(columns={
            "nama_user": "Nama User",
            "tanggal": "Date",
            "action": "Action",
            "details": "Detail",
        })[["Nama User","Date","Action","Detail"]]
        st.dataframe(df_present, use_container_width=True)
    else:
        st.info("Belum ada aktivitas.")


# -------------------------
# Main app flow
# -------------------------
def main():
    ensure_db()
    # --- Sidebar Logo ---
    # Pre-login auto-restore: run before showing login UI; safe to run multiple times per session
    try:
        user = get_current_user()
        if not user and _drive_available():
            folder_id = _setting_get('gdrive_folder_id', GDRIVE_DEFAULT_FOLDER_ID) or GDRIVE_DEFAULT_FOLDER_ID
            if folder_id:
                svc = _build_drive()
                ok, msg = attempt_auto_restore_if_seed(svc, folder_id)
                if ok:
                    st.toast("Auto-restore DB dari Drive berhasil.")
    except Exception:
        pass
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
    st.sidebar.image(logo_path)
    st.sidebar.markdown("<h2 style='text-align:center;margin-bottom:0.5em;'>WIJNA Manajemen System</h2>", unsafe_allow_html=True)
    auth_sidebar()

    # One-time post-login backup to Google Drive (non-blocking on first click)
    if st.session_state.get("__post_login_backup"):
        try:
            if _drive_available():
                folder_id = _setting_get('gdrive_folder_id', GDRIVE_DEFAULT_FOLDER_ID) or GDRIVE_DEFAULT_FOLDER_ID
                if folder_id:
                    with st.spinner("Menyinkronkan database ke Drive (sekali setelah login)..."):
                        service = _build_drive()
                        _backup_db_now(service, folder_id)
            st.session_state.pop("__post_login_backup", None)
        except Exception:
            # Clear the flag even if it fails to avoid repeating
            st.session_state.pop("__post_login_backup", None)

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
        ("Dunyim Security", "🛡️ Dunyim Security")
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
            # Durasi tidak menghitung Libur Nasional (non-working days)
            durasi = _count_days_excluding_holidays(tgl_mulai, tgl_selesai) if tgl_selesai >= tgl_mulai else 0
            cur.execute("SELECT kuota_tahunan, cuti_terpakai FROM cuti WHERE nama=? ORDER BY tgl_mulai DESC LIMIT 1", (nama,))
            row = cur.fetchone()
            kuota_tahunan = row["kuota_tahunan"] if row else 12
            cuti_terpakai = row["cuti_terpakai"] if row else 0
            sisa_kuota = kuota_tahunan - cuti_terpakai
            st.info(f"Sisa kuota cuti: {sisa_kuota} hari dari {kuota_tahunan} hari")
            st.write(f"Durasi cuti diajukan (tidak termasuk Libur Nasional): {durasi} hari")
            if durasi > 0 and sisa_kuota < durasi:
                st.error("Sisa kuota tidak cukup, pengajuan cuti otomatis ditolak.")
            st.caption("Pengajuan akan ditolak otomatis bila Sisa Kuota < Durasi pengajuan.")
            if st.button("Ajukan Cuti"):
                if not alasan or durasi <= 0:
                    st.warning("Lengkapi data dan pastikan tanggal benar.")
                elif sisa_kuota < durasi:
                    st.error("Sisa kuota tidak cukup, pengajuan cuti ditolak.")
                else:
                    cid = gen_id("cuti")
                    now = now_wib_iso()
                    # Update running totals at time of submission
                    new_cuti_terpakai = cuti_terpakai + durasi
                    new_sisa = max(0, kuota_tahunan - new_cuti_terpakai)
                    cur.execute(
                        """
                        INSERT INTO cuti (id, nama, tgl_mulai, tgl_selesai, durasi, kuota_tahunan, cuti_terpakai, sisa_kuota, status, finance_note, finance_approved, director_note, director_approved)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '', 0, '', 0)
                        """,
                        (cid, nama, tgl_mulai.isoformat(), tgl_selesai.isoformat(), durasi, kuota_tahunan, new_cuti_terpakai, new_sisa, "Menunggu Review Finance"),
                    )
                    conn.commit()
                    try:
                        notify_review_request("cuti", title=f"{nama} — {durasi} hari", entity_id=cid, recipients_roles=("finance","director"))
                    except Exception:
                        pass
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
                            # Notify Director + applicant
                            try:
                                pemohon_email = _get_user_email_by_name(row['nama'])
                                decision = "finance_approved" if approve else "finance_rejected"
                                notify_decision("cuti", title=f"{row['nama']} — {row['tgl_mulai']} s/d {row['tgl_selesai']}", decision=decision,
                                                entity_id=row['id'], recipients_roles=("director",),
                                                recipients_users=[pemohon_email] if pemohon_email else None, tag_suffix="finance")
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
                            # Notify applicant + Finance
                            try:
                                pemohon_email = _get_user_email_by_name(row['nama'])
                                decision = "director_approved" if approve else "director_rejected"
                                notify_decision("cuti", title=f"{row['nama']} — {row['tgl_mulai']} s/d {row['tgl_selesai']}", decision=decision,
                                                entity_id=row['id'], recipients_roles=("finance",),
                                                recipients_users=[pemohon_email] if pemohon_email else None, tag_suffix="director")
                            except Exception:
                                pass
                            st.rerun()
            # Rekap semua pengajuan cuti
            st.markdown("#### Rekap Pengajuan Cuti")
            df = pd.read_sql_query("SELECT * FROM cuti ORDER BY tgl_mulai DESC", conn)
            st.dataframe(df, width='stretch', hide_index=True)
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
    elif choice == "Dunyim Security":
        # Check scheduled backup once on module enter (non-blocking)
        try:
            if _drive_available():
                folder_id = _setting_get('gdrive_folder_id', GDRIVE_DEFAULT_FOLDER_ID) or GDRIVE_DEFAULT_FOLDER_ID
                if folder_id:
                    svc = _build_drive()
                    check_scheduled_backup(svc, folder_id)
        except Exception:
            pass
        dunyim_security_module()


if __name__ == "__main__":
    ensure_db()
    main()
