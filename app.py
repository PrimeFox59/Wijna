import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
import hashlib
from streamlit_option_menu import option_menu
import plotly.express as px

# --- Database Initialization ---
def init_db():
    conn = get_db_connection()
    c = conn.cursor()

    # User table
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            full_name TEXT NOT NULL,
            role TEXT NOT NULL,
            is_approved INTEGER DEFAULT 0
        )
    ''')

    # Master data tables
    c.execute('''
        CREATE TABLE IF NOT EXISTS moulding_line (name TEXT UNIQUE)
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS causes (name TEXT UNIQUE)
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS statuses (name TEXT UNIQUE)
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS pc (name TEXT UNIQUE)
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS item_names (name TEXT UNIQUE)
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS customers (name TEXT UNIQUE)
    ''')

    # APPP table
    c.execute('''
        CREATE TABLE IF NOT EXISTS appp (
            id INTEGER PRIMARY KEY,
            date TEXT,
            appp_number TEXT UNIQUE NOT NULL,
            status TEXT,
            moulding_line TEXT,
            item_name TEXT,
            customer TEXT,
            item_type TEXT,
            request_type TEXT,
            requestor TEXT,
            cause TEXT,
            completion_date TEXT,
            pc TEXT
        )
    ''')

    # Audit Trail table
    c.execute('''
        CREATE TABLE IF NOT EXISTS audit_trail (
            id INTEGER PRIMARY KEY,
            timestamp TEXT,
            user TEXT,
            action TEXT
        )
    ''')

    # Insert default data if tables are empty
    if not c.execute('SELECT 1 FROM users').fetchone():
        hashed_password = hash_password('zzz')
        c.execute("INSERT INTO users (username, password, full_name, role, is_approved) VALUES (?, ?, ?, ?, ?)", ('admin_1', hashed_password, 'Admin Satu', 'Admin', 1))
        c.execute("INSERT INTO users (username, password, full_name, role, is_approved) VALUES (?, ?, ?, ?, ?)", ('user_1', hashed_password, 'Pengguna Satu', 'User', 1))
        c.execute("INSERT INTO users (username, password, full_name, role, is_approved) VALUES (?, ?, ?, ?, ?)", ('user_2', hashed_password, 'Pengguna Dua', 'User', 1))

    if not c.execute('SELECT 1 FROM moulding_line').fetchone():
        moulding_lines = ['2ML', 'ACE', 'DISA', 'SM60', 'PMS', 'FCD', 'SM']
        for line in moulding_lines:
            c.execute("INSERT INTO moulding_line (name) VALUES (?)", (line,))

    if not c.execute('SELECT 1 FROM causes').fetchone():
        causes = ['Trial', 'Kaizen', 'Runout', 'Abnormal']
        for cause in causes:
            c.execute("INSERT INTO causes (name) VALUES (?)", (cause,))

    if not c.execute('SELECT 1 FROM statuses').fetchone():
        statuses = ['NEW PROJECT', 'MASSPRO']
        for status in statuses:
            c.execute("INSERT INTO statuses (name) VALUES (?)", (status,))

    if not c.execute('SELECT 1 FROM pc').fetchone():
        pcs = ['Pattern', 'Cavity', 'Pattern & Cavity']
        for pc in pcs:
            c.execute("INSERT INTO pc (name) VALUES (?)", (pc,))
    
    if not c.execute('SELECT 1 FROM item_names').fetchone():
        items = ['BRACKET', 'SEALING', 'FLANGE']
        for item in items:
            c.execute("INSERT INTO item_names (name) VALUES (?)", (item,))

    if not c.execute('SELECT 1 FROM customers').fetchone():
        customers = ['Aisin', 'Denso', 'Yamaha']
        for cust in customers:
            c.execute("INSERT INTO customers (name) VALUES (?)", (cust,))
    
    conn.commit()

# --- Utility Functions ---
@st.cache_resource
def get_db_connection():
    return sqlite3.connect('app_database.db', check_same_thread=False)

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def verify_password(stored_password, provided_password):
    return stored_password == hash_password(provided_password)

def log_audit_trail(user, action):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("INSERT INTO audit_trail (timestamp, user, action) VALUES (?, ?, ?)",
              (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), user, action))
    conn.commit()

# --- Data Fetching Functions ---
def get_user(username):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE username = ?", (username,))
    return c.fetchone()

def get_all_appp():
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT * FROM appp ORDER BY date DESC", conn)
    return df

def get_master_data(table_name):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(f"SELECT name FROM {table_name}")
    return sorted([row[0] for row in c.fetchall()])

def get_all_users():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM users")
    return c.fetchall()
    
def get_all_audit_logs():
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT timestamp, user, action FROM audit_trail ORDER BY timestamp DESC", conn)
    return df

# --- UI/UX Components ---
def style_buttons():
    st.markdown("""
        <style>
            .stButton>button {
                width: 100%;
                border-radius: 8px;
                background-color: #4CAF50;
                color: white;
                border: none;
                padding: 10px;
                font-size: 16px;
                cursor: pointer;
            }
            .stButton>button:hover {
                background-color: #45a049;
            }
        </style>
    """, unsafe_allow_html=True)

# --- Main App ---
def main():
    if 'db_initialized' not in st.session_state:
        init_db()
        st.session_state['db_initialized'] = True
    
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False
        st.session_state['username'] = None
        st.session_state['role'] = None
        st.session_state['full_name'] = None
    
    if 'new_item_name' not in st.session_state:
        st.session_state.new_item_name = ''
    if 'new_customer' not in st.session_state:
        st.session_state.new_customer = ''
    if 'selected_item_name_key' not in st.session_state:
        st.session_state.selected_item_name_key = '--Pilih--'
    if 'selected_customer_key' not in st.session_state:
        st.session_state.selected_customer_key = '--Pilih--'
    if 'confirm_delete_id' not in st.session_state:
        st.session_state.confirm_delete_id = None

    if st.session_state['logged_in']:
        show_app_pages()
    else:
        show_auth_page()

def show_auth_page():
    st.title("Aplikasi Penelusuran Data APPP 📊")
    st.subheader("Login atau Daftar")

    choice = st.radio("Pilih Opsi", ["Login", "Daftar"], horizontal=True, key="auth_choice")

    if choice == "Login":
        with st.form(key="login_form"):
            st.subheader("Login")
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            if st.form_submit_button("Masuk"):
                user = get_user(username)
                if user and verify_password(user[2], password):
                    if user[5] == 1:
                        st.session_state['logged_in'] = True
                        st.session_state['username'] = user[1]
                        st.session_state['role'] = user[4]
                        st.session_state['full_name'] = user[3]
                        log_audit_trail(st.session_state['username'], 'Logged in')
                        st.success("Login berhasil!")
                        st.rerun()
                    else:
                        st.error("Akun Anda belum disetujui oleh admin.")
                else:
                    st.error("Username atau password salah.")
    else:
        with st.form(key="register_form"):
            st.subheader("Daftar Akun Baru")
            new_username = st.text_input("Username Baru")
            new_password = st.text_input("Password Baru", type="password")
            full_name = st.text_input("Nama Lengkap")
            new_role = st.selectbox("Pilih Role", ["User", "Admin"])
            
            if st.form_submit_button("Daftar"):
                conn = get_db_connection()
                c = conn.cursor()
                try:
                    hashed_password = hash_password(new_password)
                    is_approved = 1 if new_role == 'Admin' else 0
                    c.execute("INSERT INTO users (username, password, full_name, role, is_approved) VALUES (?, ?, ?, ?, ?)",
                              (new_username, hashed_password, full_name, new_role, is_approved))
                    conn.commit()
                    if new_role == 'User':
                        st.success("Pendaftaran berhasil! Tunggu persetujuan dari Admin.")
                    else:
                        st.success("Pendaftaran berhasil! Anda dapat login sekarang.")
                    log_audit_trail(new_username, 'Registered new account')
                    st.rerun()
                except sqlite3.IntegrityError:
                    st.error("Username sudah ada. Pilih username lain.")

def show_app_pages():
    st.sidebar.title(f"Selamat Datang, {st.session_state['full_name']}")
    
    with st.sidebar:
        st.write("---")
        st.write("Pilih Halaman:")
        options = ["Dashboard", "Monitoring", "Input Data", "Pengaturan Pengguna", "Panduan"]
        if st.session_state['role'] == 'Admin':
            options.append("Audit Trail")
        selected = option_menu(
            menu_title=None,
            options=options,
            icons=["bar-chart-line", "table", "file-earmark-plus", "person-gear", "book", "clipboard2-check"],
            menu_icon="cast",
            default_index=0,
            styles={"nav-link-selected": {"background-color": "#02ab21"}}
        )
        st.write("---")
        if st.button("Keluar"):
            log_audit_trail(st.session_state['username'], 'Logged out')
            st.session_state['logged_in'] = False
            st.rerun()

    if selected == "Dashboard":
        show_dashboard()
    elif selected == "Monitoring":
        show_monitoring()
    elif selected == "Input Data":
        show_input_form()
    elif selected == "Pengaturan Pengguna":
        show_user_settings()
    elif selected == "Panduan":
        show_guidelines()
    elif selected == "Audit Trail":
        show_audit_trail()

def show_dashboard():
    st.title("Dashboard Statistik 📊")
    
    df_appp = get_all_appp()
    if df_appp.empty:
        st.info("Belum ada data APPP untuk ditampilkan.")
        return

    # Hitung rata-rata waktu penyelesaian
    df_appp['date'] = pd.to_datetime(df_appp['date'])
    df_appp['completion_date'] = pd.to_datetime(df_appp['completion_date'], errors='coerce')
    df_appp['duration'] = (df_appp['completion_date'] - df_appp['date']).dt.days
    avg_duration = df_appp['duration'].mean()
    
    # KARTU METRIK UTAMA
    st.subheader("Ringkasan Data")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total APPP", df_appp.shape[0], help="Total keseluruhan APPP yang tercatat.")
    with col2:
        st.metric("Customer Unik", df_appp['customer'].nunique(), help="Jumlah customer yang berbeda.")
    with col3:
        st.metric("Item Unik", df_appp['item_name'].nunique(), help="Jumlah nama barang yang berbeda.")
    with col4:
        # Menampilkan rata-rata waktu penyelesaian jika ada data
        if not pd.isna(avg_duration):
            st.metric("Rata-rata Waktu Penyelesaian", f"{avg_duration:.2f} hari", help="Rata-rata durasi dari tanggal input sampai tanggal selesai.")
        else:
            st.metric("Rata-rata Waktu Penyelesaian", "N/A", help="Data tanggal penyelesaian tidak tersedia.")

    # --- FILTER INTERAKTIF ---
    st.subheader("Filter Data")
    col_filters, _ = st.columns([1, 3])
    with col_filters:
        selected_status = st.multiselect(
            "Filter berdasarkan Status",
            options=df_appp['status'].unique(),
            default=df_appp['status'].unique()
        )
        selected_moulding_line = st.multiselect(
            "Filter berdasarkan Moulding Line",
            options=df_appp['moulding_line'].unique(),
            default=df_appp['moulding_line'].unique()
        )
    
    filtered_df = df_appp[
        df_appp['status'].isin(selected_status) &
        df_appp['moulding_line'].isin(selected_moulding_line)
    ]

    if filtered_df.empty:
        st.warning("Tidak ada data yang sesuai dengan filter yang dipilih.")
        return

    # VISUALISASI GRAFIK
    st.write("---")
    st.subheader("Visualisasi Data")

    col_charts_1, col_charts_2 = st.columns(2)

    with col_charts_1:
        # Grafik Distribusi Status
        status_counts = filtered_df['status'].value_counts().reset_index()
        status_counts.columns = ['Status', 'Jumlah']
        fig_status = px.bar(
            status_counts, 
            x='Status', 
            y='Jumlah', 
            title='Distribusi APPP berdasarkan Status',
            color='Status',
            labels={'Jumlah': 'Jumlah APPP', 'Status': 'Status Proyek'},
            text_auto=True
        )
        st.plotly_chart(fig_status, use_container_width=True)

    with col_charts_2:
        # Grafik Distribusi Moulding Line
        line_counts = filtered_df['moulding_line'].value_counts().reset_index()
        line_counts.columns = ['Moulding Line', 'Jumlah']
        fig_line = px.pie(
            line_counts, 
            names='Moulding Line', 
            values='Jumlah', 
            title='Distribusi APPP berdasarkan Moulding Line',
            hole=0.4
        )
        fig_line.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_line, use_container_width=True)
    
    # Grafik Tren APPP per Bulan
    st.write("---")
    st.subheader("Tren APPP dari Waktu ke Waktu")
    
    monthly_counts = filtered_df.groupby(filtered_df['date'].dt.to_period('M')).size().reset_index(name='count')
    monthly_counts['month_year'] = monthly_counts['date'].astype(str)
    
    fig_trend = px.line(
        monthly_counts,
        x='month_year',
        y='count',
        markers=True,
        title='Tren Jumlah APPP per Bulan',
        labels={'count': 'Jumlah APPP', 'month_year': 'Bulan'},
    )
    fig_trend.update_xaxes(tickangle=45)
    st.plotly_chart(fig_trend, use_container_width=True)

    # Grafik Top 5 Customer dan Item
    st.write("---")
    st.subheader("Analisis Top Item dan Customer")
    
    col_top_charts_1, col_top_charts_2 = st.columns(2)

    with col_top_charts_1:
        top_customers = filtered_df['customer'].value_counts().head(5).reset_index()
        top_customers.columns = ['Customer', 'Jumlah']
        fig_top_cust = px.bar(
            top_customers,
            x='Customer',
            y='Jumlah',
            title='Top 5 Customer dengan APPP Terbanyak',
            color='Customer'
        )
        st.plotly_chart(fig_top_cust, use_container_width=True)

    with col_top_charts_2:
        top_items = filtered_df['item_name'].value_counts().head(5).reset_index()
        top_items.columns = ['Item', 'Jumlah']
        fig_top_item = px.bar(
            top_items,
            x='Item',
            y='Jumlah',
            title='Top 5 Barang dengan APPP Terbanyak',
            color='Item'
        )
        st.plotly_chart(fig_top_item, use_container_width=True)


def show_monitoring():
    st.title("Monitoring Data APPP 📋")
    st.write("Daftar lengkap data APPP, dapat diedit dan dihapus.")

    df_appp = get_all_appp()
    if df_appp.empty:
        st.info("Belum ada data untuk ditampilkan.")
        return
    
    search_query = st.text_input("Cari data (No. APPP, Nama Barang, Customer, dll.)", "")
    filtered_df = df_appp[df_appp.apply(lambda row: row.astype(str).str.contains(search_query, case=False).any(), axis=1)]
    
    st.dataframe(filtered_df, use_container_width=True)
    
    col_export, col_spacer = st.columns([1, 4])
    with col_export:
        csv = filtered_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Unduh Data (.csv)",
            data=csv,
            file_name='data_appp.csv',
            mime='text/csv',
            help="Ekspor data yang difilter ke file CSV."
        )

    st.write("---")
    st.subheader("Edit/Hapus Data")
    
    appp_ids = [str(x) for x in filtered_df['id'].tolist()]
    if not appp_ids:
        st.info("Tidak ada data yang cocok untuk diedit atau dihapus.")
        return
    
    selected_id = st.selectbox("Pilih ID Data untuk Diedit/Dihapus", appp_ids, key='edit_del_select')
    
    if selected_id:
        record_to_edit = df_appp[df_appp['id'].astype(str) == selected_id].iloc[0]
        
        requestor_username = [user[1] for user in get_all_users() if user[3] == record_to_edit['requestor']]
        can_edit_delete = (st.session_state['role'] == 'Admin') or (st.session_state['username'] == record_to_edit['requestor'] if record_to_edit['requestor'] in ['DEV', 'PROD', 'QC', 'QA'] else False)
        
        if not can_edit_delete:
            st.warning("Anda hanya dapat mengedit atau menghapus data yang Anda input sendiri.")
            return

        st.markdown(f"**Mengedit Data untuk ID: {selected_id}**")
        
        requestor_options = ['DEV', 'PROD', 'QC', 'QA']

        with st.form(key=f'edit_form_{selected_id}'):
            date = st.date_input("Tanggal", value=pd.to_datetime(record_to_edit['date']), key=f'edit_date_{selected_id}')
            appp_number = st.text_input("No. APPP", value=record_to_edit['appp_number'], key=f'edit_appp_{selected_id}', disabled=True)
            status = st.selectbox("Status", get_master_data('statuses'), index=get_master_data('statuses').index(record_to_edit['status']), key=f'edit_status_{selected_id}')
            moulding_line = st.selectbox("Moulding Line", get_master_data('moulding_line'), index=get_master_data('moulding_line').index(record_to_edit['moulding_line']), key=f'edit_ml_{selected_id}')
            item_name = st.text_input("Nama Barang", value=record_to_edit['item_name'], key=f'edit_item_{selected_id}')
            customer = st.text_input("Customer", value=record_to_edit['customer'], key=f'edit_cust_{selected_id}')
            item_type = st.text_input("Jenis Barang", value=record_to_edit['item_type'], key=f'edit_item_type_{selected_id}')
            request_type = st.text_area("Jenis Permintaan", value=record_to_edit['request_type'], key=f'edit_req_type_{selected_id}')
            cause = st.selectbox("Penyebab", get_master_data('causes'), index=get_master_data('causes').index(record_to_edit['cause']), key=f'edit_cause_{selected_id}')
            completion_date_val = pd.to_datetime(record_to_edit['completion_date']) if record_to_edit['completion_date'] else None
            completion_date = st.date_input("Permintaan Selesai", value=completion_date_val, key=f'edit_comp_date_{selected_id}')
            pc = st.selectbox("P/C", get_master_data('pc'), index=get_master_data('pc').index(record_to_edit['pc']), key=f'edit_pc_{selected_id}')
            
            try:
                current_requestor_index = requestor_options.index(record_to_edit['requestor'])
            except ValueError:
                current_requestor_index = 0
            requestor = st.selectbox("Requestor", requestor_options, index=current_requestor_index, key=f'edit_requestor_{selected_id}')

            if st.form_submit_button("Simpan Perubahan"):
                conn = get_db_connection()
                c = conn.cursor()
                try:
                    c.execute("""
                        UPDATE appp SET date=?, status=?, moulding_line=?, item_name=?, customer=?, item_type=?, request_type=?, cause=?, completion_date=?, pc=?, requestor=?
                        WHERE id=?
                    """, (date.strftime('%Y-%m-%d'), status, moulding_line, item_name, customer, item_type, request_type, cause, completion_date.strftime('%Y-%m-%d') if completion_date else '', pc, requestor, selected_id))
                    conn.commit()
                    st.success("Data berhasil diubah!")
                    log_audit_trail(st.session_state['username'], f'Edited APPP ID {selected_id}')
                    st.rerun()
                except Exception as e:
                    st.error(f"Terjadi kesalahan: {e}")

        st.write("---")
        st.markdown(f"**Hapus Data untuk ID: {selected_id}**")
        if st.button("Hapus Data"):
            st.session_state.confirm_delete_id = selected_id
            st.warning(f"Apakah Anda yakin ingin menghapus data dengan ID {selected_id}?")
            
        if st.session_state.confirm_delete_id == selected_id:
            col_confirm1, col_confirm2, col_confirm3 = st.columns([1,1,2])
            with col_confirm1:
                if st.button("Ya, Hapus Saja"):
                    conn = get_db_connection()
                    c = conn.cursor()
                    c.execute("DELETE FROM appp WHERE id=?", (selected_id,))
                    conn.commit()
                    st.session_state.confirm_delete_id = None
                    st.success("Data berhasil dihapus!")
                    log_audit_trail(st.session_state['username'], f'Deleted APPP ID {selected_id}')
                    st.rerun()
            with col_confirm2:
                if st.button("Batal"):
                    st.session_state.confirm_delete_id = None
                    st.info("Penghapusan dibatalkan.")
                    st.rerun()

def show_input_form():
    st.title("Form Input Data APPP ✍️")
    st.write("Isi formulir untuk menambahkan data APPP baru.")

    moulding_lines = get_master_data('moulding_line')
    causes = get_master_data('causes')
    statuses = get_master_data('statuses')
    pcs = get_master_data('pc')

    item_names = get_master_data('item_names')
    customers = get_master_data('customers')
    requestor_options = ['DEV', 'PROD', 'QC', 'QA']

    col1, col2 = st.columns(2)
    with col1:
        date = st.date_input("Tanggal")
        appp_number = st.text_input("No. APPP", help="Nomor ini harus unik.")
        status = st.selectbox("Status", statuses)
        moulding_line = st.selectbox("Moulding Line", moulding_lines)
    
    with col2:
        item_type = st.text_input("Jenis Barang")
        request_type = st.text_area("Jenis Permintaan")
        cause = st.selectbox("Penyebab", causes)
        completion_date = st.date_input("Permintaan Selesai (Opsional)", value=None)
        pc = st.selectbox("P/C", pcs)

    st.write("---")

    col_master1, col_master2 = st.columns(2)
    with col_master1:
        item_name_options = ['--Pilih--', '--Tambah Baru--'] + item_names
        selected_item_name = st.selectbox("Nama Barang", item_name_options, key='item_name_select', 
                                        index=item_name_options.index(st.session_state.get('selected_item_name_key', '--Pilih--')))
        if selected_item_name == '--Tambah Baru--':
            new_item_name = st.text_input("Nama Barang Baru", key='new_item_name_input')
            st.session_state.new_item_name = new_item_name
        else:
            st.session_state.new_item_name = ""

    with col_master2:
        customer_options = ['--Pilih--', '--Tambah Baru--'] + customers
        selected_customer = st.selectbox("Customer", customer_options, key='customer_select',
                                       index=customer_options.index(st.session_state.get('selected_customer_key', '--Pilih--')))
        if selected_customer == '--Tambah Baru--':
            new_customer = st.text_input("Nama Customer Baru", key='new_customer_input')
            st.session_state.new_customer = new_customer
        else:
            st.session_state.new_customer = ""

    requestor = st.selectbox("Requestor", requestor_options)
    
    with st.form(key='appp_form'):
        st.write("---")
        submit_button = st.form_submit_button("Simpan Data")

        if submit_button:
            final_item_name = selected_item_name
            if selected_item_name == '--Tambah Baru--':
                final_item_name = st.session_state.new_item_name

            final_customer = selected_customer
            if selected_customer == '--Tambah Baru--':
                final_customer = st.session_state.new_customer

            if not appp_number or final_item_name == '--Pilih--' or final_customer == '--Pilih--' or (selected_item_name == '--Tambah Baru--' and not final_item_name) or (selected_customer == '--Tambah Baru--' and not final_customer):
                st.error("Ada data yang belum diisi. Pastikan semua kolom yang diperlukan telah terisi.")
                st.stop()
            
            conn = get_db_connection()
            c = conn.cursor()
            
            try:
                if selected_item_name == '--Tambah Baru--' and final_item_name not in item_names:
                    c.execute("INSERT INTO item_names (name) VALUES (?)", (final_item_name,))
                
                if selected_customer == '--Tambah Baru--' and final_customer not in customers:
                    c.execute("INSERT INTO customers (name) VALUES (?)", (final_customer,))
                
                c.execute("SELECT appp_number FROM appp WHERE appp_number=?", (appp_number,))
                if c.fetchone():
                    st.error("Nomor APPP sudah ada. Masukkan nomor lain.")
                    st.stop()
                
                c.execute("""
                    INSERT INTO appp (date, appp_number, status, moulding_line, item_name, customer, item_type, request_type, requestor, cause, completion_date, pc)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (date.strftime('%Y-%m-%d'), appp_number, status, moulding_line, final_item_name, final_customer, item_type, request_type, requestor, cause, completion_date.strftime('%Y-%m-%d') if completion_date else '', pc))
                
                conn.commit()
                st.success("Data APPP berhasil disimpan!")
                log_audit_trail(st.session_state['username'], f'Added new APPP {appp_number}')
                st.rerun()
            except sqlite3.IntegrityError as e:
                st.error(f"Terjadi kesalahan: Data sudah ada atau input duplikat. Detail: {e}")
            except Exception as e:
                st.error(f"Terjadi kesalahan: {e}")

def show_user_settings():
    if st.session_state['role'] != 'Admin':
        st.warning("Anda tidak memiliki akses ke halaman ini.")
        return

    st.title("Pengaturan Pengguna 👤")
    st.write("Kelola pengguna, setujui akun baru, dan atur peran.")

    conn = get_db_connection()
    df_users = pd.read_sql_query("SELECT id, username, full_name, role, is_approved FROM users", conn)

    st.subheader("Daftar Pengguna")
    st.dataframe(df_users, hide_index=True)

    st.subheader("Setujui Akun Baru")
    pending_users = df_users[df_users['is_approved'] == 0]['username'].tolist()
    if pending_users:
        user_to_approve = st.selectbox("Pilih Pengguna untuk Disetujui", pending_users)
        if st.button(f"Setujui {user_to_approve}"):
            c = conn.cursor()
            c.execute("UPDATE users SET is_approved = 1 WHERE username = ?", (user_to_approve,))
            conn.commit()
            st.success(f"Akun {user_to_approve} telah disetujui.")
            log_audit_trail(st.session_state['username'], f'Approved user {user_to_approve}')
            st.rerun()
    else:
        st.info("Tidak ada akun baru yang menunggu persetujuan.")
    
    st.subheader("Ubah Password Saya")
    with st.form(key="change_password_form"):
        old_pass = st.text_input("Password Lama", type="password")
        new_pass = st.text_input("Password Baru", type="password")
        if st.form_submit_button("Ubah Password"):
            user_data = get_user(st.session_state['username'])
            if verify_password(user_data[2], old_pass):
                c = conn.cursor()
                c.execute("UPDATE users SET password=? WHERE username=?", (hash_password(new_pass), st.session_state['username']))
                conn.commit()
                st.success("Password Anda berhasil diubah!")
                log_audit_trail(st.session_state['username'], 'Changed own password')
                st.rerun()
            else:
                st.error("Password lama salah.")
    
    st.subheader("Kelola Pengguna & Master Data")
    user_to_manage = st.selectbox("Pilih Pengguna yang akan dikelola", df_users['username'].tolist())
    if user_to_manage:
        user_info = get_user(user_to_manage)
        if user_info:
            new_role = st.selectbox(f"Ubah Role {user_to_manage}", ["User", "Admin"], index=0 if user_info[4] == 'User' else 1)
            new_pass = st.text_input(f"Ganti Password {user_to_manage}", type="password", help="Kosongkan jika tidak ingin mengubah.")

            col_manage1, col_manage2 = st.columns(2)
            with col_manage1:
                if st.button(f"Update Pengaturan {user_to_manage}", key=f'update_user_{user_to_manage}'):
                    c = conn.cursor()
                    if new_pass:
                        hashed_pass = hash_password(new_pass)
                        c.execute("UPDATE users SET role=?, password=? WHERE username=?", (new_role, hashed_pass, user_to_manage))
                        st.success(f"Role dan password {user_to_manage} berhasil diubah.")
                    else:
                        c.execute("UPDATE users SET role=? WHERE username=?", (new_role, user_to_manage))
                        st.success(f"Role {user_to_manage} berhasil diubah.")
                    conn.commit()
                    log_audit_trail(st.session_state['username'], f'Managed user {user_to_manage}')
                    st.rerun()
            with col_manage2:
                if st.button(f"Hapus Pengguna {user_to_manage}", key=f'delete_user_{user_to_manage}'):
                    if st.warning(f"Apakah Anda yakin ingin menghapus pengguna {user_to_manage}?"):
                        if st.button("Ya, Hapus Pengguna"):
                            c = conn.cursor()
                            c.execute("DELETE FROM users WHERE username=?", (user_to_manage,))
                            conn.commit()
                            st.success(f"Pengguna {user_to_manage} berhasil dihapus.")
                            log_audit_trail(st.session_state['username'], f'Deleted user {user_to_manage}')
                            st.rerun()
    
    st.subheader("Pengaturan Master Data")
    st.info("Tambahkan atau hapus data dari menu dropdown.")
    
    master_tables = {
        'Moulding Line': 'moulding_line', 
        'Penyebab': 'causes', 
        'Status': 'statuses', 
        'P/C': 'pc',
        'Nama Barang': 'item_names',
        'Customer': 'customers'
    }
    
    for display_name, table_name in master_tables.items():
        st.write(f"**{display_name}**")
        col_view, col_add, col_del = st.columns([2,1,1])
        current_data = get_master_data(table_name)
        with col_view:
            st.write(f"Data saat ini: {', '.join(current_data)}")
        with col_add:
            new_item = st.text_input(f"Tambah", key=f'add_{table_name}')
            if st.button(f"Tambah", key=f'btn_add_{table_name}'):
                if new_item:
                    c = conn.cursor()
                    try:
                        c.execute(f"INSERT INTO {table_name} (name) VALUES (?)", (new_item,))
                        conn.commit()
                        st.success(f"'{new_item}' berhasil ditambahkan ke {display_name}.")
                        log_audit_trail(st.session_state['username'], f'Added master data {new_item} to {table_name}')
                        st.rerun()
                    except sqlite3.IntegrityError:
                        st.warning(f"'{new_item}' sudah ada.")
        with col_del:
            if current_data:
                item_to_delete = st.selectbox(f"Hapus", ['--Pilih--'] + current_data, key=f'del_{table_name}')
                if item_to_delete != '--Pilih--':
                    if st.button(f"Hapus {item_to_delete}", key=f'btn_del_{table_name}'):
                        c = conn.cursor()
                        c.execute(f"DELETE FROM {table_name} WHERE name=?", (item_to_delete,))
                        conn.commit()
                        st.success(f"'{item_to_delete}' berhasil dihapus dari {display_name}.")
                        log_audit_trail(st.session_state['username'], f'Deleted master data {item_to_delete} from {table_name}')
                        st.rerun()

    st.subheader("Hapus Data Duplikat")
    if st.button("Hapus Data Duplikat (No. APPP)"):
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("""
            DELETE FROM appp WHERE id NOT IN (
                SELECT MIN(id) FROM appp GROUP BY appp_number
            )
        """)
        rows_deleted = c.rowcount
        conn.commit()
        if rows_deleted > 0:
            st.success(f"{rows_deleted} data duplikat berhasil dihapus.")
            log_audit_trail(st.session_state['username'], f'Deleted {rows_deleted} duplicate APPP records')
            st.rerun()
        else:
            st.info("Tidak ada data duplikat ditemukan.")

def show_guidelines():
    st.title("Panduan Penggunaan Aplikasi 📖")
    st.write("Berikut adalah panduan singkat untuk menggunakan aplikasi ini secara efektif.")

    st.subheader("1. Halaman Dashboard")
    st.write("Menampilkan ringkasan statistik dari data yang telah diinput, seperti total APPP, jumlah customer unik, dan grafik distribusi.")

    st.subheader("2. Halaman Input Data")
    st.write("Gunakan halaman ini untuk memasukkan data APPP baru. Pastikan **No. APPP** bersifat unik. Untuk mengisi **Nama Barang** dan **Customer**, pilih dari daftar yang tersedia. Jika data tidak ada, pilih **`--Tambah Baru--`** untuk memasukkan data baru.")

    st.subheader("3. Halaman Monitoring")
    st.write("Menampilkan semua data APPP dalam bentuk tabel. Anda bisa menggunakan **bilah pencarian** untuk mencari data. Fitur edit dan hapus hanya tersedia untuk **Admin** atau **pengguna yang menginput data tersebut**.")
    st.write("Gunakan tombol **Unduh Data (.csv)** untuk mengekspor data yang ditampilkan.")

    st.subheader("4. Halaman Pengaturan Pengguna (Khusus Admin)")
    st.write("Hanya dapat diakses oleh Admin. Halaman ini memungkinkan Anda:")
    st.write("- **Menyetujui** akun pengguna baru yang mendaftar.")
    st.write("- **Mengubah password Anda sendiri** atau password pengguna lain.")
    st.write("- **Mengubah peran (role)** pengguna lain menjadi 'Admin' atau 'User'.")
    st.write("- **Hapus** pengguna.")
    st.write("- **Tambah atau Hapus** data dari menu dropdown.")
    st.write("- **Hapus Data Duplikat** berdasarkan No. APPP untuk menjaga kebersihan data.")

    st.subheader("5. Halaman Audit Trail (Khusus Admin)")
    st.write("Hanya dapat diakses oleh Admin. Halaman ini menampilkan semua aktivitas pengguna secara rinci.")

    st.subheader("6. Keluar Aplikasi")
    st.write("Tekan tombol **'Keluar'** di sidebar untuk keluar dari sesi akun Anda saat ini.")

def show_audit_trail():
    st.title("Audit Trail 🕵️")
    st.write("Riwayat lengkap semua aktivitas pengguna dalam aplikasi. Hanya dapat diakses oleh Admin.")
    
    if st.session_state['role'] != 'Admin':
        st.warning("Anda tidak memiliki akses untuk melihat halaman ini.")
        return
        
    df_audit = get_all_audit_logs()
    
    if df_audit.empty:
        st.info("Belum ada log aktivitas untuk ditampilkan.")
    else:
        st.dataframe(df_audit, use_container_width=True)

if __name__ == '__main__':
    style_buttons()
    main()
