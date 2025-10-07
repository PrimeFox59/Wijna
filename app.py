import streamlit as st
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore

# --- KONEKSI KE FIREBASE ---

# Fungsi ini menggunakan @st.cache_resource agar koneksi hanya dibuat sekali
@st.cache_resource
def initialize_firebase():
    """Inisialisasi koneksi ke Firebase menggunakan service account."""
    # Cek apakah aplikasi sudah diinisialisasi
    if not firebase_admin._apps:
        # Ganti 'path/to/your/firebase-key.json' dengan path file kunci Anda
        # Jika file ada di folder yang sama, cukup gunakan 'firebase-key.json'
        cred = credentials.Certificate("firebase-key.json")
        firebase_admin.initialize_app(cred)
    return firestore.client()

# Panggil fungsi untuk mendapatkan koneksi db
db = initialize_firebase()
mahasiswa_ref = db.collection('mahasiswa')

# --- TAMPILAN APLIKASI STREAMLIT ---

st.set_page_config(page_title="Data Mahasiswa", page_icon="👨‍🎓", layout="wide")
st.title("👨‍🎓 Aplikasi CRUD Data Mahasiswa")
st.write("Aplikasi ini terhubung dengan Google Firebase Firestore.")

# --- FUNGSI-FUNGSI CRUD ---

def load_data():
    """Mengambil semua data dari koleksi 'mahasiswa' dan mengembalikannya sebagai DataFrame."""
    try:
        docs = mahasiswa_ref.stream()
        data = []
        for doc in docs:
            item = doc.to_dict()
            item['id'] = doc.id  # Simpan ID dokumen
            data.append(item)
        if not data:
            return pd.DataFrame()
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Gagal memuat data: {e}")
        return pd.DataFrame()

# --- CREATE (TAMBAH DATA) ---
st.header("➕ Tambah Data Mahasiswa Baru")
with st.form("tambah_data_form", clear_on_submit=True):
    nama_baru = st.text_input("Nama Mahasiswa", key="nama_baru")
    nim_baru = st.text_input("NIM", key="nim_baru")
    jurusan_baru = st.text_input("Jurusan", key="jurusan_baru")
    submit_button = st.form_submit_button(label="Tambah Data")

    if submit_button:
        if nama_baru and nim_baru and jurusan_baru:
            try:
                # Dokumen baru tanpa ID spesifik (Firestore akan generate otomatis)
                mahasiswa_ref.add({
                    'nama': nama_baru,
                    'nim': nim_baru,
                    'jurusan': jurusan_baru
                })
                st.success("Data mahasiswa berhasil ditambahkan!")
            except Exception as e:
                st.error(f"Gagal menambahkan data: {e}")
        else:
            st.warning("Mohon isi semua field.")

# --- READ (TAMPILKAN DATA) ---
st.header("📊 Data Mahasiswa Saat Ini")
df_mahasiswa = load_data()

if not df_mahasiswa.empty:
    # Tampilkan data tanpa kolom ID
    st.dataframe(df_mahasiswa.drop(columns=['id']), use_container_width=True)
else:
    st.info("Belum ada data mahasiswa.")


# --- UPDATE & DELETE ---
st.header("✏️ Perbarui atau Hapus Data")

if not df_mahasiswa.empty:
    # Buat daftar pilihan berdasarkan nama dan ID
    # Format: "Nama (ID)" untuk memastikan keunikan
    pilihan_list = [f"{nama} ({id_doc})" for nama, id_doc in zip(df_mahasiswa['nama'], df_mahasiswa['id'])]
    pilihan = st.selectbox("Pilih data mahasiswa untuk diubah:", pilihan_list)

    if pilihan:
        # Ekstrak ID dari string pilihan
        selected_id = pilihan.split('(')[-1].replace(')', '')
        data_terpilih = df_mahasiswa[df_mahasiswa['id'] == selected_id].iloc[0]

        with st.form("update_data_form"):
            st.write(f"Anda mengubah data untuk: **{data_terpilih['nama']}**")
            nama_update = st.text_input("Nama", value=data_terpilih['nama'])
            nim_update = st.text_input("NIM", value=data_terpilih['nim'])
            jurusan_update = st.text_input("Jurusan", value=data_terpilih['jurusan'])

            col1, col2 = st.columns(2)
            with col1:
                update_button = st.form_submit_button("Perbarui Data")
            with col2:
                delete_button = st.form_submit_button("❌ Hapus Data")

            if update_button:
                try:
                    mahasiswa_ref.document(selected_id).update({
                        'nama': nama_update,
                        'nim': nim_update,
                        'jurusan': jurusan_update
                    })
                    st.success(f"Data untuk {nama_update} berhasil diperbarui!")
                    st.rerun() # Muat ulang aplikasi untuk refresh data
                except Exception as e:
                    st.error(f"Gagal memperbarui data: {e}")

            if delete_button:
                try:
                    mahasiswa_ref.document(selected_id).delete()
                    st.success(f"Data untuk {data_terpilih['nama']} berhasil dihapus!")
                    st.rerun() # Muat ulang aplikasi
                except Exception as e:
                    st.error(f"Gagal menghapus data: {e}")
else:
    st.warning("Tidak ada data untuk diubah atau dihapus.")
