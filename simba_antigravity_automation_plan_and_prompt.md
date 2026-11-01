# Rencana Automasi (Automation Plan) & Master Prompt untuk Antigravity IDE (Format `.ipynb`)

**Proyek:** Sistem Informasi Monitor Batch & Agregasi Stok (SIMBA) — Kelompok 11  
**Cakupan Tugas:** Layer 1 (Data Source) $\rightarrow$ Layer 2 (Ingestion Layer - Pandas `.ipynb`) $\rightarrow$ Layer 3 (Storage Layer - PostgreSQL)

## 1. Daftar File yang Harus Kamu Siapkan & Lampirkan di Antigravity IDE

Agar AI Agent di **Antigravity IDE** dapat bekerja secara otomatis menggunakan **Jupyter Notebook (`.ipynb`)**, masukkan file-file berikut ke dalam satu folder kerja (*workspace folder*) di laptopmu, lalu buka folder tersebut di Antigravity IDE:

### A. File Inti (WAJIB Ada di Folder Proyek & Di-*tag* dengan `@` di Chat Antigravity)

| No | Nama File | Asal File | Fungsi bagi AI Agent di Antigravity IDE |
| :--- | :--- | :--- | :--- |
| **1** | **`Sales Transaction v.4a.csv`** | File dataset dari kamu | Sumber data mentah (536.350 baris, 41 MB) yang akan dibaca dan dibersihkan oleh notebook. |
| **2** | **`simba_ingestion_pipeline.ipynb`** | File Jupyter Notebook yang sudah saya buatkan | Notebook interaktif berisi 8 sel terstruktur (*Extract, Validate, Clean, Transform 6 Tabel Utama, Export CSV, & Batch Ingestion PostgreSQL*) lengkap dengan tabel *output* yang sudah terisi. |
| **3** | **`simba_postgres_schema.sql`** | File SQL yang sudah saya buatkan | Skema DDL pembuatan 6 tabel utama (`customers`, `products`, `orders`, `order_items`, `payments`, `reviews`) + `clean_sales_transactions` & kueri verifikasi di PostgreSQL. |
| **4** | **`SIMBA_Data_Ingestion_Cleaning_Plan.md`** | File plan yang sudah saya buatkan | Menjadi acuan (*ground truth*) bagi Agent untuk memvalidasi apakah jumlah baris hasil eksekusi notebook sudah 100% cocok dengan standar audit. |

### B. File Pendukung (Opsional — Lampirkan Jika Ingin Agent Membuat Laporan Praktikum)

| No | Nama File | Fungsi Tambahan |
| :--- | :--- | :--- |
| **5** | **`image_f49d5f.png`** | Gambar potongan diagram tugasmu, berguna jika kamu meminta Agent membuatkan `README.md` atau dokumentasi proyek. |
| **6** | **`Arsitektur_Kelompok 11.pdf`** | Dokumen arsitektur lengkap kelompokmu agar Agent memahami alasan pemilihan Pandas, PostgreSQL, PySpark, dan MADlib. |

## 2. Rencana Tahapan Automasi (Automation Plan) di Antigravity IDE

Saat kamu menjalankan **Master Prompt** di bawah pada Antigravity IDE, Agent akan mengeksekusi **5 Fase Automasi** berbasis `.ipynb` secara berurutan:

```text
[FASE 1: WORKSPACE & JUPYTER KERNEL SETUP]
 ├── Membuat file requirements.txt (pandas, numpy, sqlalchemy, psycopg2-binary, ipykernel, jupyter, nbclient)
 └── Membuat Python Virtual Environment (.venv) & mendaftarkan Jupyter Kernel
        │
        ▼
[FASE 2: EKSEKUSI NOTEBOOK MODE LOKAL & AUDIT DATA QUALITY]
 ├── Menjalankan seluruh sel pada 'simba_ingestion_pipeline.ipynb'
 └── Menghasilkan 7 file CSV bersih di './output_clean_tables/' & memverifikasi metrik:
     • Duplikat dihapus         =   5.200 baris -> Sisa 531.150 baris unik
     • CustomerNo 'GUEST'       =      55 baris
     • Transaksi CANCELLED      =   8.548 baris | COMPLETED = 522.602 baris
        │
        ▼
[FASE 3: DATABASE & SCHEMA AUTOMATION (POSTGRESQL)]
 ├── Mengecek koneksi PostgreSQL lokal (localhost:5432)
 ├── Membuat database 'simba_db' (jika belum ada)
 └── Mengeksekusi 'simba_postgres_schema.sql' (6 Tabel Utama + 1 Tabel Bersih + Index)
        │
        ▼
[FASE 4: BATCH INGESTION DARI NOTEBOOK KE POSTGRESQL]
 ├── Mengaktifkan ENABLE_POSTGRES_LOAD = True pada Sel 1 'simba_ingestion_pipeline.ipynb'
 ├── Menjalankan Sel 8 untuk memuat ke-7 tabel ke PostgreSQL (chunksize=10000)
 └── Memastikan hasil COUNT(*) di PostgreSQL 100% cocok:
     • customers                =   4.739 baris
     • products                 =   3.768 baris
     • orders                   =  23.204 baris
     • order_items              = 531.150 baris
     • payments                 =  23.204 baris
     • reviews                  =  23.204 baris
     • clean_sales_transactions = 522.602 baris (Gross Revenue: £62.781.386,54)
        │
        ▼
[FASE 5: DOKUMENTASI & PANDUAN DEMO]
 └── Membuat 'README.md' berisi panduan menjalankan tiap sel '.ipynb' saat demo ke dosen
```

## 3. Master Prompt Siap Pakai untuk Antigravity IDE (Versi `.ipynb`)

### Cara Menggunakan:
1. Buat folder baru di laptopmu, misalnya `SIMBA_Kelompok11_Ingestion`.
2. Masukkan ke-4 file wajib (`Sales Transaction v.4a.csv`, `simba_ingestion_pipeline.ipynb`, `simba_postgres_schema.sql`, dan `SIMBA_Data_Ingestion_Cleaning_Plan.md`) ke dalam folder tersebut.
3. Buka folder `SIMBA_Kelompok11_Ingestion` di **Antigravity IDE**.
4. Buka panel **Agent / Chat** di Antigravity IDE, lampirkan ke-4 file tersebut (`@Sales Transaction v.4a.csv`, `@simba_ingestion_pipeline.ipynb`, `@simba_postgres_schema.sql`, `@SIMBA_Data_Ingestion_Cleaning_Plan.md`), lalu **salin dan tempel (copy-paste) Master Prompt di bawah ini**:

### PROMPT 1: Full End-to-End Notebook Automation (Setup Kernel + Eksekusi `.ipynb` + PostgreSQL Ingestion)

```markdown
Kamu adalah Senior Data Engineer yang bertugas mengotomasi proyek Big Data "SIMBA" (Kelompok 11) untuk bagian saya, yaitu Layer 1 (Data Source), Layer 2 (Ingestion Layer - Python/Pandas di Jupyter Notebook), dan Layer 3 (Storage Layer - PostgreSQL).

Saya sudah melampirkan 4 file utama di workspace ini:
1. @Sales Transaction v.4a.csv (Dataset mentah: 536.350 baris, 8 kolom)
2. @simba_ingestion_pipeline.ipynb (File Jupyter Notebook utama untuk Batch Ingestion & Data Cleaning)
3. @simba_postgres_schema.sql (DDL PostgreSQL untuk 6 tabel utama + 1 tabel bersih PySpark)
4. @SIMBA_Data_Ingestion_Cleaning_Plan.md (Dokumen spesifikasi teknis & target metrik validasi)

Tolong jalankan langkah-langkah automasi berikut secara berurutan:

### Langkah 1: Setup Virtual Environment & Jupyter Kernel
1. Buat file `requirements.txt` berisi: `pandas`, `numpy`, `sqlalchemy`, `psycopg2-binary`, `ipykernel`, `jupyter`, `nbclient`, `nbformat`.
2. Buat Python virtual environment (`.venv`), instal seluruh paket di `requirements.txt`, dan pastikan notebook `@simba_ingestion_pipeline.ipynb` dapat dijalankan menggunakan kernel `.venv` tersebut.

### Langkah 2: Eksekusi Sel Notebook & Verifikasi Output CSV Lokal
1. Jalankan seluruh sel di `@simba_ingestion_pipeline.ipynb` (Sel 1 s.d. Sel 7) dan simpan output selnya langsung di dalam file `.ipynb`.
2. Pastikan 7 file CSV bersih berhasil terbentuk di `./output_clean_tables` dan jumlah barisnya cocok 100% dengan dokumen `@SIMBA_Data_Ingestion_Cleaning_Plan.md`:
   - `customers.csv` (4.739 baris)
   - `products.csv` (3.768 baris)
   - `orders.csv` (23.204 baris)
   - `order_items.csv` (531.150 baris)
   - `payments.csv` (23.204 baris)
   - `reviews.csv` (23.204 baris)
   - `clean_sales_transactions.csv` (522.602 baris)

### Langkah 3: Konfigurasi & Batch Ingestion ke PostgreSQL Lokal
1. Cek apakah layanan PostgreSQL lokal di `localhost:5432` sudah aktif (user default `postgres`, database target `simba_db`; tanyakan password kepada saya jika diperlukan).
2. Buat database `simba_db` jika belum ada, lalu eksekusi skema `@simba_postgres_schema.sql`.
3. Pada Sel 1 di `@simba_ingestion_pipeline.ipynb`, sesuaikan `DB_URL` dan ubah `ENABLE_POSTGRES_LOAD = True`, lalu jalankan ulang Sel 8 untuk memuat ketujuh tabel ke PostgreSQL (`chunksize=10000`).
4. Jalankan kueri verifikasi `COUNT(*)` pada ke-7 tabel di database `simba_db` dan tampilkan tabel ringkasannya kepada saya.

### Langkah 4: Dokumentasi README
1. Buatkan file `README.md` yang menjelaskan struktur tiap sel pada `simba_ingestion_pipeline.ipynb`, cara menjalankannya di Antigravity IDE, serta ringkasan hasil Data Cleaning untuk laporan kelompok saya.
```

### PROMPT 2: Alternatif Jika PostgreSQL Belum Aktif (Fokus Eksekusi `.ipynb` & Ekspor Tabel Bersih)

```markdown
Saya sudah melampirkan @Sales Transaction v.4a.csv, @simba_ingestion_pipeline.ipynb, @simba_postgres_schema.sql, dan @SIMBA_Data_Ingestion_Cleaning_Plan.md.

Saat ini PostgreSQL belum aktif di laptop saya. Tolong bantu saya mengotomasi langkah berikut:
1. Siapkan virtual environment Python (`.venv`) dan instal `pandas`, `numpy`, `sqlalchemy`, `ipykernel`, `jupyter`, `nbclient`, `nbformat`.
2. Jalankan seluruh sel pada `@simba_ingestion_pipeline.ipynb` terhadap `@Sales Transaction v.4a.csv` hingga menghasilkan ke-7 file CSV bersih di folder `./output_clean_tables` (`customers.csv`, `products.csv`, `orders.csv`, `order_items.csv`, `payments.csv`, `reviews.csv`, dan `clean_sales_transactions.csv`).
3. Tambahkan juga 1 sel opsional di bagian akhir `@simba_ingestion_pipeline.ipynb` untuk menyimpan ketujuh tabel tersebut ke database lokal SQLite (`sqlite:///simba_local.db`) sebagai simulasi sebelum saya menyalakan PostgreSQL.
4. Verifikasi jumlah baris pada semua tabel dan buatkan `README.md` panduan penggunaan notebook ini.
```