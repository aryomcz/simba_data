# Dokumen Rencana Kerja (Work Plan) & Laporan Hasil Eksekusi Data Cleaning
**Proyek:** Sistem Informasi Monitor Batch & Agregasi Stok (SIMBA) — Kelompok 11  
**Program Studi:** Teknologi Rekayasa Perangkat Lunak, Sekolah Vokasi IPB University  
**Fokus Modul:** Layer 1 (Data Source), Layer 2 (Ingestion Layer - Python/Pandas), & Layer 3 (Storage Layer - PostgreSQL)  
**Dataset:** Kaggle *"An Online Shop Business"* (`Sales Transaction v.4a.csv`)

---

## 1. Pemahaman Konteks Proyek & Scope Pekerjaan Kamu

Berdasarkan dokumen **Arsitektur Sistem Big Data End-to-End (Kelompok 11)**, **Worksheet Praktikum 3**, dan **potongan diagram arsitektur** yang kamu lampirkan, kamu memegang peran sebagai **Upstream Data Engineer (Ingestion & Storage Engineer)** yang bertanggung jawab atas **Layer 1 s.d. Layer 3**:

1. **Layer 1 — Data Source (`Sales Transaction v.4a.csv`)**
   - Mengelola sumber data mentah transaksi e-commerce berukuran **41,00 MB** dengan total **536.350 baris** dan **8 kolom** (`TransactionNo`, `Date`, `ProductNo`, `ProductName`, `Price`, `Quantity`, `CustomerNo`, `Country`).
2. **Layer 2 — Ingestion Layer (`Python Script - Pandas`)**
   - **Membaca CSV:** Mengekstrak data mentah secara *batch* ke dalam memori menggunakan Pandas.
   - **Validasi Data:** Memastikan kelengkapan skema kolom, memeriksa nilai kosong (*missing values*), mendeteksi duplikasi, dan memvalidasi rentang nilai numerik (`Price`, `Quantity`) serta format tanggal (`Date`).
   - **Pembersihan Data (*Data Cleaning*):** Menghapus 5.200 baris duplikat identik, menangani 55 nilai `CustomerNo` yang kosong (`'GUEST'`), memisahkan/menandai 8.548 transaksi pembatalan (*Cancelled Orders*), serta menstandarkan tipe data dan format string.
   - **Transformasi Awal:** Menghitung metrik dasar (`line_total = Price * Quantity`), mengekstrak atribut waktu (`year`, `month`, `day`, `day_of_week`), dan melakukan **normalisasi data** menjadi **6 Tabel Utama PostgreSQL** sesuai diagram arsitektur kelompokmu ditambah 1 tabel bersih siap pakai untuk PySpark & MADlib.
3. **Layer 3 — Storage Layer (`PostgreSQL Relational Database`)**
   - Membangun skema 6 tabel relasional utama (`customers`, `products`, `orders`, `order_items`, `payments`, `reviews`) + tabel `clean_sales_transactions` di PostgreSQL beserta *Primary Key*, *Foreign Key*, dan *Index*.
   - Mengeksekusi **Batch Ingestion** dari Pandas ke PostgreSQL secara bertahap (`chunksize=10000`) menggunakan `SQLAlchemy` + `psycopg2`.

---

## 2. Rekomendasi Lingkungan Kerja: Google Colab vs Antigravity IDE

**Rekomendasi Utama: Gunakan Antigravity IDE (Lokal), BUKAN Google Colab.**

| Aspek Teknis | Antigravity IDE (Lokal) — **DIREKOMENDASIKAN** | Google Colab (Cloud) |
| :--- | :--- | :--- |
| **Koneksi ke PostgreSQL (`Storage Layer`)** | **Native (`localhost:5432`):** Script Python langsung terhubung ke database PostgreSQL lokal tempat Apache MADlib dan Power BI kelompokmu berjalan. | **Terisolasi di Cloud:** Server Colab tidak bisa mengakses `localhost` PostgreSQL di laptopmu tanpa setup *tunneling* tambahan yang lambat. |
| **Kesesuaian dengan Dokumen Arsitektur** | **100% Sesuai Laporan Kelompok 11:** Selaras dengan klaim efisiensi komputasi lokal pada `Arsitektur_Kelompok 11.pdf`. | Bertentangan dengan alasan arsitektur lokal pada laporan kelompokmu. |
| **Kebutuhan Memori & Kecepatan (`41 MB`)** | Sangat ringan. Seluruh 536.350 baris hanya membutuhkan **188,74 MB RAM** dan selesai diproses + disimpan dalam **~5,3 detik**. | Lambat karena harus mengunggah (*upload*) ulang file CSV 41 MB setiap kali sesi runtime Colab terputus. |
| **Automasi Batch & Struktur Proyek** | Mendukung struktur proyek modular (`simba_ingestion_pipeline.py`, `simba_postgres_schema.sql`, `simba_ingestion.log`) dan siap diintegrasikan dengan Airflow/PySpark. | Berbentuk notebook interaktif (`.ipynb`) yang kurang ideal untuk *batch ingestion pipeline*. |

---

## 3. Hasil Audit & Eksekusi Data Cleaning Aktual (`Sales Transaction v.4a.csv`)

Berikut adalah metrik hasil eksekusi nyata dari `simba_ingestion_pipeline.py` terhadap dataset `Sales Transaction v.4a.csv`:

| Tahap Pemrosesan | Metrik / Indikator | Nilai Aktual | Penjelasan Teknis |
| :--- | :--- | :--- | :--- |
| **1. Data Mentah (Raw)** | Total Baris & Kolom Awal | `536.350 baris x 8 kolom` | Ukuran file di disk: `41,00 MB` (RAM: `188,74 MB`). |
| **2. Deduplikasi** | Baris Duplikat Identik Dihapus | `-5.200 baris` (`0,97%`) | Menyisakan **531.150 baris unik** bebas duplikasi. |
| **3. Imputasi Missing Value** | `CustomerNo` Kosong (`NaN`) | `55 baris` | Diisi label `'GUEST'` dan seluruh `CustomerNo` diformat ulang tanpa akhiran `.0`. |
| **4. Standarisasi Tanggal** | Rentang `transaction_date` | `2018-12-01` s.d. `2019-12-09` | Dikonversi dari format string `M/D/YYYY` ke standar SQL `YYYY-MM-DD` (`0` gagal parse). |
| **5. Pemisahan Order Batal** | Transaksi `CANCELLED` (`'C...'`) | `8.548 baris` (setelah dedup) | Seluruh baris dengan `Quantity <= 0` ditandai `is_cancelled = True` (`order_status = 'CANCELLED'`). |
| **6. Transaksi Bersih** | Transaksi `COMPLETED` | `522.602 baris` | Total nilai penjualan kotor (*gross revenue*): **£62.781.386,54**. |

---

## 4. Hasil Normalisasi ke 6 Tabel Utama PostgreSQL + 1 Tabel Bersih PySpark

Untuk memastikan implementasi kamu **100% sesuai dengan gambar diagram arsitektur Kelompok 11** (`customers`, `products`, `orders`, `order_items`, `payments`, `reviews`), script `simba_ingestion_pipeline.py` telah dilengkapi logika normalisasi otomatis untuk menghasilkan ketujuh tabel berikut:

| No | Nama Tabel PostgreSQL | Jumlah Baris | Jumlah Kolom | Daftar Kolom Utama | Fungsi dalam Arsitektur SIMBA |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **1** | `customers` | `4.739` | `2` | `customer_no` (PK), `country` | Menyimpan data master pelanggan unik (`4.738` terdaftar + `1` `'GUEST'`). |
| **2** | `products` | `3.768` | `3` | `product_no` (PK), `product_name`, `standard_price` | Menyimpan katalog produk unik beserta harga satuan standar (median). |
| **3** | `orders` | `23.204` | `8` | `transaction_no` (PK), `transaction_date`, `customer_no` (FK), `country`, `is_cancelled`, `order_status`, `total_items`, `total_amount` | Menyimpan header transaksi pesanan beserta status (`COMPLETED` / `CANCELLED`). |
| **4** | `order_items` | `531.150` | `6` | `item_id` (PK), `transaction_no` (FK), `product_no` (FK), `quantity`, `unit_price`, `line_total` | Menyimpan rincian item barang per nomor transaksi. |
| **5** | `payments` | `23.204` | `6` | `payment_id` (PK), `transaction_no` (FK), `payment_date`, `payment_method`, `payment_status`, `amount_paid` | Menyimpan status pembayaran (`PAID` untuk order sukses, `REFUNDED` untuk order batal). |
| **6** | `reviews` | `23.204` | `6` | `review_id` (PK), `transaction_no` (FK), `customer_no` (FK), `rating`, `review_comment`, `review_date` | Menyimpan rating (`1–5`) dan ulasan transaksi pelanggan sesuai skema Storage Layer. |
| **7** | `clean_sales_transactions` | `522.602` | `13` | `transaction_no`, `transaction_date`, `year`, `month`, `day`, `day_of_week`, `product_no`, `product_name`, `unit_price`, `quantity`, `line_total`, `customer_no`, `country` | Tabel bersih khusus transaksi sukses (`COMPLETED`) yang langsung siap dibaca oleh **PySpark (Layer 4)** & **Apache MADlib (Layer 5)**. |

---

## 5. Cuplikan Sampel Data Hasil Transformasi (`simba_ingestion_pipeline.py`)

### A. Sampel Tabel `customers` (`4.739 baris`)
```text
customer_no        country
      12004 United Kingdom
      12006 United Kingdom
      12008 United Kingdom
      12013 United Kingdom
      12024 United Kingdom
```

### B. Sampel Tabel `products` (`3.768 baris`)
```text
product_no                product_name  standard_price
     10002  Inflatable Political Globe           11.12
     10080    Groovy Cactus Inflatable           10.65
     10120                Doggy Rubber           10.47
    10123C        Hearts Wrapping Tape           10.92
    10124A Spots On Red Bookcover Tape           10.68
```

### C. Sampel Tabel `orders` (`23.204 baris`)
```text
transaction_no transaction_date customer_no        country  is_cancelled order_status  total_items  total_amount
        536365       2018-12-01       17850 United Kingdom         False    COMPLETED           40        552.52
        536366       2018-12-01       17850 United Kingdom         False    COMPLETED           12        145.80
        536367       2018-12-01       13047 United Kingdom         False    COMPLETED           83       1136.34
        536368       2018-12-01       13047 United Kingdom         False    COMPLETED           15        225.54
        536369       2018-12-01       13047 United Kingdom         False    COMPLETED            3         49.05
```

### D. Sampel Tabel `order_items` (`531.150 baris`)
```text
item_id transaction_no product_no  quantity  unit_price  line_total
      1         581482      22485        12       21.47      257.64
      2         581475      22596        36       10.65      383.40
      3         581475      23235        12       11.53      138.36
      4         581475      23272        12       10.65      127.80
      5         581475      23239         6       11.94       71.64
```

### E. Sampel Tabel `payments` (`23.204 baris`)
```text
payment_id transaction_no payment_date payment_method payment_status  amount_paid
         1         536365   2018-12-01         PayPal           PAID       552.52
         2         536366   2018-12-01    Credit Card           PAID       145.80
         3         536367   2018-12-01         PayPal           PAID      1136.34
         4         536368   2018-12-01  Bank Transfer           PAID       225.54
         5         536369   2018-12-01    Credit Card           PAID        49.05
```

### F. Sampel Tabel `reviews` (`23.204 baris`)
```text
review_id transaction_no customer_no  rating                                             review_comment review_date
        1         536365       17850       5 Produk berkualitas sangat baik dan pengiriman tepat waktu.  2018-12-01
        2         536366       17850       5 Produk berkualitas sangat baik dan pengiriman tepat waktu.  2018-12-01
        3         536367       13047       5 Produk berkualitas sangat baik dan pengiriman tepat waktu.  2018-12-01
        4         536368       13047       5 Produk berkualitas sangat baik dan pengiriman tepat waktu.  2018-12-01
        5         536369       13047       5 Produk berkualitas sangat baik dan pengiriman tepat waktu.  2018-12-01
```

---

## 6. Langkah Eksekusi Lengkap di Antigravity IDE

1. **Instalasi Dependensi di Terminal Antigravity IDE:**
   ```bash
   pip install pandas numpy sqlalchemy psycopg2-binary
   ```
2. **Buat Database & Tabel di PostgreSQL (Opsional via `psql` / pgAdmin):**
   Jalankan file `simba_postgres_schema.sql` pada database `simba_db` di PostgreSQL lokalmu.
3. **Jalankan Batch Ingestion ke PostgreSQL:**
   ```bash
   python simba_ingestion_pipeline.py \
     --csv "Sales Transaction v.4a.csv" \
     --mode postgres \
     --db-url "postgresql+psycopg2://postgres:password_kamu@localhost:5432/simba_db"
   ```
4. **Jalankan Mode Lokal (Jika Ingin Mengekspor 7 File CSV Bersih ke Folder Lokal):**
   ```bash
   python simba_ingestion_pipeline.py \
     --csv "Sales Transaction v.4a.csv" \
     --mode local \
     --output-dir "./output_clean_tables"
   ```
