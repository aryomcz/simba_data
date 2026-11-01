#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
PROYEK BIG DATA: SIMBA (Sistem Informasi Monitor Batch & Agregasi Stok)
Kelompok 11 - Teknologi Rekayasa Perangkat Lunak, Sekolah Vokasi IPB University
================================================================================
Modul       : Layer 2 (Ingestion Layer - Pandas) & Layer 3 (Storage Layer - PostgreSQL)
Deskripsi   : Script Batch Ingestion end-to-end untuk membaca dataset transaksi
              e-commerce ('Sales Transaction v.4a.csv'), melakukan validasi,
              pembersihan data (data cleaning), transformasi & normalisasi ke
              6 Tabel Utama PostgreSQL sesuai diagram arsitektur Kelompok 11:
              1. customers
              2. products
              3. orders
              4. order_items
              5. payments
              6. reviews
              Serta 1 tabel denormalisasi bersih ('clean_sales_transactions')
              untuk konsumsi langsung Layer 4 (PySpark) & Layer 5 (Apache MADlib).
Penulis     : Data Engineering Team - Kelompok 11
================================================================================
"""

import argparse
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Dict

import numpy as np
import pandas as pd


# ==============================================================================
# 1. KONFIGURASI LOGGING (INFORMASI & MONITORING PIPELINE)
# ==============================================================================
def setup_logger(log_file: str = "simba_ingestion.log") -> logging.Logger:
    """Mengatur format logging agar rapi di terminal maupun di file log."""
    logger = logging.getLogger("SIMBA_Ingestion")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-7s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Handler untuk layar terminal (Console)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Handler untuk menyimpan riwayat eksekusi ke file log
        file_handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


# ==============================================================================
# 2. DATA CLASS UNTUK MENYIMPAN METRIK LAPORAN DATA QUALITY
# ==============================================================================
@dataclass
class DataQualityReport:
    """Menyimpan ringkasan metrik sebelum dan sesudah proses Data Cleaning."""
    file_size_mb: float = 0.0
    raw_row_count: int = 0
    raw_col_count: int = 0
    exact_duplicates_count: int = 0
    missing_customer_count: int = 0
    cancelled_raw_count: int = 0
    negative_qty_raw_count: int = 0
    invalid_price_count: int = 0
    dedup_row_count: int = 0
    completed_tx_count: int = 0
    cancelled_dedup_count: int = 0
    min_date: str = ""
    max_date: str = ""
    min_price: float = 0.0
    max_price: float = 0.0
    total_gross_revenue: float = 0.0


# ==============================================================================
# 3. KELAS UTAMA PIPELINE INGESTION & CLEANING (PANDAS -> POSTGRESQL)
# ==============================================================================
class SimbaIngestionPipeline:
    """
    Kelas modular untuk menjalankan proses Batch Ingestion SIMBA:
    1. Extract   : Membaca CSV mentah & mencatat profil memori.
    2. Validate  : Memvalidasi skema kolom & mendeteksi anomali data.
    3. Clean     : Menghapus duplikat, imputasi nilai kosong, & standarisasi tipe.
    4. Transform : Membentuk 6 tabel relasional PostgreSQL sesuai diagram arsitektur
                   ('customers', 'products', 'orders', 'order_items', 'payments', 'reviews')
                   + 1 tabel bersih ('clean_sales_transactions') siap pakai untuk PySpark.
    5. Load      : Menyimpan tabel ke PostgreSQL (atau mengekspor CSV bersih).
    """

    REQUIRED_COLUMNS = [
        "TransactionNo",
        "Date",
        "ProductNo",
        "ProductName",
        "Price",
        "Quantity",
        "CustomerNo",
        "Country",
    ]

    def __init__(self, csv_path: str, logger: logging.Logger):
        self.csv_path = csv_path
        self.logger = logger
        self.report = DataQualityReport()

    # --------------------------------------------------------------------------
    # TAHAP 1: EXTRACT (MEMBACA CSV)
    # --------------------------------------------------------------------------
    def extract_csv(self) -> pd.DataFrame:
        """Membaca file CSV mentah dan mencatat informasi ukuran serta memori."""
        self.logger.info("=" * 80)
        self.logger.info("[TAHAP 1 - EXTRACT] Membaca dataset mentah dari sumber CSV...")

        if not os.path.exists(self.csv_path):
            raise FileNotFoundError(f"File CSV tidak ditemukan di: {self.csv_path}")

        start_time = time.time()
        self.report.file_size_mb = os.path.getsize(self.csv_path) / (1024 * 1024)

        df_raw = pd.read_csv(
            self.csv_path,
            dtype={
                "TransactionNo": "string",
                "Date": "string",
                "ProductNo": "string",
                "ProductName": "string",
                "Price": "float64",
                "Quantity": "int64",
                "CustomerNo": "float64",  # float64 karena terdapat NaN pada data mentah
                "Country": "string",
            },
        )

        elapsed = time.time() - start_time
        mem_usage_mb = df_raw.memory_usage(deep=True).sum() / (1024 * 1024)
        self.report.raw_row_count, self.report.raw_col_count = df_raw.shape

        self.logger.info(f"  -> File Sumber        : {os.path.basename(self.csv_path)}")
        self.logger.info(f"  -> Ukuran File Disk   : {self.report.file_size_mb:.2f} MB")
        self.logger.info(
            f"  -> Dimensi Data Awal  : {self.report.raw_row_count:,} baris x {self.report.raw_col_count} kolom"
        )
        self.logger.info(f"  -> Penggunaan RAM     : {mem_usage_mb:.2f} MB")
        self.logger.info(f"  -> Waktu Baca CSV     : {elapsed:.2f} detik")

        return df_raw

    # --------------------------------------------------------------------------
    # TAHAP 2: VALIDATE (VALIDASI SKEMA & AUDIT ANOMALI DATA)
    # --------------------------------------------------------------------------
    def validate_data(self, df: pd.DataFrame) -> None:
        """Memeriksa kelengkapan kolom, nilai kosong, duplikat, dan anomali nilai."""
        self.logger.info("-" * 80)
        self.logger.info("[TAHAP 2 - VALIDATE] Mengaudit kualitas dan anomali data mentah...")

        # 1. Validasi keberadaan kolom wajib
        missing_cols = [c for c in self.REQUIRED_COLUMNS if c not in df.columns]
        if missing_cols:
            raise ValueError(f"Kolom wajib tidak ditemukan di CSV: {missing_cols}")
        self.logger.info(f"  -> Validasi Skema     : LULUS ({len(self.REQUIRED_COLUMNS)} kolom lengkap)")

        # 2. Audit baris duplikat identik
        self.report.exact_duplicates_count = int(df.duplicated().sum())
        dup_pct = (self.report.exact_duplicates_count / len(df)) * 100

        # 3. Audit missing values per kolom
        null_counts = df.isna().sum().to_dict()
        self.report.missing_customer_count = int(null_counts.get("CustomerNo", 0))

        # 4. Audit transaksi pembatalan (Cancelled) dan kuantitas negatif
        is_cancelled_mask = df["TransactionNo"].str.startswith("C", na=False)
        self.report.cancelled_raw_count = int(is_cancelled_mask.sum())
        self.report.negative_qty_raw_count = int((df["Quantity"] <= 0).sum())

        # 5. Audit harga produk (Price)
        self.report.invalid_price_count = int((df["Price"] <= 0).sum())
        self.report.min_price = float(df["Price"].min())
        self.report.max_price = float(df["Price"].max())

        self.logger.info(
            f"  -> Duplikat Identik   : {self.report.exact_duplicates_count:,} baris ({dup_pct:.2f}%)"
        )
        self.logger.info(
            f"  -> Missing Values     : CustomerNo = {self.report.missing_customer_count:,} baris | "
            f"Kolom lain = {sum(v for k, v in null_counts.items() if k != 'CustomerNo')} baris"
        )
        self.logger.info(
            f"  -> Transaksi Cancel   : {self.report.cancelled_raw_count:,} baris berawalan 'C' "
            f"(Baris Quantity <= 0: {self.report.negative_qty_raw_count:,} baris)"
        )
        self.logger.info(
            f"  -> Validasi Harga     : {self.report.invalid_price_count} baris invalid "
            f"(Rentang Harga: {self.report.min_price:.2f} s.d. {self.report.max_price:.2f})"
        )

    # --------------------------------------------------------------------------
    # TAHAP 3: CLEAN (PEMBERSIHAN & STANDARISASI DATA)
    # --------------------------------------------------------------------------
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Melakukan pembersihan data secara sistematis:
        1. Menghapus duplikat identik (exact duplicates).
        2. Merapikan spasi berlebih (whitespace trimming) pada kolom teks.
        3. Menangani missing values pada CustomerNo menjadi 'GUEST' & konversi ke string bersih.
        4. Mengonversi kolom Date dari string 'M/D/YYYY' menjadi tipe tanggal standar.
        5. Menandai status transaksi ('COMPLETED' vs 'CANCELLED').
        6. Membuang baris dengan harga tidak valid (Price <= 0) jika ada.
        """
        self.logger.info("-" * 80)
        self.logger.info("[TAHAP 3 - CLEAN] Menjalankan pembersihan & standarisasi data...")

        # 1. Hapus duplikat identik
        df_clean = df.drop_duplicates().copy()
        self.report.dedup_row_count = len(df_clean)
        self.logger.info(
            f"  -> [CLEAN-1] Hapus Duplikat   : {self.report.exact_duplicates_count:,} baris dihapus "
            f"-> Sisa {self.report.dedup_row_count:,} baris unik"
        )

        # 2. Standarisasi teks (Trimming spasi di awal/akhir string)
        for text_col in ["TransactionNo", "ProductNo", "ProductName", "Country"]:
            df_clean[text_col] = df_clean[text_col].astype("string").str.strip()
        self.logger.info(
            "  -> [CLEAN-2] Standarisasi Teks: Trimming whitespace pada TransactionNo, ProductNo, ProductName, Country"
        )

        # 3. Penanganan Missing Value pada CustomerNo & konversi tipe (17490.0 -> '17490')
        missing_cust_after_dedup = int(df_clean["CustomerNo"].isna().sum())
        df_clean["CustomerNo"] = (
            df_clean["CustomerNo"]
            .fillna(-1)
            .astype("int64")
            .astype("string")
            .replace("-1", "GUEST")
        )
        self.logger.info(
            f"  -> [CLEAN-3] Imputasi Customer: {missing_cust_after_dedup} baris kosong diisi label 'GUEST' "
            "dan diformat ulang tanpa desimal (.0)"
        )

        # 4. Konversi format Tanggal (M/D/YYYY -> YYYY-MM-DD)
        df_clean["Date"] = pd.to_datetime(df_clean["Date"], format="%m/%d/%Y", errors="coerce")
        invalid_dates = int(df_clean["Date"].isna().sum())
        if invalid_dates > 0:
            self.logger.warning(f"  -> Ditemukan {invalid_dates} tanggal invalid, menghapus baris terkait...")
            df_clean = df_clean.dropna(subset=["Date"])

        self.report.min_date = df_clean["Date"].min().strftime("%Y-%m-%d")
        self.report.max_date = df_clean["Date"].max().strftime("%Y-%m-%d")
        self.logger.info(
            f"  -> [CLEAN-4] Konversi Tanggal : Berhasil dikonversi ke YYYY-MM-DD "
            f"(Rentang: {self.report.min_date} s.d. {self.report.max_date})"
        )

        # 5. Penandaan Transaksi Dibatalkan (Cancelled Orders) & Kuantitas Negatif
        df_clean["IsCancelled"] = df_clean["TransactionNo"].str.startswith("C", na=False) | (
            df_clean["Quantity"] <= 0
        )
        df_clean["OrderStatus"] = df_clean["IsCancelled"].map(
            {True: "CANCELLED", False: "COMPLETED"}
        )

        self.report.cancelled_dedup_count = int(df_clean["IsCancelled"].sum())
        self.report.completed_tx_count = int((~df_clean["IsCancelled"]).sum())
        self.logger.info(
            f"  -> [CLEAN-5] Klasifikasi Order: COMPLETED = {self.report.completed_tx_count:,} baris | "
            f"CANCELLED = {self.report.cancelled_dedup_count:,} baris"
        )

        # 6. Filter harga tidak masuk akal (Price <= 0)
        if (df_clean["Price"] <= 0).any():
            df_clean = df_clean[df_clean["Price"] > 0].copy()

        return df_clean

    # --------------------------------------------------------------------------
    # TAHAP 4: TRANSFORM (TRANSFORMASI AWAL & NORMALISASI 6 TABEL RELASIONAL)
    # --------------------------------------------------------------------------
    def transform_data(self, df_clean: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Melakukan transformasi awal:
        1. Menghitung metrik nilai transaksi (line_total = Price * Quantity).
        2. Mengekstrak komponen waktu (year, month, day, day_of_week) untuk kebutuhan analitik.
        3. Memecah (normalisasi) dataset flat menjadi 6 Tabel Utama PostgreSQL sesuai diagram:
           - Tabel 1: customers
           - Tabel 2: products
           - Tabel 3: orders
           - Tabel 4: order_items
           - Tabel 5: payments (diturunkan secara deterministik dari orders)
           - Tabel 6: reviews (diturunkan secara deterministik dari orders)
           + Tabel 7: clean_sales_transactions (siap pakai untuk PySpark & Apache MADlib).
        """
        self.logger.info("-" * 80)
        self.logger.info(
            "[TAHAP 4 - TRANSFORM] Menjalankan transformasi awal & normalisasi 6 tabel relasional..."
        )

        # 1. Hitung total nilai per baris item (line_total)
        df_clean["LineTotal"] = (df_clean["Price"] * df_clean["Quantity"]).round(2)

        # 2. Rename kolom ke standar snake_case SQL PostgreSQL
        df_sql = df_clean.rename(
            columns={
                "TransactionNo": "transaction_no",
                "Date": "transaction_date",
                "ProductNo": "product_no",
                "ProductName": "product_name",
                "Price": "unit_price",
                "Quantity": "quantity",
                "CustomerNo": "customer_no",
                "Country": "country",
                "IsCancelled": "is_cancelled",
                "OrderStatus": "order_status",
                "LineTotal": "line_total",
            }
        )

        # 3. Normalisasi ke Tabel 1: customers
        df_customers = (
            df_sql.groupby("customer_no", as_index=False)
            .agg(country=("country", "first"))
            .sort_values("customer_no")
            .reset_index(drop=True)
        )

        # 4. Normalisasi ke Tabel 2: products
        df_products = (
            df_sql.groupby("product_no", as_index=False)
            .agg(
                product_name=("product_name", "first"),
                standard_price=("unit_price", "median"),
            )
            .sort_values("product_no")
            .reset_index(drop=True)
        )
        df_products["standard_price"] = df_products["standard_price"].round(2)

        # 5. Normalisasi ke Tabel 3: orders (Header Transaksi)
        df_orders = (
            df_sql.groupby("transaction_no", as_index=False)
            .agg(
                transaction_date=("transaction_date", "first"),
                customer_no=("customer_no", "first"),
                country=("country", "first"),
                is_cancelled=("is_cancelled", "first"),
                order_status=("order_status", "first"),
                total_items=("quantity", "sum"),
                total_amount=("line_total", "sum"),
            )
            .sort_values(["transaction_date", "transaction_no"])
            .reset_index(drop=True)
        )
        df_orders["total_amount"] = df_orders["total_amount"].round(2)
        df_orders["transaction_date"] = df_orders["transaction_date"].dt.strftime("%Y-%m-%d")

        # 6. Normalisasi ke Tabel 4: order_items (Detail Barang per Transaksi)
        df_order_items = df_sql[
            ["transaction_no", "product_no", "quantity", "unit_price", "line_total"]
        ].copy()
        df_order_items.insert(0, "item_id", range(1, len(df_order_items) + 1))

        # 7. Tabel 5: payments (Sesuai Diagram Storage Layer PostgreSQL)
        # Diturunkan secara deterministik (seed=42) dari setiap transaksi di tabel orders
        rng = np.random.default_rng(seed=42)
        payment_methods = np.array(["Credit Card", "Bank Transfer", "PayPal", "Debit Card"])
        method_probs = [0.45, 0.25, 0.20, 0.10]

        df_payments = pd.DataFrame(
            {
                "payment_id": range(1, len(df_orders) + 1),
                "transaction_no": df_orders["transaction_no"],
                "payment_date": df_orders["transaction_date"],
                "payment_method": rng.choice(payment_methods, size=len(df_orders), p=method_probs),
                "payment_status": np.where(df_orders["is_cancelled"], "REFUNDED", "PAID"),
                "amount_paid": df_orders["total_amount"].abs().round(2),
            }
        )

        # 8. Tabel 6: reviews (Sesuai Diagram Storage Layer PostgreSQL)
        # Diturunkan secara deterministik untuk pesanan berdasarkan status pesanan
        completed_ratings = rng.choice([3, 4, 5], size=len(df_orders), p=[0.10, 0.35, 0.55])
        cancelled_ratings = rng.choice([1, 2, 3], size=len(df_orders), p=[0.50, 0.35, 0.15])
        ratings = np.where(df_orders["is_cancelled"], cancelled_ratings, completed_ratings)

        review_map = {
            5: "Produk berkualitas sangat baik dan pengiriman tepat waktu.",
            4: "Kualitas barang sesuai deskripsi dan pengemasan rapi.",
            3: "Produk cukup baik namun pengiriman sedikit terlambat.",
            2: "Pesanan dibatalkan karena kendala ketersediaan stok.",
            1: "Transaksi dibatalkan / retur barang.",
        }

        df_reviews = pd.DataFrame(
            {
                "review_id": range(1, len(df_orders) + 1),
                "transaction_no": df_orders["transaction_no"],
                "customer_no": df_orders["customer_no"],
                "rating": ratings,
                "review_comment": [review_map[int(r)] for r in ratings],
                "review_date": df_orders["transaction_date"],
            }
        )

        # 9. Tabel 7: clean_sales_transactions (Hanya transaksi COMPLETED & atribut waktu lengkap)
        # Tabel ini memudahkan rekan satu tim di Layer 4 (PySpark) & Layer 5 (MADlib)
        df_completed = df_sql[~df_sql["is_cancelled"]].copy()
        df_completed["year"] = df_completed["transaction_date"].dt.year
        df_completed["month"] = df_completed["transaction_date"].dt.month
        df_completed["day"] = df_completed["transaction_date"].dt.day
        df_completed["day_of_week"] = df_completed["transaction_date"].dt.day_name()
        df_completed["transaction_date"] = df_completed["transaction_date"].dt.strftime("%Y-%m-%d")

        df_clean_sales = df_completed[
            [
                "transaction_no",
                "transaction_date",
                "year",
                "month",
                "day",
                "day_of_week",
                "product_no",
                "product_name",
                "unit_price",
                "quantity",
                "line_total",
                "customer_no",
                "country",
            ]
        ].reset_index(drop=True)

        self.report.total_gross_revenue = float(df_clean_sales["line_total"].sum())

        tables = {
            "customers": df_customers,
            "products": df_products,
            "orders": df_orders,
            "order_items": df_order_items,
            "payments": df_payments,
            "reviews": df_reviews,
            "clean_sales_transactions": df_clean_sales,
        }

        for tbl_name, tbl_df in tables.items():
            self.logger.info(
                f"  -> Tabel '{tbl_name:<24}' : {len(tbl_df):>9,} baris | {tbl_df.shape[1]:>2} kolom"
            )

        return tables

    # --------------------------------------------------------------------------
    # TAHAP 5A: LOAD KE LOCAL DIRECTORY (UNTUK UJI COBA / VERIFIKASI)
    # --------------------------------------------------------------------------
    def save_to_local(self, tables: Dict[str, pd.DataFrame], output_dir: str) -> None:
        """Menyimpan seluruh tabel hasil transformasi ke dalam folder CSV lokal."""
        self.logger.info("-" * 80)
        self.logger.info(
            f"[TAHAP 5 - LOAD (LOCAL MODE)] Menyimpan tabel bersih ke direktori: {output_dir}"
        )
        os.makedirs(output_dir, exist_ok=True)

        for tbl_name, tbl_df in tables.items():
            out_path = os.path.join(output_dir, f"{tbl_name}.csv")
            tbl_df.to_csv(out_path, index=False)
            size_mb = os.path.getsize(out_path) / (1024 * 1024)
            self.logger.info(f"  -> Tersimpan: {out_path:<45} ({size_mb:>6.2f} MB)")

    # --------------------------------------------------------------------------
    # TAHAP 5B: BATCH INGESTION KE POSTGRESQL (STORAGE LAYER)
    # --------------------------------------------------------------------------
    def load_to_postgres(
        self,
        tables: Dict[str, pd.DataFrame],
        db_url: str,
        chunk_size: int = 10000,
    ) -> None:
        """
        Melakukan Batch Ingestion ke database PostgreSQL menggunakan SQLAlchemy.
        Mendukung penulisan bertahap (chunking) agar hemat RAM lokal.
        """
        self.logger.info("-" * 80)
        self.logger.info("[TAHAP 5 - LOAD (POSTGRESQL MODE)] Memulai Batch Ingestion ke PostgreSQL...")

        try:
            from sqlalchemy import create_engine, text
        except ImportError as exc:
            raise ImportError(
                "Library 'sqlalchemy' dan 'psycopg2-binary' diperlukan untuk koneksi PostgreSQL. "
                "Jalankan: pip install sqlalchemy psycopg2-binary"
            ) from exc

        engine = create_engine(db_url)

        # Urutan pemuatan harus dimulai dari tabel master (customers, products)
        # sebelum tabel transaksi (orders, order_items, payments, reviews)
        load_order = [
            "customers",
            "products",
            "orders",
            "order_items",
            "payments",
            "reviews",
            "clean_sales_transactions",
        ]

        with engine.begin() as conn:
            conn.execute(
                text(
                    "TRUNCATE TABLE clean_sales_transactions, reviews, payments, "
                    "order_items, orders, products, customers CASCADE;"
                )
            )
            for tbl_name in load_order:
                tbl_df = tables[tbl_name]
                t0 = time.time()
                self.logger.info(
                    f"  -> Memuat tabel '{tbl_name}' ({len(tbl_df):,} baris, chunksize={chunk_size:,})..."
                )
                tbl_df.to_sql(
                    name=tbl_name,
                    con=conn,
                    if_exists="append",
                    index=False,
                    chunksize=chunk_size,
                    method="multi",
                )
                elapsed = time.time() - t0
                self.logger.info(
                    f"     [OK] Tabel '{tbl_name}' berhasil dimuat dalam {elapsed:.2f} detik."
                )

            # Buat Index untuk mempercepat pembacaan PySpark & kueri Time-Series MADlib
            self.logger.info("  -> Membuat Index di PostgreSQL untuk optimasi query PySpark & MADlib...")
            conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_orders_date ON orders (transaction_date);")
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_clean_tx_date "
                    "ON clean_sales_transactions (transaction_date);"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_clean_tx_product "
                    "ON clean_sales_transactions (product_no);"
                )
            )

        self.logger.info("  -> Seluruh 6 tabel utama + tabel clean_sales_transactions berhasil dibuat!")

    # --------------------------------------------------------------------------
    # CETAK RINGKASAN AKHIR (EXECUTIVE SUMMARY)
    # --------------------------------------------------------------------------
    def print_final_summary(self) -> None:
        """Mencetak ringkasan eksekutif hasil kerja pipeline."""
        r = self.report
        self.logger.info("=" * 80)
        self.logger.info("RINGKASAN EKSEKUTIF PIPELINE INGESTION & DATA CLEANING (SIMBA)")
        self.logger.info("=" * 80)
        self.logger.info(f"  1. Total Baris Mentah (CSV)        : {r.raw_row_count:>10,} baris")
        self.logger.info(f"  2. Duplikat Identik Dihapus        : {r.exact_duplicates_count:>10,} baris (-)")
        self.logger.info(f"  3. Total Baris Unik (Deduplicated) : {r.dedup_row_count:>10,} baris")
        self.logger.info(f"  4. Transaksi Dibatalkan (CANCELLED): {r.cancelled_dedup_count:>10,} baris")
        self.logger.info(f"  5. Transaksi Sukses (COMPLETED)    : {r.completed_tx_count:>10,} baris")
        self.logger.info(f"  6. CustomerNo Kosong Diimputasi    : {r.missing_customer_count:>10,} baris ('GUEST')")
        self.logger.info(f"  7. Periode Waktu Transaksi         : {r.min_date} s.d. {r.max_date}")
        self.logger.info(f"  8. Total Gross Revenue (Completed) : £{r.total_gross_revenue:>14,.2f}")
        self.logger.info("=" * 80)


# ==============================================================================
# 4. ENTRY POINT CLI (COMMAND LINE INTERFACE)
# ==============================================================================
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="SIMBA Batch Ingestion & Data Cleaning Pipeline (Kelompok 11)"
    )
    parser.add_argument(
        "--csv",
        type=str,
        default="Sales Transaction v.4a.csv",
        help="Path ke file sumber 'Sales Transaction v.4a.csv'",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["local", "postgres", "dry-run"],
        default="dry-run",
        help="Mode eksekusi: 'dry-run' (hanya validasi & log), 'local' (simpan CSV bersih), atau 'postgres' (ingest ke PostgreSQL)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./output_clean_tables",
        help="Folder tujuan jika memilih --mode local",
    )
    parser.add_argument(
        "--db-url",
        type=str,
        default="postgresql+psycopg2://postgres:postgres@localhost:5432/simba_db",
        help="Connection string SQLAlchemy PostgreSQL jika memilih --mode postgres",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=10000,
        help="Jumlah baris per batch saat menulis ke PostgreSQL (default: 10000)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logger = setup_logger()

    pipeline_start = time.time()
    pipeline = SimbaIngestionPipeline(csv_path=args.csv, logger=logger)

    # Jalankan tahapan pipeline secara berurutan
    df_raw = pipeline.extract_csv()
    pipeline.validate_data(df_raw)
    df_clean = pipeline.clean_data(df_raw)
    tables = pipeline.transform_data(df_clean)

    if args.mode == "local":
        pipeline.save_to_local(tables=tables, output_dir=args.output_dir)
    elif args.mode == "postgres":
        pipeline.load_to_postgres(
            tables=tables, db_url=args.db_url, chunk_size=args.chunk_size
        )
    else:
        logger.info("-" * 80)
        logger.info(
            "[TAHAP 5 - DRY-RUN MODE] Validasi & transformasi selesai (tanpa menulis ke disk/DB)."
        )

    pipeline.print_final_summary()
    total_time = time.time() - pipeline_start
    logger.info(f"Pipeline selesai dengan sukses dalam total waktu {total_time:.2f} detik.")


if __name__ == "__main__":
    main()