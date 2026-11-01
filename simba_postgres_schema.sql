-- ============================================================================
-- PROYEK BIG DATA: SIMBA (Sistem Informasi Monitor Batch & Agregasi Stok)
-- Kelompok 11 - Teknologi Rekayasa Perangkat Lunak, Sekolah Vokasi IPB University
-- ============================================================================
-- File        : simba_postgres_schema.sql
-- Deskripsi   : Skema DDL PostgreSQL untuk Layer 3 (Storage Layer) yang mencakup
--               6 Tabel Utama sesuai diagram arsitektur Kelompok 11:
--               1. customers
--               2. products
--               3. orders
--               4. order_items
--               5. payments
--               6. reviews
--               Serta 1 tabel bersih ('clean_sales_transactions') dan View
--               agregasi untuk konsumsi langsung PySpark & Apache MADlib.
-- ============================================================================

-- 1. Tabel Master Pelanggan (4.739 baris unik termasuk 'GUEST')
CREATE TABLE IF NOT EXISTS customers (
    customer_no VARCHAR(20) PRIMARY KEY,
    country VARCHAR(100) NOT NULL
);

-- 2. Tabel Master Produk (3.768 produk unik)
CREATE TABLE IF NOT EXISTS products (
    product_no VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    standard_price NUMERIC(12, 2) NOT NULL CHECK (standard_price > 0)
);

-- 3. Tabel Header Pesanan / Transaksi (23.204 transaksi unik)
CREATE TABLE IF NOT EXISTS orders (
    transaction_no VARCHAR(20) PRIMARY KEY,
    transaction_date DATE NOT NULL,
    customer_no VARCHAR(20) NOT NULL REFERENCES customers(customer_no) ON UPDATE CASCADE,
    country VARCHAR(100) NOT NULL,
    is_cancelled BOOLEAN NOT NULL DEFAULT FALSE,
    order_status VARCHAR(20) NOT NULL CHECK (order_status IN ('COMPLETED', 'CANCELLED')),
    total_items INT NOT NULL,
    total_amount NUMERIC(14, 2) NOT NULL
);

-- 4. Tabel Detail Item Pesanan (531.150 baris item setelah deduplikasi)
CREATE TABLE IF NOT EXISTS order_items (
    item_id SERIAL PRIMARY KEY,
    transaction_no VARCHAR(20) NOT NULL REFERENCES orders(transaction_no) ON DELETE CASCADE,
    product_no VARCHAR(20) NOT NULL REFERENCES products(product_no) ON UPDATE CASCADE,
    quantity INT NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price > 0),
    line_total NUMERIC(14, 2) NOT NULL
);

-- 5. Tabel Pembayaran / Payments (23.204 baris, relasi 1-ke-1 dengan orders)
CREATE TABLE IF NOT EXISTS payments (
    payment_id SERIAL PRIMARY KEY,
    transaction_no VARCHAR(20) NOT NULL UNIQUE REFERENCES orders(transaction_no) ON DELETE CASCADE,
    payment_date DATE NOT NULL,
    payment_method VARCHAR(50) NOT NULL,
    payment_status VARCHAR(20) NOT NULL CHECK (payment_status IN ('PAID', 'REFUNDED')),
    amount_paid NUMERIC(14, 2) NOT NULL CHECK (amount_paid >= 0)
);

-- 6. Tabel Ulasan Pelanggan / Reviews (23.204 baris, relasi ke orders & customers)
CREATE TABLE IF NOT EXISTS reviews (
    review_id SERIAL PRIMARY KEY,
    transaction_no VARCHAR(20) NOT NULL UNIQUE REFERENCES orders(transaction_no) ON DELETE CASCADE,
    customer_no VARCHAR(20) NOT NULL REFERENCES customers(customer_no) ON UPDATE CASCADE,
    rating INT NOT NULL CHECK (rating BETWEEN 1 AND 5),
    review_comment TEXT,
    review_date DATE NOT NULL
);

-- 7. Tabel Denormalisasi Bersih (522.602 baris COMPLETED untuk PySpark & MADlib)
CREATE TABLE IF NOT EXISTS clean_sales_transactions (
    transaction_no VARCHAR(20) NOT NULL,
    transaction_date DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    day_of_week VARCHAR(15) NOT NULL,
    product_no VARCHAR(20) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL,
    quantity INT NOT NULL,
    line_total NUMERIC(14, 2) NOT NULL,
    customer_no VARCHAR(20) NOT NULL,
    country VARCHAR(100) NOT NULL
);

-- ============================================================================
-- INDEXING UNTUK OPTIMASI KUERI PYSPARK & IN-DATABASE ANALYTICS (MADLIB)
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_orders_date ON orders (transaction_date);
CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders (customer_no);
CREATE INDEX IF NOT EXISTS idx_order_items_tx ON order_items (transaction_no);
CREATE INDEX IF NOT EXISTS idx_order_items_product ON order_items (product_no);
CREATE INDEX IF NOT EXISTS idx_clean_tx_date ON clean_sales_transactions (transaction_date);
CREATE INDEX IF NOT EXISTS idx_clean_tx_year_month ON clean_sales_transactions (year, month);
CREATE INDEX IF NOT EXISTS idx_clean_tx_product ON clean_sales_transactions (product_no);

-- ============================================================================
-- KUERI VERIFIKASI & MONITORING SETELAH BATCH INGESTION
-- ============================================================================
-- Jalankan kueri di bawah ini di pgAdmin / DBeaver / psql untuk memverifikasi
-- bahwa seluruh tabel pada Storage Layer sudah terisi dengan benar:

SELECT 'customers' AS table_name, COUNT(*) AS total_rows FROM customers
UNION ALL
SELECT 'products', COUNT(*) FROM products
UNION ALL
SELECT 'orders', COUNT(*) FROM orders
UNION ALL
SELECT 'order_items', COUNT(*) FROM order_items
UNION ALL
SELECT 'payments', COUNT(*) FROM payments
UNION ALL
SELECT 'reviews', COUNT(*) FROM reviews
UNION ALL
SELECT 'clean_sales_transactions', COUNT(*) FROM clean_sales_transactions;