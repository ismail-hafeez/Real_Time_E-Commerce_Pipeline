-- ============================================================
-- E-Commerce Data Warehouse — Star Schema
-- Auto-runs on first PostgreSQL container startup
-- ============================================================

-- ─── DIMENSION: CUSTOMER ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key    SERIAL PRIMARY KEY,
    customer_id     VARCHAR(50) UNIQUE,
    country         VARCHAR(100),
    first_purchase_date DATE,
    last_purchase_date  DATE,
    total_orders    INT DEFAULT 0,
    rfm_segment     VARCHAR(50)
);

-- ─── DIMENSION: PRODUCT ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_product (
    product_key     SERIAL PRIMARY KEY,
    stock_code      VARCHAR(50) UNIQUE,
    description     VARCHAR(500),
    category        VARCHAR(200),
    avg_price_gbp   DECIMAL(10, 2)
);

-- ─── DIMENSION: DATE ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INT PRIMARY KEY,           -- Format: YYYYMMDD
    full_date       DATE UNIQUE NOT NULL,
    day_of_week     SMALLINT NOT NULL,          -- 0=Monday, 6=Sunday
    day_name        VARCHAR(10) NOT NULL,
    month           SMALLINT NOT NULL,
    month_name      VARCHAR(10) NOT NULL,
    quarter         SMALLINT NOT NULL,
    year            SMALLINT NOT NULL,
    is_weekend      BOOLEAN NOT NULL
);

-- ─── DIMENSION: COUNTRY ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_country (
    country_key     SERIAL PRIMARY KEY,
    country_name    VARCHAR(100) UNIQUE NOT NULL,
    region          VARCHAR(100),
    currency_code   VARCHAR(10)
);

-- ─── FACT: ORDERS ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_orders (
    order_key           BIGSERIAL PRIMARY KEY,
    invoice_no          VARCHAR(50) NOT NULL,
    customer_key        INT REFERENCES dim_customer(customer_key),
    product_key         INT REFERENCES dim_product(product_key),
    date_key            INT REFERENCES dim_date(date_key),
    country_key         INT REFERENCES dim_country(country_key),
    quantity            INT NOT NULL,
    unit_price_gbp      DECIMAL(10, 2),
    unit_price_usd      DECIMAL(10, 2),
    total_amount_gbp    DECIMAL(12, 2),
    total_amount_usd    DECIMAL(12, 2),
    exchange_rate       DECIMAL(10, 6),
    is_cancelled        BOOLEAN DEFAULT FALSE,
    is_anomaly          BOOLEAN DEFAULT FALSE,
    event_timestamp     TIMESTAMP NOT NULL
);

-- ─── STREAMING: REAL-TIME METRICS ────────────────────────────
-- Updated continuously by Spark Structured Streaming
CREATE TABLE IF NOT EXISTS realtime_metrics (
    metric_key          BIGSERIAL PRIMARY KEY,
    window_start        TIMESTAMP NOT NULL,
    window_end          TIMESTAMP NOT NULL,
    order_count         INT,
    total_revenue_gbp   DECIMAL(12, 2),
    total_revenue_usd   DECIMAL(12, 2),
    avg_order_value     DECIMAL(10, 2),
    top_product_code    VARCHAR(50),
    top_country         VARCHAR(100),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ─── INDEXES FOR QUERY PERFORMANCE ───────────────────────────
CREATE INDEX IF NOT EXISTS idx_fact_orders_customer   ON fact_orders(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_product    ON fact_orders(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_date       ON fact_orders(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_country    ON fact_orders(country_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_invoice    ON fact_orders(invoice_no);
CREATE INDEX IF NOT EXISTS idx_fact_orders_timestamp  ON fact_orders(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_fact_orders_cancelled  ON fact_orders(is_cancelled);
CREATE INDEX IF NOT EXISTS idx_realtime_window        ON realtime_metrics(window_start, window_end);
