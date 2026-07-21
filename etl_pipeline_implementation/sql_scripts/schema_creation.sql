-- ============================================
-- DATA WAREHOUSE SCHEMA CREATION
-- Oracle Database 19c
-- ============================================

-- Drop existing objects (for clean setup)
BEGIN
    -- Drop materialized views
    BEGIN EXECUTE IMMEDIATE 'DROP MATERIALIZED VIEW MV_CUSTOMER_ANALYTICS'; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'DROP MATERIALIZED VIEW MV_SALES_MONTHLY_SUMMARY'; EXCEPTION WHEN OTHERS THEN NULL; END;
    
    -- Drop foreign key constraints
    BEGIN EXECUTE IMMEDIATE 'ALTER TABLE FACT_SALES DROP CONSTRAINT fk_fact_sales_store'; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'ALTER TABLE FACT_SALES DROP CONSTRAINT fk_fact_sales_customer'; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'ALTER TABLE FACT_SALES DROP CONSTRAINT fk_fact_sales_product'; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'ALTER TABLE FACT_SALES DROP CONSTRAINT fk_fact_sales_date'; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'ALTER TABLE DIM_PRODUCT DROP CONSTRAINT fk_product_category'; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'ALTER TABLE DIM_CUSTOMER DROP CONSTRAINT fk_customer_region'; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'ALTER TABLE FACT_REVENUE DROP CONSTRAINT fk_revenue_source'; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'ALTER TABLE FACT_REVENUE DROP CONSTRAINT fk_revenue_channel'; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'ALTER TABLE METADATA_DATA_LINEAGE DROP CONSTRAINT fk_lineage_run'; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'ALTER TABLE METADATA_DATA_QUALITY DROP CONSTRAINT fk_quality_run'; EXCEPTION WHEN OTHERS THEN NULL; END;
    
    -- Drop tables
    BEGIN EXECUTE IMMEDIATE 'DROP TABLE FACT_SALES PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'DROP TABLE FACT_REVENUE PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'DROP TABLE DIM_STORE PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'DROP TABLE DIM_CUSTOMER PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'DROP TABLE DIM_PRODUCT PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'DROP TABLE DIM_DATE PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'DROP TABLE DIM_SOURCE PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'DROP TABLE DIM_CHANNEL PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'DROP TABLE DIM_REGION PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'DROP TABLE DIM_CATEGORY PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'DROP TABLE METADATA_DATA_QUALITY PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'DROP TABLE METADATA_DATA_LINEAGE PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'DROP TABLE METADATA_PIPELINE_RUN PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
    
    -- Drop sequences
    BEGIN EXECUTE IMMEDIATE 'DROP SEQUENCE SEQ_FACT_SALES'; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'DROP SEQUENCE SEQ_FACT_REVENUE'; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'DROP SEQUENCE SEQ_DIM_CUSTOMER'; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'DROP SEQUENCE SEQ_DIM_PRODUCT'; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN EXECUTE IMMEDIATE 'DROP SEQUENCE SEQ_DIM_STORE'; EXCEPTION WHEN OTHERS THEN NULL; END;
    COMMIT;
END;
/

-- ============================================
-- TABLESPACES
-- ============================================

-- Create tablespaces (if not exists)
BEGIN
    EXECUTE IMMEDIATE 'CREATE TABLESPACE DATA_01 DATAFILE ''data_01.dbf'' SIZE 1G AUTOEXTEND ON NEXT 100M MAXSIZE 10G';
    EXECUTE IMMEDIATE 'CREATE TABLESPACE DIM_01 DATAFILE ''dim_01.dbf'' SIZE 500M AUTOEXTEND ON NEXT 50M MAXSIZE 5G';
    EXECUTE IMMEDIATE 'CREATE TABLESPACE META_01 DATAFILE ''meta_01.dbf'' SIZE 100M AUTOEXTEND ON NEXT 10M MAXSIZE 1G';
    EXECUTE IMMEDIATE 'CREATE TABLESPACE IDX_01 DATAFILE ''idx_01.dbf'' SIZE 500M AUTOEXTEND ON NEXT 50M MAXSIZE 5G';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

-- ============================================
-- SEQUENCES
-- ============================================

CREATE SEQUENCE SEQ_FACT_SALES START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE SEQ_FACT_REVENUE START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE SEQ_DIM_CUSTOMER START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE SEQ_DIM_PRODUCT START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE SEQ_DIM_STORE START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE SEQ_DIM_CATEGORY START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE SEQ_DIM_REGION START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE SEQ_DIM_CHANNEL START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE SEQ_DIM_SOURCE START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE SEQ_METADATA_PIPELINE START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE SEQ_METADATA_LINEAGE START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
CREATE SEQUENCE SEQ_METADATA_QUALITY START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;

-- ============================================
-- DIMENSION TABLES
-- ============================================

-- Table: DIM_DATE
CREATE TABLE DIM_DATE (
    date_key           NUMBER PRIMARY KEY,
    full_date          DATE NOT NULL,
    day_of_week        NUMBER(1) NOT NULL,
    day_name           VARCHAR2(20) NOT NULL,
    day_of_month       NUMBER(2) NOT NULL,
    day_of_year        NUMBER(3) NOT NULL,
    week_of_year       NUMBER(2) NOT NULL,
    month_number       NUMBER(2) NOT NULL,
    month_name         VARCHAR2(20) NOT NULL,
    quarter            NUMBER(1) NOT NULL,
    quarter_name       VARCHAR2(10) NOT NULL,
    year               NUMBER(4) NOT NULL,
    is_weekend         VARCHAR2(1) NOT NULL,
    is_holiday         VARCHAR2(1) DEFAULT 'N',
    fiscal_year        NUMBER(4) NOT NULL,
    fiscal_quarter     NUMBER(1) NOT NULL,
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_weekend CHECK (is_weekend IN ('Y', 'N')),
    CONSTRAINT chk_holiday CHECK (is_holiday IN ('Y', 'N'))
) TABLESPACE DIM_01;

-- Table: DIM_CATEGORY
CREATE TABLE DIM_CATEGORY (
    category_key       NUMBER DEFAULT SEQ_DIM_CATEGORY.NEXTVAL PRIMARY KEY,
    category_id        VARCHAR2(50) NOT NULL,
    category_name      VARCHAR2(200) NOT NULL,
    category_description VARCHAR2(1000),
    parent_category_key NUMBER,
    category_level     NUMBER(1) DEFAULT 1,
    is_active          VARCHAR2(1) DEFAULT 'Y',
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_category_id UNIQUE (category_id),
    CONSTRAINT chk_category_active CHECK (is_active IN ('Y', 'N'))
) TABLESPACE DIM_01;

-- Table: DIM_REGION
CREATE TABLE DIM_REGION (
    region_key         NUMBER DEFAULT SEQ_DIM_REGION.NEXTVAL PRIMARY KEY,
    region_id          VARCHAR2(50) NOT NULL,
    region_name        VARCHAR2(200) NOT NULL,
    country            VARCHAR2(100) NOT NULL,
    time_zone          VARCHAR2(50),
    is_active          VARCHAR2(1) DEFAULT 'Y',
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_region_id UNIQUE (region_id),
    CONSTRAINT chk_region_active CHECK (is_active IN ('Y', 'N'))
) TABLESPACE DIM_01;

-- Table: DIM_CHANNEL
CREATE TABLE DIM_CHANNEL (
    channel_key        NUMBER DEFAULT SEQ_DIM_CHANNEL.NEXTVAL PRIMARY KEY,
    channel_id         VARCHAR2(50) NOT NULL,
    channel_name       VARCHAR2(200) NOT NULL,
    channel_type       VARCHAR2(50),
    is_active          VARCHAR2(1) DEFAULT 'Y',
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_channel_id UNIQUE (channel_id),
    CONSTRAINT chk_channel_active CHECK (is_active IN ('Y', 'N'))
) TABLESPACE DIM_01;

-- Table: DIM_SOURCE
CREATE TABLE DIM_SOURCE (
    source_key         NUMBER DEFAULT SEQ_DIM_SOURCE.NEXTVAL PRIMARY KEY,
    source_id          VARCHAR2(50) NOT NULL,
    source_name        VARCHAR2(200) NOT NULL,
    source_type        VARCHAR2(50),
    is_active          VARCHAR2(1) DEFAULT 'Y',
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_source_id UNIQUE (source_id),
    CONSTRAINT chk_source_active CHECK (is_active IN ('Y', 'N'))
) TABLESPACE DIM_01;

-- Table: DIM_PRODUCT
CREATE TABLE DIM_PRODUCT (
    product_key        NUMBER DEFAULT SEQ_DIM_PRODUCT.NEXTVAL PRIMARY KEY,
    product_id         VARCHAR2(50) NOT NULL,
    product_name       VARCHAR2(200) NOT NULL,
    product_description VARCHAR2(1000),
    category_key       NUMBER NOT NULL,
    brand_key          NUMBER NOT NULL,
    supplier_key       NUMBER,
    unit_price         NUMBER(10,2),
    cost_price         NUMBER(10,2),
    weight             NUMBER(10,2),
    dimensions         VARCHAR2(100),
    color              VARCHAR2(50),
    size               VARCHAR2(50),
    is_active          VARCHAR2(1) DEFAULT 'Y',
    launch_date        DATE,
    discontinuation_date DATE,
    effective_date     DATE NOT NULL,
    expiry_date        DATE DEFAULT TO_DATE('9999-12-31', 'YYYY-MM-DD'),
    is_current         VARCHAR2(1) DEFAULT 'Y',
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_product_active CHECK (is_active IN ('Y', 'N')),
    CONSTRAINT chk_product_current CHECK (is_current IN ('Y', 'N')),
    CONSTRAINT uq_product_id UNIQUE (product_id, effective_date),
    CONSTRAINT fk_product_category FOREIGN KEY (category_key) REFERENCES DIM_CATEGORY(category_key)
) TABLESPACE DIM_01;

-- Table: DIM_CUSTOMER
CREATE TABLE DIM_CUSTOMER (
    customer_key       NUMBER DEFAULT SEQ_DIM_CUSTOMER.NEXTVAL PRIMARY KEY,
    customer_id        VARCHAR2(50) NOT NULL,
    customer_name      VARCHAR2(200) NOT NULL,
    email              VARCHAR2(200),
    phone              VARCHAR2(20),
    gender             VARCHAR2(10),
    birth_date         DATE,
    age_group          VARCHAR2(20),
    address            VARCHAR2(500),
    city               VARCHAR2(100),
    state_province     VARCHAR2(100),
    country            VARCHAR2(100) DEFAULT 'Vietnam',
    postal_code        VARCHAR2(20),
    region_key         NUMBER NOT NULL,
    segment            VARCHAR2(50),
    customer_type      VARCHAR2(50),
    loyalty_tier       VARCHAR2(50),
    is_active          VARCHAR2(1) DEFAULT 'Y',
    effective_date     DATE NOT NULL,
    expiry_date        DATE DEFAULT TO_DATE('9999-12-31', 'YYYY-MM-DD'),
    is_current         VARCHAR2(1) DEFAULT 'Y',
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_customer_active CHECK (is_active IN ('Y', 'N')),
    CONSTRAINT chk_customer_current CHECK (is_current IN ('Y', 'N')),
    CONSTRAINT uq_customer_id UNIQUE (customer_id, effective_date),
    CONSTRAINT fk_customer_region FOREIGN KEY (region_key) REFERENCES DIM_REGION(region_key)
) TABLESPACE DIM_01;

-- Table: DIM_STORE
CREATE TABLE DIM_STORE (
    store_key          NUMBER DEFAULT SEQ_DIM_STORE.NEXTVAL PRIMARY KEY,
    store_id           VARCHAR2(50) NOT NULL,
    store_name         VARCHAR2(200) NOT NULL,
    store_type         VARCHAR2(50),
    address            VARCHAR2(500),
    city               VARCHAR2(100),
    state_province     VARCHAR2(100),
    country            VARCHAR2(100) DEFAULT 'Vietnam',
    postal_code        VARCHAR2(20),
    region             VARCHAR2(50),
    opening_date       DATE,
    square_footage     NUMBER(10,2),
    number_of_employees NUMBER,
    is_active          VARCHAR2(1) DEFAULT 'Y',
    closing_date       DATE,
    effective_date     DATE NOT NULL,
    expiry_date        DATE DEFAULT TO_DATE('9999-12-31', 'YYYY-MM-DD'),
    is_current         VARCHAR2(1) DEFAULT 'Y',
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_store_active CHECK (is_active IN ('Y', 'N')),
    CONSTRAINT chk_store_current CHECK (is_current IN ('Y', 'N')),
    CONSTRAINT uq_store_id UNIQUE (store_id, effective_date)
) TABLESPACE DIM_01;

-- ============================================
-- FACT TABLES
-- ============================================

-- Table: FACT_SALES
CREATE TABLE FACT_SALES (
    sales_key          NUMBER DEFAULT SEQ_FACT_SALES.NEXTVAL PRIMARY KEY,
    date_key           NUMBER NOT NULL,
    product_key        NUMBER NOT NULL,
    customer_key       NUMBER NOT NULL,
    store_key          NUMBER NOT NULL,
    promotion_key      NUMBER,
    quantity_sold      NUMBER(10,2) NOT NULL,
    unit_price         NUMBER(10,2) NOT NULL,
    gross_amount       NUMBER(12,2) NOT NULL,
    discount_amount    NUMBER(10,2) DEFAULT 0,
    net_amount         NUMBER(12,2) NOT NULL,
    cost_amount        NUMBER(12,2) NOT NULL,
    profit_amount      NUMBER(12,2) NOT NULL,
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system      VARCHAR2(50),
    batch_id           NUMBER,
    CONSTRAINT chk_positive_amounts CHECK (quantity_sold > 0 AND unit_price > 0 AND net_amount > 0),
    CONSTRAINT fk_fact_sales_date FOREIGN KEY (date_key) REFERENCES DIM_DATE(date_key),
    CONSTRAINT fk_fact_sales_product FOREIGN KEY (product_key) REFERENCES DIM_PRODUCT(product_key),
    CONSTRAINT fk_fact_sales_customer FOREIGN KEY (customer_key) REFERENCES DIM_CUSTOMER(customer_key),
    CONSTRAINT fk_fact_sales_store FOREIGN KEY (store_key) REFERENCES DIM_STORE(store_key)
) TABLESPACE DATA_01
PARTITION BY RANGE (date_key) (
    PARTITION p_2023 VALUES LESS THAN (20240101),
    PARTITION p_2024_q1 VALUES LESS THAN (20240401),
    PARTITION p_2024_q2 VALUES LESS THAN (20240701),
    PARTITION p_2024_q3 VALUES LESS THAN (20241001),
    PARTITION p_2024_q4 VALUES LESS THAN (20250101),
    PARTITION p_future VALUES LESS THAN (MAXVALUE)
);

-- Table: FACT_REVENUE
CREATE TABLE FACT_REVENUE (
    revenue_key        NUMBER DEFAULT SEQ_FACT_REVENUE.NEXTVAL PRIMARY KEY,
    date_key           NUMBER NOT NULL,
    product_key        NUMBER NOT NULL,
    customer_key       NUMBER NOT NULL,
    channel_key        NUMBER NOT NULL,
    source_key         NUMBER NOT NULL,
    revenue_amount     NUMBER(12,2) NOT NULL,
    cost_amount        NUMBER(12,2) NOT NULL,
    profit_amount      NUMBER(12,2) NOT NULL,
    tax_amount         NUMBER(10,2) NOT NULL,
    discount_amount    NUMBER(10,2) DEFAULT 0,
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system      VARCHAR2(50),
    batch_id           NUMBER,
    CONSTRAINT chk_revenue_positive CHECK (revenue_amount > 0),
    CONSTRAINT fk_revenue_channel FOREIGN KEY (channel_key) REFERENCES DIM_CHANNEL(channel_key),
    CONSTRAINT fk_revenue_source FOREIGN KEY (source_key) REFERENCES DIM_SOURCE(source_key)
) TABLESPACE DATA_01
PARTITION BY RANGE (date_key) (
    PARTITION p_2023 VALUES LESS THAN (20240101),
    PARTITION p_2024_q1 VALUES LESS THAN (20240401),
    PARTITION p_2024_q2 VALUES LESS THAN (20240701),
    PARTITION p_2024_q3 VALUES LESS THAN (20241001),
    PARTITION p_2024_q4 VALUES LESS THAN (20250101),
    PARTITION p_future VALUES LESS THAN (MAXVALUE)
);

-- ============================================
-- METADATA TABLES
-- ============================================

-- Table: METADATA_PIPELINE_RUN
CREATE TABLE METADATA_PIPELINE_RUN (
    run_id             NUMBER DEFAULT SEQ_METADATA_PIPELINE.NEXTVAL PRIMARY KEY,
    pipeline_name      VARCHAR2(200) NOT NULL,
    run_date           DATE NOT NULL,
    start_time         TIMESTAMP NOT NULL,
    end_time           TIMESTAMP,
    status             VARCHAR2(20) NOT NULL,
    records_processed  NUMBER,
    records_failed     NUMBER,
    error_message      VARCHAR2(4000),
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_pipeline_status CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'PARTIAL_SUCCESS'))
) TABLESPACE META_01;

-- Table: METADATA_DATA_LINEAGE
CREATE TABLE METADATA_DATA_LINEAGE (
    lineage_id         NUMBER DEFAULT SEQ_METADATA_LINEAGE.NEXTVAL PRIMARY KEY,
    source_table       VARCHAR2(200) NOT NULL,
    target_table       VARCHAR2(200) NOT NULL,
    transformation     VARCHAR2(4000),
    pipeline_name      VARCHAR2(200) NOT NULL,
    run_id             NUMBER NOT NULL,
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_lineage_run FOREIGN KEY (run_id) REFERENCES METADATA_PIPELINE_RUN(run_id)
) TABLESPACE META_01;

-- Table: METADATA_DATA_QUALITY
CREATE TABLE METADATA_DATA_QUALITY (
    quality_check_id   NUMBER DEFAULT SEQ_METADATA_QUALITY.NEXTVAL PRIMARY KEY,
    table_name         VARCHAR2(200) NOT NULL,
    check_name         VARCHAR2(200) NOT NULL,
    check_type         VARCHAR2(50) NOT NULL,
    expected_value     NUMBER,
    actual_value       NUMBER,
    status             VARCHAR2(20) NOT NULL,
    run_id             NUMBER NOT NULL,
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_quality_status CHECK (status IN ('PASSED', 'FAILED', 'WARNING')),
    CONSTRAINT fk_quality_run FOREIGN KEY (run_id) REFERENCES METADATA_PIPELINE_RUN(run_id)
) TABLESPACE META_01;

-- ============================================
-- INDEXES
-- ============================================

-- Dimension Tables Indexes
CREATE INDEX idx_dim_date_year_month ON DIM_DATE(year, month_number) TABLESPACE IDX_01;
CREATE INDEX idx_dim_date_quarter ON DIM_DATE(year, quarter) TABLESPACE IDX_01;

CREATE INDEX idx_dim_product_category ON DIM_PRODUCT(category_key) TABLESPACE IDX_01;
CREATE INDEX idx_dim_product_current ON DIM_PRODUCT(is_current) WHERE is_current = 'Y' TABLESPACE IDX_01;
CREATE INDEX idx_dim_product_effective ON DIM_PRODUCT(effective_date, expiry_date) TABLESPACE IDX_01;

CREATE INDEX idx_dim_customer_region ON DIM_CUSTOMER(region_key) TABLESPACE IDX_01;
CREATE INDEX idx_dim_customer_segment ON DIM_CUSTOMER(segment) TABLESPACE IDX_01;
CREATE INDEX idx_dim_customer_current ON DIM_CUSTOMER(is_current) WHERE is_current = 'Y' TABLESPACE IDX_01;
CREATE INDEX idx_dim_customer_effective ON DIM_CUSTOMER(effective_date, expiry_date) TABLESPACE IDX_01;

CREATE INDEX idx_dim_store_region ON DIM_STORE(region) TABLESPACE IDX_01;
CREATE INDEX idx_dim_store_current ON DIM_STORE(is_current) WHERE is_current = 'Y' TABLESPACE IDX_01;

-- Fact Tables Indexes
CREATE INDEX idx_fact_sales_date ON FACT_SALES(date_key) LOCAL TABLESPACE IDX_01;
CREATE INDEX idx_fact_sales_product ON FACT_SALES(product_key) LOCAL TABLESPACE IDX_01;
CREATE INDEX idx_fact_sales_customer ON FACT_SALES(customer_key) LOCAL TABLESPACE IDX_01;
CREATE INDEX idx_fact_sales_store ON FACT_SALES(store_key) LOCAL TABLESPACE IDX_01;
CREATE INDEX idx_fact_sales_date_product ON FACT_SALES(date_key, product_key) LOCAL TABLESPACE IDX_01;

CREATE INDEX idx_fact_revenue_date ON FACT_REVENUE(date_key) LOCAL TABLESPACE IDX_01;
CREATE INDEX idx_fact_revenue_product ON FACT_REVENUE(product_key) LOCAL TABLESPACE IDX_01;
CREATE INDEX idx_fact_revenue_customer ON FACT_REVENUE(customer_key) LOCAL TABLESPACE IDX_01;
CREATE INDEX idx_fact_revenue_channel ON FACT_REVENUE(channel_key) LOCAL TABLESPACE IDX_01;

-- Metadata Indexes
CREATE INDEX idx_meta_pipeline_date ON METADATA_PIPELINE_RUN(pipeline_name, run_date) TABLESPACE IDX_01;
CREATE INDEX idx_meta_lineage_tables ON METADATA_DATA_LINEAGE(source_table, target_table) TABLESPACE IDX_01;
CREATE INDEX idx_meta_quality_run ON METADATA_DATA_QUALITY(run_id) TABLESPACE IDX_01;

-- ============================================
-- COMMENTS
-- ============================================

COMMENT ON TABLE DIM_DATE IS 'Dimension thời gian cho data warehouse';
COMMENT ON TABLE DIM_PRODUCT IS 'Dimension sản phẩm với SCD Type 2';
COMMENT ON TABLE DIM_CUSTOMER IS 'Dimension khách hàng với SCD Type 2 để track lịch sử thay đổi';
COMMENT ON TABLE DIM_STORE IS 'Dimension cửa hàng';
COMMENT ON TABLE DIM_CATEGORY IS 'Dimension danh mục sản phẩm (snowflake)';
COMMENT ON TABLE DIM_REGION IS 'Dimension khu vực (snowflake)';
COMMENT ON TABLE DIM_CHANNEL IS 'Dimension kênh bán hàng';
COMMENT ON TABLE DIM_SOURCE IS 'Dimension nguồn dữ liệu';
COMMENT ON TABLE FACT_SALES IS 'Fact table cho sales data mart - lưu trữ giao dịch bán hàng';
COMMENT ON TABLE FACT_REVENUE IS 'Fact table cho finance data mart - lưu trữ doanh thu và lợi nhuận';
COMMENT ON TABLE METADATA_PIPELINE_RUN IS 'Theo dõi các lần chạy pipeline';
COMMENT ON TABLE METADATA_DATA_LINEAGE IS 'Theo dõi nguồn gốc và luồng dữ liệu';
COMMENT ON TABLE METADATA_DATA_QUALITY IS 'Lưu trữ kết quả data quality checks';

-- ============================================
-- GRANTS (Optional - adjust as needed)
-- ============================================

-- GRANT SELECT, INSERT, UPDATE, DELETE ON FACT_SALES TO DATA_WAREHOUSE_USER;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON DIM_DATE TO DATA_WAREHOUSE_USER;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON DIM_PRODUCT TO DATA_WAREHOUSE_USER;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON DIM_CUSTOMER TO DATA_WAREHOUSE_USER;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON DIM_STORE TO DATA_WAREHOUSE_USER;
-- GRANT SELECT ON METADATA_PIPELINE_RUN TO DATA_WAREHOUSE_USER;
-- GRANT SELECT ON METADATA_DATA_LINEAGE TO DATA_WAREHOUSE_USER;
-- GRANT SELECT ON METADATA_DATA_QUALITY TO DATA_WAREHOUSE_USER;

COMMIT;