# Tài Liệu Thiết Kế Cơ Sở Dữ Liệu
## Data Engineering Portfolio - Database Design & Data Modeling

---

## 1. Tổng Quan Thiết Kế Database

### 1.1 Mô Hình Dữ Liệu

Hệ thống sử dụng **Star Schema** và **Snowflake Schema** cho data warehouse, đảm bảo hiệu năng truy vấn tối ưu và dễ bảo trì.

### 1.2 Nguyên Tắc Thiết Kế

- **Normalization:** 3NF cho source systems
- **Denormalization:** Star schema cho data warehouse
- **Performance:** Indexing, partitioning, materialized views
- **Scalability:** Partitioning by date, horizontal scaling
- **Maintainability:** Clear naming conventions, documentation

---

## 2. Star Schema - Sales Data Mart

### 2.1 Fact Table: FACT_SALES

```sql
-- Table: FACT_SALES
-- Mô tả: Lưu trữ thông tin giao dịch bán hàng
-- Grain: Một row = Một giao dịch bán hàng

CREATE TABLE FACT_SALES (
    sales_key          NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date_key           NUMBER NOT NULL,
    product_key        NUMBER NOT NULL,
    customer_key       NUMBER NOT NULL,
    store_key          NUMBER NOT NULL,
    promotion_key      NUMBER,
    
    -- Measures
    quantity_sold      NUMBER(10,2) NOT NULL,
    unit_price         NUMBER(10,2) NOT NULL,
    gross_amount       NUMBER(12,2) NOT NULL,
    discount_amount    NUMBER(10,2) DEFAULT 0,
    net_amount         NUMBER(12,2) NOT NULL,
    cost_amount        NUMBER(12,2) NOT NULL,
    profit_amount      NUMBER(12,2) NOT NULL,
    
    -- Metadata
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system      VARCHAR2(50),
    batch_id           NUMBER
    
) TABLESPACE DATA_01
PARTITION BY RANGE (date_key) (
    PARTITION p_2024_q1 VALUES LESS THAN (20240401),
    PARTITION p_2024_q2 VALUES LESS THAN (20240701),
    PARTITION p_2024_q3 VALUES LESS THAN (20241001),
    PARTITION p_2024_q4 VALUES LESS THAN (20250101),
    PARTITION p_future VALUES LESS THAN (MAXVALUE)
);

-- Indexes
CREATE INDEX idx_fact_sales_date ON FACT_SALES(date_key);
CREATE INDEX idx_fact_sales_product ON FACT_SALES(product_key);
CREATE INDEX idx_fact_sales_customer ON FACT_SALES(customer_key);
CREATE INDEX idx_fact_sales_store ON FACT_SALES(store_key);

-- Comments
COMMENT ON TABLE FACT_SALES IS 'Fact table cho sales data mart - lưu trữ giao dịch bán hàng';
COMMENT ON COLUMN FACT_SALES.sales_key IS 'Primary key';
COMMENT ON COLUMN FACT_SALES.date_key IS 'Foreign key to DIM_DATE';
COMMENT ON COLUMN FACT_SALES.product_key IS 'Foreign key to DIM_PRODUCT';
COMMENT ON COLUMN FACT_SALES.customer_key IS 'Foreign key to DIM_CUSTOMER';
COMMENT ON COLUMN FACT_SALES.store_key IS 'Foreign key to DIM_STORE';
COMMENT ON COLUMN FACT_SALES.gross_amount IS 'Tổng tiền trước chiết khấu';
COMMENT ON COLUMN FACT_SALES.net_amount IS 'Tổng tiền sau chiết khấu';
COMMENT ON COLUMN FACT_SALES.profit_amount IS 'Lợi nhuận';
```

### 2.2 Dimension Table: DIM_DATE

```sql
-- Table: DIM_DATE
-- Mô tả: Dimension thời gian

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
    
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    
) TABLESPACE DIM_01;

-- Index
CREATE INDEX idx_dim_date_year_month ON DIM_DATE(year, month_number);

-- Comments
COMMENT ON TABLE DIM_DATE IS 'Dimension thời gian cho data warehouse';
COMMENT ON COLUMN DIM_DATE.date_key IS 'Format: YYYYMMDD (ví dụ: 20240115)';
COMMENT ON COLUMN DIM_DATE.is_weekend IS 'Y = Weekend, N = Weekday';
COMMENT ON COLUMN DIM_DATE.is_holiday IS 'Y = Holiday, N = Non-holiday';

-- Populate DIM_DATE
INSERT INTO DIM_DATE (date_key, full_date, day_of_week, day_name, day_of_month, 
                      day_of_year, week_of_year, month_number, month_name, 
                      quarter, quarter_name, year, is_weekend, fiscal_year, fiscal_quarter)
SELECT 
    TO_NUMBER(TO_CHAR(dt, 'YYYYMMDD')) as date_key,
    dt as full_date,
    TO_NUMBER(TO_CHAR(dt, 'D')) as day_of_week,
    TO_CHAR(dt, 'Day') as day_name,
    TO_NUMBER(TO_CHAR(dt, 'DD')) as day_of_month,
    TO_NUMBER(TO_CHAR(dt, 'DDD')) as day_of_year,
    TO_NUMBER(TO_CHAR(dt, 'WW')) as week_of_year,
    TO_NUMBER(TO_CHAR(dt, 'MM')) as month_number,
    TO_CHAR(dt, 'Month') as month_name,
    TO_NUMBER(TO_CHAR(dt, 'Q')) as quarter,
    'Q' || TO_CHAR(dt, 'Q') as quarter_name,
    TO_NUMBER(TO_CHAR(dt, 'YYYY')) as year,
    CASE WHEN TO_NUMBER(TO_CHAR(dt, 'D')) IN (1, 7) THEN 'Y' ELSE 'N' END as is_weekend,
    CASE WHEN TO_NUMBER(TO_CHAR(dt, 'MM')) >= 10 THEN TO_NUMBER(TO_CHAR(dt, 'YYYY')) + 1 
         ELSE TO_NUMBER(TO_CHAR(dt, 'YYYY')) END as fiscal_year,
    CASE WHEN TO_NUMBER(TO_CHAR(dt, 'MM')) >= 10 THEN TO_NUMBER(TO_CHAR(dt, 'Q')) - 3 
         ELSE TO_NUMBER(TO_CHAR(dt, 'Q')) + 1 END as fiscal_quarter
FROM (
    SELECT TRUNC(SYSDATE) - LEVEL + 1 as dt
    FROM dual
    CONNECT BY LEVEL <= 3650  -- 10 years
);

COMMIT;
```

### 2.3 Dimension Table: DIM_PRODUCT

```sql
-- Table: DIM_PRODUCT
-- Mô tả: Dimension sản phẩm

CREATE TABLE DIM_PRODUCT (
    product_key        NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id         VARCHAR2(50) NOT NULL,
    product_name       VARCHAR2(200) NOT NULL,
    product_description VARCHAR2(1000),
    category_key       NUMBER NOT NULL,
    brand_key          NUMBER NOT NULL,
    supplier_key       NUMBER,
    
    -- Attributes
    unit_price         NUMBER(10,2),
    cost_price         NUMBER(10,2),
    weight             NUMBER(10,2),
    dimensions         VARCHAR2(100),
    color              VARCHAR2(50),
    size               VARCHAR2(50),
    
    -- Status
    is_active          VARCHAR2(1) DEFAULT 'Y',
    launch_date        DATE,
    discontinuation_date DATE,
    
    -- Metadata
    effective_date     DATE NOT NULL,
    expiry_date        DATE DEFAULT TO_DATE('9999-12-31', 'YYYY-MM-DD'),
    is_current         VARCHAR2(1) DEFAULT 'Y',
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT chk_product_active CHECK (is_active IN ('Y', 'N')),
    CONSTRAINT chk_product_current CHECK (is_current IN ('Y', 'N')),
    CONSTRAINT uq_product_id UNIQUE (product_id, effective_date)
    
) TABLESPACE DIM_01;

-- Indexes
CREATE INDEX idx_dim_product_category ON DIM_PRODUCT(category_key);
CREATE INDEX idx_dim_product_brand ON DIM_PRODUCT(brand_key);
CREATE INDEX idx_dim_product_current ON DIM_PRODUCT(is_current) WHERE is_current = 'Y';

-- Comments
COMMENT ON TABLE DIM_PRODUCT IS 'Dimension sản phẩm với SCD Type 2';
COMMENT ON COLUMN DIM_PRODUCT.effective_date IS 'Ngày bắt đầu hiệu lực';
COMMENT ON COLUMN DIM_PRODUCT.expiry_date IS 'Ngày hết hiệu lực';
COMMENT ON COLUMN DIM_PRODUCT.is_current IS 'Y = Current record, N = Historical record';
```

### 2.4 Dimension Table: DIM_CUSTOMER

```sql
-- Table: DIM_CUSTOMER
-- Mô tả: Dimension khách hàng với SCD Type 2

CREATE TABLE DIM_CUSTOMER (
    customer_key       NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id        VARCHAR2(50) NOT NULL,
    customer_name      VARCHAR2(200) NOT NULL,
    email              VARCHAR2(200),
    phone              VARCHAR2(20),
    
    -- Demographics
    gender             VARCHAR2(10),
    birth_date         DATE,
    age_group          VARCHAR2(20),
    
    -- Location
    address            VARCHAR2(500),
    city               VARCHAR2(100),
    state_province     VARCHAR2(100),
    country            VARCHAR2(100) DEFAULT 'Vietnam',
    postal_code        VARCHAR2(20),
    region_key         NUMBER NOT NULL,
    
    -- Segmentation
    segment            VARCHAR2(50),
    customer_type      VARCHAR2(50),
    loyalty_tier       VARCHAR2(50),
    
    -- Status
    is_active          VARCHAR2(1) DEFAULT 'Y',
    
    -- Metadata (SCD Type 2)
    effective_date     DATE NOT NULL,
    expiry_date        DATE DEFAULT TO_DATE('9999-12-31', 'YYYY-MM-DD'),
    is_current         VARCHAR2(1) DEFAULT 'Y',
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT chk_customer_active CHECK (is_active IN ('Y', 'N')),
    CONSTRAINT chk_customer_current CHECK (is_current IN ('Y', 'N')),
    CONSTRAINT uq_customer_id UNIQUE (customer_id, effective_date)
    
) TABLESPACE DIM_01;

-- Indexes
CREATE INDEX idx_dim_customer_region ON DIM_CUSTOMER(region_key);
CREATE INDEX idx_dim_customer_segment ON DIM_CUSTOMER(segment);
CREATE INDEX idx_dim_customer_current ON DIM_CUSTOMER(is_current) WHERE is_current = 'Y';
CREATE INDEX idx_dim_customer_effective ON DIM_CUSTOMER(effective_date, expiry_date);

-- Comments
COMMENT ON TABLE DIM_CUSTOMER IS 'Dimension khách hàng với SCD Type 2 để track lịch sử thay đổi';
```

### 2.5 Dimension Table: DIM_STORE

```sql
-- Table: DIM_STORE
-- Mô tả: Dimension cửa hàng

CREATE TABLE DIM_STORE (
    store_key          NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    store_id           VARCHAR2(50) NOT NULL,
    store_name         VARCHAR2(200) NOT NULL,
    store_type         VARCHAR2(50),
    
    -- Location
    address            VARCHAR2(500),
    city               VARCHAR2(100),
    state_province     VARCHAR2(100),
    country            VARCHAR2(100) DEFAULT 'Vietnam',
    postal_code        VARCHAR2(20),
    region             VARCHAR2(50),
    
    -- Attributes
    opening_date       DATE,
    square_footage     NUMBER(10,2),
    number_of_employees NUMBER,
    
    -- Status
    is_active          VARCHAR2(1) DEFAULT 'Y',
    closing_date       DATE,
    
    -- Metadata
    effective_date     DATE NOT NULL,
    expiry_date        DATE DEFAULT TO_DATE('9999-12-31', 'YYYY-MM-DD'),
    is_current         VARCHAR2(1) DEFAULT 'Y',
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT chk_store_active CHECK (is_active IN ('Y', 'N')),
    CONSTRAINT chk_store_current CHECK (is_current IN ('Y', 'N')),
    CONSTRAINT uq_store_id UNIQUE (store_id, effective_date)
    
) TABLESPACE DIM_01;

-- Indexes
CREATE INDEX idx_dim_store_region ON DIM_STORE(region);
CREATE INDEX idx_dim_store_current ON DIM_STORE(is_current) WHERE is_current = 'Y';

-- Comments
COMMENT ON TABLE DIM_STORE IS 'Dimension cửa hàng';
```

---

## 3. Snowflake Schema - Finance Data Mart

### 3.1 Fact Table: FACT_REVENUE

```sql
-- Table: FACT_REVENUE
-- Mô tả: Fact table cho finance data mart

CREATE TABLE FACT_REVENUE (
    revenue_key        NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date_key           NUMBER NOT NULL,
    product_key        NUMBER NOT NULL,
    customer_key       NUMBER NOT NULL,
    channel_key        NUMBER NOT NULL,
    source_key         NUMBER NOT NULL,
    
    -- Measures
    revenue_amount     NUMBER(12,2) NOT NULL,
    cost_amount        NUMBER(12,2) NOT NULL,
    profit_amount      NUMBER(12,2) NOT NULL,
    tax_amount         NUMBER(10,2) NOT NULL,
    discount_amount    NUMBER(10,2) DEFAULT 0,
    
    -- Metadata
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system      VARCHAR2(50),
    batch_id           NUMBER
    
) TABLESPACE DATA_01
PARTITION BY RANGE (date_key) (
    PARTITION p_2024_q1 VALUES LESS THAN (20240401),
    PARTITION p_2024_q2 VALUES LESS THAN (20240701),
    PARTITION p_2024_q3 VALUES LESS THAN (20241001),
    PARTITION p_2024_q4 VALUES LESS THAN (20250101),
    PARTITION p_future VALUES LESS THAN (MAXVALUE)
);

-- Indexes
CREATE INDEX idx_fact_revenue_date ON FACT_REVENUE(date_key);
CREATE INDEX idx_fact_revenue_product ON FACT_REVENUE(product_key);
CREATE INDEX idx_fact_revenue_customer ON FACT_REVENUE(customer_key);
CREATE INDEX idx_fact_revenue_channel ON FACT_REVENUE(channel_key);

-- Comments
COMMENT ON TABLE FACT_REVENUE IS 'Fact table cho finance data mart - lưu trữ doanh thu và lợi nhuận';
```

### 3.2 Dimension Tables (Snowflake)

```sql
-- Table: DIM_CATEGORY (Snowflake from DIM_PRODUCT)
CREATE TABLE DIM_CATEGORY (
    category_key       NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    category_id        VARCHAR2(50) NOT NULL,
    category_name      VARCHAR2(200) NOT NULL,
    category_description VARCHAR2(1000),
    parent_category_key NUMBER,
    category_level     NUMBER(1),
    is_active          VARCHAR2(1) DEFAULT 'Y',
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_category_id UNIQUE (category_id)
) TABLESPACE DIM_01;

-- Table: DIM_REGION (Snowflake from DIM_CUSTOMER)
CREATE TABLE DIM_REGION (
    region_key         NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    region_id          VARCHAR2(50) NOT NULL,
    region_name        VARCHAR2(200) NOT NULL,
    country            VARCHAR2(100) NOT NULL,
    time_zone          VARCHAR2(50),
    is_active          VARCHAR2(1) DEFAULT 'Y',
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_region_id UNIQUE (region_id)
) TABLESPACE DIM_01;

-- Table: DIM_CHANNEL
CREATE TABLE DIM_CHANNEL (
    channel_key        NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    channel_id         VARCHAR2(50) NOT NULL,
    channel_name       VARCHAR2(200) NOT NULL,
    channel_type       VARCHAR2(50),
    is_active          VARCHAR2(1) DEFAULT 'Y',
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_channel_id UNIQUE (channel_id)
) TABLESPACE DIM_01;

-- Table: DIM_SOURCE
CREATE TABLE DIM_SOURCE (
    source_key         NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id          VARCHAR2(50) NOT NULL,
    source_name        VARCHAR2(200) NOT NULL,
    source_type        VARCHAR2(50),
    is_active          VARCHAR2(1) DEFAULT 'Y',
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_source_id UNIQUE (source_id)
) TABLESPACE DIM_01;
```

---

## 4. Metadata Management

### 4.1 Metadata Tables

```sql
-- Table: METADATA_PIPELINE_RUN
-- Mô tả: Theo dõi các lần chạy pipeline

CREATE TABLE METADATA_PIPELINE_RUN (
    run_id             NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
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
-- Mô tả: Theo dõi nguồn gốc và luồng dữ liệu

CREATE TABLE METADATA_DATA_LINEAGE (
    lineage_id         NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_table       VARCHAR2(200) NOT NULL,
    target_table       VARCHAR2(200) NOT NULL,
    transformation     VARCHAR2(4000),
    pipeline_name      VARCHAR2(200) NOT NULL,
    run_id             NUMBER NOT NULL,
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT fk_lineage_run FOREIGN KEY (run_id) REFERENCES METADATA_PIPELINE_RUN(run_id)
) TABLESPACE META_01;

-- Table: METADATA_DATA_QUALITY
-- Mô tả: Lưu trữ kết quả data quality checks

CREATE TABLE METADATA_DATA_QUALITY (
    quality_check_id   NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
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

-- Indexes
CREATE INDEX idx_meta_pipeline_date ON METADATA_PIPELINE_RUN(pipeline_name, run_date);
CREATE INDEX idx_meta_lineage_tables ON METADATA_DATA_LINEAGE(source_table, target_table);
CREATE INDEX idx_meta_quality_run ON METADATA_DATA_QUALITY(run_id);
```

---

## 5. Materialized Views

### 5.1 Sales Summary by Month

```sql
-- Materialized View: MV_SALES_MONTHLY_SUMMARY
-- Mô tả: Tổng hợp doanh số theo tháng

CREATE MATERIALIZED VIEW MV_SALES_MONTHLY_SUMMARY
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
SELECT 
    d.year,
    d.month_number,
    d.month_name,
    p.category_name,
    s.region,
    COUNT(DISTINCT f.sales_key) as total_transactions,
    SUM(f.quantity_sold) as total_quantity,
    SUM(f.gross_amount) as total_gross_amount,
    SUM(f.discount_amount) as total_discount,
    SUM(f.net_amount) as total_net_amount,
    SUM(f.profit_amount) as total_profit,
    AVG(f.net_amount) as avg_transaction_value
FROM FACT_SALES f
JOIN DIM_DATE d ON f.date_key = d.date_key
JOIN DIM_PRODUCT p ON f.product_key = p.product_key
JOIN DIM_STORE s ON f.store_key = s.store_key
WHERE d.year >= 2024
GROUP BY 
    d.year,
    d.month_number,
    d.month_name,
    p.category_name,
    s.region;

-- Index on materialized view
CREATE INDEX idx_mv_sales_monthly ON MV_SALES_MONTHLY_SUMMARY(year, month_number, category_name);

-- Comments
COMMENT ON MATERIALIZED VIEW MV_SALES_MONTHLY_SUMMARY IS 'Tổng hợp doanh số bán hàng theo tháng và category';
```

### 5.2 Customer Analytics

```sql
-- Materialized View: MV_CUSTOMER_ANALYTICS
-- Mô tả: Phân tích khách hàng

CREATE MATERIALIZED VIEW MV_CUSTOMER_ANALYTICS
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
SELECT 
    c.customer_key,
    c.customer_name,
    c.segment,
    c.region_key,
    r.region_name,
    COUNT(DISTINCT f.sales_key) as total_orders,
    SUM(f.net_amount) as total_revenue,
    AVG(f.net_amount) as avg_order_value,
    MAX(d.full_date) as last_purchase_date,
    MIN(d.full_date) as first_purchase_date,
    COUNT(DISTINCT f.date_key) as active_days
FROM DIM_CUSTOMER c
JOIN DIM_REGION r ON c.region_key = r.region_key
LEFT JOIN FACT_SALES f ON c.customer_key = f.customer_key
LEFT JOIN DIM_DATE d ON f.date_key = d.date_key
WHERE c.is_current = 'Y'
GROUP BY 
    c.customer_key,
    c.customer_name,
    c.segment,
    c.region_key,
    r.region_name;

-- Index
CREATE INDEX idx_mv_customer_segment ON MV_CUSTOMER_ANALYTICS(segment, region_name);

-- Comments
COMMENT ON MATERIALIZED VIEW MV_CUSTOMER_ANALYTICS IS 'Phân tích hành vi và giá trị khách hàng';
```

---

## 6. Database Performance Optimization

### 6.1 Indexing Strategies

```sql
-- B-tree Indexes (cho equality và range queries)
CREATE INDEX idx_fact_sales_date_product ON FACT_SALES(date_key, product_key);

-- Bitmap Indexes (cho low-cardinality columns)
CREATE BITMAP INDEX idx_fact_sales_quarter ON FACT_SALES(quarter);

-- Function-based Indexes
CREATE INDEX idx_fact_sales_year_month ON FACT_SALES(
    EXTRACT(YEAR FROM created_at),
    EXTRACT(MONTH FROM created_at)
);

-- Partition Indexes (local indexes)
CREATE INDEX idx_fact_sales_local ON FACT_SALES(product_key) LOCAL;
```

### 6.2 Partitioning Strategies

```sql
-- Range Partitioning (by date)
-- Already implemented in FACT_SALES and FACT_REVENUE

-- Subpartitioning by hash
ALTER TABLE FACT_SALES 
SUBPARTITION BY HASH (product_key) 
SUBPARTITIONS 16;
```

### 6.3 Query Optimization Examples

```sql
-- Bad Query (full table scan)
SELECT * FROM FACT_SALES WHERE product_key = 123;

-- Good Query (using partition pruning and index)
SELECT 
    sales_key,
    date_key,
    net_amount,
    profit_amount
FROM FACT_SALES
WHERE product_key = 123
  AND date_key BETWEEN 20240101 AND 20240331;

-- Using materialized view for aggregations
SELECT 
    year,
    month_name,
    category_name,
    total_net_amount
FROM MV_SALES_MONTHLY_SUMMARY
WHERE year = 2024
  AND category_name = 'Electronics'
ORDER BY month_number;
```

---

## 7. Data Integrity & Constraints

### 7.1 Foreign Key Constraints

```sql
-- Foreign Keys for Star Schema
ALTER TABLE FACT_SALES 
ADD CONSTRAINT fk_fact_sales_date 
    FOREIGN KEY (date_key) REFERENCES DIM_DATE(date_key);

ALTER TABLE FACT_SALES 
ADD CONSTRAINT fk_fact_sales_product 
    FOREIGN KEY (product_key) REFERENCES DIM_PRODUCT(product_key);

ALTER TABLE FACT_SALES 
ADD CONSTRAINT fk_fact_sales_customer 
    FOREIGN KEY (customer_key) REFERENCES DIM_CUSTOMER(customer_key);

ALTER TABLE FACT_SALES 
ADD CONSTRAINT fk_fact_sales_store 
    FOREIGN KEY (store_key) REFERENCES DIM_STORE(store_key);

-- Foreign Keys for Snowflake Schema
ALTER TABLE DIM_PRODUCT 
ADD CONSTRAINT fk_product_category 
    FOREIGN KEY (category_key) REFERENCES DIM_CATEGORY(category_key);

ALTER TABLE DIM_CUSTOMER 
ADD CONSTRAINT fk_customer_region 
    FOREIGN KEY (region_key) REFERENCES DIM_REGION(region_key);

ALTER TABLE FACT_REVENUE 
ADD CONSTRAINT fk_revenue_channel 
    FOREIGN KEY (channel_key) REFERENCES DIM_CHANNEL(channel_key);

ALTER TABLE FACT_REVENUE 
ADD CONSTRAINT fk_revenue_source 
    FOREIGN KEY (source_key) REFERENCES DIM_SOURCE(source_key);
```

### 7.2 Check Constraints

```sql
-- Ensure positive amounts
ALTER TABLE FACT_SALES 
ADD CONSTRAINT chk_positive_amounts 
    CHECK (quantity_sold > 0 AND unit_price > 0 AND net_amount > 0);

-- Ensure dates are valid
ALTER TABLE DIM_CUSTOMER 
ADD CONSTRAINT chk_effective_dates 
    CHECK (effective_date < expiry_date);

-- Ensure profit calculation is correct
ALTER TABLE FACT_SALES 
ADD CONSTRAINT chk_profit_calculation 
    CHECK (profit_amount = net_amount - cost_amount);
```

---

## 8. Database Maintenance

### 8.1 Statistics Gathering

```sql
-- Gather statistics for optimizer
BEGIN
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname => 'DATA_WAREHOUSE',
        tabname => 'FACT_SALES',
        estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
        method_opt => 'FOR ALL COLUMNS SIZE AUTO',
        degree => DBMS_STATS.AUTO_DEGREE
    );
    
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname => 'DATA_WAREHOUSE',
        tabname => 'DIM_PRODUCT',
        estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE
    );
    
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname => 'DATA_WAREHOUSE',
        tabname => 'DIM_CUSTOMER',
        estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE
    );
END;
/

-- Create a job to gather statistics regularly
BEGIN
    DBMS_SCHEDULER.CREATE_JOB(
        job_name        => 'GATHER_STATS_JOB',
        job_type        => 'PLSQL_BLOCK',
        job_action      => 'BEGIN DBMS_STATS.GATHER_SCHEMA_STATS(''DATA_WAREHOUSE''); END;',
        start_date      => SYSTIMESTAMP,
        repeat_interval => 'FREQ=DAILY; BYHOUR=2',
        enabled         => TRUE,
        comments        => 'Gather statistics daily at 2 AM'
    );
END;
/
```

### 8.2 Index Maintenance

```sql
-- Rebuild fragmented indexes
BEGIN
    FOR idx IN (
        SELECT index_name, table_name
        FROM user_indexes
        WHERE table_name IN ('FACT_SALES', 'DIM_PRODUCT', 'DIM_CUSTOMER')
    ) LOOP
        EXECUTE IMMEDIATE 'ALTER INDEX ' || idx.index_name || ' REBUILD';
    END LOOP;
END;
/

-- Create a job to rebuild indexes weekly
BEGIN
    DBMS_SCHEDULER.CREATE_JOB(
        job_name        => 'REBUILD_INDEXES_JOB',
        job_type        => 'PLSQL_BLOCK',
        job_action      => 'BEGIN /* Index rebuild logic */ NULL; END;',
        start_date      => SYSTIMESTAMP,
        repeat_interval => 'FREQ=WEEKLY; BYDAY=SUN; BYHOUR=3',
        enabled         => TRUE,
        comments        => 'Rebuild indexes weekly on Sunday at 3 AM'
    );
END;
/
```

---

## 9. Backup & Recovery

### 9.1 Backup Strategy

```sql
-- Full backup (weekly)
RMAN> BACKUP DATABASE PLUS ARCHIVELOG;

-- Incremental backup (daily)
RMAN> BACKUP INCREMENTAL LEVEL 1 DATABASE;

-- Export schema (for logical backup)
expdp data_warehouse/password DIRECTORY=backup_dir DUMPFILE=warehouse_backup.dmp 
    SCHEMAS=data_warehouse LOGFILE=backup.log;
```

### 9.2 Recovery Strategy

```sql
-- Point-in-time recovery
RMAN> RUN {
    SET UNTIL TIME '2024-01-15 14:30:00';
    RESTORE DATABASE;
    RECOVER DATABASE;
    ALTER DATABASE OPEN RESETLOGS;
}

-- Table recovery (using Flashback)
FLASHBACK TABLE FACT_SALES TO TIMESTAMP TO_TIMESTAMP('2024-01-15 14:30:00', 'YYYY-MM-DD HH24:MI:SS');
```

---

## 10. Naming Conventions

### 10.1 Object Naming Standards

| Object Type | Naming Convention | Example |
|-------------|------------------|---------|
| Tables | `{TYPE}_{NAME}` | `FACT_SALES`, `DIM_CUSTOMER` |
| Indexes | `IDX_{TABLE}_{COLUMN}` | `IDX_FACT_SALES_DATE` |
| Constraints | `{TYPE}_{TABLE}_{COLUMN}` | `FK_FACT_SALES_CUSTOMER` |
| Materialized Views | `MV_{DESCRIPTION}` | `MV_SALES_MONTHLY_SUMMARY` |
| Sequences | `SEQ_{TABLE}` | `SEQ_FACT_SALES` |
| Procedures | `SP_{ACTION}` | `SP_LOAD_FACT_SALES` |
| Functions | `FN_{ACTION}` | `FN_CALCULATE_PROFIT` |

### 10.2 Column Naming Standards

| Column Type | Naming Convention | Example |
|-------------|------------------|---------|
| Primary Key | `{table_abbreviation}_key` | `sales_key`, `cust_key` |
| Foreign Key | `{referenced_table}_key` | `date_key`, `product_key` |
| Measures | `{metric_name}` | `net_amount`, `profit_amount` |
| Dates | `{event}_date` | `effective_date`, `launch_date` |
| Flags | `is_{status}` | `is_active`, `is_current` |
| Timestamps | `{event}_at` | `created_at`, `updated_at` |

---

*Tài liệu này cung cấp thiết kế chi tiết cho cơ sở dữ liệu data warehouse, bao gồm star schema, snowflake schema, metadata management và optimization strategies.*