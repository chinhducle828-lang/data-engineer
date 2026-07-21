# Tài Liệu Thiết Kế Hệ Thống
## Data Engineering Portfolio - Vị trí Data Engineer @ ATS Vietnam

---

## 1. Tổng Quan Kiến Trúc

### 1.1 Mô Tả Hệ Thống

Hệ thống Data Engineering được thiết kế để xử lý, transform và load dữ liệu từ nhiều nguồn khác nhau vào data warehouse, phục vụ cho các bài toán phân tích kinh doanh và báo cáo.

### 1.2 Kiến Trúc Tổng Thể

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SOURCE SYSTEMS                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐             │
│  │   Oracle DB  │  │  PostgreSQL  │  │  Flat Files  │             │
│  │  (OLTP)      │  │   (OLTP)     │  │  (CSV/JSON)  │             │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘             │
└─────────┼─────────────────┼─────────────────┼──────────────────────┘
          │                 │                 │
          └─────────────────┼─────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    DATA INGESTION LAYER                             │
│  ┌──────────────────────────────────────────────────────────┐      │
│  │  Apache Spark (Extract)                                  │      │
│  │  - JDBC Connectors (Oracle, PostgreSQL)                  │      │
│  │  - File Readers (CSV, JSON, Parquet)                     │      │
│  │  - API Connectors                                        │      │
│  └──────────────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    DATA LAKE (Raw Zone)                             │
│  ┌──────────────────────────────────────────────────────────┐      │
│  │  Format: Parquet/Delta Lake                              │      │
│  │  Location: S3 / HDFS / Local Storage                     │      │
│  │  Partitioning: by date, by source system                 │      │
│  └──────────────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    DATA TRANSFORMATION LAYER                        │
│  ┌──────────────────────────────────────────────────────────┐      │
│  │  Apache Spark (Transform)                                │      │
│  │  - Data Cleaning & Validation                            │      │
│  │  - Data Integration (JOINs, UNIONs)                      │      │
│  │  - Business Logic Implementation                         │      │
│  │  - Data Type Conversions                                 │      │
│  │  - Deduplication & Aggregation                           │      │
│  └──────────────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    DATA WAREHOUSE (Curated Zone)                     │
│  ┌──────────────────────────────────────────────────────────┐      │
│  │  Oracle Database / PostgreSQL                             │      │
│  │  - Star Schema / Snowflake Schema                        │      │
│  │  - Fact Tables & Dimension Tables                        │      │
│  │  - Indexes & Materialized Views                          │      │
│  └──────────────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    DATA MARTS & ANALYTICS                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐             │
│  │ Sales Mart   │  │ Finance Mart │  │  HR Mart     │             │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘             │
│         │                 │                 │                      │
│         └─────────────────┼─────────────────┘                      │
│                           │                                        │
│                           ▼                                        │
│  ┌──────────────────────────────────────────────────────────┐      │
│  │  BI Tools (Tableau, Power BI, Looker)                     │      │
│  │  - Dashboards                                            │      │
│  │  - Reports                                               │      │
│  │  - Self-service Analytics                                 │      │
│  └──────────────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    ORCHESTRATION & SCHEDULING                       │
│  ┌──────────────────────────────────────────────────────────┐      │
│  │  Apache Airflow                                          │      │
│  │  - DAG Scheduling                                        │      │
│  │  - Task Dependencies                                     │      │
│  │  - Monitoring & Alerting                                 │      │
│  │  - Retry Logic & Error Handling                          │      │
│  └──────────────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 2. Công Nghệ Sử Dụng

### 2.1 Technology Stack

| Category | Technology | Version | Purpose |
|----------|-----------|---------|---------|
| **Data Processing** | Apache Spark | 3.4+ | Distributed data processing |
| **SQL Engine** | Spark SQL | 3.4+ | SQL queries on Spark |
| **Orchestration** | Apache Airflow | 2.7+ | Workflow orchestration |
| **Primary Database** | Oracle Database | 19c | Data warehouse |
| **Secondary DB** | PostgreSQL | 15+ | Metadata management |
| **Programming** | Python | 3.9+ | Scripting & development |
| **Data Format** | Parquet/Delta | - | Columnar storage |
| **Containerization** | Docker | 24+ | Environment management |
| **Version Control** | Git | - | Code management |

### 2.2 Lý Do Lựa Chọn

**Apache Spark:**
- Xử lý distributed data quy mô lớn (500GB+/ngày)
- In-memory processing cho hiệu năng cao
- Hỗ trợ batch và streaming
- Tích hợp tốt với các ecosystem tools

**Oracle Database:**
- Phù hợp với yêu cầu công việc (ưu tiên có kinh nghiệm Oracle)
- Hiệu năng cao cho data warehouse
- Hỗ trợ PL/SQL cho complex business logic
- ACID compliance và data integrity

**Apache Airflow:**
- Standard cho workflow orchestration
- DAG-based scheduling linh hoạt
- Monitoring và alerting tích hợp
- Large ecosystem và community support

---

## 3. Data Flow

### 3.1 Data Pipeline Flow

```
1. EXTRACT
   ├── Source: Oracle DB (OLTP)
   ├── Source: PostgreSQL
   ├── Source: CSV/JSON files
   └── Method: JDBC, File Readers, APIs
   ↓
2. RAW ZONE (Data Lake)
   ├── Format: Parquet
   ├── Partition: by_date, by_source
   └── Compression: Snappy
   ↓
3. TRANSFORM
   ├── Data Cleaning
   ├── Data Validation
   ├── Business Logic
   ├── Data Integration
   └── Aggregation
   ↓
4. CURATED ZONE (Data Warehouse)
   ├── Star Schema
   ├── Fact Tables
   ├── Dimension Tables
   └── Indexes
   ↓
5. DATA MARTS
   ├── Sales Mart
   ├── Finance Mart
   └── HR Mart
   ↓
6. CONSUMPTION
   ├── BI Dashboards
   ├── Reports
   └── ML Features
```

### 3.2 Data Volume & Frequency

| Data Source | Volume | Frequency | Latency |
|-------------|--------|-----------|---------|
| Oracle DB | 500GB/day | Hourly | 1 hour |
| PostgreSQL | 100GB/day | Daily | 24 hours |
| Flat Files | 50GB/day | Daily | 24 hours |
| **Total** | **650GB/day** | - | - |

---

## 4. Database Design

### 4.1 Star Schema - Sales Data Mart

```
                    ┌──────────────┐
                    │  DIM_DATE    │
                    │  - date_key  │
                    │  - date      │
                    │  - month     │
                    │  - quarter   │
                    │  - year      │
                    └──────┬───────┘
                           │
                           │
┌──────────────┐    ┌──────┴───────┐    ┌──────────────┐
│ DIM_PRODUCT  │    │ FACT_SALES   │    │  DIM_CUSTOMER│
│ - prod_key   │◄───┤ - sales_key  │───►│ - cust_key   │
│ - prod_id    │    │ - date_key   │    │ - cust_id    │
│ - prod_name  │    │ - prod_key   │    │ - name       │
│ - category   │    │ - cust_key   │    │ - region     │
│ - brand      │    │ - store_key  │    │ - segment    │
└──────────────┘    │ - amount     │    └──────────────┘
                    │ - quantity   │
                    │ - discount   │
                    └──────┬───────┘
                           │
                           │
                    ┌──────┴───────┐
                    │  DIM_STORE   │
                    │ - store_key  │
                    │ - store_id   │
                    │ - store_name │
                    │ - location   │
                    │ - type       │
                    └──────────────┘
```

### 4.2 Snowflake Schema - Finance Data Mart

```
                    ┌──────────────┐
                    │  DIM_DATE    │
                    └──────┬───────┘
                           │
                           │
                    ┌──────┴───────┐
                    │ FACT_REVENUE │
                    └──────┬───────┘
                           │
           ┌───────────────┼───────────────┐
           │               │               │
           ▼               ▼               ▼
    ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
    │  DIM_PRODUCT │ │  DIM_CUSTOMER│ │  DIM_CHANNEL │
    └──────┬───────┘ └──────┬───────┘ └──────┬───────┘
           │               │               │
           ▼               ▼               ▼
    ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
    │ DIM_CATEGORY │ │ DIM_REGION   │ │ DIM_SOURCE   │
    └──────────────┘ └──────────────┘ └──────────────┘
```

---

## 5. Component Interactions

### 5.1 Airflow DAG Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    AIRFLOW SCHEDULER                        │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  DAG: daily_etl_pipeline                                    │
│                                                             │
│  Task 1: extract_oracle_data                                │
│    └── SparkJob: Extract from Oracle DB                     │
│    └── Output: Raw Zone (Parquet)                           │
│                                                             │
│  Task 2: extract_postgres_data                              │
│    └── SparkJob: Extract from PostgreSQL                    │
│    └── Output: Raw Zone (Parquet)                           │
│                                                             │
│  Task 3: transform_data                                     │
│    └── SparkJob: Clean, validate, transform                 │
│    └── Output: Curated Zone (Parquet)                       │
│    └── Dependencies: Task 1, Task 2                         │
│                                                             │
│  Task 4: load_to_data_warehouse                             │
│    └── SparkJob: Load to Oracle DW                          │
│    └── Output: Fact & Dimension tables                      │
│    └── Dependencies: Task 3                                 │
│                                                             │
│  Task 5: data_quality_checks                                │
│    └── SQL Queries: Validate data quality                   │
│    └── Dependencies: Task 4                                 │
│                                                             │
│  Task 6: refresh_materialized_views                         │
│    └── PL/SQL: Refresh MVs                                  │
│    └── Dependencies: Task 5                                 │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  Monitoring & Alerting                                      │
│  - Email notifications on failure                           │
│  - Slack alerts for data anomalies                          │
│  - Metrics dashboard (Grafana)                              │
└─────────────────────────────────────────────────────────────┘
```

---

## 6. Performance Optimization

### 6.1 Spark Optimization

- **Partitioning:** Partition by date for time-series data
- **Bucketing:** Bucket large tables by frequently joined columns
- **Caching:** Cache intermediate results for iterative algorithms
- **Broadcast Joins:** Use broadcast joins for small dimension tables
- **Predicate Pushdown:** Filter data early in the pipeline
- **Columnar Format:** Use Parquet/Delta for efficient storage

### 6.2 Database Optimization

- **Indexing:** B-tree indexes on foreign keys, bitmap indexes for low-cardinality columns
- **Materialized Views:** Pre-compute aggregations for reports
- **Query Optimization:** Use EXPLAIN PLAN, avoid SELECT *, optimize JOINs
- **Partitioning:** Partition large tables by date
- **Connection Pooling:** Use connection pools for application connections

### 6.3 Airflow Optimization

- **Parallelism:** Configure max_active_runs and parallelism
- **Pool Management:** Use pools to limit concurrent tasks
- **Task Dependencies:** Optimize DAG structure to minimize wait time
- **Retry Logic:** Configure appropriate retry counts and intervals

---

## 7. Data Quality & Monitoring

### 7.1 Data Quality Checks

- **Completeness:** Check for NULL values in critical columns
- **Uniqueness:** Validate primary key constraints
- **Validity:** Check data types and value ranges
- **Consistency:** Verify referential integrity
- **Timeliness:** Monitor data freshness

### 7.2 Monitoring Metrics

- **Pipeline Success Rate:** Target > 99%
- **Data Latency:** Target < 1 hour for critical data
- **Query Performance:** Target < 5 seconds for reports
- **Data Quality Score:** Target > 95%
- **System Uptime:** Target > 99.5%

---

## 8. Security & Compliance

### 8.1 Data Security

- **Encryption:** Encrypt data at rest and in transit
- **Access Control:** Role-based access control (RBAC)
- **Audit Logging:** Log all data access and modifications
- **Data Masking:** Mask sensitive data in non-production environments

### 8.2 Compliance

- **Data Retention:** Implement data retention policies
- **GDPR Compliance:** Ensure data privacy regulations
- **Backup & Recovery:** Regular backups and disaster recovery plans

---

## 9. Scalability & Future Enhancements

### 9.1 Scalability

- **Horizontal Scaling:** Add more Spark workers
- **Database Scaling:** Implement read replicas for Oracle
- **Storage Scaling:** Use distributed storage (S3, HDFS)

### 9.2 Future Enhancements

- **Real-time Streaming:** Implement Spark Structured Streaming
- **Data Lakehouse:** Migrate to Delta Lake for unified batch/streaming
- **ML Integration:** Build feature store for ML models
- **Data Catalog:** Implement data catalog for metadata management
- **Automated Testing:** Add unit tests and integration tests for pipelines

---

## 10. Troubleshooting Guide

### 10.1 Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Spark job slow | Insufficient resources | Increase executor memory/cores |
| Oracle query slow | Missing indexes | Analyze execution plan, add indexes |
| Airflow DAG failed | Dependency issue | Check task logs, fix dependencies |
| Data quality issues | Source data problems | Implement data validation checks |

### 10.2 Debugging Steps

1. Check Airflow logs for task failures
2. Analyze Spark execution plans
3. Review Oracle AWR reports
4. Monitor system resources (CPU, memory, disk)
5. Validate data at each pipeline stage

---

*Tài liệu này cung cấp tổng quan chi tiết về kiến trúc hệ thống Data Engineering, phù hợp với yêu cầu công việc Data Engineer tại ATS Vietnam.*