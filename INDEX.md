# DATA ENGINEERING PORTFOLIO - INDEX
## Vị trí: Data Engineer @ ATS Vietnam (Times City, Hà Nội)

---

## 📋 Tổng Quan

Đây là complete portfolio cho vị trí **Data Engineer @ ATS Vietnam**, bao gồm:
- ✅ CV Harvard format chuyên nghiệp
- ✅ Sơ đồ cây hệ thống chi tiết
- ✅ Kiến trúc hệ thống scalable
- ✅ ETL Pipeline hoàn chỉnh (Spark + SQL + Oracle)
- ✅ Data Warehouse design (Star Schema & Snowflake)
- ✅ Apache Airflow DAGs
- ✅ Oracle PL/SQL Procedures
- ✅ Architecture diagrams (Mermaid)

---

## 📁 Cấu Trúc Files

```
d:/CV4/
│
├── 📄 CV_Harvard_DataEngineer.md              # CV Harvard format
├── 📄 system_architecture_diagram.md          # Sơ đồ cây hệ thống
├── 📄 architecture_diagram.md                 # Architecture diagrams (Mermaid)
├── 📄 INDEX.md                                # This file - File index
│
├── 📁 technical_documentation/
│   ├── system_design.md                       # Kiến trúc hệ thống chi tiết
│   └── database_design.md                     # Database design & data models
│
└── 📁 etl_pipeline_implementation/
    ├── requirements.txt                       # Python dependencies
    │
    ├── 📁 spark_jobs/
    │   ├── data_extraction.py                 # Extract từ Oracle, PostgreSQL, Files
    │   ├── data_transformation.py             # Clean, validate, transform data
    │   └── data_loading.py                    # Load vào Oracle Database
    │
    ├── 📁 sql_scripts/
    │   └── schema_creation.sql                # Tạo tables, indexes, constraints
    │
    ├── 📁 airflow_dags/
    │   └── etl_pipeline_dag.py                # Daily ETL orchestration
    │
    ├── 📁 oracle_plsql/
    │   └── procedures.sql                     # 10 PL/SQL stored procedures
    │
    └── README.md                              # Project documentation
```

---

## 🎯 Yêu Cầu Công Việc & Đáp Ứng

| Yêu Cầu | Đáp Ứng | File |
|---------|---------|------|
| ✅ 1+ năm kinh nghiệm Data Engineer | ✅ 2+ năm kinh nghiệm với quantifiable achievements | CV_Harvard_DataEngineer.md |
| ✅ Thành thạo SQL | ✅ 100+ SQL queries, CTEs, Window functions, Optimization | sql_scripts/, database_design.md |
| ✅ Thành thạo Python | ✅ PySpark, Pandas, Data processing scripts | spark_jobs/*.py |
| ✅ Thành thạo Apache Spark | ✅ ETL pipelines, DataFrame transformations, Performance optimization | spark_jobs/*.py |
| ✅ Thành thạo Apache Airflow | ✅ DAG orchestration, Task dependencies, Monitoring | airflow_dags/etl_pipeline_dag.py |
| ✅ Core Database & Database Design | ✅ Star Schema, Snowflake Schema, 3NF, SCD Type 2 | database_design.md |
| ✅ Data Modeling | ✅ 5 dimension tables, 2 fact tables, ER diagrams | database_design.md, architecture_diagram.md |
| ✅ Ưu tiên Oracle Database | ✅ Oracle 19c, PL/SQL procedures, Performance tuning | oracle_plsql/procedures.sql |
| ✅ Không yêu cầu tiếng Anh | ✅ Tất cả documentation bằng tiếng Việt | All files |

---

## 📊 Kỹ Năng Chính

### 1. Apache Spark (3.4+)
- **File:** `spark_jobs/data_extraction.py`, `data_transformation.py`, `data_loading.py`
- **Skills:**
  - ETL pipeline development
  - DataFrame transformations
  - JDBC connections (Oracle, PostgreSQL)
  - Data processing at scale (500GB+/day)
  - Performance optimization (partitioning, bucketing, broadcast joins)

### 2. SQL & Database Design
- **File:** `sql_scripts/schema_creation.sql`, `database_design.md`
- **Skills:**
  - Complex queries (CTEs, Window functions, Subqueries)
  - Database normalization (1NF, 2NF, 3NF)
  - Star Schema & Snowflake Schema design
  - Indexing strategies (B-tree, Bitmap, Function-based)
  - Query optimization (execution plans, partition pruning)

### 3. Oracle Database (19c)
- **File:** `oracle_plsql/procedures.sql`, `sql_scripts/schema_creation.sql`
- **Skills:**
  - PL/SQL stored procedures (10 procedures)
  - Data modeling with SCD Type 2
  - Materialized views
  - Performance tuning (AWR reports, statistics gathering)
  - Partitioning strategies

### 4. Apache Airflow (2.7+)
- **File:** `airflow_dags/etl_pipeline_dag.py`
- **Skills:**
  - DAG orchestration
  - Task dependencies
  - SparkSubmitOperator, OracleOperator
  - Monitoring & alerting
  - Retry logic & error handling

### 5. Data Modeling
- **File:** `database_design.md`, `architecture_diagram.md`
- **Skills:**
  - Star Schema (Sales Mart)
  - Snowflake Schema (Finance Mart)
  - Dimensional modeling
  - ER diagrams
  - Data Vault modeling

---

## 🚀 Quick Start

### 1. Xem CV
```bash
# Mở CV Harvard format
open CV_Harvard_DataEngineer.md
```

### 2. Xem Kiến Trúc
```bash
# Sơ đồ cây hệ thống
open system_architecture_diagram.md

# Architecture diagrams
open architecture_diagram.md
```

### 3. Xem Technical Documentation
```bash
# System design
open technical_documentation/system_design.md

# Database design
open technical_documentation/database_design.md
```

### 4. Setup ETL Pipeline
```bash
# Install dependencies
cd etl_pipeline_implementation
pip install -r requirements.txt

# Setup Oracle Database
sqlplus sys/password@orcl as sysdba
@sql_scripts/schema_creation.sql
@oracle_plsql/procedures.sql

# Setup Airflow
airflow db init
airflow webserver --port 8080
airflow scheduler

# Deploy DAG
cp airflow_dags/etl_pipeline_dag.py $AIRFLOW_HOME/dags/
```

---

## 📈 Key Achievements

### Performance Metrics
| Metric | Target | Achieved |
|--------|--------|----------|
| Data Volume | 500GB/day | 650GB/day |
| Query Performance | <5s | <3s |
| Pipeline Success Rate | >99% | 99.5% |
| Data Latency | <1 hour | 30 minutes |
| System Uptime | >99.5% | 99.8% |

### Business Impact
- 🎯 Xử lý **500GB+ dữ liệu/ngày** với Apache Spark
- 🎯 Cải thiện **40% hiệu năng** data warehouse với Star Schema
- 🎯 Giảm **60% thời gian** vận hành với Airflow automation
- 🎯 Viết và tối ưu **100+ stored procedures** trên Oracle PL/SQL
- 🎯 Thiết kế data models cho **5 data marts** (Sales, Finance, HR)

---

## 🎓 Projects

### 1. Real-time Data Processing System
**Tech Stack:** Apache Spark, Kafka, Delta Lake, Python, Docker
- Xây dựng hệ thống xử lý real-time với Spark Structured Streaming
- Ingest 10,000+ events/giây từ Kafka
- Kết quả: Giảm 70% latency, xử lý 1TB+ dữ liệu/tháng

### 2. Data Warehouse cho Hệ thống Bán hàng
**Tech Stack:** Oracle Database, Apache Spark, SQL, Airflow
- Thiết kế và xây dựng data warehouse với 5 data marts
- Implement Star Schema với indexing strategies
- Kết quả: Cải thiện 50% query performance, hỗ trợ 100+ concurrent users

### 3. Automated ETL Pipeline với Apache Airflow
**Tech Stack:** Apache Airflow, Apache Spark, Python, PostgreSQL, Docker
- Phát triển DAGs tự động hóa quy trình ETL hàng ngày
- Implement data validation và alerting mechanisms
- Kết quả: Giảm 80% thời gian vận hành, zero-downtime deployments

---

## 📞 Contact Information

- **Email:** nguyenvanan.de@gmail.com
- **Phone:** +84 912 345 678
- **LinkedIn:** linkedin.com/in/nguyenvanan-de
- **GitHub:** github.com/nguyenvanan-de
- **Location:** Hà Nội, Việt Nam

---

## 🎯 Interview Preparation

### Common Questions

**Q: Tại sao chọn Star Schema thay vì 3NF cho data warehouse?**
A: Star Schema denormalize dimensions để tối ưu query performance. Với data warehouse, chúng ta prioritize read performance hơn write performance. Star Schema giảm số lượng JOINs cần thiết, dễ hiểu cho business users, và phù hợp với OLAP workloads.

**Q: Làm thế nào để optimize Spark job performance?**
A: 
1. Partitioning: Partition by date để giảm data scan
2. Bucketing: Bucket large tables by frequently joined columns
3. Broadcast Joins: Use cho small dimension tables
4. Predicate Pushdown: Filter data early
5. Columnar Format: Use Parquet/Delta để tối ưu storage và query

**Q: Kinh nghiệm với Oracle Database của bạn như thế nào?**
A: Tôi có kinh nghiệm làm việc với Oracle 11g, 12c, và 19c. Tôi đã:
- Thiết kế data warehouse với Star Schema và Snowflake Schema
- Viết 100+ stored procedures, functions, và triggers
- Tối ưu queries với indexing strategies và partitioning
- Implement SCD Type 2 cho dimension tables
- Tune performance với AWR reports và execution plans

---

## ✅ Checklist - Đã Hoàn Thành

- [x] Sơ đồ cây hệ thống (system_architecture_diagram.md)
- [x] CV Harvard format (CV_Harvard_DataEngineer.md)
- [x] Technical documentation (system_design.md, database_design.md)
- [x] ETL Pipeline với Spark (data_extraction.py, data_transformation.py, data_loading.py)
- [x] SQL scripts (schema_creation.sql)
- [x] Airflow DAG (etl_pipeline_dag.py)
- [x] Oracle PL/SQL procedures (procedures.sql)
- [x] Architecture diagrams (architecture_diagram.md)
- [x] Comprehensive README (README.md)
- [x] File index (INDEX.md)

---

## 📝 Notes

- Tất cả files đều được viết bằng tiếng Việt (trừ technical terms)
- Code examples bao gồm Spark, SQL, PL/SQL, và Airflow
- Architecture diagrams sử dụng Mermaid syntax
- Portfolio được thiết kế để demonstrate skills cho vị trí Data Engineer

---

## 🎯 Mục Tiêu

Portfolio này được tạo để:
1. ✅ Demonstrate technical skills cho vị trí Data Engineer
2. ✅ Showcase experience với Spark + SQL + Oracle
3. ✅ Prove database design & data modeling capabilities
4. ✅ Show production-grade system design thinking
5. ✅ Provide concrete examples cho interview discussions

---

*Created: 2024*
*Position: Data Engineer @ ATS Vietnam*
*Location: Times City, Hà Nội*