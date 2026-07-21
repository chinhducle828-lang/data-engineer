# Architecture Diagram - Data Engineering Portfolio
## Data Engineer @ ATS Vietnam

---

## 1. High-Level System Architecture

```mermaid
graph TB
    subgraph "Source Systems"
        ORACLE[(Oracle DB<br/>500GB/day)]
        POSTGRES[(PostgreSQL<br/>100GB/day)]
        FILES[(Flat Files<br/>50GB/day)]
    end
    
    subgraph "Data Ingestion Layer"
        SPARK_EXTRACT[Apache Spark<br/>Extract Layer]
        JDBC[JDBC Connectors]
        FILE_READERS[File Readers<br/>CSV/JSON/Parquet]
    end
    
    subgraph "Data Lake - Raw Zone"
        RAW_S3[S3/HDFS<br/>Parquet Format]
        PARTITION_RAW[Partitioned by<br/>Date & Source]
    end
    
    subgraph "Data Transformation Layer"
        SPARK_TRANSFORM[Apache Spark<br/>Transform Layer]
        CLEAN[Data Cleaning]
        VALIDATE[Data Validation]
        INTEGRATE[Data Integration]
        AGGREGATE[Aggregation]
    end
    
    subgraph "Data Warehouse - Curated Zone"
        ORACLE_DW[(Oracle Database<br/>19c)]
        STAR_SCHEMA[Star Schema<br/>Sales Mart]
        SNOWFLAKE[Snowflake Schema<br/>Finance Mart]
    end
    
    subgraph "Data Marts"
        SALES_MART[Sales Mart]
        FINANCE_MART[Finance Mart]
        HR_MART[HR Mart]
    end
    
    subgraph "Analytics Layer"
        BI[BI Tools<br/>Tableau/Power BI]
        REPORTS[Reports<br/>Materialized Views]
        ML[ML Features]
    end
    
    subgraph "Orchestration"
        AIRFLOW[Apache Airflow<br/>DAG Scheduler]
        MONITOR[Monitoring<br/>& Alerting]
    end
    
    ORACLE --> JDBC
    POSTGRES --> JDBC
    FILES --> FILE_READERS
    
    JDBC --> SPARK_EXTRACT
    FILE_READERS --> SPARK_EXTRACT
    
    SPARK_EXTRACT --> RAW_S3
    RAW_S3 --> PARTITION_RAW
    
    PARTITION_RAW --> SPARK_TRANSFORM
    SPARK_TRANSFORM --> CLEAN
    CLEAN --> VALIDATE
    VALIDATE --> INTEGRATE
    INTEGRATE --> AGGREGATE
    
    AGGREGATE --> ORACLE_DW
    ORACLE_DW --> STAR_SCHEMA
    ORACLE_DW --> SNOWFLAKE
    
    STAR_SCHEMA --> SALES_MART
    SNOWFLAKE --> FINANCE_MART
    STAR_SCHEMA --> HR_MART
    
    SALES_MART --> BI
    FINANCE_MART --> BI
    HR_MART --> BI
    
    SALES_MART --> REPORTS
    FINANCE_MART --> REPORTS
    
    SALES_MART --> ML
    FINANCE_MART --> ML
    
    AIRFLOW --> SPARK_EXTRACT
    AIRFLOW --> SPARK_TRANSFORM
    AIRFLOW --> ORACLE_DW
    
    AIRFLOW --> MONITOR
    MONITOR --> AIRFLOW
    
    style ORACLE fill:#e74c3c
    style POSTGRES fill:#e74c3c
    style ORACLE_DW fill:#e74c3c
    style SPARK_EXTRACT fill:#f39c12
    style SPARK_TRANSFORM fill:#f39c12
    style AIRFLOW fill:#3498db
    style RAW_S3 fill:#95a5a6
    style BI fill:#2ecc71
```

---

## 2. Data Pipeline Flow Diagram

```mermaid
flowchart LR
    A[1. EXTRACT<br/>Oracle/PostgreSQL/Files] --> B[2. RAW ZONE<br/>Parquet/S3]
    B --> C[3. TRANSFORM<br/>Clean/Validate/Integrate]
    C --> D[4. CURATED ZONE<br/>Parquet]
    D --> E[5. LOAD<br/>Oracle Database]
    E --> F[6. DATA MARTS<br/>Sales/Finance/HR]
    F --> G[7. CONSUMPTION<br/>BI/Reports/ML]
    
    H[Apache Airflow<br/>Orchestration] -.-> A
    H -.-> C
    H -.-> E
    
    I[Monitoring<br/>& Alerting] -.-> H
```

---

## 3. Star Schema - Sales Data Mart

```mermaid
erDiagram
    DIM_DATE ||--o{ FACT_SALES : "has"
    DIM_PRODUCT ||--o{ FACT_SALES : "has"
    DIM_CUSTOMER ||--o{ FACT_SALES : "has"
    DIM_STORE ||--o{ FACT_SALES : "has"
    
    DIM_DATE {
        number date_key PK
        date full_date
        number year
        number month_number
        string month_name
        number quarter
        string quarter_name
        string is_weekend
        string is_holiday
    }
    
    DIM_PRODUCT {
        number product_key PK
        string product_id
        string product_name
        number category_key FK
        string category_name
        string brand
        number unit_price
        number cost_price
        date effective_date
        date expiry_date
        string is_current
    }
    
    DIM_CUSTOMER {
        number customer_key PK
        string customer_id
        string customer_name
        string email
        string phone
        string city
        number region_key FK
        string region_name
        string segment
        date effective_date
        date expiry_date
        string is_current
    }
    
    DIM_STORE {
        number store_key PK
        string store_id
        string store_name
        string store_type
        string city
        string region
        date opening_date
        date effective_date
        date expiry_date
        string is_current
    }
    
    FACT_SALES {
        number sales_key PK
        number date_key FK
        number product_key FK
        number customer_key FK
        number store_key FK
        number quantity_sold
        number unit_price
        number gross_amount
        number discount_amount
        number net_amount
        number cost_amount
        number profit_amount
        timestamp created_at
        string source_system
    }
```

---

## 4. Snowflake Schema - Finance Data Mart

```mermaid
erDiagram
    DIM_DATE ||--o{ FACT_REVENUE : "has"
    DIM_PRODUCT ||--o{ FACT_REVENUE : "has"
    DIM_CUSTOMER ||--o{ FACT_REVENUE : "has"
    DIM_CHANNEL ||--o{ FACT_REVENUE : "has"
    DIM_SOURCE ||--o{ FACT_REVENUE : "has"
    
    DIM_PRODUCT }|--|| DIM_CATEGORY : "belongs_to"
    DIM_CUSTOMER }|--|| DIM_REGION : "belongs_to"
    
    DIM_DATE {
        number date_key PK
        date full_date
        number year
        number month_number
        string month_name
    }
    
    DIM_PRODUCT {
        number product_key PK
        string product_id
        string product_name
        number category_key FK
    }
    
    DIM_CATEGORY {
        number category_key PK
        string category_id
        string category_name
        string parent_category
    }
    
    DIM_CUSTOMER {
        number customer_key PK
        string customer_id
        string customer_name
        number region_key FK
    }
    
    DIM_REGION {
        number region_key PK
        string region_id
        string region_name
        string country
    }
    
    DIM_CHANNEL {
        number channel_key PK
        string channel_id
        string channel_name
        string channel_type
    }
    
    DIM_SOURCE {
        number source_key PK
        string source_id
        string source_name
        string source_type
    }
    
    FACT_REVENUE {
        number revenue_key PK
        number date_key FK
        number product_key FK
        number customer_key FK
        number channel_key FK
        number source_key FK
        number revenue_amount
        number cost_amount
        number profit_amount
        number tax_amount
    }
```

---

## 5. Airflow DAG Flow

```mermaid
graph LR
    START([Start Pipeline]) --> EXTRACT1[Extract Oracle Data]
    START --> EXTRACT2[Extract PostgreSQL Data]
    
    EXTRACT1 --> TRANSFORM[Transform Data]
    EXTRACT2 --> TRANSFORM
    
    TRANSFORM --> LOAD[Load to Data Warehouse]
    LOAD --> QUALITY[Data Quality Checks]
    
    QUALITY -->|Pass| REFRESH[Refresh Materialized Views]
    QUALITY -->|Fail| ALERT[Send Alert]
    
    REFRESH --> LOG[Log Pipeline Run]
    LOG --> NOTIFY[Send Notification]
    NOTIFY --> END([End Pipeline])
    
    ALERT --> END
    
    style START fill:#2ecc71
    style END fill:#e74c3c
    style QUALITY fill:#f39c12
    style ALERT fill:#e74c3c
```

---

## 6. Technology Stack Components

```mermaid
graph TB
    subgraph "Data Processing"
        SPARK[Apache Spark 3.4+]
        SPARK_SQL[Spark SQL]
        PYSPARK[PySpark]
    end
    
    subgraph "Orchestration"
        AIRFLOW[Apache Airflow 2.7+]
        DAG[DAG Scheduler]
        SENSORS[Sensors & Triggers]
    end
    
    subgraph "Databases"
        ORACLE[Oracle Database 19c]
        POSTGRES[PostgreSQL 15+]
    end
    
    subgraph "Storage"
        S3[AWS S3]
        PARQUET[Parquet Format]
        DELTA[Delta Lake]
    end
    
    subgraph "Programming"
        PYTHON[Python 3.9+]
        PANDAS[Pandas]
        NUMPY[NumPy]
    end
    
    subgraph "DevOps"
        DOCKER[Docker]
        GIT[Git]
        LINUX[Linux/Unix]
    end
    
    SPARK --> SPARK_SQL
    SPARK --> PYSPARK
    
    AIRFLOW --> DAG
    AIRFLOW --> SENSORS
    
    PYSPARK --> SPARK
    PYSPARK --> PANDAS
    PYSPARK --> NUMPY
    
    SPARK --> S3
    S3 --> PARQUET
    PARQUET --> DELTA
    
    SPARK --> ORACLE
    SPARK --> POSTGRES
    
    AIRFLOW --> SPARK
    AIRFLOW --> ORACLE
    
    DOCKER --> SPARK
    DOCKER --> AIRFLOW
    DOCKER --> ORACLE
    
    style SPARK fill:#f39c12
    style AIRFLOW fill:#3498db
    style ORACLE fill:#e74c3c
    style PYTHON fill:#2ecc71
```

---

## 7. Data Flow Details

```mermaid
flowchart TD
    subgraph "Extract Phase"
        E1[Oracle JDBC<br/>500GB/day]
        E2[PostgreSQL JDBC<br/>100GB/day]
        E3[File Readers<br/>50GB/day]
    end
    
    subgraph "Raw Zone"
        R1[Partition: by_date]
        R2[Format: Parquet]
        R3[Compression: Snappy]
    end
    
    subgraph "Transform Phase"
        T1[Data Cleaning]
        T2[Data Validation]
        T3[Business Logic]
        T4[Data Integration]
        T5[Aggregation]
    end
    
    subgraph "Curated Zone"
        C1[Star Schema]
        C2[Snowflake Schema]
        C3[Metadata Tables]
    end
    
    subgraph "Load Phase"
        L1[Oracle JDBC Write]
        L2[Batch Size: 10K]
        L3[Partition by date_key]
    end
    
    E1 --> R1
    E2 --> R1
    E3 --> R1
    
    R1 --> T1
    R2 --> T1
    R3 --> T1
    
    T1 --> T2
    T2 --> T3
    T3 --> T4
    T4 --> T5
    
    T5 --> C1
    T5 --> C2
    T5 --> C3
    
    C1 --> L1
    C2 --> L1
    C3 --> L1
    
    L1 --> L2
    L2 --> L3
```

---

## 8. Component Interaction Diagram

```mermaid
sequenceDiagram
    participant A as Airflow Scheduler
    participant E as Spark Extract
    participant R as Raw Zone (S3)
    participant T as Spark Transform
    participant C as Curated Zone
    participant L as Spark Load
    participant D as Oracle DW
    participant Q as Data Quality
    participant M as Materialized Views
    
    A->>E: Trigger Extract Task
    E->>R: Write Raw Data (Parquet)
    R-->>E: Confirm Write
    E-->>A: Task Complete
    
    A->>T: Trigger Transform Task
    T->>R: Read Raw Data
    R-->>T: Return Data
    T->>C: Write Curated Data
    C-->>T: Confirm Write
    T-->>A: Task Complete
    
    A->>L: Trigger Load Task
    L->>C: Read Curated Data
    C-->>L: Return Data
    L->>D: Load to Oracle
    D-->>L: Confirm Load
    L-->>A: Task Complete (XCom: records_loaded)
    
    A->>Q: Run Quality Checks
    Q->>D: Query Tables
    D-->>Q: Return Results
    Q-->>A: Quality Status
    
    A->>M: Refresh MVs
    M->>D: Refresh Materialized Views
    D-->>M: Confirm Refresh
    M-->>A: Task Complete
    
    A->>A: Log Pipeline Run
    A->>A: Send Notification
```

---

## 9. Database Architecture

```mermaid
graph TB
    subgraph "Oracle Database 19c"
        subgraph "Dimension Tables"
            DIM_DATE[DIM_DATE]
            DIM_PRODUCT[DIM_PRODUCT]
            DIM_CUSTOMER[DIM_CUSTOMER]
            DIM_STORE[DIM_STORE]
            DIM_CATEGORY[DIM_CATEGORY]
            DIM_REGION[DIM_REGION]
            DIM_CHANNEL[DIM_CHANNEL]
            DIM_SOURCE[DIM_SOURCE]
        end
        
        subgraph "Fact Tables"
            FACT_SALES[FACT_SALES<br/>Partitioned by date_key]
            FACT_REVENUE[FACT_REVENUE<br/>Partitioned by date_key]
        end
        
        subgraph "Metadata Tables"
            META_PIPELINE[METADATA_PIPELINE_RUN]
            META_LINEAGE[METADATA_DATA_LINEAGE]
            META_QUALITY[METADATA_DATA_QUALITY]
        end
        
        subgraph "Materialized Views"
            MV_SALES[MV_SALES_MONTHLY_SUMMARY]
            MV_CUSTOMER[MV_CUSTOMER_ANALYTICS]
        end
        
        subgraph "Indexes"
            IDX1[B-tree Indexes]
            IDX2[Bitmap Indexes]
            IDX3[Partition Indexes]
        end
    end
    
    DIM_DATE --> FACT_SALES
    DIM_PRODUCT --> FACT_SALES
    DIM_CUSTOMER --> FACT_SALES
    DIM_STORE --> FACT_SALES
    
    DIM_DATE --> FACT_REVENUE
    DIM_PRODUCT --> FACT_REVENUE
    DIM_CUSTOMER --> FACT_REVENUE
    DIM_CHANNEL --> FACT_REVENUE
    DIM_SOURCE --> FACT_REVENUE
    
    DIM_CATEGORY --> DIM_PRODUCT
    DIM_REGION --> DIM_CUSTOMER
    
    FACT_SALES --> MV_SALES
    DIM_CUSTOMER --> MV_CUSTOMER
    
    META_PIPELINE --> META_LINEAGE
    META_PIPELINE --> META_QUALITY
    
    style FACT_SALES fill:#e74c3c
    style FACT_REVENUE fill:#e74c3c
    style MV_SALES fill:#2ecc71
    style MV_CUSTOMER fill:#2ecc71
```

---

## 10. Performance Optimization Strategies

```mermaid
mindmap
  root((Performance<br/>Optimization))
    Spark Optimization
      Partitioning
        Partition by date
        Partition by source
      Bucketing
        Bucket by product_key
        Bucket by customer_key
      Caching
        Cache intermediate results
        Broadcast joins
      Predicate Pushdown
        Filter early
        Reduce data scan
    Database Optimization
      Indexing
        B-tree indexes
        Bitmap indexes
        Function-based indexes
      Partitioning
        Range partitioning
        Subpartitioning
      Materialized Views
        Pre-compute aggregations
        Refresh strategies
      Query Optimization
        Execution plans
        Avoid SELECT *
        Optimize JOINs
    Airflow Optimization
      Parallelism
        Max active runs
        Concurrent tasks
      Pool Management
        Resource pools
        Limit concurrency
      Retry Logic
        Exponential backoff
        Error handling
```

---

## 11. Data Quality Framework

```mermaid
graph LR
    subgraph "Data Quality Checks"
        COMPLETENESS[Completeness<br/>NULL Checks]
        UNIQUENESS[Uniqueness<br/>Primary Key Validation]
        VALIDITY[Validity<br/>Data Type & Range]
        CONSISTENCY[Consistency<br/>Referential Integrity]
        TIMELINESS[Timeliness<br/>Data Freshness]
    end
    
    subgraph "Quality Levels"
        EXTRACT[Extraction Level]
        TRANSFORM[Transformation Level]
        LOAD[Loading Level]
        POSTLOAD[Post-Load Level]
    end
    
    EXTRACT --> COMPLETENESS
    TRANSFORM --> VALIDITY
    LOAD --> CONSISTENCY
    POSTLOAD --> TIMELINESS
    
    COMPLETENESS --> METADATA[METADATA_DATA_QUALITY]
    UNIQUENESS --> METADATA
    VALIDITY --> METADATA
    CONSISTENCY --> METADATA
    TIMELINESS --> METADATA
    
    METADATA --> ALERT[Alerting]
    METADATA --> DASHBOARD[Quality Dashboard]
```

---

## 12. Deployment Architecture

```mermaid
graph TB
    subgraph "Development Environment"
        DEV_SPARK[Spark Local]
        DEV_ORACLE[Oracle XE]
        DEV_AIRFLOW[Airflow Local]
    end
    
    subgraph "Production Environment"
        subgraph "Spark Cluster"
            SPARK_MASTER[Spark Master]
            SPARK_WORKER1[Worker 1]
            SPARK_WORKER2[Worker 2]
            SPARK_WORKER3[Worker 3]
        end
        
        subgraph "Airflow Cluster"
            AIRFLOW_WEBSERVER[Webserver]
            AIRFLOW_SCHEDULER[Scheduler]
            AIRFLOW_WORKER[Worker]
        end
        
        subgraph "Database Cluster"
            ORACLE_PRIMARY[Oracle Primary]
            ORACLE_STANDBY[Oracle Standby]
        end
        
        subgraph "Storage"
            S3_BUCKET[S3 Bucket]
            BACKUP[Backup Storage]
        end
    end
    
    subgraph "Monitoring"
        GRAFANA[Grafana]
        PROMETHEUS[Prometheus]
        ALERTMANAGER[Alert Manager]
    end
    
    DEV_SPARK --> SPARK_MASTER
    DEV_ORACLE --> ORACLE_PRIMARY
    DEV_AIRFLOW --> AIRFLOW_WEBSERVER
    
    SPARK_MASTER --> SPARK_WORKER1
    SPARK_MASTER --> SPARK_WORKER2
    SPARK_MASTER --> SPARK_WORKER3
    
    AIRFLOW_WEBSERVER --> AIRFLOW_SCHEDULER
    AIRFLOW_SCHEDULER --> AIRFLOW_WORKER
    
    AIRFLOW_WORKER --> SPARK_MASTER
    AIRFLOW_WORKER --> ORACLE_PRIMARY
    
    SPARK_WORKER1 --> S3_BUCKET
    SPARK_WORKER2 --> S3_BUCKET
    SPARK_WORKER3 --> S3_BUCKET
    
    ORACLE_PRIMARY --> ORACLE_STANDBY
    ORACLE_PRIMARY --> BACKUP
    
    SPARK_MASTER --> PROMETHEUS
    AIRFLOW_SCHEDULER --> PROMETHEUS
    ORACLE_PRIMARY --> PROMETHEUS
    
    PROMETHEUS --> GRAFANA
    PROMETHEUS --> ALERTMANAGER
    
    style SPARK_MASTER fill:#f39c12
    style AIRFLOW_SCHEDULER fill:#3498db
    style ORACLE_PRIMARY fill:#e74c3c
    style GRAFANA fill:#2ecc71
```

---

## 13. Security & Compliance

```mermaid
graph TB
    subgraph "Data Security"
        ENCRYPT_AT_REST[Encryption at Rest<br/>TDE/SSL]
        ENCRYPT_IN_TRANSIT[Encryption in Transit<br/>TLS/SSL]
        ACCESS_CONTROL[Access Control<br/>RBAC]
        AUDIT_LOGGING[Audit Logging<br/>All Access]
        DATA_MASKING[Data Masking<br/>Non-Production]
    end
    
    subgraph "Compliance"
        DATA_RETENTION[Data Retention<br/>Policies]
        GDPR[GDPR Compliance<br/>Data Privacy]
        BACKUP[Backup & Recovery<br/>Disaster Recovery]
    end
    
    subgraph "Monitoring"
        SECURITY_MONITOR[Security Monitoring]
        ANOMALY_DETECTION[Anomaly Detection]
        ALERTING[Real-time Alerting]
    end
    
    ENCRYPT_AT_REST --> ACCESS_CONTROL
    ENCRYPT_IN_TRANSIT --> ACCESS_CONTROL
    ACCESS_CONTROL --> AUDIT_LOGGING
    
    AUDIT_LOGGING --> SECURITY_MONITOR
    DATA_MASKING --> SECURITY_MONITOR
    
    SECURITY_MONITOR --> ANOMALY_DETECTION
    ANOMALY_DETECTION --> ALERTING
    
    DATA_RETENTION --> BACKUP
    GDPR --> DATA_RETENTION
    
    style ENCRYPT_AT_REST fill:#e74c3c
    style ACCESS_CONTROL fill:#3498db
    style GDPR fill:#f39c12
```

---

## 14. Scalability & Future Enhancements

```mermaid
graph LR
    subgraph "Current State"
        CURRENT[Batch Processing<br/>650GB/day]
    end
    
    subgraph "Short Term"
        ST1[Real-time Streaming<br/>Spark Structured Streaming]
        ST2[Data Lakehouse<br/>Delta Lake]
        ST3[ML Integration<br/>Feature Store]
    end
    
    subgraph "Long Term"
        LT1[Multi-Cloud<br/>AWS/GCP/Azure]
        LT2[Data Mesh<br/>Domain-Oriented]
        LT3[AI/ML Platform<br/>AutoML]
    end
    
    CURRENT --> ST1
    CURRENT --> ST2
    CURRENT --> ST3
    
    ST1 --> LT1
    ST2 --> LT2
    ST3 --> LT3
    
    style CURRENT fill:#95a5a6
    style ST1 fill:#f39c12
    style ST2 fill:#3498db
    style LT1 fill:#2ecc71
```

---

## Summary

This architecture diagram illustrates a complete data engineering solution that includes:

✅ **Scalable Architecture:** Handle 650GB+ data/day with Spark  
✅ **Robust ETL Pipeline:** Extract, Transform, Load with validation  
✅ **Data Warehouse:** Star Schema & Snowflake Schema on Oracle  
✅ **Orchestration:** Airflow DAGs for automation  
✅ **Monitoring:** Data quality checks and alerting  
✅ **Performance:** Optimized with partitioning, indexing, and materialized views  
✅ **Security:** Encryption, access control, and audit logging  
✅ **Scalability:** Horizontal scaling with Spark cluster  

This demonstrates the skills required for the Data Engineer position at ATS Vietnam, including:
- Apache Spark + SQL + Database expertise
- Data modeling and database design
- ETL pipeline development
- Performance optimization
- Production-grade system design