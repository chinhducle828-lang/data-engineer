# Sơ đồ Cây Hệ Thống - Data Engineering Portfolio
## Vị trí: Data Engineer @ ATS Vietnam (Times City, Hà Nội)

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATA ENGINEERING PORTFOLIO                    │
│                    Harvard Format CV + Technical Demo            │
└─────────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
        ▼                     ▼                     ▼
┌──────────────┐    ┌──────────────────┐   ┌─────────────────┐
│  HARVARD CV  │    │  SYSTEM DESIGN   │   │  TECHNICAL      │
│   (DOCX)     │    │  DOCUMENTATION   │   │  IMPLEMENTATION │
└──────────────┘    └──────────────────┘   └─────────────────┘
        │                     │                     │
        │                     │                     │
        ▼                     ▼                     ▼
┌──────────────┐    ┌──────────────────┐   ┌─────────────────┐
│ • Personal   │    │ • Architecture   │   │ • ETL Pipeline  │
│   Info       │    │   Diagram        │   │   (Spark + SQL) │
│ • Education  │    │ • Data Flow      │   │ • Data Models   │
│ • Experience │    │ • Tech Stack     │   │ • Airflow DAG   │
│ • Skills     │    │ • Database Design│   │ • Oracle SQL    │
│ • Projects   │    │ • ER Diagrams    │   │ • Python Scripts│
└──────────────┘    └──────────────────┘   └─────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │  DELIVERABLES    │
                    └──────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
        ▼                     ▼                     ▼
┌──────────────┐    ┌──────────────────┐   ┌─────────────────┐
│  CV_Harvard  │    │  architecture_   │   │  etl_pipeline/  │
│  _DE.docx    │    │  diagram.png     │   │  - spark_jobs/  │
│              │    │                  │   │  - sql_scripts/ │
│              │    │                  │   │  - airflow_dags/│
│              │    │                  │   │  - data_models/ │
│              │    │                  │   │  - oracle_plsql/│
└──────────────┘    └──────────────────┘   └─────────────────┘
```

## Chi tiết Module:

### 1. HARVARD CV (CV_Harvard_DE.docx)
```
├── Header Information
│   ├── Full Name
│   ├── Contact Info (Phone, Email, Location)
│   └── LinkedIn/GitHub
│
├── Professional Summary
│   └── 2-3 lines highlighting key qualifications
│
├── Education
│   └── Degree, University, Year, GPA (if applicable)
│
├── Professional Experience
│   └── Reverse chronological order
│       ├── Job Title, Company, Location, Dates
│       ├── Bullet points with action verbs
│       └── Quantifiable achievements
│
├── Technical Skills
│   ├── Programming: Python, SQL
│   ├── Big Data: Apache Spark, Spark SQL
│   ├── Orchestration: Apache Airflow
│   ├── Databases: Oracle, MySQL, PostgreSQL
│   └── Tools: Git, Docker, Linux
│
├── Projects
│   └── Data Engineering projects with tech stack
│
└── Certifications (if any)
```

### 2. SYSTEM DESIGN DOCUMENTATION
```
├── Architecture Overview
│   ├── High-level system diagram
│   ├── Data flow description
│   └── Component interactions
│
├── Technology Stack
│   ├── Data Processing: Apache Spark
│   ├── Orchestration: Apache Airflow
│   ├── Databases: Oracle, PostgreSQL
│   └── Languages: Python, SQL
│
├── Database Design
│   ├── ER Diagrams
│   ├── Data Models (Star/Snowflake)
│   └── Schema definitions
│
└── Data Pipeline Design
    ├── Source systems
    ├── ETL/ELT processes
    ├── Data warehouse
    └── Analytics layer
```

### 3. TECHNICAL IMPLEMENTATION
```
├── ETL Pipeline (etl_pipeline/)
│   ├── spark_jobs/
│   │   ├── data_extraction.py
│   │   ├── data_transformation.py
│   │   └── data_loading.py
│   │
│   ├── sql_scripts/
│   │   ├── schema_creation.sql
│   │   ├── data_warehouse.sql
│   │   └── analytics_queries.sql
│   │
│   ├── airflow_dags/
│   │   └── etl_pipeline_dag.py
│   │
│   ├── data_models/
│   │   ├── star_schema.sql
│   │   └── snowflake_schema.sql
│   │
│   └── oracle_plsql/
│       ├── procedures.sql
│       └── functions.sql
│
├── Documentation
│   ├── README.md
│   ├── setup_guide.md
│   └── architecture.md
│
└── Configuration
    ├── requirements.txt
    ├── docker-compose.yml
    └── .env.example
```

## Kỹ thuật Sử Dụng:

### Apache Spark
- Data processing at scale
- Spark SQL for querying
- DataFrame transformations
- Performance optimization

### SQL & Database Design
- Complex queries (JOINs, subqueries, CTEs)
- Database normalization
- Indexing strategies
- Query optimization

### Apache Airflow
- DAG orchestration
- Task dependencies
- Scheduling
- Monitoring

### Oracle Database
- PL/SQL procedures
- Functions and triggers
- Performance tuning
- Data modeling

## Output Files Structure:
```
d:/CV4/
├── CV_Harvard_DataEngineer.docx
├── system_architecture_diagram.md
├── architecture_diagram.png
├── technical_documentation/
│   ├── system_design.md
│   ├── database_design.md
│   └── data_flow.md
└── etl_pipeline_implementation/
    ├── spark_jobs/
    ├── sql_scripts/
    ├── airflow_dags/
    ├── data_models/
    ├── oracle_plsql/
    ├── README.md
    ├── requirements.txt
    └── docker-compose.yml
```

## Mục Tiêu:
✅ CV Harvard format chuyên nghiệp
✅ Sơ đồ hệ thống rõ ràng
✅ Code mẫu cho Spark + SQL + Database
✅ Proof of skills cho từng yêu cầu công việc
✅ Portfolio hoàn chỉnh để phỏng vấn