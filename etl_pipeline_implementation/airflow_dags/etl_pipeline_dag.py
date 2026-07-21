"""
Apache Airflow DAG: Daily ETL Pipeline
Orchestrate ETL jobs với Spark và Oracle Database
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.oracle.operators.oracle import OracleOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
import logging

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['data-engineer@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

# DAG definition
dag = DAG(
    'daily_etl_pipeline',
    default_args=default_args,
    description='Daily ETL pipeline for data warehouse',
    schedule_interval='0 2 * * *',  # Run at 2 AM every day
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'spark', 'oracle', 'data-warehouse'],
)

# ============================================
# TASKS
# ============================================

# Task 1: Start pipeline
start_pipeline = DummyOperator(
    task_id='start_pipeline',
    dag=dag,
)

# Task 2: Extract data from Oracle
extract_oracle_data = SparkSubmitOperator(
    task_id='extract_oracle_data',
    application='/opt/airflow/dags/spark_jobs/data_extraction.py',
    name='extract_oracle_data',
    application_args=[
        '--source', 'oracle',
        '--table', 'SALES_TRANSACTIONS',
        '--partition_column', 'date_key',
        '--partition_value', '{{ ds_nodash }}',
        '--output_path', 's3://data-lake/raw/oracle/sales/{{ ds }}/'
    ],
    conn_id='spark_default',
    executor_memory='4g',
    driver_memory='4g',
    executor_cores=2,
    num_executors=4,
    verbose=True,
    dag=dag,
)

# Task 3: Extract data from PostgreSQL
extract_postgres_data = SparkSubmitOperator(
    task_id='extract_postgres_data',
    application='/opt/airflow/dags/spark_jobs/data_extraction.py',
    name='extract_postgres_data',
    application_args=[
        '--source', 'postgres',
        '--table', 'customers',
        '--schema', 'public',
        '--output_path', 's3://data-lake/raw/postgres/customers/{{ ds }}/'
    ],
    conn_id='spark_default',
    executor_memory='2g',
    driver_memory='2g',
    executor_cores=1,
    num_executors=2,
    verbose=True,
    dag=dag,
)

# Task 4: Transform data
transform_data = SparkSubmitOperator(
    task_id='transform_data',
    application='/opt/airflow/dags/spark_jobs/data_transformation.py',
    name='transform_data',
    application_args=[
        '--input_path', 's3://data-lake/raw/{{ ds }}/',
        '--output_path', 's3://data-lake/curated/{{ ds }}/',
        '--transform_type', 'sales_customer_product'
    ],
    conn_id='spark_default',
    executor_memory='4g',
    driver_memory='4g',
    executor_cores=2,
    num_executors=4,
    verbose=True,
    dag=dag,
)

# Task 5: Load to data warehouse
load_to_data_warehouse = SparkSubmitOperator(
    task_id='load_to_data_warehouse',
    application='/opt/airflow/dags/spark_jobs/data_loading.py',
    name='load_to_data_warehouse',
    application_args=[
        '--input_path', 's3://data-lake/curated/{{ ds }}/',
        '--target', 'oracle',
        '--tables', 'FACT_SALES,DIM_CUSTOMER,DIM_PRODUCT',
        '--load_mode', 'append'
    ],
    conn_id='spark_default',
    executor_memory='4g',
    driver_memory='4g',
    executor_cores=2,
    num_executors=4,
    verbose=True,
    dag=dag,
)

# Task 6: Data quality checks
data_quality_checks = OracleOperator(
    task_id='data_quality_checks',
    oracle_conn_id='oracle_default',
    sql="""
        -- Check for null values in critical columns
        SELECT 
            'FACT_SALES' as table_name,
            COUNT(*) as total_records,
            SUM(CASE WHEN date_key IS NULL THEN 1 ELSE 0 END) as null_date_key,
            SUM(CASE WHEN product_key IS NULL THEN 1 ELSE 0 END) as null_product_key,
            SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) as null_customer_key,
            SUM(CASE WHEN net_amount IS NULL THEN 1 ELSE 0 END) as null_net_amount
        FROM FACT_SALES
        WHERE date_key = TO_NUMBER('{{ ds_nodash }}')
        
        UNION ALL
        
        SELECT 
            'DIM_CUSTOMER' as table_name,
            COUNT(*) as total_records,
            SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customer_id,
            SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END) as null_customer_name,
            0 as null_product_key,
            0 as null_net_amount
        FROM DIM_CUSTOMER
        WHERE is_current = 'Y'
    """,
    dag=dag,
)

# Task 7: Refresh materialized views
refresh_materialized_views = OracleOperator(
    task_id='refresh_materialized_views',
    oracle_conn_id='oracle_default',
    sql="""
        BEGIN
            -- Refresh sales monthly summary
            DBMS_MVIEW.REFRESH('MV_SALES_MONTHLY_SUMMARY', 'C');
            
            -- Refresh customer analytics
            DBMS_MVIEW.REFRESH('MV_CUSTOMER_ANALYTICS', 'C');
            
            COMMIT;
        END;
    """,
    dag=dag,
)

# Task 8: Log pipeline run
log_pipeline_run = OracleOperator(
    task_id='log_pipeline_run',
    oracle_conn_id='oracle_default',
    sql="""
        INSERT INTO METADATA_PIPELINE_RUN (
            pipeline_name,
            run_date,
            start_time,
            end_time,
            status,
            records_processed,
            records_failed,
            error_message
        ) VALUES (
            'daily_etl_pipeline',
            TO_DATE('{{ ds }}', 'YYYY-MM-DD'),
            TO_TIMESTAMP('{{ execution_date }}', 'YYYY-MM-DD HH24:MI:SS'),
            CURRENT_TIMESTAMP,
            'SUCCESS',
            {{ ti.xcom_pull(task_ids='load_to_data_warehouse', key='records_loaded') }},
            0,
            NULL
        )
    """,
    dag=dag,
)

# Task 9: Send notification (Python function)
def send_success_notification(**context):
    """Send notification khi pipeline thành công"""
    execution_date = context['execution_date']
    logger.info(f"Pipeline completed successfully for {execution_date}")
    
    # Send email/Slack notification
    # Implementation depends on your notification system
    return True

send_notification = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    provide_context=True,
    dag=dag,
)

# Task 10: End pipeline
end_pipeline = DummyOperator(
    task_id='end_pipeline',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

# Task 11: Handle failure
handle_failure = PythonOperator(
    task_id='handle_failure',
    python_callable=lambda: logger.error("Pipeline failed!"),
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag,
)

# ============================================
# TASK DEPENDENCIES
# ============================================

# Start -> Extract tasks (parallel)
start_pipeline >> [extract_oracle_data, extract_postgres_data]

# Extract tasks -> Transform
[extract_oracle_data, extract_postgres_data] >> transform_data

# Transform -> Load
transform_data >> load_to_data_warehouse

# Load -> Data quality checks
load_to_data_warehouse >> data_quality_checks

# Data quality checks -> Refresh MVs
data_quality_checks >> refresh_materialized_views

# Refresh MVs -> Log pipeline run
refresh_materialized_views >> log_pipeline_run

# Log pipeline run -> Send notification
log_pipeline_run >> send_notification

# Send notification -> End pipeline
send_notification >> end_pipeline

# All tasks -> Handle failure (if any fails)
[start_pipeline, extract_oracle_data, extract_postgres_data, 
 transform_data, load_to_data_warehouse, data_quality_checks,
 refresh_materialized_views, log_pipeline_run, send_notification] >> handle_failure

# ============================================
# DAG VARIABLES (Set these in Airflow UI)
# ============================================

"""
Variables to set in Airflow:
- spark_home: /opt/spark
- spark_master: yarn
- oracle_jdbc_conn_id: oracle_default
- postgres_conn_id: postgres_default
- s3_bucket: data-lake
- data_lake_path: s3://data-lake
- data_warehouse_schema: DATA_WAREHOUSE
"""