"""
Spark Job: Data Loading
Load transformed data vào data warehouse (Oracle Database)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from typing import Dict, List, Optional
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataLoader:
    """Class để load data vào data warehouse"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.jdbc_url = "jdbc:oracle:thin:@//oracle-host:1521/ORCLPDB1"
        self.connection_properties = {
            "user": "data_warehouse",
            "password": "password",
            "driver": "oracle.jdbc.driver.OracleDriver",
            "batchsize": "10000"
        }
    
    def load_to_oracle(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "append",
        batch_size: int = 10000
    ) -> int:
        """
        Load DataFrame vào Oracle Database
        
        Args:
            df: DataFrame cần load
            table_name: Tên bảng đích
            mode: Chế độ ghi ('append', 'overwrite', 'ignore', 'errorifexists')
            batch_size: Số records mỗi batch
        
        Returns:
            Số records đã load thành công
        """
        try:
            logger.info(f"Loading data to Oracle table: {table_name}")
            record_count = df.count()
            logger.info(f"Total records to load: {record_count}")
            
            # Write to Oracle using JDBC
            df.write \
                .mode(mode) \
                .option("batchsize", str(batch_size)) \
                .option("truncate", "false") \
                .jdbc(
                    url=self.jdbc_url,
                    table=table_name,
                    properties=self.connection_properties
                )
            
            logger.info(f"Successfully loaded {record_count} records to {table_name}")
            return record_count
            
        except Exception as e:
            logger.error(f"Error loading data to Oracle: {str(e)}")
            raise
    
    def load_with_scd_type2(
        self,
        df: DataFrame,
        table_name: str,
        key_columns: List[str],
        effective_date_column: str = "effective_date"
    ) -> int:
        """
        Load data với SCD Type 2 (Slowly Changing Dimension)
        
        Args:
            df: DataFrame chứa dữ liệu mới
            table_name: Tên bảng dimension
            key_columns: Columns để identify unique records
            effective_date_column: Tên cột effective date
        
        Returns:
            Số records đã load
        """
        try:
            logger.info(f"Loading data with SCD Type 2 to table: {table_name}")
            
            # Read existing data from Oracle
            existing_df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table=table_name,
                properties=self.connection_properties
            )
            
            # Filter existing data to get current records
            existing_current = existing_df.filter(col("is_current") == "Y")
            
            # Join new data with existing current records
            join_condition = [df[col] == existing_current[col] for col in key_columns]
            joined_df = df.join(existing_current, on=join_condition, how="left_anti")
            
            # Prepare new records with SCD metadata
            new_records = joined_df.withColumn("effective_date", current_date()) \
                                   .withColumn("expiry_date", lit("9999-12-31")) \
                                   .withColumn("is_current", lit("Y")) \
                                   .withColumn("created_at", current_timestamp()) \
                                   .withColumn("updated_at", current_timestamp())
            
            # Update existing records (set expiry date)
            changed_records = df.join(existing_current, on=join_condition, how="inner")
            
            if changed_records.count() > 0:
                # Get records that need to be updated (expired)
                keys_to_update = changed_records.select(key_columns).distinct()
                
                # Create update DataFrame
                update_df = existing_current.join(keys_to_update, on=join_condition, how="inner")
                update_df = update_df.withColumn("expiry_date", current_date()) \
                                    .withColumn("is_current", lit("N")) \
                                    .withColumn("updated_at", current_timestamp())
                
                # Load updated records (overwrite mode for specific keys)
                logger.info(f"Updating {update_df.count()} expired records")
                self.load_to_oracle(update_df, table_name, mode="overwrite")
            
            # Load new records
            new_count = new_records.count()
            if new_count > 0:
                logger.info(f"Loading {new_count} new records")
                self.load_to_oracle(new_records, table_name, mode="append")
            
            logger.info(f"SCD Type 2 load completed for {table_name}")
            return new_count
            
        except Exception as e:
            logger.error(f"Error loading data with SCD Type 2: {str(e)}")
            raise
    
    def load_fact_table(
        self,
        df: DataFrame,
        table_name: str,
        partition_column: str = "date_key",
        partition_value: Optional[str] = None
    ) -> int:
        """
        Load data vào fact table với partitioning
        
        Args:
            df: DataFrame cần load
            table_name: Tên bảng fact
            partition_column: Cột partition
            partition_value: Giá trị partition (nếu None thì load tất cả)
        
        Returns:
            Số records đã load
        """
        try:
            logger.info(f"Loading fact table: {table_name}")
            
            # Filter by partition if specified
            if partition_value:
                df = df.filter(col(partition_column) == partition_value)
                logger.info(f"Filtered by {partition_column} = {partition_value}")
            
            # Add metadata columns
            df_with_metadata = df.withColumn("created_at", current_timestamp()) \
                                .withColumn("updated_at", current_timestamp())
            
            # Load to Oracle
            record_count = self.load_to_oracle(df_with_metadata, table_name, mode="append")
            
            logger.info(f"Fact table load completed: {record_count} records")
            return record_count
            
        except Exception as e:
            logger.error(f"Error loading fact table: {str(e)}")
            raise
    
    def load_incremental(
        self,
        df: DataFrame,
        table_name: str,
        watermark_column: str,
        last_watermark: str
    ) -> tuple[int, str]:
        """
        Load incremental data (CDC)
        
        Args:
            df: DataFrame chứa dữ liệu incremental
            table_name: Tên bảng
            watermark_column: Cột watermark (ví dụ: updated_at)
            last_watermark: Watermark cuối cùng
        
        Returns:
            Tuple (số records đã load, watermark mới)
        """
        try:
            logger.info(f"Loading incremental data to {table_name}")
            
            # Filter data after last watermark
            incremental_df = df.filter(col(watermark_column) > last_watermark)
            record_count = incremental_df.count()
            
            if record_count > 0:
                # Load incremental data
                self.load_to_oracle(incremental_df, table_name, mode="append")
                
                # Get new watermark
                new_watermark = incremental_df.agg(max(watermark_column)).collect()[0][0]
                logger.info(f"Loaded {record_count} records. New watermark: {new_watermark}")
            else:
                new_watermark = last_watermark
                logger.info("No new records to load")
            
            return record_count, new_watermark
            
        except Exception as e:
            logger.error(f"Error loading incremental data: {str(e)}")
            raise
    
    def load_with_merge(
        self,
        df: DataFrame,
        table_name: str,
        key_columns: List[str],
        update_columns: Optional[List[str]] = None
    ) -> int:
        """
        Load data với MERGE operation (UPSERT)
        
        Args:
            df: DataFrame chứa dữ liệu
            table_name: Tên bảng
            key_columns: Columns để match records
            update_columns: Columns cần update (nếu None thì update tất cả)
        
        Returns:
            Số records đã xử lý
        """
        try:
            logger.info(f"Loading data with MERGE to table: {table_name}")
            
            # Create temporary view
            temp_view = f"temp_{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            df.createOrReplaceTempView(temp_view)
            
            # Build MERGE statement
            key_condition = " AND ".join([f"target.{col} = source.{col}" for col in key_columns])
            
            if update_columns is None:
                update_columns = [col for col in df.columns if col not in key_columns]
            
            update_set = ", ".join([f"target.{col} = source.{col}" for col in update_columns])
            insert_columns = ", ".join(df.columns)
            insert_values = ", ".join([f"source.{col}" for col in df.columns])
            
            merge_sql = f"""
                MERGE INTO {table_name} target
                USING {temp_view} source
                ON ({key_condition})
                WHEN MATCHED THEN
                    UPDATE SET {update_set}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_columns})
                    VALUES ({insert_values})
            """
            
            # Execute MERGE
            record_count = df.count()
            self.spark.sql(merge_sql)
            
            # Drop temporary view
            self.spark.catalog.dropTempView(temp_view)
            
            logger.info(f"MERGE completed for {table_name}: {record_count} records processed")
            return record_count
            
        except Exception as e:
            logger.error(f"Error loading data with MERGE: {str(e)}")
            raise
    
    def validate_load(self, table_name: str, expected_count: int) -> Dict:
        """
        Validate data sau khi load
        
        Args:
            table_name: Tên bảng đã load
            expected_count: Số records mong đợi
        
        Returns:
            Dictionary chứa validation results
        """
        try:
            logger.info(f"Validating load for table: {table_name}")
            
            # Read from Oracle to verify
            df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table=table_name,
                properties=self.connection_properties
            )
            
            actual_count = df.count()
            
            results = {
                "table_name": table_name,
                "expected_count": expected_count,
                "actual_count": actual_count,
                "status": "PASSED" if actual_count == expected_count else "FAILED",
                "difference": abs(actual_count - expected_count)
            }
            
            # Additional validations
            if actual_count > 0:
                results["null_check"] = {
                    col: df.filter(df[col].isNull()).count()
                    for col in df.columns
                }
            
            logger.info(f"Validation results: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Error validating load: {str(e)}")
            raise
    
    def load_dimension_date(self, start_date: str, end_date: str) -> int:
        """
        Load DIM_DATE table với date range
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
        
        Returns:
            Số records đã load
        """
        try:
            logger.info(f"Loading DIM_DATE from {start_date} to {end_date}")
            
            # Generate date dimension data
            date_df = self.spark.sql(f"""
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
                         ELSE TO_NUMBER(TO_CHAR(dt, 'Q')) + 1 END as fiscal_quarter,
                    CURRENT_TIMESTAMP as created_at
                FROM (
                    SELECT TO_DATE('{start_date}', 'YYYY-MM-DD') + LEVEL - 1 as dt
                    FROM dual
                    CONNECT BY LEVEL <= TO_DATE('{end_date}', 'YYYY-MM-DD') - TO_DATE('{start_date}', 'YYYY-MM-DD') + 1
                )
            """)
            
            # Load to Oracle
            record_count = self.load_to_oracle(date_df, "DIM_DATE", mode="overwrite")
            
            logger.info(f"DIM_DATE loaded: {record_count} records")
            return record_count
            
        except Exception as e:
            logger.error(f"Error loading DIM_DATE: {str(e)}")
            raise


def main():
    """Main function để test loading"""
    spark = SparkSession.builder \
        .appName("DataLoading") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        loader = DataLoader(spark)
        
        # Example 1: Load dimension table
        logger.info("=== Example 1: Load Dimension Table ===")
        sample_dim = [
            (1, "CUST001", "Nguyễn Văn A", "Hà Nội", "2024-01-01", "9999-12-31", "Y"),
            (2, "CUST002", "Trần Thị B", "TP.HCM", "2024-01-01", "9999-12-31", "Y")
        ]
        dim_df = spark.createDataFrame(
            sample_dim,
            ["customer_key", "customer_id", "customer_name", "city", "effective_date", "expiry_date", "is_current"]
        )
        loader.load_to_oracle(dim_df, "DIM_CUSTOMER", mode="overwrite")
        
        # Example 2: Load fact table
        logger.info("=== Example 2: Load Fact Table ===")
        sample_fact = [
            (1, 20240101, 1, 1, 1, 10, 100.0, 0.1, 90.0, 70.0, 20.0),
            (2, 20240101, 2, 2, 1, 5, 200.0, 0.05, 190.0, 150.0, 40.0)
        ]
        fact_df = spark.createDataFrame(
            sample_fact,
            ["sales_key", "date_key", "customer_key", "product_key", "store_key", 
             "quantity", "unit_price", "discount_rate", "net_amount", "cost_amount", "profit_amount"]
        )
        loader.load_fact_table(fact_df, "FACT_SALES", partition_column="date_key", partition_value="20240101")
        
        # Example 3: Validate load
        logger.info("=== Example 3: Validate Load ===")
        validation = loader.validate_load("DIM_CUSTOMER", 2)
        logger.info(f"Validation result: {validation}")
        
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()