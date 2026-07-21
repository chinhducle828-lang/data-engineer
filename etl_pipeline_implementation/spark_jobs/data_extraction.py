"""
Spark Job: Data Extraction
Extract data từ các nguồn khác nhau (Oracle, PostgreSQL, Files)
"""

from pyspark.sql import SparkSession  # type: ignore
from pyspark.sql.types import *  # type: ignore
from pyspark.sql.functions import lit, max  # type: ignore
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame  # type: ignore

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataExtractor:
    """Class để extract data từ various sources"""
    
    def __init__(self, spark: "SparkSession"):  # type: ignore
        self.spark = spark
        self.extraction_time = datetime.now()
    
    def extract_from_oracle(
        self,
        table_name: str,
        partition_column: Optional[str] = None,
        partition_value: Optional[str] = None,
        query: Optional[str] = None
    ) -> "DataFrame":
        """
        Extract data từ Oracle Database
        
        Args:
            table_name: Tên bảng cần extract
            partition_column: Cột dùng để partition (ví dụ: date_key)
            partition_value: Giá trị partition (ví dụ: '20240101')
            query: Custom query nếu cần
        
        Returns:
            DataFrame chứa dữ liệu đã extract
        """
        try:
            logger.info(f"Extracting data from Oracle table: {table_name}")
            
            # Oracle JDBC connection properties
            jdbc_url = "jdbc:oracle:thin:@//oracle-host:1521/ORCLPDB1"
            connection_properties = {
                "user": "data_warehouse",
                "password": "password",
                "driver": "oracle.jdbc.driver.OracleDriver",
                "fetchsize": "10000"
            }
            
            # Build query
            if query:
                sql_query = query
            elif partition_column and partition_value:
                sql_query = f"SELECT * FROM {table_name} WHERE {partition_column} = {partition_value}"
            else:
                sql_query = f"SELECT * FROM {table_name}"
            
            # Read from Oracle
            df = self.spark.read.jdbc(  # type: ignore
                url=jdbc_url,
                table=f"({sql_query}) as oracle_query",
                properties=connection_properties
            )
            
            # Add metadata columns
            df = df.withColumn("source_system", lit("ORACLE")) \
                   .withColumn("extraction_time", lit(self.extraction_time)) \
                   .withColumn("source_table", lit(table_name))  # type: ignore
            
            logger.info(f"Successfully extracted {df.count()} rows from {table_name}")  # type: ignore
            return df  # type: ignore
            
        except Exception as e:
            logger.error(f"Error extracting from Oracle: {str(e)}")
            raise
    
    def extract_from_postgres(
        self,
        table_name: str,
        schema: str = "public",
        query: Optional[str] = None
    ) -> "DataFrame":
        """
        Extract data từ PostgreSQL
        
        Args:
            table_name: Tên bảng cần extract
            schema: Schema name (default: public)
            query: Custom query nếu cần
        
        Returns:
            DataFrame chứa dữ liệu đã extract
        """
        try:
            logger.info(f"Extracting data from PostgreSQL table: {schema}.{table_name}")
            
            # PostgreSQL JDBC connection properties
            jdbc_url = "jdbc:postgresql://postgres-host:5432/data_warehouse"
            connection_properties = {
                "user": "postgres",
                "password": "password",
                "driver": "org.postgresql.Driver",
                "fetchsize": "10000"
            }
            
            # Build query
            if query:
                sql_query = query
            else:
                sql_query = f"SELECT * FROM {schema}.{table_name}"
            
            # Read from PostgreSQL
            df = self.spark.read.jdbc(  # type: ignore
                url=jdbc_url,
                table=f"({sql_query}) as postgres_query",
                properties=connection_properties
            )
            
            # Add metadata columns
            df = df.withColumn("source_system", lit("POSTGRES")) \
                   .withColumn("extraction_time", lit(self.extraction_time)) \
                   .withColumn("source_table", lit(f"{schema}.{table_name}"))  # type: ignore
            
            logger.info(f"Successfully extracted {df.count()} rows from {schema}.{table_name}")  # type: ignore
            return df  # type: ignore
            
        except Exception as e:
            logger.error(f"Error extracting from PostgreSQL: {str(e)}")
            raise
    
    def extract_from_csv(
        self,
        file_path: str,
        delimiter: str = ",",
        header: bool = True,
        infer_schema: bool = True
    ) -> "DataFrame":
        """
        Extract data từ CSV file
        
        Args:
            file_path: Path to CSV file (có thể là S3, HDFS, hoặc local)
            delimiter: Delimiter character
            header: Có header row không
            infer_schema: Tự động infer schema không
        
        Returns:
            DataFrame chứa dữ liệu đã extract
        """
        try:
            logger.info(f"Extracting data from CSV: {file_path}")
            
            df = self.spark.read \
                .option("delimiter", delimiter) \
                .option("header", str(header).lower()) \
                .option("inferSchema", str(infer_schema).lower()) \
                .csv(file_path)  # type: ignore
            
            # Add metadata columns
            df = df.withColumn("source_system", lit("CSV")) \
                   .withColumn("extraction_time", lit(self.extraction_time)) \
                   .withColumn("source_file", lit(file_path))  # type: ignore
            
            logger.info(f"Successfully extracted {df.count()} rows from {file_path}")  # type: ignore
            return df  # type: ignore
            
        except Exception as e:
            logger.error(f"Error extracting from CSV: {str(e)}")
            raise
    
    def extract_from_json(
        self,
        file_path: str,
        multiline: bool = False
    ) -> "DataFrame":
        """
        Extract data từ JSON file
        
        Args:
            file_path: Path to JSON file
            multiline: JSON có multi-line không
        
        Returns:
            DataFrame chứa dữ liệu đã extract
        """
        try:
            logger.info(f"Extracting data from JSON: {file_path}")
            
            df = self.spark.read \
                .option("multiline", str(multiline).lower()) \
                .json(file_path)  # type: ignore
            
            # Add metadata columns
            df = df.withColumn("source_system", lit("JSON")) \
                   .withColumn("extraction_time", lit(self.extraction_time)) \
                   .withColumn("source_file", lit(file_path))  # type: ignore
            
            logger.info(f"Successfully extracted {df.count()} rows from {file_path}")  # type: ignore
            return df  # type: ignore
            
        except Exception as e:
            logger.error(f"Error extracting from JSON: {str(e)}")
            raise
    
    def extract_from_parquet(
        self,
        file_path: str
    ) -> "DataFrame":
        """
        Extract data từ Parquet file
        
        Args:
            file_path: Path to Parquet file
        
        Returns:
            DataFrame chứa dữ liệu đã extract
        """
        try:
            logger.info(f"Extracting data from Parquet: {file_path}")
            
            df = self.spark.read.parquet(file_path)  # type: ignore
            
            # Add metadata columns if not exists
            if "source_system" not in df.columns:  # type: ignore
                df = df.withColumn("source_system", lit("PARQUET"))  # type: ignore
            if "extraction_time" not in df.columns:  # type: ignore
                df = df.withColumn("extraction_time", lit(self.extraction_time))  # type: ignore
            if "source_file" not in df.columns:  # type: ignore
                df = df.withColumn("source_file", lit(file_path))  # type: ignore
            
            logger.info(f"Successfully extracted {df.count()} rows from {file_path}")  # type: ignore
            return df  # type: ignore
            
        except Exception as e:
            logger.error(f"Error extracting from Parquet: {str(e)}")
            raise
    
    def extract_incremental(
        self,
        source_type: str,
        table_name: str,
        watermark_column: str,
        last_watermark: str,
        **kwargs: Any
    ) -> Tuple["DataFrame", str]:
        """
        Extract incremental data (CDC - Change Data Capture)
        
        Args:
            source_type: Loại source (oracle, postgres, etc.)
            table_name: Tên bảng
            watermark_column: Cột dùng để track changes (ví dụ: updated_at)
            last_watermark: Giá trị watermark cuối cùng
            **kwargs: Additional arguments cho specific source
        
        Returns:
            DataFrame chứa dữ liệu incremental
        """
        try:
            logger.info(f"Extracting incremental data from {source_type}.{table_name}")
            
            query = f"""
                SELECT * FROM {table_name} 
                WHERE {watermark_column} > '{last_watermark}'
                ORDER BY {watermark_column}
            """
            
            if source_type == "oracle":
                df = self.extract_from_oracle(table_name, query=query)
            elif source_type == "postgres":
                df = self.extract_from_postgres(table_name, query=query)
            else:
                raise ValueError(f"Unsupported source type: {source_type}")
            
            # Get new watermark
            new_watermark = df.agg(max(watermark_column)).collect()[0][0]  # type: ignore
            logger.info(f"New watermark: {new_watermark}")
            
            return df, str(new_watermark)  # type: ignore
            
        except Exception as e:
            logger.error(f"Error extracting incremental data: {str(e)}")
            raise


def main():
    """Main function để test extraction"""
    # Initialize Spark Session
    spark: "SparkSession" = SparkSession.builder \
        .appName("DataExtraction") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "2") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()  # type: ignore
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")  # type: ignore
    
    try:
        # Initialize extractor
        extractor = DataExtractor(spark)  # type: ignore
        
        # Example 1: Extract from Oracle
        logger.info("=== Example 1: Extract from Oracle ===")
        oracle_df = extractor.extract_from_oracle(  # type: ignore
            table_name="SALES_TRANSACTIONS",
            partition_column="date_key",
            partition_value="20240115"
        )
        oracle_df.show(5)  # type: ignore
        oracle_df.printSchema()  # type: ignore
        
        # Example 2: Extract from PostgreSQL
        logger.info("=== Example 2: Extract from PostgreSQL ===")
        postgres_df = extractor.extract_from_postgres(  # type: ignore
            table_name="customers",
            schema="public"
        )
        postgres_df.show(5)  # type: ignore
        
        # Example 3: Extract from CSV
        logger.info("=== Example 3: Extract from CSV ===")
        csv_df = extractor.extract_from_csv(  # type: ignore
            file_path="s3://data-lake/raw/sales_2024.csv"
        )
        csv_df.show(5)  # type: ignore
        
        # Example 4: Extract from JSON
        logger.info("=== Example 4: Extract from JSON ===")
        json_df = extractor.extract_from_json(  # type: ignore
            file_path="s3://data-lake/raw/customers.json",
            multiline=True
        )
        json_df.show(5)  # type: ignore
        
        # Example 5: Extract from Parquet
        logger.info("=== Example 5: Extract from Parquet ===")
        parquet_df = extractor.extract_from_parquet(  # type: ignore
            file_path="s3://data-lake/raw/products.parquet"
        )
        parquet_df.show(5)  # type: ignore
        
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
    finally:
        spark.stop()  # type: ignore
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()