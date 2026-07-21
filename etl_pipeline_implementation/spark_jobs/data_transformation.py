"""
Spark Job: Data Transformation
Transform, clean, và enrich data
"""

from pyspark.sql import SparkSession  # type: ignore
from pyspark.sql.functions import *  # type: ignore
from pyspark.sql.types import *  # type: ignore
from typing import Dict, List, Optional, Any
from typing import TYPE_CHECKING
import logging
from datetime import datetime

if TYPE_CHECKING:
    from pyspark.sql import DataFrame  # type: ignore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataTransformer:
    """Class để transform và clean data"""
    
    def __init__(self, spark: "SparkSession"):  # type: ignore
        self.spark = spark
    
    def clean_data(self, df: "DataFrame", rules: Dict[str, Any]) -> "DataFrame":  # type: ignore
        """
        Clean data theo các rules đã định
        
        Args:
            df: Input DataFrame
            rules: Dictionary chứa cleaning rules
                Example:
                {
                    "remove_nulls": ["customer_id", "product_id"],
                    "fill_nulls": {"discount": 0, "quantity": 1},
                    "remove_duplicates": ["transaction_id"],
                    "trim_columns": ["customer_name", "product_name"]
                }
        
        Returns:
            Cleaned DataFrame
        """
        try:
            logger.info("Starting data cleaning...")
            cleaned_df = df
            
            # Remove rows with nulls in critical columns
            if "remove_nulls" in rules:
                cleaned_df = cleaned_df.dropna(subset=rules["remove_nulls"])
                logger.info(f"Removed nulls from columns: {rules['remove_nulls']}")
            
            # Fill nulls with default values
            if "fill_nulls" in rules:
                for col, value in rules["fill_nulls"].items():
                    cleaned_df = cleaned_df.na.fill(value, subset=[col])
                logger.info(f"Filled nulls with defaults: {rules['fill_nulls']}")
            
            # Remove duplicates
            if "remove_duplicates" in rules:
                cleaned_df = cleaned_df.dropDuplicates(rules["remove_duplicates"])
                logger.info(f"Removed duplicates based on: {rules['remove_duplicates']}")
            
            # Trim string columns
            if "trim_columns" in rules:
                for col in rules["trim_columns"]:
                    cleaned_df = cleaned_df.withColumn(col, trim(col))
                logger.info(f"Trimmed columns: {rules['trim_columns']}")
            
            logger.info(f"Data cleaning completed. Rows before: {df.count()}, Rows after: {cleaned_df.count()}")
            return cleaned_df
            
        except Exception as e:
            logger.error(f"Error cleaning data: {str(e)}")
            raise
    
    def validate_data(self, df: "DataFrame", validations: Dict[str, Any]) -> Dict[str, List[str]]:  # type: ignore
        """
        Validate data quality
        
        Args:
            df: Input DataFrame
            validations: Dictionary chứa validation rules
                Example:
                {
                    "email_format": {"column": "email", "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"},
                    "positive_values": ["quantity", "price", "amount"],
                    "date_range": {"column": "order_date", "min": "2020-01-01", "max": "2025-12-31"}
                }
        
        Returns:
            Dictionary chứa validation results
        """
        try:
            logger.info("Starting data validation...")
            results: Dict[str, List[str]] = {"passed": [], "failed": [], "warnings": []}
            
            # Validate email format
            if "email_format" in validations:
                col = validations["email_format"]["column"]
                pattern = validations["email_format"]["pattern"]
                invalid_count = df.filter(~col(col).rlike(pattern)).count()
                if invalid_count > 0:
                    results["failed"].append(f"Invalid emails found: {invalid_count}")
                else:
                    results["passed"].append("Email format validation passed")
            
            # Validate positive values
            if "positive_values" in validations:
                for col_name in validations["positive_values"]:
                    if col_name in df.columns:
                        negative_count = df.filter(col(col_name) < 0).count()
                        if negative_count > 0:
                            results["failed"].append(f"Negative values in {col_name}: {negative_count}")
                        else:
                            results["passed"].append(f"Positive values validation passed for {col_name}")
            
            # Validate date range
            if "date_range" in validations:
                col = validations["date_range"]["column"]
                min_date = validations["date_range"]["min"]
                max_date = validations["date_range"]["max"]
                
                out_of_range = df.filter(
                    (col(col) < min_date) | (col(col) > max_date)
                ).count()
                
                if out_of_range > 0:
                    results["warnings"].append(f"Dates out of range in {col}: {out_of_range}")
                else:
                    results["passed"].append(f"Date range validation passed for {col}")
            
            logger.info(f"Validation completed. Passed: {len(results['passed'])}, Failed: {len(results['failed'])}")
            return results
            
        except Exception as e:
            logger.error(f"Error validating data: {str(e)}")
            raise
    
    def transform_sales_data(self, df: "DataFrame") -> "DataFrame":  # type: ignore
        """
        Transform sales data với business logic
        
        Args:
            df: Input DataFrame chứa sales data
        
        Returns:
            Transformed DataFrame
        """
        try:
            logger.info("Transforming sales data...")
            
            transformed_df = df \
                .withColumn("gross_amount", col("quantity") * col("unit_price")) \
                .withColumn("discount_amount", col("gross_amount") * col("discount_rate") / 100) \
                .withColumn("net_amount", col("gross_amount") - col("discount_amount")) \
                .withColumn("cost_amount", col("quantity") * col("cost_price")) \
                .withColumn("profit_amount", col("net_amount") - col("cost_amount")) \
                .withColumn("profit_margin", round((col("profit_amount") / col("net_amount")) * 100, 2)) \
                .withColumn("processing_date", current_date()) \
                .withColumn("processing_timestamp", current_timestamp())  # type: ignore
            
            logger.info("Sales data transformation completed")
            return transformed_df
            
        except Exception as e:
            logger.error(f"Error transforming sales data: {str(e)}")
            raise
    
    def transform_customer_data(self, df: "DataFrame") -> "DataFrame":  # type: ignore
        """
        Transform customer data với business logic
        
        Args:
            df: Input DataFrame chứa customer data
        
        Returns:
            Transformed DataFrame
        """
        try:
            logger.info("Transforming customer data...")
            
            # Calculate age from birth_date
            transformed_df = df \
                .withColumn("age", year(current_date()) - year(col("birth_date"))) \
                .withColumn("age_group",  # type: ignore
                    when(col("age") < 25, "18-24") \
                    .when(col("age") < 35, "25-34") \
                    .when(col("age") < 45, "35-44") \
                    .when(col("age") < 55, "45-54") \
                    .otherwise("55+")
                ) \
                .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name"))) \
                .withColumn("email_domain", split(col("email"), "@").getItem(1)) \
                .withColumn("phone_normalized", regexp_replace(col("phone"), "[^0-9]", "")) \
                .withColumn("address_normalized", lower(trim(col("address")))) \
                .withColumn("processing_date", current_date())  # type: ignore
            
            logger.info("Customer data transformation completed")
            return transformed_df
            
        except Exception as e:
            logger.error(f"Error transforming customer data: {str(e)}")
            raise
    
    def transform_product_data(self, df: "DataFrame") -> "DataFrame":  # type: ignore
        """
        Transform product data với business logic
        
        Args:
            df: Input DataFrame chứa product data
        
        Returns:
            Transformed DataFrame
        """
        try:
            logger.info("Transforming product data...")
            
            transformed_df = df \
                .withColumn("product_name_upper", upper(col("product_name"))) \
                .withColumn("price_tier",  # type: ignore
                    when(col("unit_price") < 100, "Low") \
                    .when(col("unit_price") < 500, "Medium") \
                    .when(col("unit_price") < 1000, "High") \
                    .otherwise("Premium")
                ) \
                .withColumn("profit_margin",  # type: ignore
                    round(((col("unit_price") - col("cost_price")) / col("unit_price")) * 100, 2)
                ) \
                .withColumn("markup_percentage",  # type: ignore
                    round(((col("unit_price") - col("cost_price")) / col("cost_price")) * 100, 2)
                ) \
                .withColumn("processing_date", current_date())  # type: ignore
            
            logger.info("Product data transformation completed")
            return transformed_df
            
        except Exception as e:
            logger.error(f"Error transforming product data: {str(e)}")
            raise
    
    def integrate_data(
        self,
        sales_df: "DataFrame",  # type: ignore
        customer_df: "DataFrame",  # type: ignore
        product_df: "DataFrame",  # type: ignore
        store_df: "DataFrame"  # type: ignore
    ) -> "DataFrame":
        """
        Integrate multiple data sources vào một unified dataset
        
        Args:
            sales_df: Sales DataFrame
            customer_df: Customer DataFrame
            product_df: Product DataFrame
            store_df: Store DataFrame
        
        Returns:
            Integrated DataFrame
        """
        try:
            logger.info("Integrating data from multiple sources...")
            
            # Join sales with customer
            integrated_df = sales_df \
                .join(customer_df, "customer_key", "left") \
                .join(product_df, "product_key", "left") \
                .join(store_df, "store_key", "left")  # type: ignore
            
            # Select relevant columns
            integrated_df = integrated_df.select(  # type: ignore
                col("sales_key"),  # type: ignore
                col("date_key"),  # type: ignore
                col("customer_key"),  # type: ignore
                col("product_key"),  # type: ignore
                col("store_key"),  # type: ignore
                col("quantity_sold"),  # type: ignore
                col("unit_price"),  # type: ignore
                col("gross_amount"),  # type: ignore
                col("discount_amount"),  # type: ignore
                col("net_amount"),  # type: ignore
                col("cost_amount"),  # type: ignore
                col("profit_amount"),  # type: ignore
                col("customer_name"),  # type: ignore
                col("customer_segment"),  # type: ignore
                col("customer_region"),  # type: ignore
                col("product_name"),  # type: ignore
                col("product_category"),  # type: ignore
                col("product_brand"),  # type: ignore
                col("store_name"),  # type: ignore
                col("store_region"),  # type: ignore
                col("processing_timestamp")  # type: ignore
            )
            
            logger.info(f"Data integration completed. Total rows: {integrated_df.count()}")
            return integrated_df
            
        except Exception as e:
            logger.error(f"Error integrating data: {str(e)}")
            raise
    
    def aggregate_data(self, df: "DataFrame", group_by: List[str], aggregations: Dict[str, Any]) -> "DataFrame":  # type: ignore
        """
        Aggregate data theo các dimensions và measures
        
        Args:
            df: Input DataFrame
            group_by: List of columns to group by
            aggregations: Dictionary chứa aggregation rules
                Example:
                {
                    "total_sales": "sum(net_amount)",
                    "avg_order_value": "avg(net_amount)",
                    "transaction_count": "count(*)",
                    "unique_customers": "count(distinct customer_key)"
                }
        
        Returns:
            Aggregated DataFrame
        """
        try:
            logger.info(f"Aggregating data by: {group_by}")
            
            # Build aggregation expressions
            agg_exprs = []
            for alias, expr in aggregations.items():  # type: ignore
                agg_exprs.append(expr(alias))  # type: ignore
            
            # Group by and aggregate
            aggregated_df = df.groupBy(*group_by).agg(*agg_exprs)  # type: ignore
            
            logger.info(f"Aggregation completed. Rows: {aggregated_df.count()}")
            return aggregated_df
            
        except Exception as e:
            logger.error(f"Error aggregating data: {str(e)}")
            raise
    
    def deduplicate_data(self, df: "DataFrame", key_columns: List[str], order_by: str) -> "DataFrame":  # type: ignore
        """
        Remove duplicates, giữ lại record mới nhất
        
        Args:
            df: Input DataFrame
            key_columns: Columns để identify duplicates
            order_by: Column để determine which record is latest
        
        Returns:
            Deduplicated DataFrame
        """
        try:
            logger.info(f"Deduplicating data by: {key_columns}")
            
            # Create window specification
            from pyspark.sql.window import Window  # type: ignore
            window_spec = Window.partitionBy(*key_columns).orderBy(desc(order_by))  # type: ignore
            
            # Add row number and filter
            deduplicated_df = df \
                .withColumn("row_num", row_number().over(window_spec)) \
                .filter(col("row_num") == 1) \
                .drop("row_num")  # type: ignore
            
            logger.info(f"Deduplication completed. Rows before: {df.count()}, Rows after: {deduplicated_df.count()}")
            return deduplicated_df
            
        except Exception as e:
            logger.error(f"Error deduplicating data: {str(e)}")
            raise
    
    def enrich_with_calculated_columns(self, df: "DataFrame") -> "DataFrame":  # type: ignore
        """
        Add calculated columns cho analytics
        
        Args:
            df: Input DataFrame
        
        Returns:
            Enriched DataFrame
        """
        try:
            logger.info("Enriching data with calculated columns...")
            
            enriched_df = df \
                .withColumn("year", year(col("date_key"))) \
                .withColumn("month", month(col("date_key"))) \
                .withColumn("quarter", quarter(col("date_key"))) \
                .withColumn("day_of_week", dayofweek(col("date_key"))) \
                .withColumn("is_weekend", when(col("day_of_week").isin([1, 7]), "Y").otherwise("N"))  # type: ignore
            
            logger.info("Data enrichment completed")
            return enriched_df
            
        except Exception as e:
            logger.error(f"Error enriching data: {str(e)}")
            raise


def main():
    """Main function để test transformation"""
    spark: "SparkSession" = SparkSession.builder \
        .appName("DataTransformation") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()  # type: ignore
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        transformer = DataTransformer(spark)
        
        # Example 1: Clean data
        logger.info("=== Example 1: Clean Data ===")
        sample_data = [
            (1, "Product A", 100, None, 10),
            (2, "Product B", None, 50, 20),
            (3, "Product A", 100, 50, 10),  # Duplicate
            (4, "  Product C  ", 200, 80, 5)
        ]
        df = spark.createDataFrame(sample_data, ["id", "name", "price", "cost", "quantity"])  # type: ignore
        
        cleaning_rules = {
            "remove_nulls": ["id", "name"],
            "fill_nulls": {"cost": 0, "quantity": 1},
            "remove_duplicates": ["id"],
            "trim_columns": ["name"]
        }
        
        cleaned_df = transformer.clean_data(df, cleaning_rules)  # type: ignore
        cleaned_df.show()  # type: ignore
        
        # Example 2: Validate data
        logger.info("=== Example 2: Validate Data ===")
        validations = {
            "positive_values": ["price", "quantity"],
            "email_format": {"column": "email", "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"}
        }
        validation_results = transformer.validate_data(cleaned_df, validations)  # type: ignore
        logger.info(f"Validation results: {validation_results}")
        
        # Example 3: Transform sales data
        logger.info("=== Example 3: Transform Sales Data ===")
        sales_data = [
            (1, 101, 10, 100.0, 0.1),
            (2, 102, 5, 200.0, 0.05),
            (3, 101, 20, 100.0, 0.15)
        ]
        sales_df = spark.createDataFrame(sales_data, ["sales_key", "product_key", "quantity", "unit_price", "discount_rate"])  # type: ignore
        transformed_sales = transformer.transform_sales_data(sales_df)  # type: ignore
        transformed_sales.show()  # type: ignore
        
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
    finally:
        spark.stop()  # type: ignore
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()