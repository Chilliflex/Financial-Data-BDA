from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, count, avg, when, desc
from pyspark.sql.types import *
import pandas as pd
import os

def enhanced_etl_financial_data(finance_csv, output_path="processed_finance_data"):
    """Enhanced ETL pipeline for augmented financial dataset - FIXED VERSION"""

    # Initialize Spark session with optimized configuration for larger dataset and Windows compatibility
    spark = SparkSession.builder \
        .appName("EnhancedFinancialDataETL") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse") \
        .getOrCreate()

    # Load the augmented dataset
    print(f"Loading dataset from: {finance_csv}")
    finance_df = spark.read.option("header", "true").option("inferSchema", "true").csv(finance_csv)

    record_count = finance_df.count()
    print(f"Loaded dataset with {record_count} records")

    # Data cleaning and validation
    finance_df = finance_df.dropDuplicates()

    # Create additional derived columns for analysis
    finance_df = finance_df.withColumn("Age_Group", 
        when(col("age") <= 25, "Young")
        .when(col("age") <= 30, "Middle")
        .otherwise("Mature"))

    finance_df = finance_df.withColumn("Risk_Profile",
        when(col("Factor") == "Risk", "Risk_Seeker")
        .when(col("Factor") == "Returns", "Return_Focused")
        .otherwise("Conservative"))

    # Cache the dataframe for multiple operations
    finance_df.cache()

    # Comprehensive aggregations
    print("\n=== PERFORMING AGGREGATIONS ===")

    # 1. Investment preference aggregation by Investment_Avenues
    agg_by_avenues = finance_df.groupBy("Investment_Avenues").agg(
        sum("Mutual_Funds").alias("Total_Mutual_Funds"),
        sum("Equity_Market").alias("Total_Equity_Market"), 
        sum("Debentures").alias("Total_Debentures"),
        sum("Government_Bonds").alias("Total_Government_Bonds"),
        sum("Fixed_Deposits").alias("Total_Fixed_Deposits"),
        sum("PPF").alias("Total_PPF"),
        sum("Gold").alias("Total_Gold"),
        count("*").alias("Count"),
        avg("age").alias("Avg_Age")
    )

    # 2. Age group analysis
    age_group_analysis = finance_df.groupBy("Age_Group").agg(
        count("*").alias("Count"),
        avg("Mutual_Funds").alias("Avg_Mutual_Funds_Preference"),
        avg("Equity_Market").alias("Avg_Equity_Preference"),
        sum(when(col("Investment_Avenues") == "Yes", 1).otherwise(0)).alias("Active_Investors")
    )

    # 3. Risk profile analysis
    risk_analysis = finance_df.groupBy("Risk_Profile", "Age_Group").agg(
        count("*").alias("Count"),
        avg("Equity_Market").alias("Avg_Equity_Preference")
    )

    # 4. Gender-based investment patterns
    gender_analysis = finance_df.groupBy("gender").agg(
        count("*").alias("Count"),
        avg("age").alias("Avg_Age"),
        sum(when(col("Investment_Avenues") == "Yes", 1).otherwise(0)).alias("Active_Investors"),
        avg("Mutual_Funds").alias("Avg_MF_Preference"),
        avg("Equity_Market").alias("Avg_Equity_Preference")
    )

    # Show results
    print("\nInvestment Avenues Aggregation:")
    agg_by_avenues.show()

    print("Age Group Analysis:")
    age_group_analysis.show()

    print("Risk Profile Analysis:")
    risk_analysis.show()

    print("Gender Analysis:")
    gender_analysis.show()

    # FIXED: Save as CSV instead of Parquet to avoid Windows permission issues
    print(f"\nSaving processed data to CSV format...")

    # Create output directory if it doesn't exist
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    try:
        # Save as CSV with coalesce to avoid multiple part files
        agg_by_avenues.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}/investment_avenues_agg")
        age_group_analysis.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}/age_group_analysis")
        risk_analysis.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}/risk_analysis")
        gender_analysis.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}/gender_analysis")

        print("✓ All aggregated data saved successfully as CSV files")

    except Exception as e:
        print(f"Warning: Could not save to files due to permissions: {e}")
        print("Continuing with in-memory processing...")

    # Convert primary aggregation to pandas for visualization
    pandas_df = agg_by_avenues.toPandas()

    # Clean up Spark session
    spark.stop()
    return pandas_df

# Alternative function for direct pandas processing (fallback option)
def pandas_etl_financial_data(finance_csv):
    """Fallback ETL using pandas only if PySpark fails"""

    print("Using pandas fallback processing...")

    # Load data with pandas
    df = pd.read_csv(finance_csv)
    print(f"Loaded {len(df)} records with pandas")

    # Create age groups
    df['Age_Group'] = pd.cut(df['age'], bins=[0, 25, 30, 100], labels=['Young', 'Middle', 'Mature'])

    # Investment avenues aggregation
    agg_by_avenues = df.groupby('Investment_Avenues').agg({
        'Mutual_Funds': 'sum',
        'Equity_Market': 'sum', 
        'Debentures': 'sum',
        'Government_Bonds': 'sum',
        'Fixed_Deposits': 'sum',
        'PPF': 'sum',
        'Gold': 'sum',
        'age': ['count', 'mean']
    }).round(2)

    agg_by_avenues.columns = ['Total_Mutual_Funds', 'Total_Equity_Market', 'Total_Debentures',
                             'Total_Government_Bonds', 'Total_Fixed_Deposits', 'Total_PPF', 
                             'Total_Gold', 'Count', 'Avg_Age']

    agg_by_avenues = agg_by_avenues.reset_index()

    print("\nInvestment Avenues Aggregation (Pandas):")
    print(agg_by_avenues)

    return agg_by_avenues

# Example usage with error handling:
# try:
#     aggregated_data = enhanced_etl_financial_data("Finance_data_augmented.csv")
# except Exception as e:
#     print(f"PySpark processing failed: {e}")
#     aggregated_data = pandas_etl_financial_data("Finance_data_augmented.csv")
