import pandas as pd
import os

print("Testing Financial Data Pipeline Setup")
print("="*50)

# Test 1: Check if augmented data exists
if os.path.exists("Finance_data_augmented.csv"):
    print("✓ Finance_data_augmented.csv found")
    df = pd.read_csv("Finance_data_augmented.csv")
    print(f"  - Records: {len(df)}")
    print(f"  - Columns: {len(df.columns)}")
else:
    print("✗ Finance_data_augmented.csv not found")
    print("  Please ensure the augmented dataset is in this directory")

# Test 2: Check PySpark availability
try:
    from pyspark.sql import SparkSession
    print("✓ PySpark is available")
except ImportError:
    print("✗ PySpark not available")
    print("  You can still use the pandas fallback processing")

# Test 3: Check matplotlib for visualization
try:
    import matplotlib.pyplot as plt
    print("✓ Matplotlib available for visualization")
except ImportError:
    print("✗ Matplotlib not available")

# Test 4: Basic data processing test
try:
    df = pd.read_csv("Finance_data_augmented.csv")
    basic_agg = df.groupby('Investment_Avenues').size()
    print("✓ Basic pandas processing works")
    print(f"  - Investment Avenues distribution:")
    for avenue, count in basic_agg.items():
        print(f"    {avenue}: {count} records")
except Exception as e:
    print(f"✗ Basic processing failed: {e}")

print("\nSetup test completed!")
