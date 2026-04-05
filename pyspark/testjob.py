from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == "__main__":
    # Create Spark session
    spark = SparkSession.builder.appName("test-job") \
        .master("spark://spark-master:7077") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", "file:///opt/spark/work-dir/spark-events").getOrCreate()

    # Sample data
    data = [
        ("Alice", 34),
        ("Bob", 45),
        ("Cathy", 29),
        ("David", 40),
        ("Eve", 35)
    ]

    # Create DataFrame
    df = spark.createDataFrame(data, ["name", "age"])

    # Show original data
    print("Original DataFrame:")
    df.show()

    # Filter rows where age > 30
    df_filtered = df.filter(col("age") > 30)

    print("Filtered DataFrame (age > 30):")
    df_filtered.show()

    # Count the filtered rows
    count = df_filtered.count()
    print(f"Number of people older than 30: {count}")

    spark.stop()