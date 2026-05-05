import os
import sys
from pyspark.sql import SparkSession

# Spark needs to know which Python to use on Windows
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder \
    .appName("local-dev") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Create a simple DataFrame
data = [("Alice", 30), ("Bob", 25), ("Carol", 35)]
df = spark.createDataFrame(data, ["name", "age"])
df.show()

spark.stop()