import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import * 
from pyspark.sql.types import StringType
from pathlib import Path

# Spark needs to know which Python to use on Windows
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder \
    .appName("local-dev") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

CUR = Path(__file__).parent

ROOT = CUR.parent

# Paths to your folders
BRONZE = ROOT / "bronze"
SILVER = CUR / "silver"

# Cleaning functions (Bronze → Silver)
def clean_near_dup_rows(movies_df: DataFrame) -> DataFrame:
    
    # Get the string columns
    string_cols = [column.name for column in movies_df.schema if column.dataType == StringType()]

    # Trim each string column
    for c in string_cols:

        # Trim whitespace and remove trailing punctuation
        movies_df = movies_df.withColumn(c, regexp_replace(col(c), r"^\W+|\W+$", ""))
       
        # Fix casing
        movies_df = movies_df.withColumn(c, initcap(col(c)))
        
    
    return movies_df

#Read files into df
movies_df = spark.read.csv(str(BRONZE / "movies_metadata.csv"), header=True,inferSchema=True)
movies_df.show()

credits_df = spark.read.csv(str(BRONZE / "credits.csv"), header=True,inferSchema=True)
credits_df.show()

keywords_df = spark.read.csv(str(BRONZE / "keywords.csv"), header=True,inferSchema=True)
keywords_df.show()

ratings_df = spark.read.csv(str(BRONZE / "ratings_small.csv"), header=True,inferSchema=True)
ratings_df.show()


no_near_dup_movies_df = clean_near_dup_rows(movies_df)
no_near_dup_movies_df.show()


