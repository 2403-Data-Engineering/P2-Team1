import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower,coalesce,array, regexp_replace, initcap
from pyspark.sql import DataFrame
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
def clean_near_dup_rows(df: DataFrame) -> DataFrame:
    
    # Get the string columns
    string_cols = [column.name for column in df.schema if column.dataType == StringType()]

    # Trim each string column
    for c in string_cols:

        # Trim whitespace and remove trailing punctuation
        df = df.withColumn(c, regexp_replace(col(c), r"^\W+|\W+$", ""))
       
        # Fix casing
        df = df.withColumn(c, initcap(col(c)))
        
    
    return df

#Read files into df
movies_df = spark.read.csv(str(BRONZE / "movies_metadata.csv"), header=True,inferSchema=True)
# movies_df.show()

credits_df = spark.read.csv(str(BRONZE / "credits.csv"), header=True,inferSchema=True)
# credits_df.show()

keywords_df = spark.read.csv(str(BRONZE / "keywords.csv"), header=True,inferSchema=True)
# keywords_df.show()

ratings_df = spark.read.csv(str(BRONZE / "ratings_small.csv"), header=True,inferSchema=True)
# ratings_df.show()


#Dropped all rows missing essential columns, weaviate can handle other nulls
movies_df1 = movies_df.dropna(subset=["id", "imdb_id", "overview","release_date","adult"]) \
        .dropna(subset=["title", "original_title"], how="all")\

credits_df1 = credits_df.dropna(subset=["id"]) \
        .dropna(subset=["cast","crew"], how="all")

keywords_df1 = keywords_df.dropna(subset=["id","keywords"])

ratings_df1 = ratings_df.dropna(subset=["userId","movieId","rating"])

#Fixed casing, removed whitespace and trailing punctuation
movies_df2 = clean_near_dup_rows(movies_df1)

credits_df2 = clean_near_dup_rows(credits_df1)

keywords_df2 = clean_near_dup_rows(keywords_df1)

ratings_df2 = clean_near_dup_rows(ratings_df1)



