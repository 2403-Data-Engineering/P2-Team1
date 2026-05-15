import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower,coalesce,array, regexp_replace, initcap, to_date,try_to_date
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

#Read files into df
movie_schema = """adult BOOLEAN,belongs_to_collection STRING,budget DECIMAL(15,2),genres STRING,
homepage STRING,id INT,imdb_id STRING,original_language STRING,original_title STRING,overview STRING,
popularity FLOAT,poster_path STRING,production_companies STRING,production_countries STRING,
release_date STRING,revenue DECIMAL(15,2),runtime INT,spoken_languages STRING,status STRING,
tagline STRING,title STRING,video BOOLEAN,vote_average FLOAT,vote_count INT"""
movies_df = spark.read.csv(str(BRONZE / "movies_metadata.csv"), header=True,schema=movie_schema)

credits_schema = "id INT, cast STRING, crew STRING"
credits_df = spark.read.csv(str(BRONZE / "credits.csv"), header=True,schema=credits_schema)

keywords_schema = "id INT,keywords STRING"
keywords_df = spark.read.csv(str(BRONZE / "keywords.csv"), header=True,schema=keywords_schema)

ratings_schema = "userId INT, movieId INT,rating DECIMAL(4,1),timestamp STRING"
ratings_df = spark.read.csv(str(BRONZE / "ratings_small.csv"), header=True,schema=ratings_schema)



def clean_null_movies(df: DataFrame) -> DataFrame:
    return df.dropna(subset=["id", "imdb_id", "overview","release_date","adult"]) \
        .dropna(subset=["title", "original_title"], how="all")

def clean_null_credits(df: DataFrame) -> DataFrame:
    return df.dropna(subset=["id"]) \
            .dropna(subset=["cast","crew"], how="all")

def clean_null_keywords(df: DataFrame) -> DataFrame:
    return df.dropna(subset=["id","keywords"])

def clean_null_ratings(df: DataFrame) -> DataFrame:
    return df.dropna(subset=["userId","movieId","rating"])

def clean_date(df: DataFrame) -> DataFrame:
    return df.withColumn("release_date",
    coalesce(
        try_to_date(col("release_date"), "yyyy-MM-dd"),
        try_to_date(col("release_date"), "MM/dd/yyyy"),
        try_to_date(col("release_date"), "dd-MM-yyyy"),
        try_to_date(col("release_date"), "yyyy.MM.dd"),
        try_to_date(col("release_date"), "MMMM dd, yyyy")
        )
    ).filter(col("release_date").isNotNull())

#Dropped all rows missing essential columns, weaviate can handle other nulls
if __name__ == "__main__":
    movies_df1 = clean_null_movies(movies_df)
    credits_df1 = clean_null_credits(credits_df)
    keywords_df1 = clean_null_keywords(keywords_df)
    ratings_df1 = clean_null_ratings(ratings_df)

    movies_df4 = clean_date(movies_df1)
    
    
