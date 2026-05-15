import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower,coalesce,array, regexp_replace, initcap
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from pathlib import Path
from jonathan_functions import clean_null_movies,clean_null_ratings,clean_null_keywords,clean_null_credits

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
movie_schema = """id INT,imdb_id INT,title STRING,original_title STRING,overview STRING,tagline STRING,
release_date DATE,runtime INT,budget DECIMAL(15,2),revenue DECIMAL(15,2),popularity DECIMAL(6,2),
vote_average DECIMAL(5,2),vote_count INT,genres STRING,production_companies STRING,production_countries STRING,
spoken_languages STRING,belongs_to_collection STRING,homepage STRING,poster_path STRING,backdrop_path STRING,
status STRING,adult BOOLEAN,video BOOLEAN"""
movies_df = spark.read.csv(str(BRONZE / "movies_metadata.csv"), header=True,schema=movie_schema)

credits_schema = "id INT, cast STRING, crew STRING"
credits_df = spark.read.csv(str(BRONZE / "credits.csv"), header=True,schema=credits_schema)

keywords_schema = "id INT,keywords STRING"
keywords_df = spark.read.csv(str(BRONZE / "keywords.csv"), header=True,schema=keywords_schema)

ratings_schema = "userId INT, movieId INT,rating DECIMAL(4,1),timestamp STRING"
ratings_df = spark.read.csv(str(BRONZE / "ratings_small.csv"), header=True,schema=ratings_schema)



#Dropped all rows missing essential columns, weaviate can handle other nulls
movies_df1 = clean_null_movies(movies_df)

credits_df1 = clean_null_credits(credits_df)

keywords_df1 = clean_null_keywords(keywords_df)

ratings_df1 = clean_null_ratings(ratings_df)

#Fixed casing, removed whitespace and trailing punctuation
movies_df2 = clean_near_dup_rows(movies_df1)

credits_df2 = clean_near_dup_rows(credits_df1)

keywords_df2 = clean_near_dup_rows(keywords_df1)

ratings_df2 = clean_near_dup_rows(ratings_df1)



