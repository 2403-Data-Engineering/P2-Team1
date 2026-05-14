import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower,coalesce,array
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
movies_df1 = movies_df.dropna(subset=["id", "imdb_id", "overview","release_date","adult"]) \
        .dropna(subset=["title", "original_title"], how="all")\

credits_df1 = credits_df.dropna(subset=["id"]) \
        .dropna(subset=["cast","crew"], how="all")

keywords_df1 = keywords_df.dropna(subset=["id","keywords"])

ratings_df1 = ratings_df.dropna(subset=["userId","movieId","rating"])
#======================================================================================================


