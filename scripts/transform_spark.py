from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, when, from_json, regexp_replace, regexp_extract
from pyspark.sql.types import ArrayType, IntegerType, DoubleType

def main():
    spark = SparkSession.builder.appName("MoviesTransform").getOrCreate()

    df = spark.read.csv(
        "/Users/kaancakir/data_projects_local/movie_etl_pipeline/data/raw/movies_tmdb.csv",
        header=True,
        inferSchema=True
    )
    print("Original Columns: ", df.columns)

    df = df.withColumn("popularity", regexp_replace("popularity", "[^0-9.]", ""))
    df = df.withColumn("vote_average", regexp_replace("vote_average", "[^0-9.]", ""))
    df = df.withColumn("vote_count", regexp_replace("vote_count", "[^0-9]", ""))

    pattern = "^\\d+(\\.\\d+)?$"
    df = df.withColumn(
        "popularity",
        when((col("popularity") == "") | (~col("popularity").rlike(pattern)), None).otherwise(col("popularity"))
    )
    df = df.withColumn(
        "vote_average",
        when((col("vote_average") == "") | (~col("vote_average").rlike(pattern)), None).otherwise(col("vote_average"))
    )
    df = df.withColumn("vote_count", when(col("vote_count") == "", None).otherwise(col("vote_count")))

    df = df.withColumn("popularity", col("popularity").cast(DoubleType()))
    df = df.withColumn("vote_average", col("vote_average").cast(DoubleType()))
    df = df.withColumn("vote_count", col("vote_count").cast("int"))

    df = df.dropna(subset=["vote_average", "popularity"])

    df = df.withColumn("release_year_raw", regexp_extract(col("release_date"), "(\\d{4})", 1))
    df = df.withColumn(
        "release_year",
        when(col("release_year_raw") == "", None).otherwise(col("release_year_raw").cast("int"))
    )
    df = df.drop("release_year_raw")

    df = df.withColumn("genre_ids_array", from_json(col("genre_ids"), ArrayType(IntegerType())))
    df = df.withColumn("genre_id", explode(col("genre_ids_array")))

    df = df.withColumn(
        "language_full",
        when(col("original_language") == "en", "English")
        .when(col("original_language") == "fr", "French")
        .when(col("original_language") == "ja", "Japanese")
        .otherwise("Other")
    )

    df = df.withColumn("rating_percent", col("vote_average") * 10)

    df = df.withColumn(
        "popularity_bucket",
        when(col("popularity") < 20, "Low")
        .when((col("popularity") >= 20) & (col("popularity") < 60), "Medium")
        .otherwise("High")
    )

    df = df.dropDuplicates(["id"])

    df = df.select(
        "id",
        "title",
        "release_year",
        "genre_id",
        "language_full",
        "rating_percent",
        "popularity_bucket",
        "vote_count"
    )

    df.write.mode("overwrite").json("../data/processed/movies_cleaned_json")

    print("Data cleaned and saved to: data/processed/movies_cleaned_json")

    spark.stop()

if __name__ == "__main__":
    main()
