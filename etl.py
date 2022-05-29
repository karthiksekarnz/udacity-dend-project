import os
import configparser
import logging
import datetime as dt
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

config = configparser.ConfigParser()
config.read('settings.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('KEYS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('KEYS', 'AWS_SECRET_ACCESS_KEY')

input_dir = config.get('FILES', 'INPUT_DIR')
output_dir = config.get('FILES', 'OUTPUT_DIR')

imdb_input = input_dir + '/imdb'
tmdb_input = input_dir + '/tmdb'
now = dt.datetime.now()


def create_spark_session():
    """
    Creates a spark session

    Arguments: None

    Returns:
         spark: Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    return spark


def process_imdb_movie_titles(spark):
    """
    Process IMDB movie titles

    :param spark:
    :return:
    """
    logging.warning('*** Processing IMDB movies titles ***')

    # imdb titles
    imdb_titles_df = read_imdb_titles(spark)

    imdb_movie_titles_df = imdb_titles_df.select(
        col("tconst").alias("title_id"),
        col("titleType").alias("title_type"),
        col("isAdult").alias("is_adult"),
        col("startYear").alias("start_year")
    )

    imdb_movie_titles_df = filter_imdb_movies(imdb_movie_titles_df)

    imdb_title_akas_df = spark.read.options(header=True, inferSchema=True, nullValue="\\N") \
        .csv(imdb_input + '/title.akas.tsv.gz', sep='\t')

    movies_titles = imdb_movie_titles_df \
        .join(imdb_title_akas_df, imdb_movie_titles_df.title_id == imdb_title_akas_df.titleId, "left") \
        .select(col("titleId").alias("imdb_title_id"),
                imdb_title_akas_df.title,
                "is_adult",
                "language",
                "region",
                "start_year",
                ).distinct()

    movies_titles = cleanup_movie_titles(movies_titles)

    # Filter movie titles by year or a region(country)
    movies_titles = apply_filters(movies_titles)

    # Check if data exists
    check_if_data_exists(movies_titles)

    movies_titles.write.partitionBy("region", "start_year").format("parquet") \
        .save(output_dir + '/movies_titles', mode="overwrite")

    logging.warning('*** Finished processing IMDB movies titles ***')


def filter_imdb_movies(df: DataFrame):
    """
    Filter out only movies and exclude adult movies.

    :param df:
    :return:
    """
    return df.filter((df['title_type'] == 'movie') & (df['is_adult'] == '0'))


def cleanup_movie_titles(df: DataFrame):
    """
    Cleanup movie titles, include only English titles and exclude movies without any region or year.

    :param df:
    :return:
    """
    return df.filter((col("language") == "en") & col("region").isNotNull() & col("start_year").isNotNull())


def apply_filters(df: DataFrame):
    """
    Applies filters like

    :param df:
    :return:
    """

    df = df.filter(col("region").isNotNull() & col("start_year").isNotNull())

    filter_by_region = config.get('FILTERS', 'region')
    filter_by_year = config.get('FILTERS', 'year')

    condition = None

    if filter_by_year and filter_by_region:
        condition = "start_year == {0} and region == {1}".format(filter_by_year, filter_by_region)

    elif filter_by_region and not filter_by_year:
        condition = "region == {0}".format(filter_by_region)

    elif filter_by_year and not filter_by_region:
        condition = "start_year == {0}".format(filter_by_year)

    if condition is not None:
        return df.filter(condition)

    return df


def process_tmdb_movies(spark):
    """
    Process movies from TMDB and writes them as parquet files

    :param spark:
    :return:
    """
    logging.warning('*** Processing TMDB movies titles ***')

    tmdb_movies = spark.read.json(tmdb_input + '/movies_list', multiLine=True)
    tmdb_movies = tmdb_movies.select(
        "id", "imdb_id", "revenue", "budget", "overview", "popularity", "release_date",
        "status", "vote_average", "vote_count"
    )

    tmdb_movies.write.format("parquet") \
        .save(output_dir + '/tmdb_movies', mode="overwrite")

    logging.warning('*** Finished processing TMDB movies titles ***')


def process_movies_titles_details(spark):
    """
    Processes imdb data and writes them into parquet files

    Arguments: None

    :param spark: Spark Context

    Returns: None
    """
    logging.warning('*** Processing movies titles details ***')

    imdb_titles_df = read_imdb_titles(spark)
    imdb_movie_titles_df = imdb_titles_df.select(
        col("tconst").alias("imdb_title_id"),
        col("titleType").alias("title_type"),
        col("primaryTitle").alias("primary_title"),
        col("originalTitle").alias("original_title"),
        col("genres"),
        col("runtimeMinutes").alias("runtime"),
        col("isAdult").alias("is_adult"),
        col("startYear").alias("start_year"),
    )

    imdb_movie_titles_df = filter_imdb_movies(imdb_movie_titles_df)
    imdb_title_akas_df = read_imdb_akas_titles(spark)

    tmdb_movies_df = get_tmdb_movies(spark)

    # movies_details query combining imdb & tmdb movies
    movies_details = imdb_movie_titles_df \
        .join(imdb_title_akas_df, imdb_movie_titles_df.imdb_title_id == imdb_title_akas_df.titleId, "left")\
        .join(tmdb_movies_df, imdb_movie_titles_df.imdb_title_id == tmdb_movies_df.imdb_id, "left") \
        .select(
            "imdb_title_id",
            imdb_title_akas_df.title,
            imdb_movie_titles_df.original_title,
            imdb_movie_titles_df.primary_title,
            col("isOriginalTitle").alias("is_original_title"),
            imdb_title_akas_df.ordering,
            imdb_movie_titles_df.genres,
            imdb_title_akas_df.language,
            imdb_title_akas_df.region,
            tmdb_movies_df.overview,
            imdb_movie_titles_df.start_year,
            imdb_movie_titles_df.runtime,
            tmdb_movies_df.release_date
    )

    # movies_details table parquet
    movies_details.write.partitionBy("region", "start_year").format("parquet") \
        .save(output_dir + '/movies_titles_details', mode="overwrite")

    logging.warning('*** Finished processing movies titles details ***')


def process_movies_finances(spark):
    """
    Process movies' finance details like budget, revenue etc and write parquet files

    :param spark:
    :return:
    """
    logging.warning('*** Processing movies finances ***')

    movies_details = get_movies_basic_details(spark)
    tmdb_movies_df = spark.read.parquet(output_dir + '/tmdb_movies')

    # Movies finances
    movies_finances = movies_details \
        .join(tmdb_movies_df, movies_details.imdb_title_id == tmdb_movies_df.imdb_id, "left") \
        .select(
            movies_details.imdb_title_id,
            movies_details.title,
            movies_details.language,
            movies_details.region,
            movies_details.start_year,
            col("revenue"),
            col("budget")
        )

    movies_finances.write.partitionBy("region", "start_year").format("parquet") \
        .save(output_dir + '/movies_finances', mode="overwrite")

    logging.warning('*** Finished processing movies finances ***')


def process_movies_ratings(spark):
    """
    Process movies ratings and write parquet files

    :param spark:
    :return:
    """
    logging.warning('*** Processing movies ratings ***')

    movies_details = get_movies_basic_details(spark)
    tmdb_movies_df = get_tmdb_movies(spark)

    # IMDB ratings DF
    imdb_ratings_df = spark.read.options(header=True, inferSchema=True, nullValue="\\N") \
        .csv(imdb_input + '/title.ratings.tsv.gz', sep='\t')

    # IMDB movie ratings query
    movies_ratings = movies_details \
        .join(imdb_ratings_df, movies_details.imdb_title_id == imdb_ratings_df.tconst, "left") \
        .join(tmdb_movies_df, movies_details.imdb_title_id == tmdb_movies_df.imdb_id, "left") \
        .select(
            "imdb_title_id",
            movies_details.title,
            "region",
            "language",
            "start_year",
            col("numVotes").alias("imdb_total_votes"),
            col("averageRating").alias("imdb_avg_rating"),
            col("vote_count").alias("tmdb_total_votes"),
            col("vote_average").alias("tmdb_avg_rating")
        )

    # movies_ratings table write parquet
    movies_ratings.write.partitionBy("region", "start_year").format("parquet") \
        .save(output_dir + '/movies_ratings', mode="overwrite")

    logging.warning('*** Finished processing movies ratings ***')


def process_movies_crews(spark):
    """
    Process movies crews and write parquet files

    :param spark:
    :return:
    """
    logging.warning('*** Processing movies crews ***')

    movies_details = get_movies_basic_details(spark)

    # movie title ids
    movie_title_ids = movies_details.select("imdb_title_id").distinct() \
        .rdd.map(lambda row: row['imdb_title_id']).collect()

    imdb_principals_df = spark.read.options(header=True, inferSchema=True, nullValue="\\N").csv(
        imdb_input + '/title.principals.tsv.gz', sep='\t')

    imdb_principal_crew = imdb_principals_df.select(
        col("tconst").alias("imdb_title_id"),
        "ordering",
        col("nconst").alias("imdb_name_id"),
        "category",
        "job",
        "characters"
    ).filter(imdb_principals_df['tconst'].isin(movie_title_ids))

    # movies_principal_crew parquet table
    imdb_principal_crew.write.partitionBy("imdb_title_id").format("parquet") \
        .save(output_dir + '/movies_principal_crew', mode="overwrite")

    movie_crew_name_ids = imdb_principal_crew.select("imdb_name_id").distinct() \
        .rdd.map(lambda row: row['imdb_name_id']).collect()

    imdb_names_df = spark.read.options(header=True, inferSchema=True, nullValue="\\N").csv(
        imdb_input + '/name.basics.tsv.gz', sep='\t')

    # Imdb crew names query
    imdb_crew_names = imdb_names_df.select(
        col("nconst").alias("imdb_name_id"),
        col("primaryName").alias("primary_name"),
        col("birthYear").alias("birth_year"),
        col("deathYear").alias("death_year"),
        col("primaryProfession").alias("primary_profession"),
        col("knownForTitles").alias("known_for_titles")
    ).filter(imdb_names_df['nconst'].isin(movie_crew_name_ids))

    # movies_crew_names parquet table
    imdb_crew_names.write.partitionBy("birth_year").format("parquet") \
        .save(output_dir + '/movies_crew_names', mode="overwrite")

    logging.warning('*** Finished processing movies crews ***')


def get_movies_basic_details(spark):
    """
    Read the parquet table to get movies details

    :param spark:
    :return:
    """

    return spark.read.parquet(output_dir + '/movies_titles')


def read_imdb_titles(spark):
    """
    Reads imdb basic titles

    :param spark:
    :return:
    """
    return spark.read.options(header=True, inferSchema=True, nullValue="\\N") \
        .csv(imdb_input + '/title.basics.tsv.gz', sep='\t')


def read_imdb_akas_titles(spark):
    """
    Reads imdb akas titles

    :param spark:
    :return:
    """
    return spark.read.options(header=True, inferSchema=True, nullValue="\\N") \
        .csv(imdb_input + '/title.akas.tsv.gz', sep='\t')


def get_movies_titles_details(spark):
    """
    Read the parquet table to get movies details

    :param spark:
    :return:
    """

    return spark.read.parquet(output_dir + '/movies_titles_details')


def get_tmdb_movies(spark):
    """
    Read the parquet table to get movies details

    :param spark:
    :return:
    """

    return spark.read.parquet(output_dir + '/tmdb_movies')


def check_if_data_exists(df: DataFrame):
    """
    Check for data quality if at least 1 row exists

    :param df:
    :return:
    """
    try:
        item = df.take(1)

        if len(item) < 1:
            raise ValueError(f"There are no records in the dataframe.")
    except ValueError:
        print("Unable to run data quality check command.")


def main():
    """
    Main function

    Arguments: None

    Returns:
         None
    """
    spark = create_spark_session()

    process_imdb_movie_titles(spark)
    process_tmdb_movies(spark)
    process_movies_titles_details(spark)
    process_movies_ratings(spark)
    process_movies_finances(spark)
    process_movies_crews(spark)

    logging.warning('*** Congratulations! the ETL has completed successfully! ***')


if __name__ == "__main__":
    main()
