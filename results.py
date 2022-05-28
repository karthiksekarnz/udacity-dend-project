import os
import configparser
import logging
from pyspark.sql import SparkSession, DataFrame

config = configparser.ConfigParser()
config.read('settings.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('KEYS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('KEYS', 'AWS_SECRET_ACCESS_KEY')

output_dir = config.get('FILES', 'OUTPUT_DIR')


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

    return spark


def get_top_rated_movies(spark):
    """
    Perform data analysis on the transformed data

    :param spark:
    :return:
    """
    movies_ratings = spark.parquet.read(output_dir + '/movies_ratings')
    movies_ratings.createOrReplaceTempView("movies_ratings")

    top_american_english_movies = spark.sql('''
            SELECT 
              imdb_title_id, 
              collect_list(title) as title, 
              imdb_total_votes, 
              imdb_avg_rating, 
              region, 
              language, 
              start_year 
            FROM movies_ratings 
            WHERE language IS NULL OR language = 'en' 
            GROUP BY 
              imdb_title_id, 
              imdb_total_votes, 
              imdb_avg_rating, 
              region, 
              language, 
              start_year 
            ORDER BY imdb_total_votes DESC
    ''')

    logging.info('*** Getting Top 10 American movies  ***')
    top_american_english_movies.show(10, False)


def main():
    """
    Main function

    Arguments: None

    Returns:
         None
    """
    spark = create_spark_session()

    get_top_rated_movies(spark)


if __name__ == "__main__":
    main()
