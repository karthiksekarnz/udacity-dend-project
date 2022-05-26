## Udacity DataEngineering capstone project
This the capstone project for Udacity DataEngineering nanodegree program.

### Objective
The objective of this project is to apply the learning from the Udacity Data engineering course. 

### Overview
This is a fictional annual movie awards hosted by Sparkademy awards.<br>
Spakademy wants to identify top movies to award based on IMDB users' votes.

As a data engineer I was asked to build a data model from the datasets, so they can analyse the data and identify the top rated movies.
I am using Spark to build a datalake by writing parquet tables into S3. Full description of the datasets are below.

### Project write up
- #### Choice of tools
    I have chosen Spark as my tool and build a data lake with parquet files in S3. The reason I have chosen these tools
    is that the IMDB dataset is very huge and ratings keep changing all the time. Had I chosen a data warehouse like Redshift, I need to run a lot of update queries.
- #### Scheduling
    I'd use Airflow to schedule every morning 7am to download the IMDB datasets and the
- #### The data was increased by 100x?
    The IMDB dataset is huge, some csv files contains as much as 50 Million records. I have worked only with a subset of this dataset. 
    I couldn't use Spark to it's full potential in the Udacity workspace, I'll use EMR to have Spark clusters and scale accordingly. <br>
    I'll also try to use serverless solution like AWS Glue.
- #### Sample analysis

### IMDB dataset
The full description of the IMDB dataset can be found here [https://www.imdb.com/interfaces/](https://www.imdb.com/interfaces/)

Each dataset is contained in a gzipped, tab-separated-values (TSV) formatted file in the UTF-8 character set.<br>
The available datasets are as follows:<br>
- **title.basics.tsv.gz** <br>
Contains all types of titles available in IMDB (e.g. movie, tv series, video game, etc) <br>~8.8M records
- **title.akas.tsv.gz** <br>
Contains additional information about titles (e.g: language, isOriginalTitle, etc.)
- **title.principals.tsv.gz** <br> ~49.7M – Contains the principal cast/crew for titles
- **title.ratings.tsv.gz** <br>
Contains the IMDb rating and votes information for titles<br>
~1.22M records
- **name.basics.tsv.gz**<br>
Contains the following information for names<br>
~11.4M records
  
### TMDB data set
The TMDB dataset is retrieved from kaggle [https://www.kaggle.com/datasets/edgartanaka1/tmdb-movies-and-series](https://www.kaggle.com/datasets/edgartanaka1/tmdb-movies-and-series)
There are over 526,000 movies json files. 

### Star schema
Read the CSV files from IMDB input data

- **movies_titles** - movie titles that are split by language/subfolder by years
- **movies_details** - movie details like titles - split language/subfolder by years
- **movies_ratings** - movie ratings from both imdb & tmdb - split language/subfolder by years
- **movies_finances** - movie's finance details like revenue and budget
- **movies_principal_crew** - movies' principal cast and crew.
- **movies_crew_names** - Names of the member's of the cast and crew.

