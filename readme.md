## Udacity DataEngineering capstone project
This the capstone project for Udacity DataEngineering nanodegree program.

### Objective
The objective of this project is to apply the learning from the Udacity Data engineering course. 

### Overview
This is a fictional annual movie awards hosted by Sparkademy awards.<br>
Spakademy wants to identify top movies to award based on IMDB users' votes.

As a data engineer I was asked to build a data model from the datasets.<br>
The data model is used to analyse and identify the top-rated IMDB movies.

### Scope
<p>The datasets are downloaded from IMDB and TMDB dataset from Kaggle into S3.<br>
The scope of the project is to build an ELT data lake from the datasets and write them as parquet files to S3.</p>
<p>The transformed parquet files can be used for analysis </p>

#### Out of scope
- Airflow scheduling 
- Automated script to download datasets from IMDB & TMDB.
- Analysis table for the top rated movies 

### Data Model
<p>The CSV files from IMDB input data and combined additional data from TMDB as JSON are transformed into parquet files.</p>
<p>I have built a star schema of parquet. The dimension tables contains the movie titles, additional details and movie crew.</p>
<p>The fact tables contains IMDB ratings and the finance data like revenue and budget.</p>

#### Fact tables
- **movies_ratings** - movie ratings from both imdb & tmdb - split language/subfolder by years
- **movies_finances** - movie's finance details like revenue and budget

#### Dimension tables
- **movies_titles** - movies titles that are split by region/subfolder by years
- **movies_details** - movie details like titles - split by region/subfolder by years
- **movies_principal_crew** - movies' principal cast and crew.
- **movies_crew_names** - Names of the member's of the cast and crew.

![Screen Shot 2022-05-29 at 3 13 29 PM](https://user-images.githubusercontent.com/2171885/170850399-2ab18d69-e92a-4062-b792-a24039634ed7.png)

### The choice of tools, technologies, and data model are justified well.
#### Data model
<p>I have de-normalised fact tables in a way to include basic information like movie title, region, language and year.<br>
The movie data set is huge in millions because it spans across multiple countries(regions), languages and several years since 1800s.<br>
</p>
<p>
I have split the partition across different years and regions because Join queries can be really expensive so I have de-normalised them to fetch basic details.
</p>
<p>Analysis can be run on movie ratings without any complex joins so we can query movie ratings for only specific country, language or a year.<br>
I'll use a join query only if I need to fetch additional details about the movies.
</p>

#### Tools
<p>
I have chosen Spark & S3 parquet to build a data lake because the IMDB dataset is very huge and ratings keep changing all the time.<br>
Had I chosen a data warehouse like Redshift, we need to run a lot of update queries on a daily basis, and it might end up.</p>

### The data was increased by 100x?
<p>The IMDB dataset is huge, some csv files contains as much as 50 Million records. I have used only with a subset of this dataset for a year.
I couldn't use Spark to it's full potential in the Udacity workspace, I was getting out of memory errors.</p>
<p>I'll use EMR to have Spark clusters and scale accordingly. I'll also try to use serverless solution like AWS Glue crawlers to especially process the JSON data.</p>

### The pipelines would be run on a daily basis by 7 am every day?
  I'd use Airflow to schedule every morning 7am potentially download the IMDB and TMDB datasets use of AWS Lambda and then do the loading and transforming of the CSVs into parquet files.

### Challenges
  The challenge with the TMDB dataset is it contains a lot of small JSON files.<br>
  It turned out that Spark seem to be efficient with few big files rather than a lot of small files.<br>
  I was not able to use AWS Glue with the Udacity account, I'll use AWS glue crawler for this use case in future.
### Sample analysis
  Extracting the top movies of the previous year through IMDB dataset and TMDB.<br>
The top 10 most voted american english movies of 2021 are below.
```jupyter
+-------------+------------------------------------------------------+----------------+---------------+------+--------+----------+
|imdb_title_id|title                                                 |imdb_total_votes|imdb_avg_rating|region|language|start_year|
+-------------+------------------------------------------------------+----------------+---------------+------+--------+----------+
|tt10872600   |[Spider-Man: No Way Home, Serenity Now]               |521999          |8.6            |US    |null    |2021      |
|tt1160419    |[Dune]                                                |512910          |8.1            |US    |null    |2021      |
|tt11286314   |[Don't Look Up]                                       |462155          |7.2            |US    |null    |2021      |
|tt12361974   |[The Snyder Cut, Zack Snyder's Justice League]        |364815          |8.1            |US    |null    |2021      |
|tt3480822    |[Blue Bayou, Black Widow]                             |341616          |6.7            |US    |null    |2021      |
|tt9376612    |[Steamboat, Shang-Chi and the Legend of the Ten Rings]|334902          |7.5            |US    |null    |2021      |
|tt2382320    |[No Time to Die]                                      |327480          |7.3            |US    |null    |2021      |
|tt6334354    |[The Suicide Squad]                                   |313874          |7.2            |US    |null    |2021      |
|tt6264654    |[Free Guy]                                            |308222          |7.2            |US    |null    |2021      |
|tt9032400    |[Eternals]                                            |274107          |6.4            |US    |null    |2021      |
+-------------+------------------------------------------------------+----------------+---------------+------+--------+----------+
```

### Data Dictionary

#### IMDB dataset
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
  
#### TMDB data set
The TMDB dataset is retrieved from kaggle [https://www.kaggle.com/datasets/edgartanaka1/tmdb-movies-and-series](https://www.kaggle.com/datasets/edgartanaka1/tmdb-movies-and-series)
There are over 526,000 movies json files.

#### Data quality checks
- Filtering out adult movies
- Check if data row exists
- Filter by language and region
- Usage of aggregate function collect_list for titles during retrieval of top 10 movies.<br>
  Movies' working title and popular title share the same imdb id.

#### Instruction to run the scripts
<p>The ETL script settings can be configured using the settings.cfg file.<br>
The default year is set to 2022 to filter minimal data so that the script finishes sooner.</p>

#### Data analysis - Run queries on existing data model
<p>Run the following command in the Udacity workspace to see the results after the ETL is run.</p>
<p>The results table shown in the readme are for 2021, here's the instructions to verify those results.<br>
Change the OUTPUT_DIR setting in the config to s3a://udacity-dend-imdb-project/output_data and set year to 2021.
</p>


```python
python3 ./results.py
```

#### ETL - Beware! this will overwrite the existing data)
Run the following command in the Udacity workspace to do perform ELT of the datasets.<br>
The ELT scripts can run for a long time and will overwrite existing data.
```python
python3 ./etl.py
```

### Copyright
<p>This is capstone project submission by Karthik Sekar, New Zealand as part of Udacity Data Engineering project.</p>

<p>Refer to [Udacity Honor code](https://www.udacity.com/legal/en-us/honor-code)</p>
<p>You can use this repository to get an idea of the capstone project but using it as-is is plagarism.</p>
<p>Copyright © 2022, Karthik Sekar, New Zealand.</p>