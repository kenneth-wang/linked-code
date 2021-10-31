![linkedcode](logo.png)

# About the Project

# Built With
- MongoDB
- Python
- PySpark
- Jupyter

# Getting Started
1. Clone this repository onto your local machine
2. Create a virtual environement using the `conda.yml` file
3. Install MongoDB suitable for your operating system or download a docker image with MongoDB pre-installed.
4. Follow the instructions below to download and ingest the data into the database

# Usage

The project is made up of several use cases. You can review the following scripts and notebooks for each of them.

## Exploratory Data Analysis

## Use Case 1: Looking for Similar Developers

## Use Case 2: Find similar repositories based on keywords

1. Retrieving a list of similar repositories given a set of input keywords

Run the [notebook](./notebooks/similar_repos.ipynb) to encode the input keywords and repository descriptions that are found in the dataset. With the vector embeddings, Annoy python package is used to retrieve the list of similar repositories.

## Use Case 3: Recommend Repositories to Users

1. Preparing the data

This [script](./src/preprocess_collabfilt.py) is used to prepare the data for training the model. Run the script in the src folder before opening the notebook in the next part.

`> python preprocess_collabfilt.py`

2. Training the model and providing recommendations

The [notebook](./notebooks/recommender_als.ipynb) will read in the previously prepared data, build the model and display a recommendation for a randomly selected user from the test set. The model will also be saved in the models folder.

## Use Case 4: Grouping developers into communities


# Acknowledgements
This project was created as part of the coursework for the CS5344 module in Semester 1 AY 2021/2022 from NUS.

# Appendix

## Loading the data into MongoDB
1. Download the data

    [https://drive.google.com/drive/folders/1ZgashUWwLhh6RfENcZXB8uxwDf8zZ15p](https://drive.google.com/drive/folders/1ZgashUWwLhh6RfENcZXB8uxwDf8zZ15p)

2. Run a MongoDB instance using Docker

    ```bash
    docker start cs5344
    ```

    If it's your first time, do

    ```bash
    docker run --name cs5344 -p 27017:27017 -d mongo
    ```

3. Download the GUI [https://www.mongodb.com/try/download/compass](https://www.mongodb.com/try/download/compass)

4. Import the data (*.json)

    a) Using GUI. Importing repo data takes about 10 mins. Importing user data takes about 25 mins.

    b) Using CLI

    ```bash
    mongoimport --db='db_name' --collection='collection_name' --file='one_big_list.json' --jsonArray
    ```

    Note the file needs to be a list of json i.e. there must be square brackets at the start and end. If there are no square brackets in the file, try removing the last flag 'jsonArray' in the command.

5. PySpark

    ```python
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import SparkSession

    COLLECTION_NAME = "gh.repos"

    spark = SparkSession \
            .builder \
            .appName("reviews") \
            .config("spark.mongodb.input.uri", "mongodb://localhost:27017/" + COLLECTION_NAME) \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0") \
            .getOrCreate()

    df_repos = (spark
        .read.format("com.mongodb.spark.sql.DefaultSource")
        .option("uri","mongodb://localhost:27017/" + COLLECTION_NAME)
        .load()
    )
    ```
