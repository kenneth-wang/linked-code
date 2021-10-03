# linked-code

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
