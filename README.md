# linked-code

1. Download the data

    [https://drive.google.com/drive/folders/1ZgashUWwLhh6RfENcZXB8uxwDf8zZ15p](https://drive.google.com/drive/folders/1ZgashUWwLhh6RfENcZXB8uxwDf8zZ15p)

2. Run a MongoDB instance using Docker

    ```bash
    docker run --name cs5344 -p 27017:27017 -d mongo
    ```

3. Download the GUI [https://www.mongodb.com/try/download/compass](https://www.mongodb.com/try/download/compass)
4. Import the data (*.json) in GUI. Importing repo data takes about 10 mins. Importing user data takes about 25 mins.
5. PySpark

    ```python
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import SparkSession

    spark = SparkSession \
            .builder \
            .appName("reviews") \
            .config("spark.mongodb.input.uri", "mongodb://localhost:27017/...") \
            .config("spark.mongodb.output.uri", "mongodb://localhost:27017/...") \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0") \
            .getOrCreate()
        # Azmi: This was my test to convert Lab1 to use mondgodb. If we are reading into spark
        # dataframe, no need to convert to JSON (i.e. remove last method).
        reviews = spark.read.format("com.mongodb.spark.sql.DefaultSource").load().toJSON()
        metas = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://localhost:27017/lab1.reviews").load().toJSON()
    ```
