import json
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

# from pymongo import MongoClient
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, types, functions

def preprocess(config: dict):
    spark = (
        SparkSession
        .builder 
        .appName("reviews") 
        .config("spark.driver.memory", "10g")
        .config("spark.driver.maxResultSize", "0") 
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0") 
        .getOrCreate()
    )

    mongodb_uri = config['data']['mongodb_uri']
    
    # collection names for the various datasets
    repos_coll = config['data']['repository_tbl']
    users_coll = config['data']['user_tbl']
    combined_coll = config['data']['combined_tbl']

    # Load the User and Repository data
    users = (spark
        .read.format("com.mongodb.spark.sql.DefaultSource")
        .option("uri","mongodb://" + mongodb_uri + users_coll)
        .load()
    )

    repos = (spark
        .read.format("com.mongodb.spark.sql.DefaultSource")
        .option("uri","mongodb://" + mongodb_uri + repos_coll)
        .load()
    )

    # Explode the starred repos for user, one row per repo/user
    users_wstarred = (
        users
        .filter(users['starred'].isNotNull())
        .withColumnRenamed('id','user_id')
        .select(['user_id',functions.explode('starred').alias('full_name')])
    )

    repos_select = repos.select(['id','full_name'])

    # Split the input df into those with numeric repo ids and text repo names
    input_wnumeric = (
        users_wstarred
        .filter(functions.col('full_name').cast('int').isNotNull())
    )
    
    input_wtext = (
        users_wstarred
        .filter(functions.col('full_name').cast('int').isNull())
    )

    # Process users with text repo names
    input_wtext2 = (
        input_wtext
        .join(repos_select,on='full_name',how='left')
        .filter('id is not Null')
        .drop('full_name')
        .withColumnRenamed('id','repo_id')
    )

    # Process users with numeric repo id
    input_wnumeric2 = (
        input_wnumeric
        .withColumnRenamed('full_name','id')
        .join(repos.select(['id','name']), on='id', how='left')
        .filter('name is not Null')
        .drop('name')
        .withColumnRenamed('id','repo_id')
    )

    # Combine the 2 subsets
    input_wnumeric3 = input_wnumeric2.withColumn('repo_id',input_wnumeric2['repo_id'].cast('int'))
    input_wtext3 = input_wtext2.withColumn('repo_id',input_wtext2['repo_id'].cast('int'))
    
    combined = (
        input_wnumeric3
        .union(input_wtext3)
        .select(['user_id','repo_id'])
        .sort(['user_id','repo_id'])
        .dropDuplicates()
    )

    print(f"Final df count = {combined.count()}")
    print(f"{combined.show()}")

    if config['recommender']['save_preprocess']:
        print("Saving preprocessing to DB")
        (
            combined
            .write
            .format("mongo")
            .mode("overwrite")
            .option("uri","mongodb://localhost:27017/cs5344.repos")
            .option("database","cs5344")
            .option("collection", "combined")
            .save()
        )
    else:
        print("Recommender preprocessing not saved to DB")

    spark.stop()

    return

if __name__ == '__main__':
    
    # Read in config file
    with open("../config.json") as fp:
        config: dict = json.load(fp)
    
    preprocess(config)