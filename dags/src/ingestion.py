import pandas as pd
import requests as re
from datetime import datetime
import pyspark.sql.functions as f
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType,
    TimestampType,
)

from pyspark.sql import SparkSession


def extract_data_api(api_endpoint):
    schema_init = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("image", StringType(), True),
            StructField("name", StringType(), False),
            StructField("price", StringType(), True),
            StructField( "rating",StructType([
                StructField("average", FloatType(), True),
                StructField("reviews", IntegerType(), True),
            ]),),
            StructField("date", TimestampType(), True),
        ]
    )
    spark = SparkSession.builder.appName("Get Data API").config("spark.jars.packages", 
                "org.postgresql:postgresql:42.6.0") \
                .getOrCreate()

    dt = datetime.now()
    res = None
    try:
        res = re.get(api_endpoint)
    except Exception as e:
        print(e)

    if res != None and res.status_code == 200:

        rdd = spark.sparkContext.parallelize([res.text])

        df = spark.read.json(rdd, schema=schema_init)
        df = df.withColumn("date", f.lit(dt)).withColumn(
            "price", f.expr("substring(price, 2, length(price))").cast(FloatType())
        )

        return df.select(
                    f.col("id"),f.col("image"),f.col("name"),f.col("price"),f.col("rating.*"),f.col("date"),
                )

def write_to_db(df):

    url = "jdbc:postgresql://localhost:5433/template_db"
 
    df.write \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", "bronze_layer.beer_reviews") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite").option("user", "postgres").option("password", "postgres") \
    .save()

if __name__ == "__main__":

    api_endpoint = "https://api.sampleapis.com/beers/ale"

    df = extract_data_api(api_endpoint)
    df.show()
    write_to_db(df)
