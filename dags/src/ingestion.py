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
    spark = SparkSession.builder.appName("Get Data API").getOrCreate()

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
            "price", f.expr("substring(price, 2, length(price))")
        )

        return df.select(
                    f.col("id"),f.col("image"),f.col("name"),f.col("price"),f.col("rating.*"),f.col("date"),
                )



api_endpoint = "https://api.sampleapis.com/beers/ale"

extract_data_api(api_endpoint).show()
