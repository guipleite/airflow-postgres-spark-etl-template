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
    """Loads data from an API endpoint and does simple transformations using PySpark
    Args:
        api_endpoint (str): API endpoint

    Returns:
        df: Spark Dataframe
    """    


    schema_init = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("image", StringType(), True),
            StructField("name", StringType(), False),
            StructField("price", StringType(), True),
            StructField(
                "rating",
                StructType(
                    [
                        StructField("average", FloatType(), True),
                        StructField("reviews", IntegerType(), True),
                    ]
                ),
            ),
            StructField("date", TimestampType(), True),
        ]
    )
    spark = (
        SparkSession.builder.appName("Get Data API")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .getOrCreate()
    )

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
            f.col("id"),
            f.col("image"),
            f.col("name"),
            f.col("price"),
            f.col("rating.*"),
            f.col("date"),
        )


def write_to_db(df, db, host, port, table, schema, user, password):
    """Write a DataFrame to a PostgreSQL database table.

    Args:
        df (DataFrame): The Spark DataFrame to be written to the database.
        db (str): The name of the database.
        host (str): The hostname or IP address of the database server.
        port (str): The port number on which the database server is listening.
        table (str): The name of the table to write the DataFrame to.
        schema (str): The name of the schema containing the table.
        user (str): The username for authentication.
        password (str): The password for authentication.

    Returns:
        None
    """
    import logging

    logging.info(",".join(str(element) for element in[db, host, port, table, schema, user, password]))
    print( )
    url = "jdbc:postgresql://" + host + ":" + port + "/" + db

    df.write.format("jdbc")\
        .option("url", url)\
        .option("dbtable", schema + "." + table)\
        .option("user", user)\
        .option("password", password)\
        .option("driver", "org.postgresql.Driver")\
        .mode("overwrite")\
        .save()


if __name__ == "__main__":
    import os
    import dotenv
    dotenv.load_dotenv()

    df = extract_data_api(os.getenv("API_ENDPOINT"))
    df.show()
    write_to_db(
        df,
        os.getenv("DB_NAME"),
        "localhost",
        os.getenv("DB_PORT"),
        os.getenv("DB_TABLE"),
        os.getenv("DB_SCHEMA"),
        os.getenv("DB_USER"),
        os.getenv("DB_PASS"),
    )
