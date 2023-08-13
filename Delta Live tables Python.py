# Databricks notebook source
import dlt
from pyspark.sql.types import *
import pyspark.sql.functions as F
from datetime import datetime as dt
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.functions import expr
import json

storage_account = "apodatalake"
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", "....."
)  # provide account key from storage account

json_schema = StructType(
    [
        StructField("deviceID", IntegerType(), True),
        StructField("rpm", IntegerType(), True),
        StructField("angle", IntegerType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("windspeed", IntegerType(), True),
        StructField("temperature", IntegerType(), True),
        StructField("deviceTimestamp", StringType(), True),
        StructField("deviceDate", StringType(), True),
    ]
)


@dlt.create_table(comment="Bronze")
def bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "avro")
        .load("abfss://test@apodatalake.dfs.core.windows.net/.../**/**/**/**/*.avro")
        .withColumn("body", F.from_json(F.col("body").cast("string"), json_schema))
        .select(
            F.col("body.deviceID"),
            F.col("body.rpm"),
            F.col("body.angle"),
            F.col("body.humidity"),
            F.col("body.windspeed"),
            F.col("body.temperature"),
            F.to_timestamp(F.col("body.deviceTimestamp"), "dd/MM/yyyy HH:mm:ss").alias(
                "deviceTimestamp"
            ),
            F.to_date(F.col("body.deviceDate"), "dd/MM/yyyy").alias("deviceDate"),
        )
    )


@dlt.table(comment="Silver")
@dlt.expect_or_drop("deviceAngle", "deviceAngle IS NOT NULL")
@dlt.expect_or_fail("temperature", "temperature > 0")
def silver():
    return (
        dlt.readStream("bronze")
        .withColumn("deviceID", expr("CAST(deviceID AS INT)"))
        .withColumnRenamed("angle", "deviceAngle")
        .withColumnRenamed("rpm", "deviceRPM")
        .select(
            "deviceID",
            "deviceAngle",
            "deviceRPM",
            "humidity",
            "windspeed",
            "temperature",
            "deviceTimestamp",
            "deviceDate",
        )
    )


@dlt.table(comment="Gold layer")
def gold():
    return (
        dlt.readStream("silver")
        .filter(expr("deviceID >=10"))
        .withColumnRenamed("windspeed", "DeviceWindspeed")
        .select(
            "deviceID",
            "deviceAngle",
            "deviceRPM",
            "humidity",
            "DeviceWindspeed",
            "temperature",
            "deviceTimestamp",
            "deviceDate",
        )
    )
