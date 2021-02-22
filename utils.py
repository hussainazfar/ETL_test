import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *

DATA_TYPES = [
    "DataType", "NullType", "StringType", "BinaryType", "BooleanType", "DateType",
    "TimestampType", "DecimalType", "DoubleType", "FloatType", "ByteType", "IntegerType",
    "LongType", "ShortType", "ArrayType", "MapType", "StructField", "StructType"
    ]

def initialize_spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("PayTM ETL Job") \
        .getOrCreate()
    return spark

def load_df(spark, data_file, fmat='csv'):
    if fmat not in ['json', 'csv', 'parquet', 'text']: raise Exception(f"Invalid File Format: {fmat}")
    
    # schema = StructType([
    #     StructField("dateCrawled", TimestampType(), True),
    #     StructField("name", StringType(), True),
    #     StructField("seller", StringType(), False),
    #     StructField("offerType", StringType(), True),
    #     StructField("price", LongType(), True),
    #     StructField("abtest", StringType(), True),
    #     StructField("vehicleType", StringType(), True),
    #     StructField("yearOfRegistration", StringType(), True),
    #     StructField("gearbox", StringType(), True),
    #     StructField("powerPS", ShortType(), True),
    #     StructField("model", StringType(), True),
    #     StructField("kilometer", LongType(), True),
    #     StructField("monthOfRegistration", StringType(), True),
    #     StructField("fuelType", StringType(), True),
    #     StructField("brand", StringType(), True),
    #     StructField("notRepairedDamage", StringType(), True),
    #     StructField("dateCreated", DateType(), True),
    #     StructField("nrOfPictures", ShortType(), True),
    #     StructField("postalCode", StringType(), True),
    #     StructField("lastSeen", TimestampType(), True)
    # ])
    
    # df = spark \
    #     .read \
    #     .format(format) \
    #     .schema(schema) \
    #     .option("header", "true") \
    #     .load(data_file)
    df = spark \
        .read \
        .format(fmat) \
        .option("header", "true") \
        .load(data_file)

    print('Total Records = {}'.format(df.count()))
    return df