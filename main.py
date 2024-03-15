from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, BinaryType, StringType
from pyspark.sql import functions as F

import argparse
import json
import re
from py4j.java_gateway import java_import
from datetime import datetime
import os

from date_utils import get_paths
from envy import Envy


envy = Envy()
REGEX = re.compile(
    envy.get_env("log_regex")
)
SCHEMA = StructType(
    [
        StructField("host", StringType(), True),
    ]
)


def get_spark():
    spark = SparkSession.builder.appName("SparkBloom").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext

    java_import(spark._jvm, 'org.apache.hadoop.fs.FileSystem')
    java_import(spark._jvm, 'org.apache.hadoop.fs.Path')
    java_import(spark._jvm, 'org.apache.hadoop.conf.Configuration')
    java_import(spark._jvm, 'java.net.URI')

    s3_conf = spark._jvm.Configuration()
    s3_conf.set("fs.s3a.access.key", envy.get_env("s3_access_key"))
    s3_conf.set("fs.s3a.secret.key", envy.get_env("s3_secret_key"))
    s3_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    s3_conf.set("fs.s3a.path.style.access", "true")
    s3_conf.set("fs.s3a.endpoint", envy.get_env("s3_endpoint"))

    fs = spark._jvm.FileSystem.get(spark._jvm.URI("s3a://detect-dl"), s3_conf)
    return spark, sc, fs


def get_s3_path(start_date, end_date):
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    return f"s3a://detect-dl/bloom_filters/bloom_filter_from_{start_date_str}_to_{end_date_str}"


def evtMapper(row):
    try:
        d = json.loads(row)
        msg = d["_raw"]
        m = re.match(REGEX, msg)
        if not m:
            return []
        if not m.group("dst"):
            return []
        res = (m.group("dst").lower(),)
        return [res] if res else []
    except Exception as e:
        return []


def preprocess(sc, ipaths):
    dummy = None
    step = None
    for ipath in ipaths:
        try:
            step = sc.textFile(ipath + "*.{gz,lz4,testds}")
            # dummy = dummy.flatMap(lambda x: evtMapper(x))
            dummy = dummy.union(step) if dummy else step
        except Exception as e:
            if "matches 0 files" in str(e):
                print("**ERROR** No files found under input path specification")
                print(str(e))

            else:
                print(str(e))

    return dummy.flatMap(evtMapper)


def valid_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        msg = f"Not a valid date: '{date_str}'. Please make ture that the format is correct"
        raise argparse.ArgumentTypeError(msg)
    

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--probability', type=float, required=False, default=0.001, help='Specify the probability of false positives')
    parser.add_argument('-sd', '--start_date', type=valid_date, required=True, help='Specify the first day of the date range you want to analyze (yyyy-mm-dd)')
    parser.add_argument('-ed', '--end_date', type=valid_date, required=False, help='Specify the last day of the date range you want to analyze (yyyy-mm-dd)')
    return parser.parse_args()

    

def main(args):
    probability = args.probability
    start_date = args.start_date
    end_date = args.end_date if args.end_date else args.start_date

    spark, sc, fs = get_spark()
    rdd = preprocess(
        sc,
        get_paths(envy.get_env("datalake_path"), start_date, end_date),
    ).distinct()
    rdd_count = rdd.count()

    print(f"RDD row count: {rdd_count}")
    print(f"Selected probability: {probability}")

    df = spark.createDataFrame(rdd, schema=SCHEMA)
    bloom_filter = df._jdf.stat().bloomFilter("host", rdd_count, probability)
    s3_path = get_s3_path(start_date, end_date)

    output_path = spark._jvm.Path(s3_path)
    stream = fs.create(output_path)
    bloom_filter.writeTo(stream)
    stream.close()
    spark.stop()


if __name__ == "__main__":
    args = parse_args()
    main(args)
