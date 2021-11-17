import configparser
import os
import subprocess
import sys
from datetime import datetime
from functools import reduce

import boto3
from botocore.exceptions import ClientError
from pyspark.sql import Row
from pyspark.sql import SparkSession

from pyspark.sql.functions import *


class TablesConf(object):
    def __init__(self, group, table_name, frequency):
        self.group = group
        self.table_name = table_name
        self.frequency = frequency


def build_table_conf(in_tables_path):
    f = open(in_tables_path, "r")
    tables_config = []
    for line in f:
        fields = line.split(",")
        group = fields[0]
        table_name = fields[1]
        frequency = fields[2]
        tables_config.append(TablesConf(group, table_name, frequency))
    return tables_config


def load_app_config(conf_path, env):
    conf = configparser.ConfigParser()
    conf.read(conf_path)[env]
    return conf


def read_table(spark, table_name):
    return spark.read.table(table_name)


def write_csv(out_df, output_path, header="true", mode="overwrite"):
    out_df.write().mode(mode).option("header", header).format("csv").save(output_path)


def upload_file(s3_client, file_name, s3_bucket, object_name=None):
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    try:
        response = s3_client.upload_file(file_name, s3_bucket, object_name)
        print("response: " + str(response))
    except ClientError as e:
        print("exception: " + str(e))
        raise e


def run_cmd(args_list):
    """
        run linux commands
        """
    print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    return s_return, s_output, s_err


spark = SparkSession \
    .builder \
    .appName("Data Transfer") \
    .enableHiveSupport() \
    .getOrCreate()

environment = sys.argv[0]
config_path = sys.argv[1]

config = load_app_config(config_path, environment)
tables_path = config["TABLES_CONF_PATH"]
tables_conf = build_table_conf(tables_path)

today_date = datetime.today().strftime('%Y-%m-%d')

base_path = config["OUTPUT_DIR"] + "/processed_date=" + today_date

bucket = config["AWS_S3_BUCKET"]
prefix = config["AWS_PREFIX"]
aws_s3_path = prefix + "/processed_date=" + today_date

client = boto3.client('s3', aws_access_key_id=config["AWS_ACCESS_KEY"], aws_secret_access_key=config["AWS_SECRET_KEY"])


def build_header_dataframe(schema, header_columns):
    emptyRDD = spark.sparkContext.emptyRDD()
    df = spark.createDataFrame(emptyRDD, schema)

    for col_name in header_columns:
        df = df.withColumn(col_name, lit(col_name))
    df = df.select(*header_columns)                                                         
    return df


for table_conf in tables_conf:
    df = read_table(table_conf.table_name)
    header_df = build_header_dataframe(df.schema, df.columns)
    # Write header first as record
    write_csv(header_df, base_path + "/" + table_conf.table_name, header="false", mode="overwrite")
    # Write detail records without header
    write_csv(df, base_path + "/" + table_conf.table_name, header="false", mode="overwrite")
    local_path = base_path + "/" + table_conf.table_name
    # merge the data and copied it to local path
    hdfs_merge = ['hdfs', 'dfs', '-getmerge', base_path + "/" + table_conf.table_name, local_path]
    run_cmd(hdfs_merge)
    # upload file to s3
    upload_file(client, local_path, bucket, aws_s3_path)
    delete_local_path = ['rm', '-r', base_path + "/" + table_conf.table_name, local_path]
    run_cmd(delete_local_path)

spark.stop()
