import os
import boto3
import io
import pyspark
from pyspark.sql import SparkSession
import pandas as pd
from pprint import pprint
from pyspark import SparkContext, SparkConf

aws_access_key_id = os.getenv('HappinessProjectAccessKeyId')
aws_secret_access_key = os.getenv('HappinessProjectAccessKey')

sparkconf = SparkConf().set('spark.executor.extraJavaOptions','-Dcom.amazonaws.services.s3.enableV4=true'). \
 set('spark.driver.extraJavaOptions','-Dcom.amazonaws.services.s3.enableV4=true').setAppName('appName').setMaster('local[1]')

sparkcontext = SparkContext(conf=sparkconf)
sparkcontext.setSystemProperty("com.amazonaws.services.s3.enableV4","true") 

hadoopConf = sparkcontext._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', aws_access_key_id)
hadoopConf.set('fs.s3a.secret.key', aws_secret_access_key)
hadoopConf.set('fs.s3a.endpoint', 's3-us-east-2.amazonaws.com')
hadoopConf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')

spark = SparkSession(sparkcontext)

s3_df=spark.read.csv('s3a://happiness-project-data/RawData/worldhappiness/world-happiness-report-2021.csv',header=True,inferSchema=True)
s3_df.show(5)