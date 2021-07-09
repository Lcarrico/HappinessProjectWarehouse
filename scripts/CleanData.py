import pyspark 
import S3Connect
import pandas 

from pyspark.sql import SparkSession



spark = SparkSession.builder.master("local[1]").appName("cleanDataApp").getOrCreate()

sparkDF = spark.createDataFrame(S3Connect.data[0][1])
sparkDF.printSchema()
sparkDF.show()