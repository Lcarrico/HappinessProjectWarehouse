import pyspark 
import S3ConnectPandas
import pandas 

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("cleanDataApp").getOrCreate()

sparkDF = spark.createDataFrame(S3ConnectPandas.data[0][1])
sparkDF.printSchema()
sparkDF.show()