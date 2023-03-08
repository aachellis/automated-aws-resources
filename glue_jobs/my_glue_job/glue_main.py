import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from utility.utils import get_data

sparkConf = SparkConf().setAppName("DHGlueJob")\
                    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
                    .set("spark.sql.hive.convertMetastoreParquet", "false")

glueContext = GlueContext(SparkContext.getOrCreate(sparkConf))
spark = glueContext.spark_session                    

df = get_data(20)

print(df.show())