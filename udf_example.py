import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from aws.glue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, udf
from pyspark.sql.types import IntegerType

spark = (SparkSession.builder
        .getOrCreate())
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds", 'output_table'])
run_date = args['ds']
output_table = args['output_table']
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

df = spark.sql("SELECT * FROM taylorhart.nba_players WHERE surrent_season = '2002')










