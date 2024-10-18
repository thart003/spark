from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .getOrCreate())
args = getResolvedOptions(sys.argc, ["JOB_NAME", "ds", 'output_table'])
run_date = args['ds']
output_table = args['output_table']
glueContext = GlueContext(spark.sparkContext)
spark - glueContext.spark_session

df = spark.sql("SELECT * FROM taylorhart.nba_game_details")
df.repartition(100).writeTo(output_table) \
    .tableProperty("write.spark.fanout.enabled", "true") \
    .createOrReplace()

job = Job(glueContext)
job.init(args["JOB_NAME"], args)
