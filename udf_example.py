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

df = spark.sql("SELECT * FROM taylorhart.nba_players WHERE surrent_season = 2002")

def consecutive_season(seasons, stat, cutoff):
        consecutive = 0
        max_consecutive = 0
        consecutive_map = {}

        configurations = [
                ('consecutive_20pt_seasons', 'pts', 20),
                ('consecutive_10reb_seasons', 'reb', 10),
                ('consecutive_5ast_seasons', 'ast', 5)
        ]
        for config in configurations:
            stat = config[1]
            cutoff = config[2]
            consecutive = 0
            max_consecutive = 0
            for season in seasons:
                if season[stat] >= cutoff:
                    consecutive += 1
                else:
                    if consecutive > max_consecutive:
                        max_consecutive = consecutive
                    consecutive = 0
           if consecutive > max_consecutive:
                max_consecutive = consecutive
           consecutive_map[config[0]] = max_consecutive
        return consecutive_map
                    
consecutive_stat udf = udf(lambda seasons: consecutive_seasons(seasons, stat, cutoff), IntegerType())

df = (df
      .withColumn("consecutive_20pt_seasons",
                  consecutive_stat_udf(col("seasons"), lit("pts"), lit(20))
      .withColumn("consecutive_10reb_seasons",
                  consecutive_stat_udf(col("seasons"), lit("reb"), lit(10))
      .withColumn("consecutive_5ast_seasons",
                  consecutive_stat_udf(col("seasons"), lit("ast"), lit(5))
                 )

df.writeTo(output_table) \
     .tableProperty("write.spark.fanout.enabled", "true") \
     .createOrReplace()

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


                  




