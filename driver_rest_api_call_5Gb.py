import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
import requests
spark = SparkSession.builder.getOrCreate())
args = getResolvedOptions(sys.argc, ["JOB_NAME", "ds", 'output_table'])
run_date = args['ds']
output_table = args['output_table']

api_key = 'api_key'
starter_url = f'https://api.polygon.io...{api_key}'
response = requests.get(starter_url)
tickers = []
data = response.json()
tickers.extend(data['results'])
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

# Collect the results on the driver
while data['status'] == 'OK' and 'next_url' in data:
  tickers.extend(data['results'])
  print(data['next_url'] + '&apiKey=' + api_key)
  response = requests.get(data['next_url'] + '&apiKey=' + api_key
  data = response.json()

date = datetime.strptime(run_date, __format: "%Y-%m-%d").date()
df = spark.createDataFrame(tickers).withColumn('date', lit(date))
output_table = taylorhart.stock_tickers'
query = f"""CREATE TABLE IF NOT EXISTS {output_table} (
  active BOOLEAN,
  cik STRING,
  composite_figi STRING,
  currency_name STRING,
  last_updated utc STRING,
  locale STRING,
  market STRING,
  name STRING,
  primary_exchange STRING,
  share_class_figi STRING,
  ticker STRING,
  `type` STRING,
  date DATE
  )
  USING iceberg
  PARTITION BY (date)
  """
spark.sql(query)
df.select(
  col("active"),
  col("cik"),
  col("composite_figi"),
  col("currency_name"),
  col("last_updated_utc"),
  col("locale"),
  col("market"),
  col("name"),
  col("primary_exchange"),
  col("share_class_figi"),
  col("ticker"),
  col("type"),
  col("date"
     ).writeTo(output_table).using("iceberg").partitionBy("date").overwritePartition
    df.printSchema

    job = JOB(glueContext)
    job.init(args["JOB_NAME"], args)





  
  




            








