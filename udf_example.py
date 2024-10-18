import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from aws.glue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, udf
from pyspark.sql.types import IntegerType


