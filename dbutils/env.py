from pyspark.sql.session import SparkSession
from pyspark.dbutils import DBUtils


def get_dbutils():
    spark = SparkSession.builder.getOrCreate()
    return DBUtils(spark)
