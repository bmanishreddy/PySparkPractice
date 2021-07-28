import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, substring_index

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("LogFileDemo") \
        .getOrCreate()

    logger = Log4j(spark)

    file_df = spark.read.text("data/apache_logs.txt")
    file_df.printSchema()
    log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'

    logs_df = file_df.select(regexp_extract('value', log_reg, 1).alias('ip'),
                             regexp_extract('value', log_reg, 4).alias('date'),
                             regexp_extract('value', log_reg, 6).alias('request'),
                             regexp_extract('value', log_reg, 10).alias('referrer'))
    logs_df.printSchema()
    logs_df.show()
    logs_df\
    .withColumn("referrer",substring_index("referrer","/",3))\
    .where("trim(referrer) != '-'")\
    .groupBy("referrer").count().show(100,truncate=False)

