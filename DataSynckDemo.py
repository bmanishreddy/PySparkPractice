from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSchemaDemo") \
        .getOrCreate()

    logger = Log4j(spark)

    flightTimeParquetDF =  flightTimeParque = spark.read.format("parquet").load("data/flight*.parquet")

   # print(flightTimeParquetDF)

    #flightTimeParquetDF.write.format("avro").mode("overwrite").option("path","datasynck/avro/").save()

    logger.info("Number of partitions before = "+str(flightTimeParquetDF.rdd.getNumPartitions()))

    flightTimeParquetDF.groupBy(spark_partition_id()).count().show()

    re_Partiotion_Df = flightTimeParquetDF.repartition(5)

    logger.info("Number of partitions after = " + str(re_Partiotion_Df.rdd.getNumPartitions()))

    re_Partiotion_Df.groupBy(spark_partition_id()).count().show()


    #let us write the data into json file

    flightTimeParquetDF.write.format("json").mode("overwrite").\
        option("path","datasynck/json/").partitionBy("OP_CARRIER","ORIGIN").\
        option("maxRecordsPerFile",10000)\
        .save()






