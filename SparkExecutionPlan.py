import sys

from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import get_spark_app_config, load_survey_df, count_by_country, load_survey_df_no_head

if __name__ == '__main__':

    conf = get_spark_app_config()
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    logger = Log4j(spark)

    if len(sys.argv) != 2:
        logger.error("Hellow code <Filename>")
        sys.exit(-1)

    logger.info("starting the spark app")

    #conf_out = spark.sparkContext.getConf()
    #logger.info(conf_out.toDebugString())



    survey_data = load_survey_df(spark,sys.argv[1])

    partioned_df = survey_data.repartition(2)



    count_df = count_by_country(partioned_df)



    logger.info(count_df.collect())

    #filter_survey_df.show()

    input("press enter")

    logger.info("stopping the app ")
    #spark.stop()
