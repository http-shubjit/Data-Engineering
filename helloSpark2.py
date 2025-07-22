from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("fireDataFrame").getOrCreate()

fire_df=spark.read\
        .format("csv")\
        .option("header","true")\
        .option("inferSchema","true")\
        .load("zipcodes.csv")

fire_df.show(3)