from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("simpleSparkSession").getOrCreate()
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

# Stop the SparkSession
spark.stop()