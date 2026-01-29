from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestSpark").getOrCreate()

data = [("Alice", 34), ("Bob", 45), ("Charlie", 28)]
df = spark.createDataFrame(data, ["name", "age"])

print("-----Test DataFrame-----")
df.show()

spark.stop()