from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LoadGithubData").getOrCreate()

df = spark.read.json("/data/raw/github-archive/2024-12-01-0.json")

df.printSchema()

total_events = df.count()
print(f"\nTotal events: {total_events}")

print("\nFirst 5 Events")
df.show(5)

spark.stop()

