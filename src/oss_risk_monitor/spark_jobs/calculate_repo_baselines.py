from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, count
import os

db_host = os.getenv("POSTGRES_HOST")
db_port = os.getenv("POSTGRES_PORT")
db_name = os.getenv("POSTGRES_DB")
db_user = os.getenv("POSTGRES_USER")
db_password = os.getenv("POSTGRES_PASSWORD")

spark = SparkSession.builder.appName("CalculateRepoBaselines").getOrCreate()

push_events = spark.read.json("/data/raw/github-archive/2024-12-01-0.json")
push_events = push_events.filter(col("type") == "PushEvent")

push_events = push_events.select(
    col("repo.name").alias("repo"),
    col("payload.size").alias("num_commits")
)

baselines = push_events.groupBy("repo").agg(
    avg("num_commits").alias("avg_commits"),
    stddev("num_commits").alias("stddev_commits"),
    count("*").alias("total_pushes")
)

baselines.show(10, truncate=False)
print(f"\nTotal repos analyzed: {baselines.count()}")

jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
connection_properties = {
    "user": db_user,
    "password": db_password,
}

baselines.write.mode("overwrite").option("driver", "org.postgresql.Driver").jdbc(
    url=jdbc_url,
    table="repo_baselines",
    properties=connection_properties
)

spark.stop()