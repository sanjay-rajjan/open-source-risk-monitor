from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("ExtractMemberEvents").getOrCreate()

df = spark.read.json("data/raw/github-archive/2024-12-01-0.json")

member_events = df.filter(col("type") == "MemberEvent")

simplified = member_events.select(
    col("id"), col("actor.login").alias("actor"),
    col("repo.name").alias("repo"), col("created_at"),
    col("payload.member.login").alias("new_member"),
    col("payload.action").alias("action")
)

print("Member Events (Maintainer Changes)")
simplified.show(10, truncate=False)

print(f"\nTotal MemberEvents: {simplified.count()}")

spark.stop()