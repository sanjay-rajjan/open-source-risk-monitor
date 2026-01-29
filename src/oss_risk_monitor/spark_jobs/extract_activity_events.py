from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("ExtractActivityEvents").getOrCreate()

df = spark.read.json("/data/raw/github-archive/2024-12-01-0.json")

issues_and_prs = df.filter((col("type") == "IssuesEvent") | (col("type") == "PullRequestEvent"))

simplified = issues_and_prs.select(
    col("id"), col("type"),
    col("actor.login").alias("actor"), col("repo.name").alias("repo"),
    col("created_at"), col("payload.action").alias("action"),
    col("payload.issue.number").alias("issue_number"),
    col("payload.pull_request.number").alias("pr_number")
)

print("Activity Events (Issues & PRs)")
simplified.show(10, truncate=False)

print(f"\nTotal Activity Events: {simplified.count()}")

spark.stop()