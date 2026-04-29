from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def run_transformations():
    spark = SparkSession.builder.appName("SocialAnalytics").getOrCreate()
    
    df = spark.read.parquet("data/staging/staged_events.parquet")
    
    dim_users = df.select("user_id", "metadata.region").distinct() \
                  .withColumn("user_segment", F.when(F.col("region") == "US", "Domestic").otherwise("International"))

    fact_events = df.select(
        "event_id",
        "user_id",
        "event_type",
        "content_id",
        F.col("timestamp").alias("event_time"),
        F.date_format("timestamp", "yyyyMMdd").alias("date_key")
    )

    dim_users_pd = dim_users.toPandas()
    fact_events_pd = fact_events.toPandas()

    dim_users_pd.to_parquet("data/warehouse/dim_users.parquet", index=False)
    fact_events_pd.to_parquet("data/warehouse/fact_events.parquet", index=False)

    print("Transformations complete. Star schema written to warehouse.")
    spark.stop()

if __name__ == "__main__":
    run_transformations()