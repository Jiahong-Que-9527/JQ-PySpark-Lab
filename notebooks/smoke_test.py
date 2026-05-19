"""Smoke test for PySpark local cluster environment.

Run from host:
    make test
Or directly:
    docker compose exec jupyter python /home/jovyan/work/smoke_test.py

Verifies:
1. SparkSession connects to the standalone cluster (NOT local mode)
2. DataFrame ops execute and shuffle correctly
3. At least one worker executor is registered
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
    .appName("smoke-test")
    .master("spark://spark-master:7077")
    .config("spark.executor.memory", "1g")
    .config("spark.executor.cores", "1")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate())

print(f"Spark version:  {spark.version}")
print(f"Master:         {spark.sparkContext.master}")
print(f"Application ID: {spark.sparkContext.applicationId}")

assert spark.sparkContext.master.startswith("spark://"), \
    "FAIL: Must connect to standalone cluster, not local mode"

# Generate partitioned data so stages/tasks are visible in Spark UI
df = (spark.range(0, 1_000_000, numPartitions=8)
      .toDF("n")
      .withColumn("squared", F.col("n") * F.col("n"))
      .withColumn("bucket", F.col("n") % 10))

# Force a shuffle (groupBy) to make the UI interesting
result = (df.groupBy("bucket")
          .agg(F.count("*").alias("cnt"),
               F.sum("squared").alias("sum_squared"))
          .orderBy("bucket")
          .collect())

print(f"\n{len(result)} buckets computed:")
for row in result:
    print(f"  bucket={row['bucket']:>2}  cnt={row['cnt']:>7}  sum_squared={row['sum_squared']}")

# Verify at least one worker executor is registered (driver + worker)
# getExecutorInfos is Java-only; access via JVM bridge
jvm_tracker = spark.sparkContext._jsc.sc().statusTracker()
executors = list(jvm_tracker.getExecutorInfos())
print(f"\nExecutors registered: {len(executors)}")
for ex in executors:
    print(f"  host={ex.host()}  port={ex.port()}  runningTasks={ex.numRunningTasks()}")

assert len(executors) >= 2, \
    f"FAIL: Expected driver + ≥1 worker executor, got {len(executors)}"

print("\n✓ Smoke test PASSED")
spark.stop()
