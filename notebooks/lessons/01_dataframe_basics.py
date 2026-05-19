# %% [markdown]
# # Module 01: DataFrame & Schema 基础
#
# **学完之后你能做到**：
#
# - 用 3 种方式创建 DataFrame（手写数据、`spark.range`、读文件）
# - **显式声明 schema**（这是生产环境的强制要求，别让 Spark 自己 infer）
# - 熟练做 column 操作：`select` / `withColumn` / `cast` / `alias` / `drop`
# - 用 `.na` API 处理 null
# - 用 `.printSchema()` / `.show()` / `.explain()` 调试 DataFrame
#
# **生产场景对应**：
# 这一节是所有后续模块的基石。生产里 90% 的 PySpark 代码都是这些操作的组合。
# 真正的难点不是 API 本身，而是**对脏数据有警觉**——80% 的"数据问题工单"
# 都跟这一节里讲的 schema 推断 / null / cast 有关。

# %% [markdown]
# ## 1. 准备：创建 SparkSession
#
# 每个 lesson 都自己建一次 session——故意的，让你看清"连到集群"这一步永远是入口。
# 后面真做项目时你会把这段抽出来，但学习阶段先重复写。

# %%
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType, BooleanType, TimestampType, DateType,
)

spark = (SparkSession.builder
    .appName("lesson-01-dataframe-basics")
    .master("spark://spark-master:7077")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate())

print(f"Spark {spark.version} | master={spark.sparkContext.master}")

# %% [markdown]
# > **Spark UI 提示**：打开 http://localhost:8080，你应该看到一个名叫
# > `lesson-01-dataframe-basics` 的 application 处于 "Running" 状态。后面每跑一段
# > 触发 action 的代码，UI 的 "Completed Jobs" 数会涨。

# %% [markdown]
# ## 2. 创建 DataFrame 的 3 种方式
#
# ### 2.1 从 Python 数据（开发/测试用）

# %%
# 最简单：用 createDataFrame + 列名列表
orders_simple = spark.createDataFrame(
    [
        (1, "alice", 99.5, "2026-01-15"),
        (2, "bob",   12.0, "2026-01-15"),
        (3, "carol", None, "2026-01-16"),  # 故意留个 null 给你看
    ],
    ["order_id", "user", "amount", "order_date"],
)
orders_simple.show()
orders_simple.printSchema()

# %% [markdown]
# **注意看 printSchema 的输出**：
#
# - `amount` 被推断成 `double`（碰到 None 也能装）
# - `order_date` 被推断成 `string`——**这就是 schema 推断的坑**：日期变成字符串，
#   后续算"昨天到今天"会出错
#
# 这就是为什么生产环境要显式声明 schema。

# %% [markdown]
# ### 2.2 `spark.range`：造大量数据测分布式行为

# %%
big = spark.range(0, 1_000_000, numPartitions=8).toDF("n")
print(f"行数 = {big.count():,}")
print(f"分区数 = {big.rdd.getNumPartitions()}")

# %% [markdown]
# `spark.range` 是练习时的好朋友——你想要多少行就有多少行，而且能控制
# `numPartitions`。后面讲 shuffle / skew 都会用它造数据。

# %% [markdown]
# ### 2.3 读文件（生产里 99% 的入口）
#
# 我们先把 DataFrame 写一个 Parquet 文件，再读回来。
#
# > **重要的生产细节**：写出去的路径**必须所有 executor 都能访问**。本环境里
# > `/data` 这个目录在 `docker-compose.yml` 里被挂到了 master / worker / jupyter
# > 三个容器上——它在**真实生产里对应的就是 S3 / HDFS**（共享对象存储）。
# > **不要**用 `/tmp/...` 或 `~/...`：driver 写到自己的本地盘，executor 在另一台机器
# > 上读不到，会报 "Unable to infer schema" 或 "Path does not exist"。

# %%
shared_path = "/data/lesson_01_orders.parquet"
orders_simple.write.mode("overwrite").parquet(shared_path)

# 读回来
orders_loaded = spark.read.parquet(shared_path)
orders_loaded.show()
orders_loaded.printSchema()

# %% [markdown]
# **Parquet 自带 schema**，所以读回来的列类型跟写出去时一致——这是 Parquet 比 CSV 强的
# 一个根本原因。CSV 没 schema，必须 infer，会慢、会错。
#
# > **生产经验**：能用 Parquet 别用 CSV。如果上游只给 CSV，**你的第一步**应该是显式
# > 声明 schema 后转成 Parquet 落盘，再做后续处理。

# %% [markdown]
# ## 3. 显式 Schema：生产 PySpark 的硬规矩
#
# 上面 `orders_simple` 里日期被当成字符串，是因为 `createDataFrame` 自己 infer 的。
# 下面我们**显式**声明一个 schema 重新建一次。

# %%
orders_schema = StructType([
    StructField("order_id",   LongType(),    nullable=False),
    StructField("user",       StringType(),  nullable=False),
    StructField("amount",     DoubleType(),  nullable=True),   # 允许 null
    StructField("order_date", DateType(),    nullable=False),
])

import datetime as dt
orders = spark.createDataFrame(
    [
        (1, "alice", 99.5, dt.date(2026, 1, 15)),
        (2, "bob",   12.0, dt.date(2026, 1, 15)),
        (3, "carol", None, dt.date(2026, 1, 16)),
    ],
    schema=orders_schema,
)
orders.printSchema()
orders.show()

# %% [markdown]
# 现在 `order_date` 是真正的 `date` 类型，可以直接做 `date_diff`、`year()` 这些。
#
# **为什么生产里必须显式声明 schema？**
#
# 1. **正确性**：避免 `"123"` 当字符串处理，或者一个字段在不同文件里类型不一致
# 2. **性能**：CSV/JSON 的 schema 推断需要**额外扫一遍数据**——50GB 的输入直接翻倍
# 3. **稳定性**：上游加了一列、改了类型，你的 job 应该**显式失败**而不是悄悄"自适应"

# %% [markdown]
# ## 4. Column 操作
#
# Column 是 PySpark 里"对一列做计算"的抽象。引用列有 4 种写法，都等价：
#
# ```python
# orders.amount               # 属性
# orders["amount"]            # 字典
# F.col("amount")             # functions.col
# "amount"                    # 字符串（部分 API 接受）
# ```
#
# 工程里**推荐用 `F.col("amount")`**，理由：
# - 跟列名是字符串变量时统一写法（`F.col(col_name)`）
# - 避免列名和 Python 关键字冲突（`F.col("class")`）
# - 一眼看出"这是一个 Column 表达式"

# %% [markdown]
# ### 4.1 select / selectExpr

# %%
orders.select("order_id", "user").show()

# selectExpr 直接写 SQL 片段——熟练 SQL 的人会喜欢
orders.selectExpr("order_id", "user", "amount * 1.1 AS amount_with_tax").show()

# %% [markdown]
# ### 4.2 withColumn / withColumnRenamed
#
# `withColumn` 加一列或**替换**一列（同名时替换）。`withColumnRenamed` 改列名。

# %%
orders_priced = (orders
    .withColumn("amount_with_tax", F.col("amount") * 1.1)
    .withColumn("is_big_order",    F.col("amount") > 50)
    .withColumnRenamed("user", "username")
)
orders_priced.show()

# %% [markdown]
# > ⚠️ **常见陷阱**：在循环里大量 `withColumn` 会构建超长的逻辑计划，导致编译变慢。
# > 一次性 `select` 多列更高效：
# > ```python
# > orders.select("*", (F.col("amount")*1.1).alias("amount_with_tax"), ...)
# > ```

# %% [markdown]
# ### 4.3 drop / alias

# %%
# drop 一列或多列
orders_priced.drop("is_big_order").show(3)

# alias 给整个 DataFrame 起别名（join 时常用，看 Module 03）
o = orders.alias("o")
o.select(F.col("o.user"), F.col("o.amount")).show(3)

# %% [markdown]
# ## 5. 类型转换 (cast)
#
# 真实世界的数据经常类型不对——金额字段是字符串，日期是 `"20260115"` 这种格式。
# 用 `.cast(...)` 转换。

# %%
# 假设上游把所有列都给成 string（CSV 常见）
dirty_raw = spark.createDataFrame(
    [
        ("1", "alice", "99.5",  "2026-01-15"),
        ("2", "bob",   "12.0",  "2026-01-15"),
        ("3", "carol", "abc",   "2026-01-16"),  # 故意写一个非法数字
        ("4", "dave",  None,    "2026-01-17"),
    ],
    ["order_id", "user", "amount", "order_date"],
)

# 显式 cast 到正确类型
cleaned = (dirty_raw
    .withColumn("order_id",   F.col("order_id").cast(LongType()))
    .withColumn("amount",     F.col("amount").cast(DoubleType()))     # "abc" → null
    .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
)
cleaned.printSchema()
cleaned.show()

# %% [markdown]
# **看上面的输出**：`"abc"` 被 cast 成了 `null`。这是 Spark 的**默认行为**——
# 不抛错、不报警，悄悄变 null。
#
# 生产里你想要哪种行为？两种思路：
#
# 1. **宽松模式**：让脏值变 null，下游用 `na.drop()` / `isNotNull()` 过滤
# 2. **严格模式**：把"应该是数字但 cast 失败"的行单独存到一个"坏数据表"
#
# 下面这个 pattern 你以后会反复用到——**带"坏数据隔离"的 cast**：

# %%
# 在 cast 之前打标记，cast 之后能区分"原本就是 null" vs "cast 失败"
flagged = (dirty_raw
    .withColumn("amount_raw", F.col("amount"))
    .withColumn("amount",     F.col("amount").cast(DoubleType()))
    .withColumn("amount_cast_failed",
        F.col("amount").isNull() & F.col("amount_raw").isNotNull())
)
flagged.show()

# 好数据
flagged.filter(~F.col("amount_cast_failed")).show()
# 坏数据
flagged.filter(F.col("amount_cast_failed")).show()

# %% [markdown]
# ## 6. Null 处理
#
# Spark 里 null 的语义跟 SQL 一样：跟任何值比较都是 null（不是 false）。
# 这是 ETL 出 bug 的高频源头。

# %%
# 测试用数据
df = spark.createDataFrame(
    [
        (1, "alice", 100),
        (2, "bob",   None),
        (3, None,    50),
        (4, "carol", None),
    ],
    ["id", "name", "score"],
)

# 6.1 isNull / isNotNull
df.filter(F.col("score").isNull()).show()
df.filter(F.col("score").isNotNull()).show()

# 6.2 .na.fill —— 填默认值
df.na.fill({"score": 0, "name": "unknown"}).show()

# 6.3 .na.drop —— 删带 null 的行
df.na.drop().show()                    # 任何一列有 null 就删
df.na.drop(subset=["score"]).show()    # 只看 score

# 6.4 coalesce —— 取第一个非 null
df.withColumn("score_or_zero", F.coalesce(F.col("score"), F.lit(0))).show()

# 6.5 when/otherwise —— 条件赋值（SQL 的 CASE WHEN）
df.withColumn(
    "score_bucket",
    F.when(F.col("score").isNull(), "missing")
     .when(F.col("score") >= 80,   "high")
     .when(F.col("score") >= 50,   "mid")
     .otherwise("low")
).show()

# %% [markdown]
# > **生产经验**：`coalesce` 和 `when/otherwise` 是处理 null 的两把瑞士军刀。
# > 用 `when` 而不是 Python `if`——`if` 是 driver 端逻辑，`when` 是分布式列表达式。

# %% [markdown]
# ## 7. 调试与观察
#
# 这些方法本身不"计算什么"，但每天都会用到：

# %%
# show: 默认 20 行，长字符串会截断
orders.show()
orders.show(n=2, truncate=False)

# printSchema: 看列类型——每次新 DataFrame 都该先 printSchema
orders.printSchema()

# describe / summary: 数值列统计
orders.describe(["amount"]).show()
orders.summary("count", "min", "max", "50%", "75%").show()

# count: 注意，这是个 action！会触发完整计算
print(f"orders 行数: {orders.count()}")

# explain: 看物理执行计划（Module 05 会专门讲）
orders.filter(F.col("amount") > 50).explain()

# %% [markdown]
# > ⚠️ **避免在大 DataFrame 上 `.toPandas()` 或 `.collect()`**——会把所有数据
# > 拉到 driver，OOM 风险高。生产里只在**已经聚合到小规模**的结果上用。

# %% [markdown]
# ## 8. 练习
#
# 把下面的练习按顺序做完。每题都先想清楚再写代码——
# 你以后做生产 ETL，思考时间和写代码时间应该是 3:1。

# %% [markdown]
# ### 练习 1（送分题）
# 用 `spark.createDataFrame` 创建以下 DataFrame：
#
# | user_id (long) | event_type (string) | ts (timestamp)           |
# |---|---|---|
# | 1001 | "click" | 2026-05-19 10:00:00 |
# | 1002 | "view"  | 2026-05-19 10:01:30 |
# | 1003 | null    | 2026-05-19 10:02:00 |
#
# 要求：**显式声明 schema**（不要让 Spark infer）。

# %%
# TODO: 在这里写你的代码
# events_schema = StructType([...])
# events = spark.createDataFrame([...], schema=events_schema)
# events.printSchema()
# events.show()

# %% [markdown]
# ### 练习 2（cast + 坏数据隔离）
#
# 给你一个"脏" DataFrame：

# %%
raw = spark.createDataFrame(
    [
        ("u1", "10.5",  "2026-05-19"),
        ("u2", "8",     "2026-05-19"),
        ("u3", "free",  "2026-05-19"),   # 故意写错
        ("u4", None,    "2026-05-20"),
        ("u5", "12.0",  "not-a-date"),   # 故意写错
    ],
    ["user", "price", "dt"],
)

# %% [markdown]
# 要求：
#
# 1. 把 `price` cast 成 `DoubleType`，`dt` cast 成 `DateType`（格式 `yyyy-MM-dd`）
# 2. 加一个布尔列 `has_bad_data`：当 `price` 或 `dt` 在 cast 之后是 null、但原始值
#    不是 null 时为 True
# 3. 输出"好数据" DataFrame（`has_bad_data = False` 的行）

# %%
# TODO: 在这里写你的代码

# %% [markdown]
# ### 练习 3（null 处理）
#
# 给你这个 DataFrame：

# %%
sales = spark.createDataFrame(
    [
        (1, "A", 100, None),
        (2, "A", None, 50),
        (3, "B", 80,   30),
        (4, "B", None, None),
        (5, "C", 200,  100),
    ],
    ["id", "region", "revenue", "cost"],
)

# %% [markdown]
# 要求：
#
# 1. 算一个 `profit` 列 = `revenue - cost`
# 2. 任何一边为 null 时，用 0 替代后再算（这是业务约定）
# 3. 再加一列 `profit_status`：profit > 50 → "good"，0~50 → "ok"，<= 0 → "bad"
# 4. 用 `when/otherwise` 实现，不要用 Python `if`

# %%
# TODO: 在这里写你的代码

# %% [markdown]
# ## 9. 去 Spark UI 看什么
#
# 1. 打开 http://localhost:8080，点进 `lesson-01-dataframe-basics` application
# 2. 看 **"Jobs"** 标签——每个 `.show()` / `.count()` / `.write` 会产生 1 个或多个 Job
# 3. 点进任意一个 Job，看 **"Stages"**：
#    - 简单的 `select/filter` 通常只有 **1 个 stage**（窄变换，无 shuffle）
#    - 后面 Module 02/03 涉及 `groupBy`/`join` 时会出现 **2+ 个 stage**（宽变换，有 shuffle）
# 4. 点进 stage，看 **"Tasks"**：每个 task 对应一个分区
#
# **这一节里你已经能观察到的关键现象**：
# - `orders` 这个 4 行的 DataFrame，count 时只有 1 个 stage、若干个 task（取决于
#   默认分区数）
# - `big = spark.range(..., numPartitions=8)`，count 时一个 stage 里有 **8 个 task**

# %% [markdown]
# ## 10. 收尾

# %%
spark.stop()
print("Lesson 01 done.")

# %% [markdown]
# ---
#
# **下一节**：`02_filtering_aggregation_sql.py`——过滤、分组聚合，以及 DataFrame API
# 和 Spark SQL API 之间的来回切换。
