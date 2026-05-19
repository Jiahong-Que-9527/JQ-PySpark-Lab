# %% [markdown]
# # Module 02: 过滤、聚合 & SQL API
#
# **学完之后你能做到**：
#
# - 用 `filter` / `where` 写复杂的多条件过滤
# - 用 `groupBy().agg()` 做单列和多列聚合
# - 熟练用 `F.count` / `F.sum` / `F.avg` / `F.min` / `F.max` / `F.countDistinct`
# - **DataFrame API ↔ Spark SQL** 自由切换（核心：理解它们其实是同一个东西的两套写法）
# - 用 `explain()` 验证 DataFrame 和 SQL 生成的物理计划**完全一致**
#
# **生产场景对应**：
# 聚合是 ETL 的核心动作（"按地区算日销售"、"按用户算 7 日留存"）。
# 不少团队的 ETL 是 SQL 主导（DBT 风格），不少团队是 DataFrame 主导（PySpark 脚本）——
# 真正能在两边切换的人才是值钱的。

# %%
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, DateType
import datetime as dt
import random

spark = (SparkSession.builder
    .appName("lesson-02-filter-agg-sql")
    .master("spark://spark-master:7077")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate())

print(f"Spark {spark.version}")

# %% [markdown]
# ## 1. 准备一个稍大点的数据集
#
# 用合成的"订单"数据，故意制造一些可观察的现象（多个地区、不同金额段、有 null）。

# %%
random.seed(42)
regions = ["north", "south", "east", "west", "central"]
products = ["A", "B", "C", "D"]
n_rows = 50_000

rows = []
for i in range(n_rows):
    rows.append((
        i,                                              # order_id
        random.choice(regions),                         # region
        random.choice(products),                        # product
        random.choice([None, round(random.uniform(5, 500), 2)]),  # amount, 50% null
        dt.date(2026, 1, 1) + dt.timedelta(days=random.randint(0, 30)),  # order_date
    ))

orders_schema = StructType([
    StructField("order_id",   LongType(),   False),
    StructField("region",     StringType(), False),
    StructField("product",    StringType(), False),
    StructField("amount",     DoubleType(), True),
    StructField("order_date", DateType(),   False),
])

orders = spark.createDataFrame(rows, schema=orders_schema)
orders.cache()  # 我们要复用它好几次，先缓存（Module 08 详细讲 cache）
print(f"行数 = {orders.count():,}")
orders.show(5)

# %% [markdown]
# ## 2. 过滤 (filter / where)
#
# `filter` 和 `where` 是**完全等价**的别名——`where` 是为了让习惯 SQL 的人觉得自然。
# 用哪个看团队风格，但**一个文件里别混用**。

# %% [markdown]
# ### 2.1 基础过滤

# %%
# 单条件
orders.filter(F.col("region") == "north").show(3)

# 多条件——用 & | ~，每个条件**必须**带括号
orders.filter((F.col("region") == "north") & (F.col("amount") > 100)).show(3)

# 等价 SQL 风格字符串
orders.where("region = 'north' AND amount > 100").show(3)

# %% [markdown]
# > ⚠️ **超常见坑**：写 `F.col("a") == 1 & F.col("b") == 2` 是错的——Python 运算符
# > 优先级会把它解析成 `F.col("a") == (1 & F.col("b")) == 2`，结果完全不是你要的。
# > **每个布尔表达式都加括号**。

# %% [markdown]
# ### 2.2 复杂过滤模式

# %%
# IN
orders.filter(F.col("region").isin("north", "south")).show(3)

# BETWEEN
orders.filter(F.col("amount").between(100, 200)).show(3)

# LIKE
orders.filter(F.col("product").like("A%")).show(3)

# NULL 处理
orders.filter(F.col("amount").isNull()).count()      # 多少行 amount 缺失？
orders.filter(F.col("amount").isNotNull()).count()   # 多少行有 amount？

# %% [markdown]
# ### 2.3 过滤的执行顺序很重要
#
# **永远先 filter 再 join、再 agg**——这是 Spark 性能的第一条铁律。
# Catalyst 优化器**会自动做下推**（predicate pushdown），但你显式写在前面更稳。

# %%
# 反面教材（虽然 Catalyst 会优化掉，但人类读不清）
bad = orders.groupBy("region").agg(F.sum("amount").alias("total")).filter(F.col("region") == "north")

# 正面教材
good = orders.filter(F.col("region") == "north").groupBy("region").agg(F.sum("amount").alias("total"))

# 看物理计划，两者一样（Catalyst 把 filter 推到了 agg 之前）
print("=== bad plan ===")
bad.explain()
print("=== good plan ===")
good.explain()

# %% [markdown]
# 看到了吗？两个的物理计划末尾都是 `Filter (region = north)` 紧跟 `Scan`——
# Catalyst 已经把 filter 下推到读数据之后立刻执行。但**写代码时按"先 filter"的顺序**，
# 出 bug 时不容易翻车，组里的人读代码也更顺。

# %% [markdown]
# ## 3. 聚合 (groupBy + agg)
#
# 聚合 = 把多行合成一行。这是**几乎所有 ETL 的核心操作**。

# %% [markdown]
# ### 3.1 单列分组、单个指标

# %%
orders.groupBy("region").count().show()
orders.groupBy("region").sum("amount").show()       # 注意：null 会被自动跳过
orders.groupBy("region").avg("amount").show()

# %% [markdown]
# ### 3.2 用 .agg() 控制多个指标 + 起别名

# %%
agg_df = (orders
    .groupBy("region")
    .agg(
        F.count("*").alias("n_orders"),                          # 行数
        F.count("amount").alias("n_with_amount"),                # 非 null 的 amount 行数
        F.countDistinct("product").alias("n_distinct_products"), # 去重计数
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"),
        F.min("amount").alias("min_amount"),
        F.max("amount").alias("max_amount"),
    )
    .orderBy("region")
)
agg_df.show()

# %% [markdown]
# > **关键区别**：
# > - `F.count("*")` 数行（包含 null 行）
# > - `F.count("amount")` 数 amount 列**非 null** 的行
# > - 第一次写聚合时这里很容易出 bug：以为算了总订单数，其实少算了 amount 为 null 的

# %% [markdown]
# ### 3.3 多列分组

# %%
(orders
    .groupBy("region", "product")
    .agg(F.sum("amount").alias("total"), F.count("*").alias("n"))
    .orderBy("region", "product")
    .show(20)
)

# %% [markdown]
# ### 3.4 条件聚合 (conditional agg)
#
# 经常需要"满足条件的子集再聚合"——比如"每个地区里大额订单数"。

# %%
# 写法 A：用 when 把不满足条件的变 null，count("amount")
(orders
    .groupBy("region")
    .agg(
        F.count("*").alias("total"),
        F.count(F.when(F.col("amount") > 100, F.col("amount"))).alias("big_orders"),
    )
    .show()
)

# 写法 B：用 sum + bool 表达式（更直白）
(orders
    .groupBy("region")
    .agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("amount") > 100, 1).otherwise(0)).alias("big_orders"),
    )
    .show()
)

# %% [markdown]
# 两种写法完全等价。生产里我**推荐写法 B**——意图最清楚（"对每行算 1 或 0，再求和"）。

# %% [markdown]
# ## 4. Spark SQL API：另一种平等公民
#
# 上面所有 DataFrame API 操作，**都可以用纯 SQL 写**，**性能完全一致**——
# 因为 Catalyst 把两种 API 编译成同一棵物理计划树。
#
# 步骤：
#
# 1. `df.createOrReplaceTempView("name")` 把 DataFrame 注册成 SQL 临时表
# 2. `spark.sql("SELECT ...")` 写 SQL，返回 DataFrame
# 3. 后续可以继续用 DataFrame API 链下去

# %%
orders.createOrReplaceTempView("orders")

# 用 SQL 写跟上面一样的聚合
sql_agg = spark.sql("""
    SELECT
        region,
        COUNT(*) AS n_orders,
        COUNT(amount) AS n_with_amount,
        COUNT(DISTINCT product) AS n_distinct_products,
        SUM(amount) AS total_amount,
        AVG(amount) AS avg_amount,
        MIN(amount) AS min_amount,
        MAX(amount) AS max_amount
    FROM orders
    GROUP BY region
    ORDER BY region
""")
sql_agg.show()

# %% [markdown]
# ### 4.1 验证 DataFrame API 和 SQL 的执行计划相同

# %%
print("=== DataFrame API plan ===")
agg_df.explain()
print("\n=== SQL plan ===")
sql_agg.explain()

# %% [markdown]
# 物理计划应该几乎一样（可能列名/顺序稍异）——Catalyst 是同一个。这意味着
# **选 SQL 还是 DataFrame 只是可读性 / 团队习惯问题，不存在"哪个更快"**。

# %% [markdown]
# ### 4.2 在 SQL 里能做的，DataFrame API 都能做（反之亦然）

# %%
# SQL 子查询
spark.sql("""
    SELECT region, total
    FROM (
        SELECT region, SUM(amount) AS total FROM orders GROUP BY region
    )
    WHERE total > 500000
    ORDER BY total DESC
""").show()

# 同样的事用 DataFrame API
(orders
    .groupBy("region")
    .agg(F.sum("amount").alias("total"))
    .filter(F.col("total") > 500000)
    .orderBy(F.col("total").desc())
    .show()
)

# %% [markdown]
# ### 4.3 混合写法（生产里非常常见）
#
# 不必非黑即白——可以**用 SQL 做最直观的部分，再切回 DataFrame API 做链式操作**。

# %%
recent_orders = spark.sql("""
    SELECT region, product, amount
    FROM orders
    WHERE order_date >= DATE '2026-01-15'
      AND amount IS NOT NULL
""")

# 切回 DataFrame API 继续
(recent_orders
    .groupBy("region")
    .agg(F.sum("amount").alias("total"), F.avg("amount").alias("avg"))
    .orderBy(F.col("total").desc())
    .show()
)

# %% [markdown]
# > **团队风格建议**：探索性分析、写一次性查询 → SQL 更快；
# > 生产 ETL pipeline、有大量函数复用 / 测试 → DataFrame API 更可维护。

# %% [markdown]
# ## 5. agg 后再处理：派生指标
#
# 聚合完得到的 DataFrame 可以**继续 select/withColumn**，加派生指标。

# %%
(orders
    .groupBy("region")
    .agg(
        F.count("*").alias("n_orders"),
        F.sum("amount").alias("total_amount"),
        F.count("amount").alias("n_with_amount"),
    )
    .withColumn("avg_amount",      F.col("total_amount") / F.col("n_with_amount"))
    .withColumn("amount_coverage", F.col("n_with_amount") / F.col("n_orders"))
    .orderBy(F.col("avg_amount").desc())
    .show()
)

# %% [markdown]
# 上面那个 `amount_coverage` 列——"该地区有多少比例的订单含有金额"——是个非常典型的
# **数据质量监控指标**。生产里你会经常加这种"完整性"列。

# %% [markdown]
# ## 6. 练习

# %% [markdown]
# ### 练习 1（过滤）
#
# 用 `orders` DataFrame，找出**满足下面所有条件的订单**：
#
# - region 是 "north" 或 "east"
# - amount 不为 null 且 >= 200
# - order_date 在 2026-01-10 到 2026-01-20 之间（含两端）
#
# 用 DataFrame API 和 SQL 各写一遍，确认结果数量一致。

# %%
# TODO: DataFrame API

# TODO: SQL

# %% [markdown]
# ### 练习 2（多列聚合 + 派生）
#
# 按 `(region, product)` 分组，输出以下列：
#
# - `n_orders`：订单数
# - `total`：金额合计（null 视为 0）
# - `avg`：非空金额的平均值
# - `pct_with_amount`：该组里"有金额的订单"占比（保留 2 位小数）
#
# 按 `(region, product)` 排序输出。

# %%
# TODO

# %% [markdown]
# ### 练习 3（条件聚合）
#
# 一次性算出每个 region 的：
#
# - 总订单数
# - 大额订单数（amount >= 300）
# - 大额订单金额合计
# - 大额订单占比（大额订单数 / 总订单数）
#
# 要求**只 groupBy 一次**——别 groupBy 两遍再 join，那样性能差。

# %%
# TODO

# %% [markdown]
# ### 练习 4（SQL 与 DataFrame API 互转）
#
# 把下面这段 SQL 改写成等价的 DataFrame API 代码：
#
# ```sql
# SELECT
#     product,
#     COUNT(*) AS total,
#     COUNT(DISTINCT region) AS regions_covered,
#     ROUND(AVG(amount), 2) AS avg_amount
# FROM orders
# WHERE amount IS NOT NULL
# GROUP BY product
# HAVING COUNT(*) > 5000
# ORDER BY avg_amount DESC
# ```

# %%
# TODO

# %% [markdown]
# ## 7. 去 Spark UI 看什么
#
# 1. http://localhost:8080 → 进入 `lesson-02-filter-agg-sql` application
# 2. 找一个**包含 `groupBy`** 的 Job（比如上面的 `agg_df.show()`）：
#    - 应该有 **2 个 stage**——这是因为 `groupBy` 触发了 **shuffle**
#    - Stage 0：读数据 + 在每个分区里做"局部 agg"（partial aggregation）
#    - Stage 1：把局部结果 shuffle 到一起，做最终 agg
# 3. 点进 stage，看 **"Shuffle Read"** 和 **"Shuffle Write"** 列——
#    这是 wide transformation 的标志。Module 06 会专门讲这个。
# 4. 对比：纯 `filter` 的 Job 通常只有 **1 个 stage**，因为 filter 是窄变换。

# %% [markdown]
# ## 8. 生产关联
#
# 这一节学的东西在工作里对应：
#
# - **每日报表**：`SELECT region, SUM(amount) ... GROUP BY region, DATE(order_date)`
#   ——这就是聚合的最常见形态
# - **数据质量监控**：`COUNT(*) - COUNT(col)` 算 null 数、`COUNT(DISTINCT)` 算基数变化
# - **A/B 测试分析**：用条件聚合算"实验组转化率 vs 对照组转化率"
# - **DBT-like SQL ETL**：实际项目里大量 SQL 通过 PySpark 跑——`spark.sql(...)` 是
#   你跟 DBT-ist 同事协作的接口

# %% [markdown]
# ## 9. 收尾

# %%
orders.unpersist()  # 释放 cache
spark.stop()
print("Lesson 02 done.")

# %% [markdown]
# ---
#
# **下一节**：`03_joins.py`——6 种 join、join 失败的常见原因、什么时候用哪种。
