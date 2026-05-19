# %% [markdown]
# # Module 03: Join 全家族
#
# **学完之后你能做到**：
#
# - 熟练用 6 种 join：`inner` / `left` / `right` / `full` / `left_semi` / `left_anti`
# - 用 alias 写**自 join**（同一个表跟自己 join）
# - 诊断 join 出错的 4 大常见原因：**key 类型不匹配、null 不会相等、重复行导致行数爆炸、列名冲突**
# - **看懂 `explain()` 里的 join 策略**：BroadcastHashJoin vs SortMergeJoin
#
# **生产场景对应**：
# Join 是 PySpark 里**最容易写错也最容易出性能问题**的操作。生产 ETL 出 bug，60% 跟 join
# 相关：行数突然翻倍、key 类型上游改了、null 行莫名其妙消失。这一节是绕不过去的硬骨头。

# %%
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType

spark = (SparkSession.builder
    .appName("lesson-03-joins")
    .master("spark://spark-master:7077")
    .config("spark.sql.shuffle.partitions", "8")
    # 先关掉 broadcast 自动优化，让我们看清原始 join 行为；后面再开
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .getOrCreate())

print(f"Spark {spark.version}")

# %% [markdown]
# ## 1. 准备数据：两个表 + 一个故意制造问题的版本

# %%
# 用户表（小，1000 行）
users = spark.createDataFrame(
    [
        (1, "alice",   "north"),
        (2, "bob",     "south"),
        (3, "carol",   "east"),
        (4, "dave",    "west"),
        (5, "eve",     "north"),
        (6, "frank",   None),         # 注意：region 为 null
    ],
    ["user_id", "name", "region"],
)

# 订单表
orders = spark.createDataFrame(
    [
        (101, 1, 99.5),
        (102, 1, 12.0),    # alice 有 2 个订单
        (103, 2, 45.0),
        (104, 3, 200.0),
        (105, 3, 150.0),   # carol 有 2 个订单
        (106, 99, 30.0),   # user_id=99 在 users 表里**不存在**
        (107, None, 80.0), # user_id 是 null
    ],
    ["order_id", "user_id", "amount"],
)

users.show()
orders.show()

# %% [markdown]
# ## 2. 6 种 Join
#
# 心理模型：把 join 想成"对每个左表的行，去右表找匹配的 key，按 join 类型决定怎么处理"。

# %% [markdown]
# ### 2.1 INNER JOIN — 只保留两边都有的
#
# 最常用，但**最容易让你丢数据而不察觉**。

# %%
inner = users.join(orders, on="user_id", how="inner")
inner.orderBy("order_id").show()

# %% [markdown]
# 注意几点：
# - user_id=4 (dave)、5 (eve)、6 (frank) **消失了**——他们没下单
# - order_id=106 (user_id=99) 也消失了——这个 user_id 在 users 表里不存在
# - order_id=107 (user_id=null) **也消失了**——**null != null in SQL**
#
# **生产里坑的根源**：上游突然有几行 user_id 为 null，inner join 一过这些行就没了。

# %% [markdown]
# ### 2.2 LEFT JOIN — 保留左表全部
#
# 也叫 `left_outer`。

# %%
left = users.join(orders, on="user_id", how="left")
left.orderBy("user_id", "order_id").show()

# %% [markdown]
# 现在 dave、eve、frank 都在，但他们的 `order_id` / `amount` 是 null。
#
# 注意 user_id=99 仍然不在——因为它只在右边的 orders 表里。

# %% [markdown]
# ### 2.3 RIGHT JOIN — 保留右表全部

# %%
right = users.join(orders, on="user_id", how="right")
right.orderBy("order_id").show()

# %% [markdown]
# user_id=99 出现了（但 `name` / `region` 为 null）。
#
# > **风格提示**：生产代码里**几乎不用 right join**——总能写成 `B.join(A, ..., "left")`，
# > 更直观。看到 right join 通常是新手代码的味道。

# %% [markdown]
# ### 2.4 FULL JOIN — 两边都保留
#
# 也叫 `outer` 或 `full_outer`。

# %%
full = users.join(orders, on="user_id", how="full")
full.orderBy("user_id", "order_id").show()

# %% [markdown]
# - dave、eve、frank：有用户没订单 → orders 字段为 null
# - user_id=99：有订单没用户 → users 字段为 null
# - user_id=null：order_id=107 的那行
#
# **生产用途**：经常用在"数据对账"——两边的 union 集合，找出"只在 A 里"和"只在 B 里"的行。

# %% [markdown]
# ### 2.5 LEFT_SEMI — "存在性"过滤（半连接）
#
# 像 `WHERE EXISTS (SELECT 1 FROM right WHERE ...)`。**只返回左表的列**，
# 且只保留"右表有匹配 key"的左表行。

# %%
# 找"下过单"的用户
buyers = users.join(orders, on="user_id", how="left_semi")
buyers.show()

# %% [markdown]
# **看到了吗？没有 order_id、amount 列**——只有左表的列。这是 semi join 的关键特点。
#
# 等价 SQL：
# ```sql
# SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.user_id)
# ```
#
# > **为什么不用 `inner join` + `dropDuplicates`？** 因为 carol 有 2 个订单，inner join
# > 后她在结果里会出现 2 次，必须再去重。`left_semi` 一步到位，**而且性能更好**——
# > 它在找到第一个匹配后就停止扫描右表。

# %% [markdown]
# ### 2.6 LEFT_ANTI — "不存在性"过滤（反连接）
#
# 像 `WHERE NOT EXISTS (...)`。返回**左表里在右表中没有匹配**的行。

# %%
# 找"从没下过单"的用户
non_buyers = users.join(orders, on="user_id", how="left_anti")
non_buyers.show()

# %% [markdown]
# dave、eve、frank——以及 user_id=null 的也算"找不到匹配"。
#
# 等价 SQL：
# ```sql
# SELECT * FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.user_id)
# ```
#
# **生产高频用法**：
# - "本期新用户" = 当前用户表 `left_anti` 上期用户表
# - "异常订单" = 全订单 `left_anti` 已知合法订单 ID 表

# %% [markdown]
# ## 3. Join 语法的几种写法
#
# 上面用的是最简洁的 `on="user_id"`（**左右列名相同**时）。还有几种常见写法：

# %% [markdown]
# ### 3.1 列名不同时：用 `==` 表达式

# %%
# 假设右表里叫 customer_id
orders2 = orders.withColumnRenamed("user_id", "customer_id")

j = users.join(orders2, users.user_id == orders2.customer_id, "inner")
j.show()

# %% [markdown]
# 这种写法会**保留两列 join key**（`user_id` 和 `customer_id`）。
# 用 `select` 或 `drop` 决定保留哪个。

# %% [markdown]
# ### 3.2 多列 join key

# %%
# 造一个按 (region, product) join 的例子
regional_quota = spark.createDataFrame(
    [("north", "A", 1000), ("north", "B", 500), ("south", "A", 800)],
    ["region", "product", "quota"],
)

product_sales = spark.createDataFrame(
    [("north", "A", 950), ("north", "B", 600), ("south", "C", 200), ("north", "A", 100)],
    ["region", "product", "sales"],
)

# on 传一个列名列表
both_keys = product_sales.join(regional_quota, on=["region", "product"], how="left")
both_keys.show()

# %% [markdown]
# ### 3.3 列名冲突的处理
#
# 当两边有同名的**非 key 列**时，join 后会有重复列名——这会让后续 `select` 出错。

# %%
df_a = spark.createDataFrame([(1, "alice", 100)], ["id", "name", "value"])
df_b = spark.createDataFrame([(1, "bob",   200)], ["id", "name", "value"])

# 用 alias 区分两边
joined = (
    df_a.alias("a")
    .join(df_b.alias("b"), on="id", how="inner")
    .select(
        F.col("a.name").alias("a_name"),
        F.col("a.value").alias("a_value"),
        F.col("b.name").alias("b_name"),
        F.col("b.value").alias("b_value"),
    )
)
joined.show()

# %% [markdown]
# > **生产经验**：**永远给 join 的两边起 alias**，列名取舍上写明 `a.xxx` / `b.xxx`。
# > 等出现"Reference 'name' is ambiguous"再回头改更费时间。

# %% [markdown]
# ## 4. 自 join (self join)
#
# 同一个表跟自己 join，**必须用 alias**。常见场景：层级关系（员工-上司）、
# 前后对比（昨天 vs 今天）。

# %%
employees = spark.createDataFrame(
    [
        (1, "Alice",   None),  # CEO
        (2, "Bob",     1),
        (3, "Carol",   1),
        (4, "Dave",    2),
        (5, "Eve",     2),
        (6, "Frank",   3),
    ],
    ["emp_id", "name", "manager_id"],
)

e1 = employees.alias("e")
e2 = employees.alias("m")

(e1.join(e2, F.col("e.manager_id") == F.col("m.emp_id"), "left")
   .select(
       F.col("e.name").alias("employee"),
       F.col("m.name").alias("manager"),
   )
   .orderBy("employee")
   .show()
)

# %% [markdown]
# ## 5. Join 的 4 个常见坑（生产事故根源）

# %% [markdown]
# ### 坑 1：Key 类型不一致
#
# 一个表 user_id 是 `long`，另一个是 `string`——join **不会报错**，但**永远匹配不上**。

# %%
left_long  = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "x"])
right_str  = spark.createDataFrame([("1", "p"), ("2", "q")], ["id", "y"])

print("=== schema 不同 ===")
left_long.printSchema()
right_str.printSchema()

print("=== join 结果 (会得到空？) ===")
left_long.join(right_str, on="id", how="inner").show()

# %% [markdown]
# **真实事故**：上游数据库迁移把 `user_id` 从 BIGINT 改成 VARCHAR，整个 ETL 默默
# 出全空结果，一周后才发现。
#
# **预防**：写 ETL 时 join 之前**显式 cast 双方 key 列到同一类型**，并加 schema
# assertion（Module 10 测试章节会讲）。

# %% [markdown]
# ### 坑 2：Null 不会跟自己相等
#
# 上面例子里已经看到——order 表里 user_id 为 null 的那行，inner join 后消失了。
# **null = null 在 SQL/Spark 里结果是 null，不是 true**。
#
# 如果你的业务里 null 应该匹配 null，用 `eqNullSafe` (`<=>`)：

# %%
left_nulls  = spark.createDataFrame([(1,), (None,), (2,)], ["k"]).toDF("k")
right_nulls = spark.createDataFrame([(1,), (None,)], ["k"])

# 普通 ==
print("=== 普通 join：null 不匹配 ===")
left_nulls.join(right_nulls, left_nulls.k == right_nulls.k, "inner").show()

# eqNullSafe：null 匹配 null
print("=== eqNullSafe：null 匹配 ===")
left_nulls.join(right_nulls, left_nulls.k.eqNullSafe(right_nulls.k), "inner").show()

# %% [markdown]
# ### 坑 3：右表 key 不唯一 → 行数爆炸
#
# 如果右表对一个 key 有多行，inner join 会**对左表的每个匹配行复制多次**。
# 这是"join 完行数突然变 10 倍"的最常见原因。

# %%
# 左表：每个 user_id 1 行
left = spark.createDataFrame([(1, "alice"), (2, "bob")], ["uid", "name"])

# 右表：user_id=1 有 3 行（脏数据 / 多对一关系）
right = spark.createDataFrame(
    [(1, "x"), (1, "y"), (1, "z"), (2, "p")],
    ["uid", "tag"],
)

print(f"left 行数={left.count()}, right 行数={right.count()}")

joined = left.join(right, on="uid", how="inner")
print(f"inner join 后行数={joined.count()}")  # alice 出现 3 次
joined.show()

# %% [markdown]
# **预防 / 诊断**：
#
# - join 前对右表 key 做 `groupBy(key).count()`，确认是否有重复
# - 如果业务上不该有重复，加断言：`assert right.dropDuplicates(["uid"]).count() == right.count()`
# - 如果重复合理但你只想要 1 行，先 `dropDuplicates` 或 `groupBy + first`

# %% [markdown]
# ### 坑 4：列名冲突导致 ambiguous reference

# %%
a = spark.createDataFrame([(1, "x")], ["id", "value"])
b = spark.createDataFrame([(1, "y")], ["id", "value"])

j = a.join(b, on="id")

# 下面这行**会抛 AnalysisException**——Spark 不知道你要哪个 value
try:
    j.select("value").show()
except Exception as e:
    print(f"如期报错: {type(e).__name__}: {str(e)[:120]}...")

# 正确写法：用 alias
fix = a.alias("a").join(b.alias("b"), on="id").select(F.col("a.value"), F.col("b.value"))
fix.show()

# %% [markdown]
# ## 6. Join 策略：BroadcastHashJoin vs SortMergeJoin
#
# 上面我们故意关掉了自动 broadcast (`autoBroadcastJoinThreshold=-1`)。现在重新打开看
# 物理计划差异。

# %%
# 先看默认行为（已经关掉 broadcast）
small = spark.range(0, 100).toDF("id").withColumn("tag", F.lit("small"))
big = spark.range(0, 100_000).toDF("id").withColumn("v", F.col("id") * 2)

print("=== broadcast 关闭：SortMergeJoin ===")
big.join(small, on="id").explain()

# 开 broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")

print("\n=== broadcast 开启：BroadcastHashJoin ===")
big.join(small, on="id").explain()

# %% [markdown]
# 两种策略的区别：
#
# | 策略 | 触发条件 | 机制 | 适用场景 |
# |---|---|---|---|
# | **BroadcastHashJoin** | 一边足够小（默认 < 10MB） | 把小表整张广播到每个 executor，在内存做 hash 查找 | 大表 join 小表（维表） |
# | **SortMergeJoin** | 两边都大 | 两边按 key 排序再归并 | 大表 join 大表 |
# | **ShuffleHashJoin** | 较少自动选 | 两边 shuffle 后用 hash 表 | 中等大小 |
#
# **生产里最重要的优化**：把"大表 join 小维表"做成 broadcast。Module 07 会详细讲。

# %% [markdown]
# 还可以用 `F.broadcast()` **显式提示** Spark 用 broadcast 策略：

# %%
print("=== 显式 broadcast hint ===")
big.join(F.broadcast(small), on="id").explain()

# %% [markdown]
# ## 7. 练习

# %% [markdown]
# ### 练习 1（基础 join + 派生）
#
# 用本节开头的 `users` 和 `orders`，做：
#
# 1. 对每个用户算订单数和总金额（**用户没有订单时，订单数 = 0，总金额 = 0**）
# 2. 加一列 `is_buyer` 标记是否下过单
# 3. 按总金额降序输出

# %%
# TODO

# %% [markdown]
# ### 练习 2（semi/anti 实战）
#
# 用 `left_semi` 和 `left_anti`，**不用 inner join + 去重**，分别得到：
#
# 1. 至少买过一次 amount > 50 的订单的用户
# 2. 完全没买过 amount > 50 订单的用户

# %%
# TODO

# %% [markdown]
# ### 练习 3（诊断坑）
#
# 给你两个表：

# %%
left_df = spark.createDataFrame(
    [(1, "alice"), (2, "bob"), (3, "carol"), (None, "ghost")],
    ["uid", "name"],
)

right_df = spark.createDataFrame(
    [("1", 100), ("2", 200), ("2", 300), (None, 999)],
    ["uid", "score"],
)

# %% [markdown]
# 这两个表 inner join 会有什么问题？写代码：
#
# 1. 先用一句话说出**至少 3 个**潜在问题
# 2. 写出修复后的代码：cast 一致 + 处理 null + 处理右表重复

# %%
# TODO：先用注释写出你看到的问题

# TODO：再写修复代码

# %% [markdown]
# ### 练习 4（broadcast 验证）
#
# 用 `spark.range` 造一个大表（100 万行）和一个小表（100 行），
# 分别在以下两种情况下 join 并 `explain()`：
#
# 1. 不加任何 hint，看 Spark 自动选了什么策略
# 2. 显式 `F.broadcast(小表)`，看物理计划差异

# %%
# TODO

# %% [markdown]
# ## 8. 去 Spark UI 看什么
#
# 跑 `inner` join 时去 Spark UI 看：
#
# 1. **stages 数**：通常会有 2-3 个 stage，其中至少 1 个是 shuffle stage
# 2. **每个 stage 的 input/output 行数**：能看到 join 后的行数是否如预期
# 3. **Shuffle Read Size**：SortMergeJoin 会有较大的 shuffle 数据量；
#    BroadcastHashJoin 的小表那边几乎没有 shuffle
# 4. **DAG 可视化**：在 stage 详情页能看到 `BroadcastExchange` (broadcast) 或
#    `Exchange hashpartitioning` (sort merge) 这样的 stage boundary
#
# **关键学习目标**：让 `explain()` 文本 ↔ Spark UI 可视化能在你脑子里对应起来。

# %% [markdown]
# ## 9. 生产关联
#
# Join 在生产里的典型场景：
#
# - **事实表 join 维表**：订单 join 用户、订单 join 商品（大 join 小 → broadcast）
# - **慢变维 SCD type 2**：用 `left join + 时间窗口` 找历史属性
# - **A/B 实验匹配**：用 `left_semi` 找"在实验组里的用户的事件"
# - **数据对账**：用 `full outer join` 找两个数据源的差异
# - **去重 + 取最新**：先 `groupBy + max(timestamp)`，再 join 回去拿对应行
#
# **永远记住的 5 件事**（join 自查清单）：
#
# 1. ✅ 双方 join key 类型一致吗？
# 2. ✅ key 里有 null 吗？业务上 null 应该匹配 null 吗？
# 3. ✅ 右表 key 唯一吗？不唯一会让行数爆炸吗？
# 4. ✅ 是不是大 join 小？该不该 broadcast？
# 5. ✅ join 后列名有没有冲突？用了 alias 吗？

# %% [markdown]
# ## 10. 收尾

# %%
spark.stop()
print("Lesson 03 done.")

# %% [markdown]
# ---
#
# **下一节 (Wave 2)**：`04_window_functions_and_udfs.py`——窗口函数、Python UDF
# 和 Pandas UDF 性能对比。
