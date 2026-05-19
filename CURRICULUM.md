# PySpark 生产化练习路线 (Curriculum)

目标：按顺序做完所有模块后，能在**生产环境**熟练使用 PySpark——不只是会写语法，而是
能读 Spark UI、能调性能、能写可测试的 ETL 代码、能处理流式数据。

## 学习方式

每个模块是一个 `notebooks/lessons/NN_<topic>.py` 文件，采用 **jupytext "percent" 格式**
（用 `# %%` 作为代码 cell 分隔，`# %% [markdown]` 作为文本 cell 分隔）。两种打开方式都行：

- **JupyterLab**：在容器里 `pip install jupytext` 后，右键 `.py` → Open With → Notebook
- **直接 `python` 跑**：每个文件都是合法 Python，可以 `docker compose exec jupyter python /home/jovyan/work/lessons/01_xxx.py` 直接执行

每个模块固定结构：

1. **概念**（5–10 分钟阅读）
2. **示例**（可直接运行的代码）
3. **练习**（从填空到自由作答，难度递增）
4. **去 Spark UI 看什么**（把抽象概念跟可视化对应起来）
5. **生产关联**（"在真实工作中这个知识点对应什么场景"）

## 模块总览

| # | 模块 | 难度 | 关键产出 | 状态 |
|---|---|---|---|---|
| 01 | DataFrame & Schema 基础 | 新手 | 会显式声明 schema，会做 column 操作 / cast / null 处理 | ✅ 已发布 |
| 02 | 过滤、聚合、SQL API | 新手 | 熟练 filter/groupBy/agg，DataFrame ↔ SQL 互转 | ✅ 已发布 |
| 03 | Join 全家族 | 新手→中级 | 6 种 join 全部用过，会判断 join 失败的常见原因 | ✅ 已发布 |
| 04 | 窗口函数 & UDF | 中级 | window + rank/lag/lead；Python UDF vs Pandas UDF 性能对比 | ⚪ 未开始 |
| 05 | 执行模型：Job/Stage/Task | 中级 | 看懂 `explain()` 和 Spark UI 里的 DAG、stage、task | ⚪ 未开始 |
| 06 | Partition & Shuffle | 中级 | 区分 narrow/wide、会用 repartition/coalesce、会调 shuffle partitions | ⚪ 未开始 |
| 07 | Skew & Broadcast Join | 进阶 | 会造倾斜数据、会 salting、会用 broadcast hint、理解 AQE | ⚪ 未开始 |
| 08 | 缓存与持久化 | 进阶 | 懂 cache vs persist、StorageLevel 选择、知道**什么时候不该 cache** | ⚪ 未开始 |
| 09 | 文件格式与写出 | 进阶 | Parquet 最佳实践、partitionBy、bucketing、幂等写入 | ⚪ 未开始 |
| 10 | 测试 PySpark 代码 | 进阶 | 用 chispa / pytest 写单元测试，能区分 unit vs integration | ⚪ 未开始 |
| 11 | Capstone (Batch ETL) | 综合 | 用真实数据集 (NYC TLC / MovieLens) 串起所有 batch 知识点 | ⚪ 未开始 |
| 12 | Structured Streaming 基础 | 中级 | 读 socket/file 源、micro-batch 模式、basic output sinks | ⚪ 未开始 |
| 13 | Streaming + Watermarking | 进阶 | 处理 late data、window-based agg、checkpoint 与 exactly-once 语义 | ⚪ 未开始 |

## 节奏建议

- **新手段（01–04）**：每个模块 1–2 小时，跑通示例 + 做完练习
- **中级段（05–08）**：每个模块 2–4 小时，**Spark UI 一定要打开对照看**
- **进阶段（09–10、12–13）**：每个模块 3–5 小时，会涉及配置调优
- **Capstone (11)**：留一整个周末，至少 8 小时——这是把所有知识点缝合起来的关键

## 写作发布节奏（agent → 用户）

我会**分波次发布**：

- **Wave 1**（当前）：01、02、03
- **Wave 2**：04、05、06
- **Wave 3**：07、08、09
- **Wave 4**：10、11
- **Wave 5**：12、13

每个 wave 写完会 commit + push。你做完一波给反馈，下一波我会根据你卡住的点调整深度。

## 怎么开始

```bash
# 1. 启动集群（如果还没跑）
make up

# 2. 打开 Jupyter（URL 由 make token 给出）

# 3. 进入 work/lessons/，打开 01_dataframe_basics.py
#    （或者直接：docker compose exec jupyter python /home/jovyan/work/lessons/01_dataframe_basics.py）

# 4. Spark UI 同时打开：http://localhost:8080
```
