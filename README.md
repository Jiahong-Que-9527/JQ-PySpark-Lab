# PySpark Local Learning Environment

Minimal Spark standalone cluster (1 master + 1 worker) plus Jupyter, for
learning PySpark in a **production-shaped** setup.

## Curriculum (start here)

See [**CURRICULUM.md**](CURRICULUM.md) for the full 13-module learning path
(beginner → intermediate → advanced + streaming basics). Lessons live in
[`notebooks/lessons/`](notebooks/lessons/).

## Quick start

    make up        # Start everything, prints Jupyter URL + token
    make test      # Run smoke test
    make down      # Stop

## URLs

- JupyterLab:       http://localhost:8888  (token from `make token`)
- Spark Master UI:  http://localhost:8080
- Spark App UI:     http://localhost:4040  (only while a job is running)

## Running a lesson

Two options:

    # 1) Run as a script
    docker compose exec jupyter python /home/jovyan/work/lessons/01_dataframe_basics.py

    # 2) Open in JupyterLab (cell-by-cell)
    #    Inside the running jupyter container:
    #      pip install jupytext
    #    Then in JupyterLab: right-click the .py file → Open With → Notebook

## Connecting from your own notebook

    from pyspark.sql import SparkSession
    spark = (SparkSession.builder
        .appName("learn")
        .master("spark://spark-master:7077")
        .getOrCreate())

Do NOT use `local[*]` — the whole point of this setup is to use the cluster
so you can see jobs in the Master UI.

## Where to put data

The `./data/` directory is mounted into **all three containers** (master,
worker, jupyter) at `/data`. **This simulates S3/HDFS** in a real cluster.
Always read/write through `/data/...` paths in lessons — never `/tmp/...`
or `~/...`, which are local to each container and not visible to executors.

## Common commands

    make up        # start
    make down      # stop
    make restart   # restart
    make logs      # tail logs
    make token     # reprint Jupyter URL + UI URLs
    make ps        # container status
    make test      # smoke test
    make clean     # stop + remove volumes + clean checkpoints

## Versions (do not upgrade casually)

- Cluster & Jupyter: `jupyter/pyspark-notebook:spark-3.5.0` (Spark 3.5.0, Python 3.11)

All three containers share the same image to guarantee Python-version parity
between driver and executors. The original spec called for `bitnami/spark:3.5`,
but that tag was removed from Docker Hub — see commit history for details.
