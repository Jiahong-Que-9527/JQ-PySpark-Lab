# PySpark Local Learning Environment

Minimal Spark standalone cluster (1 master + 1 worker) plus Jupyter, for
learning PySpark in a production-shaped setup.

## Quick start

    make up        # Start everything, prints Jupyter URL + token
    make test      # Run smoke test
    make down      # Stop

## URLs

- JupyterLab:       http://localhost:8888  (token from `make token`)
- Spark Master UI:  http://localhost:8080
- Spark App UI:     http://localhost:4040  (only while a job is running)

## Connecting from a notebook

    from pyspark.sql import SparkSession
    spark = (SparkSession.builder
        .appName("learn")
        .master("spark://spark-master:7077")
        .getOrCreate())

Do NOT use `local[*]` — the whole point of this setup is to use the cluster
so you can see jobs in the Master UI.

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

- Spark cluster: bitnami/spark:3.5
- Jupyter:       jupyter/pyspark-notebook:spark-3.5.0

The PySpark version on the Jupyter side must match the cluster. Upgrade
both together or expect cryptic serialization errors.
