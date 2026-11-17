"""Cria a SparkSession local usada pelas etapas 2 e 3."""
import os
import sys

from pyspark.sql import SparkSession

_spark = None


def get_spark() -> SparkSession:
    global _spark
    if _spark is None:
        # Garante que os workers usem o mesmo Python do driver (importante no Windows)
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        _spark = (
            SparkSession.builder
            .appName("pipeline-olist-mongodb")
            .master("local[*]")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "8")
            # valor inválido vira nulo (e vai para rejeitados) em vez de derrubar a execução
            .config("spark.sql.ansi.enabled", "false")
            .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate()
        )
        _spark.sparkContext.setLogLevel("ERROR")
    return _spark


def parar() -> None:
    global _spark
    if _spark is not None:
        _spark.stop()
        _spark = None
