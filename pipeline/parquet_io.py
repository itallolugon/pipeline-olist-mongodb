"""Leitura e escrita de Parquet.

A escrita passa pelo PyArrow (DataFrame.toArrow, do Spark 4) em vez do writer do Spark.
Assim o projeto roda no Windows sem precisar instalar o winutils/Hadoop, e os tipos
(inteiros com nulos, decimais, datas) são preservados. A leitura é feita pelo próprio Spark.
"""
import shutil
from pathlib import Path

import pyarrow.parquet as pq
from pyspark.sql import DataFrame

from .spark import get_spark


def gravar(df: DataFrame, destino: Path, particao: str | None = None) -> int:
    tabela = df.toArrow()
    if destino.exists():
        shutil.rmtree(destino)
    destino.mkdir(parents=True, exist_ok=True)
    if particao:
        pq.write_to_dataset(tabela, root_path=str(destino), partition_cols=[particao],
                            compression="snappy")
    else:
        pq.write_table(tabela, str(destino / "part-0.parquet"), compression="snappy")
    return tabela.num_rows


def ler(origem: Path) -> DataFrame:
    return get_spark().read.parquet(str(origem))
