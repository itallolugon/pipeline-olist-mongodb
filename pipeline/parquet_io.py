"""Leitura e escrita de Parquet.

A escrita passa pelo PyArrow (DataFrame.toArrow, do Spark 4) em vez do writer do Spark.
Assim o projeto roda no Windows sem precisar instalar o winutils/Hadoop, e os tipos
(inteiros com nulos, decimais, datas) são preservados. A leitura é feita pelo próprio Spark.

O particionamento (pasta coluna=valor) é feito aqui mesmo, só com pyarrow.parquet, sem o
módulo pyarrow.dataset: em alguns Windows o Controle Inteligente de Aplicativos bloqueia
as DLLs desse módulo.
"""
import shutil
from pathlib import Path

import pyarrow.parquet as pq
from pyspark.sql import DataFrame

from .spark import get_spark


def _valor_pasta(v) -> str:
    return "__HIVE_DEFAULT_PARTITION__" if v is None else str(v)


def gravar(df: DataFrame, destino: Path, particao: str | None = None) -> int:
    if destino.exists():
        shutil.rmtree(destino)
    destino.mkdir(parents=True, exist_ok=True)

    if not particao:
        tabela = df.toArrow()
        pq.write_table(tabela, str(destino / "part-0.parquet"), compression="snappy")
        return tabela.num_rows

    # Ordena pela coluna de partição e grava cada faixa contínua numa pasta coluna=valor
    tabela = df.orderBy(particao).toArrow()
    valores = tabela.column(particao).to_pylist()
    dados = tabela.drop_columns([particao])
    inicio = 0
    for i in range(1, len(valores) + 1):
        if i == len(valores) or valores[i] != valores[inicio]:
            pasta = destino / f"{particao}={_valor_pasta(valores[inicio])}"
            pasta.mkdir(parents=True, exist_ok=True)
            pq.write_table(dados.slice(inicio, i - inicio), str(pasta / "part-0.parquet"),
                           compression="snappy")
            inicio = i
    return tabela.num_rows


def ler(origem: Path) -> DataFrame:
    """Lê o Parquet com o PyArrow e entrega ao Spark como DataFrame.

    O leitor de Parquet do próprio Spark lista a pasta pela biblioteca nativa do Hadoop,
    que no Windows exige o winutils.exe/hadoop.dll. Lendo pelo PyArrow isso não é preciso.
    As pastas coluna=valor do particionamento viram coluna de novo.
    """
    import pyarrow as pa

    partes = []
    for arquivo in sorted(origem.rglob("*.parquet")):
        tabela = pq.ParquetFile(str(arquivo)).read()
        for pasta in arquivo.relative_to(origem).parent.parts:  # ex.: ano_mes=2017-10
            coluna, valor = pasta.split("=", 1)
            valor = None if valor == "__HIVE_DEFAULT_PARTITION__" else valor
            tabela = tabela.append_column(coluna, pa.array([valor] * tabela.num_rows, pa.string()))
        partes.append(tabela)
    if not partes:
        raise FileNotFoundError(f"Nenhum Parquet em {origem}. Rode a etapa de limpeza primeiro.")
    return get_spark().createDataFrame(pa.concat_tables(partes))
