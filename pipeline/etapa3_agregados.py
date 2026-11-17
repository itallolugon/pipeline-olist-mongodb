"""Etapa 4 da especificação - Transformação em agregados (Apache Spark).

Lê o Parquet da camada staging e monta os documentos no formato do modelo de agregados
(N2, seção 4): pedidos com itens, pagamentos, avaliações, resumo do cliente e métricas
embutidos; e as coleções clientes, produtos e vendedores. Os documentos são gravados em
data/gold/<colecao>.jsonl no formato JSON estendido do MongoDB (mantém datas e decimais).
"""
from decimal import Decimal

from bson import Decimal128
from bson import json_util
from pyspark.sql import functions as F

from . import config, parquet_io
from .log import log


def _limpar(valor):
    """Remove campos nulos (campo ausente no documento) e converte Decimal -> Decimal128."""
    if isinstance(valor, dict):
        saida = {}
        for k, v in valor.items():
            v = _limpar(v)
            listas_fixas = k in ("itens", "pagamentos", "avaliacoes")  # ficam mesmo vazias
            if v is None or (v in ([], {}) and not listas_fixas):
                continue
            saida[k] = v
        return saida
    if isinstance(valor, list):
        return [_limpar(v) for v in valor]
    if isinstance(valor, Decimal):
        return Decimal128(valor)
    return valor


def _gravar_jsonl(df, nome: str) -> int:
    config.DIR_GOLD.mkdir(parents=True, exist_ok=True)
    n = 0
    with open(config.DIR_GOLD / f"{nome}.jsonl", "w", encoding="utf-8") as f:
        for row in df.toLocalIterator():
            doc = _limpar(row.asDict(recursive=True))
            f.write(json_util.dumps(doc, ensure_ascii=False) + "\n")
            n += 1
    log(f"  gold/{nome}.jsonl: {n:,} documentos")
    return n


def _geo_point(lat, lng):
    return F.when(F.col(lat).isNotNull(),
                  F.struct(F.lit("Point").alias("type"), F.array(F.col(lng), F.col(lat)).alias("coordinates")))


def executar() -> dict:
    st = config.DIR_STAGING
    pedidos = parquet_io.ler(st / "pedidos")
    itens = parquet_io.ler(st / "itens")
    pagamentos = parquet_io.ler(st / "pagamentos")
    avaliacoes = parquet_io.ler(st / "avaliacoes")
    clientes = parquet_io.ler(st / "clientes")
    produtos = parquet_io.ler(st / "produtos")
    vendedores = parquet_io.ler(st / "vendedores")
    geo = parquet_io.ler(st / "geolocalizacao")

    # ---------- coleção produtos ----------
    col_produtos = produtos.select(
        F.col("id_produto").alias("_id"),
        F.struct(F.col("categoria_pt").alias("nome_pt"), F.col("categoria_en").alias("nome_en")).alias("categoria"),
        F.struct("peso_g", "comprimento_cm", "altura_cm", "largura_cm").alias("dimensoes"),
        "tam_nome", "tam_descricao", "qtd_fotos",
    )

    # ---------- coleção vendedores ----------
    vend_geo = vendedores.join(geo, "cep_prefixo", "left")
    col_vendedores = vend_geo.select(
        F.col("id_vendedor").alias("_id"),
        F.struct("cep_prefixo", "cidade", "uf", _geo_point("lat", "lng").alias("geo")).alias("localizacao"),
    )

    # ---------- coleção clientes (agrupada pela pessoa) ----------
    cli_geo = clientes.join(geo, "cep_prefixo", "left")
    col_clientes = (cli_geo.groupBy("id_cliente_unico").agg(
        F.sort_array(F.collect_set("id_cliente_pedido")).alias("ids_cliente_pedido"),
        F.first(F.struct("cep_prefixo", "cidade", "uf", _geo_point("lat", "lng").alias("geo")),
                ignorenulls=True).alias("localizacao"),
    ).withColumnRenamed("id_cliente_unico", "_id"))

    # ---------- coleção pedidos ----------
    itens_doc = (itens
                 .join(produtos.select("id_produto", F.col("categoria_pt").alias("categoria")), "id_produto", "left")
                 .join(vendedores.select("id_vendedor", F.col("uf").alias("uf_vendedor")), "id_vendedor", "left")
                 .groupBy("id_pedido").agg(
                     F.sort_array(F.collect_list(F.struct(
                         "seq", "id_produto", "categoria", "id_vendedor", "uf_vendedor",
                         "preco", "frete", "data_limite_envio"))).alias("itens"),
                     F.sum("preco").alias("valor_itens"), F.sum("frete").alias("valor_frete"),
                     F.count("*").alias("qtd_itens")))
    pag_doc = (pagamentos.groupBy("id_pedido").agg(
        F.sort_array(F.collect_list(F.struct(
            "sequencial", "tipo", "parcelas", "valor",
            F.when(F.col("suspeito"), F.lit(True)).alias("suspeito")))).alias("pagamentos"),
        F.sum("valor").alias("valor_pago")))
    av_doc = (avaliacoes.groupBy("id_pedido").agg(
        F.collect_list(F.struct("id_avaliacao", "nota", "titulo", "comentario",
                                "data_criacao", "data_resposta")).alias("avaliacoes")))
    cli_resumo = clientes.select("id_cliente_pedido", "id_cliente_unico", "cidade", "uf")

    p = (pedidos.join(cli_resumo, "id_cliente_pedido", "left")
         .join(itens_doc, "id_pedido", "left")
         .join(pag_doc, "id_pedido", "left")
         .join(av_doc, "id_pedido", "left"))

    entregue = F.col("entrega_cliente").isNotNull()
    col_pedidos = p.select(
        F.col("id_pedido").alias("_id"),
        "status",
        F.struct("compra", "aprovacao", "envio_transportadora", "entrega_cliente", "entrega_estimada").alias("datas"),
        F.struct("id_cliente_unico", "id_cliente_pedido", "cidade", "uf").alias("cliente"),
        F.coalesce("itens", F.array()).alias("itens"),
        F.coalesce("pagamentos", F.array()).alias("pagamentos"),
        F.coalesce("avaliacoes", F.array()).alias("avaliacoes"),
        F.struct(
            F.coalesce("valor_itens", F.lit(0).cast("decimal(12,2)")).alias("valor_itens"),
            F.coalesce("valor_frete", F.lit(0).cast("decimal(12,2)")).alias("valor_frete"),
            F.coalesce("valor_pago", F.lit(0).cast("decimal(12,2)")).alias("valor_pago"),
            F.coalesce("qtd_itens", F.lit(0)).alias("qtd_itens"),
            F.when(entregue, F.datediff(F.to_date("entrega_cliente"), F.to_date("compra"))).alias("dias_entrega"),
            F.when(entregue, F.datediff(F.to_date("entrega_cliente"), F.to_date("entrega_estimada"))).alias("atraso_dias"),
            F.when(entregue, F.col("entrega_cliente") <= F.col("entrega_estimada")).alias("no_prazo"),
        ).alias("metricas"),
    )

    contagem = {}
    for nome, df in [("produtos", col_produtos), ("vendedores", col_vendedores),
                     ("clientes", col_clientes), ("pedidos", col_pedidos)]:
        contagem[nome] = _gravar_jsonl(df, nome)
    return contagem
