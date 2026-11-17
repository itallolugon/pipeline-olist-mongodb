"""Etapa 2 - Validação e limpeza (Apache Spark) + Etapa 3 - armazenamento em Parquet.

Lê os CSV da camada raw com esquema declarado (tudo como texto, depois convertido com
regras explícitas), aplica as regras de qualidade da especificação (N2, Tabela 2) e grava
uma tabela Parquet por arquivo na camada staging. Linhas inválidas vão para a pasta de
rejeitados, sem interromper a carga. No fim gera saidas/relatorio_qualidade.json.
"""
import json
from functools import reduce

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType, StructField, StructType

from . import config, parquet_io
from .etapa1_captura import ultimo_raw
from .log import log
from .spark import get_spark

DEC = "decimal(10,2)"


def _ler_csv(nome: str, colunas: list[str]) -> DataFrame:
    """Lê como texto com esquema fixo. multiLine/escape tratam comentários com quebra de linha."""
    esquema = StructType([StructField(c, StringType(), True) for c in colunas])
    caminho = str(ultimo_raw() / nome)
    return (get_spark().read
            .option("header", True).option("multiLine", True)
            .option("quote", '"').option("escape", '"')
            .schema(esquema).csv(caminho))


def _ts(col: str):
    # TIMESTAMP_NTZ: guarda a data/hora exatamente como no arquivo, sem conversão de fuso.
    # Sem isso, o Python converte para o fuso do computador e a hora muda (ex.: -3h no Brasil).
    return F.to_timestamp_ntz(F.col(col), F.lit("yyyy-MM-dd HH:mm:ss"))


def _cep(col: str):
    return F.lpad(F.trim(F.col(col)), 5, "0")


def _separar_rejeitados(df: DataFrame, nome: str, obrigatorias: list[str], rel: dict) -> DataFrame:
    """Linhas com campo obrigatório nulo (ou que não converteu) vão para data/rejeitados."""
    cond_ruim = reduce(lambda a, b: a | b, [F.col(c).isNull() for c in obrigatorias])
    ruins = df.filter(cond_ruim)
    n_ruins = ruins.count()
    if n_ruins:
        parquet_io.gravar(ruins, config.DIR_REJEITADOS / nome)
    rel.setdefault("rejeitados", {})[nome] = n_ruins
    return df.filter(~cond_ruim)


def executar() -> dict:
    rel: dict = {"origem": str(ultimo_raw()), "linhas_lidas": {}, "linhas_gravadas": {}, "regras": {}}

    # ---------- clientes ----------
    cli_raw = _ler_csv("olist_customers_dataset.csv",
                       ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
                        "customer_city", "customer_state"])
    rel["linhas_lidas"]["clientes"] = cli_raw.count()
    rel["regras"]["cep_cliente_sem_zero_a_esquerda"] = cli_raw.filter(
        F.length("customer_zip_code_prefix") < 5).count()
    clientes = cli_raw.select(
        F.col("customer_id").alias("id_cliente_pedido"),
        F.col("customer_unique_id").alias("id_cliente_unico"),
        _cep("customer_zip_code_prefix").alias("cep_prefixo"),
        F.col("customer_city").alias("cidade"),
        F.upper("customer_state").alias("uf"),
    )
    clientes = _separar_rejeitados(clientes, "clientes", ["id_cliente_pedido", "id_cliente_unico"], rel)

    # ---------- geolocalização ----------
    geo_raw = _ler_csv("olist_geolocation_dataset.csv",
                       ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng",
                        "geolocation_city", "geolocation_state"])
    rel["linhas_lidas"]["geolocalizacao"] = geo_raw.count()
    geo_dedup = geo_raw.dropDuplicates()
    rel["regras"]["geolocalizacao_linhas_duplicadas_removidas"] = rel["linhas_lidas"]["geolocalizacao"] - geo_dedup.count()
    geo = (geo_dedup
           .select(_cep("geolocation_zip_code_prefix").alias("cep_prefixo"),
                   F.col("geolocation_lat").cast("double").alias("lat"),
                   F.col("geolocation_lng").cast("double").alias("lng"))
           .filter(F.col("lat").between(-34, 6) & F.col("lng").between(-74, -34))  # dentro do Brasil
           .groupBy("cep_prefixo")
           .agg(F.round(F.avg("lat"), 6).alias("lat"), F.round(F.avg("lng"), 6).alias("lng")))

    # ---------- pedidos ----------
    ped_raw = _ler_csv("olist_orders_dataset.csv",
                       ["order_id", "customer_id", "order_status", "order_purchase_timestamp",
                        "order_approved_at", "order_delivered_carrier_date",
                        "order_delivered_customer_date", "order_estimated_delivery_date"])
    rel["linhas_lidas"]["pedidos"] = ped_raw.count()
    pedidos = ped_raw.select(
        F.col("order_id").alias("id_pedido"),
        F.col("customer_id").alias("id_cliente_pedido"),
        F.col("order_status").alias("status"),
        _ts("order_purchase_timestamp").alias("compra"),
        _ts("order_approved_at").alias("aprovacao"),
        _ts("order_delivered_carrier_date").alias("envio_transportadora"),
        _ts("order_delivered_customer_date").alias("entrega_cliente"),
        _ts("order_estimated_delivery_date").alias("entrega_estimada"),
    ).withColumn("ano_mes", F.date_format("compra", "yyyy-MM"))
    rel["regras"]["pedidos_sem_data_entrega_cliente"] = pedidos.filter(F.col("entrega_cliente").isNull()).count()
    pedidos = _separar_rejeitados(pedidos, "pedidos", ["id_pedido", "id_cliente_pedido", "compra"], rel)

    # ---------- itens ----------
    it_raw = _ler_csv("olist_order_items_dataset.csv",
                      ["order_id", "order_item_id", "product_id", "seller_id",
                       "shipping_limit_date", "price", "freight_value"])
    rel["linhas_lidas"]["itens"] = it_raw.count()
    itens = it_raw.select(
        F.col("order_id").alias("id_pedido"),
        F.col("order_item_id").cast("int").alias("seq"),
        F.col("product_id").alias("id_produto"),
        F.col("seller_id").alias("id_vendedor"),
        _ts("shipping_limit_date").alias("data_limite_envio"),
        F.col("price").cast(DEC).alias("preco"),
        F.col("freight_value").cast(DEC).alias("frete"),
    )
    itens = _separar_rejeitados(itens, "itens", ["id_pedido", "seq", "id_produto", "id_vendedor", "preco"], rel)
    itens = itens.join(pedidos.select("id_pedido", "ano_mes"), "id_pedido", "left") \
                 .fillna({"ano_mes": "desconhecido"})

    # ---------- pagamentos ----------
    pg_raw = _ler_csv("olist_order_payments_dataset.csv",
                      ["order_id", "payment_sequential", "payment_type",
                       "payment_installments", "payment_value"])
    rel["linhas_lidas"]["pagamentos"] = pg_raw.count()
    pagamentos = pg_raw.select(
        F.col("order_id").alias("id_pedido"),
        F.col("payment_sequential").cast("int").alias("sequencial"),
        F.col("payment_type").alias("tipo"),
        F.col("payment_installments").cast("int").alias("parcelas"),
        F.col("payment_value").cast(DEC).alias("valor"),
    ).withColumn("suspeito", (F.col("valor") == 0) | (F.col("parcelas") == 0))
    rel["regras"]["pagamentos_suspeitos_valor_ou_parcela_zero"] = pagamentos.filter("suspeito").count()
    pagamentos = _separar_rejeitados(pagamentos, "pagamentos", ["id_pedido", "sequencial", "valor"], rel)

    # ---------- avaliações ----------
    av_raw = _ler_csv("olist_order_reviews_dataset.csv",
                      ["review_id", "order_id", "review_score", "review_comment_title",
                       "review_comment_message", "review_creation_date", "review_answer_timestamp"])
    rel["linhas_lidas"]["avaliacoes"] = av_raw.count()
    avaliacoes = av_raw.select(
        F.col("review_id").alias("id_avaliacao"),
        F.col("order_id").alias("id_pedido"),
        F.col("review_score").cast("int").alias("nota"),
        F.trim("review_comment_title").alias("titulo"),
        F.trim("review_comment_message").alias("comentario"),
        _ts("review_creation_date").alias("data_criacao"),
        _ts("review_answer_timestamp").alias("data_resposta"),
    )
    rel["regras"]["review_id_ligado_a_mais_de_um_pedido"] = (
        avaliacoes.groupBy("id_avaliacao").agg(F.countDistinct("id_pedido").alias("n"))
        .filter("n > 1").count())
    avaliacoes = avaliacoes.dropDuplicates(["id_avaliacao", "id_pedido"])  # chave composta
    avaliacoes = _separar_rejeitados(avaliacoes, "avaliacoes", ["id_avaliacao", "id_pedido", "nota"], rel)

    # ---------- produtos + tradução ----------
    pr_raw = _ler_csv("olist_products_dataset.csv",
                      ["product_id", "product_category_name", "product_name_lenght",
                       "product_description_lenght", "product_photos_qty", "product_weight_g",
                       "product_length_cm", "product_height_cm", "product_width_cm"])
    rel["linhas_lidas"]["produtos"] = pr_raw.count()
    rel["regras"]["produtos_sem_categoria"] = pr_raw.filter(F.col("product_category_name").isNull()).count()
    tr_raw = _ler_csv("product_category_name_translation.csv",
                      ["product_category_name", "product_category_name_english"])
    rel["linhas_lidas"]["traducao_categorias"] = tr_raw.count()
    # Criado em SQL (só na JVM). Um createDataFrame a partir de lista do Python abriria um
    # processo worker do Python, que quebra no Windows com Python 3.14.
    complemento = get_spark().sql("""
        SELECT * FROM VALUES
          ('pc_gamer', 'pc_gamer'),
          ('portateis_cozinha_e_preparadores_de_alimentos', 'portable_kitchen_and_food_preparers'),
          ('sem_categoria', 'no_category')
        AS t(product_category_name, product_category_name_english)""")
    traducao = tr_raw.unionByName(complemento).dropDuplicates(["product_category_name"])
    produtos = (pr_raw
                .withColumn("product_category_name", F.coalesce("product_category_name", F.lit("sem_categoria")))
                .join(traducao, "product_category_name", "left")
                .select(
                    F.col("product_id").alias("id_produto"),
                    F.col("product_category_name").alias("categoria_pt"),
                    F.col("product_category_name_english").alias("categoria_en"),
                    F.col("product_name_lenght").cast("int").alias("tam_nome"),         # corrige "lenght"
                    F.col("product_description_lenght").cast("int").alias("tam_descricao"),
                    F.col("product_photos_qty").cast("int").alias("qtd_fotos"),
                    F.col("product_weight_g").cast("int").alias("peso_g"),
                    F.col("product_length_cm").cast("int").alias("comprimento_cm"),
                    F.col("product_height_cm").cast("int").alias("altura_cm"),
                    F.col("product_width_cm").cast("int").alias("largura_cm"),
                ))
    rel["regras"]["categorias_sem_traducao_complementadas"] = 2
    produtos = _separar_rejeitados(produtos, "produtos", ["id_produto"], rel)

    # ---------- vendedores ----------
    vd_raw = _ler_csv("olist_sellers_dataset.csv",
                      ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"])
    rel["linhas_lidas"]["vendedores"] = vd_raw.count()
    vendedores = vd_raw.select(
        F.col("seller_id").alias("id_vendedor"),
        _cep("seller_zip_code_prefix").alias("cep_prefixo"),
        F.col("seller_city").alias("cidade"),
        F.upper("seller_state").alias("uf"),
    )
    vendedores = _separar_rejeitados(vendedores, "vendedores", ["id_vendedor"], rel)

    # ---------- regras de consistência ----------
    ceps_geo = geo.select("cep_prefixo")
    rel["regras"]["clientes_sem_coordenada"] = clientes.join(ceps_geo, "cep_prefixo", "left_anti").count()
    rel["regras"]["vendedores_sem_coordenada"] = vendedores.join(ceps_geo, "cep_prefixo", "left_anti").count()
    rel["regras"]["pedidos_sem_itens"] = pedidos.join(itens, "id_pedido", "left_anti").count()
    rel["regras"]["pedidos_sem_pagamento"] = pedidos.join(pagamentos, "id_pedido", "left_anti").count()
    rel["regras"]["itens_sem_pedido"] = itens.join(pedidos, "id_pedido", "left_anti").count()
    soma_itens = itens.groupBy("id_pedido").agg((F.sum("preco") + F.sum("frete")).alias("total_itens"))
    soma_pag = pagamentos.groupBy("id_pedido").agg(F.sum("valor").alias("total_pago"))
    divergentes = (soma_itens.join(soma_pag, "id_pedido")
                   .withColumn("diferenca", F.abs(F.col("total_itens") - F.col("total_pago")))
                   .filter(F.col("diferenca") > 0.05))
    rel["regras"]["pedidos_pagamento_diferente_de_itens_mais_frete"] = divergentes.count()
    parquet_io.gravar(divergentes, config.DIR_REJEITADOS / "relatorio_divergencia_pagamento")

    # ---------- grava staging (Parquet) ----------
    tabelas = {
        "clientes": (clientes, None), "geolocalizacao": (geo, None), "pedidos": (pedidos, "ano_mes"),
        "itens": (itens, "ano_mes"), "pagamentos": (pagamentos, None), "avaliacoes": (avaliacoes, None),
        "produtos": (produtos, None), "vendedores": (vendedores, None),
    }
    for nome, (df, part) in tabelas.items():
        n = parquet_io.gravar(df, config.DIR_STAGING / nome, part)
        rel["linhas_gravadas"][nome] = n
        log(f"  staging/{nome}: {n:,} linhas" + (f" (particionado por {part})" if part else ""))

    config.DIR_SAIDAS.mkdir(exist_ok=True)
    (config.DIR_SAIDAS / "relatorio_qualidade.json").write_text(
        json.dumps(rel, indent=2, ensure_ascii=False), encoding="utf-8")
    log("Relatório de qualidade gravado em saidas/relatorio_qualidade.json")
    for k, v in rel["regras"].items():
        log(f"  regra {k}: {v:,}")
    return rel
