"""Configurações do pipeline. Tudo pode ser trocado por variável de ambiente."""
import os
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent

# Camadas de dados
DIR_DATA = Path(os.getenv("DATA_DIR", RAIZ / "data"))
DIR_RAW = DIR_DATA / "raw"          # CSV originais, sem alteração
DIR_STAGING = DIR_DATA / "staging"  # Parquet limpo
DIR_GOLD = DIR_DATA / "gold"        # documentos prontos para o MongoDB (JSON estendido)
DIR_REJEITADOS = DIR_DATA / "rejeitados"
DIR_SAIDAS = RAIZ / "saidas"        # relatórios e resultados das análises

# MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "olist")

# Fonte dos dados
KAGGLE_DATASET = "olistbr/brazilian-ecommerce"
# Espelho oficial publicado pela própria Olist no GitHub (usado se não houver credencial do Kaggle)
GITHUB_ESPELHO = "https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/"

ARQUIVOS = [
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv",
]

TAMANHO_LOTE = 1000  # documentos por lote na carga
