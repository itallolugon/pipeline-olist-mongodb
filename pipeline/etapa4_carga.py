"""Etapa 5 da especificação - Carga no MongoDB.

- cria (ou atualiza) cada coleção com um validador $jsonSchema;
- grava os documentos da camada gold em lotes, com upsert pelo _id
  (rodar duas vezes não duplica nada);
- cria os índices da especificação (N2, seção 4.3);
- confere se a quantidade no MongoDB bate com a quantidade gerada.
"""
from bson import json_util
from pymongo import ASCENDING, GEOSPHERE, MongoClient, ReplaceOne
from pymongo.errors import CollectionInvalid

from . import config
from .log import log

DATA = {"bsonType": "date"}
DEC = {"bsonType": ["decimal", "double", "int"]}

VALIDADORES = {
    "pedidos": {"$jsonSchema": {
        "bsonType": "object",
        "required": ["_id", "status", "datas", "cliente", "itens", "pagamentos", "avaliacoes", "metricas"],
        "properties": {
            "_id": {"bsonType": "string"},
            "status": {"enum": ["delivered", "shipped", "canceled", "unavailable", "invoiced",
                                "processing", "created", "approved"]},
            "datas": {"bsonType": "object", "required": ["compra"], "properties": {"compra": DATA}},
            "cliente": {"bsonType": "object", "required": ["id_cliente_unico"]},
            "itens": {"bsonType": "array", "items": {
                "bsonType": "object", "required": ["seq", "id_produto", "id_vendedor", "preco", "frete"],
                "properties": {"preco": DEC, "frete": DEC}}},
            "pagamentos": {"bsonType": "array", "items": {
                "bsonType": "object", "required": ["tipo", "valor"], "properties": {"valor": DEC}}},
            "avaliacoes": {"bsonType": "array", "items": {
                "bsonType": "object", "required": ["id_avaliacao", "nota"],
                "properties": {"nota": {"bsonType": "int", "minimum": 1, "maximum": 5}}}},
            "metricas": {"bsonType": "object", "required": ["valor_itens", "valor_pago", "qtd_itens"]},
        }}},
    "clientes": {"$jsonSchema": {
        "bsonType": "object", "required": ["_id", "ids_cliente_pedido", "localizacao"],
        "properties": {"ids_cliente_pedido": {"bsonType": "array", "minItems": 1},
                       "localizacao": {"bsonType": "object", "required": ["cep_prefixo", "uf"]}}}},
    "produtos": {"$jsonSchema": {
        "bsonType": "object", "required": ["_id", "categoria"],
        "properties": {"categoria": {"bsonType": "object", "required": ["nome_pt"]}}}},
    "vendedores": {"$jsonSchema": {
        "bsonType": "object", "required": ["_id", "localizacao"],
        "properties": {"localizacao": {"bsonType": "object", "required": ["cep_prefixo", "uf"]}}}},
}

INDICES = {
    "pedidos": [
        ([("datas.compra", ASCENDING)], {}),
        ([("status", ASCENDING), ("metricas.no_prazo", ASCENDING)], {}),
        ([("cliente.id_cliente_unico", ASCENDING)], {}),
        ([("itens.categoria", ASCENDING)], {}),
        ([("itens.id_vendedor", ASCENDING)], {}),
        ([("cliente.uf", ASCENDING)], {}),
    ],
    "clientes": [([("localizacao.geo", GEOSPHERE)], {})],
    "vendedores": [([("localizacao.geo", GEOSPHERE)], {})],
}


def conectar():
    cliente = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=8000)
    cliente.admin.command("ping")
    return cliente, cliente[config.MONGO_DB]


def _preparar_colecao(db, nome: str) -> None:
    try:
        db.create_collection(nome, validator=VALIDADORES[nome], validationLevel="strict",
                             validationAction="error")
        log(f"  coleção {nome} criada com validador $jsonSchema")
    except CollectionInvalid:  # já existe: só atualiza o validador
        db.command("collMod", nome, validator=VALIDADORES[nome], validationLevel="strict")


def _carregar(db, nome: str) -> int:
    col = db[nome]
    lote, total = [], 0
    with open(config.DIR_GOLD / f"{nome}.jsonl", encoding="utf-8") as f:
        for linha in f:
            doc = json_util.loads(linha)
            lote.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
            if len(lote) >= config.TAMANHO_LOTE:
                col.bulk_write(lote, ordered=False)
                total += len(lote)
                lote = []
    if lote:
        col.bulk_write(lote, ordered=False)
        total += len(lote)
    return total


def executar() -> dict:
    cliente, db = conectar()
    log(f"Conectado em {config.MONGO_URI} (banco '{config.MONGO_DB}', MongoDB {cliente.server_info()['version']})")
    resultado = {}
    for nome in ["produtos", "vendedores", "clientes", "pedidos"]:
        _preparar_colecao(db, nome)
        gerados = sum(1 for _ in open(config.DIR_GOLD / f"{nome}.jsonl", encoding="utf-8"))
        enviados = _carregar(db, nome)
        for chaves, opcoes in INDICES.get(nome, []):
            db[nome].create_index(chaves, **opcoes)
        no_banco = db[nome].count_documents({})
        ok = "OK" if no_banco == gerados else "DIVERGENTE"
        log(f"  {nome}: {enviados:,} enviados, {no_banco:,} no banco, {gerados:,} gerados -> {ok}")
        resultado[nome] = {"gerados": gerados, "no_banco": no_banco, "ok": no_banco == gerados}
    cliente.close()
    return resultado
