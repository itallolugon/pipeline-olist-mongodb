"""Etapa 6 da especificação - Análises.

Roda as consultas de agregação da especificação direto no MongoDB, mostra no terminal
e grava tudo em saidas/analises.md (as "saídas esperadas" da solução).
"""
from datetime import datetime

from bson import json_util

from . import config
from .etapa4_carga import conectar
from .log import log


def _tabela(cab: list[str], linhas: list[list]) -> str:
    out = "| " + " | ".join(cab) + " |\n|" + "---|" * len(cab) + "\n"
    for l in linhas:
        out += "| " + " | ".join(str(x) for x in l) + " |\n"
    return out


def _i(n) -> str:
    return f"{int(n):,}".replace(",", ".")


def _f(n, casas=2):
    return f"{float(str(n)):,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def executar() -> str:
    _, db = conectar()
    ped = db.pedidos
    md = [f"# Saídas do pipeline Olist → MongoDB\n\nGerado em {datetime.now():%d/%m/%Y %H:%M}, "
          f"banco `{config.MONGO_DB}`.\n"]

    # 0. contagem das coleções
    linhas = [[c, f"{db[c].count_documents({}):,}".replace(",", ".")] for c in ["pedidos", "clientes", "produtos", "vendedores"]]
    md.append("## 0. Documentos carregados\n\n" + _tabela(["Coleção", "Documentos"], linhas))

    # 1. atraso x nota
    r = list(ped.aggregate([
        {"$match": {"status": "delivered", "metricas.no_prazo": {"$exists": True}, "avaliacoes.0": {"$exists": True}}},
        {"$unwind": "$avaliacoes"},
        {"$group": {"_id": "$metricas.no_prazo", "nota_media": {"$avg": "$avaliacoes.nota"},
                    "pedidos": {"$addToSet": "$_id"}}},
        {"$project": {"nota_media": 1, "pedidos": {"$size": "$pedidos"}}},
        {"$sort": {"_id": -1}}]))
    linhas = [["No prazo" if x["_id"] else "Atrasado", _f(x["nota_media"]), f'{x["pedidos"]:,}'.replace(",", ".")] for x in r]
    entregues = ped.count_documents({"metricas.no_prazo": {"$exists": True}})
    atrasados = ped.count_documents({"metricas.no_prazo": False})
    md.append("## 1. Atraso na entrega x nota do cliente\n\n```js\n"
              "db.pedidos.aggregate([\n  { $match: { status: \"delivered\", \"avaliacoes.0\": { $exists: true } } },\n"
              "  { $unwind: \"$avaliacoes\" },\n  { $group: { _id: \"$metricas.no_prazo\", nota_media: { $avg: \"$avaliacoes.nota\" } } }\n])\n```\n\n"
              + _tabela(["Entrega", "Nota média", "Pedidos"], linhas)
              + f"\n{_f(100 * atrasados / entregues, 1)}% dos pedidos entregues chegaram depois do prazo "
                f"({_i(atrasados)} de {_i(entregues)}).\n")

    # 2. vendas por mês
    r = list(ped.aggregate([
        {"$match": {"status": {"$nin": ["canceled", "unavailable"]}}},
        {"$group": {"_id": {"$dateToString": {"format": "%Y-%m", "date": "$datas.compra"}},
                    "pedidos": {"$sum": 1}, "faturamento": {"$sum": "$metricas.valor_itens"}}},
        {"$sort": {"pedidos": -1}}, {"$limit": 5}]))
    dia = list(ped.aggregate([
        {"$group": {"_id": {"$dateToString": {"format": "%d/%m/%Y", "date": "$datas.compra"}}, "n": {"$sum": 1}}},
        {"$sort": {"n": -1}}, {"$limit": 1}]))[0]
    md.append("## 2. Meses com mais pedidos\n\n" + _tabela(
        ["Mês", "Pedidos", "Faturamento (R$)"],
        [[x["_id"], f'{x["pedidos"]:,}'.replace(",", "."), _f(x["faturamento"])] for x in r])
        + f"\nDia com mais pedidos: **{dia['_id']}** ({_i(dia['n'])} pedidos, Black Friday).\n")

    # 3. categorias
    r = list(ped.aggregate([
        {"$unwind": "$itens"},
        {"$group": {"_id": "$itens.categoria", "itens": {"$sum": 1}, "receita": {"$sum": "$itens.preco"}}},
        {"$sort": {"itens": -1}}, {"$limit": 5}]))
    md.append("## 3. Categorias mais vendidas\n\n" + _tabela(
        ["Categoria", "Itens", "Receita (R$)"],
        [[x["_id"], f'{x["itens"]:,}'.replace(",", "."), _f(x["receita"])] for x in r]))

    # 4. pagamentos
    total = list(ped.aggregate([{"$unwind": "$pagamentos"}, {"$count": "n"}]))[0]["n"]
    r = list(ped.aggregate([
        {"$unwind": "$pagamentos"},
        {"$group": {"_id": "$pagamentos.tipo", "n": {"$sum": 1}, "parcelas_max": {"$max": "$pagamentos.parcelas"},
                    "valor_medio": {"$avg": "$pagamentos.valor"}}},
        {"$sort": {"n": -1}}]))
    md.append("## 4. Formas de pagamento\n\n" + _tabela(
        ["Tipo", "Registros", "% do total", "Máx. parcelas", "Valor médio (R$)"],
        [[x["_id"], f'{x["n"]:,}'.replace(",", "."), _f(100 * x["n"] / total, 1), x["parcelas_max"], _f(x["valor_medio"])] for x in r]))

    # 5. recompra
    total_cli = db.clientes.count_documents({})
    recompra = db.clientes.count_documents({"ids_cliente_pedido.1": {"$exists": True}})
    md.append(f"## 5. Recompra\n\nDos {_i(total_cli)} clientes únicos, **{_i(recompra)}** fizeram mais de um pedido "
              f"({_f(100 * recompra / total_cli, 1)}%).\n")

    # 6. nota por UF
    r = list(ped.aggregate([
        {"$unwind": "$avaliacoes"},
        {"$group": {"_id": "$cliente.uf", "nota": {"$avg": "$avaliacoes.nota"}, "n": {"$sum": 1},
                    "atraso": {"$avg": {"$cond": [{"$eq": ["$metricas.no_prazo", False]}, 1, 0]}}}},
        {"$match": {"n": {"$gte": 500}}}, {"$sort": {"nota": 1}}]))
    sel = r[:3] + r[-3:]
    md.append("## 6. Nota média por estado do cliente (3 piores e 3 melhores, mínimo 500 avaliações)\n\n" + _tabela(
        ["UF", "Nota média", "% atrasados", "Avaliações"],
        [[x["_id"], _f(x["nota"]), _f(100 * x["atraso"], 1), f'{x["n"]:,}'.replace(",", ".")] for x in sel]))

    # 7. geoespacial
    r = list(db.vendedores.aggregate([
        {"$geoNear": {"near": {"type": "Point", "coordinates": [-48.5482, -27.5954]},
                      "distanceField": "dist", "spherical": True, "key": "localizacao.geo"}},
        {"$limit": 5}]))
    md.append("## 7. Consulta geoespacial: 5 vendedores mais próximos de Florianópolis\n\n" + _tabela(
        ["Vendedor", "Cidade", "UF", "Distância (km)"],
        [[x["_id"][:8] + "…", x["localizacao"]["cidade"], x["localizacao"]["uf"], _f(x["dist"] / 1000, 1)] for x in r]))

    # 8. documento de exemplo
    doc = ped.find_one({"_id": "e481f51cbdc54678b7cc49136f2d6af7"})
    md.append("## 8. Exemplo de documento na coleção pedidos\n\n```json\n"
              + json_util.dumps(doc, indent=2, ensure_ascii=False, json_options=json_util.RELAXED_JSON_OPTIONS)
              + "\n```\n")

    texto = "\n".join(md)
    config.DIR_SAIDAS.mkdir(exist_ok=True)
    (config.DIR_SAIDAS / "analises.md").write_text(texto, encoding="utf-8")
    print("\n" + texto)
    log("Resultados gravados em saidas/analises.md")
    return texto
