// Consultas para demonstrar a solução ao vivo no mongosh ou no MongoDB Compass (aba Aggregations).
// Uso: mongosh "mongodb://localhost:27017/olist" consultas_mongosh.js

print("\n== Documentos por coleção ==");
["pedidos", "clientes", "produtos", "vendedores"].forEach(c => print(c, db[c].countDocuments()));

print("\n== 1. Atraso x nota ==");
db.pedidos.aggregate([
  { $match: { status: "delivered", "metricas.no_prazo": { $exists: true }, "avaliacoes.0": { $exists: true } } },
  { $unwind: "$avaliacoes" },
  { $group: { _id: "$metricas.no_prazo", nota_media: { $avg: "$avaliacoes.nota" }, avaliacoes: { $sum: 1 } } }
]).forEach(printjson);

print("\n== 2. Top 5 categorias por itens vendidos ==");
db.pedidos.aggregate([
  { $unwind: "$itens" },
  { $group: { _id: "$itens.categoria", itens: { $sum: 1 } } },
  { $sort: { itens: -1 } }, { $limit: 5 }
]).forEach(printjson);

print("\n== 3. Formas de pagamento ==");
db.pedidos.aggregate([
  { $unwind: "$pagamentos" },
  { $group: { _id: "$pagamentos.tipo", registros: { $sum: 1 } } },
  { $sort: { registros: -1 } }
]).forEach(printjson);

print("\n== 4. Vendedores mais próximos de Florianópolis (índice 2dsphere) ==");
db.vendedores.aggregate([
  { $geoNear: { near: { type: "Point", coordinates: [-48.5482, -27.5954] },
                distanceField: "distancia_m", spherical: true, key: "localizacao.geo" } },
  { $limit: 5 },
  { $project: { "localizacao.cidade": 1, "localizacao.uf": 1, distancia_m: { $round: ["$distancia_m", 0] } } }
]).forEach(printjson);

print("\n== 5. Um pedido completo (tudo num documento) ==");
printjson(db.pedidos.findOne({ _id: "e481f51cbdc54678b7cc49136f2d6af7" }));
