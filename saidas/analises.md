# Saídas do pipeline Olist → MongoDB

Gerado em 17/11/2026 17:12, banco `olist`.

## 0. Documentos carregados

| Coleção | Documentos |
|---|---|
| pedidos | 99.441 |
| clientes | 96.096 |
| produtos | 32.951 |
| vendedores | 3.095 |

## 1. Atraso na entrega x nota do cliente

```js
db.pedidos.aggregate([
  { $match: { status: "delivered", "avaliacoes.0": { $exists: true } } },
  { $unwind: "$avaliacoes" },
  { $group: { _id: "$metricas.no_prazo", nota_media: { $avg: "$avaliacoes.nota" } } }
])
```

| Entrega | Nota média | Pedidos |
|---|---|---|
| No prazo | 4,29 | 88.163 |
| Atrasado | 2,57 | 7.661 |

8,1% dos pedidos entregues chegaram depois do prazo (7.827 de 96.476).

## 2. Meses com mais pedidos

| Mês | Pedidos | Faturamento (R$) |
|---|---|---|
| 2017-11 | 7.423 | 1.003.862,14 |
| 2018-01 | 7.187 | 945.456,29 |
| 2018-03 | 7.168 | 981.051,06 |
| 2018-04 | 6.919 | 993.592,98 |
| 2018-05 | 6.833 | 992.871,75 |

Dia com mais pedidos: **24/11/2017** (1.176 pedidos, Black Friday).

## 3. Categorias mais vendidas

| Categoria | Itens | Receita (R$) |
|---|---|---|
| cama_mesa_banho | 11.115 | 1.036.988,68 |
| beleza_saude | 9.670 | 1.258.681,34 |
| esporte_lazer | 8.641 | 988.048,97 |
| moveis_decoracao | 8.334 | 729.762,49 |
| informatica_acessorios | 7.827 | 911.954,32 |

## 4. Formas de pagamento

| Tipo | Registros | % do total | Máx. parcelas | Valor médio (R$) |
|---|---|---|---|---|
| credit_card | 76.795 | 73,9 | 24 | 163,32 |
| boleto | 19.784 | 19,0 | 10 | 145,03 |
| voucher | 5.775 | 5,6 | 1 | 65,70 |
| debit_card | 1.529 | 1,5 | 1 | 142,57 |
| not_defined | 3 | 0,0 | 1 | 0,00 |

## 5. Recompra

Dos 96.096 clientes únicos, **2.997** fizeram mais de um pedido (3,1%).

## 6. Nota média por estado do cliente (3 piores e 3 melhores, mínimo 500 avaliações)

| UF | Nota média | % atrasados | Avaliações |
|---|---|---|---|
| MA | 3,76 | 18,4 | 746 |
| PA | 3,85 | 11,5 | 968 |
| CE | 3,85 | 14,7 | 1.329 |
| MG | 4,14 | 5,4 | 11.625 |
| SP | 4,17 | 5,6 | 41.690 |
| PR | 4,18 | 4,8 | 5.038 |

## 7. Consulta geoespacial: 5 vendedores mais próximos de Florianópolis

| Vendedor | Cidade | UF | Distância (km) |
|---|---|---|---|
| b7d5b4b8… | florianopolis | SC | 0,5 |
| b5e3e386… | florianopolis | SC | 0,5 |
| ec846398… | florianopolis | SC | 0,5 |
| ea3ebea5… | florianopolis | SC | 2,6 |
| 5c030029… | sao jose | SC | 3,4 |

## 8. Exemplo de documento na coleção pedidos

```json
{
  "_id": "e481f51cbdc54678b7cc49136f2d6af7",
  "status": "delivered",
  "datas": {
    "compra": {
      "$date": "2017-10-02T10:56:33Z"
    },
    "aprovacao": {
      "$date": "2017-10-02T11:07:15Z"
    },
    "envio_transportadora": {
      "$date": "2017-10-04T19:55:00Z"
    },
    "entrega_cliente": {
      "$date": "2017-10-10T21:25:13Z"
    },
    "entrega_estimada": {
      "$date": "2017-10-18T00:00:00Z"
    }
  },
  "cliente": {
    "id_cliente_unico": "7c396fd4830fd04220f754e42b4e5bff",
    "id_cliente_pedido": "9ef432eb6251297304e76186b10a928d",
    "cidade": "sao paulo",
    "uf": "SP"
  },
  "itens": [
    {
      "seq": 1,
      "id_produto": "87285b34884572647811a353c7ac498a",
      "categoria": "utilidades_domesticas",
      "id_vendedor": "3504c0cb71d7fa48d967e0e4c94d59d9",
      "uf_vendedor": "SP",
      "preco": {
        "$numberDecimal": "29.99"
      },
      "frete": {
        "$numberDecimal": "8.72"
      },
      "data_limite_envio": {
        "$date": "2017-10-06T11:07:15Z"
      }
    }
  ],
  "pagamentos": [
    {
      "sequencial": 1,
      "tipo": "credit_card",
      "parcelas": 1,
      "valor": {
        "$numberDecimal": "18.12"
      }
    },
    {
      "sequencial": 2,
      "tipo": "voucher",
      "parcelas": 1,
      "valor": {
        "$numberDecimal": "18.59"
      }
    },
    {
      "sequencial": 3,
      "tipo": "voucher",
      "parcelas": 1,
      "valor": {
        "$numberDecimal": "2.00"
      }
    }
  ],
  "avaliacoes": [
    {
      "id_avaliacao": "a54f0611adc9ed256b57ede6b6eb5114",
      "nota": 4,
      "comentario": "Não testei o produto ainda, mas ele veio correto e em boas condições. Apenas a caixa que veio bem amassada e danificada, o que ficará chato, pois se trata de um presente.",
      "data_criacao": {
        "$date": "2017-10-11T00:00:00Z"
      },
      "data_resposta": {
        "$date": "2017-10-12T03:43:48Z"
      }
    }
  ],
  "metricas": {
    "valor_itens": {
      "$numberDecimal": "29.99"
    },
    "valor_frete": {
      "$numberDecimal": "8.72"
    },
    "valor_pago": {
      "$numberDecimal": "38.71"
    },
    "qtd_itens": 1,
    "dias_entrega": 8,
    "atraso_dias": -8,
    "no_prazo": true
  }
}
```
