# Pipeline de Big Data: Olist → MongoDB

Avaliação N3 de **Desenvolvimento com Linguagens Disruptivas I** (UNICESUSC, ADS).
Implementação do pipeline de ingestão especificado na N2, usando o
[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

**Equipe:** Hevelyn Leivas, Itallo Lugon, João Miguel e Maisa Carolliny

---

## O que o pipeline faz

```
 Kaggle / espelho Olist          Apache Spark                 Apache Spark            MongoDB
 ┌───────────────┐   ┌──────────────────────────┐   ┌──────────────────────┐   ┌──────────────────┐
 │ 1. CAPTURA    │──▶│ 2. VALIDAÇÃO E LIMPEZA   │──▶│ 4. AGREGADOS         │──▶│ 5. CARGA         │──▶ 6. ANÁLISES
 │ 9 CSV (125MB) │   │ 3. PARQUET (staging)     │   │ documentos (gold)    │   │ upsert + índices │
 └───────────────┘   └──────────────────────────┘   └──────────────────────┘   └──────────────────┘
   data/raw/<data>      data/staging/<tabela>           data/gold/*.jsonl         banco "olist"
```

| Etapa | Arquivo | O que faz |
|---|---|---|
| 1. Captura | `pipeline/etapa1_captura.py` | Baixa os 9 CSV (API do Kaggle, ou o espelho oficial da Olist no GitHub se não houver credencial). Grava sem alteração em `data/raw/<data>/` com um manifesto SHA-256 para detectar se algo mudou. |
| 2 e 3. Limpeza + Parquet | `pipeline/etapa2_limpeza.py` | Lê com esquema declarado no Spark, aplica as regras de qualidade da N2 e grava um Parquet por tabela em `data/staging/`. Pedidos e itens são particionados por ano-mês. Linhas inválidas vão para `data/rejeitados/`. |
| 4. Agregados | `pipeline/etapa3_agregados.py` | Monta os documentos do modelo de agregados: itens, pagamentos e avaliações viram listas dentro do pedido, e as métricas de entrega e valor são calculadas. Saída em `data/gold/*.jsonl` (JSON estendido, mantém datas e decimais). |
| 5. Carga | `pipeline/etapa4_carga.py` | Cria as coleções com validador `$jsonSchema`, grava em lotes com **upsert pelo `_id`** (rodar de novo não duplica), cria os índices (inclusive `2dsphere`) e confere as contagens. |
| 6. Análises | `pipeline/etapa5_analises.py` | Roda as consultas de agregação e grava o resultado em `saidas/analises.md`. |

### Regras de qualidade aplicadas (N2, Tabela 2)

| Problema encontrado | Tratamento |
|---|---|
| 23.995 CEPs de clientes sem o zero à esquerda (`9790` em vez de `09790`) | CEP lido como texto e completado com zeros |
| 261.831 linhas repetidas na geolocalização | Duplicadas removidas; média de lat/lng por prefixo de CEP |
| 610 produtos sem categoria; 2 categorias sem tradução | `sem_categoria`; tradução complementada |
| Colunas com erro de digitação (`product_name_lenght`) | Renomeadas |
| 789 avaliações ligadas a mais de um pedido | Chave composta (avaliação + pedido) |
| 11 pagamentos com valor ou parcela zero | Marcados com `suspeito: true` |
| 775 pedidos sem itens, 2.965 sem data de entrega | Mantidos (é o status do pedido, não erro) |
| Pagamento diferente de preço + frete | 323 pedidos listados em `data/rejeitados/relatorio_divergencia_pagamento` |

### Coleções no MongoDB

| Coleção | Documentos | Conteúdo |
|---|---|---|
| `pedidos` | 99.441 | pedido + `datas`, `cliente`, `itens[]`, `pagamentos[]`, `avaliacoes[]`, `metricas` |
| `clientes` | 96.096 | cliente único + `localizacao` com ponto GeoJSON |
| `produtos` | 32.951 | categoria (pt/en) e dimensões |
| `vendedores` | 3.095 | `localizacao` com ponto GeoJSON |

---

## Como executar

### 1. Pré-requisitos

| Ferramenta | Versão | Observação |
|---|---|---|
| Python | 3.10 a 3.13 | `python --version` |
| Java (JDK) | **17 ou 21** | exigido pelo Spark 4. Windows: [Eclipse Temurin](https://adoptium.net/) e marque "Set JAVA_HOME" na instalação. `java -version` |
| MongoDB | 6 ou mais novo | local, Docker ou Atlas (veja abaixo) |

### 2. Subir o MongoDB (escolha uma opção)

- **Já tem MongoDB instalado:** só deixe o serviço rodando em `localhost:27017`.
- **Docker:** `docker compose up -d`
- **MongoDB Atlas:** defina a variável `MONGO_URI` com a sua string de conexão (passo 4).

### 3. Instalar as dependências

```bash
git clone https://github.com/itallolugon/pipeline-olist-mongodb.git
cd pipeline-olist-mongodb
python -m venv .venv

# Windows (PowerShell)
.venv\Scripts\Activate.ps1
# Linux / macOS
source .venv/bin/activate

pip install -r requirements.txt
```

### 4. (Opcional) Configurar

| Variável | Padrão | Para quê |
|---|---|---|
| `MONGO_URI` | `mongodb://localhost:27017` | conexão com o MongoDB (ex.: Atlas) |
| `MONGO_DB` | `olist` | nome do banco |
| `KAGGLE_USERNAME` / `KAGGLE_KEY` | - | baixar pela API do Kaggle. Sem elas, o download vem do espelho oficial da Olist no GitHub |

```powershell
# exemplo no PowerShell
$env:MONGO_URI = "mongodb+srv://usuario:senha@cluster.mongodb.net"
```

### 5. Rodar

```bash
python main.py
```

Leva uns 2 minutos. Outras formas:

```bash
python main.py limpeza                  # atalho: da limpeza em diante, sem baixar de novo
python main.py --etapa carga            # roda só uma etapa
python main.py --a-partir-de agregados  # roda desta etapa em diante
python main.py --origem C:\Downloads\olist   # usa CSV já baixados (zip do Kaggle extraído)
```

Etapas: `captura`, `limpeza`, `agregados`, `carga`, `analises`.

### 6. Conferir no MongoDB

```bash
mongosh "mongodb://localhost:27017/olist" consultas_mongosh.js
```

Ou abra o **MongoDB Compass**, conecte em `mongodb://localhost:27017` e veja o banco `olist`.

---

## Saídas esperadas

Os arquivos abaixo são de uma execução real e estão versionados na pasta `saidas/`:

- [`saidas/execucao.log`](saidas/execucao.log): log completo da execução
- [`saidas/relatorio_qualidade.json`](saidas/relatorio_qualidade.json): linhas lidas, gravadas, rejeitadas e contagem de cada regra
- [`saidas/analises.md`](saidas/analises.md): resultado de todas as consultas

Final do log:

```
ETAPA 5 - CARGA NO MONGODB (upsert + $jsonSchema + índices)
  produtos: 32,951 enviados, 32,951 no banco, 32,951 gerados -> OK
  vendedores: 3,095 enviados, 3,095 no banco, 3,095 gerados -> OK
  clientes: 96,096 enviados, 96,096 no banco, 96,096 gerados -> OK
  pedidos: 99,441 enviados, 99,441 no banco, 99,441 gerados -> OK
```

Principal resultado (pergunta do cenário da N2):

| Entrega | Nota média | Pedidos |
|---|---|---|
| No prazo | 4,29 | 88.163 |
| Atrasado | 2,57 | 7.661 |

8,1% dos pedidos entregues chegaram depois do prazo, e esses pedidos têm nota média quase 2 pontos menor.

---

## Problemas comuns

| Erro | Solução |
|---|---|
| `JAVA_HOME is not set` / `Java gateway process exited` | Instale o JDK 17 ou 21 e reabra o terminal. Confira com `java -version`. |
| `ServerSelectionTimeoutError` | O MongoDB não está rodando ou a `MONGO_URI` está errada. |
| No Windows, aviso sobre `winutils.exe` / `HADOOP_HOME` | É só aviso. O projeto grava Parquet pelo PyArrow justamente para não precisar do winutils. |
| Python não encontrado pelo Spark no Windows | Rode com o ambiente virtual ativado. O projeto já aponta o Spark para o mesmo Python. |

## Estrutura

```
pipeline-olist-mongodb/
├── main.py                 # orquestra as etapas
├── pipeline/
│   ├── config.py           # caminhos, MONGO_URI, lista de arquivos
│   ├── etapa1_captura.py
│   ├── etapa2_limpeza.py
│   ├── etapa3_agregados.py
│   ├── etapa4_carga.py
│   ├── etapa5_analises.py
│   ├── parquet_io.py       # Parquet via PyArrow (funciona no Windows sem winutils)
│   ├── spark.py            # SparkSession local
│   └── log.py
├── consultas_mongosh.js    # consultas para demonstração ao vivo
├── docker-compose.yml      # MongoDB opcional via Docker
├── requirements.txt
└── saidas/                 # saídas de uma execução real
```

`data/` é gerada na execução e não vai para o Git.
