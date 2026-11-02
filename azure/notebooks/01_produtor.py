# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Produtor
# MAGIC Publica pedidos fictícios no tópico `pedidos` do Event Hubs, pelo protocolo Kafka.
# MAGIC
# MAGIC - **Iniciar:** execute todas as células.
# MAGIC - **Parar ou mudar o ritmo:** interrompa a última célula, ajuste os campos e execute-a de novo.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.text("taxa", "50", "Pedidos por segundo")
dbutils.widgets.text("atrasados_pct", "0", "% de eventos atrasados (5 min)")

# COMMAND ----------

import random
import uuid
from datetime import datetime, timedelta, timezone

REGIOES = {"Sudeste": 45, "Sul": 20, "Nordeste": 18, "Centro-Oeste": 10, "Norte": 7}
PRODUTOS = {"Notebook": 3500, "Celular": 2200, "Fone": 250, "Monitor": 1200, "Teclado": 180}


def novo_pedido(atrasados_pct):
    momento = datetime.now(timezone.utc)
    if random.uniform(0, 100) < atrasados_pct:
        momento -= timedelta(minutes=5)
    produto = random.choice(list(PRODUTOS))
    quantidade = random.randint(1, 3)
    return {
        "pedido_id": str(uuid.uuid4()),
        "evento_ts": momento.isoformat(),
        "regiao": random.choices(list(REGIOES), weights=REGIOES.values())[0],
        "produto": produto,
        "quantidade": quantidade,
        "valor": round(PRODUTOS[produto] * quantidade * random.uniform(0.9, 1.1), 2),
        "status": random.choices(["aprovado", "cancelado"], weights=[95, 5])[0],
    }

# COMMAND ----------

taxa = int(dbutils.widgets.get("taxa"))
atrasados_pct = float(dbutils.widgets.get("atrasados_pct"))
total = ler_json(ARQ_PRODUZIDAS, {"produzidas": 0})["produzidas"]
inicio, enviadas = time.time(), 0
print(f"Produzindo {taxa} pedidos/s, com {atrasados_pct}% atrasados. Interrompa a célula para parar.")

while True:
    inicio_lote = time.time()
    # Calcula pelo tempo decorrido: se um envio demorar, o próximo compensa.
    devidas = min(int(taxa * (inicio_lote - inicio)) - enviadas, taxa * 5)
    if devidas > 0:
        pedidos = [novo_pedido(atrasados_pct) for _ in range(devidas)]
        linhas = [(p["pedido_id"], json.dumps(p)) for p in pedidos]
        (
            spark.createDataFrame(linhas, "key string, value string")
            .write.format("kafka")
            .options(**KAFKA)
            .option("topic", TOPICO)
            .save()
        )
        enviadas += devidas
        total += devidas
        gravar_json(ARQ_PRODUZIDAS, {"produzidas": total})
    time.sleep(max(0, 1 - (time.time() - inicio_lote)))
