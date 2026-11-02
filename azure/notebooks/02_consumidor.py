# Databricks notebook source
# MAGIC %md
# MAGIC # 2. Consumidor
# MAGIC Duas consultas leem o mesmo tópico:
# MAGIC - **bronze:** grava cada pedido em Delta, usado nas perguntas em SQL.
# MAGIC - **janelas:** soma pedidos e receita por região em janelas de 30 s, com watermark.
# MAGIC
# MAGIC **Iniciar:** execute todas as células. **Parar:** interrompa a última célula.
# MAGIC O checkpoint guarda a posição, e ao iniciar de novo o consumo continua de onde parou.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.text("trigger_s", "5", "Intervalo do micro-lote (s)")
dbutils.widgets.text("max_por_lote", "1000", "Máximo por lote")

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

spark.conf.set("spark.sql.shuffle.partitions", "4")

ESQUEMA = StructType([
    StructField("pedido_id", StringType()),
    StructField("evento_ts", TimestampType()),
    StructField("regiao", StringType()),
    StructField("produto", StringType()),
    StructField("quantidade", IntegerType()),
    StructField("valor", DoubleType()),
    StructField("status", StringType()),
])

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {JANELAS}
    (inicio TIMESTAMP, regiao STRING, pedidos BIGINT, receita DOUBLE)
""")


def grava_janelas(df, _batch_id):
    """Modo update: cada lote traz só as janelas alteradas, com o total acumulado."""
    origem = df.select(F.col("janela.start").alias("inicio"), "regiao", "pedidos", "receita")
    (
        DeltaTable.forName(df.sparkSession, JANELAS).alias("t")
        .merge(origem.alias("s"), "t.inicio = s.inicio AND t.regiao = s.regiao")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def iniciar_consultas(trigger_s, max_por_lote):
    pedidos = (
        spark.readStream.format("kafka")
        .options(**KAFKA)
        .option("subscribe", TOPICO)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", max_por_lote)
        .option("failOnDataLoss", "false")
        .load()
        .select(
            F.from_json(F.col("value").cast("string"), ESQUEMA).alias("p"),
            F.col("partition").alias("particao"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .select("p.*", "particao", "offset", "kafka_ts")
    )

    intervalo = f"{trigger_s} seconds"
    bronze = (
        pedidos.writeStream.queryName("bronze")
        .option("checkpointLocation", f"{CHECKPOINTS}/bronze")
        .outputMode("append")
        .trigger(processingTime=intervalo)
        .toTable(BRONZE)
    )
    janela = (
        pedidos.withWatermark("evento_ts", "1 minute")
        .groupBy(F.window("evento_ts", "30 seconds").alias("janela"), "regiao")
        .agg(F.count("*").alias("pedidos"), F.sum("valor").alias("receita"))
        .writeStream.queryName("janelas")
        .outputMode("update")
        .foreachBatch(grava_janelas)
        .option("checkpointLocation", f"{CHECKPOINTS}/janelas")
        .trigger(processingTime=intervalo)
        .start()
    )
    return [bronze, janela]


def publicar_metricas(consultas, metricas):
    for q in consultas:
        for progresso in q.recentProgress:
            # No Databricks, as taxas ficam em "sources", só no JSON do progresso.
            p = json.loads(progresso.json) if hasattr(progresso, "json") else progresso
            estado = p.get("stateOperators") or [{}]
            metricas[f'{p["name"]}-{p["timestamp"]}'] = {
                "consulta": p["name"],
                "momento": p["timestamp"],
                "entrada": sum(s.get("inputRowsPerSecond") or 0 for s in p["sources"]),
                "processamento": sum(s.get("processedRowsPerSecond") or 0 for s in p["sources"]),
                "duracao_ms": p.get("batchDuration") or 0,
                "descartados": estado[0].get("numRowsDroppedByWatermark") or 0,
            }
    recentes = sorted(metricas.items(), key=lambda m: m[1]["momento"])[-400:]
    gravar_json(ARQ_METRICAS, dict(recentes))

# COMMAND ----------

for q in spark.streams.active:
    q.stop()

trigger_s = int(dbutils.widgets.get("trigger_s"))
max_por_lote = int(dbutils.widgets.get("max_por_lote"))
metricas = ler_json(ARQ_METRICAS, {})
consultas = iniciar_consultas(trigger_s, max_por_lote)
print(f"Consumindo: lote a cada {trigger_s}s, máximo de {max_por_lote}. Interrompa a célula para parar.")

try:
    while True:
        for q in consultas:
            if q.exception():
                raise RuntimeError(f"Consulta {q.name} falhou: {q.exception()}")
        publicar_metricas(consultas, metricas)
        time.sleep(2)
finally:
    for q in consultas:
        q.stop()
    print("Consumo parado. O checkpoint guardou a posição.")
