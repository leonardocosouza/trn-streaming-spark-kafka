# Demo de streaming: consome o tópico "pedidos" do Kafka com Structured Streaming.
#
# Duas consultas leem o mesmo tópico:
#   bronze  -> grava cada pedido em Parquet (exactly-once, usado no SQL do painel)
#   janelas -> soma pedidos e receita por região em janelas de 30s, com watermark
#
# O consumo não começa sozinho: este driver obedece ao arquivo de controle gravado
# pelo painel, e publica as métricas de cada lote para o painel exibir.
import json
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType, DoubleType

KAFKA = "kafka:9092"
TOPICO = "pedidos"
CONTROLE = "/controle"
DEMO = "/opt/spark/demo"
BRONZE = f"{DEMO}/bronze"
ARQ_ESTADO = f"{CONTROLE}/estado.json"
ARQ_JANELAS = f"{CONTROLE}/janelas.json"
ARQ_METRICAS = f"{CONTROLE}/metricas.jsonl"
MAX_JANELAS = 60

ESQUEMA = StructType([
    StructField("pedido_id", StringType()),
    StructField("evento_ts", TimestampType()),
    StructField("regiao", StringType()),
    StructField("produto", StringType()),
    StructField("quantidade", IntegerType()),
    StructField("valor", DoubleType()),
    StructField("status", StringType()),
])


def ler_estado():
    try:
        with open(ARQ_ESTADO) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def gravar_json(caminho, dados):
    tmp = f"{caminho}.tmp"
    with open(tmp, "w") as f:
        json.dump(dados, f)
    os.replace(tmp, caminho)


def carregar_janelas():
    try:
        with open(ARQ_JANELAS) as f:
            return {(j["inicio"], j["regiao"]): j for j in json.load(f)}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


janelas = carregar_janelas()


def grava_janelas(df, _batch_id):
    """Modo update: cada lote traz só as janelas alteradas, com o total acumulado."""
    for r in df.collect():
        inicio = r["janela"]["start"].isoformat()
        janelas[(inicio, r["regiao"])] = {
            "inicio": inicio,
            "regiao": r["regiao"],
            "pedidos": r["pedidos"],
            "receita": round(r["receita"], 2),
        }
    recentes = sorted({k[0] for k in janelas})[-MAX_JANELAS:]
    for chave in [k for k in janelas if k[0] not in recentes]:
        del janelas[chave]
    gravar_json(ARQ_JANELAS, sorted(janelas.values(), key=lambda j: (j["inicio"], j["regiao"])))


def iniciar_consultas(spark, trigger_s, max_por_lote):
    pedidos = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
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
        .format("parquet")
        .option("path", BRONZE)
        .option("checkpointLocation", f"{DEMO}/checkpoints/bronze")
        .outputMode("append")
        .trigger(processingTime=intervalo)
        .start()
    )
    janela = (
        pedidos.withWatermark("evento_ts", "1 minute")
        .groupBy(F.window("evento_ts", "30 seconds").alias("janela"), "regiao")
        .agg(F.count("*").alias("pedidos"), F.sum("valor").alias("receita"))
        .writeStream.queryName("janelas")
        .outputMode("update")
        .foreachBatch(grava_janelas)
        .option("checkpointLocation", f"{DEMO}/checkpoints/janelas")
        .trigger(processingTime=intervalo)
        .start()
    )
    return [bronze, janela]


def publicar_metricas(consultas, vistos):
    with open(ARQ_METRICAS, "a") as f:
        for q in consultas:
            for p in q.recentProgress:
                chave = (p["name"], p["timestamp"])
                if chave not in vistos:
                    vistos.add(chave)
                    f.write(json.dumps(p) + "\n")


def main():
    spark = SparkSession.builder.appName("demo-streaming-pedidos").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    consultas, config, vistos = [], None, set()

    while True:
        estado = ler_estado()
        desejada = (estado.get("trigger_s", 5), estado.get("max_por_lote", 1000))

        if consultas and (not estado.get("consumindo") or desejada != config):
            print("Parando o consumo (o checkpoint guarda a posição)", flush=True)
            for q in consultas:
                q.stop()
            publicar_metricas(consultas, vistos)
            consultas = []

        if estado.get("consumindo") and not consultas:
            print(f"Iniciando o consumo: trigger={desejada[0]}s, máximo por lote={desejada[1]}", flush=True)
            consultas, config = iniciar_consultas(spark, *desejada), desejada

        for q in consultas:
            if q.exception():
                raise RuntimeError(f"Consulta {q.name} falhou: {q.exception()}")

        publicar_metricas(consultas, vistos)
        time.sleep(1)


if __name__ == "__main__":
    main()
