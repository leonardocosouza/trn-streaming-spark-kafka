# Databricks notebook source
# MAGIC %md
# MAGIC # Configuração da demo
# MAGIC Os outros notebooks carregam este com `%run`. Não é preciso executá-lo sozinho.

# COMMAND ----------

import json
import time

# Troque aqui se o namespace foi criado com outro nome.
NAMESPACE = "evh-streaming-demo"
TOPICO = "pedidos"

BRONZE = "streaming_demo.pedidos.bronze"
JANELAS = "streaming_demo.pedidos.janelas"
VOLUMES = "/Volumes/streaming_demo/pedidos"
CHECKPOINTS = f"{VOLUMES}/checkpoints"
CONTROLE = f"{VOLUMES}/controle"
ARQ_PRODUZIDAS = f"{CONTROLE}/produzidas.json"
ARQ_METRICAS = f"{CONTROLE}/metricas.json"

conexao = dbutils.secrets.get("streaming-demo", "eventhub-connection")
KAFKA = {
    "kafka.bootstrap.servers": f"{NAMESPACE}.servicebus.windows.net:9093",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="$ConnectionString" password="{conexao}";'
    ),
    "kafka.request.timeout.ms": "60000",
    "kafka.session.timeout.ms": "30000",
}


def ler_json(caminho, padrao):
    # O Volume pode falhar se outro notebook gravar o arquivo durante a leitura.
    for _ in range(5):
        try:
            with open(caminho) as f:
                return json.load(f)
        except FileNotFoundError:
            return padrao
        except (OSError, json.JSONDecodeError):
            time.sleep(0.3)
    return padrao


def gravar_json(caminho, dados):
    for _ in range(5):
        try:
            with open(caminho, "w") as f:
                json.dump(dados, f)
            return
        except OSError:
            time.sleep(0.3)
