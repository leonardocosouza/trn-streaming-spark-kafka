"""Estado da demo, compartilhado entre painel, produtor e consumidor Spark."""
import json
import os

PASTA = os.environ.get("PASTA_CONTROLE", "/controle")
ARQ_ESTADO = f"{PASTA}/estado.json"
ARQ_JANELAS = f"{PASTA}/janelas.json"
ARQ_METRICAS = f"{PASTA}/metricas.jsonl"

KAFKA = os.environ.get("KAFKA_SERVIDOR", "kafka:9092")
TOPICO = "pedidos"

# A demo começa parada: quem apresenta decide quando produzir e quando consumir.
PADRAO = {
    "produzindo": False,
    "consumindo": False,
    "taxa": 50,
    "atrasados_pct": 0,
    "trigger_s": 5,
    "max_por_lote": 1000,
}


def ler():
    try:
        with open(ARQ_ESTADO) as f:
            return {**PADRAO, **json.load(f)}
    except (FileNotFoundError, json.JSONDecodeError):
        return dict(PADRAO)


def gravar(**mudancas):
    estado = {**ler(), **mudancas}
    tmp = f"{ARQ_ESTADO}.{os.getpid()}.tmp"
    with open(tmp, "w") as f:
        json.dump(estado, f)
    os.replace(tmp, ARQ_ESTADO)
    return estado
