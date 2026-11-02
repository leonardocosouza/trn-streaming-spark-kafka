# Databricks notebook source
# MAGIC %md
# MAGIC # 3. Painel
# MAGIC Mostra produção, consumo, lag e lotes, atualizando a cada 5 segundos.
# MAGIC Execute todas as células. Interromper este notebook não afeta o produtor nem o consumidor.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from datetime import datetime

import matplotlib.pyplot as plt
from IPython.display import clear_output, display

historico = []

while True:
    produzidas = ler_json(ARQ_PRODUZIDAS, {"produzidas": 0})["produzidas"]
    consumidas = spark.table(BRONZE).count() if spark.catalog.tableExists(BRONZE) else 0
    lotes = list(ler_json(ARQ_METRICAS, {}).values())
    bronze = [m for m in lotes if m["consulta"] == "bronze"][-30:]
    descartados = sum(m["descartados"] for m in lotes if m["consulta"] == "janelas")
    lag = max(produzidas - consumidas, 0)
    historico = (historico + [(datetime.now(), lag)])[-120:]

    figura, (esquerda, direita) = plt.subplots(1, 2, figsize=(13, 3.6))
    esquerda.fill_between(*zip(*historico), alpha=0.4)
    esquerda.set_title("Lag ao longo do tempo")
    esquerda.tick_params(axis="x", labelrotation=30)
    direita.plot([m["entrada"] for m in bronze], label="Entrada/s", marker="o")
    direita.plot([m["processamento"] for m in bronze], label="Processamento/s", marker="o")
    direita.set_title("Entrada e processamento por lote")
    direita.legend()
    figura.tight_layout()

    clear_output(wait=True)
    print(f"Produzidas: {produzidas:>8,}   Consumidas: {consumidas:>8,}   Lag: {lag:>8,}".replace(",", "."))
    if bronze:
        print(f'Duração do último lote: {bronze[-1]["duracao_ms"] / 1000:.2f} s')
    print(f"Eventos atrasados descartados (lotes recentes): {descartados}")
    display(figura)
    plt.close(figura)
    time.sleep(5)
