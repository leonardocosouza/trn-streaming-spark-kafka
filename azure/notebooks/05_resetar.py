# Databricks notebook source
# MAGIC %md
# MAGIC # 5. Resetar
# MAGIC Apaga tabelas, checkpoints e métricas da demo. Antes de executar:
# MAGIC 1. Interrompa os notebooks do produtor, do consumidor e do painel.
# MAGIC 2. Recrie o hub `pedidos`, como descrito no documento do ambiente. Sem isso, as mensagens antigas seriam consumidas de novo.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {BRONZE}")
spark.sql(f"DROP TABLE IF EXISTS {JANELAS}")
for pasta in (CHECKPOINTS, CONTROLE):
    for item in dbutils.fs.ls(pasta):
        dbutils.fs.rm(item.path, True)
print("Demo zerada.")
