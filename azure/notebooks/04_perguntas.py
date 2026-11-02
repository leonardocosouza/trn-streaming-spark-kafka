# Databricks notebook source
# MAGIC %md
# MAGIC # 4. Perguntas
# MAGIC Consultas sobre os pedidos já consumidos. Edite o SQL com as dúvidas do público.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quantos pedidos e qual a receita até agora?
# MAGIC SELECT count(*) AS pedidos, round(sum(valor), 2) AS receita
# MAGIC FROM streaming_demo.pedidos.bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Qual região vende mais?
# MAGIC SELECT regiao, count(*) AS pedidos, round(sum(valor), 2) AS receita
# MAGIC FROM streaming_demo.pedidos.bronze
# MAGIC GROUP BY regiao ORDER BY receita DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Qual produto vende mais?
# MAGIC SELECT produto, sum(quantidade) AS unidades, round(sum(valor), 2) AS receita
# MAGIC FROM streaming_demo.pedidos.bronze
# MAGIC GROUP BY produto ORDER BY receita DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Qual a taxa de cancelamento por região?
# MAGIC SELECT regiao, round(100.0 * count(*) FILTER (WHERE status = 'cancelado') / count(*), 1) AS pct_cancelado
# MAGIC FROM streaming_demo.pedidos.bronze
# MAGIC GROUP BY regiao ORDER BY pct_cancelado DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quanto tempo o pedido leva do evento até o Kafka?
# MAGIC SELECT round(avg(timestampdiff(MILLISECOND, evento_ts, kafka_ts)) / 1000.0, 3) AS atraso_medio_s
# MAGIC FROM streaming_demo.pedidos.bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Receita por região nas janelas mais recentes
# MAGIC SELECT inicio, regiao, pedidos, round(receita, 2) AS receita
# MAGIC FROM streaming_demo.pedidos.janelas
# MAGIC ORDER BY inicio DESC, receita DESC
# MAGIC LIMIT 25

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Nenhuma mensagem foi duplicada?
# MAGIC SELECT count(*) AS linhas, count(DISTINCT pedido_id) AS pedidos_distintos
# MAGIC FROM streaming_demo.pedidos.bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência final
# MAGIC Compara o tópico com a tabela. Execute depois de parar o produtor e esperar o lag zerar.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

no_topico = (
    spark.read.format("kafka")
    .options(**KAFKA)
    .option("subscribe", TOPICO)
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
    .count()
)
tabela = spark.table(BRONZE)
print(f"No tópico: {no_topico}")
print(f"Na tabela: {tabela.count()}")
print(f"Pedidos distintos: {tabela.select('pedido_id').distinct().count()}")
