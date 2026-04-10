# Databricks notebook source
# MAGIC %md
# MAGIC # EVO Cohort — Análise no Databricks
# MAGIC Este notebook carrega os dados do EVO Cohort e cria tabelas para análise.
# MAGIC
# MAGIC ## Como usar:
# MAGIC 1. Faça upload do arquivo `fato_contratos.csv` para o Databricks
# MAGIC 2. Execute as células abaixo na ordem

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Carregar os dados
# MAGIC Depois de fazer upload do CSV, ajuste o caminho abaixo.

# COMMAND ----------

# Se voce fez upload via UI do Databricks, o arquivo fica em:
# /Volumes/main/default/volume_name/fato_contratos.csv
# Ajuste o caminho conforme necessario

# Opcao 1: Ler do volume (Databricks Free Edition)
try:
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "UTF-8") \
        .load("/Volumes/main/default/my_volume/fato_contratos.csv")
    print(f"Registros carregados: {df.count()}")
except Exception as e:
    print(f"Ajuste o caminho do arquivo. Erro: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Visualizar os dados

# COMMAND ----------

display(df.limit(20))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Criar tabela Delta

# COMMAND ----------

# Salva como tabela Delta (persistente no Databricks)
df.write.mode("overwrite").saveAsTable("main.default.fato_contratos")
print("Tabela 'fato_contratos' criada com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Análises SQL
# MAGIC Agora voce pode usar SQL direto!

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total de contratos por status
# MAGIC SELECT
# MAGIC   status,
# MAGIC   COUNT(*) as total,
# MAGIC   ROUND(AVG(valor_venda), 2) as ticket_medio,
# MAGIC   ROUND(SUM(valor_venda), 2) as receita_total
# MAGIC FROM main.default.fato_contratos
# MAGIC GROUP BY status
# MAGIC ORDER BY total DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Entradas por mes
# MAGIC SELECT
# MAGIC   mes_entrada,
# MAGIC   COUNT(*) as entradas,
# MAGIC   COUNT(DISTINCT id_member) as membros_unicos
# MAGIC FROM main.default.fato_contratos
# MAGIC WHERE mes_entrada IS NOT NULL
# MAGIC GROUP BY mes_entrada
# MAGIC ORDER BY mes_entrada

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cancelamentos por mes
# MAGIC SELECT
# MAGIC   mes_cancelamento,
# MAGIC   COUNT(*) as cancelamentos
# MAGIC FROM main.default.fato_contratos
# MAGIC WHERE status_id = 2
# MAGIC   AND mes_cancelamento IS NOT NULL
# MAGIC GROUP BY mes_cancelamento
# MAGIC ORDER BY mes_cancelamento

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Comparativo 2025 vs 2026 (entradas)
# MAGIC SELECT
# MAGIC   num_mes_entrada as mes,
# MAGIC   SUM(CASE WHEN ano_entrada = '2025' THEN 1 ELSE 0 END) as entradas_2025,
# MAGIC   SUM(CASE WHEN ano_entrada = '2026' THEN 1 ELSE 0 END) as entradas_2026,
# MAGIC   ROUND(
# MAGIC     (SUM(CASE WHEN ano_entrada = '2026' THEN 1 ELSE 0 END)
# MAGIC      - SUM(CASE WHEN ano_entrada = '2025' THEN 1 ELSE 0 END))
# MAGIC     * 100.0
# MAGIC     / NULLIF(SUM(CASE WHEN ano_entrada = '2025' THEN 1 ELSE 0 END), 0)
# MAGIC   , 1) as variacao_pct
# MAGIC FROM main.default.fato_contratos
# MAGIC WHERE ano_entrada IN ('2025', '2026')
# MAGIC GROUP BY num_mes_entrada
# MAGIC ORDER BY mes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 planos com mais cancelamentos
# MAGIC SELECT
# MAGIC   nome_plano,
# MAGIC   COUNT(*) as cancelamentos
# MAGIC FROM main.default.fato_contratos
# MAGIC WHERE status_id = 2
# MAGIC GROUP BY nome_plano
# MAGIC ORDER BY cancelamentos DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top filiais com mais cancelamentos
# MAGIC SELECT
# MAGIC   nome_filial,
# MAGIC   COUNT(*) as cancelamentos,
# MAGIC   ROUND(COUNT(*) * 100.0 / (
# MAGIC     SELECT COUNT(*) FROM main.default.fato_contratos WHERE status_id = 2
# MAGIC   ), 1) as percentual
# MAGIC FROM main.default.fato_contratos
# MAGIC WHERE status_id = 2
# MAGIC GROUP BY nome_filial
# MAGIC ORDER BY cancelamentos DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Motivos de cancelamento
# MAGIC SELECT
# MAGIC   COALESCE(motivo_cancelamento, 'Nao informado') as motivo,
# MAGIC   COUNT(*) as quantidade,
# MAGIC   ROUND(COUNT(*) * 100.0 / (
# MAGIC     SELECT COUNT(*) FROM main.default.fato_contratos WHERE status_id = 2
# MAGIC   ), 1) as percentual
# MAGIC FROM main.default.fato_contratos
# MAGIC WHERE status_id = 2
# MAGIC GROUP BY motivo_cancelamento
# MAGIC ORDER BY quantidade DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Taxa de churn mensal
# MAGIC WITH entradas AS (
# MAGIC   SELECT mes_entrada AS mes, COUNT(*) AS total_entradas
# MAGIC   FROM main.default.fato_contratos
# MAGIC   WHERE mes_entrada IS NOT NULL
# MAGIC   GROUP BY mes_entrada
# MAGIC ),
# MAGIC cancelamentos AS (
# MAGIC   SELECT mes_cancelamento AS mes, COUNT(*) AS total_cancel
# MAGIC   FROM main.default.fato_contratos
# MAGIC   WHERE status_id = 2 AND mes_cancelamento IS NOT NULL
# MAGIC   GROUP BY mes_cancelamento
# MAGIC )
# MAGIC SELECT
# MAGIC   COALESCE(e.mes, c.mes) AS mes,
# MAGIC   COALESCE(e.total_entradas, 0) AS entradas,
# MAGIC   COALESCE(c.total_cancel, 0) AS cancelamentos,
# MAGIC   ROUND(COALESCE(c.total_cancel, 0) * 100.0 / NULLIF(COALESCE(e.total_entradas, 0), 0), 1) AS taxa_churn_pct
# MAGIC FROM entradas e
# MAGIC FULL OUTER JOIN cancelamentos c ON e.mes = c.mes
# MAGIC ORDER BY mes

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Tempo medio ate cancelamento

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ROUND(AVG(dias_ate_cancelamento), 0) as media_dias,
# MAGIC   ROUND(AVG(meses_ate_cancelamento), 1) as media_meses,
# MAGIC   MIN(dias_ate_cancelamento) as min_dias,
# MAGIC   MAX(dias_ate_cancelamento) as max_dias
# MAGIC FROM main.default.fato_contratos
# MAGIC WHERE status_id = 2
# MAGIC   AND dias_ate_cancelamento IS NOT NULL
