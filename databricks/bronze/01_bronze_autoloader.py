# Databricks notebook source
# MAGIC %md
# MAGIC # Rota Livre - 01 Bronze (Auto Loader)
# MAGIC
# MAGIC Le os CSVs enviados ao Volume `chegada` e grava **sem transformar os valores** em tabelas Delta:
# MAGIC
# MAGIC | Tabela | Arquivos |
# MAGIC |---|---|
# MAGIC | `rota_livre_bronze.sp156_solicitacoes` | `sp156_calcadas_AAAA-MM-DD.csv` |
# MAGIC | `rota_livre_bronze.infosiga_sinistros` | `infosiga_atropelamentos_sp_AAAA-MM-DD.csv` |
# MAGIC
# MAGIC **Incremental e sem duplicar:** o checkpoint do Auto Loader guarda quais arquivos ja foram lidos.
# MAGIC Uma reexecucao, ou um re-upload do mesmo arquivo, nao gera linhas novas; um lote novo carrega so os arquivos novos.
# MAGIC Cada execucao grava contagens antes/depois em `rota_livre_bronze.log_ingestao`.
# MAGIC
# MAGIC Colunas tecnicas: `_arquivo_origem`, `_arquivo_nome`, `_arquivo_modificado_em` (momento do upload),
# MAGIC `_lote` (data do upload, fuso de SP), `_data_ingestao`, `_execucao_id`, `_rescued_data`.

# COMMAND ----------

import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F

CATALOGO = "workspace"
BRONZE = f"{CATALOGO}.rota_livre_bronze"
CHEGADA = f"/Volumes/{CATALOGO}/rota_livre_bronze/chegada"
SISTEMA = f"/Volumes/{CATALOGO}/rota_livre_bronze/sistema"

FONTES = {
    "sp156_solicitacoes": {"glob": "sp156_calcadas_*.csv", "fonte": "SP156"},
    "infosiga_sinistros": {"glob": "infosiga_atropelamentos_sp_*.csv", "fonte": "INFOSIGA"},
}

EXECUCAO_ID = str(uuid.uuid4())
INICIO = datetime.now(timezone.utc)
print("execucao_id:", EXECUCAO_ID)
display(dbutils.fs.ls(CHEGADA))

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE}.log_ingestao (
  execucao_id STRING, executado_em TIMESTAMP, tabela STRING, fonte STRING,
  contagem_antes BIGINT, contagem_depois BIGINT, linhas_novas BIGINT,
  arquivos_novos ARRAY<STRING>, lotes_novos ARRAY<STRING>
) COMMENT 'Contagens da Bronze antes/depois de cada execucao do Auto Loader'
""")


def contar(tabela: str) -> int:
    return spark.table(tabela).count() if spark.catalog.tableExists(tabela) else 0


def ingerir(nome: str, cfg: dict) -> dict:
    tabela = f"{BRONZE}.{nome}"
    antes = contar(tabela)

    fluxo = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{SISTEMA}/schemas/{nome}")
        .option("cloudFiles.inferColumnTypes", "false")      # Bronze = tudo string, como na fonte
        .option("cloudFiles.schemaEvolutionMode", "rescue")  # colunas inesperadas vao para _rescued_data
        .option("pathGlobFilter", cfg["glob"])
        .option("header", "true")
        .option("sep", ";")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .option("encoding", "UTF-8")
        .load(CHEGADA)
        .select(
            "*",
            F.col("_metadata.file_path").alias("_arquivo_origem"),
            F.col("_metadata.file_name").alias("_arquivo_nome"),
            F.col("_metadata.file_modification_time").alias("_arquivo_modificado_em"),
        )
        .withColumn("_lote", F.date_format(F.from_utc_timestamp("_arquivo_modificado_em", "America/Sao_Paulo"), "yyyy-MM-dd"))
        .withColumn("_data_ingestao", F.current_timestamp())
        .withColumn("_execucao_id", F.lit(EXECUCAO_ID))
    )

    consulta = (
        fluxo.writeStream
        .option("checkpointLocation", f"{SISTEMA}/checkpoints/{nome}")
        .trigger(availableNow=True)  # serverless/Free Edition: processa o que ha de novo e para
        .toTable(tabela)
    )
    consulta.awaitTermination()

    depois = contar(tabela)
    novos = (spark.table(tabela).where(F.col("_execucao_id") == EXECUCAO_ID)
             .groupBy("_arquivo_nome", "_lote").count().collect())
    reg = {
        "execucao_id": EXECUCAO_ID, "executado_em": INICIO, "tabela": tabela, "fonte": cfg["fonte"],
        "contagem_antes": antes, "contagem_depois": depois, "linhas_novas": depois - antes,
        "arquivos_novos": sorted(r["_arquivo_nome"] for r in novos),
        "lotes_novos": sorted({r["_lote"] for r in novos}),
    }
    spark.createDataFrame([reg], spark.table(f"{BRONZE}.log_ingestao").schema) \
         .write.mode("append").saveAsTable(f"{BRONZE}.log_ingestao")
    return reg


resultado = [ingerir(n, c) for n, c in FONTES.items()]
# Exibe a partir da tabela de log (schema fixo). Criar o DataFrame direto da lista falha
# quando nao ha arquivos novos: arrays vazios nao permitem inferir o tipo (CANNOT_DETERMINE_TYPE).
display(spark.table(f"{BRONZE}.log_ingestao").where(F.col("execucao_id") == EXECUCAO_ID))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidencias
# MAGIC 1. Historico de execucoes: depois da 2a execucao com os mesmos arquivos, `linhas_novas = 0`.

# COMMAND ----------

display(spark.sql(f"""
SELECT executado_em, fonte, contagem_antes, contagem_depois, linhas_novas, arquivos_novos, lotes_novos
FROM {BRONZE}.log_ingestao ORDER BY executado_em, fonte
"""))

# COMMAND ----------

# MAGIC %md 2. Registros recebidos por lote e por arquivo.

# COMMAND ----------

display(spark.sql(f"""
SELECT 'SP156' AS fonte, _lote, _arquivo_nome, COUNT(*) AS registros, COUNT(DISTINCT _execucao_id) AS execucoes_que_carregaram
FROM {BRONZE}.sp156_solicitacoes GROUP BY ALL
UNION ALL
SELECT 'INFOSIGA', _lote, _arquivo_nome, COUNT(*), COUNT(DISTINCT _execucao_id)
FROM {BRONZE}.infosiga_sinistros GROUP BY ALL
ORDER BY fonte, _lote
"""))

# COMMAND ----------

# MAGIC %md 3. Prova de que nenhum arquivo foi carregado duas vezes (resultado esperado: zero linhas).

# COMMAND ----------

display(spark.sql(f"""
SELECT 'SP156' AS fonte, _arquivo_nome, COUNT(DISTINCT _execucao_id) AS vezes_carregado
FROM {BRONZE}.sp156_solicitacoes GROUP BY _arquivo_nome HAVING COUNT(DISTINCT _execucao_id) > 1
UNION ALL
SELECT 'INFOSIGA', _arquivo_nome, COUNT(DISTINCT _execucao_id)
FROM {BRONZE}.infosiga_sinistros GROUP BY _arquivo_nome HAVING COUNT(DISTINCT _execucao_id) > 1
"""))

# COMMAND ----------

# MAGIC %md 4. Colunas inesperadas (deve ser 0) - `_rescued_data` preenchido indica mudanca de layout na fonte.

# COMMAND ----------

display(spark.sql(f"""
SELECT 'SP156' AS fonte, COUNT(*) AS linhas_com_rescued FROM {BRONZE}.sp156_solicitacoes WHERE _rescued_data IS NOT NULL
UNION ALL
SELECT 'INFOSIGA', COUNT(*) FROM {BRONZE}.infosiga_sinistros WHERE _rescued_data IS NOT NULL
"""))
