# Databricks notebook source
# MAGIC %md
# MAGIC # Rota Livre - 02 Prata (tipos, nulos, qualidade e MERGE)
# MAGIC
# MAGIC Entrada: execucoes da Bronze ainda nao processadas (tabela `controle_processamento`).
# MAGIC Saidas em `workspace.rota_livre_prata`:
# MAGIC
# MAGIC | Tabela | Chave do MERGE | Conteudo |
# MAGIC |---|---|---|
# MAGIC | `sp156_barreiras` | `id_ocorrencia` | solicitacoes validas de barreiras em calcada |
# MAGIC | `infosiga_atropelamentos` | `id_sinistro` | atropelamentos de pedestres validos na capital |
# MAGIC | `sp156_rejeitados`, `infosiga_rejeitados` | - | quarentena, com o motivo de cada rejeicao |
# MAGIC | `log_qualidade` | - | quantidade barrada **por regra** em cada execucao |
# MAGIC | `log_merge` | - | entrada, duplicidades evitadas, novos, atualizados, existentes |
# MAGIC | `ref_categoria_barreira` | - | dominio de servicos SP156 considerados barreira |
# MAGIC
# MAGIC ### Regras de qualidade
# MAGIC | Regra | SP156 | Infosiga | Por que |
# MAGIC |---|---|---|---|
# MAGIC | **R1_COORDENADA** | lat/lon ausente, invalida ou fora do municipio | idem | sem localizacao a barreira nao entra no mapa nem no cruzamento territorial |
# MAGIC | **R2_DATA** | abertura nula, futura, < 2015, fora do trimestre do arquivo, ou finalizacao < abertura | data nula, futura ou < 2015 | datas erradas distorcem evolucao temporal e tempo de resolucao |
# MAGIC | **R3_DOMINIO** | servico fora de `ref_categoria_barreira` | nao e atropelamento de pedestre, fora da capital ou tipo de registro desconhecido | a coleta usa palavras-chave amplas; o dominio separa barreira de calcada de "tapa-buraco", "isencao de rodizio PCD" etc. |
# MAGIC
# MAGIC ### Tratamentos especificos (encontrados no perfil dos dados reais)
# MAGIC - **Formato de data por arquivo**: o SP156 2026-T3 vem em `MM/dd/yyyy`; os demais em `dd/MM/yyyy`. O formato e detectado por trimestre (qual posicao tem valores > 12).
# MAGIC - **SP156 sem ID de protocolo**: `id_ocorrencia = sha256(atributos imutaveis + ordinal)`. O ordinal separa solicitacoes identicas (mesmo dia, servico e endereco), que sao pedidos distintos de pessoas diferentes.
# MAGIC - **Nulos**: texto vazio vira NULL; subprefeitura/distrito ausentes viram `NAO INFORMADO`; contagens do Infosiga vazias viram 0; CEP com 7 digitos recebe o zero a esquerda perdido.

# COMMAND ----------

import uuid
from datetime import datetime, timezone

from pyspark.sql import Window as W
from pyspark.sql import functions as F

dbutils.widgets.dropdown("reprocessar_tudo", "nao", ["nao", "sim"], "Reprocessar toda a Bronze?")
REPROCESSAR = dbutils.widgets.get("reprocessar_tudo") == "sim"

CAT = "workspace"
BRONZE, PRATA = f"{CAT}.rota_livre_bronze", f"{CAT}.rota_livre_prata"
EXECUCAO_ID = str(uuid.uuid4())
AGORA = datetime.now(timezone.utc)
LAT_MIN, LAT_MAX, LON_MIN, LON_MAX = -24.01, -23.35, -46.83, -46.36  # municipio de Sao Paulo (bbox)
DATA_MIN = "2015-01-01"
print("execucao_id:", EXECUCAO_ID, "| reprocessar_tudo:", REPROCESSAR)

_ACENTOS = "áàâãäéèêëíìîïóòôõöúùûüçñÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑ"
_SEM = "aaaaaeeeeiiiiooooouuuucnAAAAAEEEEIIIIOOOOOUUUUCN"


def norm(c):
    """minusculas, sem acento, so letras/numeros separados por 1 espaco."""
    return F.trim(F.regexp_replace(F.lower(F.translate(F.coalesce(c, F.lit("")), _ACENTOS, _SEM)), "[^a-z0-9]+", " "))


def txt(nome):
    """trim + string vazia -> NULL"""
    return F.expr(f"nullif(trim({nome}), '')")


def dbl(nome, virgula=False):
    v = f"replace({nome}, ',', '.')" if virgula else nome
    return F.expr(f"try_cast(nullif(trim({v}), '') as double)")


def inteiro0(nome):
    return F.coalesce(F.expr(f"try_cast(nullif(trim({nome}), '') as int)"), F.lit(0))


def data_txt(nome, formato):
    return F.expr(f"to_date(try_to_timestamp(nullif(trim(substr({nome}, 1, 10)), ''), '{formato}'))")


def fora_bbox(lat, lon):
    return lat.isNull() | lon.isNull() | ~lat.between(LAT_MIN, LAT_MAX) | ~lon.between(LON_MIN, LON_MAX)

# COMMAND ----------

# MAGIC %md ## Tabelas de controle, logs e dominio

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS {PRATA}.controle_processamento (
  fonte STRING, bronze_execucao_id STRING, prata_execucao_id STRING, processado_em TIMESTAMP)""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {PRATA}.log_qualidade (
  execucao_id STRING, executado_em TIMESTAMP, fonte STRING, regra STRING, descricao STRING,
  registros_avaliados BIGINT, registros_barrados BIGINT)""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {PRATA}.log_merge (
  execucao_id STRING, executado_em TIMESTAMP, fonte STRING, lotes ARRAY<STRING>,
  linhas_entrada BIGINT, duplicidades_evitadas BIGINT, registros_unicos BIGINT,
  registros_validos BIGINT, registros_invalidos BIGINT,
  inseridos BIGINT, atualizados BIGINT, existentes_sem_mudanca BIGINT, total_tabela_apos BIGINT)""")

# Dominio de barreiras: nomes reais de servicos do SP156 (verificados nos 4 trimestres coletados).
DOMINIO = [
    ("calcada publica solicitar manutencao", "CALCADA_DANIFICADA", "Calcada publica danificada (buraco, piso solto, desnivel)"),
    ("calcada particular denunciar calcada danificada ou inexistente", "CALCADA_DANIFICADA", "Calcada de imovel particular danificada ou inexistente"),
    ("sugerir construcao de calcadas em areas publicas e pracas", "CALCADA_INEXISTENTE", "Trecho publico sem calcada"),
    ("acessibilidade solicitar avaliacao de obstaculo na calcada", "OBSTACULO", "Obstaculo que bloqueia a faixa livre da calcada"),
    ("denunciar fiacao solta na calcada", "OBSTACULO", "Fiacao solta sobre a calcada"),
    ("guias para travessia de pedestres solicitar rebaixamento", "TRAVESSIA_SEM_RAMPA", "Travessia sem guia rebaixada (rampa)"),
    ("guia rebaixada solicitar fiscalizacao de rebaixamento irregular", "DESNIVEL", "Rebaixamento irregular de guia que cria desnivel"),
    ("guias sarjetas e sarjetoes solicitar manutencao", "GUIA_SARJETA", "Guia (meio-fio) ou sarjeta danificada na borda da calcada"),
    ("travessia de pedestres solicitar avaliacao", "TRAVESSIA_INSEGURA", "Pedido de avaliacao de travessia de pedestres"),
]
spark.createDataFrame(DOMINIO, "servico_chave STRING, grupo_problema STRING, descricao STRING") \
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{PRATA}.ref_categoria_barreira")
display(spark.table(f"{PRATA}.ref_categoria_barreira"))

# COMMAND ----------

def pendentes(tabela_bronze: str, fonte: str):
    df = spark.table(tabela_bronze)
    if REPROCESSAR:
        return df
    feitos = (spark.table(f"{PRATA}.controle_processamento").where(F.col("fonte") == fonte)
              .select(F.col("bronze_execucao_id").alias("_execucao_id")))
    return df.join(feitos, "_execucao_id", "left_anti")


def registrar_qualidade(fonte, df_flags, regras: dict):
    """df_flags tem uma coluna booleana por regra; conta barrados por regra + total."""
    aggs = [F.count(F.lit(1)).alias("avaliados")]
    aggs += [F.sum(F.col(r).cast("int")).alias(r) for r in regras]
    qualquer = F.lit(False)
    for r in regras:
        qualquer = qualquer | F.col(r)
    aggs += [F.sum(qualquer.cast("int")).alias("TOTAL_REJEITADOS")]
    linha = df_flags.agg(*aggs).collect()[0].asDict()
    descr = {**regras, "TOTAL_REJEITADOS": "registros barrados por ao menos uma regra"}
    regs = [(EXECUCAO_ID, AGORA, fonte, r, descr[r], int(linha["avaliados"]), int(linha[r] or 0)) for r in descr]
    spark.createDataFrame(regs, spark.table(f"{PRATA}.log_qualidade").schema) \
         .write.mode("append").saveAsTable(f"{PRATA}.log_qualidade")
    return linha


def merge_por_chave(df, tabela, chave, colunas_conteudo):
    """MERGE idempotente: insere chaves novas, atualiza so quando o conteudo mudou."""
    df = df.withColumn("hash_conteudo", F.sha2(F.concat_ws("|", *[
        F.coalesce(F.col(c).cast("string"), F.lit("")) for c in colunas_conteudo]), 256))
    df = df.withColumn("primeira_carga_em", F.current_timestamp()).withColumn("atualizado_em", F.current_timestamp())
    if not spark.catalog.tableExists(tabela):
        df.limit(0).write.saveAsTable(tabela)
    df.createOrReplaceTempView("_entrada_merge")
    cols = df.columns
    atualiza = ", ".join(f"t.{c} = s.{c}" for c in cols if c not in (chave, "primeira_carga_em"))
    r = spark.sql(f"""
        MERGE INTO {tabela} t USING _entrada_merge s ON t.{chave} = s.{chave}
        WHEN MATCHED AND t.hash_conteudo <> s.hash_conteudo THEN UPDATE SET {atualiza}
        WHEN NOT MATCHED THEN INSERT ({", ".join(cols)}) VALUES ({", ".join("s." + c for c in cols)})
    """).collect()[0].asDict()
    return int(r.get("num_inserted_rows", 0)), int(r.get("num_updated_rows", 0))


def registrar_merge(fonte, lotes, entrada, unicos, validos, invalidos, ins, upd, tabela):
    reg = (EXECUCAO_ID, AGORA, fonte, lotes, entrada, entrada - unicos, unicos, validos, invalidos,
           ins, upd, validos - ins - upd, spark.table(tabela).count())
    spark.createDataFrame([reg], spark.table(f"{PRATA}.log_merge").schema) \
         .write.mode("append").saveAsTable(f"{PRATA}.log_merge")


def marcar_processado(fonte, bronze_df):
    ids = [r[0] for r in bronze_df.select("_execucao_id").distinct().collect()]
    regs = [(fonte, i, EXECUCAO_ID, AGORA) for i in ids]
    spark.createDataFrame(regs, "fonte STRING, bronze_execucao_id STRING, prata_execucao_id STRING, processado_em TIMESTAMP") \
         .write.mode("append").saveAsTable(f"{PRATA}.controle_processamento")

# COMMAND ----------

# MAGIC %md ## SP156 - barreiras em calcadas

# COMMAND ----------

b_sp = pendentes(f"{BRONZE}.sp156_solicitacoes", "SP156")
entrada_sp = b_sp.count()
print("SP156 - linhas da Bronze a processar:", entrada_sp)

if entrada_sp:
    # 1) formato de data de cada arquivo/trimestre (o 2026-T3 veio em MM/dd/yyyy)
    formatos = (b_sp.groupBy("_arquivo_nome", "trimestre_fonte").agg(
        F.sum(F.when(F.expr("try_cast(substr(data_abertura, 1, 2) as int)") > 12, 1).otherwise(0)).alias("pos1_maior_12"),
        F.sum(F.when(F.expr("try_cast(substr(data_abertura, 4, 2) as int)") > 12, 1).otherwise(0)).alias("pos2_maior_12"))
        .withColumn("formato_data", F.when(F.col("pos2_maior_12") > F.col("pos1_maior_12"), "MM/dd/yyyy").otherwise("dd/MM/yyyy")))
    display(formatos.orderBy("_arquivo_nome", "trimestre_fonte"))

    s = b_sp.join(formatos.select("_arquivo_nome", "trimestre_fonte", "formato_data"), ["_arquivo_nome", "trimestre_fonte"])

    def data_sp(c):
        return F.when(F.col("formato_data") == "MM/dd/yyyy", data_txt(c, "MM/dd/yyyy")).otherwise(data_txt(c, "dd/MM/yyyy"))

    cep_dig = F.regexp_replace(F.coalesce(F.col("cep"), F.lit("")), "[^0-9]", "")
    status_n = norm(F.col("status"))
    s = s.select(
        data_sp("data_abertura").alias("data_abertura"),
        data_sp("data_finalizacao").alias("data_finalizacao"),
        F.upper(txt("meio_comunicacao")).alias("meio_comunicacao"),
        txt("tema").alias("tema"), txt("assunto").alias("assunto"), txt("servico").alias("servico"),
        norm(F.col("servico")).alias("servico_chave"),
        F.when(status_n == "finalizada", "FINALIZADA").when(status_n.isin("aberto", "aberta"), "ABERTA")
         .when(status_n == "em andamento", "EM_ANDAMENTO").when(status_n == "cancelada", "CANCELADA")
         .when(status_n == "", "NAO_INFORMADO").otherwise("OUTRO").alias("status"),
        txt("orgao").alias("orgao"), txt("logradouro").alias("logradouro"), txt("numero").alias("numero"),
        F.when(F.length(cep_dig) == 8, cep_dig).when(F.length(cep_dig) == 7, F.lpad(cep_dig, 8, "0")).alias("cep"),
        txt("latitude").alias("_lat_txt"), txt("longitude").alias("_lon_txt"),
        dbl("latitude").alias("latitude"), dbl("longitude").alias("longitude"),
        F.coalesce(F.upper(txt("subprefeitura")), F.lit("NAO INFORMADO")).alias("subprefeitura"),
        F.coalesce(F.upper(txt("distrito")), F.lit("NAO INFORMADO")).alias("distrito"),
        "trimestre_fonte", "formato_data", "_arquivo_nome", "_lote", "_arquivo_modificado_em", "_execucao_id",
    )

    # 2) chave substituta: atributos imutaveis + ordinal entre solicitacoes identicas do mesmo arquivo
    s = s.withColumn("_chave_base", F.sha2(F.concat_ws("|",
            F.coalesce(F.col("data_abertura").cast("string"), F.lit("")), F.col("servico_chave"),
            F.coalesce("_lat_txt", F.lit("")), F.coalesce("_lon_txt", F.lit("")), F.coalesce("cep", F.lit("")),
            norm(F.col("logradouro")), F.coalesce("numero", F.lit("")), F.coalesce("meio_comunicacao", F.lit(""))), 256))
    ordem = W.partitionBy("_arquivo_nome", "_chave_base").orderBy(
        "status", "data_finalizacao", "orgao", "tema", "assunto", "subprefeitura", "distrito", "trimestre_fonte")
    s = s.withColumn("ordinal", F.row_number().over(ordem)) \
         .withColumn("id_ocorrencia", F.sha2(F.concat_ws("#", "_chave_base", F.col("ordinal").cast("string")), 256))

    # 3) duplicidade entre lotes: a mesma ocorrencia em varios arquivos -> fica a versao do arquivo mais recente
    recente = W.partitionBy("id_ocorrencia").orderBy(F.col("_arquivo_modificado_em").desc(), F.col("_arquivo_nome").desc())
    s = s.withColumn("_rk", F.row_number().over(recente)).where("_rk = 1").drop("_rk")

    # 4) regras de qualidade
    dominio = spark.table(f"{PRATA}.ref_categoria_barreira").select("servico_chave", "grupo_problema")
    s = s.join(F.broadcast(dominio), "servico_chave", "left")
    tri_abertura = F.concat(F.year("data_abertura").cast("string"), F.lit("-T"), F.quarter("data_abertura").cast("string"))
    s = (s.withColumn("R1_COORDENADA", fora_bbox(F.col("latitude"), F.col("longitude")))
          .withColumn("R2_DATA", F.col("data_abertura").isNull()
                      | (F.col("data_abertura") > F.current_date()) | (F.col("data_abertura") < F.lit(DATA_MIN))
                      | (tri_abertura != F.col("trimestre_fonte"))
                      | (F.col("data_finalizacao").isNotNull() & (F.col("data_finalizacao") < F.col("data_abertura"))))
          .withColumn("R3_DOMINIO", F.col("grupo_problema").isNull()))

    # materializa a etapa (serverless nao tem cache): garante que contagens e MERGE usam os mesmos dados
    s.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{PRATA}._stg_sp156")
    s = spark.table(f"{PRATA}._stg_sp156")

    regras_sp = {
        "R1_COORDENADA": "latitude/longitude ausente, invalida ou fora do municipio de Sao Paulo",
        "R2_DATA": "abertura nula/futura/<2015/fora do trimestre do arquivo ou finalizacao < abertura",
        "R3_DOMINIO": "servico fora do dominio de barreiras em calcada (ref_categoria_barreira)",
    }
    q = registrar_qualidade("SP156", s, regras_sp)
    invalido = F.col("R1_COORDENADA") | F.col("R2_DATA") | F.col("R3_DOMINIO")

    (s.where(invalido)
       .withColumn("motivos", F.filter(F.array(*[F.when(F.col(r), F.lit(r)) for r in regras_sp]), lambda x: x.isNotNull()))
       .select("id_ocorrencia", "motivos", "data_abertura", "servico", "status", "latitude", "longitude",
               "trimestre_fonte", "_arquivo_nome", "_lote", F.lit(EXECUCAO_ID).alias("prata_execucao_id"))
       .write.mode("append").option("mergeSchema", "true").saveAsTable(f"{PRATA}.sp156_rejeitados"))

    validos = (s.where(~invalido)
        .withColumn("dias_resolucao", F.when((F.col("status") == "FINALIZADA") & F.col("data_finalizacao").isNotNull(),
                                             F.datediff("data_finalizacao", "data_abertura")))
        .select("id_ocorrencia", "data_abertura", "data_finalizacao", "dias_resolucao", "status",
                "tema", "assunto", "servico", "servico_chave", "grupo_problema", "orgao", "meio_comunicacao",
                "logradouro", "numero", "cep", "latitude", "longitude", "subprefeitura", "distrito",
                "trimestre_fonte", "formato_data", F.col("_lote").alias("lote"), F.col("_arquivo_nome").alias("arquivo_origem")))
    n_validos = validos.count()
    unicos_sp = s.count()
    conteudo = ["data_abertura", "data_finalizacao", "status", "servico", "grupo_problema", "orgao",
                "subprefeitura", "distrito", "latitude", "longitude", "cep", "logradouro", "numero"]
    ins, upd = merge_por_chave(validos, f"{PRATA}.sp156_barreiras", "id_ocorrencia", conteudo)
    lotes = sorted(r[0] for r in b_sp.select("_lote").distinct().collect())
    registrar_merge("SP156", lotes, entrada_sp, unicos_sp, n_validos, unicos_sp - n_validos, ins, upd, f"{PRATA}.sp156_barreiras")
    marcar_processado("SP156", b_sp)
    print({"entrada": entrada_sp, "unicos": unicos_sp, "validos": n_validos, "inseridos": ins, "atualizados": upd, **q})

# COMMAND ----------

# MAGIC %md ## Infosiga - atropelamentos de pedestres

# COMMAND ----------

b_inf = pendentes(f"{BRONZE}.infosiga_sinistros", "INFOSIGA")
entrada_inf = b_inf.count()
print("INFOSIGA - linhas da Bronze a processar:", entrada_inf)

if entrada_inf:
    i = b_inf.select(
        txt("id_sinistro").alias("id_sinistro"),
        data_txt("data_sinistro", "dd/MM/yyyy").alias("data_sinistro"),
        F.expr("try_to_timestamp(concat(trim(substr(data_sinistro,1,10)), ' ', nullif(trim(hora_sinistro),'')), 'dd/MM/yyyy HH:mm')").alias("data_hora_sinistro"),
        F.upper(txt("dia_da_semana")).alias("dia_da_semana"), F.upper(txt("turno")).alias("turno"),
        F.upper(txt("tipo_registro")).alias("tipo_registro"),
        txt("logradouro").alias("logradouro"), txt("numero_logradouro").alias("numero_logradouro"),
        F.upper(txt("tipo_via")).alias("tipo_via"), F.upper(txt("tipo_local")).alias("tipo_local"),
        dbl("latitude", virgula=True).alias("latitude"), dbl("longitude", virgula=True).alias("longitude"),
        txt("cod_ibge").alias("cod_ibge"), F.upper(txt("municipio")).alias("municipio"),
        F.upper(txt("tp_sinistro_primario")).alias("tp_sinistro_primario"),
        F.upper(txt("tp_sinistro_atrop_pedestre")).alias("_atrop_pedestre"),
        inteiro0("qtd_pedestre").alias("qtd_pedestre"),
        inteiro0("qtd_gravidade_fatal").alias("qtd_vitimas_fatais"),
        inteiro0("qtd_gravidade_grave").alias("qtd_vitimas_graves"),
        inteiro0("qtd_gravidade_leve").alias("qtd_vitimas_leves"),
        "_arquivo_nome", "_lote", "_arquivo_modificado_em", "_execucao_id",
    )
    recente = W.partitionBy("id_sinistro").orderBy(F.col("_arquivo_modificado_em").desc(), F.col("_arquivo_nome").desc())
    i = i.withColumn("_rk", F.row_number().over(recente)).where("_rk = 1").drop("_rk")

    i = (i.withColumn("R1_COORDENADA", fora_bbox(F.col("latitude"), F.col("longitude")))
          .withColumn("R2_DATA", F.col("data_sinistro").isNull() | (F.col("data_sinistro") > F.current_date())
                      | (F.col("data_sinistro") < F.lit(DATA_MIN)))
          .withColumn("R3_DOMINIO", F.col("id_sinistro").isNull() | (F.coalesce(F.col("_atrop_pedestre"), F.lit("")) != "S")
                      | (norm(F.col("municipio")) != "sao paulo")
                      | ~F.coalesce(F.col("tipo_registro"), F.lit("")).isin("SINISTRO FATAL", "SINISTRO NAO FATAL", "NOTIFICACAO")))
    i.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{PRATA}._stg_infosiga")
    i = spark.table(f"{PRATA}._stg_infosiga")

    regras_inf = {
        "R1_COORDENADA": "latitude/longitude ausente, invalida ou fora do municipio de Sao Paulo",
        "R2_DATA": "data do sinistro nula, futura ou anterior a 2015",
        "R3_DOMINIO": "nao e atropelamento de pedestre, fora da capital ou tipo_registro desconhecido",
    }
    q = registrar_qualidade("INFOSIGA", i, regras_inf)
    invalido = F.col("R1_COORDENADA") | F.col("R2_DATA") | F.col("R3_DOMINIO")

    (i.where(invalido)
       .withColumn("motivos", F.filter(F.array(*[F.when(F.col(r), F.lit(r)) for r in regras_inf]), lambda x: x.isNotNull()))
       .select("id_sinistro", "motivos", "data_sinistro", "tipo_registro", "latitude", "longitude",
               "_arquivo_nome", "_lote", F.lit(EXECUCAO_ID).alias("prata_execucao_id"))
       .write.mode("append").option("mergeSchema", "true").saveAsTable(f"{PRATA}.infosiga_rejeitados"))

    validos = (i.where(~invalido)
        .withColumn("confirmado", F.col("tipo_registro") != "NOTIFICACAO")
        .withColumn("gravidade_maxima", F.when(F.col("qtd_vitimas_fatais") > 0, "FATAL")
                    .when(F.col("qtd_vitimas_graves") > 0, "GRAVE").when(F.col("qtd_vitimas_leves") > 0, "LEVE")
                    .otherwise("NAO INFORMADA"))
        .drop("_atrop_pedestre", "_execucao_id", "_arquivo_modificado_em", "R1_COORDENADA", "R2_DATA", "R3_DOMINIO")
        .withColumnRenamed("_lote", "lote").withColumnRenamed("_arquivo_nome", "arquivo_origem"))
    n_validos = validos.count()
    unicos_inf = i.count()
    conteudo = ["data_sinistro", "data_hora_sinistro", "tipo_registro", "latitude", "longitude", "logradouro",
                "qtd_pedestre", "qtd_vitimas_fatais", "qtd_vitimas_graves", "qtd_vitimas_leves"]
    ins, upd = merge_por_chave(validos, f"{PRATA}.infosiga_atropelamentos", "id_sinistro", conteudo)
    lotes = sorted(r[0] for r in b_inf.select("_lote").distinct().collect())
    registrar_merge("INFOSIGA", lotes, entrada_inf, unicos_inf, n_validos, unicos_inf - n_validos, ins, upd,
                    f"{PRATA}.infosiga_atropelamentos")
    marcar_processado("INFOSIGA", b_inf)
    print({"entrada": entrada_inf, "unicos": unicos_inf, "validos": n_validos, "inseridos": ins, "atualizados": upd, **q})

# COMMAND ----------

# MAGIC %md ## Evidencias
# MAGIC 1. Quantidade barrada por regra (esta execucao)

# COMMAND ----------

display(spark.sql(f"""
SELECT fonte, regra, descricao, registros_avaliados, registros_barrados,
       round(100 * registros_barrados / registros_avaliados, 2) AS pct_barrado
FROM {PRATA}.log_qualidade WHERE execucao_id = '{EXECUCAO_ID}' ORDER BY fonte, regra"""))

# COMMAND ----------

# MAGIC %md 2. Historico do MERGE: entrada, duplicidades evitadas, validos/invalidos, novos x existentes

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {PRATA}.log_merge ORDER BY executado_em, fonte"))

# COMMAND ----------

# MAGIC %md 3. Contagem por camada e prova de unicidade da chave (duplicadas deve ser 0)

# COMMAND ----------

display(spark.sql(f"""
SELECT 'bronze.sp156_solicitacoes' AS tabela, COUNT(*) AS registros, NULL AS chaves_duplicadas FROM {BRONZE}.sp156_solicitacoes
UNION ALL SELECT 'bronze.infosiga_sinistros', COUNT(*), NULL FROM {BRONZE}.infosiga_sinistros
UNION ALL SELECT 'prata.sp156_barreiras', COUNT(*), COUNT(*) - COUNT(DISTINCT id_ocorrencia) FROM {PRATA}.sp156_barreiras
UNION ALL SELECT 'prata.infosiga_atropelamentos', COUNT(*), COUNT(*) - COUNT(DISTINCT id_sinistro) FROM {PRATA}.infosiga_atropelamentos
"""))

# COMMAND ----------

# MAGIC %md 4. Exemplos reais barrados por regra e o registro de teste encontrado na fonte oficial ("tema001 - teste")

# COMMAND ----------

display(spark.sql(f"""
WITH r AS (
  SELECT m AS regra, servico, data_abertura, latitude, longitude, trimestre_fonte, id_ocorrencia
  FROM {PRATA}.sp156_rejeitados LATERAL VIEW explode(motivos) t AS m
  WHERE prata_execucao_id = '{EXECUCAO_ID}')
SELECT regra, servico, data_abertura, latitude, longitude, trimestre_fonte
FROM (SELECT *, row_number() OVER (PARTITION BY regra ORDER BY id_ocorrencia) AS rn FROM r)
WHERE rn <= 5 OR lower(servico) LIKE '%8888%' OR lower(servico) LIKE '%teste%'
ORDER BY regra"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT grupo_problema, status, COUNT(*) AS solicitacoes, ROUND(AVG(dias_resolucao), 1) AS media_dias_resolucao
FROM {PRATA}.sp156_barreiras GROUP BY ALL ORDER BY grupo_problema, solicitacoes DESC"""))
