# Databricks notebook source
# MAGIC %md
# MAGIC # Rota Livre - 03 Ouro (modelo dimensional, protecao de dados e tabela do MVP)
# MAGIC
# MAGIC Reconstroi a Ouro a partir da Prata (rebuild completo, deterministico).
# MAGIC
# MAGIC | Tabela | Grao | Observacao |
# MAGIC |---|---|---|
# MAGIC | `fato_ocorrencias` | 1 solicitacao SP156 de barreira | status, dias de resolucao, FKs para tempo/localizacao/categoria |
# MAGIC | `fato_sinistros_pedestres` | 1 atropelamento (Infosiga) | contexto territorial complementar |
# MAGIC | `dim_tempo` | 1 dia | data, dia, mes, ano, trimestre, dia da semana |
# MAGIC | `dim_localizacao` | celula H3 (res. 8, ~0,74 km2) x subprefeitura x distrito | municipio, subprefeitura (regiao), distrito, celula, centro |
# MAGIC | `dim_categoria` | 1 servico SP156 | categoria, descricao, grupo de problema |
# MAGIC | **`mvp_mapa_barreiras`** | 1 celula H3 | **consumida pela tela "Mapa de barreiras" do MVP** |
# MAGIC
# MAGIC **Protecao de dados pessoais (LGPD)** - o SP156 traz logradouro + numero + CEP de cada pedido, o que
# MAGIC aponta para o endereco (muitas vezes a residencia) de quem reclamou. Na Ouro:
# MAGIC - `cep_hash` = SHA-256(CEP + salt secreto) - permite contar/agrupar sem revelar o CEP;
# MAGIC - `numero_mascarado` = numero com as 2 ultimas posicoes trocadas por `X` (`1732` -> `17XX`);
# MAGIC - coordenadas generalizadas para 3 casas decimais (~110 m) e celula H3 (~0,74 km2).
# MAGIC Endereco exato e CEP em claro ficam apenas na Prata (camada restrita).

# COMMAND ----------

from pyspark.sql import functions as F

CAT = "workspace"
PRATA, OURO = f"{CAT}.rota_livre_prata", f"{CAT}.rota_livre_ouro"
H3_RES = 8

# Salt do hash - nunca fica no codigo. Ordem de busca:
#   1) secret scope rota_livre/salt_hash   2) widget salt_hash   3) arquivo no Volume sistema (acesso restrito)
ARQUIVO_SALT = f"/Volumes/{CAT}/rota_livre_bronze/sistema/segredos/salt_hash.txt"
dbutils.widgets.text("salt_hash", "", "Salt do hash (se nao houver secret)")
SALT, origem_salt = "", None
try:
    SALT, origem_salt = dbutils.secrets.get("rota_livre", "salt_hash"), "secret scope rota_livre/salt_hash"
except Exception:
    pass
if len(SALT.strip()) < 16:
    SALT, origem_salt = dbutils.widgets.get("salt_hash"), "widget salt_hash"
if len(SALT.strip()) < 16:
    try:
        with open(ARQUIVO_SALT, encoding="utf-8") as f:
            SALT, origem_salt = f.read(), f"arquivo {ARQUIVO_SALT}"
    except FileNotFoundError:
        pass
SALT = SALT.strip()
if len(SALT) < 16:
    raise ValueError(
        "Salt nao encontrado (16+ caracteres). Opcoes: (a) envie .segredos/salt_hash.txt para "
        f"{ARQUIVO_SALT}; (b) preencha o widget 'salt_hash' e pressione Enter; (c) crie o secret rota_livre/salt_hash.")
print("salt lido de:", origem_salt)

# H3 nativo do Databricks; se indisponivel, grade regular de ~500 m como alternativa.
try:
    spark.sql(f"SELECT h3_longlatash3(-46.63, -23.55, {H3_RES})").collect()
    USAR_H3 = True
except Exception as e:
    USAR_H3 = False
    print("H3 indisponivel, usando grade de 0,005 grau:", str(e)[:200])
print("USAR_H3:", USAR_H3)


def celula(lat, lon):
    if USAR_H3:
        return f"h3_longlatash3({lon}, {lat}, {H3_RES})"
    return f"cast(floor(({lat} + 90) / 0.005) * 1000000 + floor(({lon} + 180) / 0.005) as bigint)"


def centro(c):
    """(lat, lon, geojson) do centro da celula. WKT do H3: 'POINT(lon lat)'."""
    if USAR_H3:
        wkt = f"h3_centeraswkt({c})"
        return (f"cast(regexp_extract({wkt}, '(-?[0-9.]+) (-?[0-9.]+)', 2) as double)",
                f"cast(regexp_extract({wkt}, '(-?[0-9.]+) (-?[0-9.]+)', 1) as double)",
                f"h3_boundaryasgeojson({c})")
    return (f"(floor({c} / 1000000) * 0.005 - 90 + 0.0025)", f"(({c} % 1000000) * 0.005 - 180 + 0.0025)", "CAST(NULL AS STRING)")


LAT_C, LON_C, GEOJSON = centro("celula_h3")

# COMMAND ----------

# MAGIC %md ## Referencia territorial (celula -> distrito)
# MAGIC Atropelamentos do Infosiga nao trazem distrito. Inferimos o distrito predominante de cada celula a partir
# MAGIC dos ~245 mil pontos georreferenciados do SP156 (etapa `_stg_sp156` da Prata). Sem referencia: `NAO INFORMADO`.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW ref_celula AS
SELECT celula_h3, subprefeitura, distrito FROM (
  SELECT celula_h3, subprefeitura, distrito,
         row_number() OVER (PARTITION BY celula_h3 ORDER BY COUNT(*) DESC, distrito) AS rk
  FROM (SELECT {celula('latitude', 'longitude')} AS celula_h3, subprefeitura, distrito
        FROM {PRATA}._stg_sp156
        WHERE NOT R1_COORDENADA AND distrito <> 'NAO INFORMADO')
  GROUP BY celula_h3, subprefeitura, distrito)
WHERE rk = 1
""")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW base_ocorrencias AS
SELECT b.*, {celula('b.latitude', 'b.longitude')} AS celula_h3 FROM {PRATA}.sp156_barreiras b
""")
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW base_sinistros AS
SELECT s.*,
       coalesce(r.subprefeitura, 'NAO INFORMADO') AS subprefeitura,
       coalesce(r.distrito, 'NAO INFORMADO')      AS distrito,
       CASE WHEN r.distrito IS NULL THEN 'NAO_INFORMADO' ELSE 'INFERIDO_CELULA_SP156' END AS origem_distrito
FROM (SELECT x.*, {celula('x.latitude', 'x.longitude')} AS celula_h3 FROM {PRATA}.infosiga_atropelamentos x) s
LEFT JOIN ref_celula r ON r.celula_h3 = s.celula_h3
""")

# COMMAND ----------

# MAGIC %md ## Dimensoes

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {OURO}.dim_tempo COMMENT 'Dimensao tempo (1 linha por dia)' AS
WITH limites AS (
  SELECT least(min(d1), min(d2)) AS ini, greatest(max(d1), max(d2)) AS fim FROM (
    SELECT min(data_abertura) d1, max(coalesce(data_finalizacao, data_abertura)) d2 FROM {PRATA}.sp156_barreiras
    UNION ALL SELECT min(data_sinistro), max(data_sinistro) FROM {PRATA}.infosiga_atropelamentos))
SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS id_tempo, d AS data,
       day(d) AS dia, month(d) AS mes, year(d) AS ano, quarter(d) AS trimestre,
       date_format(d, 'yyyy-MM') AS ano_mes, dayofweek(d) AS dia_semana_num,
       CASE dayofweek(d) WHEN 1 THEN 'DOMINGO' WHEN 2 THEN 'SEGUNDA' WHEN 3 THEN 'TERCA' WHEN 4 THEN 'QUARTA'
            WHEN 5 THEN 'QUINTA' WHEN 6 THEN 'SEXTA' ELSE 'SABADO' END AS dia_semana
FROM limites LATERAL VIEW explode(sequence(ini, fim, interval 1 day)) t AS d
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {OURO}.dim_categoria COMMENT 'Dimensao categoria de barreira (servico SP156)' AS
SELECT xxhash64(r.servico_chave) AS id_categoria,
       coalesce(max(b.servico), r.servico_chave) AS categoria,
       r.descricao, r.grupo_problema,
       CASE r.grupo_problema
         WHEN 'CALCADA_DANIFICADA' THEN 'Calcada danificada' WHEN 'CALCADA_INEXISTENTE' THEN 'Calcada inexistente'
         WHEN 'OBSTACULO' THEN 'Obstaculo na calcada' WHEN 'TRAVESSIA_SEM_RAMPA' THEN 'Travessia sem rampa'
         WHEN 'DESNIVEL' THEN 'Desnivel' WHEN 'GUIA_SARJETA' THEN 'Guia e sarjeta'
         WHEN 'TRAVESSIA_INSEGURA' THEN 'Travessia insegura' END AS grupo_problema_rotulo
FROM {PRATA}.ref_categoria_barreira r LEFT JOIN {PRATA}.sp156_barreiras b USING (servico_chave)
GROUP BY r.servico_chave, r.descricao, r.grupo_problema
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {OURO}.dim_localizacao COMMENT 'Dimensao localizacao: celula H3 x subprefeitura x distrito' AS
WITH chaves AS (
  SELECT DISTINCT celula_h3, subprefeitura, distrito, 'SP156' AS origem_distrito FROM base_ocorrencias
  UNION
  SELECT DISTINCT celula_h3, subprefeitura, distrito, origem_distrito FROM base_sinistros)
SELECT xxhash64(celula_h3, subprefeitura, distrito) AS id_localizacao,
       'SAO PAULO' AS municipio, subprefeitura, distrito,
       celula_h3, {H3_RES if USAR_H3 else 'NULL'} AS h3_resolucao,
       {LAT_C} AS latitude_centro, {LON_C} AS longitude_centro,
       min_by(origem_distrito, CASE origem_distrito WHEN 'SP156' THEN 1 WHEN 'INFERIDO_CELULA_SP156' THEN 2 ELSE 3 END) AS origem_distrito
FROM chaves GROUP BY celula_h3, subprefeitura, distrito
""")
display(spark.sql(f"SELECT origem_distrito, COUNT(*) AS linhas FROM {OURO}.dim_localizacao GROUP BY ALL"))

# COMMAND ----------

# MAGIC %md ## Fatos (com dados pessoais protegidos)

# COMMAND ----------

MASCARA_NUMERO = """CASE WHEN {c} RLIKE '^[0-9]{{3,}}$' THEN concat(substr({c}, 1, length({c}) - 2), 'XX')
                         WHEN {c} RLIKE '^[0-9]{{1,2}}$' THEN 'XX'
                         WHEN {c} IS NULL THEN NULL ELSE 'S/N' END"""

fato_oc = spark.sql(f"""
SELECT o.id_ocorrencia,
       xxhash64(o.celula_h3, o.subprefeitura, o.distrito) AS id_localizacao,
       xxhash64(o.servico_chave)                          AS id_categoria,
       CAST(date_format(o.data_abertura, 'yyyyMMdd') AS INT)    AS id_tempo_abertura,
       CAST(date_format(o.data_finalizacao, 'yyyyMMdd') AS INT) AS id_tempo_finalizacao,
       o.status,
       CASE WHEN o.status IN ('ABERTA', 'EM_ANDAMENTO') THEN 1 ELSE 0 END AS flag_aberta,
       CASE WHEN o.status = 'FINALIZADA' THEN 1 ELSE 0 END                AS flag_finalizada,
       o.dias_resolucao,
       1 AS qtd_ocorrencias,
       o.meio_comunicacao,
       o.celula_h3,
       round(o.latitude, 3)  AS latitude_aprox,
       round(o.longitude, 3) AS longitude_aprox,
       o.logradouro,
       {MASCARA_NUMERO.format(c='o.numero')} AS numero_mascarado,
       CASE WHEN o.cep IS NOT NULL THEN sha2(concat(o.cep, :salt), 256) END AS cep_hash
FROM base_ocorrencias o
""", args={"salt": SALT})
fato_oc.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{OURO}.fato_ocorrencias")
spark.sql(f"COMMENT ON TABLE {OURO}.fato_ocorrencias IS 'Fato: solicitacoes SP156 de barreiras em calcadas (dados pessoais mascarados)'")

spark.sql(f"""
CREATE OR REPLACE TABLE {OURO}.fato_sinistros_pedestres
COMMENT 'Fato: atropelamentos de pedestres na capital (Infosiga), contexto territorial' AS
SELECT s.id_sinistro,
       xxhash64(s.celula_h3, s.subprefeitura, s.distrito) AS id_localizacao,
       CAST(date_format(s.data_sinistro, 'yyyyMMdd') AS INT) AS id_tempo,
       s.tipo_registro, s.confirmado, s.gravidade_maxima,
       s.qtd_pedestre, s.qtd_vitimas_fatais, s.qtd_vitimas_graves, s.qtd_vitimas_leves,
       1 AS qtd_atropelamentos,
       s.turno, s.tipo_via, s.celula_h3,
       round(s.latitude, 3) AS latitude_aprox, round(s.longitude, 3) AS longitude_aprox,
       s.logradouro, {MASCARA_NUMERO.format(c='s.numero_logradouro')} AS numero_mascarado
FROM base_sinistros s
""")

# COMMAND ----------

# MAGIC %md ## Tabela do MVP - tela "Mapa de barreiras"
# MAGIC Uma linha por celula H3 com barreiras e/ou atropelamentos. A tela desenha cada celula (`geojson_celula`
# MAGIC ou centro) colorida por `classe_prioridade`; o painel lateral mostra os totais da celula.
# MAGIC
# MAGIC `indice_prioridade` = barreiras **nao resolvidas** x atropelamentos na mesma celula. Classes por percentil
# MAGIC entre as celulas com indice > 0: CRITICA (top 5%), ALTA (top 20%), MEDIA, BAIXA; `SEM_CRUZAMENTO` quando falta um dos lados.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {OURO}.mvp_mapa_barreiras
COMMENT 'Consumida pela tela Mapa de barreiras do MVP Rota Livre (sem dados pessoais)' AS
WITH b AS (
  SELECT f.celula_h3, COUNT(*) AS barreiras_total, SUM(f.flag_aberta) AS barreiras_abertas,
         SUM(f.flag_finalizada) AS barreiras_finalizadas, ROUND(AVG(f.dias_resolucao), 1) AS media_dias_resolucao,
         mode(c.grupo_problema_rotulo) AS problema_mais_frequente, MAX(t.data) AS ultima_solicitacao
  FROM {OURO}.fato_ocorrencias f
  JOIN {OURO}.dim_categoria c USING (id_categoria)
  JOIN {OURO}.dim_tempo t ON t.id_tempo = f.id_tempo_abertura
  GROUP BY f.celula_h3),
s AS (
  SELECT celula_h3, COUNT(*) AS atropelamentos, SUM(CASE WHEN confirmado THEN 1 ELSE 0 END) AS atropelamentos_confirmados,
         SUM(qtd_vitimas_fatais) AS vitimas_fatais, SUM(qtd_vitimas_graves) AS vitimas_graves
  FROM {OURO}.fato_sinistros_pedestres GROUP BY celula_h3),
loc AS (
  SELECT celula_h3, subprefeitura, distrito FROM (
    SELECT l.celula_h3, l.subprefeitura, l.distrito,
           row_number() OVER (PARTITION BY l.celula_h3 ORDER BY (l.distrito = 'NAO INFORMADO'), COUNT(f.id_ocorrencia) DESC, l.distrito) rk
    FROM {OURO}.dim_localizacao l LEFT JOIN {OURO}.fato_ocorrencias f USING (id_localizacao)
    GROUP BY l.celula_h3, l.subprefeitura, l.distrito) WHERE rk = 1),
j AS (
  SELECT coalesce(b.celula_h3, s.celula_h3) AS celula_h3,
         coalesce(b.barreiras_total, 0) AS barreiras_total, coalesce(b.barreiras_abertas, 0) AS barreiras_abertas,
         coalesce(b.barreiras_finalizadas, 0) AS barreiras_finalizadas, b.media_dias_resolucao,
         b.problema_mais_frequente, b.ultima_solicitacao,
         coalesce(s.atropelamentos, 0) AS atropelamentos, coalesce(s.atropelamentos_confirmados, 0) AS atropelamentos_confirmados,
         coalesce(s.vitimas_fatais, 0) AS vitimas_fatais, coalesce(s.vitimas_graves, 0) AS vitimas_graves
  FROM b FULL OUTER JOIN s ON b.celula_h3 = s.celula_h3),
idx AS (
  SELECT *, barreiras_abertas * atropelamentos AS indice_prioridade FROM j),
pr AS (
  SELECT celula_h3, percent_rank() OVER (ORDER BY indice_prioridade) AS pct FROM idx WHERE indice_prioridade > 0)
SELECT i.celula_h3, l.subprefeitura, l.distrito,
       {LAT_C.replace('celula_h3', 'i.celula_h3')} AS latitude_centro,
       {LON_C.replace('celula_h3', 'i.celula_h3')} AS longitude_centro,
       {GEOJSON.replace('celula_h3', 'i.celula_h3')} AS geojson_celula,
       i.barreiras_total, i.barreiras_abertas, i.barreiras_finalizadas, i.media_dias_resolucao,
       i.problema_mais_frequente, i.ultima_solicitacao,
       i.atropelamentos, i.atropelamentos_confirmados, i.vitimas_fatais, i.vitimas_graves,
       i.indice_prioridade,
       CASE WHEN i.indice_prioridade = 0 THEN 'SEM_CRUZAMENTO'
            WHEN pr.pct >= 0.95 THEN 'CRITICA' WHEN pr.pct >= 0.80 THEN 'ALTA'
            WHEN pr.pct >= 0.50 THEN 'MEDIA' ELSE 'BAIXA' END AS classe_prioridade,
       dense_rank() OVER (ORDER BY i.indice_prioridade DESC) AS ranking_prioridade,
       current_timestamp() AS atualizado_em
FROM idx i LEFT JOIN pr USING (celula_h3) LEFT JOIN loc l USING (celula_h3)
""")

# COMMAND ----------

# MAGIC %md ## Evidencias
# MAGIC 1. Quantidade de registros em cada camada (indicador tecnico)

# COMMAND ----------

display(spark.sql(f"""
SELECT 'bronze' AS camada, 'sp156_solicitacoes' AS tabela, COUNT(*) AS registros FROM {CAT}.rota_livre_bronze.sp156_solicitacoes
UNION ALL SELECT 'bronze', 'infosiga_sinistros', COUNT(*) FROM {CAT}.rota_livre_bronze.infosiga_sinistros
UNION ALL SELECT 'prata', 'sp156_barreiras', COUNT(*) FROM {PRATA}.sp156_barreiras
UNION ALL SELECT 'prata', 'infosiga_atropelamentos', COUNT(*) FROM {PRATA}.infosiga_atropelamentos
UNION ALL SELECT 'ouro', 'fato_ocorrencias', COUNT(*) FROM {OURO}.fato_ocorrencias
UNION ALL SELECT 'ouro', 'fato_sinistros_pedestres', COUNT(*) FROM {OURO}.fato_sinistros_pedestres
UNION ALL SELECT 'ouro', 'dim_tempo', COUNT(*) FROM {OURO}.dim_tempo
UNION ALL SELECT 'ouro', 'dim_localizacao', COUNT(*) FROM {OURO}.dim_localizacao
UNION ALL SELECT 'ouro', 'dim_categoria', COUNT(*) FROM {OURO}.dim_categoria
UNION ALL SELECT 'ouro', 'mvp_mapa_barreiras', COUNT(*) FROM {OURO}.mvp_mapa_barreiras
"""))

# COMMAND ----------

# MAGIC %md 2. Integridade referencial (todas as contagens de orfaos devem ser 0)

# COMMAND ----------

display(spark.sql(f"""
SELECT 'ocorrencias sem localizacao' AS checagem, COUNT(*) AS orfaos FROM {OURO}.fato_ocorrencias f LEFT ANTI JOIN {OURO}.dim_localizacao USING (id_localizacao)
UNION ALL SELECT 'ocorrencias sem categoria', COUNT(*) FROM {OURO}.fato_ocorrencias LEFT ANTI JOIN {OURO}.dim_categoria USING (id_categoria)
UNION ALL SELECT 'ocorrencias sem tempo', COUNT(*) FROM {OURO}.fato_ocorrencias f LEFT ANTI JOIN {OURO}.dim_tempo t ON t.id_tempo = f.id_tempo_abertura
UNION ALL SELECT 'sinistros sem localizacao', COUNT(*) FROM {OURO}.fato_sinistros_pedestres LEFT ANTI JOIN {OURO}.dim_localizacao USING (id_localizacao)
UNION ALL SELECT 'sinistros sem tempo', COUNT(*) FROM {OURO}.fato_sinistros_pedestres f LEFT ANTI JOIN {OURO}.dim_tempo t ON t.id_tempo = f.id_tempo
"""))

# COMMAND ----------

# MAGIC %md 3. Protecao de dados: Prata (restrita) x Ouro (mascarada) - mesma ocorrencia lado a lado

# COMMAND ----------

display(spark.sql(f"""
SELECT p.id_ocorrencia,
       p.cep AS prata_cep, o.cep_hash AS ouro_cep_hash,
       p.numero AS prata_numero, o.numero_mascarado AS ouro_numero,
       p.latitude AS prata_latitude, o.latitude_aprox AS ouro_latitude
FROM {PRATA}.sp156_barreiras p JOIN {OURO}.fato_ocorrencias o USING (id_ocorrencia)
WHERE p.cep IS NOT NULL AND p.numero RLIKE '^[0-9]{{3,}}$'
ORDER BY p.id_ocorrencia LIMIT 5"""))

# COMMAND ----------

# MAGIC %md 4. Nenhuma coluna da Ouro expoe CEP/numero em claro (resultado esperado: 0 linhas)

# COMMAND ----------

display(spark.sql(f"""
SELECT table_name, column_name FROM {CAT}.information_schema.columns
WHERE table_schema = 'rota_livre_ouro' AND lower(column_name) IN ('cep', 'numero', 'numero_logradouro', 'latitude', 'longitude')
"""))

# COMMAND ----------

# MAGIC %md 5. Amostra do que a tela "Mapa de barreiras" recebe (celulas mais prioritarias)

# COMMAND ----------

display(spark.sql(f"""
SELECT ranking_prioridade, classe_prioridade, subprefeitura, distrito, barreiras_abertas, barreiras_total,
       atropelamentos, vitimas_fatais, problema_mais_frequente, latitude_centro, longitude_centro
FROM {OURO}.mvp_mapa_barreiras ORDER BY ranking_prioridade LIMIT 15"""))
