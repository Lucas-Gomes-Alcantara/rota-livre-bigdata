-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Rota Livre - 04 Consultas analiticas (Ouro)
-- MAGIC Periodo das barreiras: trimestres 2025-T4 a 2026-T3 do SP156. Atropelamentos: Infosiga, 2022 em diante.
-- MAGIC Rodar num SQL Warehouse ou notebook serverless depois de `03_ouro`.

-- COMMAND ----------

USE CATALOG workspace;
USE SCHEMA rota_livre_ouro;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consulta 1 - Regioes com maior quantidade de ocorrencias
-- MAGIC **Responde:** onde se concentram as reclamacoes de barreiras e quanto delas segue sem solucao.
-- MAGIC **Uso:** priorizar vistorias por subprefeitura/distrito e comparar regioes de tamanho parecido.

-- COMMAND ----------

SELECT l.subprefeitura,
       l.distrito,
       COUNT(*)                                         AS ocorrencias,
       SUM(f.flag_aberta)                               AS abertas,
       ROUND(100 * SUM(f.flag_aberta) / COUNT(*), 1)    AS pct_abertas,
       ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_do_total
FROM fato_ocorrencias f
JOIN dim_localizacao l USING (id_localizacao)
GROUP BY l.subprefeitura, l.distrito
ORDER BY ocorrencias DESC
LIMIT 15;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consulta 2 - Categorias de barreiras mais frequentes
-- MAGIC **Responde:** que tipo de problema mais aparece e qual tem maior taxa de resolucao.
-- MAGIC **Uso:** definir as categorias do formulario do app e direcionar cada tipo ao orgao responsavel.

-- COMMAND ----------

SELECT c.grupo_problema_rotulo                         AS grupo_problema,
       c.categoria                                     AS servico_sp156,
       COUNT(*)                                        AS ocorrencias,
       ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_do_total,
       ROUND(100 * SUM(f.flag_finalizada) / COUNT(*), 1) AS pct_finalizadas,
       SUM(f.flag_aberta)                              AS ainda_abertas
FROM fato_ocorrencias f
JOIN dim_categoria c USING (id_categoria)
GROUP BY ALL
ORDER BY ocorrencias DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consulta 3 - Evolucao temporal das ocorrencias
-- MAGIC **Responde:** como o volume de reclamacoes varia mes a mes, por grupo de problema, e quanto foi resolvido.
-- MAGIC **Uso:** identificar sazonalidade (ex.: periodo de chuvas) e medir se a fila de abertas cresce ou diminui.

-- COMMAND ----------

SELECT t.ano_mes,
       COUNT(*)                                                         AS ocorrencias,
       SUM(f.flag_aberta)                                               AS ainda_abertas,
       SUM(CASE WHEN c.grupo_problema = 'CALCADA_DANIFICADA' THEN 1 ELSE 0 END) AS calcada_danificada,
       SUM(CASE WHEN c.grupo_problema = 'OBSTACULO' THEN 1 ELSE 0 END)          AS obstaculo,
       SUM(CASE WHEN c.grupo_problema IN ('TRAVESSIA_SEM_RAMPA', 'TRAVESSIA_INSEGURA', 'DESNIVEL') THEN 1 ELSE 0 END) AS travessia_desnivel,
       SUM(CASE WHEN c.grupo_problema IN ('GUIA_SARJETA', 'CALCADA_INEXISTENTE') THEN 1 ELSE 0 END)                AS guia_ou_sem_calcada,
       ROUND(100 * (COUNT(*) / LAG(COUNT(*)) OVER (ORDER BY t.ano_mes) - 1), 1)      AS variacao_mes_anterior_pct
FROM fato_ocorrencias f
JOIN dim_tempo t ON t.id_tempo = f.id_tempo_abertura
JOIN dim_categoria c USING (id_categoria)
GROUP BY t.ano_mes
ORDER BY t.ano_mes;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consulta 4 - Tempo medio de resolucao por categoria e regiao
-- MAGIC **Responde:** quanto tempo a Prefeitura leva para finalizar cada tipo de barreira em cada subprefeitura
-- MAGIC (so solicitacoes FINALIZADAS, grupos com 20+ casos para evitar medias instaveis).
-- MAGIC **Uso:** expor desigualdade de atendimento entre regioes; base para o indicador de SLA no app.

-- COMMAND ----------

SELECT c.grupo_problema_rotulo                             AS grupo_problema,
       l.subprefeitura,
       COUNT(*)                                            AS finalizadas,
       ROUND(AVG(f.dias_resolucao), 1)                     AS media_dias,
       percentile_approx(f.dias_resolucao, 0.5)            AS mediana_dias,
       percentile_approx(f.dias_resolucao, 0.9)            AS p90_dias
FROM fato_ocorrencias f
JOIN dim_categoria c USING (id_categoria)
JOIN dim_localizacao l USING (id_localizacao)
WHERE f.flag_finalizada = 1 AND f.dias_resolucao IS NOT NULL
GROUP BY ALL
HAVING COUNT(*) >= 20
ORDER BY media_dias DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consulta 5 - Cruzamento territorial barreiras x atropelamentos (INDICADOR DE IMPACTO)
-- MAGIC **Pergunta de impacto:** *se a Prefeitura priorizar as 50 areas (celulas H3 de ~0,74 km2) apontadas pelo
-- MAGIC Rota Livre, que parcela dos atropelamentos de pedestres e das barreiras nao resolvidas da cidade essas areas concentram?*
-- MAGIC Area do municipio: ~1.521 km2 (IBGE). Atropelamentos: historico 2022+ (contexto de risco, nao causalidade).

-- COMMAND ----------

WITH ranking AS (
  SELECT *, row_number() OVER (ORDER BY indice_prioridade DESC, atropelamentos DESC) AS posicao
  FROM mvp_mapa_barreiras),
totais AS (
  SELECT SUM(atropelamentos) AS atrop_total, SUM(barreiras_abertas) AS abertas_total,
         SUM(vitimas_fatais) AS fatais_total, COUNT(*) AS celulas_total
  FROM mvp_mapa_barreiras)
SELECT 50                                                            AS areas_priorizadas,
       ROUND(50 * 0.737, 1)                                          AS area_aprox_km2,
       ROUND(100 * 50 * 0.737 / 1521, 2)                             AS pct_area_cidade,
       SUM(r.atropelamentos)                                         AS atropelamentos_nas_areas,
       ROUND(100 * SUM(r.atropelamentos) / MAX(t.atrop_total), 1)    AS pct_atropelamentos_cidade,
       SUM(r.vitimas_fatais)                                         AS vitimas_fatais_nas_areas,
       ROUND(100 * SUM(r.vitimas_fatais) / MAX(t.fatais_total), 1)   AS pct_vitimas_fatais,
       SUM(r.barreiras_abertas)                                      AS barreiras_abertas_nas_areas,
       ROUND(100 * SUM(r.barreiras_abertas) / MAX(t.abertas_total), 1) AS pct_barreiras_abertas
FROM ranking r CROSS JOIN totais t
WHERE r.posicao <= 50;

-- COMMAND ----------

-- MAGIC %md Detalhe: as 15 areas mais criticas (o que a tela "Mapa de barreiras" destaca em vermelho)

-- COMMAND ----------

SELECT ranking_prioridade, classe_prioridade, subprefeitura, distrito,
       barreiras_abertas, barreiras_total, problema_mais_frequente,
       atropelamentos, atropelamentos_confirmados, vitimas_fatais, vitimas_graves,
       indice_prioridade, latitude_centro, longitude_centro
FROM mvp_mapa_barreiras
WHERE classe_prioridade <> 'SEM_CRUZAMENTO'
ORDER BY ranking_prioridade
LIMIT 15;

-- COMMAND ----------

-- MAGIC %md Sensibilidade: concentracao de atropelamentos por classe de prioridade

-- COMMAND ----------

SELECT classe_prioridade,
       COUNT(*)                                                   AS celulas,
       SUM(barreiras_abertas)                                     AS barreiras_abertas,
       SUM(atropelamentos)                                        AS atropelamentos,
       ROUND(100 * SUM(atropelamentos) / SUM(SUM(atropelamentos)) OVER (), 1) AS pct_atropelamentos,
       ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)           AS pct_celulas
FROM mvp_mapa_barreiras
GROUP BY classe_prioridade
ORDER BY CASE classe_prioridade WHEN 'CRITICA' THEN 1 WHEN 'ALTA' THEN 2 WHEN 'MEDIA' THEN 3 WHEN 'BAIXA' THEN 4 ELSE 5 END;
