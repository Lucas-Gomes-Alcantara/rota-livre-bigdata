-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Rota Livre - 00 Setup (Unity Catalog / Databricks Free Edition)
-- MAGIC Cria os schemas das camadas e os Volumes. Rodar **uma vez**, num SQL Warehouse ou notebook serverless.
-- MAGIC
-- MAGIC Depois, envie os arquivos de `chegada/` pela UI:
-- MAGIC **Catalog > workspace > rota_livre_bronze > Volumes > chegada > Upload to this volume**.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.rota_livre_bronze COMMENT 'Rota Livre - dados brutos (Auto Loader)';
CREATE SCHEMA IF NOT EXISTS workspace.rota_livre_prata  COMMENT 'Rota Livre - dados tipados, limpos e deduplicados (MERGE)';
CREATE SCHEMA IF NOT EXISTS workspace.rota_livre_ouro   COMMENT 'Rota Livre - modelo dimensional e visoes do MVP';

-- COMMAND ----------

-- Volume de pouso: recebe os CSVs gerados pelo Airflow (upload manual, um lote por dia)
CREATE VOLUME IF NOT EXISTS workspace.rota_livre_bronze.chegada
  COMMENT 'Arquivos da pasta chegada/ do Airflow, enviados manualmente em lotes';

-- Volume tecnico: checkpoints e schemas do Auto Loader
CREATE VOLUME IF NOT EXISTS workspace.rota_livre_bronze.sistema
  COMMENT 'Checkpoints e schemaLocation do Auto Loader';

-- COMMAND ----------

SHOW VOLUMES IN workspace.rota_livre_bronze;
