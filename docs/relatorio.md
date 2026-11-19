# Relatorio - Rota Livre | Big Data Architecture & Data Integration

Roteiro do PDF na ordem exigida pelo enunciado. Numeros: `docs/evidencias.md` e `docs/consultas.md`.
Itens marcados **[PENDENTE]** dependem de informacao ou print que ainda nao existe - nao inventar.

---

## 1. Integrantes

| Nome completo | RM |
|---|---|
| Lucas Gomes Alcântara | RM98766 |
| Gabriel Gomes Catanzaro | RM93445 |
| Henrique Canali Cuzato | RM551326 |

## 2. Produto e perguntas do MVP
**Produto em uma frase:** o Rota Livre mapeia barreiras de acessibilidade nas calcadas de Sao Paulo e aponta onde
consertá-las protege mais pedestres.

Perguntas respondidas pelo MVP:
1. Onde estao as barreiras de calcada registradas pelos cidadaos e quantas seguem sem solucao?
2. Quais tipos de barreira sao mais frequentes e quais demoram mais para ser resolvidos, em cada regiao?
3. Em quais areas barreiras nao resolvidas e atropelamentos de pedestres se sobrepoem?

## 3. Diagrama do fluxo de dados
Diagrama do `README.md` (Mermaid; o GitHub renderiza). Print: **[PENDENTE]** `Diagrama.png`
(print do README no GitHub ou exportacao do diagrama).

## 4. Fontes

| Fonte | Origem | Formato | Frequencia |
|---|---|---|---|
| SP156 - Solicitacoes | Prefeitura de SP (SMIT) - Portal de Dados Abertos, dataset `dados-do-sp156`, via API CKAN | CSV trimestral, latin-1, `;` | trimestral |
| Infosiga - Sinistros | Detran-SP - Portal de Dados Abertos do Estado, dataset `sinistros-infosiga`, via API CKAN | ZIP com CSVs, latin-1, `;` | mensal (~dia 15) |

## 5. Requisitos 01 a 09 (print + explicacao curta)

| # | Requisito | Status | Evidencia (print) | Explicacao curta |
|---|---|---|---|---|
| 01 | 2+ fontes, 1+ API/base aberta | **Atendido** | `docs/verificacao/relatorio_fontes.json` | SP156 e Infosiga, ambas abertas e acessadas por API CKAN. |
| 02 | DAG agendado, 1 arquivo por coleta com data | **Atendido** | `docs/prints/AIRFLOW/AIRFLOW - Execução DAG 2 - grade.png`, `docs/prints/DATABRICKS/0. Lotes de Volume.png` | `schedule="0 7 * * *"`; o lote 2 veio do run `scheduled__2026-11-19`. Arquivos `*_AAAA-MM-DD.csv`. |
| 03 | 2 tarefas paralelas, sensor de chegada, credenciais em Variables | **Atendido** | `docs/prints/AIRFLOW/AIRFLOW - Execução DAG 1 - gráfico.png`, `docs/prints/AIRFLOW/AIRFLOW - Váriaveis.png` | `coletar_sp156` e `coletar_infosiga` em paralelo; `verificar_chegada` confere existencia, arquivo completo e contagem; 10 Variables `rota_livre_*` criptografadas, chave da API mascarada (`***`). |
| 04 | Volume em 2 lotes de dias diferentes | **Atendido** | `docs/prints/DATABRICKS/0. Lotes de Volume.png`, `docs/prints/DATABRICKS/1. BRONZE/Camada BRONZE - execução 2 - Evidências 1.png` | Lote 1 em 18/11, lote 2 em 19/11 (coluna `lotes_novos`). |
| 05 | Bronze com Auto Loader, so arquivos novos, sem duplicar | **Atendido** | `docs/prints/DATABRICKS/1. BRONZE/` (5 prints) | Checkpoint: reexecucao = 0 linhas novas; lote 2 carregou so os arquivos de 19/11; consulta de arquivo carregado 2x = vazia. |
| 06 | Prata: tipos, nulos/invalidos, MERGE, 3 regras com contagem | **Atendido** | `docs/prints/DATABRICKS/2. PRATA/` (8 prints) | R1 coordenada, R2 data, R3 dominio em `log_qualidade`; MERGE por chave (lote 2: 0 inseridos, 64.008 existentes). |
| 07 | Ouro: 1 fato, 2+ dimensoes, 5 consultas, 1 de impacto | **Atendido** | `docs/prints/DATABRICKS/3. OURO/Camada OURO - Evidências 1.png` e `2.png`, `docs/prints/DATABRICKS/4. CONSULTAS/` | 2 fatos + 3 dimensoes, 0 orfaos; consulta 5 = indicador de impacto. |
| 08 | Dados pessoais protegidos; tabela e tela do MVP | **Atendido** | `docs/prints/DATABRICKS/3. OURO/Camada OURO - Evidências 3.png`, `4.png`, `5.png` | CEP -> SHA-256 com salt, numero mascarado, coordenada com 3 casas. `mvp_mapa_barreiras` -> tela **"Mapa de barreiras"** (campo -> elemento no README). |
| 09 | Repositorio com README (integrantes, arquitetura, reproducao) | **Parcialmente atendido** | **[PENDENTE: print do repositorio no GitHub]** | README com integrantes, arquitetura e reproducao em https://github.com/Lucas-Gomes-Alcantara/rota-livre-bigdata. |

## 6. Airflow - visoes Graph e Grid
Pasta `docs/prints/AIRFLOW/`: `AIRFLOW - Execução DAG 1 - gráfico.png`, `... DAG 1 - grade.png`, `... DAG 2 - gráfico.png`, `... DAG 2 - grade.png`.
Nota: a Grid mostra runs com falha em 18/11 antes das 19:27. Causa diagnosticada: sem `FERNET_KEY` fixa,
cada container gerava a propria chave e o api-server nao conseguia descriptografar as Variables
(`Can't decrypt _val`). Correcao: chave Fernet unica no `.env`, compartilhada por todos os componentes.

## 7. Contagens da Bronze antes e depois do 2o lote e da 2a execucao
Print: `docs/prints/DATABRICKS/1. BRONZE/Camada BRONZE - execução 2 - Evidências 1.png` (historico completo de `log_ingestao`).

| Momento | SP156 antes -> depois | Infosiga antes -> depois | Linhas novas |
|---|---|---|---|
| Lote 1 (18/11 22:55 UTC) | 0 -> 315.155 | 0 -> 30.588 | 315.155 / 30.588 |
| 2a execucao (18/11 22:57 e 22:58 UTC) | 315.155 -> 315.155 | 30.588 -> 30.588 | 0 / 0 |
| Lote 2 (19/11 10:43 UTC) | 315.155 -> 630.310 | 30.588 -> 61.176 | 315.155 / 30.588 (so arquivos de 19/11) |

As linhas 5 a 8 do print sao a mesma execucao de reprocessamento registrada duas vezes (celula reexecutada apos
corrigir um erro de exibicao); todas com 0 linhas novas.

## 8. Cinco consultas SQL com resultado e interpretacao
Conteudo integral em `docs/consultas.md` (tabelas reais + interpretacao + uso). Prints em `docs/prints/DATABRICKS/4. CONSULTAS/`.

## 9. Conclusao tecnica e link do repositorio
O pipeline coleta duas fontes publicas reais com Airflow 3 (agendado, tarefas paralelas, sensor e configuracao em
Variables), entrega um arquivo datado por coleta e alimenta o Databricks Free Edition em lotes manuais. A Bronze com
Auto Loader provou ingestao incremental e idempotente (reexecucao = 0 linhas; lote 2 = so arquivos novos). A Prata
tratou problemas reais das fontes - formato de data trocado no SP156 2026-T3, ausencia de protocolo, coordenadas
ausentes - com tres regras de qualidade registradas por regra e MERGE por chave, que reconheceu as 64.008 linhas
repetidas do lote 2 como existentes. A Ouro organiza os dados em modelo dimensional com celulas H3, protege CEP e
endereco e entrega a tabela `mvp_mapa_barreiras` para a tela "Mapa de barreiras".

Resultado de impacto: **50 areas (2,4% da cidade) concentram 12,2% dos atropelamentos de pedestres de 2022-2026,
11,5% das vitimas fatais e 13,3% das barreiras de calcada ainda abertas** - uma fila de prioridade objetiva para
zeladoria. Limites: coincidencia territorial nao e causalidade; "finalizada" e status de protocolo; o servico de guias
e sarjetas saiu do catalogo SP156 em 2026-T3.

Proximos passos: atualizar a coleta quando sair o SP156 2026-T4 e o Infosiga de dezembro (o MERGE vai mostrar
registros atualizados), normalizar contagens por area/populacao e incluir os relatos do proprio app Rota Livre.

Repositorio: https://github.com/Lucas-Gomes-Alcantara/rota-livre-bigdata
