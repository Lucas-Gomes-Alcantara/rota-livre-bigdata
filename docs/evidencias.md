# Evidencias de execucao - Rota Livre (valores reais do ambiente)

Todos os numeros abaixo vieram das execucoes reais (Airflow local + Databricks Free Edition).
Prints correspondentes em `docs/prints/`.

## 1. Coleta (Airflow - `chegada/_manifesto/coletas.csv`)

| Lote | Run | Fonte | Arquivo | Linhas |
|---|---|---|---|---|
| 1 | `manual__2026-11-18T22:27` | SP156 | `sp156_calcadas_2026-11-18.csv` | 315.155 |
| 1 | `manual__2026-11-18T22:27` | Infosiga | `infosiga_atropelamentos_sp_2026-11-18.csv` | 30.588 |
| 2 | `scheduled__2026-11-19T10:00` (07:00 SP) | SP156 | `sp156_calcadas_2026-11-19.csv` | 315.155 |
| 2 | `scheduled__2026-11-19T10:00` (07:00 SP) | Infosiga | `infosiga_atropelamentos_sp_2026-11-19.csv` | 30.588 |

SP156 por trimestre (linhas ligadas a calcada / linhas lidas): 2025-T4 77.830 / 479.701 ·
2026-T1 90.971 / 577.989 · 2026-T2 69.382 / 463.686 · 2026-T3 76.972 / 484.232.
Infosiga: 1.465.291 linhas lidas no ZIP (3 arquivos de sinistros) -> 30.588 atropelamentos de pedestres na capital, 2022+.

## 2. Bronze (Auto Loader)

| Momento | sp156_solicitacoes | infosiga_sinistros |
|---|---|---|
| Antes do lote 1 | 0 | 0 |
| Depois do lote 1 | 315.155 | 30.588 |
| Depois da 2a execucao (mesmos arquivos) | 315.155 (0 novas) | 30.588 (0 novas) |
| Depois do lote 2 | 630.310 | 61.176 |

## 3. Prata (qualidade + MERGE)

Barrados por regra (cada lote; um registro pode falhar em mais de uma regra):

| Fonte | Avaliados | R1_COORDENADA | R2_DATA | R3_DOMINIO | Total rejeitado | Validos |
|---|---|---|---|---|---|---|
| SP156 | 315.155 | 70.448 | 0 | 277.305 | 277.319 | 37.836 |
| Infosiga | 30.588 | 4.416 | 0 | 0 | 4.416 | 26.172 |

R2 = 0 e resultado do tratamento de formato de data: o arquivo 2026-T3 do SP156 vem em MM/dd/yyyy; lido como
dd/MM/yyyy, 70.877 das 76.972 linhas do trimestre seriam barradas pela R2.

MERGE:

| Fonte | Lote | Entrada | Duplicidades no lote | Validos | Inseridos | Atualizados | Existentes sem mudanca |
|---|---|---|---|---|---|---|---|
| Infosiga | 2026-11-18 | 30.588 | 0 | 26.172 | 26.172 | 0 | 0 |
| SP156 | 2026-11-18 | 315.155 | 0 | 37.836 | 37.836 | 0 | 0 |
| Infosiga | 2026-11-19 | 30.588 | 0 | 26.172 | 0 | 0 | 26.172 |
| SP156 | 2026-11-19 | 315.155 | 0 | 37.836 | 0 | 0 | 37.836 |

Leitura: o lote 2 trouxe o mesmo conteudo (fontes nao atualizaram entre os dias). Todos os registros validos
foram reconhecidos pela chave como ja existentes: 64.008 duplicidades evitadas entre lotes (37.836 + 26.172).
A Bronze guarda os dois lotes (historico bruto); a Prata manteve 1 registro por chave.

## 4. Ouro

| Camada | Tabela | Registros |
|---|---|---|
| bronze | sp156_solicitacoes | 630.310 |
| bronze | infosiga_sinistros | 61.176 |
| prata | sp156_barreiras | 37.836 |
| prata | infosiga_atropelamentos | 26.172 |
| ouro | fato_ocorrencias | 37.836 |
| ouro | fato_sinistros_pedestres | 26.172 |
| ouro | dim_tempo | 1.765 |
| ouro | dim_localizacao | 2.582 (2.463 do SP156, 66 inferidas por celula, 53 sem distrito) |
| ouro | dim_categoria | 9 |
| ouro | mvp_mapa_barreiras | 1.546 celulas H3 (res. 8) |

Integridade referencial: 0 orfaos em todas as 5 checagens. Nenhuma coluna `cep`, `numero`, `latitude` ou `longitude`
em claro nas tabelas da Ouro. Exemplo de mascaramento: CEP `05843160` -> SHA-256 com salt; numero `1243` -> `12XX`;
latitude `-23.6514357` -> `-23.651`.
