# Consultas analiticas - resultados reais e interpretacao

Fonte: `databricks/consultas/04_consultas.sql`, executado sobre a Ouro em 19/11/2026.
Base: 37.836 solicitacoes de barreiras (SP156, out/2025 a set/2026) e 26.172 atropelamentos de pedestres
na capital (Infosiga, 2022 a 2026). "Abertas" = status ABERTA ou EM_ANDAMENTO no momento da extracao da fonte.

> Cuidados de leitura validos para todas as consultas
> - **"Finalizada" e o status do protocolo no SP156**, nao prova de obra executada: inclui pedidos encerrados
>   administrativamente. Por isso categorias de "sugestao" chegam a 99% finalizadas.
> - Os numeros sao **contagens absolutas**, sem ajuste por area ou populacao do distrito.
> - Barreiras e atropelamentos no mesmo lugar indicam **coincidencia territorial, nao causalidade**.

---

## Consulta 1 - Regioes com maior quantidade de ocorrencias

| Subprefeitura | Distrito | Ocorrencias | Abertas | % abertas | % do total |
|---|---|---|---|---|---|
| CAMPO LIMPO | CAPAO REDONDO | 836 | 200 | 23,9 | 2,21 |
| IPIRANGA | SACOMA | 800 | 337 | 42,1 | 2,11 |
| CAPELA DO SOCORRO | GRAJAU | 789 | 239 | 30,3 | 2,09 |
| JABAQUARA | JABAQUARA | 772 | 209 | 27,1 | 2,04 |
| ITAQUERA | ITAQUERA | 766 | 638 | 83,3 | 2,02 |
| JACANA-TREMEMBE | TREMEMBE | 744 | 444 | 59,7 | 1,97 |
| VILA MARIANA | VILA MARIANA | 738 | 321 | 43,5 | 1,95 |
| PIRITUBA-JARAGUA | JARAGUA | 654 | 442 | 67,6 | 1,73 |
| CAMPO LIMPO | CAMPO LIMPO | 649 | 170 | 26,2 | 1,72 |
| PINHEIROS | ITAIM BIBI | 648 | 290 | 44,8 | 1,71 |
| SAPOPEMBA | SAPOPEMBA | 646 | 461 | 71,4 | 1,71 |
| PIRITUBA-JARAGUA | PIRITUBA | 643 | 352 | 54,7 | 1,70 |
| IPIRANGA | IPIRANGA | 621 | 330 | 53,1 | 1,64 |
| CIDADE ADEMAR | CIDADE ADEMAR | 594 | 154 | 25,9 | 1,57 |
| M BOI MIRIM | JARDIM SAO LUIS | 573 | 306 | 53,4 | 1,51 |

**Interpretacao.** As reclamacoes estao espalhadas: nenhum distrito passa de 2,2% do total, e os 15 primeiros
somam cerca de 28%. O dado que mais diferencia as regioes e a **taxa de pendencia**, nao o volume:
Itaquera tem quase o mesmo volume de Capao Redondo (766 x 836), mas 83% das solicitacoes seguem abertas,
contra 24%. Sapopemba (71%) e Jaragua (68%) tambem se destacam.

**Uso.** Ordenar vistorias por pendencia, e nao so por volume; cobrar das subprefeituras com fila alta.

---

## Consulta 2 - Categorias de barreiras mais frequentes

| Grupo | Servico SP156 | Ocorrencias | % do total | % finalizadas | Abertas |
|---|---|---|---|---|---|
| Guia e sarjeta | Guias, sarjetas e sarjetoes - solicitar manutencao | 15.782 | 41,7 | 58,4 | 6.444 |
| Calcada danificada | Calcada publica - solicitar manutencao | 6.534 | 17,3 | 71,8 | 1.806 |
| Obstaculo na calcada | Acessibilidade - avaliacao de obstaculo na calcada | 5.356 | 14,2 | 17,7 | 4.354 |
| Calcada danificada | Calcada particular - calcada danificada ou inexistente | 4.524 | 12,0 | 21,1 | 3.498 |
| Travessia insegura | Travessia de pedestres - solicitar avaliacao | 2.138 | 5,7 | 81,2 | 401 |
| Obstaculo na calcada | Denunciar fiacao solta na calcada | 1.317 | 3,5 | 13,1 | 1.140 |
| Travessia sem rampa | Guias para travessia - solicitar rebaixamento | 770 | 2,0 | 87,8 | 90 |
| Desnivel | Guia rebaixada - fiscalizacao de rebaixamento irregular | 761 | 2,0 | 20,5 | 598 |
| Calcada inexistente | Sugerir construcao de calcadas | 654 | 1,7 | 99,1 | 6 |

**Interpretacao.** Guia/sarjeta lidera em volume, mas so existe nos tres primeiros trimestres (ver Consulta 3).
O achado mais relevante para acessibilidade esta nas **categorias que bloqueiam a passagem**: obstaculo na
calcada (17,7% finalizadas), fiacao solta (13,1%), desnivel por rebaixamento irregular (20,5%) e calcada
particular danificada (21,1%) tem as menores taxas de encerramento. Juntas, somam **9.590 das 18.337
barreiras ainda abertas (52%)**. Ja calcada publica (71,8%) e rebaixamento para travessia (87,8%) andam melhor.

**Uso.** Definir as categorias do formulario do app; direcionar obstaculos/fiacao/calcada particular aos
orgaos certos, porque sao as filas que menos andam.

---

## Consulta 3 - Evolucao temporal das ocorrencias

| Mes | Ocorrencias | Ainda abertas | Calcada danificada | Obstaculo | Travessia/desnivel | Guia ou sem calcada | Var. mes ant. (%) |
|---|---|---|---|---|---|---|---|
| 2025-10 | 3.774 | 1.166 | 734 | 355 | 274 | 2.411 | - |
| 2025-11 | 3.631 | 1.201 | 709 | 417 | 263 | 2.242 | -3,8 |
| 2025-12 | 3.050 | 1.095 | 691 | 421 | 239 | 1.699 | -16,0 |
| 2026-01 | 4.420 | 1.807 | 1.242 | 405 | 242 | 2.531 | 44,9 |
| 2026-02 | 4.861 | 2.321 | 1.281 | 530 | 279 | 2.771 | 10,0 |
| 2026-03 | 5.036 | 2.595 | 1.356 | 657 | 309 | 2.714 | 3,6 |
| 2026-04 | 3.727 | 2.127 | 1.082 | 669 | 310 | 1.666 | -26,0 |
| 2026-05 | 2.039 | 1.234 | 898 | 609 | 308 | 224 | -45,3 |
| 2026-06 | 1.642 | 1.051 | 769 | 531 | 307 | 35 | -19,5 |
| 2026-07 | 1.728 | 1.089 | 686 | 694 | 295 | 53 | 5,2 |
| 2026-08 | 2.044 | 1.327 | 820 | 733 | 445 | 46 | 18,3 |
| 2026-09 | 1.884 | 1.324 | 790 | 652 | 398 | 44 | -7,8 |

**Interpretacao.**
1. **A queda de ~5.000 para ~1.900 por mes a partir de maio/2026 e um artefato da fonte, nao melhora real.**
   O servico "Guias, sarjetas e sarjetoes" some do SP156 no 2026-T3 (estava presente nos 3 trimestres
   anteriores; ver `docs/verificacao`). A coluna "guia ou sem calcada" cai de 2.714 para 35. Descontando essa
   coluna, o volume mensal fica entre 1.351 e 2.322 (minimo em dez/2025, maximo em mar/2026) e termina o periodo
   em 1.840 - nao ha queda estrutural das reclamacoes.
2. **Obstaculos na calcada dobraram**: de 355 (out/2025) para 733 (ago/2026), +106%.
3. **Pico de calcada danificada em jan-mar/2026** (1.242 a 1.356 por mes, contra ~700 no fim de 2025),
   compativel com o periodo de chuvas - hipotese a confirmar com mais anos de dados.
4. "Ainda abertas" e maior nos meses recentes tambem porque esses pedidos tiveram menos tempo para serem resolvidos.

**Uso.** Monitorar a fila por categoria; avisar o usuario do app quando houver mudanca no catalogo da Prefeitura.

---

## Consulta 4 - Tempo medio de resolucao por categoria e regiao (top 20, min. 20 casos)

| Grupo | Subprefeitura | Finalizadas | Media (dias) | Mediana | P90 |
|---|---|---|---|---|---|
| Desnivel | IPIRANGA | 119 | 149,0 | 141 | 209 |
| Obstaculo na calcada | ITAIM PAULISTA | 28 | 106,8 | 95 | 170 |
| Obstaculo na calcada | MOOCA | 26 | 105,8 | 88 | 238 |
| Obstaculo na calcada | M BOI MIRIM | 21 | 101,6 | 107 | 148 |
| Guia e sarjeta | CASA VERDE-CACHOEIRINHA | 163 | 73,2 | 67 | 131 |
| Calcada danificada | FREGUESIA-BRASILANDIA | 128 | 70,4 | 54 | 162 |
| Guia e sarjeta | FREGUESIA-BRASILANDIA | 344 | 70,3 | 62 | 154 |
| Obstaculo na calcada | BUTANTA | 59 | 68,4 | 63 | 163 |
| Obstaculo na calcada | SANTO AMARO | 60 | 67,6 | 56 | 123 |
| Obstaculo na calcada | ARICANDUVA-FORMOSA-CARRAO | 29 | 66,8 | 56 | 160 |
| Obstaculo na calcada | PIRITUBA-JARAGUA | 72 | 66,0 | 44 | 184 |
| Obstaculo na calcada | VILA PRUDENTE | 93 | 62,2 | 44 | 147 |
| Obstaculo na calcada | IPIRANGA | 52 | 62,0 | 34 | 140 |
| Obstaculo na calcada | CIDADE TIRADENTES | 34 | 60,3 | 29 | 190 |
| Obstaculo na calcada | GUAIANASES | 63 | 58,2 | 54 | 121 |
| Obstaculo na calcada | VILA MARIANA | 111 | 57,7 | 38 | 134 |
| Obstaculo na calcada | JACANA-TREMEMBE | 54 | 57,6 | 34 | 130 |
| Travessia insegura | FREGUESIA-BRASILANDIA | 47 | 53,6 | 21 | 153 |
| Calcada danificada | CASA VERDE-CACHOEIRINHA | 24 | 52,5 | 43 | 97 |
| Guia e sarjeta | SAO MATEUS | 129 | 52,0 | 42 | 133 |

**Interpretacao.** O caso mais lento e **desnivel por rebaixamento irregular no Ipiranga: quase 5 meses em media**
(mediana 141 dias, 10% passam de 209). **Obstaculo na calcada domina o ranking** (13 das 20 linhas), passando de
100 dias em Itaim Paulista, Mooca e M'Boi Mirim. A mesma categoria varia muito entre regioes (obstaculo: 106,8 dias
em Itaim Paulista x 57,7 na Vila Mariana), sinal de desigualdade de atendimento. O tempo real e **maior** que o
mostrado: so entram pedidos ja finalizados, e os ainda abertos (os mais demorados) ficam de fora.

**Uso.** Base para um indicador de prazo no app ("pedidos deste tipo nesta regiao levam ~X dias").

---

## Consulta 5 - Cruzamento territorial barreiras x atropelamentos (INDICADOR DE IMPACTO)

**Pergunta de impacto:** se a Prefeitura priorizar as 50 areas apontadas pelo Rota Livre, que parcela dos
atropelamentos de pedestres e das barreiras nao resolvidas essas areas concentram?

| Areas | Area aprox. | % da cidade | Atropelamentos | % do total | Vitimas fatais | % do total | Barreiras abertas | % do total |
|---|---|---|---|---|---|---|---|---|
| 50 celulas H3 | 36,9 km2 | 2,42% | 3.189 | 12,2% | 178 | 11,5% | 2.444 | 13,3% |

**Indicador de impacto: 2,4% da area da cidade concentra 12,2% dos atropelamentos de pedestres (2022-2026),
11,5% das vitimas fatais e 13,3% das barreiras ainda abertas - cerca de 5 vezes o esperado pela area.**

Sensibilidade por classe de prioridade (1.546 celulas):

| Classe | Celulas | % celulas | Barreiras abertas | Atropelamentos | % atropelamentos |
|---|---|---|---|---|---|
| CRITICA | 64 | 4,1 | 3.037 | 3.721 | 14,2 |
| ALTA | 190 | 12,3 | 4.915 | 6.688 | 25,6 |
| MEDIA | 378 | 24,5 | 5.930 | 8.482 | 32,4 |
| BAIXA | 638 | 41,3 | 4.325 | 6.685 | 25,5 |
| SEM_CRUZAMENTO | 276 | 17,9 | 130 | 596 | 2,3 |

As celulas CRITICA + ALTA (16,4% das celulas) concentram **39,8% dos atropelamentos** e **43,4% das barreiras
abertas**. As 15 areas mais criticas ficam sobretudo no centro (Se: Consolacao, Republica, Santa Cecilia,
Bela Vista, Se), alem de Santana, Jacana, Santo Amaro, Sacoma, Tucuruvi, Parque do Carmo e Ipiranga.

**Interpretacao.** Ha forte coincidencia territorial entre calcada ruim e atropelamento, o que justifica usar o
mapa como fila de prioridade. **Limite importante:** o centro concentra fluxo de pedestres, o que eleva tanto
atropelamentos quanto reclamacoes; o indice mostra onde os dois problemas se sobrepoem, nao que a calcada cause o
atropelamento. Os periodos tambem diferem (barreiras de 12 meses x atropelamentos de ~5 anos).

**Uso.** E a regra de cor da tela "Mapa de barreiras" (`classe_prioridade`) e o argumento do projeto junto a
Prefeitura: concentrar zeladoria de calcadas nas 50 areas atinge uma parcela desproporcional do risco ao pedestre.
