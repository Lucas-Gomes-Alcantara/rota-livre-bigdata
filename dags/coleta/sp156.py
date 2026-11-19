"""
Coleta SP156 (Prefeitura de Sao Paulo) - solicitacoes ligadas a calcadas.

Fonte: Portal de Dados Abertos da Prefeitura (CKAN), dataset 'dados-do-sp156'.
Cada trimestre e um CSV (latin-1, ';'). Os nomes das colunas variam entre trimestres,
por isso o cabecalho e harmonizado por ALIASES. Os VALORES nao sao alterados:
tipagem, limpeza e regras de qualidade ficam na Prata (Databricks).

Filtro da coleta: palavras-chave (Variable) em Tema/Assunto/Servico. E propositalmente
amplo; o dominio exato de barreiras e aplicado pela regra de qualidade 3 na Prata.
"""
from __future__ import annotations

import csv
import re
from collections import Counter
from pathlib import Path

from .comum import baixar, ckan_package, detectar, escrever_csv_atomico, formato_recurso, norm

# coluna canonica -> nomes possiveis no arquivo de origem (normalizados com norm()).
ALIASES = {
    "data_abertura": ["data de abertura"],
    "data_parecer": ["data do parecer"],
    "data_finalizacao": ["data de finalizacao"],
    "meio_comunicacao": ["meio de comunicacao"],
    "tema": ["tema"],
    "assunto": ["assunto"],
    "servico": ["servico"],
    "status": ["status", "status da solicitacao"],
    "orgao": ["orgao"],
    "logradouro": ["logradouro"],
    "numero": ["numero"],
    "cep": ["cep"],
    "latitude": ["latitude"],
    "longitude": ["longitude"],
    "subprefeitura": ["subprefeitura", "prefeitura operacional"],
    "distrito": ["distrito"],
}
COLUNAS_SAIDA = list(ALIASES) + ["trimestre_fonte", "recurso_fonte", "fonte", "data_coleta"]
_RE_TRI = re.compile(r"(\d)\s*[ºo°]?\s*TRI\w*\s*(\d{4})", re.I)


def trimestres_recentes(pacote: dict, n: int) -> list[dict]:
    """Os n trimestres mais recentes com recurso CSV (1 recurso por trimestre)."""
    por_tri: dict[str, dict] = {}
    for r in pacote.get("resources", []):
        m = _RE_TRI.search(r.get("name") or "")
        if not m or formato_recurso(r) != "CSV":
            continue
        tri = f"{m.group(2)}-T{m.group(1)}"
        atual = por_tri.get(tri)
        if atual is None or (r.get("last_modified") or "") > (atual.get("last_modified") or ""):
            por_tri[tri] = {**r, "trimestre": tri}
    return [por_tri[t] for t in sorted(por_tri, reverse=True)[:n]]


def mapear_cabecalho(colunas: list[str]) -> tuple[dict, list[str]]:
    """{coluna_original: coluna_canonica} e lista de colunas sem correspondencia."""
    mapa, desconhecidas = {}, []
    for c in colunas:
        n = norm(c)
        alvo = next((k for k, v in ALIASES.items() if n in v), None)
        if alvo:
            mapa[c] = alvo
        else:
            desconhecidas.append(c)
    return mapa, desconhecidas


def filtrar_trimestre(arquivo: Path, destino: Path, palavras: list[str], trimestre: str,
                      recurso: str) -> dict:
    """Le um CSV trimestral bruto e grava somente as linhas ligadas a calcada."""
    with open(arquivo, "rb") as f:
        enc, delim = detectar(f.read(65536))
    palavras = [norm(p) for p in palavras]
    stats = {"trimestre": trimestre, "encoding": enc, "linhas_lidas": 0}

    def linhas():
        with open(arquivo, encoding=enc, newline="", errors="replace") as f:
            leitor = csv.DictReader(f, delimiter=delim)
            mapa, desconhecidas = mapear_cabecalho(leitor.fieldnames or [])
            stats["colunas_desconhecidas"] = desconhecidas
            stats["colunas_ausentes"] = [k for k in ALIASES if k not in mapa.values()]
            for bruta in leitor:
                stats["linhas_lidas"] += 1
                linha = {mapa[k]: v for k, v in bruta.items() if k in mapa}
                texto = norm(f"{linha.get('tema')} {linha.get('assunto')} {linha.get('servico')}")
                if any(p in texto for p in palavras):
                    linha.update(trimestre_fonte=trimestre, recurso_fonte=recurso, fonte="SP156")
                    yield linha

    stats["linhas_filtradas"] = escrever_csv_atomico(destino, COLUNAS_SAIDA, linhas())
    return stats


def coletar(cfg: dict, data_coleta: str) -> dict:
    """Gera chegada/sp156_calcadas_<data>.csv com os n trimestres mais recentes.

    Cache: o recorte filtrado de cada trimestre fica salvo e e reaproveitado enquanto o
    recurso no portal nao mudar (mesmo id + last_modified). Assim a coleta diaria nao
    rebaixa ~100 MB por trimestre sem necessidade.
    """
    cache = Path(cfg["cache_dir"]) / "sp156"
    pacote = ckan_package(cfg["ckan_url"], cfg["user_agent"], cfg.get("api_key"))
    recursos = trimestres_recentes(pacote, int(cfg["trimestres"]))
    if not recursos:
        raise RuntimeError("Nenhum CSV trimestral do SP156 encontrado no CKAN")

    partes, detalhes = [], []
    for r in recursos:
        versao = re.sub(r"[^0-9A-Za-z]", "", r.get("last_modified") or r.get("created") or "sem_data")
        filtrado = cache / f"filtrado_{r['trimestre']}_{r['id'][:8]}_{versao}.csv"
        if filtrado.exists():
            detalhes.append({"trimestre": r["trimestre"], "cache": True})
        else:
            bruto = baixar(r["url"], cache / f"bruto_{r['trimestre']}.csv", cfg["user_agent"])
            st = filtrar_trimestre(bruto, filtrado, cfg["palavras_chave"], r["trimestre"], r["name"])
            bruto.unlink(missing_ok=True)
            detalhes.append({**st, "cache": False, "url": r["url"]})
        partes.append(filtrado)

    destino = Path(cfg["chegada_dir"]) / f"sp156_calcadas_{data_coleta}.csv"
    por_tri = Counter()

    def todas():
        for parte in partes:
            with open(parte, encoding="utf-8", newline="") as f:
                for linha in csv.DictReader(f, delimiter=";"):
                    linha["data_coleta"] = data_coleta
                    por_tri[linha["trimestre_fonte"]] += 1
                    yield linha

    total = escrever_csv_atomico(destino, COLUNAS_SAIDA, todas())
    return {"fonte": "SP156", "arquivo": str(destino), "linhas": total,
            "trimestres": [r["trimestre"] for r in recursos],
            "linhas_por_trimestre": dict(por_tri), "detalhes": detalhes}
