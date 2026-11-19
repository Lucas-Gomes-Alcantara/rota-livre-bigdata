"""
Coleta Infosiga (Detran-SP) - atropelamentos de pedestres no municipio de Sao Paulo.

Fonte: ZIP com CSVs (latin-1, ';', decimal com virgula), atualizado mensalmente.
A URL do ZIP e descoberta pela API CKAN do Portal de Dados Abertos do Estado
(o portal novo do Infosiga mudou o link em 2026); URLs conhecidas ficam como alternativa.

Filtro da coleta (para gerar um arquivo pequeno): arquivos sinistros_*.csv,
municipio = SAO PAULO, tp_sinistro_atrop_pedestre = 'S', ano_sinistro >= ano_minimo.
Os valores sao mantidos como estao na fonte; a Prata faz tipagem e qualidade.
"""
from __future__ import annotations

import csv
import io
import time
import zipfile
from collections import Counter
from pathlib import Path

from .comum import baixar, ckan_package, detectar, escrever_csv_atomico, formato_recurso, norm


def urls_candidatas(cfg: dict) -> list[str]:
    urls = []
    try:
        pacote = ckan_package(cfg["ckan_url"], cfg["user_agent"])
        for r in pacote.get("resources", []):
            if formato_recurso(r) == "ZIP" and "dicion" not in norm(r.get("name")) and r.get("url"):
                urls.append(r["url"])
    except Exception as e:  # CKAN fora do ar nao deve impedir as URLs conhecidas
        print(f"[infosiga] aviso: CKAN estadual indisponivel: {e!r}")
    return list(dict.fromkeys(urls + list(cfg.get("urls_alternativas", []))))


def obter_zip(cfg: dict) -> tuple[Path, str]:
    destino = Path(cfg["cache_dir"]) / "infosiga" / "dados_infosiga.zip"
    max_idade = float(cfg.get("cache_horas", 20)) * 3600
    if destino.exists() and zipfile.is_zipfile(destino) and time.time() - destino.stat().st_mtime < max_idade:
        return destino, "cache"
    erros = []
    for url in urls_candidatas(cfg):
        try:
            baixar(url, destino, cfg["user_agent"])
        except Exception as e:
            erros.append(f"{url}: {e!r}")
            continue
        if zipfile.is_zipfile(destino):
            return destino, url
        erros.append(f"{url}: resposta nao e ZIP")
        destino.unlink(missing_ok=True)
    raise RuntimeError("Nenhuma URL do Infosiga retornou ZIP valido: " + " | ".join(erros))


def filtrar_zip(arquivo_zip: Path, destino: Path, ano_minimo: int, data_coleta: str,
                municipio: str = "SAO PAULO") -> dict:
    stats = {"linhas_lidas": Counter(), "linhas_filtradas": Counter()}
    colunas: list[str] = []

    with zipfile.ZipFile(arquivo_zip) as z:
        membros = sorted(n for n in z.namelist()
                         if norm(Path(n).name).startswith("sinistros") and n.lower().endswith(".csv"))
        if not membros:
            raise RuntimeError(f"ZIP sem arquivos sinistros_*.csv: {z.namelist()}")
        for nome in membros:  # cabecalho unificado (mesma ordem da fonte)
            with z.open(nome) as f:
                enc, delim = detectar(f.read(65536))
            with z.open(nome) as fb:
                cab = next(csv.reader(io.TextIOWrapper(fb, encoding=enc, errors="replace"), delimiter=delim))
            colunas += [c for c in cab if c not in colunas]
        saida = colunas + ["arquivo_fonte", "fonte", "data_coleta"]

        def linhas():
            for nome in membros:
                with z.open(nome) as f:
                    enc, delim = detectar(f.read(65536))
                with z.open(nome) as fb:
                    leitor = csv.DictReader(io.TextIOWrapper(fb, encoding=enc, errors="replace", newline=""),
                                            delimiter=delim)
                    for linha in leitor:
                        stats["linhas_lidas"][nome] += 1
                        if norm(linha.get("municipio")) != norm(municipio):
                            continue
                        if (linha.get("tp_sinistro_atrop_pedestre") or "").strip().upper() != "S":
                            continue
                        ano = (linha.get("ano_sinistro") or "").strip()
                        if ano.isdigit() and int(ano) < ano_minimo:
                            continue
                        stats["linhas_filtradas"][nome] += 1
                        linha.update(arquivo_fonte=nome, fonte="INFOSIGA", data_coleta=data_coleta)
                        yield linha

        total = escrever_csv_atomico(destino, saida, linhas())
    return {"linhas": total, "arquivos_zip": membros,
            "linhas_lidas": dict(stats["linhas_lidas"]),
            "linhas_filtradas": dict(stats["linhas_filtradas"])}


def coletar(cfg: dict, data_coleta: str) -> dict:
    zip_path, origem = obter_zip(cfg)
    destino = Path(cfg["chegada_dir"]) / f"infosiga_atropelamentos_sp_{data_coleta}.csv"
    st = filtrar_zip(zip_path, destino, int(cfg["ano_minimo"]), data_coleta)
    return {"fonte": "INFOSIGA", "arquivo": str(destino), "origem_zip": origem, **st}
