"""Utilitarios compartilhados pelas coletas (somente biblioteca padrao)."""
from __future__ import annotations

import csv
import json
import os
import re
import unicodedata
from pathlib import Path
from urllib.request import Request, urlopen

csv.field_size_limit(2**31 - 1)


def norm(s) -> str:
    """Minusculas, sem acento e com espacos simples (para comparar textos)."""
    s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode()
    return re.sub(r"\s+", " ", s).strip().lower()


def _headers(user_agent: str, api_key: str | None = None) -> dict:
    h = {"User-Agent": user_agent}
    if api_key:  # opcional: chave CKAN guardada em Variable, nunca no codigo
        h["Authorization"] = api_key
    return h


def ckan_package(url: str, user_agent: str, api_key: str | None = None) -> dict:
    with urlopen(Request(url, headers=_headers(user_agent, api_key)), timeout=120) as r:
        corpo = json.load(r)
    if not corpo.get("success"):
        raise RuntimeError(f"CKAN respondeu sem sucesso: {url}")
    return corpo["result"]


def baixar(url: str, destino: Path, user_agent: str, timeout: int = 900) -> Path:
    """Download em streaming para arquivo temporario + rename atomico."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_name(destino.name + ".part")
    with urlopen(Request(url, headers=_headers(user_agent)), timeout=timeout) as r, open(tmp, "wb") as f:
        while True:
            bloco = r.read(1 << 20)
            if not bloco:
                break
            f.write(bloco)
    os.replace(tmp, destino)
    return destino


def detectar(amostra: bytes) -> tuple[str, str]:
    """(encoding, delimitador) a partir dos primeiros bytes do arquivo."""
    try:
        amostra.decode("utf-8-sig")
        enc = "utf-8-sig"
    except UnicodeDecodeError:
        enc = "latin-1"
    linhas = "\n".join(amostra.decode(enc, errors="ignore").splitlines()[:20])
    try:
        delim = csv.Sniffer().sniff(linhas, delimiters=";,|\t").delimiter
    except csv.Error:
        delim = ";"
    return enc, delim


def escrever_csv_atomico(destino: Path, colunas: list[str], linhas) -> int:
    """Escreve CSV UTF-8 com ';' em .part e renomeia ao final.
    O sensor do DAG nunca enxerga um arquivo pela metade."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_name(destino.name + ".part")
    n = 0
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=colunas, delimiter=";", quoting=csv.QUOTE_ALL,
                           extrasaction="ignore")
        w.writeheader()
        for linha in linhas:
            w.writerow(linha)
            n += 1
    os.replace(tmp, destino)
    return n


def formato_recurso(recurso: dict) -> str:
    """Formato declarado no CKAN, tolerando '.CSV', 'csv', vazio (usa extensao da URL)."""
    fmt = (recurso.get("format") or "").strip().strip(".").upper()
    if not fmt:
        fmt = Path(recurso.get("url") or "").suffix.strip(".").upper()
    return fmt
