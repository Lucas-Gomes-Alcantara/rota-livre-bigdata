#!/usr/bin/env python3
"""
Gabarito independente da Prata (Python puro, sem Spark).
Reaplica as regras do notebook databricks/prata/02_prata.py sobre os CSVs de chegada/
para conferir os numeros que o Databricks mostrar (teste de regressao do lote 1).
Uso: python scripts/gabarito_prata.py chegada/sp156_calcadas_2026-11-18.csv chegada/infosiga_atropelamentos_sp_2026-11-18.csv
"""
import csv, datetime as dt, hashlib, re, sys, unicodedata
from collections import Counter, defaultdict
csv.field_size_limit(2**31 - 1)
LAT, LON = (-24.01, -23.35), (-46.83, -46.36)
HOJE, MIN = dt.date.today(), dt.date(2015, 1, 1)
DOMINIO = {
 "calcada publica solicitar manutencao", "calcada particular denunciar calcada danificada ou inexistente",
 "sugerir construcao de calcadas em areas publicas e pracas", "acessibilidade solicitar avaliacao de obstaculo na calcada",
 "denunciar fiacao solta na calcada", "guias para travessia de pedestres solicitar rebaixamento",
 "guia rebaixada solicitar fiscalizacao de rebaixamento irregular", "guias sarjetas e sarjetoes solicitar manutencao",
 "travessia de pedestres solicitar avaliacao"}

def norm(s):
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", " ", s).strip()
def fnum(v, virg=False):
    v = (v or "").strip().replace(",", ".") if virg else (v or "").strip()
    try: return float(v) if v else None
    except ValueError: return None
def fdata(v, fmt):
    try: return dt.datetime.strptime((v or "").strip()[:10], fmt).date()
    except ValueError: return None
def fora(la, lo): return la is None or lo is None or not (LAT[0] <= la <= LAT[1] and LON[0] <= lo <= LON[1])

def sp156(path):
    rows = list(csv.DictReader(open(path, encoding="utf-8", newline=""), delimiter=";"))
    p = defaultdict(lambda: [0, 0])
    for r in rows:
        a = r["data_abertura"]
        if a[0:2].isdigit() and int(a[0:2]) > 12: p[r["trimestre_fonte"]][0] += 1
        if a[3:5].isdigit() and int(a[3:5]) > 12: p[r["trimestre_fonte"]][1] += 1
    fmt = {t: ("%m/%d/%Y" if v[1] > v[0] else "%d/%m/%Y") for t, v in p.items()}
    print("formato por trimestre:", fmt)
    grupos = Counter(); R = Counter(); valid = 0
    for r in rows:
        f = fmt[r["trimestre_fonte"]]
        ab, fi = fdata(r["data_abertura"], f), fdata(r["data_finalizacao"], f)
        la, lo = fnum(r["latitude"]), fnum(r["longitude"])
        sk = norm(r["servico"])
        base = (str(ab), sk, r["latitude"].strip(), r["longitude"].strip(), r["cep"], norm(r["logradouro"]), r["numero"].strip(), r["meio_comunicacao"].strip().upper())
        grupos[base] += 1
        r1 = fora(la, lo)
        r2 = ab is None or ab > HOJE or ab < MIN or (f"{ab.year}-T{(ab.month-1)//3+1}" != r["trimestre_fonte"]) or (fi is not None and fi < ab)
        r3 = sk not in DOMINIO
        R["R1_COORDENADA"] += r1; R["R2_DATA"] += r2; R["R3_DOMINIO"] += r3; R["TOTAL_REJEITADOS"] += (r1 or r2 or r3)
        valid += not (r1 or r2 or r3)
    print(f"SP156  entrada={len(rows)} unicos={len(rows)} (ordinal separa {sum(c-1 for c in grupos.values() if c>1)} identicas) validos={valid}")
    print("       barrados por regra:", dict(R))

def infosiga(path):
    rows = list(csv.DictReader(open(path, encoding="utf-8", newline=""), delimiter=";"))
    ids = {r["id_sinistro"] for r in rows}
    R = Counter(); valid = 0
    for r in rows:
        d = fdata(r["data_sinistro"], "%d/%m/%Y")
        r1 = fora(fnum(r["latitude"], True), fnum(r["longitude"], True))
        r2 = d is None or d > HOJE or d < MIN
        r3 = (not r["id_sinistro"].strip()) or r["tp_sinistro_atrop_pedestre"].strip().upper() != "S" or norm(r["municipio"]) != "sao paulo" \
             or r["tipo_registro"].strip().upper() not in ("SINISTRO FATAL", "SINISTRO NAO FATAL", "NOTIFICACAO")
        R["R1_COORDENADA"] += r1; R["R2_DATA"] += r2; R["R3_DOMINIO"] += r3; R["TOTAL_REJEITADOS"] += (r1 or r2 or r3)
        valid += not (r1 or r2 or r3)
    print(f"INFOSIGA entrada={len(rows)} unicos={len(ids)} validos={valid}")
    print("       barrados por regra:", dict(R))

if __name__ == "__main__":
    sp156(sys.argv[1]); infosiga(sys.argv[2])
