#!/usr/bin/env python3
"""
Rota Livre - Passo 0: verificacao das fontes reais (SP156 e Infosiga).

Usa somente a biblioteca padrao do Python (sem pip install).
Gera:
  docs/verificacao/relatorio_fontes.json     -> resumo de tudo
  docs/verificacao/sp156_servicos_candidatos.csv -> Tema/Assunto/Servico ligados a calcada
  docs/verificacao/infosiga_colunas.json     -> cabecalhos reais dos CSVs do ZIP
  amostras/sp156_amostra.csv, amostras/infosiga_sinistros_amostra.csv (50 linhas)

Uso (na raiz do projeto):
  python scripts/00_verificar_fontes.py
ou, sem Python no Windows:
  docker run --rm -v "${PWD}:/w" -w /w python:3.12-slim python scripts/00_verificar_fontes.py
"""
import csv
import datetime as dt
import io
import json
import re
import sys
import time
import unicodedata
import zipfile
from collections import Counter
from pathlib import Path
from urllib.request import Request, urlopen

RAIZ = Path(__file__).resolve().parents[1]
SAIDA = RAIZ / "docs" / "verificacao"
AMOSTRAS = RAIZ / "amostras"
CACHE = RAIZ / ".cache"
UA = {"User-Agent": "RotaLivre-FIAP/0.1 (projeto academico)"}

CKAN_SP156 = "https://dados.prefeitura.sp.gov.br/api/3/action/package_show?id=dados-do-sp156"
# O portal novo do Infosiga (2026) mudou o link. Descobrimos a URL atual pela API CKAN
# do Portal de Dados Abertos do Estado e mantemos as URLs conhecidas como alternativa.
CKAN_INFOSIGA = "https://dadosabertos.sp.gov.br/api/3/action/package_show?id=sinistros-infosiga"
INFOSIGA_CANDIDATAS = [
    "https://infosiga.detran.sp.gov.br/painel/download/file/dados_infosiga.zip",
    "https://infosiga.detran.sp.gov.br/rest/painel/download/file/dados_infosiga.zip",
]

# Palavras-chave (sem acento, minusculas) para achar servicos de barreira em calcada.
PALAVRAS = ["calcad", "passeio", "rampa", "acessib", "rebaix", "piso tatil",
            "desnivel", "obstru", "pedestre", "buraco"]

csv.field_size_limit(2**31 - 1)


def norm(s):
    s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode()
    return re.sub(r"\s+", " ", s).strip().lower()


def get(url, timeout=60):
    return urlopen(Request(url, headers=UA), timeout=timeout)


def baixar(url, destino, max_idade_h=12):
    destino.parent.mkdir(parents=True, exist_ok=True)
    if destino.exists() and time.time() - destino.stat().st_mtime < max_idade_h * 3600:
        print(f"  usando cache: {destino.name}")
        return destino
    with get(url, 600) as r, open(destino, "wb") as f:
        total = int(r.headers.get("Content-Length") or 0)
        lido = 0
        while True:
            bloco = r.read(1 << 20)
            if not bloco:
                break
            f.write(bloco)
            lido += len(bloco)
            print(f"\r  baixando {destino.name}: {lido/1e6:,.1f} MB"
                  + (f" de {total/1e6:,.1f} MB" if total else ""), end="", flush=True)
    print()
    return destino


def detectar(amostra: bytes):
    """Retorna (encoding, delimitador) a partir dos primeiros bytes."""
    try:
        amostra.decode("utf-8-sig")
        enc = "utf-8-sig"
    except UnicodeDecodeError:
        enc = "latin-1"
    texto = amostra.decode(enc, errors="ignore")
    linhas = "\n".join(texto.splitlines()[:20])
    try:
        delim = csv.Sniffer().sniff(linhas, delimiters=";,|\t").delimiter
    except csv.Error:
        delim = ";"
    return enc, delim


def achar(colunas, *trechos):
    """Primeira coluna cujo nome normalizado contem todos os trechos."""
    for c in colunas:
        n = norm(c)
        if all(t in n for t in trechos):
            return c
    return None


# ----------------------------------------------------------------------------- SP156
def verificar_sp156(rel):
    print("\n==================== SP156 (Prefeitura de SP) ====================")
    info = {"fonte": "SP156", "endpoint_ckan": CKAN_SP156}
    rel["sp156"] = info
    try:
        pacote = json.load(get(CKAN_SP156))["result"]
    except Exception as e:
        info.update(ok=False, erro=repr(e))
        print("  FALHOU ao consultar a API CKAN:", e)
        return

    recursos = []
    for r in pacote.get("resources", []):
        nome = r.get("name") or ""
        m = re.search(r"(\d)\s*[ºo°]?\s*TRI\w*\s*(\d{4})", nome, re.I)
        recursos.append({
            "nome": nome, "formato": (r.get("format") or "").strip().strip(".").upper(), "url": r.get("url"),
            "atualizado": r.get("last_modified") or r.get("created"),
            "datastore_active": r.get("datastore_active"),
            "trimestre": f"{m.group(2)}-T{m.group(1)}" if m else None,
        })
    info["recursos"] = recursos
    info["licenca"] = pacote.get("license_title")
    info["metadata_modified"] = pacote.get("metadata_modified")
    print(f"  recursos encontrados: {len(recursos)}  | licenca: {info['licenca']}")
    for r in recursos:
        print(f"   - [{r['formato'] or '?':5}] {r['trimestre'] or '-':8} {r['nome']}")

    trimestrais = sorted([r for r in recursos if r["trimestre"] and r["formato"] in ("CSV", "")],
                         key=lambda r: r["trimestre"], reverse=True)
    info["ultimos_4_trimestres"] = [r["trimestre"] for r in trimestrais[:4]]
    print("  4 trimestres mais recentes (CSV):", info["ultimos_4_trimestres"])
    if not trimestrais:
        info["ok"] = False
        info["erro"] = "nenhum CSV trimestral identificado pelo nome"
        return

    alvo = trimestrais[0]
    print(f"  analisando o mais recente: {alvo['nome']}\n  url: {alvo['url']}")
    try:
        arq = baixar(alvo["url"], CACHE / f"sp156_{alvo['trimestre']}.csv")
    except Exception as e:
        info.update(ok=False, erro=f"download falhou: {e!r}")
        print("  FALHOU no download:", e)
        return

    with open(arq, "rb") as f:
        enc, delim = detectar(f.read(65536))
    info.update(arquivo_analisado=alvo["nome"], encoding=enc, delimitador=delim)

    with open(arq, encoding=enc, newline="", errors="replace") as f:
        leitor = csv.DictReader(f, delimiter=delim)
        cols = leitor.fieldnames or []
        info["colunas"] = cols
        print(f"  encoding={enc} delimitador='{delim}'\n  colunas: {cols}")
        c_tema, c_assunto, c_serv = achar(cols, "tema"), achar(cols, "assunto"), achar(cols, "servico")
        c_lat, c_lon = achar(cols, "latitude"), achar(cols, "longitude")
        c_status, c_abert = achar(cols, "status"), achar(cols, "abertura")
        c_parecer = achar(cols, "parecer")

        total, casados = 0, 0
        servicos, status = Counter(), Counter()
        sem_coord, datas = 0, []
        amostra = []
        for linha in leitor:
            total += 1
            texto = norm(" ".join(str(linha.get(c) or "") for c in (c_tema, c_assunto, c_serv) if c))
            if not any(p in texto for p in PALAVRAS):
                continue
            casados += 1
            servicos[(linha.get(c_tema), linha.get(c_assunto), linha.get(c_serv))] += 1
            status[linha.get(c_status)] += 1
            if not (linha.get(c_lat) or "").strip() or not (linha.get(c_lon) or "").strip():
                sem_coord += 1
            if c_abert and len(datas) < 5:
                datas.append({"abertura": linha.get(c_abert), "parecer": linha.get(c_parecer),
                              "lat": linha.get(c_lat), "lon": linha.get(c_lon)})
            if len(amostra) < 50:
                amostra.append(linha)

    info.update(ok=True, linhas_total_trimestre=total, linhas_palavras_chave=casados,
                sem_coordenada=sem_coord, status=dict(status.most_common()),
                exemplos_formato=datas)
    print(f"  linhas no trimestre: {total:,} | ligadas a calcada (palavras-chave): {casados:,}"
          f" | sem coordenada: {sem_coord:,}")
    print("  status:", dict(status.most_common()))
    print("  exemplos de formato:", datas[:3])

    SAIDA.mkdir(parents=True, exist_ok=True)
    with open(SAIDA / "sp156_servicos_candidatos.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["tema", "assunto", "servico", "qtd"])
        for (t, a, s), q in servicos.most_common():
            w.writerow([t, a, s, q])
    print("  servicos candidatos (top 25):")
    for (t, a, s), q in servicos.most_common(25):
        print(f"   {q:>7,}  {t} > {a} > {s}")
    AMOSTRAS.mkdir(exist_ok=True)
    if amostra:
        with open(AMOSTRAS / "sp156_amostra.csv", "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols, delimiter=";")
            w.writeheader()
            w.writerows(amostra)


# ----------------------------------------------------------------------------- INFOSIGA
def urls_infosiga(info):
    """URL do ZIP de dados pela API CKAN estadual + candidatas conhecidas (sem repetir)."""
    urls = []
    try:
        pacote = json.load(get(CKAN_INFOSIGA))["result"]
        info["recursos_ckan"] = [{"nome": r.get("name"), "formato": r.get("format"), "url": r.get("url"),
                                  "atualizado": r.get("last_modified") or r.get("created")}
                                 for r in pacote.get("resources", [])]
        info["licenca"] = pacote.get("license_title")
        info["metadata_modified"] = pacote.get("metadata_modified")
        for r in info["recursos_ckan"]:
            print(f"   - CKAN: [{r['formato']}] {r['nome']} -> {r['url']}")
            if "dados_infosiga" in (r["url"] or "") or "dicion" not in norm(r["nome"]):
                if (r["formato"] or "").upper() == "ZIP" and r["url"]:
                    urls.append(r["url"])
    except Exception as e:
        info["erro_ckan"] = repr(e)
        print("  aviso: API CKAN estadual indisponivel:", e)
    return list(dict.fromkeys(urls + INFOSIGA_CANDIDATAS))


def verificar_infosiga(rel):
    print("\n==================== Infosiga (Detran-SP) ====================")
    info = {"fonte": "Infosiga", "endpoint_ckan": CKAN_INFOSIGA, "tentativas": []}
    rel["infosiga"] = info
    arq = None
    for url in urls_infosiga(info):
        destino = CACHE / "dados_infosiga.zip"
        if destino.exists() and not zipfile.is_zipfile(destino):
            destino.unlink()  # descarta cache invalido (ex.: HTML)
        try:
            print(f"  tentando: {url}")
            baixar(url, destino)
        except Exception as e:
            info["tentativas"].append({"url": url, "erro": repr(e)})
            print("   falhou:", e)
            continue
        if zipfile.is_zipfile(destino):
            info["url"] = url
            info["tentativas"].append({"url": url, "ok": True})
            arq = destino
            break
        inicio = destino.read_bytes()[:80]
        info["tentativas"].append({"url": url, "erro": f"nao e ZIP, inicio={inicio!r}"})
        print(f"   resposta nao e ZIP (inicio: {inicio[:40]!r})")
        destino.unlink()
    if arq is None:
        info.update(ok=False, erro="nenhuma URL retornou um ZIP valido")
        print("  FALHOU: nenhuma URL retornou um ZIP valido")
        return
    info["tamanho_mb"] = round(arq.stat().st_size / 1e6, 1)

    colunas = {}
    with zipfile.ZipFile(arq) as z:
        membros = [(m.filename, round(m.file_size / 1e6, 1)) for m in z.infolist()]
        info["arquivos_no_zip"] = membros
        print("  arquivos no ZIP:")
        for n, mb in membros:
            print(f"   - {n} ({mb} MB)")

        for nome, _ in membros:
            if not nome.lower().endswith(".csv"):
                continue
            with z.open(nome) as f:
                enc, delim = detectar(f.read(65536))
            with z.open(nome) as fb:
                leitor = csv.DictReader(io.TextIOWrapper(fb, encoding=enc, errors="replace", newline=""),
                                        delimiter=delim)
                cols = leitor.fieldnames or []
                colunas[nome] = {"encoding": enc, "delimitador": delim, "colunas": cols}
                print(f"\n  {nome}: encoding={enc} delim='{delim}'\n  colunas: {cols}")
                if "sinistro" not in norm(nome):
                    continue
                # Analise detalhada so dos arquivos de sinistros (eventos)
                c_mun = achar(cols, "municipio")
                c_lat, c_lon = achar(cols, "latitude"), achar(cols, "longitude")
                c_data = achar(cols, "data", "sinistro") or achar(cols, "data")
                cols_ped = [c for c in cols if "atropel" in norm(c) or "pedestre" in norm(c)]
                cols_tipo = [c for c in cols if norm(c).startswith("tipo") or norm(c).startswith("tp_")]
                total, capital, sem_coord = 0, 0, 0
                ped = {c: Counter() for c in cols_ped}
                tipos = {c: Counter() for c in cols_tipo[:15]}
                exemplos, amostra = [], []
                for linha in leitor:
                    total += 1
                    if c_mun and norm(linha.get(c_mun)) != "sao paulo":
                        continue
                    capital += 1
                    if not (linha.get(c_lat) or "").strip():
                        sem_coord += 1
                    for c in ped:
                        ped[c][linha.get(c)] += 1
                    for c in tipos:
                        tipos[c][linha.get(c)] += 1
                    if len(exemplos) < 3:
                        exemplos.append({"data": linha.get(c_data), "lat": linha.get(c_lat),
                                         "lon": linha.get(c_lon)})
                    if len(amostra) < 50 and any((linha.get(c) or "").strip() not in ("", "0", "0,0", "NAO", "Não")
                                                 for c in cols_ped):
                        amostra.append(linha)
                resumo = {
                    "linhas_total": total, "linhas_capital": capital, "capital_sem_coordenada": sem_coord,
                    "coluna_municipio": c_mun, "coluna_data": c_data,
                    "colunas_pedestre": {c: dict(v.most_common(8)) for c, v in ped.items()},
                    "colunas_tipo": {c: dict(v.most_common(8)) for c, v in tipos.items()},
                    "exemplos_formato": exemplos,
                }
                colunas[nome]["analise"] = resumo
                print(f"  linhas: {total:,} | municipio SAO PAULO: {capital:,} | sem coordenada: {sem_coord:,}")
                print("  colunas ligadas a pedestre/atropelamento:", resumo["colunas_pedestre"])
                print("  exemplos de formato:", exemplos)
                if amostra and not (AMOSTRAS / "infosiga_sinistros_amostra.csv").exists():
                    AMOSTRAS.mkdir(exist_ok=True)
                    with open(AMOSTRAS / "infosiga_sinistros_amostra.csv", "w", encoding="utf-8", newline="") as f:
                        w = csv.DictWriter(f, fieldnames=cols, delimiter=";")
                        w.writeheader()
                        w.writerows(amostra)

    info["ok"] = True
    SAIDA.mkdir(parents=True, exist_ok=True)
    (SAIDA / "infosiga_colunas.json").write_text(json.dumps(colunas, ensure_ascii=False, indent=2),
                                                 encoding="utf-8")


def main():
    rel = {"executado_em": dt.datetime.now().isoformat(timespec="seconds"),
           "python": sys.version.split()[0]}
    verificar_sp156(rel)
    verificar_infosiga(rel)
    SAIDA.mkdir(parents=True, exist_ok=True)
    (SAIDA / "relatorio_fontes.json").write_text(json.dumps(rel, ensure_ascii=False, indent=2),
                                                 encoding="utf-8")
    print("\n==================== RESUMO ====================")
    for k in ("sp156", "infosiga"):
        print(f"  {k}: {'OK' if rel.get(k, {}).get('ok') else 'FALHOU - ' + str(rel.get(k, {}).get('erro'))}")
    print(f"  relatorio: {SAIDA / 'relatorio_fontes.json'}")


if __name__ == "__main__":
    main()
