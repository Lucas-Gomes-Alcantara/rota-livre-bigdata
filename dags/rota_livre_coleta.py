"""
### Rota Livre - coleta diaria (SP156 + Infosiga)

Fluxo:
`preparar_pastas` -> **[`coletar_sp156`, `coletar_infosiga`] em paralelo** -> `verificar_chegada` (sensor)
-> `registrar_manifesto`

Saidas (uma por coleta, com a data no nome), em `chegada/`:
- `sp156_calcadas_AAAA-MM-DD.csv` - solicitacoes SP156 ligadas a calcada (4 trimestres mais recentes)
- `infosiga_atropelamentos_sp_AAAA-MM-DD.csv` - atropelamentos de pedestres na capital

Toda configuracao (URLs, filtros, chave opcional da API) vem de **Variables** `rota_livre_*`.
Nenhuma URL ou credencial fica no codigo.
"""
from __future__ import annotations

import csv
import hashlib
import json
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import PokeReturnValue, Variable, dag, get_current_context, task

TZ = "America/Sao_Paulo"


def _cfg_comum() -> dict:
    return {
        "chegada_dir": Variable.get("rota_livre_chegada_dir"),
        "cache_dir": Variable.get("rota_livre_cache_dir"),
        "user_agent": Variable.get("rota_livre_user_agent"),
    }


def _data_coleta() -> str:
    """Data real da coleta (fuso de SP). Em execucoes manuais do Airflow 3 o
    logical_date pode ser nulo, por isso usamos run_after."""
    ctx = get_current_context()
    momento = ctx.get("run_after") or pendulum.now(TZ)
    return pendulum.instance(momento).in_timezone(TZ).to_date_string()


@dag(
    dag_id="rota_livre_coleta",
    description="Coleta SP156 (calcadas) e Infosiga (atropelamentos) para a pasta chegada/",
    schedule="0 7 * * *",  # todo dia as 07:00 (America/Sao_Paulo)
    start_date=pendulum.datetime(2026, 11, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "rota-livre",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=45),
    },
    tags=["rota-livre", "coleta", "bigdata"],
    doc_md=__doc__,
)
def rota_livre_coleta():
    @task
    def preparar_pastas() -> str:
        cfg = _cfg_comum()
        for p in (cfg["chegada_dir"], cfg["cache_dir"], f"{cfg['chegada_dir']}/_manifesto"):
            Path(p).mkdir(parents=True, exist_ok=True)
        return _data_coleta()

    @task
    def coletar_sp156(data_coleta: str) -> dict:
        from coleta import sp156

        cfg = {
            **_cfg_comum(),
            "ckan_url": Variable.get("rota_livre_sp156_ckan_url"),
            "trimestres": int(Variable.get("rota_livre_sp156_trimestres", default="4")),
            "palavras_chave": Variable.get("rota_livre_sp156_palavras_chave", deserialize_json=True),
            "api_key": Variable.get("rota_livre_ckan_api_key", default="") or None,
        }
        res = sp156.coletar(cfg, data_coleta)
        print(json.dumps(res, ensure_ascii=False, indent=2))
        return res

    @task
    def coletar_infosiga(data_coleta: str) -> dict:
        from coleta import infosiga

        cfg = {
            **_cfg_comum(),
            "ckan_url": Variable.get("rota_livre_infosiga_ckan_url"),
            "urls_alternativas": Variable.get("rota_livre_infosiga_urls_alternativas", deserialize_json=True),
            "ano_minimo": int(Variable.get("rota_livre_infosiga_ano_minimo", default="2022")),
        }
        res = infosiga.coletar(cfg, data_coleta)
        print(json.dumps(res, ensure_ascii=False, indent=2))
        return res

    @task.sensor(poke_interval=20, timeout=600, mode="reschedule")
    def verificar_chegada(res_sp156: dict, res_infosiga: dict) -> PokeReturnValue:
        """Confere se os dois arquivos do dia chegaram, estao completos (sem .part)
        e tem exatamente o numero de linhas informado pela coleta."""
        checagem, tudo_ok = {}, True
        for res in (res_sp156, res_infosiga):
            p = Path(res["arquivo"])
            item = {"arquivo": p.name, "existe": p.exists(),
                    "parcial_pendente": p.with_name(p.name + ".part").exists()}
            if item["existe"]:
                with open(p, encoding="utf-8", newline="") as f:
                    linhas = sum(1 for _ in csv.reader(f, delimiter=";")) - 1
                h = hashlib.sha256(p.read_bytes()).hexdigest()
                item.update(bytes=p.stat().st_size, linhas=linhas, esperado=res["linhas"], sha256=h)
                item["ok"] = linhas == res["linhas"] and linhas > 0 and not item["parcial_pendente"]
            else:
                item["ok"] = False
            tudo_ok &= item["ok"]
            checagem[res["fonte"]] = item
        print(json.dumps(checagem, ensure_ascii=False, indent=2))
        return PokeReturnValue(is_done=tudo_ok, xcom_value=checagem)

    @task
    def registrar_manifesto(checagem: dict, res_sp156: dict, res_infosiga: dict) -> str:
        """Acrescenta uma linha por arquivo em chegada/_manifesto/coletas.csv
        (evidencia de 'registros coletados por fonte')."""
        ctx = get_current_context()
        destino = Path(_cfg_comum()["chegada_dir"]) / "_manifesto" / "coletas.csv"
        novo = not destino.exists()
        with open(destino, "a", encoding="utf-8", newline="") as f:
            w = csv.writer(f, delimiter=";")
            if novo:
                w.writerow(["registrado_em", "run_id", "fonte", "arquivo", "linhas", "bytes", "sha256", "detalhes"])
            for fonte, res in (("SP156", res_sp156), ("INFOSIGA", res_infosiga)):
                c = checagem[fonte]
                detalhes = {k: v for k, v in res.items() if k not in ("arquivo", "fonte", "linhas")}
                w.writerow([pendulum.now(TZ).to_iso8601_string(), ctx["run_id"], fonte, c["arquivo"],
                            c["linhas"], c["bytes"], c["sha256"], json.dumps(detalhes, ensure_ascii=False)])
        return str(destino)

    data = preparar_pastas()
    sp = coletar_sp156(data)
    inf = coletar_infosiga(data)
    registrar_manifesto(verificar_chegada(sp, inf), sp, inf)


rota_livre_coleta()
