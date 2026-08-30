import time

import requests
from prefect import get_run_logger, task

from flows.extracao import escrever_csv

campos_receitaws = [
    "cnpj_basico",
    "situacao",
    "porte",
    "abertura",
    "capital_social",
    "atividade_principal",
]

intervalo_entre_consultas = 21


def consultar(cnpj_basico):
    resposta = requests.get(
        f"https://receitaws.com.br/v1/cnpj/{cnpj_basico}000100", timeout=30
    )
    resposta.raise_for_status()
    dados = resposta.json()
    if dados.get("status") == "ERROR":
        return None
    atividades = dados.get("atividade_principal") or [{}]
    return [
        cnpj_basico,
        dados.get("situacao"),
        dados.get("porte"),
        dados.get("abertura"),
        dados.get("capital_social"),
        atividades[0].get("code"),
    ]


@task(retries=2, retry_delay_seconds=60)
def enriquecer(cnpjs, quantidade):
    registrador = get_run_logger()
    linhas = []
    for posicao, cnpj_basico in enumerate(cnpjs[:quantidade]):
        if posicao:
            time.sleep(intervalo_entre_consultas)
        try:
            linha = consultar(cnpj_basico)
        except Exception as erro:
            registrador.warning(f"receitaws indisponivel para {cnpj_basico}: {erro}")
            continue
        if linha:
            linhas.append(linha)
    escrever_csv("receitaws", campos_receitaws, linhas)
    registrador.info(f"receitaws: {len(linhas)} linhas")
    return len(linhas)
