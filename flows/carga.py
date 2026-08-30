from pathlib import Path

import duckdb
from prefect import get_run_logger, task

tabelas = [
    "empresas",
    "estabelecimentos",
    "socios",
    "simples",
    "cnaes",
    "naturezas",
    "receitaws",
]


@task(retries=2, retry_delay_seconds=10)
def carregar_duckdb():
    banco = Path("data/franq.duckdb")
    banco.parent.mkdir(parents=True, exist_ok=True)
    contagens = {}
    with duckdb.connect(str(banco)) as conexao:
        conexao.execute("create schema if not exists raw")
        for tabela in tabelas:
            arquivo = Path("data/amostra") / f"{tabela}.csv"
            if not arquivo.exists():
                continue
            conexao.execute(
                f"""
                create or replace table raw.{tabela} as
                select *, current_timestamp as carregado_em
                from read_csv('{arquivo.as_posix()}', header = true, all_varchar = true)
                """
            )
            contagens[tabela] = conexao.sql(f"select count(*) from raw.{tabela}").fetchone()[0]
    get_run_logger().info(f"carga concluida: {contagens}")
    return contagens
