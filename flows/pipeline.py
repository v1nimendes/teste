from prefect import flow

from flows import carga, dbt, extracao, receitaws


@flow(name="pipeline-cnpj", log_prints=True)
def pipeline_cnpj(linhas=10000, cnpjs_receitaws=0, full_refresh=False):
    cnpjs = extracao.extrair_estabelecimentos(linhas)

    tarefas = [
        extracao.extrair_por_cnpj.submit(
            "empresas", "*.EMPRECSV", extracao.campos_empresas, cnpjs
        ),
        extracao.extrair_por_cnpj.submit(
            "socios", "*.SOCIOCSV", extracao.campos_socios, cnpjs
        ),
        extracao.extrair_por_cnpj.submit(
            "simples", "*SIMPLES*", extracao.campos_simples, cnpjs
        ),
        extracao.extrair_dominio.submit("cnaes", "*.CNAECSV", extracao.campos_cnaes),
        extracao.extrair_dominio.submit("naturezas", "*.NATJUCSV", extracao.campos_naturezas),
        receitaws.enriquecer.submit(cnpjs, cnpjs_receitaws),
    ]
    for tarefa in tarefas:
        tarefa.result()

    extracao.conciliar_amostra(["estabelecimentos", "socios", "simples"])
    carga.carregar_duckdb()

    dbt.executar("deps")
    dbt.executar("snapshot")
    dbt.executar("run", full_refresh)
    dbt.executar("test")


if __name__ == "__main__":
    pipeline_cnpj()
