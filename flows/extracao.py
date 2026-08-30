import csv
from pathlib import Path

from prefect import get_run_logger, task

campos_empresas = [
    "cnpj_basico",
    "razao_social",
    "natureza_juridica",
    "qualificacao_responsavel",
    "capital_social",
    "porte",
    "ente_federativo_responsavel",
]

campos_estabelecimentos = [
    "cnpj_basico",
    "cnpj_ordem",
    "cnpj_dv",
    "identificador_matriz_filial",
    "nome_fantasia",
    "situacao_cadastral",
    "data_situacao_cadastral",
    "motivo_situacao_cadastral",
    "nome_cidade_exterior",
    "pais",
    "data_inicio_atividade",
    "cnae_fiscal_principal",
    "cnae_fiscal_secundaria",
    "tipo_logradouro",
    "logradouro",
    "numero",
    "complemento",
    "bairro",
    "cep",
    "uf",
    "municipio",
    "ddd_1",
    "telefone_1",
    "ddd_2",
    "telefone_2",
    "ddd_fax",
    "fax",
    "correio_eletronico",
    "situacao_especial",
    "data_situacao_especial",
]

campos_socios = [
    "cnpj_basico",
    "identificador_socio",
    "nome_socio",
    "cnpj_cpf_socio",
    "qualificacao_socio",
    "data_entrada_sociedade",
    "pais",
    "representante_legal",
    "nome_representante",
    "qualificacao_representante",
    "faixa_etaria",
]

campos_simples = [
    "cnpj_basico",
    "opcao_simples",
    "data_opcao_simples",
    "data_exclusao_simples",
    "opcao_mei",
    "data_opcao_mei",
    "data_exclusao_mei",
]

campos_cnaes = ["codigo", "descricao"]

campos_naturezas = ["codigo", "descricao"]


def localizar(padrao):
    return sorted(caminho for caminho in Path("files").rglob(padrao) if caminho.is_file())


def ler_linhas(caminho):
    with open(caminho, encoding="latin-1", newline="") as entrada:
        yield from csv.reader(entrada, delimiter=";", quotechar='"')


def ler_amostra(nome):
    with open(Path("data/amostra") / f"{nome}.csv", encoding="utf-8", newline="") as entrada:
        leitor = csv.reader(entrada)
        return next(leitor), list(leitor)


def escrever_csv(nome, campos, linhas):
    destino = Path("data/amostra")
    destino.mkdir(parents=True, exist_ok=True)
    with open(destino / f"{nome}.csv", "w", encoding="utf-8", newline="") as saida:
        escritor = csv.writer(saida)
        escritor.writerow(campos)
        escritor.writerows(linhas)


@task(retries=2, retry_delay_seconds=10)
def extrair_estabelecimentos(limite):
    linhas = []
    for linha in ler_linhas(localizar("*.ESTABELE")[0]):
        if len(linhas) >= limite:
            break
        linhas.append(linha)
    escrever_csv("estabelecimentos", campos_estabelecimentos, linhas)
    get_run_logger().info(f"estabelecimentos: {len(linhas)} linhas")
    return sorted({linha[0] for linha in linhas})


@task(retries=2, retry_delay_seconds=10)
def extrair_por_cnpj(nome, padrao, campos, cnpjs):
    chaves = set(cnpjs)
    linhas = []
    for caminho in localizar(padrao):
        for linha in ler_linhas(caminho):
            if linha[0] in chaves:
                linhas.append(linha)
    escrever_csv(nome, campos, linhas)
    get_run_logger().info(f"{nome}: {len(linhas)} linhas")
    return len(linhas)


@task(retries=2, retry_delay_seconds=10)
def extrair_dominio(nome, padrao, campos):
    linhas = list(ler_linhas(localizar(padrao)[0]))
    escrever_csv(nome, campos, linhas)
    get_run_logger().info(f"{nome}: {len(linhas)} linhas")
    return len(linhas)


@task(retries=2, retry_delay_seconds=10)
def conciliar_amostra(nomes):
    chaves = {linha[0] for linha in ler_amostra("empresas")[1]}
    descartadas = {}
    for nome in nomes:
        campos, originais = ler_amostra(nome)
        mantidas = [linha for linha in originais if linha[0] in chaves]
        descartadas[nome] = len(originais) - len(mantidas)
        escrever_csv(nome, campos, mantidas)
    get_run_logger().info(f"conciliacao: {descartadas}")
    return descartadas
