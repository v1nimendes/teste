# Pipeline CNPJ — dbt + Prefect + DuckDB

Data Lakehouse sobre os dados públicos do CNPJ da Receita Federal, com transformação
em dbt Core, orquestração em Prefect e execução local em DuckDB. O modelo foi desenhado
para ser portado ao BigQuery sem reescrita — a proposta de arquitetura cloud e a estratégia
de custo estão em [docs/finops_bigquery.md](docs/finops_bigquery.md).

## Arquitetura

```
files/  ──►  Prefect  ──►  raw  ──►  staging  ──►  intermediate  ──►  marts
(CSV RF)     extração      DuckDB     views e        agregações       dimensões
             + carga                  incremental                     e fato
                                          │
                                          └──►  snapshots (SCD Type 2)
```

| Camada | Materialização | Papel |
|---|---|---|
| `raw` | tabela | Cópia fiel do arquivo, tudo em texto, com carimbo de carga |
| `staging` | view / incremental | Tipagem, padronização, tradução de domínios, deduplicação |
| `intermediate` | ephemeral | Agregações reaproveitáveis, sem materializar nada |
| `marts` | tabela / incremental | Dimensões e fato consumidos pelo negócio |
| `snapshots` | tabela | Histórico SCD Type 2 do capital social |

## Fontes

| Fonte | Grão | Arquivo |
|---|---|---|
| Empresas | CNPJ básico | `*.EMPRECSV` |
| Estabelecimentos | CNPJ completo | `*.ESTABELE` |
| Sócios | Sócio por empresa | `*.SOCIOCSV` |
| Simples Nacional | CNPJ básico | `*SIMPLES*` |
| CNAE | Subclasse | `*.CNAECSV` |
| Natureza jurídica | Código | `*.NATJUCSV` |
| ReceitaWS | CNPJ completo | API pública, opcional |

## Como executar

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Descompacte os arquivos da Receita Federal em qualquer subpasta de `files/` — a busca é
recursiva e localiza por extensão, então a organização das pastas não importa.

```bash
python -m flows.pipeline
```

O flow aceita parâmetros:

```bash
python -c "from flows.pipeline import pipeline_cnpj; pipeline_cnpj(linhas=10000, cnpjs_receitaws=5)"
```

| Parâmetro | Padrão | Efeito |
|---|---|---|
| `linhas` | `10000` | Tamanho da amostra de estabelecimentos |
| `cnpjs_receitaws` | `0` | Quantidade de CNPJs enriquecidos pela API; `0` desliga a chamada externa |
| `full_refresh` | `False` | Reconstrói os modelos incrementais do zero |

Para rodar apenas o dbt, a partir de `dbt_franq/`:

```bash
dbt deps && dbt snapshot && dbt run && dbt test
```

## Amostragem coerente

Pegar as dez mil primeiras linhas de cada arquivo isoladamente produziria joins vazios:
`Empresas` começa no CNPJ `00000000` e `Estabelecimentos` não segue a mesma ordem. O fato
ficaria sem linhas e os testes de integridade referencial falhariam por artefato da amostra,
não por defeito do dado.

A extração resolve isso em duas etapas: lê as primeiras `linhas` de `Estabelecimentos`,
extrai daí o conjunto de CNPJs básicos e varre `Empresas`, `Sócios` e `Simples` em streaming
filtrando por esse conjunto. As tabelas de domínio entram inteiras, por serem pequenas.
Uma etapa final de conciliacao descarta estabelecimentos, socios e adesoes ao Simples
cuja empresa nao esteja presente na amostra. Isso mantem a integridade referencial mesmo
quando nem todos os arquivos particionados da Receita estao disponiveis localmente. O
resultado e uma amostra reduzida com integridade referencial real.

## Decisões de modelagem

- **Incremental onde pesa.** `stg_estabelecimentos` e `fct_empresas_ativas` usam
  `incremental` com `delete+insert` sobre a chave natural, filtrando por `carregado_em`.
  As demais staging são views: reprocessá-las é barato e mantém o lineage simples.
- **Deduplicação na entrada.** Toda staging aplica `qualify row_number()` sobre a chave
  natural, ordenando pela carga mais recente. Duplicatas do arquivo de origem nunca chegam
  às camadas seguintes.
- **Nulo é nulo.** Códigos vazios viram `NULL` em vez de string em branco, o que evita
  falso positivo em `accepted_values` e mantém os `relationships` corretos.
- **Enriquecimento não derruba o pipeline.** A ReceitaWS vem desligada por padrão e, quando
  ativada, respeita o limite de três requisições por minuto. Qualquer falha de rede é
  registrada e o flow segue: o modelo `stg_receitaws` simplesmente fica vazio.

## Testes

São 38 testes cobrindo as quatro categorias exigidas:

| Categoria | Onde |
|---|---|
| Unicidade e não-nulos | Chaves de todas as staging, dimensões e do fato |
| Integridade referencial | `estabelecimentos → empresas`, `sócios → empresas`, `fato → dim_cnae`, `fato → dim_natureza_juridica` |
| Valores aceitos | Porte, matriz/filial, situação cadastral, tipo de sócio, UF |
| Pacote externo | `dbt_utils.unique_combination_of_columns`, `expression_is_true`, `accepted_range` |
| Regra de negócio | `tests/empresa_societaria_sem_socio.sql` e a identificação do sócio em `stg_socios` |

O teste de regra de negócio cruza o fato com a dimensão de natureza jurídica e acusa
empresas de natureza societária — excluídos Empresário Individual e EIRELI — que estejam
ativas com o quadro societário vazio. Ele usa `warn_if: '>0'` e `error_if: '>100'`: dado
público tem inconsistência residual, e quebrar o pipeline por causa dela seria ruído. A
tolerância é explícita e versionada, então qualquer aumento relevante vira erro de verdade.

Na amostra atual ele acusa 26 empresas em 1.254 — com todos os arquivos da Receita presentes,
não é artefato de amostragem, é inconsistência da própria fonte.

Pelo mesmo motivo, `stg_socios` não testa `nome_socio` nem `documento_socio` como não-nulos
isoladamente: a base tem um sócio sem nome e três sem documento. O que a fonte de fato garante
é que todo sócio é identificável por pelo menos um dos dois, e é isso que o teste valida.

## Snapshot

`snp_capital_social` guarda o histórico de capital social e porte por empresa, com estratégia
`check`. Ele lê direto da camada `raw`, não de um modelo, para que a ordem `snapshot` antes de
`run` funcione mesmo em ambiente novo.

Para ver o SCD Type 2 gerando uma segunda versão, rode o pipeline, altere um capital social na
origem e rode o snapshot de novo:

```sql
update raw.empresas
set capital_social = '999999,00'
where cnpj_basico = (select min(cnpj_basico) from raw.empresas);
```

```bash
dbt snapshot
```

A empresa passa a ter duas linhas em `snapshots.snp_capital_social`: a primeira com
`dbt_valid_to` preenchido, a segunda com `dbt_valid_to` nulo.

## Macros

| Macro | Problema que resolve |
|---|---|
| `parse_valor_br` | Capital social chega como `120000000000,00`; converte para decimal |
| `parse_data_rf` | Datas chegam como `20170210`, `0` ou vazio; converte para date sem quebrar |
| `limpa_texto` | Colapsa espaços, padroniza caixa e transforma vazio em nulo |
| `formata_cnpj` | Monta o CNPJ de quatorze dígitos a partir de básico, ordem e dígito |
| `traduz_dominio` | Gera o `CASE` de tradução a partir de um dicionário Jinja |
| `secao_cnae` | Deriva a seção CNAE a partir das faixas de divisão |
| `colunas_auditoria` | Carimba data e `invocation_id` do dbt em todo modelo |

## Estrutura

```
flows/          extração, carga, chamada da API e orquestração
dbt_franq/      projeto dbt: models, macros, snapshots e testes
docs/           proposta de arquitetura e FinOps para BigQuery
files/          arquivos brutos da Receita Federal (fora do versionamento)
data/           amostras e banco DuckDB (fora do versionamento)
```
