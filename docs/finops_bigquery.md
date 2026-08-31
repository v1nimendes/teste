# Proposta de arquitetura e FinOps — Pipeline CNPJ no BigQuery

Documento de onboarding do pipeline de dados públicos do CNPJ. Descreve como o modelo
desenvolvido localmente em DuckDB é promovido a um ambiente Google Cloud, quais decisões
de particionamento e clusterização foram tomadas, e como essas decisões se traduzem em
redução de bytes lidos e de custo.

Repositório do projeto: <https://github.com/v1nimendes/teste>

---

## 1. Para quem é este documento

Você acabou de entrar no time e vai mexer neste pipeline. As seções 2 e 3 explicam o
desenho; a 4 e a 5 explicam as escolhas de custo, que são as que mais doem quando ignoradas;
a 8 é o roteiro do seu primeiro dia.

Três ideias sustentam o resto do texto:

1. **No BigQuery você paga pelo que lê, não pelo que guarda.** Armazenamento é barato,
   varredura não. Toda decisão de modelagem aqui existe para reduzir bytes lidos.
2. **Particionamento elimina blocos inteiros; clusterização ordena o que sobrou.** São
   complementares, não alternativas.
3. **O que não é filtrável em produção não deveria ser materializado como tabela larga.**

---

## 2. Arquitetura no Google Cloud

```
Receita Federal          Cloud Storage           BigQuery                    Consumo
   (ZIP/CSV)      ──►    gs://cnpj-raw/    ──►   raw ──► staging ──► marts ──►   BI
                          zona de pouso        (external)   (view)   (partic.)   ML
                                                                                 API

           Prefect em Cloud Run Job orquestra todas as setas
```

**Ingestão.** Um job do Prefect em Cloud Run baixa os arquivos mensais da Receita, converte
para Parquet e grava em `gs://cnpj-raw/{entidade}/dt={data_carga}/`. Parquet em vez de CSV
porque é colunar e comprimido: o mesmo dado que ocupa 1,2 GB em CSV cai para cerca de 200 MB,
e o BigQuery consegue ler apenas as colunas necessárias.

**Camada raw.** Tabelas externas apontando para o GCS, com particionamento Hive por `dt`.
Nada é copiado para dentro do BigQuery nesta camada: o armazenamento fica no GCS, que é mais
barato, e o histórico bruto permanece auditável.

**Camadas staging, intermediate e marts.** Idênticas ao projeto local. O mesmo código dbt
roda nos dois ambientes; muda apenas o `profiles.yml` e os `+partition_by`, que o adaptador
DuckDB ignora silenciosamente.

**Orquestração.** O flow do Prefect vira um deployment agendado em Cloud Run Job, com
Workload Identity para autenticar no BigQuery sem chave estática. Cada task mantém os
`retries` já definidos no projeto local.

**Separação por dataset.** `cnpj_raw`, `cnpj_staging`, `cnpj_marts` e `cnpj_snapshots`, cada
um com sua política de acesso. Analista lê `cnpj_marts`; apenas a service account do pipeline
escreve nas demais.

---

## 3. Volume real do dado

A amostra local tem dez mil estabelecimentos. Em produção a base completa tem outra ordem de
grandeza, e é ela que dita as decisões de custo:

| Tabela | Linhas | Tamanho estimado no BigQuery |
|---|---|---|
| `stg_estabelecimentos` | ~63 milhões | ~28 GB |
| `stg_empresas` | ~60 milhões | ~9 GB |
| `stg_socios` | ~24 milhões | ~4 GB |
| `stg_simples` | ~40 milhões | ~2 GB |
| `fct_empresas_ativas` | ~22 milhões | ~6 GB |
| `snp_capital_social` | crescente | ~1 GB por ano de histórico |

Uma consulta ingênua — `SELECT *` sem filtro — sobre `stg_estabelecimentos` lê 28 GB. No
modelo sob demanda, ao preço de referência de US$ 6,25 por TiB varrido, isso custa cerca de
**US$ 0,17 por execução**. Um dashboard que atualiza a cada quinze minutos, oito horas por
dia, roda 640 vezes ao mês: **US$ 109 mensais por um único painel**. É esse número que as
próximas duas seções atacam.

---

## 4. Particionamento

O particionamento quebra a tabela em blocos fisicamente separados. Quando a consulta filtra
pela coluna de partição, o BigQuery descarta os blocos irrelevantes antes de ler qualquer
byte. É o corte mais barato que existe, porque acontece no metadado.

| Tabela | Coluna de partição | Granularidade | Justificativa |
|---|---|---|---|
| `raw_*` | `dt` (data da carga) | Diária | Reprocessar uma competência sem tocar no histórico; expiração automática em 90 dias |
| `stg_estabelecimentos` | `data_situacao_cadastral` | Mensal | Toda análise de baixa, suspensão ou reativação recorta um período, e a coluna tem boa distribuição |
| `fct_empresas_ativas` | `data_inicio_atividade` | Mensal | É o eixo temporal do negócio: safras de abertura, maturidade de carteira, comparativo ano a ano |
| `snp_capital_social` | `dbt_valid_from` | Mensal | Consultas de histórico sempre delimitam a janela de vigência |

**Por que mensal e não diária.** Uma partição diária sobre `data_inicio_atividade` criaria
mais de 45 mil partições, acima do limite de 10 mil por tabela, e cada partição ficaria
pequena demais, o que degrada a leitura. Mensal cobre 120 anos de histórico em cerca de 1.400
partições, com blocos de tamanho saudável.

**Por que não particionar por `uf`.** São 27 valores com distribuição muito desigual: São
Paulo concentra perto de 30% da base. Particionar por coluna categórica desbalanceada gera
blocos assimétricos e ainda deixa o filtro temporal, que é o mais frequente, sem poda. A `uf`
rende muito mais como primeira coluna de clusterização.

**`require_partition_filter = true`** em todas as tabelas acima de 1 GB. A consulta que
esquecer o filtro de partição falha com erro explícito, em vez de varrer a tabela inteira e
virar fatura. É a proteção mais eficaz contra custo acidental e custa uma linha de
configuração.

---

## 5. Clusterização

Dentro de cada partição, a clusterização ordena fisicamente as linhas pelas colunas
escolhidas e mantém estatísticas por bloco. Quando a consulta filtra por uma coluna de
cluster, o BigQuery pula os blocos cujo intervalo não contém o valor procurado.

| Tabela | Colunas de cluster, nesta ordem |
|---|---|
| `stg_estabelecimentos` | `uf`, `cod_situacao_cadastral`, `cod_cnae_principal` |
| `fct_empresas_ativas` | `uf`, `cod_cnae_principal`, `cod_porte` |
| `stg_socios` | `cnpj_basico` |
| `snp_capital_social` | `cnpj_basico` |

**A ordem não é decorativa.** O BigQuery trata as colunas de cluster como uma chave composta,
da esquerda para a direita. Filtrar por `uf` sozinho aproveita a clusterização; filtrar por
`cod_porte` sozinho, não. Por isso a ordem segue a frequência de uso real: quase toda análise
começa recortando estado, depois atividade econômica, e só então porte.

**Limite de quatro colunas.** É uma restrição do BigQuery e é saudável, porque mais colunas
diluiriam a ordenação. Ficamos em três nas tabelas principais, deixando margem para uma
quarta quando um novo padrão de consulta se firmar.

**`cnpj_basico` nas tabelas de detalhe.** `stg_socios` e o snapshot são quase sempre
consultados por empresa específica, em busca pontual. Clusterizar pela chave transforma uma
varredura completa em leitura de poucos blocos.

---

## 6. Impacto estimado em bytes e custo

Cenário: *empresas ativas em São Paulo, no CNAE de restaurantes, abertas a partir de 2015*.
Tabela base `fct_empresas_ativas`, 6 GB, 22 milhões de linhas.

| Versão da consulta | Bytes lidos | Custo por execução | Redução |
|---|---|---|---|
| `SELECT *`, sem filtro aproveitável | 6,0 GB | US$ 0,037 | — |
| Apenas as 6 colunas necessárias | 900 MB | US$ 0,0055 | 85% |
| Mais a poda de partição por `data_inicio_atividade` | 380 MB | US$ 0,0023 | 94% |
| Mais a clusterização por `uf` e CNAE | 45 MB | US$ 0,00027 | **99,3%** |

O ganho vem em três camadas independentes e multiplicativas. A seleção de colunas é
consequência de o BigQuery ser colunar: nada a configurar, apenas não escrever `SELECT *`.
A poda de partição corta o eixo temporal. A clusterização corta dentro do que sobrou.

**Extrapolando para o dashboard da seção 3**, as mesmas 640 execuções mensais saem de US$ 109
para menos de **US$ 0,20**. Em uma operação com dezenas de painéis e centenas de consultas ad
hoc, a diferença anual é da ordem de dezenas de milhares de dólares.

**Sobre slots.** No modelo sob demanda o BigQuery aloca slots automaticamente e a cobrança é
por bytes; ler menos dados significa menos slots-segundo consumidos e resposta mais rápida.
Se a operação migrar para capacidade reservada, a economia muda de forma: em vez de reduzir a
fatura por consulta, libera slots da reserva para outras cargas e adia a compra de mais
capacidade. A regra de ouro é a mesma nos dois modelos.

---

## 7. Configuração no dbt e demais controles

As decisões acima moram no `dbt_project.yml`, não em DDL solta:

```yaml
models:
  franq:
    marts:
      +materialized: incremental
      +incremental_strategy: insert_overwrite
      +on_schema_change: append_new_columns
      fct_empresas_ativas:
        +partition_by:
          field: data_inicio_atividade
          data_type: date
          granularity: month
        +cluster_by: ["uf", "cod_cnae_principal", "cod_porte"]
        +require_partition_filter: true
```

**`insert_overwrite` em vez de `merge`.** A estratégia `merge` varre a tabela de destino
inteira para casar as chaves, e essa varredura é cobrada a cada execução. O `insert_overwrite`
reescreve apenas as partições afetadas pelo lote. Em uma tabela de 6 GB atualizada
diariamente, a diferença é ler 6 GB por execução contra ler algumas dezenas de megabytes.

Outros controles adotados:

- **Expiração de partição** de 90 dias nas tabelas `raw`. O histórico permanece no GCS, que
  custa uma fração do armazenamento do BigQuery.
- **Armazenamento físico** em vez de lógico nos datasets grandes: cobra pelo tamanho
  comprimido, tipicamente 40% a 60% menor no perfil deste dado.
- **Custom quota** de bytes por usuário e por projeto, como rede de proteção contra consulta
  acidental em produção.
- **Labels** `time`, `camada` e `ambiente` em todos os jobs, para que o relatório de custo
  seja atribuível a um dono.
- **Monitoramento** via `INFORMATION_SCHEMA.JOBS_BY_PROJECT`, com painel semanal das dez
  consultas mais caras. Otimização começa por medição.
- **CI barato** com `dbt build --select state:modified+` e deferral contra produção, para que
  o pull request construa apenas o que mudou.
- **Materialized views** sobre `fct_empresas_ativas` para as agregações por UF e CNAE que o BI
  consulta o tempo todo. O BigQuery as mantém atualizadas de forma incremental e cobra apenas
  a leitura da view, muito menor que a da tabela.

---

## 8. Primeiro dia no projeto

1. Clone o repositório e siga o `README.md` para rodar o pipeline local em DuckDB. Ele não
   depende de credencial nenhuma e termina em poucos minutos.
2. Abra `dbt_franq/models/staging/` e leia um modelo inteiro. O padrão se repete: CTE de
   origem, CTE de padronização com as macros, deduplicação por `qualify` e carimbo de
   auditoria.
3. Rode `dbt docs generate && dbt docs serve` para navegar no lineage.
4. Antes de escrever qualquer consulta em produção, confira nesta ordem: filtrou a partição,
   listou as colunas, aproveitou a primeira coluna de cluster.

**Glossário rápido**

| Termo | Significado neste projeto |
|---|---|
| CNPJ básico | Oito primeiros dígitos; identifica a empresa |
| CNPJ completo | Quatorze dígitos; identifica o estabelecimento |
| Matriz e filial | Um CNPJ básico tem uma matriz e zero ou mais filiais |
| Situação cadastral | O código `02` significa ativa; é o filtro que define o fato |
| CNAE | Código de atividade econômica, hierárquico da seção à subclasse |
| Natureza jurídica | Código de quatro dígitos; define se a empresa exige quadro societário |

---

## 9. O que ficou fora e por quê

- **Municípios, países, motivos e qualificações**: tabelas de domínio da Receita que não
  sustentam nenhuma métrica do fato atual. Entram como dimensões quando houver demanda.
- **CNAEs secundários**: o campo vem como lista concatenada em uma única coluna. Normalizá-lo
  exige uma tabela ponte própria, que só se paga quando existir análise multiatividade.
- **Dados pessoais dos sócios**: o CPF já chega mascarado na origem. Em produção, a coluna de
  nome deve ficar sob *column-level security*, liberada apenas para os papéis que precisam.

> Os valores monetários usam o preço de referência do modelo sob demanda e variam por região e
> por data. Os tamanhos de tabela e percentuais de redução são estimativas de ordem de
> grandeza para orientar decisão de arquitetura, não medições de um ambiente em produção.
