with empresas as (

    select * from {{ ref('stg_empresas') }}

),

enriquecimento as (

    select * from {{ ref('stg_receitaws') }}

)

select
    empresas.cnpj_basico,
    empresas.razao_social,
    empresas.cod_natureza_juridica,
    empresas.cod_qualificacao_responsavel,
    empresas.cod_porte,
    empresas.porte,
    empresas.capital_social,
    empresas.ente_federativo_responsavel,
    enriquecimento.situacao_receitaws,
    enriquecimento.capital_social_receitaws,
    {{ colunas_auditoria() }}
from empresas
left join enriquecimento
    on empresas.cnpj_basico = enriquecimento.cnpj_basico
