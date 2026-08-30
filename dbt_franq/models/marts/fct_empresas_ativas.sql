{{
    config(
        materialized='incremental',
        unique_key='cnpj_basico',
        incremental_strategy='delete+insert'
    )
}}

with empresas as (

    select * from {{ ref('dim_empresa') }}

),

estabelecimentos as (

    select * from {{ ref('int_estabelecimentos_por_empresa') }}

),

socios as (

    select * from {{ ref('int_socios_por_empresa') }}

),

simples as (

    select * from {{ ref('stg_simples') }}

),

consolidado as (

    select
        empresas.cnpj_basico,
        empresas.razao_social,
        empresas.cod_natureza_juridica,
        estabelecimentos.cod_cnae_principal,
        estabelecimentos.uf_matriz as uf,
        empresas.cod_porte,
        empresas.porte,
        estabelecimentos.data_inicio_atividade,
        empresas.capital_social,
        estabelecimentos.qtd_estabelecimentos,
        estabelecimentos.qtd_estabelecimentos_ativos,
        coalesce(socios.qtd_socios, 0) as qtd_socios,
        coalesce(socios.qtd_socios_pessoa_juridica, 0) as qtd_socios_pessoa_juridica,
        coalesce(simples.optante_simples, false) as optante_simples,
        coalesce(simples.optante_mei, false) as optante_mei,
        date_diff('year', estabelecimentos.data_inicio_atividade, current_date) as anos_atividade,
        estabelecimentos.carregado_em
    from empresas
    inner join estabelecimentos
        on empresas.cnpj_basico = estabelecimentos.cnpj_basico
    left join socios
        on empresas.cnpj_basico = socios.cnpj_basico
    left join simples
        on empresas.cnpj_basico = simples.cnpj_basico
    where estabelecimentos.qtd_estabelecimentos_ativos > 0

    {% if is_incremental() %}
    and estabelecimentos.carregado_em > (select max(carregado_em) from {{ this }})
    {% endif %}

)

select
    *,
    {{ colunas_auditoria() }}
from consolidado
