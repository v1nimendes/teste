{{
    config(
        materialized='incremental',
        unique_key='cnpj',
        incremental_strategy='delete+insert'
    )
}}

with origem as (

    select * from {{ source('receita_federal', 'estabelecimentos') }}

    {% if is_incremental() %}
    where carregado_em > (select max(carregado_em) from {{ this }})
    {% endif %}

),

padronizado as (

    select
        {{ formata_cnpj('cnpj_basico', 'cnpj_ordem', 'cnpj_dv') }} as cnpj,
        trim(cnpj_basico) as cnpj_basico,
        trim(cnpj_ordem) as cnpj_ordem,
        nullif(trim(identificador_matriz_filial), '') as cod_matriz_filial,
        {{ traduz_dominio('identificador_matriz_filial', {
            '1': 'MATRIZ',
            '2': 'FILIAL'
        }) }} as matriz_filial,
        {{ limpa_texto('nome_fantasia') }} as nome_fantasia,
        nullif(trim(situacao_cadastral), '') as cod_situacao_cadastral,
        {{ traduz_dominio('situacao_cadastral', {
            '01': 'NULA',
            '02': 'ATIVA',
            '03': 'SUSPENSA',
            '04': 'INAPTA',
            '08': 'BAIXADA'
        }) }} as situacao_cadastral,
        {{ parse_data_rf('data_situacao_cadastral') }} as data_situacao_cadastral,
        {{ parse_data_rf('data_inicio_atividade') }} as data_inicio_atividade,
        lpad(nullif(trim(cnae_fiscal_principal), ''), 7, '0') as cod_cnae_principal,
        nullif(upper(trim(uf)), '') as uf,
        trim(municipio) as cod_municipio,
        nullif(trim(cep), '') as cep,
        lower(nullif(trim(correio_eletronico), '')) as correio_eletronico,
        carregado_em
    from origem
    where trim(cnpj_basico) <> ''

)

select
    *,
    {{ colunas_auditoria() }}
from padronizado
qualify row_number() over (partition by cnpj order by carregado_em desc) = 1
