with origem as (

    select * from {{ source('receita_federal', 'receitaws') }}

),

padronizado as (

    select
        trim(cnpj_basico) as cnpj_basico,
        {{ limpa_texto('situacao') }} as situacao_receitaws,
        {{ limpa_texto('porte') }} as porte_receitaws,
        try_strptime(nullif(trim(abertura), ''), '%d/%m/%Y')::date as data_abertura_receitaws,
        try_cast(nullif(trim(capital_social), '') as decimal(18, 2)) as capital_social_receitaws,
        regexp_replace(trim(atividade_principal), '[^0-9]', '', 'g') as cod_cnae_receitaws,
        carregado_em
    from origem
    where trim(cnpj_basico) <> ''

)

select
    *,
    {{ colunas_auditoria() }}
from padronizado
qualify row_number() over (partition by cnpj_basico order by carregado_em desc) = 1
