with origem as (

    select * from {{ source('receita_federal', 'simples') }}

),

padronizado as (

    select
        trim(cnpj_basico) as cnpj_basico,
        trim(opcao_simples) = 'S' as optante_simples,
        {{ parse_data_rf('data_opcao_simples') }} as data_opcao_simples,
        {{ parse_data_rf('data_exclusao_simples') }} as data_exclusao_simples,
        trim(opcao_mei) = 'S' as optante_mei,
        {{ parse_data_rf('data_opcao_mei') }} as data_opcao_mei,
        {{ parse_data_rf('data_exclusao_mei') }} as data_exclusao_mei,
        carregado_em
    from origem
    where trim(cnpj_basico) <> ''

)

select
    *,
    {{ colunas_auditoria() }}
from padronizado
qualify row_number() over (partition by cnpj_basico order by carregado_em desc) = 1
