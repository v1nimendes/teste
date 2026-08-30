with origem as (

    select * from {{ source('receita_federal', 'cnaes') }}

),

padronizado as (

    select
        lpad(trim(codigo), 7, '0') as cod_cnae,
        {{ limpa_texto('descricao') }} as descricao_cnae,
        carregado_em
    from origem
    where trim(codigo) <> ''

)

select
    *,
    {{ colunas_auditoria() }}
from padronizado
qualify row_number() over (partition by cod_cnae order by carregado_em desc) = 1
