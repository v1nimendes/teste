with origem as (

    select * from {{ source('receita_federal', 'naturezas') }}

),

padronizado as (

    select
        lpad(trim(codigo), 4, '0') as cod_natureza_juridica,
        {{ limpa_texto('descricao') }} as descricao_natureza_juridica,
        carregado_em
    from origem
    where trim(codigo) <> ''

)

select
    *,
    {{ colunas_auditoria() }}
from padronizado
qualify row_number() over (partition by cod_natureza_juridica order by carregado_em desc) = 1
