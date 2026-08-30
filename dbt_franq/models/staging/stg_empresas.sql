with origem as (

    select * from {{ source('receita_federal', 'empresas') }}

),

padronizado as (

    select
        trim(cnpj_basico) as cnpj_basico,
        {{ limpa_texto('razao_social') }} as razao_social,
        lpad(nullif(trim(natureza_juridica), ''), 4, '0') as cod_natureza_juridica,
        nullif(trim(qualificacao_responsavel), '') as cod_qualificacao_responsavel,
        {{ parse_valor_br('capital_social') }} as capital_social,
        nullif(trim(porte), '') as cod_porte,
        {{ traduz_dominio('porte', {
            '00': 'NAO INFORMADO',
            '01': 'MICRO EMPRESA',
            '03': 'EMPRESA DE PEQUENO PORTE',
            '05': 'DEMAIS'
        }) }} as porte,
        {{ limpa_texto('ente_federativo_responsavel') }} as ente_federativo_responsavel,
        carregado_em
    from origem
    where trim(cnpj_basico) <> ''

)

select
    *,
    {{ colunas_auditoria() }}
from padronizado
qualify row_number() over (partition by cnpj_basico order by carregado_em desc) = 1
