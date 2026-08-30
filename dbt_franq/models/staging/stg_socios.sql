with origem as (

    select * from {{ source('receita_federal', 'socios') }}

),

padronizado as (

    select
        trim(cnpj_basico) as cnpj_basico,
        nullif(trim(identificador_socio), '') as cod_tipo_socio,
        {{ traduz_dominio('identificador_socio', {
            '1': 'PESSOA JURIDICA',
            '2': 'PESSOA FISICA',
            '3': 'ESTRANGEIRO'
        }) }} as tipo_socio,
        {{ limpa_texto('nome_socio') }} as nome_socio,
        trim(cnpj_cpf_socio) as documento_socio,
        nullif(trim(qualificacao_socio), '') as cod_qualificacao_socio,
        {{ parse_data_rf('data_entrada_sociedade') }} as data_entrada_sociedade,
        nullif(trim(faixa_etaria), '') as cod_faixa_etaria,
        carregado_em
    from origem
    where trim(cnpj_basico) <> ''

)

select
    *,
    {{ colunas_auditoria() }}
from padronizado
qualify
    row_number() over (
        partition by cnpj_basico, documento_socio, cod_qualificacao_socio
        order by carregado_em desc
    ) = 1
