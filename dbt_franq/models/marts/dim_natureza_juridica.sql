with naturezas as (

    select * from {{ ref('stg_naturezas') }}

)

select
    cod_natureza_juridica,
    descricao_natureza_juridica,
    substr(cod_natureza_juridica, 1, 1) as cod_categoria,
    {{ traduz_dominio('substr(cod_natureza_juridica, 1, 1)', {
        '0': 'NAO INFORMADA',
        '1': 'ADMINISTRACAO PUBLICA',
        '2': 'ENTIDADES EMPRESARIAIS',
        '3': 'ENTIDADES SEM FINS LUCRATIVOS',
        '4': 'PESSOAS FISICAS',
        '5': 'ORGANIZACOES INTERNACIONAIS'
    }) }} as categoria,
    substr(cod_natureza_juridica, 1, 1) = '2'
        and cod_natureza_juridica not in ('2135', '2305', '2313') as exige_socios,
    {{ colunas_auditoria() }}
from naturezas
