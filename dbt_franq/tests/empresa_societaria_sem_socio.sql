{{ config(warn_if = '>0', error_if = '>100') }}

select
    fato.cnpj_basico,
    fato.cod_natureza_juridica,
    fato.qtd_socios
from {{ ref('fct_empresas_ativas') }} as fato
inner join {{ ref('dim_natureza_juridica') }} as natureza
    on fato.cod_natureza_juridica = natureza.cod_natureza_juridica
where natureza.exige_socios
    and fato.qtd_socios = 0
