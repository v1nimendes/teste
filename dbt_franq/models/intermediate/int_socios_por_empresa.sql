select
    cnpj_basico,
    count(*) as qtd_socios,
    count(*) filter (where cod_tipo_socio = '1') as qtd_socios_pessoa_juridica,
    count(*) filter (where cod_tipo_socio = '2') as qtd_socios_pessoa_fisica,
    min(data_entrada_sociedade) as data_entrada_primeiro_socio
from {{ ref('stg_socios') }}
group by cnpj_basico
