select
    cnpj_basico,
    count(*) as qtd_estabelecimentos,
    count(*) filter (
        where cod_situacao_cadastral = '{{ var("situacao_ativa") }}'
    ) as qtd_estabelecimentos_ativos,
    min(data_inicio_atividade) as data_inicio_atividade,
    max_by(uf, cod_matriz_filial = '1') as uf_matriz,
    max_by(cod_cnae_principal, cod_matriz_filial = '1') as cod_cnae_principal,
    max(carregado_em) as carregado_em
from {{ ref('stg_estabelecimentos') }}
group by cnpj_basico
