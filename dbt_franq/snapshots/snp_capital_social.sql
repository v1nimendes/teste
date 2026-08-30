{% snapshot snp_capital_social %}

{{
    config(
        unique_key='cnpj_basico',
        strategy='check',
        check_cols=['capital_social', 'cod_porte']
    )
}}

select
    trim(cnpj_basico) as cnpj_basico,
    {{ limpa_texto('razao_social') }} as razao_social,
    {{ parse_valor_br('capital_social') }} as capital_social,
    trim(porte) as cod_porte
from {{ source('receita_federal', 'empresas') }}
where trim(cnpj_basico) <> ''
qualify row_number() over (partition by trim(cnpj_basico) order by carregado_em desc) = 1

{% endsnapshot %}
