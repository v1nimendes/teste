with cnaes as (

    select * from {{ ref('stg_cnaes') }}

),

hierarquia as (

    select
        cod_cnae,
        descricao_cnae,
        substr(cod_cnae, 1, 5) as cod_classe,
        substr(cod_cnae, 1, 3) as cod_grupo,
        substr(cod_cnae, 1, 2) as cod_divisao,
        {{ secao_cnae('cod_cnae') }} as cod_secao
    from cnaes

)

select
    cod_cnae,
    descricao_cnae,
    cod_classe,
    cod_grupo,
    cod_divisao,
    cod_secao,
    {{ traduz_dominio('cod_secao', {
        'A': 'AGRICULTURA, PECUARIA, PRODUCAO FLORESTAL, PESCA E AQUICULTURA',
        'B': 'INDUSTRIAS EXTRATIVAS',
        'C': 'INDUSTRIAS DE TRANSFORMACAO',
        'D': 'ELETRICIDADE E GAS',
        'E': 'AGUA, ESGOTO, GESTAO DE RESIDUOS E DESCONTAMINACAO',
        'F': 'CONSTRUCAO',
        'G': 'COMERCIO E REPARACAO DE VEICULOS AUTOMOTORES E MOTOCICLETAS',
        'H': 'TRANSPORTE, ARMAZENAGEM E CORREIO',
        'I': 'ALOJAMENTO E ALIMENTACAO',
        'J': 'INFORMACAO E COMUNICACAO',
        'K': 'ATIVIDADES FINANCEIRAS, DE SEGUROS E SERVICOS RELACIONADOS',
        'L': 'ATIVIDADES IMOBILIARIAS',
        'M': 'ATIVIDADES PROFISSIONAIS, CIENTIFICAS E TECNICAS',
        'N': 'ATIVIDADES ADMINISTRATIVAS E SERVICOS COMPLEMENTARES',
        'O': 'ADMINISTRACAO PUBLICA, DEFESA E SEGURIDADE SOCIAL',
        'P': 'EDUCACAO',
        'Q': 'SAUDE HUMANA E SERVICOS SOCIAIS',
        'R': 'ARTES, CULTURA, ESPORTE E RECREACAO',
        'S': 'OUTRAS ATIVIDADES DE SERVICOS',
        'T': 'SERVICOS DOMESTICOS',
        'U': 'ORGANISMOS INTERNACIONAIS E OUTRAS INSTITUICOES EXTRATERRITORIAIS'
    }) }} as descricao_secao,
    {{ colunas_auditoria() }}
from hierarquia
