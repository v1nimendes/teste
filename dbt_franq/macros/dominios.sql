{% macro traduz_dominio(coluna, valores, padrao='NAO INFORMADO') %}
    case trim({{ coluna }})
        {% for chave, descricao in valores.items() -%}
        when '{{ chave }}' then '{{ descricao }}'
        {% endfor -%}
        else '{{ padrao }}'
    end
{% endmacro %}


{% macro secao_cnae(coluna) %}
    {%- set faixas = [
        (1, 3, 'A'), (5, 9, 'B'), (10, 33, 'C'), (35, 35, 'D'), (36, 39, 'E'),
        (41, 43, 'F'), (45, 47, 'G'), (49, 53, 'H'), (55, 56, 'I'), (58, 63, 'J'),
        (64, 66, 'K'), (68, 68, 'L'), (69, 75, 'M'), (77, 82, 'N'), (84, 84, 'O'),
        (85, 85, 'P'), (86, 88, 'Q'), (90, 93, 'R'), (94, 96, 'S'), (97, 97, 'T'),
        (99, 99, 'U')
    ] -%}
    case
        {% for inicio, fim, letra in faixas -%}
        when try_cast(substr({{ coluna }}, 1, 2) as integer) between {{ inicio }} and {{ fim }} then '{{ letra }}'
        {% endfor -%}
    end
{% endmacro %}
