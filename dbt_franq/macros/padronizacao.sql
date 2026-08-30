{% macro parse_valor_br(coluna) %}
    try_cast(replace(nullif(trim({{ coluna }}), ''), ',', '.') as decimal(18, 2))
{% endmacro %}


{% macro parse_data_rf(coluna) %}
    try_strptime(nullif(trim({{ coluna }}), ''), '%Y%m%d')::date
{% endmacro %}


{% macro limpa_texto(coluna) %}
    nullif(upper(trim(regexp_replace({{ coluna }}, '\s+', ' ', 'g'))), '')
{% endmacro %}


{% macro formata_cnpj(basico, ordem, dv) %}
    lpad(trim({{ basico }}), 8, '0')
        || lpad(trim({{ ordem }}), 4, '0')
        || lpad(trim({{ dv }}), 2, '0')
{% endmacro %}
