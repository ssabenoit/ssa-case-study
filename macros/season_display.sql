{% macro season_display(season_col) %}
    {#- Formats an NHL season key (20252026) as its display form (2025-26) -#}
    (substr({{ season_col }}::string, 1, 4) || '-' || substr({{ season_col }}::string, 7, 2))
{% endmacro %}
