{% macro parse_toi(toi_expression) %}
    {#- Converts an NHL "MM:SS" time-on-ice string to integer seconds -#}
    (
        cast(split_part({{ toi_expression }}, ':', 1) as int) * 60
        + cast(split_part({{ toi_expression }}, ':', -1) as int)
    )
{% endmacro %}
