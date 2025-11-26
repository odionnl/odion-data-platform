{% macro get_leeftijdsgroep(leeftijd_col) %}
    case
        when {{ leeftijd_col }} is null then 'Onbekend'
        when {{ leeftijd_col }} < 18 then '<18'
        when {{ leeftijd_col }} between 18 and 49 then '18-49'
        when {{ leeftijd_col }} between 50 and 64 then '50-64'
        when {{ leeftijd_col }} >= 65 then '65+'
        else 'Onbekend'
    end
{% endmacro %}