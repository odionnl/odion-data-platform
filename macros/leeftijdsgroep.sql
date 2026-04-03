{% macro get_leeftijdsgroep1(leeftijd_col) %}
    case
        when {{ leeftijd_col }} is null then 'Onbekend'
        when {{ leeftijd_col }} < 18    then '<18'
        when {{ leeftijd_col }} between 18 and 49 then '18-49'
        when {{ leeftijd_col }} between 50 and 64 then '50-64'
        when {{ leeftijd_col }} >= 65   then '65+'
        else 'Onbekend'
    end
{% endmacro %}

{% macro get_leeftijdsgroep2(leeftijd_col) %}
    case
        when {{ leeftijd_col }} is null then 'Onbekend'
        when {{ leeftijd_col }} < 18    then '<18'
        when {{ leeftijd_col }} between 18 and 30 then '18-30'
        when {{ leeftijd_col }} between 31 and 50 then '31-50'
        when {{ leeftijd_col }} > 50    then '50+'
        else 'Onbekend'
    end
{% endmacro %}
