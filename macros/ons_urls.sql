
{% macro ons_dossier_url
(path, client_id) -%}
case
    when {{ client_id }} is not null
    then concat
('https://odion.ons-dossier.nl/clients/', {{ client_id }}, '/{{ path }}')
end
{%- endmacro %}

{% macro ons_administratie_url
(client_id) -%}
case
    when {{ client_id }} is not null
    then concat
('https://odion.ioservice.net/client/', {{ client_id }}, '/view')
end
{%- endmacro %}

{% macro ons_medicatie_url
(client_id) -%}
case
    when {{ client_id }} is not null
    then concat
('https://odion.ons-medicatie.nl/clients/', {{ client_id }})
end
{%- endmacro %}

{% macro ons_medicatie_dag_url
(client_id, datum) -%}
case
    when {{ client_id }} is not null and {{ datum }} is not null
    then concat
('https://odion.ons-medicatie.nl/clients/', {{ client_id }}, '/days/', convert(char(10), {{ datum }}, 23))
end
{%- endmacro %}
