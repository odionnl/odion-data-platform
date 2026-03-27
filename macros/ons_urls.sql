
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
