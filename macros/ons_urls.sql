{% macro ons_dossier_url(path, client_object_id) -%}
case
  when {{ client_object_id }} is not null then
    concat('https://odion.ons-dossier.nl/clients/', {{ client_object_id }}, '/{{ path }}')
end
{%- endmacro %}

{% macro ons_administratie_url(client_object_id) -%}
case
  when {{ client_object_id }} is not null then
    concat('https://odion.ioservice.net/client/', {{ client_object_id }}, '/view')
end
{%- endmacro %}
