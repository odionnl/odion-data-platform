{% macro ons_url(path, client_object_id) -%}
concat('https://odion.ons-dossier.nl/clients/', {{ client_object_id }}, '/{{ path }}')
{%- endmacro %}
