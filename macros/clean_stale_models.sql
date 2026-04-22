{#
  Dropt objecten die in dbt-target-schemas staan maar niet meer in het dbt-project.
  Vergelijkt graph.nodes (huidige modellen/seeds/snapshots) met INFORMATION_SCHEMA.TABLES.

  WAARSCHUWING: alleen zinvol na een volledige `dbt run`. Na een partial run
  (--select ...) zou de macro niet-gebuilde modellen als stale zien.

  Gebruik:
    dbt run-operation clean_stale_models                         # dry-run (default)
    dbt run-operation clean_stale_models --args '{dryrun: false}' # echt droppen
#}

{% macro clean_stale_models(dryrun=true) %}

  {% if execute %}

    {% set expected = [] %}
    {% for node in graph.nodes.values() %}
      {% if node.resource_type in ['model', 'seed', 'snapshot'] %}
        {% do expected.append((node.schema, node.alias)) %}
      {% endif %}
    {% endfor %}

    {% if expected | length == 0 %}
      {% do log("Geen dbt-modellen in graph gevonden, niets te doen.", info=true) %}
      {% do return(none) %}
    {% endif %}

    {% set dbt_schemas = expected | map(attribute=0) | unique | list %}
    {% set schemas_sql = "'" ~ dbt_schemas | join("','") ~ "'" %}

    {% set query %}
      SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
      FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA IN ({{ schemas_sql }})
    {% endset %}

    {% set results = run_query(query) %}

    {% set expected_keys = [] %}
    {% for s, n in expected %}
      {% do expected_keys.append(s ~ '.' ~ n) %}
    {% endfor %}

    {% set stale = [] %}
    {% for row in results %}
      {% set key = row[0] ~ '.' ~ row[1] %}
      {% if key not in expected_keys %}
        {% set kind = 'VIEW' if row[2] == 'VIEW' else 'TABLE' %}
        {% do stale.append({'schema': row[0], 'name': row[1], 'kind': kind}) %}
      {% endif %}
    {% endfor %}

    {% if stale | length == 0 %}
      {% do log("Geen stale objecten in schemas: " ~ dbt_schemas | join(', '), info=true) %}
    {% else %}
      {% do log("Gevonden " ~ (stale | length) ~ " stale object(en) in " ~ dbt_schemas | join(', ') ~ ":", info=true) %}
      {% for o in stale %}
        {% do log("  " ~ o.kind ~ " [" ~ o.schema ~ "].[" ~ o.name ~ "]", info=true) %}
      {% endfor %}

      {% if dryrun %}
        {% do log("DRY-RUN: niets gedropt. Run met --args '{dryrun: false}' om uit te voeren.", info=true) %}
      {% else %}
        {% for o in stale %}
          {% set drop_stmt = "DROP " ~ o.kind ~ " IF EXISTS [" ~ o.schema ~ "].[" ~ o.name ~ "]" %}
          {% do log("Uitvoeren: " ~ drop_stmt, info=true) %}
          {% do run_query(drop_stmt) %}
        {% endfor %}
        {% do log("Klaar: " ~ (stale | length) ~ " object(en) gedropt.", info=true) %}
      {% endif %}
    {% endif %}

  {% endif %}

{% endmacro %}
