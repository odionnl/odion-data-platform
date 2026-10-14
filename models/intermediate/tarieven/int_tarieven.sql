-- Alle tarievenlijsten (seeds in seeds/tarieven/) samengevoegd.
-- Eén rij per financieringstype + debiteur + prestatiecode + geldigheidsperiode.
-- Nieuwe tarievenlijst: seed genereren met scripts/tarieven_naar_seed.py en hieronder toevoegen.

{% set tariefbestanden = [
    'tarieven_wlz_2026',
] %}

with tarieven as (

    {% for tariefbestand in tariefbestanden %}
    select
        financieringstype_naam,
        nullif(trim(debiteurnummer), '')    as debiteurnummer,
        geldig_vanaf,
        geldig_tot,
        prestatiecode,
        prestatienaam,
        tijdseenheid,
        cast(nullif(trim(tarief), '') as decimal(18, 4)) as tarief,
        '{{ tariefbestand }}'               as tariefbestand
    from {{ ref(tariefbestand) }}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}

)

select * from tarieven
