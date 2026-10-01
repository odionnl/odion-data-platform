-- Long-format variant van mart_feitelijk_geleverde_zorg_per_client: één rij per client per
-- datapunt, in plaats van één kolom per datapunt. Bedoeld voor Power BI, zodat
-- datapunten als dimensie op een as/slicer gebruikt kunnen worden en er niet per
-- datapunt een aparte measure nodig is.
--
-- Grain: één rij per client per datapunt (4 rijen per client in zorg).
-- waarde = NULL betekent "niet van toepassing" (alleen mogelijk bij
-- medicatie_afgetekend, als de client geen medicatie heeft). Power BI negeert
-- NULL bij AVERAGE, dus een gemiddelde over waarde geeft direct het slagingspercentage.
--
-- Locatie-informatie zit bewust niet in deze mart: die komt in Power BI via de
-- relatie op client_id (mart_clienten_actueel / mart_locatiekoppelingen_actueel).

with feitelijk_geleverde_zorg as (

    select * from {{ ref('mart_feitelijk_geleverde_zorg_per_client') }}

),

datapunten as (

    select
        client_id,
        'actueel_zorgplan'      as datapunt,
        'Actueel zorgplan'      as datapunt_label,
        1                       as datapunt_volgorde,
        actueel_zorgplan        as waarde,
        peildatum
    from feitelijk_geleverde_zorg

    union all

    select
        client_id,
        'recente_rapportages'   as datapunt,
        'Recente rapportages'   as datapunt_label,
        2                       as datapunt_volgorde,
        recente_rapportages     as waarde,
        peildatum
    from feitelijk_geleverde_zorg

    union all

    select
        client_id,
        'medicatie_afgetekend'  as datapunt,
        'Medicatie afgetekend'  as datapunt_label,
        3                       as datapunt_volgorde,
        medicatie_afgetekend    as waarde,
        peildatum
    from feitelijk_geleverde_zorg

    union all

    select
        client_id,
        'zorgdossier_bekeken'   as datapunt,
        'Zorgdossier bekeken'   as datapunt_label,
        4                       as datapunt_volgorde,
        zorgdossier_bekeken     as waarde,
        peildatum
    from feitelijk_geleverde_zorg

)

select * from datapunten
