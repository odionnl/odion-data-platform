-- Long-format variant van mart_feitelijk_geleverde_zorg: één rij per client per
-- datapunt, in plaats van één kolom per datapunt. Bedoeld voor Power BI, zodat
-- datapunten als dimensie op een as/slicer gebruikt kunnen worden en er niet per
-- datapunt een aparte measure nodig is.
--
-- Grain: één rij per client per datapunt (4 rijen per client in zorg).
-- waarde = NULL betekent "niet van toepassing" (alleen mogelijk bij
-- medicatie_toegediend, als de client geen medicatie heeft). Power BI negeert
-- NULL bij AVERAGE, dus een gemiddelde over waarde geeft direct het slagingspercentage.
--
-- Locatie-informatie zit bewust niet in deze mart: die komt in Power BI via de
-- relatie op client_id (mart_clienten_actueel / mart_locatiekoppelingen_actueel).

with feitelijk_geleverde_zorg as (

    select * from {{ ref('mart_feitelijk_geleverde_zorg') }}

),

datapunten as (

    select
        client_id,
        'geldig_zorgplan'       as datapunt,
        'Geldig zorgplan'       as datapunt_label,
        1                       as datapunt_volgorde,
        geldig_zorgplan         as waarde,
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
        'medicatie_toegediend'  as datapunt,
        'Medicatie toegediend'  as datapunt_label,
        3                       as datapunt_volgorde,
        medicatie_toegediend    as waarde,
        peildatum
    from feitelijk_geleverde_zorg

    union all

    select
        client_id,
        'zorgplan_ingezien'     as datapunt,
        'Zorgplan ingezien'     as datapunt_label,
        4                       as datapunt_volgorde,
        zorgplan_ingezien       as waarde,
        peildatum
    from feitelijk_geleverde_zorg

)

select * from datapunten
