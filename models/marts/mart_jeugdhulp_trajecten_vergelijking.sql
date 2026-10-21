-- Vergelijking verwachte jeugdhulptrajecten (op basis van locatiekoppelingen) met de
-- geregistreerde CBS-jeugdhulptrajecten, per Jeugdwet-client en hulpvorm.
-- Koppeling op client + hulpvorm + overlappende periode (full outer join), zodat
-- zowel ontbrekende trajecten als trajecten zonder locatie zichtbaar worden.
-- Datums worden exact vergeleken; verschil in dagen staat in aparte kolommen.
-- Grain: één rij per locatieperiode × overlappend traject.

with locatieperiodes as (

    select * from {{ ref('int_jeugdhulp_locatieperiodes') }}

),

trajecten as (

    select * from {{ ref('int_jeugdhulp_trajecten') }}

),

jeugdwet_clienten as (

    select * from {{ ref('int_jeugdwet_clienten') }}

),

gekoppeld as (

    select
        coalesce(locatieperiodes.client_id, trajecten.client_id)                         as client_id,
        coalesce(locatieperiodes.hulpvorm_omschrijving, trajecten.hulpvorm_omschrijving) as hulpvorm_omschrijving,

        -- Locatiekant (verwacht)
        locatieperiodes.periode_nummer,
        locatieperiodes.startdatum      as locatie_startdatum,
        locatieperiodes.einddatum       as locatie_einddatum,
        locatieperiodes.aantal_locaties,
        locatieperiodes.locaties,

        -- Trajectkant (geregistreerd)
        trajecten.jeugdhulptraject_id,
        trajecten.hulpvorm_code,
        trajecten.startdatum            as traject_startdatum,
        trajecten.einddatum             as traject_einddatum,
        trajecten.reden_beeindiging_code

    from locatieperiodes
    full outer join trajecten
        on  trajecten.client_id             = locatieperiodes.client_id
        and trajecten.hulpvorm_omschrijving = locatieperiodes.hulpvorm_omschrijving
        and trajecten.startdatum           <= coalesce(locatieperiodes.einddatum, cast('9999-12-31' as date))
        and locatieperiodes.startdatum     <= coalesce(trajecten.einddatum, cast('9999-12-31' as date))

),

met_aantallen as (

    select
        *,
        case
            when locatie_startdatum is not null and jeugdhulptraject_id is not null
            then count(jeugdhulptraject_id) over (
                partition by client_id, hulpvorm_omschrijving, periode_nummer
            )
        end as aantal_trajecten_in_periode

    from gekoppeld

),

definitief as (

    select
        met_aantallen.client_id,
        met_aantallen.hulpvorm_omschrijving,
        jeugdwet_clienten.heeft_actieve_beschikking,

        met_aantallen.locatie_startdatum,
        met_aantallen.locatie_einddatum,
        met_aantallen.aantal_locaties,
        met_aantallen.locaties,

        met_aantallen.jeugdhulptraject_id,
        met_aantallen.hulpvorm_code,
        met_aantallen.traject_startdatum,
        met_aantallen.traject_einddatum,
        met_aantallen.reden_beeindiging_code,

        met_aantallen.aantal_trajecten_in_periode,

        -- Positief = traject start/eindigt later dan de locatieperiode
        datediff(day, met_aantallen.locatie_startdatum, met_aantallen.traject_startdatum) as verschil_startdatum_dagen,
        datediff(day, met_aantallen.locatie_einddatum,  met_aantallen.traject_einddatum)  as verschil_einddatum_dagen,

        case
            when met_aantallen.jeugdhulptraject_id is null
                then 'Traject ontbreekt'
            when met_aantallen.locatie_startdatum is null
                then 'Traject zonder locatie'
            when met_aantallen.aantal_trajecten_in_periode > 1
                then 'Meerdere trajecten voor één locatieperiode'
            when (met_aantallen.traject_einddatum is null
                  and met_aantallen.locatie_einddatum < cast(getdate() as date))
              or met_aantallen.traject_einddatum > met_aantallen.locatie_einddatum
                then 'Traject loopt door na laatste locatie'
            when met_aantallen.traject_einddatum
                 < coalesce(met_aantallen.locatie_einddatum, cast('9999-12-31' as date))
                then 'Traject gestopt terwijl locatie loopt'
            when met_aantallen.traject_startdatum <> met_aantallen.locatie_startdatum
                then 'Startdatum wijkt af'
            else 'Akkoord'
        end as vergelijking_status,

        -- Actueel: locatieperiode of traject loopt vandaag nog
        case
            when (met_aantallen.locatie_startdatum is not null
                  and (met_aantallen.locatie_einddatum is null
                       or met_aantallen.locatie_einddatum >= cast(getdate() as date)))
              or (met_aantallen.jeugdhulptraject_id is not null
                  and (met_aantallen.traject_einddatum is null
                       or met_aantallen.traject_einddatum >= cast(getdate() as date)))
            then 1 else 0
        end as is_actief

    from met_aantallen
    left join jeugdwet_clienten
        on jeugdwet_clienten.client_id = met_aantallen.client_id

)

select * from definitief
