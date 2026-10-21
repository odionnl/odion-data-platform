-- CBS-jeugdhulptrajecten van Jeugdwet-cliënten, verrijkt met hulpvorm-omschrijving.
-- Alleen hulpvormen die in de seed locatie_hulpvorm_mapping voorkomen: voor andere
-- hulpvormen bestaat geen verwachte locatie, dus vergelijken heeft geen zin.
-- Grain: één rij per jeugdhulptraject.

with jeugdwet_clienten as (

    select client_id from {{ ref('int_jeugdwet_clienten') }}

),

trajecten as (

    select * from {{ ref('stg_onsdb__cbsbj_jeugdhulps') }}

),

hulpvormen as (

    select * from {{ ref('stg_onsdb__cbsbj_hulpvormen') }}

),

gemapte_hulpvormen as (

    select distinct hulpvorm_omschrijving from {{ ref('locatie_hulpvorm_mapping') }}

),

definitief as (

    select
        trajecten.jeugdhulptraject_id,
        trajecten.client_id,
        trajecten.hulpvorm_code,
        -- Seed (OdionDataPlatform) en OnsDB hebben een andere collation; gelijktrekken
        -- zodat de mart deze kolom kan joinen met int_jeugdhulp_locatieperiodes
        hulpvormen.hulpvorm_omschrijving collate database_default as hulpvorm_omschrijving,
        cast(trajecten.startdatum as date)  as startdatum,
        cast(trajecten.einddatum as date)   as einddatum,
        trajecten.leveringsvorm,
        trajecten.reden_beeindiging_code

    from trajecten
    inner join jeugdwet_clienten
        on jeugdwet_clienten.client_id = trajecten.client_id
    inner join hulpvormen
        on hulpvormen.hulpvorm_code = trajecten.hulpvorm_code
    inner join gemapte_hulpvormen
        on gemapte_hulpvormen.hulpvorm_omschrijving collate database_default
         = hulpvormen.hulpvorm_omschrijving collate database_default

)

select * from definitief
