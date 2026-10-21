-- Verwachte jeugdhulptrajecten op basis van locatiekoppelingen.
-- Locaties krijgen een hulpvorm via de seed locatie_hulpvorm_mapping; locaties die
-- daar niet in staan tellen niet mee. Overlappende en aansluitende koppelingen met
-- dezelfde hulpvorm worden samengevoegd (gaps-and-islands): stopt één locatie maar
-- loopt een andere door, dan loopt de periode door tot de laatste locatie stopt.
-- Wachtlijstkoppelingen (WAITING) tellen niet mee.
-- Grain: één rij per client × hulpvorm × aaneengesloten periode.

with jeugdwet_clienten as (

    select client_id from {{ ref('int_jeugdwet_clienten') }}

),

koppelingen as (

    select * from {{ ref('stg_onsdb__location_assignments') }}
    where type_toekenning is null or type_toekenning <> 'WAITING'

),

locaties as (

    select * from {{ ref('stg_onsdb__locations') }}

),

mapping as (

    select * from {{ ref('locatie_hulpvorm_mapping') }}

),

locatiekoppelingen as (

    select
        koppelingen.client_id,
        koppelingen.locatie_id,
        locaties.locatienaam collate database_default   as locatienaam,
        mapping.hulpvorm_omschrijving,
        cast(koppelingen.startdatum as date)                                        as startdatum,
        -- Open einddatum als '9999-12-31' zodat MAX() een lopende koppeling herkent
        coalesce(cast(koppelingen.einddatum as date), cast('9999-12-31' as date))   as einddatum

    from koppelingen
    inner join jeugdwet_clienten
        on jeugdwet_clienten.client_id = koppelingen.client_id
    inner join locaties
        on locaties.locatie_id = koppelingen.locatie_id
    -- Seed (OdionDataPlatform) en OnsDB hebben een andere collation
    inner join mapping
        on mapping.locatienaam collate database_default = locaties.locatienaam collate database_default
    where koppelingen.startdatum is not null

),

met_vorige_einddatum as (

    select
        *,
        max(einddatum) over (
            partition by client_id, hulpvorm_omschrijving
            order by startdatum, einddatum
            rows between unbounded preceding and 1 preceding
        ) as max_vorige_einddatum

    from locatiekoppelingen

),

met_periode_nummer as (

    select
        *,
        sum(
            -- Nieuwe periode als er minstens één dag tussen zit (datediff i.p.v.
            -- dateadd om overflow op '9999-12-31' te voorkomen)
            case
                when max_vorige_einddatum is null
                  or datediff(day, max_vorige_einddatum, startdatum) > 1
                then 1 else 0
            end
        ) over (
            partition by client_id, hulpvorm_omschrijving
            order by startdatum, einddatum
            rows unbounded preceding
        ) as periode_nummer

    from met_vorige_einddatum

),

periodes as (

    select
        client_id,
        hulpvorm_omschrijving,
        periode_nummer,
        min(startdatum)                 as startdatum,
        max(einddatum)                  as einddatum,
        count(distinct locatie_id)      as aantal_locaties

    from met_periode_nummer
    group by client_id, hulpvorm_omschrijving, periode_nummer

),

locatienamen as (

    -- Eerst ontdubbelen: STRING_AGG ondersteunt geen DISTINCT
    select
        client_id,
        hulpvorm_omschrijving,
        periode_nummer,
        string_agg(locatienaam, ' | ') within group (order by locatienaam) as locaties

    from (
        select distinct client_id, hulpvorm_omschrijving, periode_nummer, locatienaam
        from met_periode_nummer
    ) as uniek
    group by client_id, hulpvorm_omschrijving, periode_nummer

),

definitief as (

    select
        periodes.client_id,
        periodes.hulpvorm_omschrijving,
        periodes.periode_nummer,
        periodes.startdatum,
        nullif(periodes.einddatum, cast('9999-12-31' as date)) as einddatum,
        periodes.aantal_locaties,
        locatienamen.locaties

    from periodes
    inner join locatienamen
        on  locatienamen.client_id             = periodes.client_id
        and locatienamen.hulpvorm_omschrijving = periodes.hulpvorm_omschrijving
        and locatienamen.periode_nummer        = periodes.periode_nummer

)

select * from definitief
