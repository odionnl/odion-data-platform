with koppelingen as (

    select * from {{ ref('int_clienten_met_locaties') }}

),

clienten as (

    select * from {{ ref('stg_onsdb__clients') }}

),

definitief as (

    select
        koppelingen.locatiekoppeling_id,

        -- Client
        koppelingen.client_id,
        clienten.clientnummer,
        clienten.clientnaam,

        -- Locatie
        koppelingen.locatie_id,
        koppelingen.locatienaam,

        -- Type & hoofdlocatie-vlag
        koppelingen.type_toekenning,
        case when koppelingen.type_toekenning = 'MAIN' then 1 else 0 end as is_hoofdlocatie,

        -- Datums
        koppelingen.locatie_startdatum  as startdatum,
        koppelingen.locatie_einddatum   as einddatum,

        -- Actueel vandaag
        case
            when (koppelingen.locatie_startdatum is null or koppelingen.locatie_startdatum <= cast(getdate() as date))
             and (koppelingen.locatie_einddatum  is null or koppelingen.locatie_einddatum  >= cast(getdate() as date))
            then 1 else 0
        end as is_actief

    from koppelingen
    left join clienten
        on clienten.client_id = koppelingen.client_id

)

select * from definitief
