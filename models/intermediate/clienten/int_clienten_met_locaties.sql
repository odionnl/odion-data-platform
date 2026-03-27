with clienten as (

    select * from {{ ref('stg_onsdb__clients') }}

),

locatiekoppelingen as (

    select * from {{ ref('stg_onsdb__location_assignments') }}

),

locaties as (

    select * from {{ ref('stg_onsdb__locations') }}

),

definitief as (

    select
        clienten.client_id,
        clienten.voornaam,
        clienten.achternaam,
        locaties.locatie_id,
        locaties.locatienaam,
        locaties.is_intramuraal,
        locatiekoppelingen.startdatum   as locatie_startdatum,
        locatiekoppelingen.einddatum    as locatie_einddatum,
        locatiekoppelingen.type_toekenning,
        locatiekoppelingen.is_verblijfslocatie

    from clienten
    inner join locatiekoppelingen
        on locatiekoppelingen.client_id = clienten.client_id
    inner join locaties
        on locaties.locatie_id = locatiekoppelingen.locatie_id

)

select * from definitief
