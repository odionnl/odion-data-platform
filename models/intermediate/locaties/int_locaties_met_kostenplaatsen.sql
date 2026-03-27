-- Locaties met al hun kostenplaatskoppelingen over alle tijd.
-- Grain: één rij per locatie × koppelingsperiode.
-- Gebruik startdatum_koppeling / einddatum_koppeling om te filteren op een specifieke datum.

with locaties as (

    select * from {{ ref('stg_onsdb__locations') }}

),

kostenplaats_koppelingen as (

    select * from {{ ref('stg_onsdb__costcenter_assignments') }}

),

kostenplaatsen as (

    select * from {{ ref('stg_onsdb__costcenters') }}

),

definitief as (

    select
        locaties.locatie_id,
        locaties.locatienaam,
        locaties.locatie_hierarchie_pad,
        kostenplaatsen.kostenplaats_code    as kostenplaats_id,
        kostenplaats_koppelingen.startdatum as startdatum_koppeling,
        kostenplaats_koppelingen.einddatum  as einddatum_koppeling

    from locaties
    left join kostenplaats_koppelingen
        on kostenplaats_koppelingen.locatie_id = locaties.locatie_id
    left join kostenplaatsen
        on kostenplaatsen.kostenplaats_id = kostenplaats_koppelingen.kostenplaats_id

)

select * from definitief
