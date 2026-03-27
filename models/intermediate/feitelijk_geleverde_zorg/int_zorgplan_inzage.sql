with bron as (

    select * from {{ ref('stg_ons_audits__zorgplan_inzage') }}

),

definitief as (

    select
        tijdstip,
        left(gebruiker_medewerkernummer,
             charindex('-', gebruiker_medewerkernummer) - 1) as medewerker_id,
        clientnummer

    from bron
    where gebruiker_medewerkernummer like '%-%'

)

select * from definitief
