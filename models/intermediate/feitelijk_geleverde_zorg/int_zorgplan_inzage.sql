with bron as (

    select * from {{ ref('stg_ons_audits__zorgplan_inzage') }}

),

medewerkers as (

    select
        medewerker_id,
        personeelsnummer
    from {{ ref('stg_onsdb__employees') }}
    where personeelsnummer is not null
      and personeelsnummer <> ''

),

met_personeelsnummer as (

    select
        tijdstip,
        left(gebruiker_medewerkernummer,
             charindex('-', gebruiker_medewerkernummer) - 1) as personeelsnummer,
        clientnummer

    from bron
    where gebruiker_medewerkernummer like '%-%'

),

definitief as (

    select
        mp.tijdstip,
        m.medewerker_id,
        mp.personeelsnummer,
        mp.clientnummer

    from met_personeelsnummer as mp
    left join medewerkers as m
        on m.personeelsnummer collate database_default
         = mp.personeelsnummer collate database_default

)

select * from definitief
