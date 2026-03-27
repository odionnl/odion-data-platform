with bron as (

    select * from {{ source('ons_audits', 'audits') }}

),

definitief as (

    select
        tijdstip,
        gebruiker_medewerkernummer,
        betreft_cli_nt_cli_ntnummer as clientnummer

    from bron

)

select * from definitief
