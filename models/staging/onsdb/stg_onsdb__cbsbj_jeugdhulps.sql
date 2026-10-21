-- CBS-jeugdhulptrajecten. Postcode, gemeente en tevredenheidscijfers
-- worden bewust niet overgenomen (dataminimalisatie).

with bron as (

    select * from {{ source('ons_plan_2', 'cbsbj_jeugdhulps') }}

),

definitief as (

    select
        objectId                as jeugdhulptraject_id,
        clientObjectId          as client_id,
        datumAanvang            as startdatum,
        datumBeeindiging        as einddatum,
        hulpvorm                as hulpvorm_code,
        leveringsvorm,
        redenBeeindiging        as reden_beeindiging_code,
        verwijzer               as verwijzer_code,
        createdAt               as aangemaakt_op,
        updatedAt               as gewijzigd_op

    from bron

)

select * from definitief
