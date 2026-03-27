with zorgplannen as (

    select * from {{ ref('int_zorgplannen_met_versie') }}

),

clienten as (

    select * from {{ ref('stg_onsdb__clients') }}

),

aanmakers as (

    select * from {{ ref('stg_onsdb__employees') }}

),

definitief as (

    select
        z.zorgplan_id,
        z.client_id,
        c.clientnummer,
        c.clientnaam                                              as client_naam,
        z.startdatum,
        z.einddatum,
        z.status_omschrijving                                   as status,
        z.geldigheid,
        z.zorgplan_versie,
        z.aantal_regels,
        z.aantal_geldige_regels,
        z.aangemaakt_door_id,
        m.voornaam + ' ' + m.achternaam                        as aangemaakt_door_naam,
        z.aangemaakt_op,
        z.gewijzigd_op,
        {{ ons_dossier_url('care_plan', 'z.client_id') }}      as url_ons_zorgplan

    from zorgplannen z
    left join clienten c
        on c.client_id = z.client_id
    left join aanmakers m
        on m.medewerker_id = z.aangemaakt_door_id

)

select * from definitief
