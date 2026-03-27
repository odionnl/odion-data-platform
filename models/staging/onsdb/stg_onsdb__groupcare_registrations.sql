with bron as (

    select * from {{ source('ons_plan_2', 'groupcare_registrations') }}

),

definitief as (

    select
        id                  as groepszorg_registratie_id,
        client_id           as groepszorg_client_id,
        client_external_id  as client_id,
        external_id         as booking_id,
        group_id            as groepszorg_groep_id,
        timeline_id         as groupcare_afspraak_id,
        team_cost_center    as kostenplaats_id,
        [date]              as registratie_datum,
        starts_at           as starttijd,
        ends_at             as eindtijd,
        status,
        case status
            when 0 then 'Aanwezig'
            when 1 then 'Afwezig'
            when 2 then 'Aangemaakt'
            when 3 then 'No-show'
            when 4 then 'Verwijderd'
            else 'Onbekend'
        end                 as status_omschrijving,
        cast(locked as int) as is_gefiatteerd,
        created_at          as aangemaakt_op,
        updated_at          as gewijzigd_op

    from bron

    where status != 4  -- verwijderde registraties uitsluiten

)

select * from definitief
