with bron as (

    select * from {{ source('ons_plan_2', 'groupcare_groups') }}

),

definitief as (

    select
        id                      as groepszorg_groep_id,
        [name]                  as groepsnaam,
        location_id             as groepszorg_locatie_id,
        team_id                 as team_id,
        cast(archived as int)   as is_gearchiveerd,
        start_time              as standaard_starttijd,
        end_time                as standaard_eindtijd,
        minimum_capacity        as min_capaciteit,
        maximum_capacity        as max_capaciteit,
        created_at              as aangemaakt_op,
        updated_at              as gewijzigd_op

    from bron

)

select * from definitief
