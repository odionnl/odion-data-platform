with bron as (

    select * from {{ source('ortec', 'diensten') }}

),

definitief as (

    select
        start_time          as starttijd,
        end_time            as eindtijd,
        employee_id         as medewerker_id,
        employee_name       as medewerker_naam,
        cost_center_id      as kostenplaats_id,
        cost_center_name    as kostenplaats_naam

    from bron

)

select * from definitief
