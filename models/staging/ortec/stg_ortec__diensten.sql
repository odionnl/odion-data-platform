with bron as (

    select * from {{ source('ortec', 'diensten') }}

),

definitief as (

    select
        shift_id            as dienst_id,
        shift_code          as dienstcode,
        start_time          as starttijd,
        end_time            as eindtijd,
        time_at_work        as tijd_op_werk,
        illness_time        as ziek_tijd,
        leave_time          as verlof_tijd,
        standby_time        as standby_tijd,
        employee_number     as personeelsnummer,
        employee_name       as medewerker_naam,
        cost_center_id      as kostenplaats_id,
        cost_center_name    as kostenplaats_naam,
        department_id       as afdeling_id,
        department_name     as afdeling_naam,
        roster_status       as roosterstatus

    from bron

)

select * from definitief
