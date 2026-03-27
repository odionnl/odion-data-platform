with bron as (

    select * from {{ source('ons_plan_2', 'careplan_domain_definitions') }}

),

definitief as (

    select
        objectId            as domein_definitie_id,
        name                as domein_naam,
        hidden              as is_oud_domein,
        classificationId    as classificatie_id,
        createdAt           as aangemaakt_op,
        updatedAt           as gewijzigd_op

    from bron

)

select * from definitief
