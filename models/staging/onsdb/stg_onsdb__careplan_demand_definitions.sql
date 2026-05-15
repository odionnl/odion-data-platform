with bron as (

    select * from {{ source('ons_plan_2', 'careplan_demand_definitions') }}

),

definitief as (

    select
        objectId            as aandachtspuntdefinitie_id,
        domainObjectId      as domein_definitie_id,
        ltrim(rtrim(cast([name] as nvarchar(500))))    as aandachtspunt_naam,
        hidden              as is_verborgen,
        classificationId    as classificatie_id,
        createdAt           as aangemaakt_op,
        updatedAt           as gewijzigd_op

    from bron

)

select * from definitief
