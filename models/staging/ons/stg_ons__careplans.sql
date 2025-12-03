with

source as (

    select * from {{ source('ons_plan_2', 'careplans') }}

),

renamed as (

    select
        objectId as zorgplan_id,
        clientObjectId as client_id,
        employeeObjectId as werknemer_id,
        [status] as zorgplan_status_code,
        beginDate as startdatum_zorgplan,
        coalesce(endDate, cast('9999-12-31' as date)) as einddatum_zorgplan,
        createdAt as aangemaakt_op,
        updatedAt as bewerkt_op

    from source

)

select 
    zorgplan_id,
    client_id,
    werknemer_id,
    zorgplan_status_code,
    startdatum_zorgplan,
    einddatum_zorgplan,
    aangemaakt_op,
    bewerkt_op
from renamed