with

source as (

    select * from {{ source('ons_plan_2', 'care_orders') }}

),

renamed as (

    select
        objectId as legitimatie_id,
        careOrderType as legitimatie_type,
        clientObjectId as client_id,
        id as legitimatie_nummer,
        beginDateClipped as startdatum_legitimatie,
        coalesce(endDateClipped, cast('9999-12-31' as date)) as einddatum_legitimatie,
        financeTypeObjectId as finance_type_id
    from source

)

select 
    legitimatie_id,
    legitimatie_type,
    client_id,
    legitimatie_nummer,
    startdatum_legitimatie,
    einddatum_legitimatie,
    finance_type_id
from renamed