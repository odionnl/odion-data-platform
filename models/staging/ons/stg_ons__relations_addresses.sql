with

source as (

    select * from {{ source('ons_plan_2', 'relations_addresses') }}

),

renamed as (

    select
        relationObjectId as relatie_id,
        addressObjectId as adres_id
    from source

)

select 
    relatie_id,
    adres_id
from renamed