with

source as (

    select * from {{ source('ons_plan_2', 'clients_addresses') }}

),

renamed as (

    select
        addressObjectId as adres_id,
        clientObjectId as client_id
    from source

)

select 
    adres_id,
    client_id
from renamed