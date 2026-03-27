with bron as (

    select * from {{ source('ons_plan_2', 'clients_addresses') }}

),

definitief as (

    select
        addressObjectId     as adres_id,
        clientObjectId      as client_id

    from bron

)

select * from definitief
