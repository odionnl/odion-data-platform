with

source as (

    select * from {{ source('ortec', 'dim_time') }}

),

renamed as (

    select
        TIME_KEY as tijdstip_id,
        TIME24 as tijdstip
    from source

)

select 
    tijdstip_id,
    tijdstip
from renamed