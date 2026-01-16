with

source as (

    select * from {{ source('ortec', 'dim_date') }}

),

renamed as (

    select
        DATE_KEY as datum_id,
        FULL_DATE as volledige_datum
    from source

)

select 
    datum_id,
    volledige_datum
from renamed