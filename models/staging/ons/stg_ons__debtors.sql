with

source as (

    select * from {{ source('ons_plan_2', 'debtors') }}

),

renamed as (

    select
        objectId as debiteur_id,
        debtorNumber as debiteur_nummer,
        beginDate as startdatum_debiteur,
        endDate as einddatum_debiteur,
        organizationName as organisatienaam
    from source

)

select 
    debiteur_id,
    debiteur_nummer,
    startdatum_debiteur,
    einddatum_debiteur,
    organisatienaam
from renamed