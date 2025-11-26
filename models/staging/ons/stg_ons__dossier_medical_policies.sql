with

source as (

    select * from {{ source('ons_plan_2', 'dossier_medical_policies') }}

),

renamed as (

    select
        clientObjectId              as client_id,
        employeeObjectId            as werknemer_id,
        snomedExpressionValue       as snomed_expressie,
        documentName                as document_naam,
        narrative                   as document_beschrijving,
        cast(createdAt as datetime) as aangemaakt_op,
        cast(updatedAt as datetime) as bewerkt_op,
        cast([date] as date)        as document_datum
    from source

)

select 
    client_id,
    werknemer_id,
    snomed_expressie,
    document_naam,
    document_beschrijving,
    aangemaakt_op,
    bewerkt_op,
    document_datum
from renamed