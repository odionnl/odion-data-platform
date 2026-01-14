with

source as (

    select * from {{ source('ons_plan_2', 'careplan_reports') }}

),

renamed as (

    select
        objectId as rapportage_id,
        careplanEntryObjectId as zorgplanregel_id,
        clientObjectId as client_id,
        employeeObjectId as werknemer_id,
        reportingDate as rapportage_datum,
        reportTypeObjectId as rapportage_type_id,
        comment as opmerking,
        status as status_code

    from source

)

select 
    rapportage_id,
    zorgplanregel_id,
    client_id,
    werknemer_id,
    rapportage_datum,
    rapportage_type_id,
    opmerking,
    status_code
from renamed