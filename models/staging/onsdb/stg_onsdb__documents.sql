with bron as (

    select * from {{ source('ons_plan_2', 'documents') }}

),

definitief as (

    select
        objectId            as document_id,
        clientObjectId      as client_id,
        employeeObjectId    as medewerker_id,
        cast(description as nvarchar(max)) as beschrijving,
        filename            as bestandsnaam,
        status              as status_code,
        cast(case when confidential = 1 then 1 else 0 end as bit)
                            as is_vertrouwelijk,
        createdAt           as aangemaakt_op,
        updatedAt           as gewijzigd_op

    from bron

)

select * from definitief
