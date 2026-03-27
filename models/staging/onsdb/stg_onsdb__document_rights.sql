with bron as (

    select * from {{ source('ons_plan_2', 'document_rights') }}

),

definitief as (

    select
        documentObjectId    as document_id,
        educationObjectId   as deskundigheid_id

    from bron

)

select * from definitief
