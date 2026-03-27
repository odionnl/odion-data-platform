with bron as (

    select * from {{ source('ons_plan_2', 'document_tags') }}

),

definitief as (

    select
        documentObjectId    as document_id,
        tagObjectId         as tag_id

    from bron

)

select * from definitief
