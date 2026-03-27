with bron as (

    select * from {{ source('ons_plan_2', 'document_expertise_groups') }}

),

definitief as (

    select
        documentObjectId        as document_id,
        expertiseGroupObjectId  as deskundigheidsgroep_id

    from bron

)

select * from definitief
