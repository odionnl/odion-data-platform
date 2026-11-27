with bron as (

    select * from {{ source('ons_plan_2', 'agenda_event_labels') }}

),

definitief as (

    select
        objectId                as afspraak_label_id,
        eventObjectId           as afspraak_id,
        labelObjectId           as label_id,
        createdAt               as aangemaakt_op,
        updatedAt               as gewijzigd_op

    from bron

)

select * from definitief
