with bron as (

    select * from {{ source('ons_plan_2', 'agenda_labels') }}

),

definitief as (

    select
        objectId                as label_id,
        [name]                  as labelnaam,
        icon                    as icoon,
        priority                as prioriteit,
        validFrom               as startdatum,
        validTo                 as einddatum,
        createdAt               as aangemaakt_op,
        updatedAt               as gewijzigd_op

    from bron

)

select * from definitief
