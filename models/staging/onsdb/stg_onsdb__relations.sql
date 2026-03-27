with bron as (

    select * from {{ source('ons_plan_2', 'relations') }}

),

definitief as (

    select
        objectId                        as relatie_id,
        clientObjectId                  as client_id,
        personalRelationTypeId          as persoonlijk_relatietype_id,
        clientContactRelationTypeId     as contactpersoon_relatietype_id,
        name                            as naam,
        gender                          as geslacht,
        inCaseOfEmergency               as is_noodcontact,
        organization                    as organisatie,
        createdAt                       as aangemaakt_op,
        updatedAt                       as gewijzigd_op

    from bron

)

select * from definitief
