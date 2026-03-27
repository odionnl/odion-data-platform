with bron as (

    select * from {{ source('ons_plan_2', 'activities') }}

),

definitief as (

    select
        objectId                as activiteit_id,
        description             as beschrijving,
        identificationNo        as identificatienummer,
        activityGroupObjectId   as activiteitgroep_id,
        active                  as is_actief,
        payrolling              as is_werktijd,
        direct                  as is_direct,
        isTravelTime            as is_reistijd,
        activityType            as activiteit_type,
        isGroupCare             as is_groepszorg

    from bron

)

select * from definitief
