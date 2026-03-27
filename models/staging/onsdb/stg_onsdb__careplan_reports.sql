with bron as (

    select * from {{ source('ons_plan_2', 'careplan_reports') }}

),

definitief as (

    select
        objectId                as rapportage_id,
        clientObjectId          as client_id,
        employeeObjectId        as medewerker_id,
        reportTypeObjectId      as rapportage_type_id,
        -- Koppeling aan zorgplanregel: gebruik carePlanLinkId voor stabiele verwijzing bij kopiëren
        carePlanEntryObjectId   as zorgplanregel_id,
        carePlanLinkId          as zorgplanregel_link_id,
        reportingDate           as rapportagedatum,
        comment                 as opmerking,
        -- SOEP-velden (alleen gevuld bij SOEP-rapportages)
        subjective              as soep_subjectief,
        objective               as soep_objectief,
        assessment              as soep_evaluatie,
        [plan]                  as soep_plan,
        -- Status & zichtbaarheid
        status                  as status_code,
        cast(case when flagged = 1 then 1 else 0 end as bit) as is_gemarkeerd,
        cast(case when hidden  = 1 then 1 else 0 end as bit) as is_verborgen,
        hidingReason            as verbergingsreden,
        createdAt               as aangemaakt_op,
        updatedAt               as gewijzigd_op

    from bron

)

select * from definitief
