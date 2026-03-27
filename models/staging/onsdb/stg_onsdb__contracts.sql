with bron as (

    select * from {{ source('ons_plan_2', 'contracts') }}

),

definitief as (

    select
        objectId                as contract_id,
        employeeObjectId        as medewerker_id,
        contractTypeObjectId    as contracttype_id,
        beginDate               as startdatum,
        endDate                 as einddatum,
        fixedHoursPerWeek       as normtijd_uren_per_week,
        varHoursPerWeek         as variabele_uren_per_week,
        createdAt               as aangemaakt_op,
        updatedAt               as gewijzigd_op

    from bron

)

select * from definitief
