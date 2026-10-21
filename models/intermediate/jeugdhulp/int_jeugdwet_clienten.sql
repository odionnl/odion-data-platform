-- Cliënten met minstens één zorglegitimatie met financieringstype 'Jeugdwet'.
-- Testcliënten (int_testclienten) zijn uitgesloten.
-- Grain: één rij per client (client_id).

with zorglegitimaties as (

    select * from {{ ref('stg_onsdb__care_orders') }}

),

financieringstypes as (

    select * from {{ ref('stg_onsdb__finance_types') }}

),

testclienten as (

    select * from {{ ref('int_testclienten') }}

),

jeugdwet_legitimaties as (

    select
        zorglegitimaties.client_id,
        cast(zorglegitimaties.startdatum as date)   as startdatum,
        cast(zorglegitimaties.einddatum as date)    as einddatum

    from zorglegitimaties
    inner join financieringstypes
        on financieringstypes.financieringstype_id = zorglegitimaties.financieringstype_id
    where financieringstypes.financieringstype_naam = 'Jeugdwet'
      and zorglegitimaties.client_id not in (select client_id from testclienten)
      -- Geclipte einddatum vóór startdatum = volledig overlapt door een andere legitimatie
      and (zorglegitimaties.einddatum is null or zorglegitimaties.einddatum >= zorglegitimaties.startdatum)

),

definitief as (

    select
        client_id,
        min(startdatum) as eerste_startdatum_beschikking,
        case
            when count(*) > count(einddatum) then null
            else max(einddatum)
        end as laatste_einddatum_beschikking,
        max(
            case
                when startdatum <= cast(getdate() as date)
                 and (einddatum is null or einddatum >= cast(getdate() as date))
                then 1 else 0
            end
        ) as heeft_actieve_beschikking

    from jeugdwet_legitimaties
    group by client_id

)

select * from definitief
