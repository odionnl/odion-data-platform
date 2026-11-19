-- Graadmeters cliënten per maand-ultimo voor de P&C-maandrapportage.
-- Grain: één rij per maand. Bevat alleen aantallen, geen cliëntgegevens (dataminimalisatie).
-- Peildatum = laatste dag van de maand; voor de lopende maand is dat vandaag.
-- Definities sluiten aan op mart_clienten (in zorg) en mart_zorglegitimaties (actief).

with testclienten as (

    select * from {{ ref('int_testclienten') }}

),

maanden as (

    select distinct
        maand_start,
        jaar_maand,
        jaar_maand_sortering,
        case
            when eomonth(maand_start) > cast(getdate() as date) then cast(getdate() as date)
            else eomonth(maand_start)
        end as peildatum

    from {{ ref('mart_datums') }}
    where maand_start >= cast('2024-01-01' as date)
      and maand_start <= cast(getdate() as date)

),

zorgtoewijzingen as (

    select * from {{ ref('stg_onsdb__care_allocations') }}
    where client_id not in (select client_id from testclienten)

),

zorglegitimaties as (

    select * from {{ ref('stg_onsdb__care_orders') }}
    where client_id not in (select client_id from testclienten)

),

clienten_in_zorg as (

    -- Cliënt is in zorg als er op de peildatum een actieve zorgtoewijzing is
    select distinct
        maanden.peildatum,
        zorgtoewijzingen.client_id

    from maanden
    inner join zorgtoewijzingen
        on zorgtoewijzingen.startdatum <= maanden.peildatum
       and (zorgtoewijzingen.einddatum is null or zorgtoewijzingen.einddatum > maanden.peildatum)

),

clienten_met_legitimatie as (

    -- Cliënt in zorg met minstens één actieve zorglegitimatie op de peildatum
    select distinct
        clienten_in_zorg.peildatum,
        clienten_in_zorg.client_id

    from clienten_in_zorg
    inner join zorglegitimaties
        on zorglegitimaties.client_id = clienten_in_zorg.client_id
       and zorglegitimaties.startdatum <= clienten_in_zorg.peildatum
       and (zorglegitimaties.einddatum is null or zorglegitimaties.einddatum > clienten_in_zorg.peildatum)

),

in_zorg_per_maand as (

    select peildatum, count(*) as aantal_clienten_in_zorg
    from clienten_in_zorg
    group by peildatum

),

legitimatie_per_maand as (

    select peildatum, count(*) as aantal_clienten_met_legitimatie
    from clienten_met_legitimatie
    group by peildatum

),

definitief as (

    select
        maanden.maand_start,
        maanden.jaar_maand,
        maanden.jaar_maand_sortering,
        maanden.peildatum,
        coalesce(in_zorg_per_maand.aantal_clienten_in_zorg, 0)                as aantal_clienten_in_zorg,
        coalesce(legitimatie_per_maand.aantal_clienten_met_legitimatie, 0)    as aantal_clienten_met_legitimatie,
        coalesce(in_zorg_per_maand.aantal_clienten_in_zorg, 0)
            - coalesce(legitimatie_per_maand.aantal_clienten_met_legitimatie, 0) as aantal_clienten_zonder_legitimatie

    from maanden
    left join in_zorg_per_maand
        on in_zorg_per_maand.peildatum = maanden.peildatum
    left join legitimatie_per_maand
        on legitimatie_per_maand.peildatum = maanden.peildatum

)

select * from definitief
