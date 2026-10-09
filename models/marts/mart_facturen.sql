-- Facturen (historisch + actueel) met geaggregeerde regelinformatie.
-- Eén rij per factuur. Regels van testcliënten tellen niet mee
-- (via mart_factuurregels). Bedragen in euro's.

with facturen as (

    select * from {{ ref('stg_onsdb__invoices') }}

),

factuurregels as (

    select * from {{ ref('mart_factuurregels') }}

),

regels_per_factuur as (

    select
        factuur_id,
        count(*)                                                        as aantal_regels,
        sum(case when status = 'Goedgekeurd' then 1 else 0 end)         as aantal_regels_goedgekeurd,
        sum(case when status = 'Afgekeurd' then 1 else 0 end)           as aantal_regels_afgekeurd,
        sum(case when status = 'Nog niet beoordeeld' then 1 else 0 end) as aantal_regels_niet_beoordeeld,
        sum(totaalbedrag)                                               as totaalbedrag,
        sum(toegekend_bedrag)                                           as toegekend_bedrag

    from factuurregels
    group by factuur_id

),

definitief as (

    select
        facturen.factuur_id,
        facturen.factuurnummer,
        facturen.factuur_omschrijving,
        facturen.factuur_type_code,
        facturen.debiteur_id,
        facturen.zorgaanbieder_code,

        -- Datums
        facturen.factuurdatum,
        facturen.startdatum,
        facturen.einddatum,
        facturen.boekdatum,

        -- Status
        facturen.is_geexporteerd,
        facturen.is_afgekeurd,
        facturen.retour_verwerkt_op,
        cast(facturen.retourmelding as nvarchar(4000))      as retourmelding,

        -- Regels
        coalesce(regels.aantal_regels, 0)                   as aantal_regels,
        coalesce(regels.aantal_regels_goedgekeurd, 0)       as aantal_regels_goedgekeurd,
        coalesce(regels.aantal_regels_afgekeurd, 0)         as aantal_regels_afgekeurd,
        coalesce(regels.aantal_regels_niet_beoordeeld, 0)   as aantal_regels_niet_beoordeeld,
        regels.totaalbedrag,
        regels.toegekend_bedrag,

        -- Timestamps
        facturen.aangemaakt_op,
        facturen.gewijzigd_op

    from facturen
    left join regels_per_factuur regels
        on regels.factuur_id = facturen.factuur_id

)

select * from definitief
