-- Factuurregels (historisch + actueel) t.b.v. tariefcontrole.
-- Eén rij per factuurregel. Bedragen omgerekend van centen naar euro's.
-- Testcliënten (int_testclienten) zijn uitgesloten.

with testclienten as (

    select client_id from {{ ref('int_testclienten') }}

),

factuurregels as (

    select * from {{ ref('stg_onsdb__invoice_rows') }}
    where client_id not in (select client_id from testclienten)

),

facturen as (

    select * from {{ ref('stg_onsdb__invoices') }}

),

clienten as (

    select * from {{ ref('stg_onsdb__clients') }}

),

productdefinities as (

    select * from {{ ref('stg_onsdb__products') }}

),

financieringstypen as (

    select * from {{ ref('stg_onsdb__finance_types') }}

),

definitief as (

    select
        regels.factuurregel_id,
        regels.factuur_id,
        regels.vorige_factuurregel_id,
        facturen.factuurnummer,

        -- Periode
        regels.startdatum,
        regels.einddatum,

        -- Client
        regels.client_id,
        clienten.clientnummer,

        -- Product
        regels.product_id,
        productdefinities.product_code,
        productdefinities.product_omschrijving,
        financieringstypen.financieringstype_naam,
        regels.zorglegitimatie_product_id,

        -- Aantal en tarief
        regels.aantal,
        regels.eenheid_code,
        cast(regels.tarief_in_centen as decimal(18, 2)) / 100           as tarief,
        regels.tarief_eenheid_code,
        cast(regels.totaalbedrag_in_centen as decimal(18, 2)) / 100     as totaalbedrag,
        cast(regels.berekend_bedrag_in_centen as decimal(18, 2)) / 100  as berekend_bedrag,
        cast(regels.toegekend_bedrag_in_centen as decimal(18, 2)) / 100 as toegekend_bedrag,

        -- Status: afkeur op factuurniveau gaat voor; zonder verwerkt retourbericht
        -- en zonder goedkeuring is de regel nog niet beoordeeld.
        case
            when facturen.is_afgekeurd = 1
                or (regels.is_goedgekeurd = 0 and facturen.retour_verwerkt_op is not null)
            then 'Afgekeurd'
            when regels.is_goedgekeurd = 1
            then 'Goedgekeurd'
            else 'Nog niet beoordeeld'
        end                                                             as status,
        cast(regels.retourmelding as nvarchar(4000))                    as retourmelding,
        facturen.retour_verwerkt_op,

        -- Timestamps
        regels.aangemaakt_op,
        regels.gewijzigd_op

    from factuurregels regels
    left join facturen
        on facturen.factuur_id = regels.factuur_id
    left join clienten
        on clienten.client_id = regels.client_id
    left join productdefinities
        on productdefinities.product_id = regels.product_id
    left join financieringstypen
        on financieringstypen.financieringstype_id = productdefinities.financieringstype_id

)

select * from definitief
