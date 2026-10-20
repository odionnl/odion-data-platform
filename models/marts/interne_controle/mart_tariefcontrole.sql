-- Tariefcontrole: gedeclareerd tarief per factuurregel vs. de tarievenlijsten (int_tarieven).
-- Eén rij per factuurregel (alle jaren; jaarselectie in Power BI op startdatum).
-- Koppeling op factuurtype (mart_facturen), vektiscode = prestatiecode, startdatum binnen de
-- geldigheid en (bij Wmo) debiteur. Testcliënten zijn al uitgesloten in mart_factuurregels.
-- Overige regelgegevens via factuurregel_id → mart_factuurregels.

with factuurregels as (

    select * from {{ ref('mart_factuurregels') }}

),

facturen as (

    select * from {{ ref('mart_facturen') }}

),

tarieven as (

    select * from {{ ref('int_tarieven') }}

),

regels as (

    select
        factuurregels.factuurregel_id,
        factuurregels.factuur_id,
        factuurregels.factuurnummer,
        facturen.factuur_type,
        facturen.factuurdatum,
        facturen.debiteurnummer,
        facturen.debiteur_naam,
        factuurregels.startdatum,
        factuurregels.einddatum,
        factuurregels.client_id,
        factuurregels.product_code,
        factuurregels.product_omschrijving,
        factuurregels.vektiscode,
        factuurregels.financieringstype_naam,
        factuurregels.aantal,
        factuurregels.tarief,
        factuurregels.tarief_eenheid,
        factuurregels.totaalbedrag,
        factuurregels.status

    from factuurregels
    left join facturen
        on facturen.factuur_id = factuurregels.factuur_id

),

-- Is er voor factuurtype + periode (+ debiteur) een tarievenlijst geladen?
tarievenlijsten as (

    select distinct factuur_type, debiteurnummer, geldig_vanaf, geldig_tot
    from tarieven

),

regels_met_lijst as (

    select distinct regels.factuurregel_id
    from regels
    inner join tarievenlijsten lijsten
        on lijsten.factuur_type collate database_default
         = regels.factuur_type collate database_default
        and cast(regels.startdatum as date) between lijsten.geldig_vanaf and lijsten.geldig_tot
        and (lijsten.debiteurnummer is null
            or lijsten.debiteurnummer collate database_default
             = cast(regels.debiteurnummer as nvarchar(50)) collate database_default)

),

-- Debiteur-specifiek tarief (Wmo) gaat voor op een algemeen tarief
tariefmatch as (

    select
        regels.factuurregel_id,
        tarieven.prestatienaam,
        tarieven.tarief,
        tarieven.tijdseenheid,
        tarieven.tariefbestand,
        row_number() over (
            partition by regels.factuurregel_id
            order by case when tarieven.debiteurnummer is null then 1 else 0 end
        ) as volgorde

    from regels
    inner join tarieven
        on tarieven.factuur_type collate database_default
         = regels.factuur_type collate database_default
        and tarieven.prestatiecode collate database_default
          = regels.vektiscode collate database_default
        and cast(regels.startdatum as date) between tarieven.geldig_vanaf and tarieven.geldig_tot
        and (tarieven.debiteurnummer is null
            or tarieven.debiteurnummer collate database_default
             = cast(regels.debiteurnummer as nvarchar(50)) collate database_default)

),

definitief as (

    select
        regels.*,

        -- Tarievenlijst
        tariefmatch.tariefbestand,
        tariefmatch.prestatienaam,
        tariefmatch.tarief                                              as verwacht_tarief,
        tariefmatch.tijdseenheid                                        as verwachte_tijdseenheid,

        -- Controle
        regels.tarief - tariefmatch.tarief                              as tariefverschil,
        regels.aantal * (regels.tarief - tariefmatch.tarief)            as bedragsverschil,
        case
            when regels_met_lijst.factuurregel_id is null
            then 'Geen tarievenlijst'
            when tariefmatch.factuurregel_id is null
            then 'Niet in tarievenlijst'
            when tariefmatch.tarief is null
            then 'Geen vast tarief'
            when abs(regels.tarief - tariefmatch.tarief) >= 0.01
            then 'Afwijkend tarief'
            else 'Akkoord'
        end                                                             as tariefcontrole

    from regels
    left join regels_met_lijst
        on regels_met_lijst.factuurregel_id = regels.factuurregel_id
    left join tariefmatch
        on tariefmatch.factuurregel_id = regels.factuurregel_id
        and tariefmatch.volgorde = 1

)

select * from definitief
