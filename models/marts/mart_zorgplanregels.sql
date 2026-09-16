-- Brede zorgplanregels-mart: één rij per zorgplanregel met alle context
-- (domein, doel, aandachtspunt, zorgplan-status/versie, client).
-- Grain: één rij per zorgplanregel. Geen datumfilter (historisch + actueel).

with regels as (

    select * from {{ ref('stg_onsdb__careplan_entries') }}

),

zorgplannen as (

    select * from {{ ref('int_zorgplannen_met_versie') }}

),

doeldefinities as (

    select * from {{ ref('stg_onsdb__careplan_target_definitions') }}

),

domeindefinities as (

    select * from {{ ref('stg_onsdb__careplan_domain_definitions') }}

),

aandachtspuntdefinities as (

    select * from {{ ref('stg_onsdb__careplan_demand_definitions') }}

),

clienten as (

    select
        client_id,
        clientnummer,
        clientnaam
    from {{ ref('stg_onsdb__clients') }}

)

select
    -- Regel
    r.zorgplanregel_id,
    r.zorgplanregel_link_id,

    -- Zorgplan-context (via int_zorgplannen_met_versie)
    r.zorgplan_id,
    z.status_omschrijving   as zorgplan_status,
    z.zorgplan_versie       as zorgplan_versie_plan,
    z.geldigheid            as zorgplan_geldigheid,
    z.startdatum            as startdatum_zorgplan,
    z.einddatum             as einddatum_zorgplan,

    -- Client
    z.client_id,
    cli.clientnummer,
    cli.clientnaam          as client_naam,

    -- Domein
    dom.domein_definitie_id,
    cast(dom.domein_naam as nvarchar(500))      as domein_naam,
    case
        when dom.is_oud_domein = 1 then 'Oud'
        when dom.is_oud_domein = 0 then 'Nieuw'
        else 'Onbekend'
    end                                          as domein_versie,

    -- Aandachtspunt (alle records; filter op is_verborgen_aandachtspunt = 0
    -- voor alleen Odion's actuele categorieën Thuis/Daginvulling/Mijn verhaal)
    r.aandachtspuntdefinitie_id,
    aandacht.aandachtspunt_naam,
    aandacht.is_verborgen                        as is_verborgen_aandachtspunt,

    -- Doel
    r.doeldefinitie_id,
    cast(doel.doel_naam as nvarchar(500))        as doel_naam,
    cast(r.doel_titel as nvarchar(2000))         as doel_titel,
    cast(r.doel_opmerking as nvarchar(4000))     as doel_opmerking,
    r.streefdatum,
    r.percentage_gerealiseerd,

    -- Tijdstempels
    r.aangemaakt_op,
    r.gewijzigd_op

from regels r
left join zorgplannen z
    on z.zorgplan_id = r.zorgplan_id
left join clienten cli
    on cli.client_id = z.client_id
left join doeldefinities doel
    on doel.doeldefinitie_id = r.doeldefinitie_id
left join domeindefinities dom
    on dom.domein_definitie_id = doel.domein_definitie_id
left join aandachtspuntdefinities aandacht
    on aandacht.aandachtspuntdefinitie_id = r.aandachtspuntdefinitie_id
