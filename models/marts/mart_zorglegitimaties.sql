with legitimaties_met_producten as (

    select * from {{ ref('int_zorglegitimaties_met_producten') }}

),

clienten as (

    select * from {{ ref('stg_onsdb__clients') }}

),

teams as (

    select * from {{ ref('stg_onsdb__teams') }}

),

product_samenvatting as (

    select
        zorglegitimatie_id,
        count(distinct zorglegitimatieproduct_id)   as aantal_producten,
        sum(hoeveelheid_in_minuten)                 as totaal_minuten

    from legitimaties_met_producten
    where zorglegitimatieproduct_id is not null
    group by zorglegitimatie_id

),

zorglegitimaties as (

    select distinct
        zorglegitimatie_id,
        client_id,
        zorglegitimatie_type,
        legitimatienummer,
        legitimatie_startdatum,
        legitimatie_einddatum,
        team_id

    from legitimaties_met_producten

),

definitief as (

    select
        zorglegitimaties.zorglegitimatie_id,
        zorglegitimaties.zorglegitimatie_type,
        zorglegitimaties.legitimatienummer,
        zorglegitimaties.legitimatie_startdatum,
        zorglegitimaties.legitimatie_einddatum,

        -- Client
        zorglegitimaties.client_id,
        clienten.clientnaam                                       as client_naam,

        -- Team
        teams.teamnaam,

        -- Producten
        coalesce(product_samenvatting.aantal_producten, 0)  as aantal_producten,
        coalesce(product_samenvatting.totaal_minuten, 0)    as totaal_minuten

    from zorglegitimaties
    left join clienten
        on clienten.client_id = zorglegitimaties.client_id
    left join teams
        on teams.team_id = zorglegitimaties.team_id
    left join product_samenvatting
        on product_samenvatting.zorglegitimatie_id = zorglegitimaties.zorglegitimatie_id

)

select * from definitief
