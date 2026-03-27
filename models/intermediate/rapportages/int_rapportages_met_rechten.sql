-- Toegangsrechtclassificatie per rapportage op basis van deskundigheden en deskundigheidsgroepen.
-- Alleen rechten van het type 'Zichtbaar voor' worden meegenomen.
-- afgeschermd_voor wordt bepaald via EXISTS (geen joins, geen aggregatie).
-- Grain: één rij per rapportage.

with rapportages as (

    select rapportage_id
    from {{ ref('stg_onsdb__careplan_reports') }}

),

definitief as (

    select
        r.rapportage_id,
        case
            when exists (
                select 1
                from {{ ref('stg_onsdb__rapportage_rechten') }} rr
                inner join {{ ref('stg_onsdb__lst_rapportage_recht_typen') }} rt
                    on rt.type_code = rr.type_code
                   and rt.type_omschrijving = 'Zichtbaar voor'
                where rr.rapportage_id = r.rapportage_id
                  and rr.deskundigheid_id is not null
            ) and exists (
                select 1
                from {{ ref('stg_onsdb__rapportage_rechten') }} rr
                inner join {{ ref('stg_onsdb__lst_rapportage_recht_typen') }} rt
                    on rt.type_code = rr.type_code
                   and rt.type_omschrijving = 'Zichtbaar voor'
                where rr.rapportage_id = r.rapportage_id
                  and rr.deskundigheidsgroep_id is not null
            ) then 'beiden'
            when exists (
                select 1
                from {{ ref('stg_onsdb__rapportage_rechten') }} rr
                inner join {{ ref('stg_onsdb__lst_rapportage_recht_typen') }} rt
                    on rt.type_code = rr.type_code
                   and rt.type_omschrijving = 'Zichtbaar voor'
                where rr.rapportage_id = r.rapportage_id
                  and rr.deskundigheid_id is not null
            ) then 'deskundigheid'
            when exists (
                select 1
                from {{ ref('stg_onsdb__rapportage_rechten') }} rr
                inner join {{ ref('stg_onsdb__lst_rapportage_recht_typen') }} rt
                    on rt.type_code = rr.type_code
                   and rt.type_omschrijving = 'Zichtbaar voor'
                where rr.rapportage_id = r.rapportage_id
                  and rr.deskundigheidsgroep_id is not null
            ) then 'deskundigheidsgroep'
            else 'geen'
        end as afgeschermd_voor

    from rapportages r

)

select * from definitief
