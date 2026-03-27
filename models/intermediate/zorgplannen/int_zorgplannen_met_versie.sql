with zorgplannen as (

    select * from {{ ref('stg_onsdb__careplans') }}

),

statussen as (

    select * from {{ ref('stg_onsdb__lst_care_plan_statuses') }}

),

regels_agg as (

    select
        zorgplan_id,
        count(*)                                                            as aantal_regels,
        count(case when domein_versie in ('Nieuw', 'Oud') then 1 end)      as aantal_geldige_regels,
        min(case when domein_versie in ('Nieuw', 'Oud') then domein_versie end) as min_versie,
        max(case when domein_versie in ('Nieuw', 'Oud') then domein_versie end) as max_versie

    from {{ ref('int_zorgplanregels_met_domeinen') }}
    group by zorgplan_id

),

definitief as (

    select
        z.zorgplan_id,
        z.client_id,
        z.aangemaakt_door_id,
        z.startdatum,
        z.einddatum,
        z.status_code,
        s.status_omschrijving,
        case
            when z.startdatum > getdate()                                        then 'Nog niet gestart'
            when z.startdatum <= getdate()
                 and (z.einddatum is null or z.einddatum > getdate())            then 'Geldig'
            else 'Verlopen'
        end                                                                     as geldigheid,
        case
            when ra.zorgplan_id is null or ra.aantal_geldige_regels = 0         then 'Leeg'
            when ra.min_versie = ra.max_versie                                   then ra.min_versie
            else 'Mix'
        end                                                                     as zorgplan_versie,
        coalesce(ra.aantal_regels, 0)                                           as aantal_regels,
        coalesce(ra.aantal_geldige_regels, 0)                                   as aantal_geldige_regels,
        z.aangemaakt_op,
        z.gewijzigd_op

    from zorgplannen z
    left join statussen s
        on s.status_code = z.status_code
    left join regels_agg ra
        on ra.zorgplan_id = z.zorgplan_id

)

select * from definitief
