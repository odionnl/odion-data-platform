-- Koppeling van documenten aan labels (tags).
-- Grain: één rij per document × label. Documenten zonder labels komen hier niet voor.

with definitief as (

    select
        dt.document_id,
        t.tag_id,
        t.label_naam

    from {{ ref('stg_onsdb__document_tags') }} dt
    inner join {{ ref('stg_onsdb__tags') }} t
        on t.tag_id = dt.tag_id

)

select * from definitief
