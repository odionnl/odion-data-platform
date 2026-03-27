-- Toegangsrechtclassificatie per document op basis van deskundigheden en deskundigheidsgroepen.
-- afgeschermd_voor wordt bepaald via EXISTS (geen joins, geen aggregatie).
-- Grain: één rij per document.

with documenten as (

    select document_id
    from {{ ref('stg_onsdb__documents') }}

),

definitief as (

    select
        d.document_id,
        case
            when exists (
                select 1 from {{ ref('stg_onsdb__document_deskundigheidsgroepen') }}
                where document_id = d.document_id
            ) and exists (
                select 1 from {{ ref('stg_onsdb__document_rights') }}
                where document_id = d.document_id
            ) then 'beiden'
            when exists (
                select 1 from {{ ref('stg_onsdb__document_rights') }}
                where document_id = d.document_id
            ) then 'deskundigheid'
            when exists (
                select 1 from {{ ref('stg_onsdb__document_deskundigheidsgroepen') }}
                where document_id = d.document_id
            ) then 'deskundigheidsgroep'
            else 'geen'
        end as afgeschermd_voor

    from documenten d

)

select * from definitief
