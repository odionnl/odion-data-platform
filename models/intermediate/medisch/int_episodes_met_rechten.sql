-- Toegangsrechtclassificatie per episode op basis van deskundigheden en deskundigheidsgroepen.
-- afgeschermd_voor wordt bepaald via EXISTS (geen joins, geen aggregatie).
-- Grain: één rij per episode.

with episodes as (

    select episode_id
    from {{ ref('stg_onsdb__dossier_episodes') }}

),

definitief as (

    select
        e.episode_id,
        case
            when exists (
                select 1 from {{ ref('stg_onsdb__dossier_deskundigheidsgroep_autorisaties') }}
                where episode_id = e.episode_id
            ) and exists (
                select 1 from {{ ref('stg_onsdb__dossier_deskundigheid_autorisaties') }}
                where episode_id = e.episode_id
            ) then 'beiden'
            when exists (
                select 1 from {{ ref('stg_onsdb__dossier_deskundigheid_autorisaties') }}
                where episode_id = e.episode_id
            ) then 'deskundigheid'
            when exists (
                select 1 from {{ ref('stg_onsdb__dossier_deskundigheidsgroep_autorisaties') }}
                where episode_id = e.episode_id
            ) then 'deskundigheidsgroep'
            else 'geen'
        end as afgeschermd_voor

    from episodes e

)

select * from definitief
