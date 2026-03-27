-- Breed episodeoverzicht met clientnaam, aanmaakinfo, afscherming per deskundigheid/-groep.
-- STRING_AGG voor afgeschermde deskundigheden en deskundigheidsgroepen gebeurt hier in de mart.
-- Grain: één rij per episode.

with episodes as (

    select * from {{ ref('stg_onsdb__dossier_episodes') }}

),

rechten as (

    select * from {{ ref('int_episodes_met_rechten') }}

),

episode_deskundigheden_agg as (

    -- Deskundigheden waarvoor de episode afgeschermd is
    select
        dda.episode_id                                   as episode_id,
        string_agg(d.deskundigheid_naam, ' | ')
            within group (order by d.deskundigheid_naam)        as afgeschermd_voor_deskundigheden

    from (select distinct episode_id, deskundigheid_id from {{ ref('stg_onsdb__dossier_deskundigheid_autorisaties') }}) dda
    inner join {{ ref('stg_onsdb__deskundigheden') }} d
        on d.deskundigheid_id = dda.deskundigheid_id

    group by dda.episode_id

),

episode_deskundigheidsgroepen_agg as (

    -- Deskundigheidsgroepen waarvoor de episode afgeschermd is
    select
        ddga.episode_id                                  as episode_id,
        string_agg(g.deskundigheidsgroep_naam, ' | ')
            within group (order by g.deskundigheidsgroep_naam)  as afgeschermd_voor_deskundigheidsgroepen

    from (select distinct episode_id, deskundigheidsgroep_id from {{ ref('stg_onsdb__dossier_deskundigheidsgroep_autorisaties') }}) ddga
    inner join {{ ref('stg_onsdb__deskundigheidsgroepen') }} g
        on g.deskundigheidsgroep_id = ddga.deskundigheidsgroep_id

    group by ddga.episode_id

),

medewerker_deskundigheden_agg as (

    -- Deskundigheden van de medewerker die de episode heeft aangemaakt (aangemaakt_door_id)
    select
        k.medewerker_id                                  as medewerker_id,
        string_agg(d.deskundigheid_naam, ' | ')
            within group (order by d.deskundigheid_naam)        as medewerker_deskundigheden

    from (
        select distinct medewerker_id, deskundigheid_id
        from {{ ref('stg_onsdb__deskundigheid_koppelingen') }}
    ) k
    inner join {{ ref('stg_onsdb__deskundigheden') }} d
        on d.deskundigheid_id = k.deskundigheid_id
        and d.is_zichtbaar = 1

    group by k.medewerker_id

),

medewerker_deskundigheidsgroepen_agg as (

    -- Deskundigheidsgroepen van de medewerker die de episode heeft aangemaakt (aangemaakt_door_id)
    select
        k.medewerker_id                                  as medewerker_id,
        string_agg(g.deskundigheidsgroep_naam, ' | ')
            within group (order by g.deskundigheidsgroep_naam)  as medewerker_deskundigheidsgroepen

    from (
        select distinct medewerker_id, deskundigheid_id
        from {{ ref('stg_onsdb__deskundigheid_koppelingen') }}
    ) k
    inner join {{ ref('stg_onsdb__deskundigheden') }} d
        on d.deskundigheid_id = k.deskundigheid_id
        and d.is_zichtbaar = 1
    inner join {{ ref('stg_onsdb__deskundigheidsgroep_deskundigheden') }} gd
        on gd.deskundigheid_id = d.deskundigheid_id
    inner join {{ ref('stg_onsdb__deskundigheidsgroepen') }} g
        on g.deskundigheidsgroep_id = gd.deskundigheidsgroep_id

    group by k.medewerker_id

),

clienten as (

    select
        client_id,
        clientnummer,
        clientnaam

    from {{ ref('stg_onsdb__clients') }}

),

medewerkers as (

    select
        medewerker_id,
        personeelsnummer,
        voornaam,
        achternaam

    from {{ ref('stg_onsdb__employees') }}

),

definitief as (

    select
        e.episode_id,

        -- Client
        e.client_id,
        c.clientnummer,
        c.clientnaam                                              as client_naam,

        -- Episode-metadata
        e.titel,
        e.startdatum,
        e.einddatum,
        e.evaluatiedatum,
        e.doel,
        e.is_gemarkeerd,
        e.is_belangrijk,

        -- Toegangsbeperking
        r.afgeschermd_voor,
        eda.afgeschermd_voor_deskundigheden,
        edga.afgeschermd_voor_deskundigheidsgroepen,

        -- Aanmaak/wijzigingsinformatie
        e.aangemaakt_door_id                                    as medewerker_id,
        ma.personeelsnummer                                    as medewerker_personeelsnummer,
        concat(ma.voornaam, ' ', ma.achternaam)                as medewerker_naam,
        mda.medewerker_deskundigheden,
        mga.medewerker_deskundigheidsgroepen,
        e.gewijzigd_door_id,
        mw.personeelsnummer                                    as gewijzigd_door_personeelsnummer,
        concat(mw.voornaam, ' ', mw.achternaam)                as gewijzigd_door_naam,
        e.aangemaakt_op,
        e.gewijzigd_op,

        -- Deep-link naar episodes in ONS dossier
        {{ ons_dossier_url('episodes', 'e.client_id') }}       as url_ons_episodes

    from episodes e
    left join rechten r
        on r.episode_id = e.episode_id
    left join episode_deskundigheden_agg eda
        on eda.episode_id = e.episode_id
    left join episode_deskundigheidsgroepen_agg edga
        on edga.episode_id = e.episode_id
    left join clienten c
        on c.client_id = e.client_id
    left join medewerkers ma
        on ma.medewerker_id = e.aangemaakt_door_id
    left join medewerker_deskundigheden_agg mda
        on mda.medewerker_id = e.aangemaakt_door_id
    left join medewerker_deskundigheidsgroepen_agg mga
        on mga.medewerker_id = e.aangemaakt_door_id
    left join medewerkers mw
        on mw.medewerker_id = e.gewijzigd_door_id

)

select * from definitief
