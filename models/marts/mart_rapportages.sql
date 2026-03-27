-- Breed rapportageoverzicht met clientnaam, medewerkersnaam, rapportage-type en toegangsrechten.
-- STRING_AGG voor deskundigheden en deskundigheidsgroepen gebeurt hier in de mart.
-- Grain: één rij per rapportage.

with rapportages as (

    select * from {{ ref('stg_onsdb__careplan_reports') }}

),

rechten as (

    select * from {{ ref('int_rapportages_met_rechten') }}

),

rapportage_deskundigheden_agg as (

    -- Deskundigheden waarvoor de rapportage afgeschermd is (alleen 'Zichtbaar voor'-rechten)
    select
        rr.rapportage_id,
        string_agg(d.deskundigheid_naam, ' | ')
            within group (order by d.deskundigheid_naam)        as afgeschermd_voor_deskundigheden

    from (
        select distinct rr2.rapportage_id, rr2.deskundigheid_id
        from {{ ref('stg_onsdb__rapportage_rechten') }} rr2
        inner join {{ ref('stg_onsdb__lst_rapportage_recht_typen') }} rt
            on rt.type_code = rr2.type_code
           and rt.type_omschrijving = 'Zichtbaar voor'
        where rr2.deskundigheid_id is not null
    ) rr
    inner join {{ ref('stg_onsdb__deskundigheden') }} d
        on d.deskundigheid_id = rr.deskundigheid_id

    group by rr.rapportage_id

),

rapportage_deskundigheidsgroepen_agg as (

    -- Deskundigheidsgroepen waarvoor de rapportage afgeschermd is (alleen 'Zichtbaar voor'-rechten)
    select
        rr.rapportage_id,
        string_agg(g.deskundigheidsgroep_naam, ' | ')
            within group (order by g.deskundigheidsgroep_naam)  as afgeschermd_voor_deskundigheidsgroepen

    from (
        select distinct rr2.rapportage_id, rr2.deskundigheidsgroep_id
        from {{ ref('stg_onsdb__rapportage_rechten') }} rr2
        inner join {{ ref('stg_onsdb__lst_rapportage_recht_typen') }} rt
            on rt.type_code = rr2.type_code
           and rt.type_omschrijving = 'Zichtbaar voor'
        where rr2.deskundigheidsgroep_id is not null
    ) rr
    inner join {{ ref('stg_onsdb__deskundigheidsgroepen') }} g
        on g.deskundigheidsgroep_id = rr.deskundigheidsgroep_id

    group by rr.rapportage_id

),

medewerker_deskundigheden_agg as (

    -- Deskundigheden van de medewerker die de rapportage heeft opgesteld
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

    -- Deskundigheidsgroepen van de medewerker die de rapportage heeft opgesteld
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

rapportage_typen as (

    select
        rapportage_type_id,
        rapportage_type

    from {{ ref('stg_onsdb__careplan_report_types') }}

),

definitief as (

    select
        rap.rapportage_id,

        -- Client
        rap.client_id,
        c.clientnummer,
        c.clientnaam                                              as client_naam,

        -- Medewerker die de rapportage heeft opgesteld
        rap.medewerker_id,
        m.personeelsnummer                                     as medewerker_personeelsnummer,
        concat(m.voornaam, ' ', m.achternaam)                  as medewerker_naam,
        mda.medewerker_deskundigheden,
        mga.medewerker_deskundigheidsgroepen,

        -- Rapportage-metadata
        rap.rapportage_type_id,
        rt.rapportage_type,
        rap.rapportagedatum,
        rap.status_code,
        rap.is_gemarkeerd,
        rap.is_verborgen,

        -- Toegangsbeperking
        r.afgeschermd_voor,
        rda.afgeschermd_voor_deskundigheden,
        rdga.afgeschermd_voor_deskundigheidsgroepen,

        -- Tijdstempels
        rap.aangemaakt_op,
        rap.gewijzigd_op,

        -- Deep-link naar rapportages in ONS dossier
        {{ ons_dossier_url('reports', 'rap.client_id') }}      as url_ons_rapportages

    from rapportages rap
    left join rechten r
        on r.rapportage_id = rap.rapportage_id
    left join rapportage_deskundigheden_agg rda
        on rda.rapportage_id = rap.rapportage_id
    left join rapportage_deskundigheidsgroepen_agg rdga
        on rdga.rapportage_id = rap.rapportage_id
    left join medewerker_deskundigheden_agg mda
        on mda.medewerker_id = rap.medewerker_id
    left join medewerker_deskundigheidsgroepen_agg mga
        on mga.medewerker_id = rap.medewerker_id
    left join clienten c
        on c.client_id = rap.client_id
    left join medewerkers m
        on m.medewerker_id = rap.medewerker_id
    left join rapportage_typen rt
        on rt.rapportage_type_id = rap.rapportage_type_id

)

select * from definitief
