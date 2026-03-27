-- Breed documentenoverzicht met clientnaam, medewerkersnaam, labels en toegangsrechten.
-- STRING_AGG voor labels, deskundigheden en deskundigheidsgroepen gebeurt hier in de mart.
-- Grain: één rij per document.

with documenten as (

    select * from {{ ref('stg_onsdb__documents') }}

),

status as (

    select * from {{ ref('int_documenten_met_status') }}

),

rechten as (

    select * from {{ ref('int_documenten_met_rechten') }}

),

labels_agg as (

    select
        document_id,
        string_agg(label_naam, ' | ') within group (order by label_naam) as labels

    from {{ ref('int_documenten_met_labels') }}
    group by document_id

),

document_deskundigheden_agg as (

    -- Deskundigheden waarvoor het document afgeschermd is
    select
        dr.document_id,
        string_agg(d.deskundigheid_naam, ' | ')
            within group (order by d.deskundigheid_naam)        as afgeschermd_voor_deskundigheden

    from (select distinct document_id, deskundigheid_id from {{ ref('stg_onsdb__document_rights') }}) dr
    inner join {{ ref('stg_onsdb__deskundigheden') }} d
        on d.deskundigheid_id = dr.deskundigheid_id

    group by dr.document_id

),

document_deskundigheidsgroepen_agg as (

    -- Deskundigheidsgroepen waarvoor het document afgeschermd is
    select
        dg.document_id,
        string_agg(g.deskundigheidsgroep_naam, ' | ')
            within group (order by g.deskundigheidsgroep_naam)  as afgeschermd_voor_deskundigheidsgroepen

    from (select distinct document_id, deskundigheidsgroep_id from {{ ref('stg_onsdb__document_deskundigheidsgroepen') }}) dg
    inner join {{ ref('stg_onsdb__deskundigheidsgroepen') }} g
        on g.deskundigheidsgroep_id = dg.deskundigheidsgroep_id

    group by dg.document_id

),

medewerker_deskundigheden_agg as (

    -- Deskundigheden van de medewerker die het document heeft geüpload
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

    -- Deskundigheidsgroepen van de medewerker die het document heeft geüpload
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
        d.document_id,

        -- Client
        d.client_id,
        c.clientnummer,
        c.clientnaam                                              as client_naam,

        -- Medewerker die het document heeft geüpload
        d.medewerker_id,
        m.personeelsnummer                                     as medewerker_personeelsnummer,
        concat(m.voornaam, ' ', m.achternaam)                  as medewerker_naam,
        mda.medewerker_deskundigheden,
        mga.medewerker_deskundigheidsgroepen,

        -- Document-metadata
        d.beschrijving,
        d.bestandsnaam,
        s.status_omschrijving                                   as documentstatus,
        la.labels,

        -- Toegangsbeperking
        d.is_vertrouwelijk,
        r.afgeschermd_voor,
        dda.afgeschermd_voor_deskundigheden,
        ddga.afgeschermd_voor_deskundigheidsgroepen,

        -- Tijdstempels
        d.aangemaakt_op,
        d.gewijzigd_op,

        -- Deep-link naar documenten in ONS dossier
        {{ ons_dossier_url('documents', 'd.client_id') }}       as url_ons_documenten

    from documenten d
    left join status s
        on s.document_id = d.document_id
    left join rechten r
        on r.document_id = d.document_id
    left join labels_agg la
        on la.document_id = d.document_id
    left join document_deskundigheden_agg dda
        on dda.document_id = d.document_id
    left join document_deskundigheidsgroepen_agg ddga
        on ddga.document_id = d.document_id
    left join medewerker_deskundigheden_agg mda
        on mda.medewerker_id = d.medewerker_id
    left join medewerker_deskundigheidsgroepen_agg mga
        on mga.medewerker_id = d.medewerker_id
    left join clienten c
        on c.client_id = d.client_id
    left join medewerkers m
        on m.medewerker_id = d.medewerker_id

)

select * from definitief
