with H as (

    -- Anker: rootlocaties (zonder ouder)
    select
        l.locatie_id,
        l.ouder_locatie_id,

        l.startdatum_locatie    as startdatum,
        l.einddatum_locatie     as einddatum,

        cast(l.locatienaam as nvarchar(255)) collate database_default as locatienaam,

        cast(l.locatienaam as nvarchar(255)) collate database_default as niveau1,
        cast(null as nvarchar(255))          collate database_default as niveau2,
        cast(null as nvarchar(255))          collate database_default as niveau3,
        cast(null as nvarchar(255))          collate database_default as niveau4,
        cast(null as nvarchar(255))          collate database_default as niveau5,
        cast(null as nvarchar(255))          collate database_default as niveau6,

        1 as locatie_niveau,
        cast('.' + cast(l.locatie_id as nvarchar(50)) + '.' as nvarchar(1000)) as locatie_pad,

        l.aantal_kindlocaties,
        l.is_leaf_locatie,
        l.is_actief_vandaag

    from {{ ref('int_locaties_basis') }} l
    where l.ouder_locatie_id is null

    union all

    -- Recursief: kinderen
    select
        k.locatie_id,
        k.ouder_locatie_id,

        k.startdatum_locatie    as startdatum,
        k.einddatum_locatie     as einddatum,

        cast(k.locatienaam as nvarchar(255)) collate database_default as locatienaam,

        H.niveau1,
        cast(case when H.locatie_niveau = 1 then k.locatienaam else H.niveau2 end as nvarchar(255)) collate database_default as niveau2,
        cast(case when H.locatie_niveau = 2 then k.locatienaam else H.niveau3 end as nvarchar(255)) collate database_default as niveau3,
        cast(case when H.locatie_niveau = 3 then k.locatienaam else H.niveau4 end as nvarchar(255)) collate database_default as niveau4,
        cast(case when H.locatie_niveau = 4 then k.locatienaam else H.niveau5 end as nvarchar(255)) collate database_default as niveau5,
        cast(case when H.locatie_niveau = 5 then k.locatienaam else H.niveau6 end as nvarchar(255)) collate database_default as niveau6,

        H.locatie_niveau + 1 as locatie_niveau,
        cast(H.locatie_pad + cast(k.locatie_id as nvarchar(50)) + '.' as nvarchar(1000)) as locatie_pad,

        k.aantal_kindlocaties,
        k.is_leaf_locatie,
        k.is_actief_vandaag

    from {{ ref('int_locaties_basis') }} k
    join H
      on k.ouder_locatie_id = H.locatie_id
    where H.locatie_niveau < 6

)

select
    locatie_id,
    locatienaam,
    ouder_locatie_id,
    startdatum,
    einddatum,

    -- hiërarchiepositie
    locatie_pad,
    locatie_niveau,
    aantal_kindlocaties,
    is_leaf_locatie,
    is_actief_vandaag,

    -- niveaunamen
    niveau1,
    niveau2,
    niveau3,
    niveau4,
    niveau5,
    niveau6,

    -- cluster (CC-specifiek via macro — pas macros/locaties.sql aan voor andere organisaties)
    {{ get_locatiecluster('locatienaam', 'niveau2', 'niveau3') }} as cluster

from H
