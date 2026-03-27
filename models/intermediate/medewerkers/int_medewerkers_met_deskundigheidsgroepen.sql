with profielkoppelingen as (

    select * from {{ ref('stg_onsdb__deskundigheid_koppelingen') }}

),

medewerkers as (

    select
        medewerker_id,
        personeelsnummer
    from {{ ref('stg_onsdb__employees') }}

),

profielen as (

    select * from {{ ref('stg_onsdb__deskundigheden') }}

),

groepkoppelingen as (

    select * from {{ ref('stg_onsdb__deskundigheidsgroep_deskundigheden') }}

),

groepen as (

    select * from {{ ref('stg_onsdb__deskundigheidsgroepen') }}

),

definitief as (

    select
        medewerkers.personeelsnummer        as medewerker_id,
        groepen.deskundigheidsgroep_naam    as deskundigheidsgroep,
        profielkoppelingen.startdatum,
        profielkoppelingen.einddatum

    from profielkoppelingen
    inner join medewerkers
        on medewerkers.medewerker_id = profielkoppelingen.medewerker_id
    inner join profielen
        on profielen.deskundigheid_id = profielkoppelingen.deskundigheid_id
        and profielen.is_zichtbaar = 1
    inner join groepkoppelingen
        on groepkoppelingen.deskundigheid_id = profielen.deskundigheid_id
    inner join groepen
        on groepen.deskundigheidsgroep_id = groepkoppelingen.deskundigheidsgroep_id

)

select * from definitief
