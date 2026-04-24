with diensten as (

    select * from {{ ref('int_geldige_diensten') }}

),

locaties as (

    select * from {{ ref('int_locaties_met_kostenplaatsen') }}

),

medewerkers as (

    select
        medewerker_id,
        personeelsnummer
    from {{ ref('stg_onsdb__employees') }}
    where personeelsnummer is not null
      and personeelsnummer <> ''

),

definitief as (

    select distinct
        medewerkers.medewerker_id,
        diensten.personeelsnummer,
        locaties.locatienaam

    from diensten
    inner join locaties
        on locaties.kostenplaats_id collate database_default
         = diensten.kostenplaats_id collate database_default
        and locaties.startdatum_koppeling <= cast(diensten.starttijd as date)
        and (locaties.einddatum_koppeling is null
             or locaties.einddatum_koppeling > cast(diensten.starttijd as date))
    left join medewerkers
        on medewerkers.personeelsnummer collate database_default
         = diensten.personeelsnummer collate database_default
    where diensten.starttijd >= dateadd(day, -{{ var('evaluatieperiode_dagen') }}, cast(getdate() as date))
      and diensten.starttijd <= cast(getdate() as date)

)

select * from definitief
