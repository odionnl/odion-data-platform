with diensten as (

    select * from {{ ref('stg_ortec__diensten') }}

),

locaties as (

    select * from {{ ref('int_locaties_met_kostenplaatsen') }}

),

definitief as (

    select distinct
        diensten.medewerker_id,
        locaties.locatienaam

    from diensten
    inner join locaties
        on locaties.kostenplaats_id collate database_default
         = diensten.kostenplaats_id collate database_default
        and locaties.startdatum_koppeling <= cast(diensten.starttijd as date)
        and (locaties.einddatum_koppeling is null
             or locaties.einddatum_koppeling > cast(diensten.starttijd as date))
    where diensten.starttijd >= dateadd(day, -{{ var('evaluatieperiode_dagen') }}, cast(getdate() as date))
      and diensten.starttijd <= cast(getdate() as date)

)

select * from definitief
