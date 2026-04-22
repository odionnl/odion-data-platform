-- Per (medewerker_id, datum) één rij als er een ORTEC-dienst is op die dag.
-- Zowel startdatum als einddatum van een dienst tellen, zodat nachtdiensten
-- voor beide dagen meetellen. Mapping ORTEC-personeelsnummer → OnsDB medewerker_id
-- via stg_onsdb__employees.personeelsnummer.

with diensten as (

    select
        medewerker_id as personeelsnummer,
        starttijd,
        eindtijd
    from {{ ref('stg_ortec__diensten') }}

),

medewerkers as (

    select
        medewerker_id,
        personeelsnummer
    from {{ ref('stg_onsdb__employees') }}
    where personeelsnummer is not null
      and personeelsnummer <> ''

),

dienstdagen as (

    select personeelsnummer, cast(starttijd as date) as datum from diensten
    union
    select personeelsnummer, cast(eindtijd   as date) as datum from diensten

),

definitief as (

    select distinct
        medewerkers.medewerker_id,
        dienstdagen.datum
    from dienstdagen
    inner join medewerkers
        on medewerkers.personeelsnummer collate database_default
         = dienstdagen.personeelsnummer collate database_default

)

select * from definitief
