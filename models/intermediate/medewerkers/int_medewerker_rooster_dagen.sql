-- Per (medewerker_id, datum) één rij als er een geldige ORTEC-dienst is op die dag.
-- Zowel startdatum als einddatum van een dienst tellen, zodat nachtdiensten
-- voor beide dagen meetellen. Mapping ORTEC-personeelsnummer → OnsDB medewerker_id
-- via stg_onsdb__employees.personeelsnummer.
-- heeft_werk_tijd = 1 als minstens één dienst op die dag tijd_op_werk > 0 heeft
-- (dus geen pure ziek/verlof/standby-dag).

with diensten as (

    select
        personeelsnummer,
        starttijd,
        eindtijd,
        tijd_op_werk
    from {{ ref('int_geldige_diensten') }}

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

    select personeelsnummer, cast(starttijd as date) as datum, tijd_op_werk from diensten
    union all
    select personeelsnummer, cast(eindtijd   as date) as datum, tijd_op_werk from diensten

),

definitief as (

    select
        medewerkers.medewerker_id,
        dienstdagen.datum,
        max(case when dienstdagen.tijd_op_werk > 0 then 1 else 0 end) as heeft_werk_tijd
    from dienstdagen
    inner join medewerkers
        on medewerkers.personeelsnummer collate database_default
         = dienstdagen.personeelsnummer collate database_default
    group by medewerkers.medewerker_id, dienstdagen.datum

)

select * from definitief
