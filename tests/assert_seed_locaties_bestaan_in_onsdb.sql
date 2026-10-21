-- Elke locatienaam in de seed locatie_hulpvorm_mapping moet exact voorkomen in OnsDB.
-- Vervangt een relationships-test: seed en OnsDB hebben een andere collation.
-- Geeft alleen seed-waarden terug (referentiedata), geen cliëntgegevens.

select mapping.locatienaam
from {{ ref('locatie_hulpvorm_mapping') }} as mapping
where not exists (
    select 1
    from {{ ref('stg_onsdb__locations') }} as locaties
    where locaties.locatienaam collate database_default = mapping.locatienaam collate database_default
)
