-- Elke hulpvorm-omschrijving in de seed locatie_hulpvorm_mapping moet exact voorkomen
-- in de CBS-codelijst. Vervangt een relationships-test: seed en OnsDB hebben een
-- andere collation. Geeft alleen seed-waarden terug (referentiedata).

select distinct mapping.hulpvorm_omschrijving
from {{ ref('locatie_hulpvorm_mapping') }} as mapping
where not exists (
    select 1
    from {{ ref('stg_onsdb__cbsbj_hulpvormen') }} as hulpvormen
    where hulpvormen.hulpvorm_omschrijving collate database_default
        = mapping.hulpvorm_omschrijving collate database_default
)
