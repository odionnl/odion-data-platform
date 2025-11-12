-- =============================================================================
-- silver.ons_clients
-- =============================================================================
select
    objectId,
    identificationNo,
    dateOfBirth,
    deathDate,
    lastName,
    birthName,
    givenName,
    partnerName,
    initials,
    prefix,
    [name]
from {{ source('ons_plan_2', 'clients') }}
where dateOfBirth is not null
  and identificationNo not like '%[^0-9]%';  -- keep numeric only
