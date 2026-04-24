-- ORTEC-diensten die meetellen voor verantwoording- en RPA-checks.
-- Alleen roosters die niet meer in concept-staat zijn: Processed / Published / Final.
-- Wordt gebruikt door int_medewerker_rooster_dagen en int_medewerkers_met_dienst_locaties.

with bron as (

    select * from {{ ref('stg_ortec__diensten') }}

)

select *
from bron
where roosterstatus in ('Processed', 'Published', 'Final')
