-- Testcliënten die uit alle marts en rapportages gefilterd moeten worden.
-- Twee categorieën:
--   1. Vaste testclientnummers (handmatig onderhouden)
--   2. Cliënten met een (historische) locatiekoppeling onder '99. Trainingslocatie'
-- Grain: één rij per testcliënt (client_id).

with vaste_testclienten as (

    select client_id
    from {{ ref('stg_onsdb__clients') }}
    where clientnummer in ('10510', '11428')

),

trainingslocatie_clienten as (

    select distinct la.client_id
    from {{ ref('stg_onsdb__location_assignments') }} la
    inner join {{ ref('int_locatie_hierarchie') }} lh
        on lh.locatie_id = la.locatie_id
    where lh.niveau2 = '99. Trainingslocatie'

),

definitief as (

    select client_id from vaste_testclienten
    union
    select client_id from trainingslocatie_clienten

)

select * from definitief
