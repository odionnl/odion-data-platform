{{ config(materialized='view') }}

-- Clientselectie: alleen 18+ cliënten
with 
client_info as (
    select * from {{ ref('dim_clienten') }}
    where leeftijd >= 18
),


-- Wachtlijst selectie: alleen cliënten op VG BW of VG GIW
wachtlijst_vg_bw_giw as (
    select distinct
        client_id
    from {{ ref('fct_client_wachtlijsten_actueel') }}
    where
        niveau4 like 'VG BW%'
        or niveau4 like 'VG GIW%'
),

-- Woonsituatie: alleen 'Woont bij verwant'
woonsituatie as (
    select
        client_id,
        clientnummer,
        in_zorg,
        locatie_id,
        hoofdlocatie,
        cluster,
        woonsituatie,
        contactpersoon_relatietype,
        persoonlijke_relatietype,
        client_plaats,
        client_gemeente

    from {{ ref('fct_client_woonsituatie') }}
    where woonsituatie = 'Woont bij verwant'
)

select
    client_info.client_id,
    client_info.clientnummer,
    client_info.in_zorg,
    client_info.leeftijd,
    woonsituatie.locatie_id,
    woonsituatie.hoofdlocatie,
    woonsituatie.cluster,
    woonsituatie.woonsituatie,
    woonsituatie.contactpersoon_relatietype as contactpersoon_relatie,
    woonsituatie.persoonlijke_relatietype as persoonlijke_relatie, 
    woonsituatie.client_plaats as woonplaats,
    woonsituatie.client_gemeente as gemeente
from client_info
inner join wachtlijst_vg_bw_giw
    on wachtlijst_vg_bw_giw.client_id = client_info.client_id
inner join woonsituatie
  on woonsituatie.client_id = client_info.client_id;
