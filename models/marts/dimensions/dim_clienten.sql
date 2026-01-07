with src as (

    select * from {{ ref('int_clients') }}

)

select
    src.client_id,
    src.clientnummer,
    src.geboortedatum,
    src.overlijdensdatum,
    src.achternaam,
    src.geboortenaam,
    src.voornaam,
    src.partnernaam,
    src.initialen,
    src.prefix,
    src.naam,
    src.leeftijd,
    src.leeftijdsgroep,
    src.in_zorg,

    -- urls (ONS)
    {{ ons_url('view', 'src.client_id') }}              as url_ons_administratie,
    {{ ons_url('medical/overview', 'src.client_id') }}  as url_ons_dossier,
    {{ ons_url('care_plan', 'src.client_id') }}         as url_ons_zorgplan,
    {{ ons_url('calendar', 'src.client_id') }}          as url_ons_agenda,
    {{ ons_url('reports', 'src.client_id') }}           as url_ons_rapportages


    from src
;
