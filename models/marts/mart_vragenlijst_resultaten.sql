with resultaten as (

    select * from {{ ref('int_vragenlijst_resultaten') }}

)

select
    vragenlijst_resultaat_id,
    vragenlijst_id,
    vragenlijst_titel,
    client_id,
    client_naam,
    medewerker_id,
    medewerker_naam,
    voltooid_op,
    status_omschrijving as status,
    aangemaakt_op,
    gewijzigd_op

from resultaten
