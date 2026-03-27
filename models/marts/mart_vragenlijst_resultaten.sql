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
    ingevuld_door,
    ingevuld_op,
    status_code,
    status_omschrijving,
    is_readonly,
    aangemaakt_op,
    gewijzigd_op

from resultaten
