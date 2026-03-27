with resultaten as (

    select * from {{ ref('stg_onsdb__vragenlijst_resultaten') }}

),

vragenlijsten as (

    select * from {{ ref('stg_onsdb__vragenlijsten') }}

),

clienten as (

    select * from {{ ref('stg_onsdb__clients') }}

),

medewerkers as (

    select * from {{ ref('stg_onsdb__employees') }}

),

statussen as (

    select * from {{ ref('stg_onsdb__lst_vragenlijst_resultaat_statussen') }}

),

definitief as (

    select
        resultaten.vragenlijst_resultaat_id,
        resultaten.vragenlijst_id,
        vragenlijsten.titel                                             as vragenlijst_titel,
        vragenlijsten.is_actief                                         as vragenlijst_actief,
        resultaten.client_id,
        COALESCE(clienten.voornaam + ' ', '') + clienten.achternaam     as client_naam,
        resultaten.medewerker_id,
        COALESCE(medewerkers.voornaam + ' ', '') + medewerkers.achternaam as ingevuld_door,
        resultaten.ingevuld_op,
        resultaten.status_code,
        statussen.status_omschrijving,
        resultaten.is_readonly,
        resultaten.aangemaakt_op,
        resultaten.gewijzigd_op

    from resultaten
    left join vragenlijsten
        on vragenlijsten.vragenlijst_id = resultaten.vragenlijst_id
    left join clienten
        on clienten.client_id = resultaten.client_id
    left join medewerkers
        on medewerkers.medewerker_id = resultaten.medewerker_id
    left join statussen
        on statussen.status_code = resultaten.status_code

)

select * from definitief
