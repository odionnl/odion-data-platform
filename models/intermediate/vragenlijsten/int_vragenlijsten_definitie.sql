with vragenlijsten as (

    select * from {{ ref('stg_onsdb__vragenlijsten') }}

),

categorieen as (

    select * from {{ ref('stg_onsdb__vragenlijst_categorieen') }}

),

groepen as (

    select * from {{ ref('stg_onsdb__vragenlijst_groepen') }}

),

vragen as (

    select * from {{ ref('stg_onsdb__vragenlijst_vragen') }}

),

antwoord_groepen as (

    select * from {{ ref('stg_onsdb__vragenlijst_antwoord_definitie_groepen') }}

),

definitief as (

    select
        vragen.vragenlijst_vraag_id,
        vragenlijsten.vragenlijst_id,
        vragenlijsten.titel                     as vragenlijst_titel,
        vragenlijsten.is_actief                 as vragenlijst_actief,
        categorieen.vragenlijst_categorie_id,
        categorieen.titel                       as categorie_titel,
        categorieen.volgnummer                  as categorie_volgnummer,
        groepen.vragenlijst_groep_id,
        groepen.beschrijving                    as groep_beschrijving,
        groepen.volgnummer                      as groep_volgnummer,
        groepen.drempelwaarde_score,
        groepen.max_score,
        vragen.volgnummer                       as vraag_volgnummer,
        vragen.vraagtekst,
        vragen.aanvullende_info,
        vragen.antwoord_type,
        vragen.is_verplicht,
        vragen.is_inactief,
        vragen.antwoord_definitie_groep_id,
        antwoord_groepen.beschrijving           as antwoord_groep_beschrijving

    from vragen
    inner join groepen
        on groepen.vragenlijst_groep_id = vragen.vragenlijst_groep_id
    inner join categorieen
        on categorieen.vragenlijst_categorie_id = groepen.vragenlijst_categorie_id
    inner join vragenlijsten
        on vragenlijsten.vragenlijst_id = categorieen.vragenlijst_id
    left join antwoord_groepen
        on antwoord_groepen.antwoord_definitie_groep_id = vragen.antwoord_definitie_groep_id

)

select * from definitief
