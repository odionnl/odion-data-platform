with antwoorden as (

    select * from {{ ref('stg_onsdb__vragenlijst_antwoorden') }}

),

vragen as (

    select * from {{ ref('stg_onsdb__vragenlijst_vragen') }}

),

antwoord_definities as (

    select * from {{ ref('stg_onsdb__vragenlijst_antwoord_definities') }}

),

resultaten as (

    select
        vragenlijst_resultaat_id,
        vragenlijst_id,
        client_id,
        ingevuld_op

    from {{ ref('stg_onsdb__vragenlijst_resultaten') }}

),

definitief as (

    select
        antwoorden.vragenlijst_antwoord_id,
        antwoorden.vragenlijst_resultaat_id,
        resultaten.vragenlijst_id,
        resultaten.client_id,
        resultaten.ingevuld_op,
        antwoorden.vragenlijst_vraag_id,
        vragen.vraagtekst,
        vragen.antwoord_type,
        vragen.volgnummer                       as vraag_volgnummer,
        antwoorden.antwoord_definitie_id,
        antwoord_definities.antwoord_tekst,
        antwoord_definities.waarde              as antwoord_waarde,
        antwoord_definities.score               as antwoord_score,
        antwoorden.tekst_antwoord,
        antwoorden.ja_nee_antwoord,
        antwoorden.is_belangrijk,
        antwoorden.aangemaakt_op,
        antwoorden.gewijzigd_op

    from antwoorden
    inner join resultaten
        on resultaten.vragenlijst_resultaat_id = antwoorden.vragenlijst_resultaat_id
    left join vragen
        on vragen.vragenlijst_vraag_id = antwoorden.vragenlijst_vraag_id
    left join antwoord_definities
        on antwoord_definities.antwoord_definitie_id = antwoorden.antwoord_definitie_id

)

select * from definitief
