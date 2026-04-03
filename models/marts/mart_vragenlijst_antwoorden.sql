with antwoorden as (

    select * from {{ ref('int_vragenlijst_antwoorden') }}

),

resultaten as (

    select * from {{ ref('int_vragenlijst_resultaten') }}

),

definitie as (

    select * from {{ ref('int_vragenlijsten_definitie') }}

),

definitief as (

    select
        antwoorden.vragenlijst_antwoord_id,
        antwoorden.vragenlijst_resultaat_id,
        antwoorden.vragenlijst_id,

        -- Vragenlijst
        resultaten.vragenlijst_titel,

        -- Client
        antwoorden.client_id,
        resultaten.client_naam,

        -- Medewerker
        resultaten.medewerker_id,
        resultaten.ingevuld_door,

        -- Invulling
        antwoorden.ingevuld_op,
        resultaten.status_code,
        resultaten.status_omschrijving,

        -- Vragenlijst structuur (categorie > groep > vraag)
        definitie.vragenlijst_categorie_id,
        definitie.categorie_titel,
        definitie.categorie_volgnummer,
        definitie.vragenlijst_groep_id,
        cast(definitie.groep_beschrijving as nvarchar(500)) as groep_beschrijving,
        definitie.groep_volgnummer,
        definitie.drempelwaarde_score,
        definitie.max_score,

        -- Vraag
        antwoorden.vragenlijst_vraag_id,
        cast(antwoorden.vraagtekst as nvarchar(2000)) as vraagtekst,
        antwoorden.antwoord_type,
        antwoorden.vraag_volgnummer,

        -- Antwoord
        antwoorden.antwoord_definitie_id,
        cast(antwoorden.antwoord_tekst as nvarchar(1000)) as antwoord_tekst,
        antwoorden.antwoord_waarde,
        antwoorden.antwoord_score,
        cast(antwoorden.tekst_antwoord as nvarchar(4000)) as tekst_antwoord,
        antwoorden.ja_nee_antwoord,
        antwoorden.is_belangrijk,

        -- Tijdstempels
        antwoorden.aangemaakt_op,
        antwoorden.gewijzigd_op

    from antwoorden
    inner join resultaten
        on resultaten.vragenlijst_resultaat_id = antwoorden.vragenlijst_resultaat_id
    left join definitie
        on definitie.vragenlijst_vraag_id = antwoorden.vragenlijst_vraag_id

)

select * from definitief
