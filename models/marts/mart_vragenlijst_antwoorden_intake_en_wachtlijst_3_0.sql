-- Alleen antwoorden op de vragenlijst "Intake en wachtlijst 3.0".

select * from {{ ref('mart_vragenlijst_antwoorden') }}
where vragenlijst_titel = 'Intake en wachtlijst 3.0'
