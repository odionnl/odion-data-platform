-- Alle antwoorden op vragenlijsten met titel "Levensloop".

select * from {{ ref('mart_vragenlijst_antwoorden') }}
where vragenlijst_titel = 'Levensloop'
