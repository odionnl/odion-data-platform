-- Alleen ingevulde vragenlijsten met titel "Levensloop".

select * from {{ ref('mart_vragenlijst_resultaten') }}
where vragenlijst_titel = 'Levensloop'
