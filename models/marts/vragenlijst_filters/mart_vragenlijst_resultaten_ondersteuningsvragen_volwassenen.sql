-- Alleen ingevulde vragenlijsten met titel "Ondersteuningsvragen voor volwassenen".

select * from {{ ref('mart_vragenlijst_resultaten') }}
where vragenlijst_titel = 'Ondersteuningsvragen voor volwassenen'
