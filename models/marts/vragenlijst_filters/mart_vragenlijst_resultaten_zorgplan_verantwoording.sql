-- Alleen ingevulde vragenlijsten met titel "Zorgplan, welke keuzes hebben we gemaakt?".

select * from {{ ref('mart_vragenlijst_resultaten') }}
where vragenlijst_titel = 'Zorgplan, welke keuzes hebben we gemaakt?'
