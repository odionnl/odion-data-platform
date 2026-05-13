-- Alleen ingevulde vragenlijsten met titel "Integratief persoonsbeeld".

select * from {{ ref('mart_vragenlijst_resultaten') }}
where vragenlijst_titel = 'Integratief persoonsbeeld'
