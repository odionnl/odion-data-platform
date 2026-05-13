-- Alleen ingevulde vragenlijsten met titel "Persoonsbeeld, bejegening, signaleringsplan".

select * from {{ ref('mart_vragenlijst_resultaten') }}
where vragenlijst_titel = 'Persoonsbeeld, bejegening, signaleringsplan'
