{{ config(materialized='view') }}

-- Alleen antwoorden op de vragenlijst "Integratief persoonsbeeld".

select * from {{ ref('mart_vragenlijst_antwoorden') }}
where vragenlijst_titel = 'Integratief persoonsbeeld'
