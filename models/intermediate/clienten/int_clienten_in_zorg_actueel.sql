-- Clients met een actieve zorgtoewijzing op vandaag (GETDATE()).
-- Grain: één rij per actieve client.

with clienten as (

    select * from {{ ref('stg_onsdb__clients') }}

),

zorgtoewijzingen as (

    select * from {{ ref('stg_onsdb__care_allocations') }}

),

actieve_zorgtoewijzingen as (

    select distinct client_id
    from zorgtoewijzingen
    where startdatum <= cast(getdate() as date)
      and (einddatum is null or einddatum > cast(getdate() as date))

),

definitief as (

    select
        clienten.client_id,
        clienten.clientnaam as client_naam

    from clienten
    inner join actieve_zorgtoewijzingen
        on actieve_zorgtoewijzingen.client_id = clienten.client_id

)

select * from definitief
