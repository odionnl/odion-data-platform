-- Ons Agenda: genodigden van een afspraak.
-- genodigde_id verwijst naar onsagenda_employees, _clients, _locations, _teams of _groups (zie int_agenda_genodigden).

with bron as (

    select * from {{ source('ons_plan_2', 'agenda_invitations') }}

),

definitief as (

    select
        objectId                        as uitnodiging_id,
        inviteeObjectId                 as genodigde_id,
        eventObjectId                   as afspraak_id,
        cast(accepted as int)           as is_geaccepteerd,
        originalInviteeObjectId         as oorspronkelijke_genodigde_id,
        hourTypeObjectId                as agenda_uursoort_id,
        externalCareOrderObjectId       as zorglegitimatie_id,
        createdAt                       as aangemaakt_op,
        updatedAt                       as gewijzigd_op

    from bron

)

select * from definitief
