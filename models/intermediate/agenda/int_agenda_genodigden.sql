-- Genodigden van Ons Agenda-afspraken, vertaald naar Ons Administratie-ID's.
-- genodigde_id verwijst naar één van vijf onsagenda_*-tabellen; het type volgt uit welke tabel matcht.
-- Aanname: objectIds zijn uniek over deze tabellen heen (getest via unique op uitnodiging_id en not_null op genodigde_type).
-- Grain: één rij per uitnodiging.

with uitnodigingen as (

    select * from {{ ref('stg_onsdb__agenda_invitations') }}

),

afspraken as (

    select afspraak_id, agenda_uursoort_id
    from {{ ref('stg_onsdb__agenda_events') }}

),

agenda_medewerkers as (

    select * from {{ ref('stg_onsdb__onsagenda_employees') }}

),

agenda_clienten as (

    select * from {{ ref('stg_onsdb__onsagenda_clients') }}

),

agenda_locaties as (

    select * from {{ ref('stg_onsdb__onsagenda_locations') }}

),

agenda_teams as (

    select * from {{ ref('stg_onsdb__onsagenda_teams') }}

),

agenda_groepen as (

    select * from {{ ref('stg_onsdb__onsagenda_groups') }}

),

uursoorten as (

    select * from {{ ref('stg_onsdb__agenda_hour_types') }}

),

definitief as (

    select
        u.uitnodiging_id,
        u.afspraak_id,
        u.genodigde_id,

        case
            when m.agenda_medewerker_id is not null then 'Medewerker'
            when c.agenda_client_id     is not null then 'Cliënt'
            when l.agenda_locatie_id    is not null then 'Locatie'
            when t.agenda_team_id       is not null then 'Team'
            when g.agenda_groep_id      is not null then 'Groep'
        end                                                     as genodigde_type,

        m.medewerker_id,
        c.client_id,
        l.locatie_id,
        t.team_id,
        g.groepszorg_groep_id,

        u.is_geaccepteerd,
        om.medewerker_id                                        as oorspronkelijke_medewerker_id,
        case
            when u.oorspronkelijke_genodigde_id is not null
             and u.oorspronkelijke_genodigde_id <> u.genodigde_id then 1
            else 0
        end                                                     as is_vervangen,

        -- Uursoort op de uitnodiging gaat voor die op de afspraak
        coalesce(u.agenda_uursoort_id, a.agenda_uursoort_id)    as agenda_uursoort_id,
        us.activiteit_id,

        u.zorglegitimatie_id,
        u.aangemaakt_op,
        u.gewijzigd_op

    from uitnodigingen u
    left join afspraken a
        on a.afspraak_id = u.afspraak_id
    left join agenda_medewerkers m
        on m.agenda_medewerker_id = u.genodigde_id
    left join agenda_clienten c
        on c.agenda_client_id = u.genodigde_id
    left join agenda_locaties l
        on l.agenda_locatie_id = u.genodigde_id
    left join agenda_teams t
        on t.agenda_team_id = u.genodigde_id
    left join agenda_groepen g
        on g.agenda_groep_id = u.genodigde_id
    left join agenda_medewerkers om
        on om.agenda_medewerker_id = u.oorspronkelijke_genodigde_id
    left join uursoorten us
        on us.agenda_uursoort_id = coalesce(u.agenda_uursoort_id, a.agenda_uursoort_id)

)

select * from definitief
