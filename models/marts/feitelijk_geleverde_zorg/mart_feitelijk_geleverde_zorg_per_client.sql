with clienten as (

    select * from {{ ref('int_clienten_in_zorg_actueel') }}

),

actueel_zorgplan as (

    select * from {{ ref('int_check_actueel_zorgplan') }}

),

recente_rapportages as (

    select * from {{ ref('int_check_recente_rapportages') }}

),

medicatie_afgetekend as (

    select * from {{ ref('int_check_medicatie_afgetekend') }}

),

zorgdossier_bekeken as (

    select * from {{ ref('int_check_zorgdossier_bekeken') }}

),

definitief as (

    select
        clienten.client_id,
        clienten.client_naam,

        -- Individuele checks
        coalesce(actueel_zorgplan.actueel_zorgplan, 0)          as actueel_zorgplan,
        coalesce(recente_rapportages.recente_rapportages, 0)    as recente_rapportages,
        medicatie_afgetekend.medicatie_afgetekend,
        coalesce(zorgdossier_bekeken.zorgdossier_bekeken, 0)    as zorgdossier_bekeken,

        -- Score: som van behaalde punten gedeeld door het aantal van toepassing zijnde checks.
        -- Fractie tussen 0 en 1 (bv. 0.67) — Power BI formatteert dit als percentage.
        -- Vaste checks (altijd van toepassing): actueel_zorgplan, recente_rapportages, zorgdossier_bekeken.
        -- Optionele check: medicatie_afgetekend (NULL = niet van toepassing voor deze client).
        -- Door de deler dynamisch te berekenen, blijft de score correct als er checks worden toegevoegd.
        cast(round(
            (
                coalesce(actueel_zorgplan.actueel_zorgplan, 0.0)
                + coalesce(recente_rapportages.recente_rapportages, 0.0)
                + coalesce(medicatie_afgetekend.medicatie_afgetekend, 0.0)
                + coalesce(zorgdossier_bekeken.zorgdossier_bekeken, 0.0)
            ) / nullif(
                -- Vaste checks
                case when actueel_zorgplan.client_id      is not null then 1 else 0 end
                + case when recente_rapportages.client_id is not null then 1 else 0 end
                + case when zorgdossier_bekeken.client_id is not null then 1 else 0 end
                -- Optionele check: alleen meetellen als er medicatiedata beschikbaar is
                + case when medicatie_afgetekend.medicatie_afgetekend is not null then 1 else 0 end,
                0  -- voorkomt deling door nul als alle joins leeg zijn
            ),
        2) as decimal(5,2))                                     as client_score,

        -- Metadata
        cast(getdate() as date)                                 as peildatum,
        dateadd(day, -{{ var('evaluatieperiode_dagen') }}, cast(getdate() as date)) as startdatum

    from clienten
    left join actueel_zorgplan
        on actueel_zorgplan.client_id = clienten.client_id
    left join recente_rapportages
        on recente_rapportages.client_id = clienten.client_id
    left join medicatie_afgetekend
        on medicatie_afgetekend.client_id = clienten.client_id
    left join zorgdossier_bekeken
        on zorgdossier_bekeken.client_id = clienten.client_id

)

select * from definitief
