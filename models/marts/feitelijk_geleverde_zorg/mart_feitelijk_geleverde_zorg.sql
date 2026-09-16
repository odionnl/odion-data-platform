with clienten as (

    select * from {{ ref('int_clienten_in_zorg_actueel') }}

),

geldig_zorgplan as (

    select * from {{ ref('int_check_geldig_zorgplan') }}

),

recente_rapportages as (

    select * from {{ ref('int_check_recente_rapportages') }}

),

medicatie_toegediend as (

    select * from {{ ref('int_check_medicatie_toegediend') }}

),

zorgplan_ingezien as (

    select * from {{ ref('int_check_zorgplan_ingezien') }}

),

definitief as (

    select
        clienten.client_id,
        clienten.client_naam,

        -- Individuele checks
        coalesce(geldig_zorgplan.geldig_zorgplan, 0)            as geldig_zorgplan,
        coalesce(recente_rapportages.recente_rapportages, 0)    as recente_rapportages,
        medicatie_toegediend.medicatie_toegediend,
        coalesce(zorgplan_ingezien.zorgplan_ingezien, 0)        as zorgplan_ingezien,

        -- Score: som van behaalde punten gedeeld door het aantal van toepassing zijnde checks.
        -- Vaste checks (altijd van toepassing): geldig_zorgplan, recente_rapportages, zorgplan_ingezien.
        -- Optionele check: medicatie_toegediend (NULL = niet van toepassing voor deze client).
        -- Door de deler dynamisch te berekenen, blijft de score correct als er checks worden toegevoegd.
        cast(round(
            (
                coalesce(geldig_zorgplan.geldig_zorgplan, 0.0)
                + coalesce(recente_rapportages.recente_rapportages, 0.0)
                + coalesce(medicatie_toegediend.medicatie_toegediend, 0.0)
                + coalesce(zorgplan_ingezien.zorgplan_ingezien, 0.0)
            ) * 100.0 / nullif(
                -- Vaste checks
                case when geldig_zorgplan.client_id       is not null then 1 else 0 end
                + case when recente_rapportages.client_id is not null then 1 else 0 end
                + case when zorgplan_ingezien.client_id   is not null then 1 else 0 end
                -- Optionele check: alleen meetellen als er medicatiedata beschikbaar is
                + case when medicatie_toegediend.medicatie_toegediend is not null then 1 else 0 end,
                0  -- voorkomt deling door nul als alle joins leeg zijn
            ),
        0) as int)                                              as client_score,

        -- Metadata
        cast(getdate() as date)                                 as peildatum,
        dateadd(day, -{{ var('evaluatieperiode_dagen') }}, cast(getdate() as date)) as startdatum

    from clienten
    left join geldig_zorgplan
        on geldig_zorgplan.client_id = clienten.client_id
    left join recente_rapportages
        on recente_rapportages.client_id = clienten.client_id
    left join medicatie_toegediend
        on medicatie_toegediend.client_id = clienten.client_id
    left join zorgplan_ingezien
        on zorgplan_ingezien.client_id = clienten.client_id

)

select * from definitief
