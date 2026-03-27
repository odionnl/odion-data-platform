with clienten as (

    select * from {{ ref('int_clienten_in_zorg_actueel') }}

),

toedieningen as (

    select * from {{ ref('int_medicatie_toedieningen') }}

),

-- Meest recente overzicht per client+datum (voor peildatum)
huidige_overzichten as (

    select
        medication_chart_id,
        row_number() over (
            partition by client_id, overzicht_datum
            order by overzicht_gegenereerd_op desc
        ) as rn

    from toedieningen
    where overzicht_gegenereerd_op < cast(getdate() as date)

),

-- Filter: in evaluatieperiode, huidig overzicht, geen 'none_scheduled'
relevante_toedieningen as (

    select
        t.client_id,
        case
            when t.status in ('handed', 'administered', 'prepared', 'self_managed') then 1
            else 0
        end as geldig_status

    from toedieningen t
    inner join huidige_overzichten ho
        on ho.medication_chart_id = t.medication_chart_id
        and ho.rn = 1
    where t.ingepland_op >= dateadd(day, -{{ var('evaluatieperiode_dagen') }}, cast(getdate() as date))
      and t.ingepland_op <= cast(getdate() as date)
      and t.status != 'none_scheduled'

),

-- Per client: heeft minstens 1 geldige toediening?
per_client as (

    select
        client_id,
        max(geldig_status) as medicatie_toegediend

    from relevante_toedieningen
    group by client_id

),

definitief as (

    select
        clienten.client_id,
        per_client.medicatie_toegediend

    from clienten
    left join per_client
        on per_client.client_id = clienten.client_id

)

select * from definitief
