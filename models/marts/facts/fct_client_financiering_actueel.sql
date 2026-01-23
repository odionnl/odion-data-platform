with clienten as (
    select
        client_id,
        in_zorg
    from {{ ref('dim_clienten') }}
),

financiering_ranked as (
    select
        client_id,
        product_financiering,
        row_number() over (
            partition by client_id
            order by case product_financiering
                when 'Zorgzwaartepakket' then 1
                when 'PGB' then 2
                when 'Wet maatschappelijke ondersteuning' then 3
                when 'Jeugdwet' then 4
                when 'Onderaanneming' then 5
                else 999
            end
        ) as rn
    from {{ ref('fct_client_zorglegitimatie_actueel') }}
    where product_financiering in (
        'Zorgzwaartepakket',
        'PGB',
        'Wet maatschappelijke ondersteuning',
        'Jeugdwet',
        'Onderaanneming'
    )
),

financiering as (
    select
        client_id,
        product_financiering
    from financiering_ranked
    where rn = 1
),

final as (
    select
        c.client_id,
        c.in_zorg,
        coalesce(f.product_financiering, 'Overig') as financiering
    from clienten c
    left join financiering f
        on f.client_id = c.client_id
)

select * from final;
