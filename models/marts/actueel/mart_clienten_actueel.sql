-- Snapshot: alleen cliënten die vandaag in zorg zijn,
-- verrijkt met huidige hoofdlocatie en primaire financieringsvorm.

with clienten as (

    select * from {{ ref('mart_clienten') }}
    where is_in_zorg = 1

),

hoofdlocatie as (

    -- Huidige hoofdlocatie per client (actieve MAIN-koppeling)
    select
        client_id,
        locatie_id,
        locatienaam,
        row_number() over (
            partition by client_id
            order by
                case
                    when locatie_einddatum is null
                      or locatie_einddatum >= cast(getdate() as date)
                    then 0 else 1
                end,
                locatie_startdatum desc
        ) as rn

    from {{ ref('int_clienten_met_locaties') }}
    where type_toekenning = 'MAIN'

),

financiering as (

    select * from {{ ref('int_clienten_financiering_actueel') }}

)

select
    c.*,

    -- Hoofdlocatie
    hl.locatie_id       as hoofdlocatie_id,
    hl.locatienaam      as hoofdlocatienaam,

    -- Financiering
    f.financiering

from clienten c
left join hoofdlocatie hl
    on hl.client_id = c.client_id
    and hl.rn = 1
left join financiering f
    on f.client_id = c.client_id
