with leeftijden as (

    -- Genereer rijen voor leeftijd 0 t/m 120
    select top (121)
        row_number() over (order by object_id) - 1 as leeftijd
    from sys.all_objects

),

leeftijden_met_onbekend as (

    select leeftijd from leeftijden

    union all

    -- Sentinel voor onbekende leeftijd (NULL in mart_clienten wordt -1)
    select -1 as leeftijd

)

select
    leeftijd,

    {{ get_leeftijdsgroep1('leeftijd') }}          as leeftijdsgroep1,
    {{ get_leeftijdsgroep1_volgorde('leeftijd') }} as leeftijdsgroep1_volgorde,

    {{ get_leeftijdsgroep2('leeftijd') }}          as leeftijdsgroep2,
    {{ get_leeftijdsgroep2_volgorde('leeftijd') }} as leeftijdsgroep2_volgorde,

    {{ get_leeftijdsgroep3('leeftijd') }}          as leeftijdsgroep3,
    {{ get_leeftijdsgroep3_volgorde('leeftijd') }} as leeftijdsgroep3_volgorde

from leeftijden_met_onbekend
