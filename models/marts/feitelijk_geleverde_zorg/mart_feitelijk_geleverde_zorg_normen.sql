-- Normen per datapunt voor de verantwoording feitelijk geleverde zorg.
-- Kleine, statische dimensietabel: één rij per datapunt, met de streefwaarde.
-- Koppelt in Power BI op `datapunt` aan mart_feitelijk_geleverde_zorg_datapunten,
-- zodat de gerealiseerde score tegen de norm afgezet kan worden.
--
-- De norm is een fractie tussen 0 en 1, net als client_score in
-- mart_feitelijk_geleverde_zorg. Power BI formatteert dit als percentage.
--
-- LET OP: de normwaarden hieronder zijn organisatiebeleid, geen afgeleide data.
-- Pas ze hier aan als het beleid wijzigt — dit is de enige plek waar ze staan.

with normen as (

    select * from (values
        --  datapunt,               datapunt_label,         volgorde, norm
            ('geldig_zorgplan',      'Geldig zorgplan',      1,        0.75),
            ('recente_rapportages',  'Recente rapportages',  2,        0.90),
            ('medicatie_toegediend', 'Medicatie toegediend', 3,        0.90),
            ('zorgplan_ingezien',    'Zorgplan ingezien',    4,        0.90)
    ) as v (datapunt, datapunt_label, datapunt_volgorde, norm)

)

select
    cast(datapunt as varchar(50))           as datapunt,
    cast(datapunt_label as varchar(100))    as datapunt_label,
    cast(datapunt_volgorde as int)          as datapunt_volgorde,
    cast(norm as decimal(5,2))              as norm

from normen
