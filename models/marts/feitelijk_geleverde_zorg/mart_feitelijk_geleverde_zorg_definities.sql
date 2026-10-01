-- Definities per datapunt voor de verantwoording feitelijk geleverde zorg.
-- Kleine, statische dimensietabel: één rij per datapunt, met de streefwaarde
-- en een leesbare definitie van wanneer het datapunt behaald is.
-- Koppelt in Power BI op `datapunt` aan mart_feitelijk_geleverde_zorg_per_client_datapunt,
-- zodat de gerealiseerde score tegen de norm afgezet kan worden.
--
-- De norm is een fractie tussen 0 en 1, net als client_score in
-- mart_feitelijk_geleverde_zorg_per_client. Power BI formatteert dit als percentage.
--
-- LET OP: de normwaarden en definities hieronder zijn organisatiebeleid, geen afgeleide data.
-- Pas ze hier aan als het beleid wijzigt — dit is de enige plek waar ze staan.

with definities as (

    select * from (values
        --  datapunt,               datapunt_label,         volgorde, norm,
        --      definitie
            ('actueel_zorgplan',     'Actueel zorgplan',     1,        0.75,
                N'Op de peildatum is er een geldig zorgplan aanwezig voor de cliënt met de status ''actief''.'),
            ('recente_rapportages',  'Recente rapportages',  2,        0.90,
                N'In de beoordelingsperiode is er minimaal 1 rapportage gedaan. Dit mag elke vorm van rapportage zijn (Rapportage, Medisch, Stemming, etc.).'),
            ('medicatie_afgetekend', 'Medicatie afgetekend', 3,        0.90,
                N'In de beoordelingsperiode is minimaal 1 keer de voorgeschreven medicatie in het toedienoverzicht van de cliënt toegediend, aangereikt of klaargezet.'),
            ('zorgdossier_bekeken',  'Zorgdossier bekeken',  4,        0.90,
                N'In de beoordelingsperiode is het zorgdossier minimaal 1 keer geopend of aangepast door een zorgmedewerker. Deze zorgmedewerker heeft minimaal 1 keer op het rooster gestaan van de locatie van de cliënt in de beoordelingsperiode.')
    ) as v (datapunt, datapunt_label, datapunt_volgorde, norm, definitie)

)

select
    cast(datapunt as varchar(50))           as datapunt,
    cast(datapunt_label as varchar(100))    as datapunt_label,
    cast(datapunt_volgorde as int)          as datapunt_volgorde,
    cast(norm as decimal(5,2))              as norm,
    cast(definitie as nvarchar(500))        as definitie

from definities
