-- Is het mogelijk om voor ons een lijst uit te draaien van alle inkomensverklaringen in ONS? (in ieder geval degene met een juiste benaming)
-- Het zou dan gaan om documenten met in de omschrijving: “Inkomensverklaring”, “Inkomen” en “IB60”.

select
    document_id,
    client_id,
    clientnummer,
    beschrijving,
    bestandsnaam,
    documentstatus,
    labels,
    is_vertrouwelijk,
    aangemaakt_op,
    gewijzigd_op

from [OdionDataPlatformCC].[dbo_marts].[mart_documenten]

where (
    bestandsnaam LIKE '%inkomen%'
    or bestandsnaam LIKE 'inkomsten'
    or bestandsnaam LIKE '%ib60%'
    or bestandsnaam LIKE '%ib-60%'
    or bestandsnaam LIKE '%ib 60%'
    or beschrijving LIKE '%inkomen%'
    or beschrijving LIKE '%inkomsten%'
    or beschrijving LIKE '%ib60%'
    or beschrijving LIKE '%ib-60%'
    or beschrijving LIKE '%ib 60%'
    )

    

