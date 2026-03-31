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

from [OdionDataPlatform].[dbo].[mart_documenten]

where (
        bestandsnaam LIKE '%identiteit%'
    or bestandsnaam LIKE '%id-bewijs%'
    or bestandsnaam LIKE '%idbewijs%'
    or bestandsnaam LIKE '%paspoort%'
    or bestandsnaam LIKE '%rijbewijs%'
    or bestandsnaam LIKE '%verblijfsvergunning%'
    or bestandsnaam LIKE '%legitimatie%'
    or bestandsnaam LIKE '%ID%' COLLATE Latin1_General_CS_AS -- alleen hoofdletters ID
    or bestandsnaam LIKE '%[^a-z]id[^a-z]%' -- voorafgegaan door niet-letter (bv. -id., _id.)
    or beschrijving LIKE '%identiteit%'
    or beschrijving LIKE '%id-bewijs%'
    or beschrijving LIKE '%idbewijs%'
    or beschrijving LIKE '%paspoort%'
    or beschrijving LIKE '%rijbewijs%'
    or beschrijving LIKE '%legitimatie%'
    or beschrijving LIKE '%verblijfsvergunning%'
    or beschrijving LIKE '%ID%' COLLATE Latin1_General_CS_AS -- alleen hoofdletters ID
    or beschrijving LIKE '%[^a-z]id[^a-z]%'
    )
    and bestandsnaam NOT LIKE '%covid%'
    and beschrijving NOT LIKE '%covid%'
    

