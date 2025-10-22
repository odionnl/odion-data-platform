/*
=============================================================
Database en Schema's aanmaken
=============================================================
Doel van het script:
    Dit script maakt een nieuwe database met de naam 'OdionDataPlatform' aan, 
    nadat is gecontroleerd of deze al bestaat. 
    Als de database al bestaat, wordt deze verwijderd en opnieuw aangemaakt. 
    Daarnaast maakt het script drie schema's aan binnen de database: 
    'bronze', 'silver' en 'gold'.
	
WAARSCHUWING:
    Het uitvoeren van dit script zal de gehele database 'OdionDataPlatform' verwijderen 
    als deze al bestaat. Alle gegevens in de database zullen permanent worden verwijderd. 
    Ga voorzichtig te werk en zorg ervoor dat je over de juiste back-ups beschikt 
    voordat je dit script uitvoert.
*/

USE master;
GO

-- Verwijder en maak de database 'OdionDataPlatform' opnieuw aan
IF EXISTS (SELECT 1
FROM sys.databases
WHERE name = 'OdionDataPlatform')
BEGIN
    ALTER DATABASE OdionDataPlatform SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE OdionDataPlatform;
END;
GO

-- Maak de database 'OdionDataPlatform' aan
CREATE DATABASE OdionDataPlatform;
GO

USE OdionDataPlatform;
GO

-- Maak schema's aan
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
