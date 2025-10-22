/*
===============================================================================
DDL-script: silver-tabellen aanmaken
===============================================================================
Doel van het script:
    Dit script maakt tabellen aan in het 'silver'-schema en verwijdert bestaande 
    tabellen als deze al bestaan. 
    Voer dit script uit om de DDL-structuur van de 'silver'-tabellen opnieuw te definiëren.
===============================================================================
*/

USE OdionDataPlatform;
GO

-- silver.ons_clients
IF OBJECT_ID('silver.ons_clients', 'U') IS NOT NULL
    DROP TABLE silver.ons_clients;
GO

CREATE TABLE silver.ons_clients
(
    objectId INT,
    identificationNo NVARCHAR(50),
    dateOfBirth DATE,
    lastName NVARCHAR(50),
    birthName NVARCHAR(50),
    givenName NVARCHAR(50),
    partnerName NVARCHAR(50),
    initials NVARCHAR(50),
    prefix NVARCHAR(50),
    [name] NVARCHAR(50)
);
GO

-- silver.ons_locations
IF OBJECT_ID('silver.ons_locations', 'U') IS NOT NULL
    DROP TABLE silver.ons_locations;
GO

CREATE TABLE silver.ons_locations
(
    objectId INT,
    beginDate DATE,
    endDate DATE,
    [name] NVARCHAR(250),
    parentObjectId INT,
    materializedPath NVARCHAR(50)
);
GO