/*
===============================================================================
DDL-script: Bronze-tabellen aanmaken
===============================================================================
Doel van het script:
    Dit script maakt tabellen aan in het 'bronze'-schema en verwijdert bestaande 
    tabellen als deze al bestaan. 
    Voer dit script uit om de DDL-structuur van de 'bronze'-tabellen opnieuw te definiëren.
===============================================================================
*/

USE OdionDataPlatform;
GO

IF OBJECT_ID('bronze.ons_clients', 'U') IS NOT NULL
    DROP TABLE bronze.ons_clients;
GO

CREATE TABLE bronze.ons_clients
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
