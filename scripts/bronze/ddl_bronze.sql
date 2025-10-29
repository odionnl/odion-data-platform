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

-- =============================================================================
-- bronze.ons_clients
-- =============================================================================

IF OBJECT_ID('bronze.ons_clients', 'U') IS NOT NULL
    DROP TABLE bronze.ons_clients;
GO

CREATE TABLE bronze.ons_clients
(
    objectId INT,
    identificationNo NVARCHAR(50),
    dateOfBirth DATE,
    deathDate DATE,
    lastName NVARCHAR(50),
    birthName NVARCHAR(50),
    givenName NVARCHAR(50),
    partnerName NVARCHAR(50),
    initials NVARCHAR(50),
    prefix NVARCHAR(50),
    [name] NVARCHAR(50)
);
GO

-- =============================================================================
-- bronze.ons_locations
-- =============================================================================

IF OBJECT_ID('bronze.ons_locations', 'U') IS NOT NULL
    DROP TABLE bronze.ons_locations;
GO

CREATE TABLE bronze.ons_locations
(
    objectId INT,
    beginDate DATE,
    endDate DATE,
    [name] NVARCHAR(250),
    parentObjectId INT,
    materializedPath NVARCHAR(50)
);
GO

-- =============================================================================
-- bronze.ons_location_assignments
-- =============================================================================

IF OBJECT_ID('bronze.ons_location_assignments', 'U') IS NOT NULL
    DROP TABLE bronze.ons_location_assignments;
GO

CREATE TABLE bronze.ons_location_assignments
(
    clientObjectId INT,
    locationObjectId INT,
    beginDate DATE,
    endDate DATE,
    locationType NVARCHAR(50),
);
GO