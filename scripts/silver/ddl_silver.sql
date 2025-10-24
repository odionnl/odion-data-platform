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

-- =============================================================================
-- silver.ons_clients
-- =============================================================================

IF OBJECT_ID('silver.ons_clients', 'U') IS NOT NULL
    DROP TABLE silver.ons_clients;
GO

CREATE TABLE silver.ons_clients
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
-- silver.ons_locations
-- =============================================================================

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

-- =============================================================================
-- silver.ons_location_assignments
-- =============================================================================

IF OBJECT_ID('silver.ons_location_assignments', 'U') IS NOT NULL
    DROP TABLE silver.ons_location_assignments;
GO

CREATE TABLE silver.ons_location_assignments
(
    clientObjectId INT,
    locationObjectId INT,
    beginDate DATE,
    endDate DATE,
    locationType NVARCHAR(50),
);
GO

-- =============================================================================
-- silver.ons_care_allocations
-- =============================================================================

IF OBJECT_ID('silver.ons_care_allocations', 'U') IS NOT NULL
    DROP TABLE silver.ons_care_allocations;
GO

CREATE TABLE silver.ons_care_allocations
(
    clientObjectId INT,
    dateBegin DATE,
    dateEnd DATE
);
GO


-- =============================================================================
-- silver.dim_date
-- =============================================================================

IF OBJECT_ID('silver.dim_date', 'U') IS NOT NULL
    DROP TABLE silver.dim_date;
GO

CREATE TABLE silver.dim_date
(
    date_key INT PRIMARY KEY,
    -- e.g., 20251023
    full_date DATE NOT NULL,
    [day] INT,
    [month] INT,
    month_name VARCHAR(20),
    [quarter] INT,
    [year] INT,
    day_of_week INT,
    day_name VARCHAR(20),
    is_weekend BIT
);
GO
