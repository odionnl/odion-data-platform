/*
===============================================================================
Stored Procedure: Laad Bronze-laag (Bron -> Bronze)
===============================================================================
Doel van het script:
    Deze stored procedure laadt data in het 'bronze'-schema vanuit de externe 
    SQL Server-database 'Ons_Plan_2'. 
    De procedure voert de volgende acties uit:
    - Leegt de bronze-tabellen voordat nieuwe data wordt geladen.
    - Laadt data met een `INSERT ... SELECT` vanuit de brontabel dbo.clients 
      in de database 'Ons_Plan_2'.

Parameters:
    Geen.
    Deze stored procedure accepteert geen parameters en retourneert geen waarden.

Gebruik:
    EXEC bronze.load_bronze;
===============================================================================
*/
USE OdionDataPlatform;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE @start_time DATETIME, 
            @end_time DATETIME, 
            @batch_start_time DATETIME, 
            @batch_end_time DATETIME;

    -- =============================================================================
    -- Logging van start laadtijd
    -- =============================================================================

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Laden van Bronze-laag gestart';
        PRINT '================================================';

        PRINT '------------------------------------------------';
        PRINT 'Laden van ONS-tabellen';
        PRINT '------------------------------------------------';

-- =============================================================================
-- bronze.ons_clients
-- =============================================================================

        SET @start_time = GETDATE();

        PRINT '>> Leegmaken van tabel: bronze.ons_clients';
        TRUNCATE TABLE bronze.ons_clients;

        PRINT '>> Data invoegen in: bronze.ons_clients vanuit Ons_Plan_2.dbo.clients';
        INSERT INTO bronze.ons_clients
        (
        objectId,
        identificationNo,
        dateOfBirth,
        deathDate,
        lastName,
        birthName,
        givenName,
        partnerName,
        initials,
        prefix,
        [name]
        )
    SELECT
        objectId,
        identificationNo,
        dateOfBirth,
        deathDate,
        lastName,
        birthName,
        givenName,
        partnerName,
        initials,
        prefix,
        [name]
    FROM Ons_Plan_2.dbo.clients;

        SET @end_time = GETDATE();

        PRINT '>> Laadtijd: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconden';
        PRINT '>> -------------';

-- =============================================================================
-- bronze.ons_locations
-- =============================================================================

        SET @start_time = GETDATE();

        PRINT '>> Leegmaken van tabel: bronze.ons_locations';
        TRUNCATE TABLE bronze.ons_locations;

        PRINT '>> Data invoegen in: bronze.ons_locations vanuit Ons_Plan_2.dbo.locations';
        INSERT INTO bronze.ons_locations
        (
        objectId,
        beginDate,
        endDate,
        [name],
        parentObjectId,
        materializedPath
        )
    SELECT
        objectId,
        beginDate,
        endDate,
        [name],
        parentObjectId,
        materializedPath
    FROM Ons_Plan_2.dbo.locations;

        SET @end_time = GETDATE();

        PRINT '>> Laadtijd: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconden';
        PRINT '>> -------------';

-- =============================================================================
-- Logging van totale laadtijd
-- =============================================================================

        SET @batch_end_time = GETDATE();
        PRINT 'Totale laadtijd (batch): ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconden';
        PRINT 'Laden van Bronze-laag voltooid.';

-- =============================================================================
-- Foutmeldingen
-- =============================================================================

    END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'FOUT OPGETREDEN TIJDENS HET LADEN VAN DE BRONZE-LAAG'
		PRINT 'Foutmelding: ' + ERROR_MESSAGE();
		PRINT 'Foutnummer: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Foutstatus: ' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END;
GO
