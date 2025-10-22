/*
===============================================================================
Stored Procedure: Laad Silver-laag (Bronze -> Silver)
===============================================================================
Doel van het script:
    Deze stored procedure voert het ETL-proces (Extract, Transform, Load) uit 
    om de tabellen in het 'silver'-schema te vullen met gegevens uit het 
    'bronze'-schema.
	Uitgevoerde acties:
		- Leegt (trunct) de Silver-tabellen.
		- Voegt getransformeerde en opgeschoonde gegevens uit Bronze in Silver-tabellen in.
		
Parameters:
    Geen. 
	Deze stored procedure accepteert geen parameters en retourneert geen waarden.

Gebruik:
    EXEC silver.load_silver;
===============================================================================
*/
USE OdionDataPlatform;
GO

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Laden van Silver-laag';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Laden van ONS-tabellen';
		PRINT '------------------------------------------------';

		-- Laden van silver.ons_clients
        SET @start_time = GETDATE();
		PRINT '>> Leegmaken van tabel: silver.ons_clients';
		TRUNCATE TABLE silver.ons_clients;
		PRINT '>> Data invoegen in: silver.ons_clients';
		INSERT INTO silver.ons_clients
        (
        objectId,
        identificationNo,
        dateOfBirth,
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
        lastName,
        birthName,
        givenName,
        partnerName,
        initials,
        prefix,
        [name]
    FROM OdionDataPlatform.bronze.ons_clients;

		SET @end_time = GETDATE();
        PRINT '>> Laadtijd: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconden';
        PRINT '>> -------------';


		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Laden van Silver-laag is voltooid';
        PRINT '   - Totale laadtijd: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconden';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'FOUT OPGETREDEN TIJDENS HET LADEN VAN DE SILVER-LAAG'
		PRINT 'Foutmelding: ' + ERROR_MESSAGE();
		PRINT 'Foutnummer: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Foutstatus: ' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
