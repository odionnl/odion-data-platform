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

-- =============================================================================
-- silver.ons_clients
-- =============================================================================

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
    FROM OdionDataPlatform.bronze.ons_clients
    WHERE dateOfBirth IS NOT NULL AND identificationNo NOT LIKE '%[^0-9]%';

		SET @end_time = GETDATE();
        PRINT '>> Laadtijd: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconden';
        PRINT '>> -------------';

-- =============================================================================
-- silver.ons_locations
-- =============================================================================

        SET @start_time = GETDATE();
		PRINT '>> Leegmaken van tabel: silver.ons_locations';
		TRUNCATE TABLE silver.ons_locations;
		PRINT '>> Data invoegen in: silver.ons_locations';
		INSERT INTO silver.ons_locations
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
    FROM OdionDataPlatform.bronze.ons_locations;

		SET @end_time = GETDATE();
        PRINT '>> Laadtijd: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconden';
        PRINT '>> -------------';

-- =============================================================================
-- silver.ons_location_assignments
-- =============================================================================

        SET @start_time = GETDATE();
		PRINT '>> Leegmaken van tabel: silver.ons_location_assignments';
		TRUNCATE TABLE silver.ons_location_assignments;
		PRINT '>> Data invoegen in: silver.ons_location_assignments';
		INSERT INTO silver.ons_location_assignments
        (
        clientObjectId,
        locationObjectId,
        beginDate,
        endDate,
        locationType
        )
    SELECT
        clientObjectId,
        locationObjectId,
        beginDate,
        endDate,
        locationType
    FROM Ons_Plan_2.dbo.location_assignments;

		SET @end_time = GETDATE();
        PRINT '>> Laadtijd: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconden';
        PRINT '>> -------------';

-- =============================================================================
-- silver.ons_care_allocations
-- =============================================================================

        SET @start_time = GETDATE();
		PRINT '>> Leegmaken van tabel: silver.ons_care_allocations';
		TRUNCATE TABLE silver.ons_care_allocations;
		PRINT '>> Data invoegen in: silver.ons_care_allocations';
		INSERT INTO silver.ons_care_allocations
        (
        clientObjectId,
        dateBegin,
        dateEnd
        )
    SELECT
        clientObjectId,
        dateBegin,
        dateEnd
    FROM Ons_Plan_2.dbo.care_allocations;

		SET @end_time = GETDATE();
        PRINT '>> Laadtijd: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconden';
        PRINT '>> -------------';

-- =============================================================================
-- silver.dim_date
-- =============================================================================

        SET @start_time = GETDATE();
		PRINT '>> Leegmaken van tabel: silver.dim_date';
		TRUNCATE TABLE silver.dim_date;
		PRINT '>> Data invoegen in: silver.dim_date';
WITH
        DateSeries
        AS
        (
                            SELECT CAST('2020-01-01' AS DATE) AS d
            UNION ALL
                SELECT DATEADD(DAY, 1, d)
                FROM DateSeries
                WHERE d < '2030-12-31'
        )
    INSERT INTO silver.dim_date
        (date_key, full_date, [day], [month], month_name, [quarter], [year], day_of_week, day_name, is_weekend)
    SELECT
        YEAR(d) * 10000 + MONTH(d) * 100 + DAY(d) AS date_key,
        d AS full_date,
        DAY(d) AS [day],
        MONTH(d) AS [month],
        DATENAME(MONTH, d) AS month_name,
        DATEPART(QUARTER, d) AS [quarter],
        YEAR(d) AS [year],
        DATEPART(WEEKDAY, d) AS day_of_week, -- 1 = Sunday by default
        DATENAME(WEEKDAY, d) AS day_name,
        CASE WHEN DATENAME(WEEKDAY, d) IN ('Saturday','Sunday') THEN 1 ELSE 0 END AS is_weekend
    FROM DateSeries
    OPTION
    (MAXRECURSION
    0);


/*
    ===============================================================================
    END OF BATCH: ONS-tabellen
    ===============================================================================
    */
SET @batch_end_time = GETDATE();
PRINT '=========================================='
PRINT 'Laden van Silver-laag is voltooid';
PRINT '   - Totale laadtijd: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconden';
PRINT '=========================================='

END
    TRY
	BEGIN CATCH
PRINT '=========================================='
PRINT 'FOUT OPGETREDEN TIJDENS HET LADEN VAN DE SILVER-LAAG'
PRINT 'Foutmelding: ' + ERROR_MESSAGE();
PRINT 'Foutnummer: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
PRINT 'Foutstatus: ' + CAST (ERROR_STATE() AS NVARCHAR);
PRINT '=========================================='
END CATCH
END
