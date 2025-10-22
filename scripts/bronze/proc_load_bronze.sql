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

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE @start_time DATETIME, 
            @end_time DATETIME, 
            @batch_start_time DATETIME, 
            @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Laden van Bronze-laag gestart';
        PRINT '================================================';

        PRINT '------------------------------------------------';
        PRINT 'Laden van ONS-tabellen';
        PRINT '------------------------------------------------';

        -- Starttijd voor tabel
        SET @start_time = GETDATE();

        PRINT '>> Leegmaken van tabel: bronze.ons_clients';
        TRUNCATE TABLE bronze.ons_clients;

        PRINT '>> Data invoegen in: bronze.ons_clients vanuit Ons_Plan_2.dbo.clients';
        INSERT INTO bronze.ons_clients
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
    FROM Ons_Plan_2.dbo.clients;

        SET @end_time = GETDATE();

        PRINT '>> Laadtijd: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconden';
        PRINT '>> -------------';

        SET @batch_end_time = GETDATE();
        PRINT 'Totale laadtijd (batch): ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconden';
        PRINT 'Laden van Bronze-laag voltooid.';

    END TRY
    BEGIN CATCH
        PRINT '!!! FOUT tijdens laden van Bronze-laag !!!';
        PRINT ERROR_MESSAGE();
    END CATCH
END;
GO
