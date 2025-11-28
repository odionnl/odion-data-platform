{{ config(materialized='table') }}

WITH numbers AS (
    SELECT TOP (DATEDIFF(day, '2015-01-01', '2035-12-31') + 1)
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
    FROM sys.all_objects a
    CROSS JOIN sys.all_objects b   -- 🔹 gives you plenty of rows
),

spine AS (
    SELECT DATEADD(day, n, CAST('2015-01-01' AS date)) AS datum
    FROM numbers
),

dim AS (
    SELECT
        datum,

        -- Basis
        YEAR(datum) AS jaar,
        MONTH(datum) AS maandnummer,
        DAY(datum) AS dagnummer,

        -- Nederlandse maandnaam
        FORMAT(datum, 'MMMM', 'nl-NL') AS maandnaam,

        -- Nederlandse weekdag
        FORMAT(datum, 'dddd', 'nl-NL') AS weekdag_naam,
        DATEPART(weekday, datum) AS weekdag_nummer,
        DATEPART(isowk, datum) AS weeknummer,

        CONCAT(YEAR(datum), '-W', FORMAT(DATEPART(isowk, datum), '00')) AS jaar_week,

        DATEPART(quarter, datum) AS kwartaal,
        CONCAT('Q', DATEPART(quarter, datum)) AS kwartaal_label,

        DATEPART(dayofyear, datum) AS dag_van_jaar,

        CASE WHEN DATEPART(weekday, datum) IN (2,3,4,5,6) THEN 1 ELSE 0 END AS is_werkdag,
        CASE WHEN DATEPART(weekday, datum) IN (1,7) THEN 1 ELSE 0 END AS is_weekend
    FROM spine
)

SELECT *
FROM dim;
