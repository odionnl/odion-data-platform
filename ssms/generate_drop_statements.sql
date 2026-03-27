-- Genereer DROP statements voor alle objecten in dbt schema's
SELECT
    'DROP ' + 
    CASE TABLE_TYPE WHEN 'VIEW' THEN 'VIEW' ELSE 'TABLE' END + 
    ' IF EXISTS [' + TABLE_SCHEMA + '].[' + TABLE_NAME + '];'
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('dbo_staging', 'dbo_intermediate', 'dbo_marts')
ORDER BY TABLE_TYPE DESC, TABLE_SCHEMA, TABLE_NAME;