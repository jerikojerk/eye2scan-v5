USE [e2sMaster]
SELECT
 SCHEMA_NAME(sysTab.SCHEMA_ID) as SchemaName,
 sysTab.NAME AS TableName,
 db_name() as DatabaseName,
 '['+ db_name()+'].['+SCHEMA_NAME(sysTab.SCHEMA_ID)+'].['+sysTab.NAME+']' as express
FROM sys.tables sysTab
WHERE systab.type = 'U' AND sysTab.NAME in ('__EFMigrationsHistory','control','model_import','model_control')
order by 1,2
