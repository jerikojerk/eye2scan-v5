USE [{0}{1}];
with   REPORT_E2S (REFQUERY) as (
	select  distinct * from string_split( (
		select STRING_AGG( RPT_IMPORT_MAP,'|') from [e2sGeneral].[dbo].REPORT where RPT_SELECTED = 1  ),'|')
)
select r.REFQUERY,x.compagny,x.DatabaseName,x.SchemaName,x.TableName,x.RowCounts,x.TotalSpaceKB
from  REPORT_E2S r left join 
(SELECT
	cy.CPY_LABEL as compagny ,
	db_name() as DatabaseName,
	SCHEMA_NAME(sysTab.SCHEMA_ID) as SchemaName,
	sysTab.NAME AS TableName, 
	parti.rows AS RowCounts,
	SUM(alloUni.total_pages) * 8 AS TotalSpaceKB,
	SUM(alloUni.used_pages) * 8 AS UsedSpaceKB,
	(SUM(alloUni.total_pages) - SUM(alloUni.used_pages)) * 8 AS UnusedSpaceKB
FROM sys.tables sysTab
INNER JOIN sys.indexes ind ON sysTab.OBJECT_ID = ind.OBJECT_ID and ind.Index_ID<=1
INNER JOIN sys.partitions parti ON ind.OBJECT_ID = parti.OBJECT_ID AND ind.index_id = parti.index_id
INNER JOIN sys.allocation_units alloUni ON parti.partition_id = alloUni.container_id
LEFT JOIN [e2sGeneral].[dbo].COMPANY cy on cy.CPY_CODE = SCHEMA_NAME(sysTab.SCHEMA_ID) 
WHERE sysTab.is_ms_shipped = 0 AND ind.OBJECT_ID > 255 AND parti.rows>0
GROUP BY cy.CPY_LABEL,sysTab.Name, parti.Rows,sysTab.SCHEMA_ID
) x	on ( x.TableName = r.REFQUERY  or 'T_'+r.REFQUERY = x.TableName )
order by x.SchemaName,REFQUERY,x.TableName
