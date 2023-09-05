USE [{0}{1}]
SELECT '['+DB_NAME()+'].['+schema_name( t.schema_id) +'].['+t.name+']' FULLNAME,
	schema_name( t.schema_id) SCHEMANAME,t.name TABLENAME,c.CPY_CODE, c.CPY_LABEL, sum(p.rows) ROWS_COUNT
	FROM sys.tables t left join sys.partitions p on t.object_id = p.object_id
	 left join e2sGeneral.dbo.COMPANY c  on schema_name( t.schema_id) = c.CPY_CODE
	WHERE t.type = @UTYPE and t.name like @PATTERN 
	GROUP BY t.name, t.schema_id,t.object_id, c.CPY_LABEL,cpy_code 
	Having sum(p.rows) > 0 
ORDER BY schema_name( t.schema_id),t.name
