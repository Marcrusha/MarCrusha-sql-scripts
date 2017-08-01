/*
Triage all Indexes on all Tables

Sometimes, you want to Rebuild/Reorganize all Indexes on a Table as compared to one Index at a time.

ALTER INDEX ALL ON dbo.Table_Name
REBUILD WITH (ONLINE = ON)
*/

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH
	sys_indexes AS (
		SELECT	O.[object_id],
				I.index_id,
				[table_name]	= O.[Name],
				[schema]		= SC.[Name],
				[table_modified] = O.modify_date,
				[Last Stats Update] = CONVERT(CHAR(10), STATS_DATE(O.[object_id], I.index_id), 101)
		FROM sys.indexes AS I
			INNER JOIN sys.objects AS O ON O.[object_id] = I.[object_id]
			INNER JOIN sys.schemas AS SC ON O.[schema_id] = SC.[schema_id]
			LEFT OUTER JOIN sys.fulltext_indexes AS FI ON I.[object_id] = FI.[object_id] 
				AND I.index_id = FI.unique_index_id
		WHERE O.[type] IN ('U','V')),
		
	sys_index_operational_stats AS (
		SELECT	[object_id],
				index_id,
				leaf_allocation_count -- this equates to page splits, and it counts both good and bad
		FROM [sys].[dm_db_index_operational_stats](DB_ID(), NULL, NULL, NULL)),

	sys_index_usage_stats AS (
		SELECT	O.[object_id],
				user_seeks			= SUM(U.user_seeks),
				user_scans			= SUM(U.user_scans),
				user_lookups		= SUM(U.user_lookups),
				user_updates		= SUM(U.user_updates),
				last_user_read		= CASE
										WHEN MAX(COALESCE(U.last_user_seek, '1970-01-01 00:00:00')) > MAX(COALESCE(U.last_user_scan, '1970-01-01 00:00:00')) AND MAX(COALESCE(U.last_user_seek, '1970-01-01 00:00:00')) > MAX(COALESCE(U.last_user_lookup, '1970-01-01 00:00:00')) THEN MAX(COALESCE(U.last_user_seek, '1970-01-01 00:00:00'))
										WHEN MAX(COALESCE(U.last_user_scan, '1970-01-01 00:00:00')) > MAX(COALESCE(U.last_user_seek, '1970-01-01 00:00:00')) AND MAX(COALESCE(U.last_user_scan, '1970-01-01 00:00:00')) > MAX(COALESCE(U.last_user_lookup, '1970-01-01 00:00:00')) THEN MAX(COALESCE(U.last_user_scan, '1970-01-01 00:00:00'))
										WHEN MAX(COALESCE(U.last_user_lookup, '1970-01-01 00:00:00')) > MAX(COALESCE(U.last_user_scan, '1970-01-01 00:00:00')) AND MAX(COALESCE(U.last_user_lookup, '1970-01-01 00:00:00')) > MAX(COALESCE(U.last_user_seek, '1970-01-01 00:00:00')) THEN MAX(COALESCE(U.last_user_lookup, '1970-01-01 00:00:00')) END,
				last_system_read	= CASE
										WHEN MAX(COALESCE(U.last_system_seek, '1970-01-01 00:00:00')) > MAX(COALESCE(U.last_system_scan, '1970-01-01 00:00:00')) AND MAX(COALESCE(U.last_system_seek, '1970-01-01 00:00:00')) > MAX(COALESCE(U.last_system_lookup, '1970-01-01 00:00:00')) THEN MAX(COALESCE(U.last_system_seek, '1970-01-01 00:00:00'))
										WHEN MAX(COALESCE(U.last_system_scan, '1970-01-01 00:00:00')) > MAX(COALESCE(U.last_system_seek, '1970-01-01 00:00:00')) AND MAX(COALESCE(U.last_system_scan, '1970-01-01 00:00:00')) > MAX(COALESCE(U.last_system_lookup, '1970-01-01 00:00:00')) THEN MAX(COALESCE(U.last_system_scan, '1970-01-01 00:00:00'))
										WHEN MAX(COALESCE(U.last_system_lookup, '1970-01-01 00:00:00')) > MAX(COALESCE(U.last_system_scan, '1970-01-01 00:00:00')) AND MAX(COALESCE(U.last_system_lookup, '1970-01-01 00:00:00')) > MAX(COALESCE(U.last_system_seek, '1970-01-01 00:00:00')) THEN MAX(COALESCE(U.last_system_lookup, '1970-01-01 00:00:00')) END
		FROM sys.indexes AS I
			INNER JOIN sys.objects AS O ON I.[object_id] = O.[object_id]
			INNER JOIN sys.dm_db_index_usage_stats AS U ON I.[object_id] = U.[object_id] -- Statistics are zeroed during online rebuilds in 2012
				AND I.index_id = U.index_id
		WHERE U.database_id = DB_ID()
			AND O.[type] IN ('U','V')
		GROUP BY O.[object_id]),
		
	sys_index_physical_stats AS (
		SELECT	[object_id],
				index_id,
				pages					= SUM(page_count),
				[page_density]			= SUM(ROUND(avg_page_space_used_in_percent, 1)),
				page_fragmentation		= CASE WHEN MAX(index_type_desc) <> 'HEAP' THEN SUM(ROUND(avg_fragmentation_in_percent, 1)) ELSE NULL END,
				[rows]					= SUM(record_count),
				fw_records				= SUM(forwarded_record_count)
		FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL , NULL, 'SAMPLED')
		WHERE alloc_unit_type_desc = 'IN_ROW_DATA'
		GROUP BY [object_id], index_id, partition_number),
	
	IndexSpaceUsed AS (
		SELECT	[object_id]			= I.[object_id],
				[index_ID]			= I.index_id,
				[Size (KB)]			= SUM(S.used_page_count) * 8			
		FROM sys.dm_db_partition_stats AS S
			INNER JOIN sys.indexes AS I ON S.[object_id] = I.[object_id] AND S.index_id = I.index_id
			INNER JOIN sys.objects AS O ON O.[object_id] = I.[object_id]
			INNER JOIN sys.schemas AS SC ON O.SCHEMA_ID = SC.SCHEMA_ID
		WHERE O.[type] IN ('U','V')		
		GROUP BY I.[object_id], I.index_id, I.name, S.used_page_count, O.create_date, O.modify_date, SC.name),

	RowsPerTable AS (
		SELECT [object_id], rows = SUM(rows)
		FROM [sys].[partitions]
		GROUP BY [object_id]
	)
		
	SELECT	[Database]				= DB_NAME(DB_ID()),
			[Schema]				= SI.[schema],
			[Table]					= SI.[table_name],
			[Reads]					= SUM(DUS.user_seeks + DUS.user_scans + DUS.user_lookups),
			[Seeks]					= SUM(DUS.user_seeks), 
			[Scans]					= SUM(DUS.user_scans),
			[Lookups]				= SUM(DUS.user_lookups),
			[Writes]				= SUM(DUS.user_updates),
			[Reads Per Write]		= CONVERT(DECIMAL(10,2), MAX(CASE WHEN DUS.user_updates < 1 THEN 100.00 ELSE 1.00 * (DUS.user_seeks + DUS.user_scans + DUS.user_lookups) / DUS.user_updates END)),
			[Last User Read]		= CONVERT(DATE, MAX(DUS.last_user_read)),
			[Last System Read]		= CONVERT(DATE, MAX(DUS.last_system_read)),
			[Page Count]			= SUM(FR.pages),
			[Page Density]			= ROUND(AVG(FR.pages * FR.[page_density])/SUM(FR.pages),1),
			[Page Fragmentation]	= ROUND(AVG(FR.page_fragmentation),1),			
			[Rows]					= SUM(P.[rows]),
			[IX Size (MB)]			= SUM(SU.[Size (KB)]/1024),
			[Wasted space (GB)]		= SUM((FR.page_fragmentation/100)*(SU.[Size (KB)]/1024/1024)),							
			[Last Stats Update]		= MAX(SI.[Last Stats Update]),		
			[Forwarded Records]		= SUM(FR.fw_records),
			[IX Page Splits]		= SUM(DOS.leaf_allocation_count)
		FROM [sys].[indexes] AS IX
			INNER JOIN [sys].[objects]						AS O ON IX.[object_id] = O.[object_id]
			LEFT OUTER JOIN RowsPerTable					AS P ON P.[object_id] = O.[object_id]
			LEFT OUTER JOIN sys_indexes						AS SI ON O.[object_id] = SI.[object_id] AND IX.index_id = SI.index_id
			LEFT OUTER JOIN sys_index_operational_stats		AS DOS ON DOS.index_id = IX.index_id AND DOS.[object_id] = IX.[object_id]
			LEFT OUTER JOIN sys_index_usage_stats			AS DUS ON DUS.[object_id] = O.[object_id]
			LEFT OUTER JOIN sys_index_physical_stats		AS FR ON FR.[object_id] = IX.[object_id] AND fr.index_id = ix.index_id
			LEFT OUTER JOIN IndexSpaceUsed					AS SU ON FR.[object_id] = SU.[object_id] AND SU.index_id = FR.index_id
		WHERE O.is_ms_shipped = 0
			AND IX.name IS NOT NULL
			AND STATS_DATE(IX.[object_id], IX.index_id) < GETDATE() -7
		GROUP BY SI.[schema], SI.table_name
		HAVING SUM(DUS.user_seeks + DUS.user_scans + DUS.user_lookups) > 0
			AND SUM(FR.pages) > 1000		--We don't care about very small Indexes/Tables
			AND AVG(FR.[page_density]) < 90
			AND AVG(FR.page_fragmentation) > 20
		ORDER BY SI.[schema] ASC, SI.table_name ASC;