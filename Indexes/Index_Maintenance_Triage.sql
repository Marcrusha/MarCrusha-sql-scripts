/*
Triage maintenance on Indexes
Displays Indexes with high Page Fragmentation or low Page Density.

Author: Marcus Hartman

ALTER INDEX ALL ON dbo.Account
REBUILD WITH (ONLINE = ON, FILLFACTOR = 70)
*/

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH
	sys_partitions AS (
		SELECT
			partition_scheme_name	= SPS.[Name],
			partition_function_name = SPF.[Name],
			data_space_id			= SPS.data_space_id
		FROM sys.partition_schemes AS SPS
			INNER JOIN sys.partition_functions AS SPF
				ON SPS.function_id = SPF.function_id
		),

	sys_indexes AS (
		SELECT
			O.[object_id],
			I.index_id,
			I.is_disabled,
			I.fill_factor, -- fullness at page level
			[Type] = CONVERT(VARCHAR(210),
						CASE	WHEN I.index_id = 1				THEN 'clustered'
								WHEN I.index_id = 0				THEN 'heap' ELSE 'nonclustered' END
						+ CASE	WHEN I.[ignore_dup_key] <> 0	THEN ', ignore duplicate keys' ELSE '' END
						+ CASE	WHEN I.is_unique <> 0			THEN ', unique' ELSE '' END
						+ CASE	WHEN I.is_primary_key <> 0		THEN ', primary key' ELSE '' END
						+ CASE	WHEN I.has_filter = 1			THEN ', filtered' ELSE '' END
						+ CASE	WHEN I.[type] = 6				THEN ', columnstore' ELSE '' END
						+ CASE	WHEN FI.[object_id] IS NOT NULL	THEN ', fulltext' ELSE '' END),
			[table_name] = O.[Name],
			[schema] = SC.[Name],
			[table_modified] = O.modify_date
		FROM sys.indexes AS I
			INNER JOIN sys.objects AS O
				ON O.[object_id] = I.[object_id]
			INNER JOIN sys.schemas AS SC
				ON O.[schema_id] = SC.[schema_id]
			LEFT OUTER JOIN sys.fulltext_indexes AS FI
				ON I.[object_id] = FI.[object_id] 
				AND I.index_id = FI.unique_index_id
		WHERE O.[type] IN ('U','V')
			AND O.is_ms_shipped = 0
			AND I.[type] NOT IN (0,6)
		),
		
	sys_index_operational_stats AS (
		SELECT
			[object_id],
			index_id,
			leaf_allocation_count, -- this equates to page splits, and it counts both good and bad
			range_scan_count,
			singleton_lookup_count,
			forwarded_fetch_count,
			lob_fetch_in_pages,
			lob_fetch_in_bytes,
			row_lock_count,
			row_lock_wait_count,
			row_lock_wait_in_ms,
			page_lock_count,
			page_lock_wait_count,
			page_lock_wait_in_ms,
			page_latch_wait_count,
			page_io_latch_wait_count
		FROM [sys].[dm_db_index_operational_stats](DB_ID(), NULL, NULL, NULL)
		),

	sys_index_usage_stats AS (
		SELECT	O.[object_id],
				I.index_id,
				Index_Name			= I.[name],
				user_seeks			= U.user_seeks,
				last_user_seek		= U.last_user_seek,
				last_system_seek	= U.last_system_seek,
				user_scans			= U.user_scans,
				last_user_scan		= U.last_user_scan,
				last_system_scan	= U.last_system_scan,
				user_lookups		= U.user_lookups,
				last_user_lookup	= U.last_user_lookup,
				last_system_lookup	= U.last_system_lookup,
				user_updates		= U.user_updates
		FROM sys.indexes AS I
			INNER JOIN sys.objects AS O
				ON I.[object_id] = O.[object_id]
			INNER JOIN sys.dm_db_index_usage_stats AS U
				ON I.[object_id] = U.[object_id] -- Statistics are zeroed during online rebuilds in 2012
				AND I.index_id = U.index_id
		WHERE U.database_id = DB_ID()
			AND O.[type] IN ('U','V')
			AND O.is_ms_shipped = 0
			AND I.[type] NOT IN (0,6)
		),
		
	sys_index_physical_stats AS (
		SELECT
			partition_number,
			[object_id],
			index_id,
			pages					= SUM(page_count),
			[page_density]			= SUM(ROUND(avg_page_space_used_in_percent, 1)),
			page_fragmentation		= CASE WHEN index_id > 0 THEN ROUND(SUM(avg_fragmentation_in_percent), 1) ELSE NULL END,
			[rows]					= SUM(record_count),
			fw_records				= SUM(forwarded_record_count)
		FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL , NULL, 'SAMPLED')
		WHERE alloc_unit_type_desc = 'IN_ROW_DATA'
			AND Index_ID <> 0
		GROUP BY partition_number, [object_id], index_id
		),
	
	IndexSpaceUsed AS (
		SELECT
			[object_id]			= I.[object_id],
			[index_ID]			= I.index_id,
			[Size (KB)]			= SUM(S.used_page_count) * 8096		
		FROM sys.dm_db_partition_stats AS S
			INNER JOIN sys.indexes AS I
				ON S.[object_id] = I.[object_id]
				AND S.index_id = I.index_id
			INNER JOIN sys.objects AS O
				ON O.[object_id] = I.[object_id]
		WHERE O.[type] IN ('U','V')
			AND O.is_ms_shipped = 0
			AND I.[type] NOT IN (0,6)
		GROUP BY I.[object_id], I.index_id
		)
		
	SELECT	[Database]				= DB_NAME(DB_ID()),
			[Schema]				= SI.[schema],
			[Table]					= SI.[table_name],
			[Tbl Modified]			= CONVERT(CHAR(10), SI.[table_modified], 101),
			[IX]					= ISNULL(IX.name, ''),
			[IX Type]				= SI.[Type],
			[Key Columns]			= ISNULL(REPLACE(REPLACE(REPLACE((
										SELECT columnName = C.[Name] + CASE WHEN sic.is_descending_key = 0 THEN ' ASC' ELSE ' DESC' END
										FROM [sys].[index_columns] AS SIC
											INNER JOIN [sys].[columns] AS C
												ON C.column_id = SIC.column_id
												AND C.[object_id] = SIC.[object_id]
										WHERE SIC.[object_id] = IX.[object_id]
											AND SIC.index_id = IX.index_id
											AND is_included_column = 0
										ORDER BY SIC.index_column_id ASC
										FOR XML RAW), '"/><row columnName="', ', '), '<row columnName="', ''), '"/>', ''), ''),
			[Included Columns]		= ISNULL(REPLACE(REPLACE(REPLACE((
										SELECT columnName = C.[Name]
										FROM [sys].[index_columns] AS SIC
											INNER JOIN [sys].[columns] AS C
												ON C.column_id = SIC.column_id
												AND C.[object_id] = SIC.[object_id]
										WHERE SIC.[object_id] = IX.[object_id]
											AND SIC.index_id = IX.index_id
											AND is_included_column = 1
										ORDER BY sic.index_column_id ASC
										FOR XML RAW), '"/><row columnName="',', '), '<row columnName="',''), '"/>',''),''),
			[Filtered Columns]		= ISNULL(REPLACE(REPLACE(REPLACE(REPLACE(IX.filter_definition,']',''),'[',''),'(',''),')',''),''),
			[Reads]					= SUM(DUS.user_seeks + DUS.user_scans + DUS.user_lookups),
			[Seeks]					= DUS.user_seeks, 
			[Scans]					= DUS.user_scans,
			[Lookups]				= DUS.user_lookups,
			[Writes]				= DUS.user_updates,
			[Reads Per Write]		= CONVERT(DECIMAL(10,2), MAX(CASE WHEN DUS.user_updates < 1 THEN 100.00 ELSE 1.00 * (DUS.user_seeks + DUS.user_scans + DUS.user_lookups) / DUS.user_updates END)),
			[Last Read]				= CONVERT(CHAR(10), (SELECT MAX(v) FROM (VALUES (DUS.last_user_seek), (DUS.last_user_scan), (DUS.last_user_lookup)) AS value(v)), 101),
			[Page Count]			= FR.pages,
			[Page Density]			= FR.[page_density],
			[Page Fragmentation]	= FR.page_fragmentation,			
			[Rows]					= (SELECT SUM(P.[rows]) FROM [sys].[partitions] AS P WHERE P.index_id = IX.index_id AND IX.[object_id] = P.[object_id]),
			[IX Size (MB)]			= (SU.[Size (KB)] /1024 /1024),
			[Wasted space (GB)]		= ((1- (FR.page_density/100)) *FR.pages*8096) /1024/1024/1024,							
			[Last Stats Update]		= CONVERT(CHAR(10), STATS_DATE(IX.[object_id], IX.index_id), 101),			
			[IX IsDisabled]			= SI.is_disabled,
			[IX Fill Factor]		= SI.fill_factor,
			[Forwarded Records]		= FR.fw_records,
			[IX Page Splits]		= DOS.leaf_allocation_count
	FROM sys.indexes AS IX
		INNER JOIN sys.objects AS O
			ON IX.[object_id] = O.[object_id]
		INNER JOIN sys_indexes AS SI
			ON O.[object_id] = SI.[object_id]
			AND IX.index_id = SI.index_id
		LEFT OUTER JOIN sys_index_operational_stats AS DOS
			ON DOS.[object_id] = IX.[object_id]
			AND DOS.index_id = IX.index_id
		LEFT OUTER JOIN sys_index_usage_stats AS DUS
			ON DUS.[object_id] = O.[object_id]
			AND IX.index_id = DUS.index_id
		LEFT OUTER JOIN sys_index_physical_stats AS FR
			ON FR.[object_id] = IX.[object_id]
			AND fr.index_id = ix.index_id
		LEFT OUTER JOIN IndexSpaceUsed AS SU
			ON FR.[object_id] = SU.[object_id]
			AND SU.index_id = FR.index_id
		LEFT OUTER JOIN sys_partitions AS PT
			ON IX.data_space_id = PT.data_space_id
	WHERE O.is_ms_shipped = 0
		AND O.[type] IN ('U','V')
		AND IX.[type] NOT IN (0,6)
		AND IX.[name] IS NOT NULL
		--AND STATS_DATE(IX.[object_id], IX.index_id) < GETDATE() -7
		AND (FR.[page_density] < 60 OR FR.page_fragmentation > 40)
		AND FR.pages >= 1000
	GROUP BY SI.[schema], SI.table_name, SI.table_modified, IX.Index_ID, IX.[Name], SI.[Type], ix.[object_id], ix.[index_id], 
		IX.filter_definition, DUS.user_updates, FR.pages, FR.[page_density], FR.page_fragmentation,	SU.[Size (KB)], SI.is_disabled, SI.fill_factor,
		FR.fw_records, DUS.last_user_seek, DUS.last_user_scan, DUS.last_user_lookup, DOS.leaf_allocation_count, DUS.user_seeks, DUS.user_scans, DUS.user_lookups
	ORDER BY SI.[schema], SI.table_name, IX.Index_ID
	--ORDER BY (FR.page_fragmentation/100)*(SU.[Size (KB)]/1024/1024) DESC
	--OPTION (MAXDOP 1);

