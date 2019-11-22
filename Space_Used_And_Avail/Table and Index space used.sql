/*
Identify disk space used by Tables & Indexes.

Author: Marcus Hartman
Thanks to Kirsten Benzel
*/


/*
	INDEX SIZE
		Measures "Page Size" of an Index (heaps excluded). Includes Pages used in B-Trees, allocation metadata, leaf pages, et al.
		UsedPageSpace measures "Pages actually in use", and is unrelated to Index Fill Factor.
		RowCounts displays row count of the Index. A Filtered Index may have a smaller row count than its Table.
*/
	SELECT
		[Table]				= S.[Name] + '.' + T.[Name],
		[IndexName]			= I.[Name],
		[Type]				= CONVERT(VARCHAR(210),
								CASE	WHEN I.index_id = 1				THEN 'clustered'
										WHEN I.index_id = 0				THEN 'heap'
										ELSE 'nonclustered' END
								+ CASE	WHEN I.[ignore_dup_key] <> 0	THEN ', ignore duplicate keys' ELSE '' END
								+ CASE	WHEN I.is_unique <> 0			THEN ', unique' ELSE '' END
								+ CASE	WHEN I.is_primary_key <> 0		THEN ', primary key' ELSE '' END
								+ CASE	WHEN I.has_filter = 1			THEN ', filtered' ELSE '' END
								+ CASE	WHEN I.[type] = 6				THEN ', columnstore' ELSE '' END
								+ CASE	WHEN FI.[object_id] IS NOT NULL	THEN ', fulltext' ELSE '' END),
		[RowCounts]			= REPLACE(CONVERT(VARCHAR(20), (CAST(FLOOR(SUM(P.[rows])) AS MONEY)), 1), '.00', ''),
		[TotalPageCount]	= REPLACE(CONVERT(VARCHAR(20), (CAST(FLOOR(SUM(A.total_pages)) AS MONEY)), 1), '.00', ''),
		[TotalPageSpaceGB]	= CONVERT(DECIMAL(18,2), (SUM(A.total_pages) * 8) / 1024.0 / 1024.0),
		[UsedPageSpaceGB]	= CONVERT(DECIMAL(18,2), (SUM(A.used_pages) * 8 ) / 1024.0 / 1024.0),
		[UnusedPageSpaceGB]	= CONVERT(DECIMAL(18,2), ((SUM(A.total_pages) - SUM(A.used_pages) ) * 8 ) / 1024.0 / 1024.0),
		[IndexFillFactor]	= AVG(I.Fill_Factor)
	FROM sys.tables AS T
		INNER JOIN sys.schemas AS S
			ON S.[schema_id] = T.[schema_id]
		INNER JOIN sys.indexes AS I
			ON T.[object_id] = I.[object_id]
		INNER JOIN sys.partitions AS P
			ON I.[object_id] = P.[object_id]
				AND I.index_id = P.index_id
		INNER JOIN sys.allocation_units	AS A
			ON P.[partition_id] = A.container_id
		LEFT JOIN sys.fulltext_indexes AS FI
			ON I.[object_id] = FI.[object_id] 
	WHERE I.[object_id] > 255
		AND I.[Type] <> 0
	GROUP BY S.[Name], T.[Name], I.[Name],
		CONVERT(VARCHAR(210),
			CASE	WHEN I.index_id = 1				THEN 'clustered'
					WHEN I.index_id = 0				THEN 'heap'
					ELSE 'nonclustered' END
			+ CASE	WHEN I.[ignore_dup_key] <> 0	THEN ', ignore duplicate keys' ELSE '' END
			+ CASE	WHEN I.is_unique <> 0			THEN ', unique' ELSE '' END
			+ CASE	WHEN I.is_primary_key <> 0		THEN ', primary key' ELSE '' END
			+ CASE	WHEN I.has_filter = 1			THEN ', filtered' ELSE '' END
			+ CASE	WHEN I.[type] = 6				THEN ', columnstore' ELSE '' END
			+ CASE	WHEN FI.[object_id] IS NOT NULL	THEN ', fulltext' ELSE '' END)
	ORDER BY SUM(A.total_pages) DESC, [Table] ASC, [IndexName] ASC;

/*
	TABLE SIZE
		Measures "Page Size" of a Table. Includes Pages used in B-Trees, allocation metadata, leaf pages, et al.
		UsedPageSpace measures "Pages actually in use", and is unrelated to Index Fill Factor.
		RowCounts displays row count of the Table. Indexes are not counted towards the RowCounts.
*/
	SELECT
		[Table]				= S.[Name] + '.' + T.[Name],
		[RowCounts]			= REPLACE(CONVERT(VARCHAR(20), (CAST(FLOOR(MAX(P.[rows])) AS MONEY)), 1), '.00', ''),
		[TotalPageCount]	= REPLACE(CONVERT(VARCHAR(20), (CAST(FLOOR(SUM(A.total_pages)) AS MONEY)), 1), '.00', ''),
		[TotalPageSpaceGB]	= CONVERT(DECIMAL(18,2), (SUM(A.total_pages) * 8) / 1024.0 / 1024.0),
		[UsedPageSpaceGB]	= CONVERT(DECIMAL(18,2), (SUM(A.used_pages) * 8 ) / 1024.0 / 1024.0),
		[UnusedPageSpaceGB]	= CONVERT(DECIMAL(18,2), ((SUM(A.total_pages) - SUM(A.used_pages) ) * 8 ) / 1024.0 / 1024.0)
	FROM sys.tables AS T
		INNER JOIN sys.schemas AS S
			ON S.[schema_id] = T.[schema_id]
		INNER JOIN sys.indexes AS I
			ON T.[object_id] = I.[object_id]
		INNER JOIN sys.partitions AS P
			ON I.[object_id] = P.[object_id]
				AND I.index_id = P.index_id
		INNER JOIN sys.allocation_units	AS A
			ON P.[partition_id] = A.container_id
		LEFT JOIN sys.fulltext_indexes AS FI
			ON I.[object_id] = FI.[object_id] 
	WHERE I.[object_id] > 255
	GROUP BY S.[Name], T.[Name]
	ORDER BY SUM(A.total_pages) DESC, [Table] ASC;