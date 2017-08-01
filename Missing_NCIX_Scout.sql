/*
NonClustered Index Suggestion Script
Author: Marcus Hartman

Nota bene: Please review the existing Indexes on your Table before creating the suggesting Index,
as you could create nearly identical Indexes or can combine two suggestions into one Index.

*/
--Identify the Tables which need Indexes the most based on Cost, Impact, Seeks, and Scans.
WITH [tables] AS(
	SELECT dm_mid.[statement]
	FROM sys.dm_db_missing_index_groups AS dm_mig
	INNER JOIN sys.dm_db_missing_index_group_stats AS dm_migs ON dm_migs.group_handle = dm_mig.index_group_handle
	INNER JOIN sys.dm_db_missing_index_details dm_mid ON dm_mig.index_handle = dm_mid.index_handle
	WHERE dm_migs.avg_total_user_cost*(dm_migs.avg_user_impact/100)*(dm_migs.user_seeks+dm_migs.user_scans) > 100000
	GROUP BY dm_mid.[statement]
	)

--Suggests Indexes for the above Tables. Please note the suggested Indexed_Columns and Covering_Columns may be partially duplicated with existing Indexes or with other suggestions.
SELECT
	[FQTN] = dm_mid.[statement],
	Total_User_Cost = ROUND(dm_migs.avg_total_user_cost*(dm_migs.avg_user_impact/100)*(dm_migs.user_seeks+dm_migs.user_scans), 0),
	Avg_Cost_Savings_per_Query = CAST(dm_migs.avg_user_impact AS VARCHAR(5)) +'%',
	Indexed_Columns =		COALESCE(dm_mid.equality_columns,'') 
							+ (CASE WHEN dm_mid.equality_columns IS NOT NULL AND dm_mid.inequality_columns IS NOT NULL THEN ', ' ELSE '' END)
							+ COALESCE(dm_mid.inequality_columns,''),
	Covering_Columns = dm_mid.included_columns,
	Create_Index_Statement	= 'CREATE NONCLUSTERED INDEX [NCIX_' + OBJECT_NAME(dm_mid.OBJECT_ID,dm_mid.database_id) + '_'
							+ REPLACE(REPLACE(REPLACE(COALESCE(dm_mid.equality_columns,''),', ','_'),'[',''),']','') 
							+ CASE WHEN dm_mid.equality_columns IS NOT NULL AND dm_mid.inequality_columns IS NOT NULL THEN '_' ELSE '' END
							+ REPLACE(REPLACE(REPLACE(COALESCE(dm_mid.inequality_columns,''),', ','_'),'[',''),']','')
							+ ']'
							+ ' ON ' + dm_mid.statement
							+ ' (' + COALESCE(dm_mid.equality_columns,'')
							+ CASE WHEN dm_mid.equality_columns IS NOT NULL AND dm_mid.inequality_columns IS NOT NULL THEN ',' ELSE '' END
							+ COALESCE(dm_mid.inequality_columns, '')
							+ ')'
							+ COALESCE(' INCLUDE (' + dm_mid.included_columns + ')', '')
FROM sys.dm_db_missing_index_groups AS dm_mig
INNER JOIN sys.dm_db_missing_index_group_stats AS dm_migs ON dm_migs.group_handle = dm_mig.index_group_handle
INNER JOIN sys.dm_db_missing_index_details dm_mid ON dm_mig.index_handle = dm_mid.index_handle
INNER JOIN [tables] [tables] ON [tables].[statement] = dm_mid.[statement]
WHERE dm_migs.avg_total_user_cost*(dm_migs.avg_user_impact/100)*(dm_migs.user_seeks+dm_migs.user_scans) > 100000
ORDER BY FQTN ASC, Indexed_Columns ASC, Total_User_Cost ASC
