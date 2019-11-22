
DECLARE @DB VARCHAR(100) = 'sm_reporting'

/* Proc run times in minutes */
SELECT	TOP 10
		[Metric]					= 'Duration',
		[Database]					= DB_NAME(eST.[dbid]),
		[Schema]					= OBJECT_SCHEMA_NAME(ePS.object_id, ePS.database_id),
		[Procedure]					= OBJECT_NAME(ePS.object_id, ePS.database_id),
		[min_duration]				= CONVERT(NUMERIC(10,2), ePS.min_elapsed_time / 1000000.0 / 60.0),
		[max_duration]				= CONVERT(NUMERIC(10,2), ePS.max_elapsed_time / 1000000.0 / 60.0),
		[avg_duration]				= CONVERT(NUMERIC(10,2), ePS.total_elapsed_time/ISNULL(ePS.execution_count, 1) / 1000000.0 / 60.0),
		[last_duration]				= CONVERT(NUMERIC(10,2), ePS.last_elapsed_time / 1000000.0 / 60.0),
		[total_duration]			= CONVERT(NUMERIC(10,2), ePS.total_elapsed_time / 1000000.0 / 60.0),
		[last_execution]			= ePS.last_execution_time,
		[stored_procedure_text]		= LTRIM(eST.[text]),
		[query plan]				= eQP.query_plan,
		[plan_handle]				= ePS.plan_handle
FROM sys.dm_exec_procedure_stats AS ePS
    CROSS APPLY sys.dm_exec_sql_text(ePS.sql_handle) AS eST
	CROSS APPLY sys.dm_exec_query_plan (ePS.plan_handle) AS eQP
WHERE DB_NAME(eST.[dbid]) = @DB
ORDER BY ePS.total_elapsed_time / ISNULL(ePS.execution_count, 1) DESC;

/* Logical Reads */
SELECT	TOP 10
		[Metric]				= 'Logical Reads',
		[Database]				= DB_NAME(eST.[dbid]),
		[Schema]				= OBJECT_SCHEMA_NAME(ePS.object_id, ePS.database_id),
		[Procedure]				= OBJECT_NAME(ePS.object_id, ePS.database_id),
		[min_logical_reads]		= ePS.min_logical_reads,
		[max_logical_reads]		= ePS.max_logical_reads,
		[avg_logical_reads]		= ePS.total_logical_reads/ISNULL(ePS.execution_count, 1),
		[last_logical_reads]	= ePS.last_logical_reads,
		[total logical reads]	= ePS.total_logical_reads,
		[last_execution]		= ePS.last_execution_time,
		[stored_procedure_text]	= LTRIM(eST.[text]),
		[query plan]			= eQP.query_plan,
		[plan_handle]			= ePS.plan_handle
FROM sys.dm_exec_procedure_stats AS ePS
    CROSS APPLY sys.dm_exec_sql_text(ePS.sql_handle) AS eST
	CROSS APPLY sys.dm_exec_query_plan (ePS.plan_handle) AS eQP
WHERE DB_NAME(eST.[dbid]) = @DB
ORDER BY ePS.total_logical_reads/ISNULL(ePS.execution_count, 1) DESC;

/* Physical Reads */
SELECT	TOP 10
		[Metric]				= 'Physical Reads',
		[Database]				= DB_NAME(eST.[dbid]),
		[Schema]				= OBJECT_SCHEMA_NAME(ePS.[object_id], ePS.database_id),
		[Procedure]				= OBJECT_NAME(ePS.[object_id], ePS.database_id),
		[min_physical_reads]	= ePS.min_physical_reads,
		[max_physical_reads]	= ePS.max_physical_reads,
		[avg_physical_reads]	= ePS.total_physical_reads / ISNULL(ePS.execution_count, 1),
		[last_physical_reads]	= ePS.last_physical_reads,
		[total physical reads]	= ePS.total_physical_reads,
		[last_execution]		= ePS.last_execution_time,
		[stored_procedure_text]	= LTRIM(eST.[text]),
		[query plan]			= eQP.query_plan,
		[plan_handle]			= ePS.plan_handle
FROM sys.dm_exec_procedure_stats AS ePS
    CROSS APPLY sys.dm_exec_sql_text(ePS.sql_handle) AS eST
	CROSS APPLY sys.dm_exec_query_plan (ePS.plan_handle) AS eQP 
WHERE DB_NAME(eST.[dbid]) = @DB
ORDER BY ePS.total_physical_reads/ISNULL(ePS.execution_count, 1) DESC;

/* CPU Usage*/
SELECT	TOP 10
		[Metric]				= 'CPU',
		[Database]				= DB_NAME(eST.[dbid]),
		[Schema]				= OBJECT_SCHEMA_NAME(ePS.[object_id], ePS.database_id),
		[Procedure]				= OBJECT_NAME(ePS.[object_id], ePS.database_id),
		[min_cpu]				= ePS.min_worker_time,
		[max_cpu]				= ePS.max_worker_time,
		[avg_cpu]				= ePS.total_worker_time / ISNULL(ePS.execution_count, 1),
		[last_cpu]				= ePS.last_elapsed_time,
		[total_cpu]				= ePS.total_worker_time,
		[last_execution]		= ePS.last_execution_time,
		[stored_procedure_text]	= LTRIM(eST.[text]),
		[query plan]			= eQP.query_plan,
		[plan_handle]			= ePS.plan_handle
FROM sys.dm_exec_procedure_stats AS ePS
    CROSS APPLY sys.dm_exec_sql_text(ePS.sql_handle) AS eST
	CROSS APPLY sys.dm_exec_query_plan (ePS.plan_handle) AS eQP 
WHERE DB_NAME(eST.[dbid]) = @DB
ORDER BY ePS.total_worker_time / ISNULL(ePS.execution_count, 1) DESC;

/* Writes */
SELECT	TOP 10
		[Metric]				= 'Writes',
		[Database]				= DB_NAME(eST.[dbid]),
		[Schema]				= OBJECT_SCHEMA_NAME(ePS.[object_id], ePS.database_id),
		[Procedure]				= OBJECT_NAME(ePS.[object_id], ePS.database_id),
		[min_writes]			= ePS.min_logical_writes,
		[max_writes]			= ePS.max_logical_writes,
		[avg_writes]			= ePS.total_logical_writes / ISNULL(ePS.execution_count, 1),
		[last_writes]			= ePS.last_logical_writes,
		[total_writes]			= ePS.total_logical_writes,
		[last_execution]		= ePS.last_execution_time,
		[stored_procedure_text]	= LTRIM(eST.[text]),
		[query plan]			= eQP.query_plan,
		[plan_handle]			= ePS.plan_handle
FROM sys.dm_exec_procedure_stats AS ePS
    CROSS APPLY sys.dm_exec_sql_text(ePS.sql_handle) AS eST
	CROSS APPLY sys.dm_exec_query_plan (ePS.plan_handle) AS eQP 
WHERE DB_NAME(eST.[dbid]) = @DB
ORDER BY ePS.total_logical_writes / ISNULL(ePS.execution_count, 1) DESC;

/* Execution Counts */
SELECT	TOP 10
		[Metric]				= 'Executions',
		[Database]				= DB_NAME(eST.[dbid]),
		[Schema]				= OBJECT_SCHEMA_NAME(ePS.[object_id], ePS.database_id),
		[Procedure]				= OBJECT_NAME(ePS.[object_id], ePS.database_id),
		[Executions]			= ePS.execution_count,
		[last_execution]		= ePS.last_execution_time,
		[stored_procedure_text]	= LTRIM(eST.[text]),
		[query plan]			= eQP.query_plan,
		[plan_handle]			= ePS.plan_handle
FROM sys.dm_exec_procedure_stats AS ePS
    CROSS APPLY sys.dm_exec_sql_text(ePS.sql_handle) AS eST
	CROSS APPLY sys.dm_exec_query_plan (ePS.plan_handle) AS eQP 
WHERE DB_NAME(eST.[dbid]) = @DB
ORDER BY ePS.execution_count DESC;

/*
SELECT *
FROM sys.dm_exec_query_plan(0x05004900999F2528006F94E39E01000001000000000000000000000000000000000000000000000000000000)
*/