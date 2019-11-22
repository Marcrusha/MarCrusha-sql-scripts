/*
	Database File Sizes in GB
*/

SELECT
	DB.Database_ID,
	DB.[Name],
	[Data File Size (GB)]					= CONVERT(DECIMAL(18,2), D.cntr_value /1024.0 /1024.0),
	[Data File Size as Percentage of Total Data File Size] =
		CONVERT(VARCHAR(8), CONVERT(DECIMAL(5,2),
			D.cntr_value * 100.0 / 
			(SELECT cntr_value = cntr_value * 1.0 FROM sys.dm_os_performance_counters WHERE counter_name = 'Data File(s) Size (KB)' AND instance_name = '_Total')
		)) + '%'
		 ,

	[Log File Size (GB)]					= CONVERT(DECIMAL(18,2), A.cntr_value /1024.0 /1024.0),
	[Log File Space Used (GB)]				= CONVERT(DECIMAL(18,2), B.cntr_value /1024.0 /1024.0),
	[Percent Log Used]						= CONVERT(VARCHAR(4), C.cntr_value) + '%'
FROM (
	SELECT
		Database_ID,
		[Name]
	FROM sys.databases
	UNION
	SELECT
		Database_ID = -1,
		[Name] = '_Total'
	) AS DB
	INNER JOIN (
		SELECT
			instance_name,
			cntr_value
		FROM sys.dm_os_performance_counters
		WHERE counter_name = 'Log File(s) Size (KB)'
	) AS A
		ON A.instance_name = DB.[Name]
	INNER JOIN (
		SELECT
			instance_name,
			cntr_value
		FROM sys.dm_os_performance_counters
		WHERE counter_name = 'Log File(s) Used Size (KB)'
	) AS B
		ON B.instance_name = DB.[Name]
	INNER JOIN (
		SELECT
			instance_name,
			cntr_value
		FROM sys.dm_os_performance_counters
		WHERE counter_name = 'Percent Log Used'
	) AS C
		ON C.instance_name = DB.[Name]
	INNER JOIN (
		SELECT
			instance_name,
			cntr_value
		FROM sys.dm_os_performance_counters
		WHERE counter_name = 'Data File(s) Size (KB)'
	) AS D
		ON D.instance_name = DB.[Name]
ORDER BY [Data File Size (GB)] DESC;