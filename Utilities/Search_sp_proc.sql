/*
Search proc
Authors: Kirsten Benzel, Marcus Hartman

Creates a proc which allows quick searching for a string across a database. Useful for hunting down columns in procs and for finding dependencies.
*/
DROP PROCEDURE [util].[Search_sp]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [util].[Search_sp](
   @search_string NVARCHAR(4000),     
   @database_list NVARCHAR(MAX) = NULL,
   @case_sensitive BIT = 0,
   @exact_string BIT = 1,
   @include_jobs BIT = 1,
   @include_columns BIT = 0,
   @include_parameters BIT = 0,
   @include_system_objects BIT = 0,
   @include_system_databases BIT = 0
)   
AS
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/*
How to Use:

EXEC [util].[Search_sp]
    @search_string = 'string',
    @database_list = 'database1,database2',
    @case_sensitive = 0,
    @exact_string = 1, 		-- 0 = treat "_" and "*" as wildcards. 1 = DO NOT treat "_" and "*" as wildcards.
    @include_jobs = 1,		
    @include_columns = 1,
    @include_parameters = 1,
    @include_system_objects = 0,
    @include_system_databases = 0
GO
*/

 BEGIN;

	DECLARE @init_sql  NVARCHAR(MAX),
			@run_sql   NVARCHAR(MAX),
			@dbname    NVARCHAR(128),
			@all_text  NVARCHAR(10),
			@coll_text NVARCHAR(50);

   CREATE TABLE #t (
       [database]      SYSNAME,
       [schema]        SYSNAME,
       [object]        SYSNAME,
       [type]          SYSNAME,
       [create_date]   DATETIME,
       [modify_date]   DATETIME,
       [definition]    NVARCHAR(MAX));

   CREATE TABLE #j(
       [job_name]      SYSNAME,
       [step_id]       INT,
       [step_name]     SYSNAME,
       [create_date]   DATETIME,
       [modify_date]   DATETIME,
       [definition]    NVARCHAR(MAX));

   CREATE TABLE #cp(
       [database]      SYSNAME,
       [schema]        SYSNAME,
       [object]        SYSNAME,
       [type]          SYSNAME,
       [create_date]   DATETIME,
       [modify_date]   DATETIME,
       [param]         NVARCHAR(128),
       [column]        NVARCHAR(128));

	SET @database_list = REPLACE(@database_list, CHAR(32), '')

	IF @exact_string = 1
		BEGIN;
			SET @search_string =  REPLACE(REPLACE(@search_string,CHAR(37),'['+CHAR(37)+']'),CHAR(95),'['+CHAR(95)+']')
		END;

	SELECT	@all_text = CASE @include_system_objects WHEN 1 THEN N'all_' ELSE N'' END,
			@coll_text = CASE @case_sensitive WHEN 1 THEN N'COLLATE Latin1_General_BIN' ELSE N'' END;

	SET @init_sql = N'SELECT [database] = ''$db$'',
							[schema] = QUOTENAME(s.name),
							[object]   = QUOTENAME(o.name),
							[type]     = o.type_desc,
							o.create_date,
							o.modify_date,
							m.[definition]
					FROM $db$.sys.$all$sql_modules AS m
						INNER JOIN $db$.sys.$all$objects AS o
							ON m.[object_id] = o.[object_id]
						INNER JOIN $db$.sys.schemas AS s
							ON o.[schema_id] = s.[schema_id]
					WHERE m.definition $coll$ LIKE N''%'' + @search_string + ''%'' $coll$;';

	SET @init_sql = REPLACE(REPLACE(@init_sql,'$all$',@all_text),'$coll$',@coll_text);

	SET @search_string = REPLACE(@search_string,'''','''''');

	DECLARE c CURSOR
		LOCAL STATIC FORWARD_ONLY READ_ONLY
			FOR 
				SELECT QUOTENAME(d.name)
				FROM sys.databases AS d
					LEFT OUTER JOIN dba.SplitStringsXML_fn(@database_list, N',') AS s
						ON 1 = 1
				WHERE (LOWER(d.name) = LOWER(s.Item)
						OR NULLIF(RTRIM(LTRIM(@database_list)), N'') IS NULL)
					AND d.database_id BETWEEN CASE @include_system_databases WHEN 1 THEN 1 ELSE 5 END AND 32766
					AND D.State = 0
				ORDER BY d.name;
	OPEN c;
   
		FETCH NEXT FROM c INTO @dbname;
			WHILE @@FETCH_STATUS = 0
				BEGIN
					SET @run_sql = REPLACE(@init_sql, N'$db$', @dbname);

					INSERT INTO #t 
					EXEC sp_executesql @run_sql, N'@search_string NVARCHAR(4000)', @search_string;

							SET @run_sql = N'SELECT [database] = ''$db$'',
													[schema]   = QUOTENAME(s.name),
													[object]   = QUOTENAME(o.name),
													[type]     = o.type_desc,
													o.create_date,
													o.modify_date,
													c.name
											FROM $db$.sys.tables AS c
												INNER JOIN $db$.sys.$all$objects AS o
													ON c.[object_id] = o.[object_id]
												INNER JOIN $db$.sys.schemas AS s
													ON o.[schema_id] = s.[schema_id]
												WHERE c.name $coll$ 
													  LIKE N''%'' + @search_string + ''%'' $coll$;';

							SET @run_sql = REPLACE(REPLACE(REPLACE(@run_sql,'$all$',@all_text),'$coll$',@coll_text),'$db$',@dbname);

								INSERT INTO #t ([database],[schema],[object],[type],[create_date],[modify_date],[definition])
								EXEC sp_executesql @run_sql, N'@search_string NVARCHAR(4000)', @search_string;

					IF @include_columns = 1
						BEGIN;
							SET @run_sql = N'SELECT [database] = ''$db$'',
													[schema]   = QUOTENAME(s.name),
													[object]   = QUOTENAME(o.name),
													[type]     = o.type_desc,
													o.create_date,
													o.modify_date,
													NULL,
													c.name
											FROM $db$.sys.$all$columns AS c
												INNER JOIN $db$.sys.$all$objects AS o
													ON c.[object_id] = o.[object_id]
												INNER JOIN $db$.sys.schemas AS s
													ON o.[schema_id] = s.[schema_id]
												WHERE c.name $coll$ 
													  LIKE N''%'' + @search_string + ''%'' $coll$;';

				SET @run_sql = REPLACE(REPLACE(REPLACE(@run_sql,'$all$',@all_text),'$coll$',@coll_text),'$db$',@dbname);

				INSERT INTO #cp
				EXEC sp_executesql @run_sql, N'@search_string NVARCHAR(4000)',@search_string;
			END;

	IF @include_parameters = 1
		BEGIN;
		
           SET @run_sql = N'SELECT	[database] = ''$db$'',
									[schema]   = QUOTENAME(s.name),
									[object]   = QUOTENAME(o.name),
									[type]     = o.type_desc,
									o.create_date,
									o.modify_date,
									p.name,
									NULL
								FROM $db$.sys.$all$parameters AS p
									INNER JOIN $db$.sys.$all$objects AS o
										ON p.[object_id] = o.[object_id]
								INNER JOIN $db$.sys.schemas AS s
									ON o.[schema_id] = s.[schema_id]
								WHERE p.name $coll$ LIKE N''%'' + @search_string + ''%'' $coll$;';

			SET @run_sql = REPLACE(REPLACE(REPLACE(@run_sql,'$all$',@all_text),'$coll$',@coll_text),'$db$',@dbname);

			INSERT INTO #cp
			EXEC sp_executesql @run_sql, N'@search_string NVARCHAR(4000)', @search_string;

		END;

	 FETCH NEXT FROM c INTO @dbname;
	
	END;

   CLOSE c;
   DEALLOCATE c;
   
   SELECT 'Objects:'   

   SELECT	[database],
			[schema],
			[object],
			[type],
			[definition] = CONVERT(XML, '<?query --
			USE ' + [database] + ';' + CHAR(13) + CHAR(10) + 'GO' + CHAR(13) + CHAR(10) + [definition] + ' --?>'),
			first_line = (DATALENGTH(abbrev_def) - DATALENGTH(REPLACE(abbrev_def, CHAR(13), '')))/2 + 1,
			create_date,
			modify_date
   FROM (
		SELECT	*, 
				[count] = (DATALENGTH([definition]) - DATALENGTH(REPLACE([definition], @search_string, '')))/DATALENGTH(@search_string),
				abbrev_def = SUBSTRING([definition], 1, 
				CHARINDEX(@search_string, [definition]))
		FROM #t) AS x
   ORDER BY [database], [schema], [object];

	IF @include_jobs = 1
		BEGIN;
		
			SELECT 'Jobs:';

			SET @run_sql = N'SELECT	job_name = j.name, 
									s.step_id, 
									s.step_name, 
									j.date_created,
									j.date_modified,
									[definition] = s.command
							FROM msdb.dbo.sysjobs AS j
								INNER JOIN msdb.dbo.sysjobsteps AS s
									ON j.job_id = s.job_id
							WHERE s.command $coll$ LIKE ''%'' + @search_string + ''%'' $coll$
							ORDER BY j.name, s.step_id;';

			SET @run_sql = REPLACE(@run_sql, '$coll$', @coll_text); 

			INSERT INTO #j EXEC sp_executesql @run_sql, N'@search_string NVARCHAR(4000)', @search_string;

			SELECT	job_name,
					step_id,
					step_name,
					[command] = CONVERT(XML, '<?query --' + [definition] + ' --?>'),
					create_date,
					modify_date
			FROM #j;
       
		END;

	IF @include_columns = 1 OR @include_parameters = 1
		BEGIN;

			SELECT 'Columns/Parameters';

			SELECT [database],
				   [schema],
				   [object],
				   [type],
				   [param],
				   [column],
				   [create_date],
				   [modify_date]
			FROM #cp
			ORDER BY [database], [schema], [object], [param], [column];
			
		END;

   DROP TABLE #t, #j, #cp;
   
 END;
 
SET NOCOUNT OFF;
RETURN 0;

GO
