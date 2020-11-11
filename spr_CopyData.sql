USE [AdminDB]
GO

/*
Author: Antonio Turkovic (Microsoft Data&AI CE)
Version: 202011-01
Supported SQL Server Versions: >= SQL Server 2016 SP2 (Standard and Enterprise Edition)	

Description:
This script will copy the data from the tables in the source database to the tables in the destination database.
It will create a stored procedure called "dbo.spr_CopyData"

WARNING: Copying data with this method can cause issues in referencial integrity. Depending on the data size it will take a lot of time.
		 Additionally, ensure that there is enough disk space on the transaction log disk.

Requirements:
- User must have SYSADMIN privileges
- Ensure you have enough disk space (2.5 - 3 times the data)
- Call the procedure only on the SQL Server itself to prevent any remote procedure timeouts
- SQL Server Version must be newer than SQL Server 2016 SP2
- BEFORE running the process, perform a FULL backup of your database (destination database will be set to SIMPLE recovery model)

Parameter:
	- @sourceDB: Specify the source database to copy the data from

	- @destinationDB: Specify the destination database to copy the data to
		- The destination database must be inplace, unless you are using the "@cloneDB = 1" parameter
		- However, it is highly recommended to create the database and the schema manually (eg. via "Generate Scripts" Wizard in SSMS)
	
	- @cloneDB: Specify if you want to clone the source database
		- This procedure is not supported for productional use
		- Default value: 0


Example:
	EXEC dbo.spr_CopyData @source = 'testDB', @destinationDB = 'testDB_Copy'

Output:
	- Failed Tables: Could not copy these table (more details in the "Messages" tab)
	- Row Counts: Row Counts from source and destination tables

*/


CREATE PROCEDURE spr_CopyData
	-- Source Database to copy the data from
	@sourceDB NVARCHAR(MAX),

	-- Destination Database to copy the data to
	@destinationDB NVARCHAR(MAX),

	-- Clone the source database (not for productional use)
	@cloneDB BIT = 0
AS
BEGIN
	SET NOCOUNT ON;

	-- Messages
	DECLARE @msg NVARCHAR(4000) = ''

	-- Dyn CMD
	DECLARE @cmd NVARCHAR(MAX)

	-- CMD Param
	DECLARE @param NVARCHAR(2000)

	-- Var for INT return Values
	DECLARE @cmdResult BIGINT = 0

	-- SQL Version
	DECLARE @sqlVersion INT = (SELECT CONVERT(INT, SERVERPROPERTY('ProductMajorVersion')))

	-- Date String
	DECLARE @thisDate NVARCHAR(32) = FORMAT(GETDATE(), 'yyyyMMdd')

	-- Time String
	DECLARE @thisTime NVARCHAR(32) = FORMAT(GETDATE(), 'HHmmss')

	-- Server Name
	DECLARE @serverName NVARCHAR(512) = CONVERT(NVARCHAR(512), SERVERPROPERTY('Servername'))

	--Missing Objects
	DECLARE @tblMissingObjects TABLE (objName NVARCHAR(MAX))

	-- Table for TableNames
	DECLARE @tblTableNames TABLE (objName NVARCHAR(MAX))

	-- Table for CHECK Constraints
	DECLARE @tblCheckConstraints TABLE (objName NVARCHAR(MAX), tableName NVARCHAR(MAX))

	-- Table for FK Constraints
	DECLARE @tblFKConstraints TABLE (objName NVARCHAR(MAX), tableName NVARCHAR(MAX))

	-- Table for Triggers
	DECLARE @tblTriggers TABLE (objName NVARCHAR(MAX), tableName NVARCHAR(MAX))

	-- Check RowCount Destination Table
	DECLARE @chkDestDBRowCount TABLE (tableName NVARCHAR(MAX), rowCounts BIGINT)

	-- Check RowCount Source Table
	DECLARE @chkSourceDBRowCount TABLE (tableName NVARCHAR(MAX), rowCounts BIGINT)

	-- Table for IDENTITY tables
	DECLARE @tblIdentityTables TABLE (objName NVARCHAR(MAX))

	-- Table for INSERT Columns
	DECLARE @tblInsertColumns TABLE (tableName NVARCHAR(MAX), insertColumns NVARCHAR(MAX))

	-- Table for SELECT Columns
	DECLARE @tblSelectColumns TABLE (tableName NVARCHAR(MAX), selectColumns NVARCHAR(MAX))

	-- Table for Columns with XML data type
	DECLARE @tblColsWithXML TABLE (tableName NVARCHAR(MAX), xmlColumns NVARCHAR(MAX))

	-- Table for failed Tables
	DECLARE @tblFailed TABLE (tableName NVARCHAR(MAX))

	-- Cursor Variables --> Object
	DECLARE @curObjName NVARCHAR(MAX)
	
	-- Cursor Variables --> TableName
	DECLARE @curTableName NVARCHAR(MAX)

	-- ERROR COUNT
	DECLARE @errorCount INT = 0

	--##################################################################
	-- VALIDATION
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Validating configuration'
	RAISERROR(@msg, 10, 1) WITH NOWAIT;

	-- Check if User is Sysadmin
	--Check if user is SYSADMIN
	IF (IS_SRVROLEMEMBER('sysadmin') != 1)
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | SYSADMIN privileges required - terminating script'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		RETURN;
	END

	-- Check if SourceDB is SYSDB
	IF(@sourceDB IN ('master', 'model', 'msdb', 'tempdb'))
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Source database must not be member of system databases - terminating script'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		RETURN;
	END

	-- Check if Destination is SYSDB
	IF(@destinationDB IN ('master', 'model', 'msdb', 'tempdb'))
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Destination database must not be member of system databases - terminating script'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		RETURN;
	END

	-- Check if Source DB exists
	IF NOT EXISTS (SELECT 1 FROM master.sys.databases WHERE name = @sourceDB)
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Source database not found - terminating script'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		RETURN;
	END

	-- Create Destination DB if @cloneDB is 1
	IF(@cloneDB = 1)
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Cloning database ' + @sourceDB + ' to ' + @destinationDB
		RAISERROR(@msg, 10, 1) WITH NOWAIT;

		SET @cmd = 'DBCC CLONEDATABASE (' + @sourceDB + ', ' + @destinationDB + ') WITH NO_STATISTICS;'
		BEGIN TRY
			EXEC sp_executesql @cmd
		END TRY
		BEGIN CATCH
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not clone database' + CHAR(13) + CHAR(10) + ERROR_MESSAGE()
			RAISERROR(@msg, 10, 1) WITH NOWAIT;
			RETURN;
		END CATCH
	END

	-- Change DestinationDB to READ_WRITE
	IF(@cloneDB = 1)
	BEGIN
		SET @cmd = 'ALTER DATABASE [' + @destinationDB + '] SET READ_WRITE WITH ROLLBACK IMMEDIATE;'
		BEGIN TRY
			EXEC sp_executesql @cmd
		END TRY
		BEGIN CATCH
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not change destination database to READ_WRITE' + CHAR(13) + CHAR(10) + ERROR_MESSAGE()
			RAISERROR(@msg, 10, 1) WITH NOWAIT;
			RETURN;
		END CATCH
	END

	-- Check if Destination DB exists
	IF NOT EXISTS (SELECT 1 FROM master.sys.databases WHERE name = @destinationDB)
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Destination database not found - terminating script'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		RETURN;
	END

	-- Check if Destination DB is in Simple Mode
	IF((SELECT recovery_model_desc FROM master.sys.databases WHERE name = @destinationDB) != 'SIMPLE')
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | WARN | Setting recovery model of destination database to SIMPLE'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;

		SET @cmd = 'USE [master];
		ALTER DATABASE [' + @destinationDB + '] SET RECOVERY SIMPLE WITH NO_WAIT;'

		BEGIN TRY
			EXEC sp_executesql @cmd
		END TRY
		BEGIN CATCH
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not set recovery model to simple on database ' + @destinationDB + CHAR(13) + CHAR(10) + ERROR_MESSAGE()
			RAISERROR(@msg, 10, 1) WITH NOWAIT;
			RETURN;
		END CATCH
	END

	-- Compare Objects
	--Source DB
	SET @cmd = '
		SELECT name
		FROM [' + @sourceDB + '].sys.objects
		WHERE is_ms_shipped = 0
		AND name NOT IN (SELECT name FROM [' + @destinationDB + '].sys.objects)
	'
	BEGIN TRY
		INSERT INTO @tblMissingObjects (objName)
		EXEC sp_executesql @cmd
	END TRY
	BEGIN CATCH
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | WARN | Could not collect objects information' + CHAR(13) + CHAR(10) + ERROR_MESSAGE()
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
	END CATCH

	IF EXISTS (
		SELECT 1 FROM @tblMissingObjects
	)
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | WARN | Schema mismatch in source and destination database'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
	END

	-- Read all Table Names from Source
	SET @cmd = '
		USE [' + @sourceDB + '];
		SELECT ''['' + SCHEMA_NAME(schema_id) + ''].['' + name + '']''
		FROM [' + @sourceDB + '].sys.objects
		WHERE is_ms_shipped = 0
		AND type = ''U''
	'
	BEGIN TRY
		INSERT INTO @tblTableNames (objName)
		EXEC sp_executesql @cmd
	END TRY
	BEGIN CATCH
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not collect table names' + CHAR(13) + CHAR(10) + ERROR_MESSAGE()
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		RETURN;
	END CATCH

	-- CHECK ROW COUNT IN DESTINATION
	DECLARE @tmpRowCountTableName NVARCHAR(MAX)
	DECLARE curCheckRowCountsInDestination CURSOR FOR
		SELECT objName FROM @tblTableNames

	OPEN curCheckRowCountsInDestination
	FETCH NEXT FROM curCheckRowCountsInDestination INTO @tmpRowCountTableName
	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @cmd = 'SELECT ''' + @tmpRowCountTableName + ''', COUNT(1) FROM [' + @destinationDB + '].' + @tmpRowCountTableName
		
		BEGIN TRY
			INSERT INTO @chkDestDBRowCount (tableName, rowCounts)
			EXEC sp_executesql @cmd
		END TRY
		BEGIN CATCH
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not count rows in destination table ' + @tmpRowCountTableName + CHAR(13) + CHAR(10) + ERROR_MESSAGE()
			RAISERROR(@msg, 10, 1) WITH NOWAIT;
			RETURN;
		END CATCH

		FETCH NEXT FROM curCheckRowCountsInDestination INTO @tmpRowCountTableName
	END
	CLOSE curCheckRowCountsInDestination
	DEALLOCATE curCheckRowCountsInDestination

	-- Check if rowcounts SUM > 0
	IF((SELECT SUM(rowCounts) FROM @chkDestDBRowCount) > 0)
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Rows found in destination database - destination tables must be empty'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		RETURN;
	END

	-- Collect CHECK Constraints
	SET @cmd = '
		USE [' + @sourceDB + '];
		SELECT ''['' + a.[name] + '']'' AS consName,
			   ''['' + SCHEMA_NAME(b.schema_id) + ''].['' + b.[name] + '']'' as tblName
		FROM [' + @sourceDB + '].sys.check_constraints a
		LEFT OUTER JOIN [' + @sourceDB + '].sys.objects b
			ON a.parent_object_id = b.object_id
		WHERE b.is_ms_shipped = 0
		AND a.is_disabled = 0
	';
	BEGIN TRY
		INSERT INTO @tblCheckConstraints (objName, tableName)
		EXEC sp_executesql @cmd
	END TRY
	BEGIN CATCH
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not collect CHECK CONSTRAINTS' + CHAR(13) + CHAR(10) + ERROR_MESSAGE()
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		RETURN;
	END CATCH

	-- Status
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | CHECK CONSTRAINTS found: ' + (SELECT CONVERT(NVARCHAR(MAX), COUNT(1)) FROM @tblCheckConstraints)
	RAISERROR(@msg, 10, 1) WITH NOWAIT

	-- Collect FK Constraints
	SET @cmd = '
		USE [' + @sourceDB + '];
		SELECT ''['' + a.name + '']''  AS FKConName,
			   ''['' + schema_name(b.schema_id) + ''].['' + b.[name] + '']'' as [table]
		FROM [' + @sourceDB + '].sys.foreign_keys a
		LEFT OUTER JOIN [' + @sourceDB + '].sys.objects b
			ON a.parent_object_id = b.object_id
		WHERE a.is_ms_shipped = 0
		AND a.is_disabled = 0
	';
	BEGIN TRY
		INSERT INTO @tblFKConstraints (objName, tableName)
		EXEC sp_executesql @cmd
	END TRY
	BEGIN CATCH
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not collect FOREIGN KEY CONSTRAINTS' + CHAR(13) + CHAR(10) + ERROR_MESSAGE()
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		RETURN;
	END CATCH

	-- Status
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | FOREIGN KEY CONSTRAINTS found: ' + (SELECT CONVERT(NVARCHAR(MAX), COUNT(1)) FROM @tblFKConstraints)
	RAISERROR(@msg, 10, 1) WITH NOWAIT

	-- Collect Triggers
	SET @cmd = '
		USE [' + @sourceDB + '];
		SELECT ''['' + schema_name(b.schema_id) + ''].['' + a.name + '']'' AS TriggerName,
			   ''['' + schema_name(b.schema_id) + ''].['' + b.[name] + '']'' as [table]
		FROM [' + @sourceDB + '].sys.triggers a
		LEFT OUTER JOIN [' + @sourceDB + '].sys.objects b
			ON a.parent_id = b.object_id
		WHERE a.is_disabled = 0
	';
	BEGIN TRY
		INSERT INTO @tblTriggers (objName, tableName)
		EXEC sp_executesql @cmd
	END TRY
	BEGIN CATCH
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not collect TRIGGERS' + CHAR(13) + CHAR(10) + ERROR_MESSAGE()
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		RETURN;
	END CATCH

	-- Status
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | TRIGGERS found: ' + (SELECT CONVERT(NVARCHAR(MAX), COUNT(1)) FROM @tblTriggers)
	RAISERROR(@msg, 10, 1) WITH NOWAIT

	-- Collect Tables with IDENTITY property
	SET @cmd = '
		USE [' + @sourceDB + '];
		SELECT ''['' + b.name + ''].['' + a.name + '']''
		FROM [' + @sourceDB + '].sys.objects a
		INNER JOIN [' + @sourceDB + '].sys.schemas b
		ON a.schema_id = b.schema_id
		INNER JOIN [' + @sourceDB + '].sys.columns c
			ON a.object_id = c.object_id
		WHERE a.[type] = ''U''
		AND a.is_ms_shipped = 0
		AND c.is_identity = 1
	'
	BEGIN TRY
		INSERT INTO @tblIdentityTables (objName)
		EXEC sp_executesql @cmd
	END TRY
	BEGIN CATCH
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not collect tables with IDENTITY property' + CHAR(13) + CHAR(10) + ERROR_MESSAGE()
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		RETURN;
	END CATCH

	-- Status
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Tables with IDENTITY property found: ' + (SELECT CONVERT(NVARCHAR(MAX), COUNT(1)) FROM @tblIdentityTables)
	RAISERROR(@msg, 10, 1) WITH NOWAIT


	-- INSERT COLUMNS
	SET @cmd = '
		USE [' + @sourceDB + '];
		SELECT 
			  table_name = ''['' + s.name + ''].['' + o.name + '']''
			, [columns] = STUFF((
				SELECT '', '' + ''['' + c.name + '']''
				FROM [' + @sourceDB + '].sys.columns c WITH (NOWAIT)
				WHERE c.[object_id] = o.[object_id]
				AND c.is_computed = 0
				FOR XML PATH(''''), TYPE).value(''.'', ''NVARCHAR(MAX)''), 1, 2, '''')
		FROM (
			SELECT 
				  o.[object_id]
				, o.name
				, o.[schema_id]
			FROM [' + @sourceDB + '].sys.objects o WITH (NOWAIT)
			WHERE o.[type] = ''U''
				AND o.is_ms_shipped = 0
		) o
		INNER JOIN [' + @sourceDB + '].sys.schemas s WITH (NOWAIT) ON o.[schema_id] = s.[schema_id] 
		ORDER BY 
			  s.name
			, o.name;
	';
	BEGIN TRY
		INSERT INTO @tblInsertColumns (tableName, insertColumns)
		EXEC sp_executesql @cmd
	END TRY
	BEGIN CATCH
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not collect table INSERT columns' + CHAR(13) + CHAR(10) + ERROR_MESSAGE()
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		RETURN;
	END CATCH

	-- SELECT COLUMNS
	INSERT INTO @tblSelectColumns (tableName, selectColumns)
	SELECT tableName, insertColumns
	FROM @tblInsertColumns

	-- Get Columns with XML data type
	SET @cmd = '
		USE [' + @sourceDB + '];
		SELECT ''['' + SCHEMA_NAME(a.schema_id) + ''].['' + a.name + '']'',
			   ''['' + b.name + '']''
		FROM [' + @sourceDB + '].sys.objects a
		INNER JOIN [' + @sourceDB + '].sys.columns b
			ON a.object_id = b.object_id
		WHERE a.is_ms_shipped = 0
		AND a.type = ''U''
		AND b.xml_collection_id > 0
	';

	BEGIN TRY
		INSERT INTO @tblColsWithXML (tableName, xmlColumns)
		EXEC sp_executesql @cmd
	END TRY
	BEGIN CATCH
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not collect columns with XML data type' + CHAR(13) + CHAR(10) + ERROR_MESSAGE()
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		RETURN;
	END CATCH

	-- Replace Columns in Select Columns with CONVERT(XML, <column>)
	DECLARE @xmlColTableName NVARCHAR(MAX)
	DECLARE @xmlColumnName NVARCHAR(MAX)
	DECLARE curXMLColumns CURSOR FOR
		SELECT tableName, xmlColumns FROM @tblColsWithXML
	OPEN curXMLColumns
	FETCH NEXT FROM curXMLColumns INTO @xmlColTableName, @xmlColumnName
	WHILE @@FETCH_STATUS = 0
	BEGIN
		UPDATE @tblSelectColumns
		SET selectColumns = REPLACE(selectColumns, @xmlColumnName, 'CONVERT(XML, ' + @xmlColumnName + ')')
		WHERE tableName = @xmlColTableName
		FETCH NEXT FROM curXMLColumns INTO @xmlColTableName, @xmlColumnName
	END
	CLOSE curXMLColumns
	DEALLOCATE curXMLColumns

	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Validation finished - starting process'
	RAISERROR(@msg, 10, 1) WITH NOWAIT

	/*
	
	DISABLE OBJECTS

	*/

	-- Disable Check Constraints
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Disabling CHECK CONSTRAINTS'
	RAISERROR(@msg, 10, 1) WITH NOWAIT
	
	DECLARE curDisableCheckConstraints CURSOR FOR
		SELECT objName, tableName FROM @tblCheckConstraints
	OPEN curDisableCheckConstraints
	FETCH NEXT FROM curDisableCheckConstraints INTO @curObjName, @curTableName
	WHILE @@FETCH_STATUS = 0
	BEGIN
		
		SET @cmd = '
			USE [' + @destinationDB + '];
			ALTER TABLE ' + @curTableName + ' NOCHECK CONSTRAINT ' + @curObjName + ';
		';
		BEGIN TRY
			EXEC sp_executesql @cmd
		END TRY
		BEGIN CATCH
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not disable CHECK CONSTRAINT ' + @curObjName + ' on table ' + @curTableName + CHAR(13) + CHAR(10) + ERROR_MESSAGE()
			RAISERROR(@msg, 10, 1) WITH NOWAIT;
			SET @errorCount += 1
		END CATCH
		FETCH NEXT FROM curDisableCheckConstraints INTO @curObjName, @curTableName
	END
	CLOSE curDisableCheckConstraints
	DEALLOCATE curDisableCheckConstraints
	
	-- CHECK ERROR COUNT
	IF(@errorCount > 0)
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Terminating script'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		RETURN;
	END
	ELSE
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | All CHECK CONSTRAINTS disabled'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
	END

	-- Disable FK Constraints
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Disabling FOREIGN KEY CONSTRAINTS'
	RAISERROR(@msg, 10, 1) WITH NOWAIT
	SET @curObjName = ''
	SET @curTableName = ''
	DECLARE curDisableFKConstraints CURSOR FOR
		SELECT objName, tableName FROM @tblFKConstraints
	OPEN curDisableFKConstraints
	FETCH NEXT FROM curDisableFKConstraints INTO @curObjName, @curTableName
	WHILE @@FETCH_STATUS = 0
	BEGIN
		
		SET @cmd = '
			USE [' + @destinationDB + '];
			ALTER TABLE ' + @curTableName + ' NOCHECK CONSTRAINT ' + @curObjName + ';
		';
		BEGIN TRY
			EXEC sp_executesql @cmd
		END TRY
		BEGIN CATCH
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not disable FK CONSTRAINT ' + @curObjName + ' on table ' + @curTableName + CHAR(13) + CHAR(10) + ERROR_MESSAGE()
			RAISERROR(@msg, 10, 1) WITH NOWAIT;
			SET @errorCount += 1
		END CATCH
		FETCH NEXT FROM curDisableFKConstraints INTO @curObjName, @curTableName
	END
	CLOSE curDisableFKConstraints
	DEALLOCATE curDisableFKConstraints
	
	-- CHECK ERROR COUNT
	IF(@errorCount > 0)
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Terminating script'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		RETURN;
	END
	ELSE
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | All FOREIGN KEY CONSTRAINTS disabled'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
	END


	-- Disable Triggers
	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Disabling TRIGGERS'
	RAISERROR(@msg, 10, 1) WITH NOWAIT
	SET @curObjName = ''
	SET @curTableName = ''
	DECLARE curDisableTriggers CURSOR FOR
		SELECT objName, tableName FROM @tblTriggers
	OPEN curDisableTriggers
	FETCH NEXT FROM curDisableTriggers INTO @curObjName, @curTableName
	WHILE @@FETCH_STATUS = 0
	BEGIN
		
		SET @cmd = '
			USE [' + @destinationDB + '];
			DISABLE TRIGGER ' + @curObjName + ' ON ' + @curTableName + ';
		';
		BEGIN TRY
			EXEC sp_executesql @cmd
		END TRY
		BEGIN CATCH
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not disable TRIGGER ' + @curObjName + ' on table ' + @curTableName + CHAR(13) + CHAR(10) + ERROR_MESSAGE()
			RAISERROR(@msg, 10, 1) WITH NOWAIT;
			SET @errorCount += 1
		END CATCH
		FETCH NEXT FROM curDisableTriggers INTO @curObjName, @curTableName
	END
	CLOSE curDisableTriggers
	DEALLOCATE curDisableTriggers
	
	-- CHECK ERROR COUNT
	IF(@errorCount > 0)
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Terminating script'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		RETURN;
	END
	ELSE
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | All TRIGGERS disabled'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
	END

	/*

	TRANSFER DATA

	*/

	DECLARE @curCopyTableName NVARCHAR(MAX)
	DECLARE @curCopyInsertColumns NVARCHAR(MAX)
	DECLARE @curCopySelectColumns NVARCHAR(MAX)
	DECLARE @curCopyIsIdentity INT
	DECLARE curCopyTable CURSOR FOR
		SELECT a.tableName, 
			   a.insertColumns, 
			   b.selectColumns, 
			   CASE WHEN c.objName IS NULL
					THEN 0
					ELSE 1
			   END AS isIdentity
		FROM @tblInsertColumns a
		INNER JOIN @tblSelectColumns b
			ON a.tableName = b.tableName
		LEFT JOIN @tblIdentityTables c
			ON a.tableName = c.objName
	OPEN curCopyTable
	FETCH NEXT FROM curCopyTable INTO @curCopyTableName, @curCopyInsertColumns, @curCopySelectColumns, @curCopyIsIdentity
	-- Loop through each table
	WHILE @@FETCH_STATUS = 0
	BEGIN
		
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Copy data to table ' + @curCopyTableName
		RAISERROR(@msg, 10, 1) WITH NOWAIT;

		SET @cmd = 'USE [' + @destinationDB + '];'
		
		-- Check if IDENTITY
		IF(@curCopyIsIdentity = 1)
		BEGIN
			SET @cmd += 'SET IDENTITY_INSERT ' + @curCopyTableName + ' ON; '
		END
		
		-- INSERT ... SELECT
		SET @cmd += 'INSERT INTO [' + @destinationDB + '].' + @curCopyTableName + ' (' + @curCopyInsertColumns + ') 
					 SELECT ' + @curCopySelectColumns + ' FROM [' + @sourceDB + '].' + @curCopyTableName + '; '; 

		-- Check if IDENTITY
		IF(@curCopyIsIdentity = 1)
		BEGIN
			SET @cmd += 'SET IDENTITY_INSERT ' + @curCopyTableName + ' OFF; '
		END

		-- EXECUTE
		BEGIN TRY
			EXEC sp_executesql @cmd
		END TRY
		BEGIN CATCH
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not copy data to table ' + @curCopyTableName + CHAR(13) + CHAR(10) + ERROR_MESSAGE()
			RAISERROR(@msg, 10, 1) WITH NOWAIT;
			INSERT INTO @tblFailed (tableName) VALUES (@curCopyTableName)
			SET @errorCount += 1;
		END CATCH

		FETCH NEXT FROM curCopyTable INTO @curCopyTableName, @curCopyInsertColumns, @curCopySelectColumns, @curCopyIsIdentity
	END

	CLOSE curCopyTable
	DEALLOCATE curCopyTable

	-- Status
	IF(@errorCount > 0)
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | WARN | Failed tables count: ' + CONVERT(NVARCHAR(MAX), @errorCount)
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
	END
	ELSE
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | All tables copied'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
	END

	/*
	
	ENABLE OBJECTS

	*/

	-- Enable Triggers
	SET @errorCount = 0

	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Enabling TRIGGERS'
	RAISERROR(@msg, 10, 1) WITH NOWAIT
	SET @curObjName = ''
	SET @curTableName = ''
	DECLARE curEnableTriggers CURSOR FOR
		SELECT objName, tableName FROM @tblTriggers
	OPEN curEnableTriggers
	FETCH NEXT FROM curEnableTriggers INTO @curObjName, @curTableName
	WHILE @@FETCH_STATUS = 0
	BEGIN
		
		SET @cmd = '
			USE [' + @destinationDB + '];
			ENABLE TRIGGER ' + @curObjName + ' ON ' + @curTableName + ';
		';
		BEGIN TRY
			EXEC sp_executesql @cmd
		END TRY
		BEGIN CATCH
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not enable TRIGGER ' + @curObjName + ' on table ' + @curTableName + CHAR(13) + CHAR(10) + ERROR_MESSAGE()
			RAISERROR(@msg, 10, 1) WITH NOWAIT;
			SET @errorCount += 1
		END CATCH
		FETCH NEXT FROM curEnableTriggers INTO @curObjName, @curTableName
	END
	CLOSE curEnableTriggers
	DEALLOCATE curEnableTriggers
	
	-- CHECK ERROR COUNT
	IF(@errorCount > 0)
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | WARN | Some or all triggers have not been enabled'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
	END
	ELSE
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | All TRIGGERS enabled'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
	END


	-- Enable FK Constraints
	SET @errorCount = 0

	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Enabling FOREIGN KEY CONSTRAINTS'
	RAISERROR(@msg, 10, 1) WITH NOWAIT
	SET @curObjName = ''
	SET @curTableName = ''
	DECLARE curEnableFKConstraints CURSOR FOR
		SELECT objName, tableName FROM @tblFKConstraints
	OPEN curEnableFKConstraints
	FETCH NEXT FROM curEnableFKConstraints INTO @curObjName, @curTableName
	WHILE @@FETCH_STATUS = 0
	BEGIN
		
		SET @cmd = '
			USE [' + @destinationDB + '];
			ALTER TABLE ' + @curTableName + ' CHECK CONSTRAINT ' + @curObjName + ';
		';
		BEGIN TRY
			EXEC sp_executesql @cmd
		END TRY
		BEGIN CATCH
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not enable FK CONSTRAINT ' + @curObjName + ' on table ' + @curTableName + CHAR(13) + CHAR(10) + ERROR_MESSAGE()
			RAISERROR(@msg, 10, 1) WITH NOWAIT;
			SET @errorCount += 1
		END CATCH
		FETCH NEXT FROM curEnableFKConstraints INTO @curObjName, @curTableName
	END
	CLOSE curEnableFKConstraints
	DEALLOCATE curEnableFKConstraints
	
	-- CHECK ERROR COUNT
	IF(@errorCount > 0)
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | WARN | Some or all FK CONSTRAINTS have not been enabled'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		RETURN;
	END
	ELSE
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | All FOREIGN KEY CONSTRAINTS enabled'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
	END


	-- Enable Check Constraints
	SET @errorCount = 0;

	SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | Enabling CHECK CONSTRAINTS'
	RAISERROR(@msg, 10, 1) WITH NOWAIT
	
	DECLARE curEnableCheckConstraints CURSOR FOR
		SELECT objName, tableName FROM @tblCheckConstraints
	OPEN curEnableCheckConstraints
	FETCH NEXT FROM curEnableCheckConstraints INTO @curObjName, @curTableName
	WHILE @@FETCH_STATUS = 0
	BEGIN
		
		SET @cmd = '
			USE [' + @destinationDB + '];
			ALTER TABLE ' + @curTableName + ' CHECK CONSTRAINT ' + @curObjName + ';
		';
		BEGIN TRY
			EXEC sp_executesql @cmd
		END TRY
		BEGIN CATCH
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not enable CHECK CONSTRAINT ' + @curObjName + ' on table ' + @curTableName + CHAR(13) + CHAR(10) + ERROR_MESSAGE()
			RAISERROR(@msg, 10, 1) WITH NOWAIT;
			SET @errorCount += 1
		END CATCH
		FETCH NEXT FROM curEnableCheckConstraints INTO @curObjName, @curTableName
	END
	CLOSE curEnableCheckConstraints
	DEALLOCATE curEnableCheckConstraints
	
	-- CHECK ERROR COUNT
	IF(@errorCount > 0)
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | WARN | Some or all CHECK CONSTRAINTS have not been enabled'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
		RETURN;
	END
	ELSE
	BEGIN
		SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | INFO | All CHECK CONSTRAINTS enabled'
		RAISERROR(@msg, 10, 1) WITH NOWAIT;
	END

	-- SELECT Failed Tables
	SELECT tableName AS [Failed Tables] FROM @tblFailed

	-- COLLECT Destination ROWCOUNTS
	-- Delete all Entries from table
	DELETE FROM @chkDestDBRowCount

	-- CHECK ROW COUNT IN DESTINATION
	DECLARE @finalRowCountTableName NVARCHAR(MAX)
	DECLARE curCheckRowCountsInDestinationFinal CURSOR FOR
		SELECT objName FROM @tblTableNames

	OPEN curCheckRowCountsInDestinationFinal
	FETCH NEXT FROM curCheckRowCountsInDestinationFinal INTO @finalRowCountTableName
	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @cmd = 'SELECT ''' + @finalRowCountTableName + ''', COUNT(1) FROM [' + @destinationDB + '].' + @finalRowCountTableName
		
		BEGIN TRY
			INSERT INTO @chkDestDBRowCount (tableName, rowCounts)
			EXEC sp_executesql @cmd
		END TRY
		BEGIN CATCH
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not count rows in destination table ' + @finalRowCountTableName + CHAR(13) + CHAR(10) + ERROR_MESSAGE()
			RAISERROR(@msg, 10, 1) WITH NOWAIT;
			RETURN;
		END CATCH

		FETCH NEXT FROM curCheckRowCountsInDestinationFinal INTO @finalRowCountTableName
	END
	CLOSE curCheckRowCountsInDestinationFinal
	DEALLOCATE curCheckRowCountsInDestinationFinal

	--  Check row counts in Source
	SET @finalRowCountTableName = ''
	DECLARE curCheckRowCountsInSourceFinal CURSOR FOR
		SELECT objName FROM @tblTableNames

	OPEN curCheckRowCountsInSourceFinal
	FETCH NEXT FROM curCheckRowCountsInSourceFinal INTO @finalRowCountTableName
	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @cmd = 'SELECT ''' + @finalRowCountTableName + ''', COUNT(1) FROM [' + @sourceDB + '].' + @finalRowCountTableName
		
		BEGIN TRY
			INSERT INTO @chkSourceDBRowCount (tableName, rowCounts)
			EXEC sp_executesql @cmd
		END TRY
		BEGIN CATCH
			SET @msg = FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss') + ' | ERROR | Could not count rows in source table ' + @finalRowCountTableName + CHAR(13) + CHAR(10) + ERROR_MESSAGE()
			RAISERROR(@msg, 10, 1) WITH NOWAIT;
			RETURN;
		END CATCH

		FETCH NEXT FROM curCheckRowCountsInSourceFinal INTO @finalRowCountTableName
	END
	CLOSE curCheckRowCountsInSourceFinal
	DEALLOCATE curCheckRowCountsInSourceFinal

	-- SELECT RESULT
	SELECT a.tableName AS [TableName],
		   a.rowCounts AS [Source Row Count],
		   b.rowCounts AS [Dest Row Count]
	FROM @chkSourceDBRowCount a
	INNER JOIN @chkDestDBRowCount b
		ON a.tableName = b.tableName

END
