/*
Copyright (c) 2016 SQL Fairy http://sqlfairy.com.au

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

___UPDATE HISTORY___

8/6/2016... for version V002.

Update scripts to support backup to an URL like \\Servername\Sharename or \\Servername\Sharename\Whatever
 
done:	Update sp_ScheduleBackups to accept an @urlPath parameter.  This will be mutually exclusive with @StorageContainerURL.  
done:	Update sp_ScheduleBackups to check if an @urlPath param has been supplied and that it looks like \\someServer\someShare
done:	Update sp_ScheduleBackups to accept optional @intFullBackupExpireDays, @intDiffBackupExpireDays and @intLogBackupExpireDays parameters.  If supplied these will be used with Ola's scripts to expire the backups.
done:	Update sp_ScheduleBackups to create jobs as backup to UNC paths instead of URL
done:	Update usp_RestoreDB to have optional parameter for BlobCredential.  If possible have it check to see if any of the files to be restored are from an URL path and complain about not having the parameter if necessary.
done:	Update usp_AutoMirror to have optional parameter for BlobCredential.  If possible have it check to see if any of the files to be restored are from an URL path and complain about not having the parameter if necessary.
done:	Make changes so that all of the above param changes work :)
done:	Update documentation

Todo: Test basic use cases for sp_ScheduleBackups, usp_RestoreDB, usp_AutoMirror
Todo: In testing noticed that it is possible to encounter an error because a database has not yet been backed up with a full + trans log backup.  It should be pretty easy to check for this as we do with the 
			tests for whether there is an URL backup or not.  Consider adding better checking & advice for this rather than allowing the process to fail with an ambiguous(ish) error.



_______________________

The following stored procs are provided by SQLFairy http://sqlfairy.com.au .  

Hopefully they'll make your life more pleasant in some small way. Enjoy :)

----------------------------------------------------
SQLFairyBackupTools_V001.sql adds the following stored procs:

dbo.sp_ScheduleBackups – Generates full, differential and transaction log SQL Server Agent jobs to backup all databases to Azure blob storage using Ola Hallengren’s backup script.  Also schedules the jobs based on provided input.

dbo.sp_ScheduleMaintenance – generates separate indexOptimise and DatabaseIntegrityCheck jobs which call on Ola Hallengren’s IndexOptimize and DatabaseIntegrityCheck stored procedures respectively for each of your databases.

dbo.usp_RestoreDB – Allows for automagic restore of databases from servers which are being backed up to Azure.

dbo.usp_AutoMirror – An awesome and fun tool :) which uses a similar approach to usp_RestoreDB but takes things a step further by automatically mirroring databases.

dbo.usp_GenerateRestoreScriptForThisServer – Called by dbo.usp_EmailRestoreScript.  Uses sp_RestoreGene to generate a current restore script for all databases (which have been backed up) on the server.

dbo.usp_EmailRestoreScript – This script is called from a step in each of the backup jobs created by dbo.sp_ScheduleBackups.  After each backup it generates and emails a current restore script for all of the databases on the server.  This means that in the case of an emergency where you might no longer be able to use the tools which rely on the backup history in the master database, you still have a restore script on hand which will allow you to get back up and running on another server/whatever.

----------------------------------------------------

You'll find a detailed discussion on these tools at http://www.sqlfairy.com.au/2016/06/backup-to-the-cloud-super-dev-ops-restore-powers-dr-and-more/

*/


--------------------------------------------------------------------

USE [master] 
GO 
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'usp_GenerateRestoreScriptForThisServer') 
EXEC ('CREATE PROC dbo.usp_GenerateRestoreScriptForThisServer AS SELECT ''stub version, to be replaced''') 
GO

ALTER PROCEDURE usp_GenerateRestoreScriptForThisServer @BlobCredential VARCHAR(255) = NULL , @Debug INT = NULL
AS 
BEGIN

/*
Copyright (c) 2016 SQL Fairy http://sqlfairy.com.au

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.


dbo.usp_GenerateRestoreScriptForThisServer - Called by dbo.usp_EmailRestoreScript.  
Uses sp_RestoreGene to generate a current restore script for all databases (which have been backed up) on the server.

Provided by SQLFairy http://sqlfairy.com.au

For more info please visit http://www.sqlfairy.com.au/2016/06/backup-to-the-cloud-super-dev-ops-restore-powers-dr-and-more/
*/


	SET NOCOUNT ON
	IF @BlobCredential IS NULL SET @BlobCredential = 'myBlobCredentialHere'
	
	DECLARE @runRestoreGeneSQL NVARCHAR(max)
	SET @runRestoreGeneSQL = 
		--CASE WHEN @SourceServerIsLocal = 0 THEN '[' + @SourceServer + '].' ELSE '' END +
		'	[master].[dbo].[sp_RestoreGene]
			@BlobCredential = ''' + @BlobCredential + '''
		,	@WithRecovery = 1 
		,	@WithReplace = 0 ' 


	DECLARE @RestoreRows AS TABLE
	(	TSQL NVARCHAR(MAX)
	,	BackupDate DATETIME
	,	BackupDevice NVARCHAR(255)
	,	Last_LSN NUMERIC(32) NULL
	,	Databaase_Name sysname
	,	SortSequence INT
	,	blnProcessed BIT DEFAULT 0
	)

	INSERT INTO @RestoreRows
			([TSQL]
			,[BackupDate]
			,[BackupDevice]
			,[Last_LSN]
			,[Databaase_Name]
			,[SortSequence]
			)

	EXEC sp_executesql @runRestoreGeneSQL

	IF @debug > 2 SELECT * FROM @RestoreRows

	DECLARE @RestoreStatement NVARCHAR(max)

	SET @RestoreStatement = '' --Can't append a null

	WHILE (SELECT COUNT(SortSequence) FROM @RestoreRows WHERE blnProcessed = 0) > 0
	BEGIN 
		SET @RestoreStatement = @RestoreStatement + (SELECT TOP 1 TSQL FROM @RestoreRows WHERE [@RestoreRows].[blnProcessed] = 0 ORDER BY [@RestoreRows].[SortSequence]) + CHAR(10) 
		UPDATE @RestoreRows SET [blnProcessed] = 1 WHERE [@RestoreRows].[SortSequence] = (SELECT TOP 1 [@RestoreRows].[SortSequence] FROM @RestoreRows WHERE [@RestoreRows].[blnProcessed] = 0 ORDER BY [@RestoreRows].[SortSequence])
	END

	IF @debug > 1 PRINT '--Restore Script to be run on ' + @@servername + CHAR(10) + @RestoreStatement + CHAR(10)

	SELECT @RestoreStatement AS [DR restore script]
END 
GO 

--EXEC usp_GenerateRestoreScriptForThisServer

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'usp_EmailRestoreScript') 
EXEC ('CREATE PROC dbo.usp_EmailRestoreScript AS SELECT ''stub version, to be replaced''') 
GO
ALTER PROCEDURE usp_EmailRestoreScript 
(
	@BlobCredential VARCHAR(255) = NULL
,	@profileName	NVARCHAR(255)
,	@recipientAddress NVARCHAR(1024) --That's something like what the rfc says iirc???
)
AS
BEGIN
/*
Copyright (c) 2016 SQL Fairy http://sqlfairy.com.au

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

dbo.usp_EmailRestoreScript 
This script is called from a step in each of the backup jobs created by dbo.sp_ScheduleBackups.  
After each backup it generates and emails a current restore script for all of the databases on the server.  
This means that in the case of an emergency where you might no longer be able to use the tools which rely on the backup history in the master database, 
you still have a restore script on hand which will allow you to get back up and running on another server/whatever.

Provided by SQLFairy http://sqlfairy.com.au

For more info please visit http://www.sqlfairy.com.au/2016/06/backup-to-the-cloud-super-dev-ops-restore-powers-dr-and-more/
*/

	DECLARE 
		@attachmentFilename VARCHAR(255)
	,	@bodyText NVARCHAR(255)
	,	@commandText NVARCHAR(MAX)
	,	@subjectText VARCHAR(255)

	SET @attachmentFilename = 'RestoreScript_' + @@servername + '_' + REPLACE(CONVERT(varchar(30),getdate(), 126),':','-') +'.SQL'
	SET @subjectText = 'DR restore script for ' + @@servername + ' ' + REPLACE(CONVERT(varchar(30),getdate(), 126),':','-')
	SET @bodyText = 'Attached ' + @attachmentFilename + ' contains restore commands required to recover databases from ' + @@servername + '.'
	SET @commandText = 'SET NOCOUNT ON;EXEC usp_GenerateRestoreScriptForThisServer' + 
		CASE WHEN @BlobCredential IS NOT NULL THEN ' @BlobCredential = ''' + @BlobCredential + '''' ELSE '' END

		
	EXEC msdb.dbo.sp_send_dbmail	
		@profile_name = @profileName
	,	@recipients = @recipientAddress
	,	@query = @commandText
	,	@attach_query_result_as_file = 1
	,	@query_no_truncate = 1
	,	@exclude_query_output = 0
	,	@query_result_header = 1
	,	@query_attachment_filename = @attachmentFilename
	,	@query_result_width = 4096
	,	@body = @bodyText
	,	@subject = @subjectText
	
END
GO
/*
EXEC usp_EmailRestoreScript	@BlobCredential = 'ThisIsMyCred'
								,@profileName = N'DataShed'
								,@recipientAddress = N'your.email@here.com'
--*/



USE [master] 
GO 
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'sp_ScheduleBackups') 
EXEC ('CREATE PROC dbo.sp_ScheduleBackups AS SELECT ''stub version, to be replaced''') 
GO 

ALTER PROC [dbo].[sp_ScheduleBackups]
(
	@OlaDBName SYSNAME = 'master' --The database which contains the OLA Hallengren maintenance solution.
,	@StorageContainerURL NVARCHAR(255) = NULL --Full url to storage container where this server will backup.  This is the Azure "Primary blob service endpoint" + / + "Blob storage Container name" NB: Now optional.  Must supply either this or @URLBackupPath.
,	@BlobCredential NVARCHAR(255) = NULL --The name of the blob credential.  This corresponds to the "Storage Account Name" of your Storage Account.  
,	@BlobCredentialSecret NVARCHAR(255) = NULL --Shhhh.  This is either the "primary access key" or "secondary access key" for your Azure Storage Account.
,	@ProfileName NVARCHAR(255) --The email profile to use on the system.  Required because we are sending emails from a job step.  Must already exist
,	@OperatorName NVARCHAR(255) --This is the name of an Operator on your server.  If the operator specified doesn't exist it will be created.
,	@OperatorEmail NVARCHAR(1024) --This is the email address of the operator specified in @OperatorName.  This is only effective if the operator doesn't already exist.  It will not update the email of an existing operator.
,	@DRScriptEmail NVARCHAR(1024) = NULL --Can be specified to send database restore scripts to a different email address.  If not supplied defaults to @OperatorEmail
,	@FullBackupDay VARCHAR(15) = NULL --We default to Saturdays further down if not supplied (or matched).  This can be supplied as 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat' or 'Sun'. 
,	@LogBackupFreqMins INT = 15 --Default to 15 minute interval if not supplied.  This is how often we're going to schedule transaction log backups.
,	@BackupTime TIME(0) = NULL --Time that Full and Differential backups will start e.g. '11:00:00 PM' or '23:00:00'
,	@URLBackupPath NVARCHAR(255) = NULL --An optional URL path \\Servername\Sharename\SomeOptional\OtherPath\ which can be supplied instead of the @StorageContainerURL
,	@intFullBackupExpireDays INT = NULL --Optional param to supply the number of days for backup expiry to Ola Hallengren jobs.  Only effective with @URLBackupPath as this is not supported for blob storage
,	@intDiffBackupExpireDays INT = NULL --Optional param to supply the number of days for backup expiry to Ola Hallengren jobs.  Only effective with @URLBackupPath as this is not supported for blob storage
,	@intLogBackupExpireDays INT  = NULL --Optional param to supply the number of days for backup expiry to Ola Hallengren jobs.  Only effective with @URLBackupPath as this is not supported for blob storage
)
AS
BEGIN
/*
Copyright (c) 2016 SQL Fairy http://sqlfairy.com.au

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

dbo.sp_ScheduleBackups 
Generates full, differential and transaction log SQL Server Agent jobs to backup all databases to Azure blob storage using Ola Hallengren’s backup script.  
Also schedules the jobs based on provided input.

@OlaDBName SYSNAME
 The database which contains the Ola Hallengren Maintenance Solution scripts

@StorageContainerURL NVARCHAR(255)
 The full URL to the storage container where this server will backup.  This is the Azure "Primary blob service endpoint" + / + "Blob storage Container name"
 This parameter is optional and is mutually exclusive with @URLBackupPath.  Highlander.

@BlobCredential NVARCHAR(255)
 The name of the blob credential.  This corresponds to the "Storage Account Name" of your Azure Storage Account.

@BlobCredentialSecret NVARCHAR(255)
 Shhhh.  This is either the "primary access key" or "secondary access key" for your Azure Storage Account.

@ProfileName NVARCHAR(255)
 The email profile to use on the system.  Required because we are sending emails from a job step.  Must already exist

@OperatorName NVARCHAR(255)
 This is the name of an Operator on your server.  If the operator specified doesn't exist it will be created.

@OperatorEmail NVARCHAR(1024)
 This is the email address of the operator specified in @OperatorName.  This is only effective if the operator doesn't already exist.  It will not update the email of an existing operator.

@DRScriptEmail NVARCHAR(1024) = NULL
 Can be specified to send database restore scripts to a different email address.  If not supplied defaults to @OperatorEmail

@FullBackupDay VARCHAR(15) = NULL
 The week day that the full backup will be scheduled on.  We default to Saturdays further down if not supplied (or matched).  This can be supplied as 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat' or 'Sun'.

@LogBackupFreqMins INT = 15
 This is how often we're going to schedule transaction log backups.  Defaults to 15 minute interval if not supplied.

@BackupTime TIME(0) = NULL
 Time that Full and Differential backups will start e.g. '11:00:00 PM' or '23:00:00'

@URLBackupPath NVARCHAR(255) = NULL 
 URL path \\Servername\Sharename\SomeOptional\OtherPath\ which can be supplied instead of the @StorageContainerURL
 This param is optional but mutually exclusive with @StorageContainerURL

@intFullBackupExpireDays INT = NULL 
 Optional param to supply the number of days for backup expiry to Ola Hallengren jobs.  Only effective with @URLBackupPath as this is not supported for blob storage

@intDiffBackupExpireDays INT = NULL 
	Optional param to supply the number of days for backup expiry to Ola Hallengren jobs.  Only effective with @URLBackupPath as this is not supported for blob storage

@intLogBackupExpireDays INT  = NULL 
	Optional param to supply the number of days for backup expiry to Ola Hallengren jobs.  Only effective with @URLBackupPath as this is not supported for blob storage

Provided by SQLFairy http://sqlfairy.com.au

For more info please visit http://www.sqlfairy.com.au/2016/06/backup-to-the-cloud-super-dev-ops-restore-powers-dr-and-more/
*/

DECLARE @JobOwner NVARCHAR(255)
,		@FullBackupJobName sysname
,		@FullBackupDescription NVARCHAR(max)
,		@FullBackupJobCommand NVARCHAR(max) 
,		@DiffBackupJobName sysname
,		@DiffBackupDescription NVARCHAR(max)
,		@DiffBackupJobCommand NVARCHAR(max)
,		@LogBackupJobName sysname
,		@LogBackupDescription NVARCHAR(max)
,		@LogBackupJobCommand NVARCHAR(max) 
,		@MonthlyBackupJobName sysname
,		@MonthlyBackupDescription NVARCHAR(max)
,		@MonthlyBackupJobCommand NVARCHAR(max) 
,		@SendDREmailCommand NVARCHAR(MAX)

--Now we're supporting EITHER supplying a value for @BlobCredential OR @URLBackupPath.  Can't have both I'm afraid (or could you?... Hmmm)

--Check that we have one or the other and not both...
DECLARE 
	@BackupTypeParamErrorLevel INT = 0
,	@BackupTypeParamErrorMessage NVARCHAR(255) = NULL

IF @StorageContainerURL IS NULL AND @URLBackupPath IS NULL 
	BEGIN
		SET @BackupTypeParamErrorLevel = 2 --Error- Abort :(
		SET @BackupTypeParamErrorMessage = 'Must supply a value for either @URLBackupPath or @StorageContainerURL'
	END 

IF @StorageContainerURL IS NOT NULL AND @URLBackupPath IS NOT NULL 
	BEGIN
		SET @BackupTypeParamErrorLevel = 2 --Error- Abort :(
		SET @BackupTypeParamErrorMessage = 'Can only supply a value for either @URLBackupPath or @StorageContainerURL.  You supplied both?'
	END

IF @StorageContainerURL IS NOT NULL AND @URLBackupPath IS NULL AND (@BlobCredentialSecret IS NULL OR @BlobCredential IS NULL)
	BEGIN
		SET @BackupTypeParamErrorLevel = 2 --Error- Abort :(
		SET @BackupTypeParamErrorMessage = 'Need to specify a value for @BlobCredential and @BlobCredentialSecret'
	END

IF @StorageContainerURL IS NULL AND @URLBackupPath IS NOT NULL AND (@BlobCredentialSecret IS NOT NULL OR @BlobCredential IS NOT NULL)
	BEGIN
		SET @BackupTypeParamErrorLevel = 1 --Warning! Continue
		SET @BackupTypeParamErrorMessage = '@BlobCredential &/or @BlobCredentailSecret supplied but backing up to an URL'
	END

--CHECK FOR A VALID URL
IF @StorageContainerURL IS NULL AND @URLBackupPath IS NOT NULL AND @URLBackupPath NOT LIKE '\\[0-z]%\[0-z]%' 
	BEGIN
		SET @BackupTypeParamErrorLevel = 2 --Error- Abort :(
		SET @BackupTypeParamErrorMessage = '@URLBackupPath must be in the form \\Servername\Sharename or \\Servername\Sharename\Some\Path'
	END



IF @BackupTypeParamErrorLevel > 0
	BEGIN
    	IF @BackupTypeParamErrorLevel = 1 PRINT 'WARNING!' + CHAR(10) + @BackupTypeParamErrorMessage
		IF @BackupTypeParamErrorLevel = 2
			BEGIN
            	RAISERROR(@BackupTypeParamErrorMessage, 1, 1) 
				RETURN --Stop here
            END

    END

---Check that the maximum email attachment size is large enough.  Otherwise we might receive errors unexpectedly.
---Don't just blindly set it otherwise we could actually be decreasing a larger val set for some other reason :)
IF CAST(ISNULL((select paramvalue FROM msdb.dbo.sysmail_configuration WHERE paramname = 'MaxFileSize'), 0) as int) < 104857600
BEGIN
	PRINT 'Increasing sysmail MaxFileSize to 104857600' + CHAR(10)
	EXECUTE msdb.dbo.sysmail_configure_sp 'MaxFileSize', '104857600';
END
ELSE PRINT 'sysmail MaxFileSize seems large enough. Carry on...' + CHAR(10)



--Handle supplied backup day text and also determine other days for differential backups...
/*
freq_interval is one or more of the following:

 1 = Sunday
 2 = Monday
 4 = Tuesday
 8 = Wednesday
 16 = Thursday
 32 = Friday
 64 = Saturday

See the timeless elegant bitmaskyness of it all.  If you want to schedule for Monday, Wednesday, Friday it's 2+8+32

*/
DECLARE 
	@intFullBackupDay int 
,	@intDiffBackupDays int 
 
SET @intFullBackupDay =
	 CASE @FullBackupDay
 		WHEN 'Sunday' THEN 1
		WHEN 'Sun' THEN 1
		WHEN 'Su' THEN 1
 		WHEN 'Monday' THEN 2
		WHEN 'Mon' THEN 2
		WHEN 'Mo' THEN 2
		WHEN 'Tuesday' THEN  4
		WHEN 'Tue' THEN  4
		WHEN 'Tu' THEN  4
		WHEN 'Wednesday' THEN  8
		WHEN 'Wed' THEN  8
		WHEN 'We' THEN  8
		WHEN 'Thursday' THEN  16
		WHEN 'Thur' THEN  16
		WHEN 'Th' THEN  16
		WHEN 'Friday' THEN  32
		WHEN 'Fri' THEN  32
		WHEN 'Fr' THEN  32
		WHEN 'Saturday' THEN  64
		WHEN 'Sat' THEN  64
		WHEN 'Sa' THEN  64
	ELSE 64 --Default to running full backup on a Saturday if not identified.  I could probably do some more checking & throw back errors but meh.
	END

SET @intDiffBackupDays = 127 - @intFullBackupDay --The other days of the week on which we will run differential backups

---Let's convert our backup day to the text we want to use in descriptions
SET @FullBackupDay =
	CASE @intFullBackupDay
		WHEN 1 THEN 'Sunday'
		WHEN 2 THEN 'Monday'
		WHEN 4 THEN 'Tuesday'
		WHEN 8 THEN 'Wednesday'
		WHEN 16 THEN 'Thursday'
		WHEN 32 THEN 'Friday'
		WHEN 64 THEN 'Saturday'
	ELSE 'Saturday' --Don't see how but ...
	END

--Convert the supplied @BackupTime time value to an int
IF @BackupTime IS null SET @BackupTime = '1:00:00 AM' --Default to 1am if not supplied above
DECLARE @intBackupTime INT
SET @intBackupTime = CAST(REPLACE(CAST(@BackupTime AS VARCHAR(50)), ':', '') AS INT)	--There's probably a convert format for this.  Check this out...	

--Set the @DRScript email to default to @OperatorEmail address if not supplied.
IF @DRScriptEmail IS NULL SET @DRScriptEmail = @OperatorEmail


--Create or update credential for access to blob storage

DECLARE @credentialSQL NVARCHAR(max)

IF NOT EXISTS (SELECT [credentials].[credential_id] FROM [master].[sys].[credentials] WHERE [master].[sys].[credentials].[name] = @BlobCredential)
	SET @credentialSQL = 'CREATE CREDENTIAL ' + @BlobCredential + ' WITH IDENTITY = ''' + @BlobCredential + ''', SECRET = ''' + @BlobCredentialSecret +''''
ELSE 
	SET @credentialSQL = 'ALTER CREDENTIAL ' + @BlobCredential + ' WITH IDENTITY = ''' + @BlobCredential + ''', SECRET = ''' + @BlobCredentialSecret +''''

--PRINT @credentialSQL

EXEC [sys].[sp_executesql] @credentialSQL


SET @JobOwner = 'SA'

--Okay.  We're now going to handle either creating jobs for Azure blob storage or a UNC path...

IF @URLBackupPath IS NULL
	BEGIN --Backup to blob
		SET @FullBackupJobName = 'DatabaseBackup BLOB - FULL - ALL_DATABASES_WITH_EXCEPTIONS'
		SET @FullBackupJobCommand = N'sqlcmd -E -S $(ESCAPE_SQUOTE(SRVR)) -d ' + @OlaDBName + ' -Q "EXECUTE [dbo].[DatabaseBackup] @Databases = ''ALL_DATABASES, -%DROPDB%'', @URL = ''' + @StorageContainerURL +''',@Credential = ''' + @BlobCredential + ''', @BackupType = ''FULL'', @ChangeBackupType = ''Y'', @Verify = ''Y'', @Compress = ''Y'', @CheckSum = ''Y'', @LogToTable = ''Y''" -b' 
		SET @FullBackupDescription = N'Backup all user and system databases to blob storage with the exception of databases whose name is like ''%DROPDB%'''

		SET @DiffBackupJobName = 'DatabaseBackup BLOB - DIFF - ALL_DATABASES_WITH_EXCEPTIONS'
		SET @DiffBackupJobCommand =N'sqlcmd -E -S $(ESCAPE_SQUOTE(SRVR)) -d ' + @OlaDBName + ' -Q "EXECUTE [dbo].[DatabaseBackup] @Databases = ''ALL_DATABASES, -%DROPDB%'', @URL = ''' + @StorageContainerURL +''',@Credential = ''' + @BlobCredential + ''', @BackupType = ''DIFF'', @ChangeBackupType = ''Y'', @Verify = ''Y'', @Compress = ''Y'', @CheckSum = ''Y'', @LogToTable = ''Y''" -b' 
		SET @DiffBackupDescription =N'Perform differential backup of all user and system databases to blob storage with the exception of databases whose name is like ''%DROPDB%''' 

		SET @LogBackupJobName =N'DatabaseBackup BLOB - LOG - ALL_DATABASES_WITH_EXCEPTIONS'
		SET @LogBackupJobCommand  =N'sqlcmd -E -S $(ESCAPE_SQUOTE(SRVR)) -d ' + @OlaDBName + ' -Q "EXECUTE [dbo].[DatabaseBackup] @Databases = ''ALL_DATABASES, -%DROPDB%'', @URL = ''' + @StorageContainerURL +''',@Credential = ''' + @BlobCredential + ''', @BackupType = ''LOG'', @ChangeBackupType = ''Y'', @Verify = ''Y'', @Compress = ''Y'', @CheckSum = ''Y'', @LogToTable = ''Y''" -b' 
		SET @LogBackupDescription =N'Perform transaction log backup of all user and system databases to blob storage with the exception of databases whose name is like ''%DROPDB%'''

		SET @MonthlyBackupJobName=N'DatabaseBackup BLOB - FULL COPY ONLY - ALL_DATABASES_WITH_EXCEPTIONS'
		SET @MonthlyBackupJobCommand  =N'sqlcmd -E -S $(ESCAPE_SQUOTE(SRVR)) -d ' + @OlaDBName + ' -Q "EXECUTE [dbo].[DatabaseBackup] @Databases = ''ALL_DATABASES, -%DROPDB%'', @URL = ''' + @StorageContainerURL +''',@Credential = ''' + @BlobCredential + ''', @BackupType = ''FULL'', @CopyOnly = ''Y'', @Verify = ''Y'', @Compress = ''Y'', @CheckSum = ''Y'', @LogToTable = ''Y''" -b' 
		SET @MonthlyBackupDescription =N'Perform full copy only backup of all user and system databases to blob storage with the exception of databases whose name is like ''%DROPDB%'''
	END
ELSE 
	BEGIN --Backup to UNC path
    	SET @FullBackupJobName = 'DatabaseBackup UNC - FULL - ALL_DATABASES_WITH_EXCEPTIONS'
		SET @FullBackupJobCommand = N'sqlcmd -E -S $(ESCAPE_SQUOTE(SRVR)) -d ' + @OlaDBName + ' -Q "EXECUTE [dbo].[DatabaseBackup] @Databases = ''ALL_DATABASES, -%DROPDB%'', @Directory = ''' + @URLBackupPath + ''', @BackupType = ''FULL'', @ChangeBackupType = ''Y'', @Verify = ''Y'', @Compress = ''Y'', @CheckSum = ''Y'',' + CASE WHEN @intFullBackupExpireDays IS NOT NULL THEN ' @CleanupTime = ' + CAST(@intFullBackupExpireDays * 24 AS VARCHAR(10)) + ',' ELSE '' END + ' @LogToTable = ''Y''" -b' 
		SET @FullBackupDescription = N'Backup all user and system databases to a UNC path with the exception of databases whose name is like ''%DROPDB%'''

		SET @DiffBackupJobName = 'DatabaseBackup UNC - DIFF - ALL_DATABASES_WITH_EXCEPTIONS'
		SET @DiffBackupJobCommand =N'sqlcmd -E -S $(ESCAPE_SQUOTE(SRVR)) -d ' + @OlaDBName + ' -Q "EXECUTE [dbo].[DatabaseBackup] @Databases = ''ALL_DATABASES, -%DROPDB%'', @Directory = ''' + @URLBackupPath +''', @BackupType = ''DIFF'', @ChangeBackupType = ''Y'', @Verify = ''Y'', @Compress = ''Y'', @CheckSum = ''Y'',' + CASE WHEN @intDiffBackupExpireDays IS NOT NULL THEN ' @CleanupTime = ' + CAST(@intDiffBackupExpireDays * 24 AS VARCHAR(10)) + ',' ELSE '' END + ' @LogToTable = ''Y''" -b' 
		SET @DiffBackupDescription =N'Perform differential backup of all user and system databases to a UNC path with the exception of databases whose name is like ''%DROPDB%''' 

		SET @LogBackupJobName =N'DatabaseBackup UNC - LOG - ALL_DATABASES_WITH_EXCEPTIONS'
		SET @LogBackupJobCommand  =N'sqlcmd -E -S $(ESCAPE_SQUOTE(SRVR)) -d ' + @OlaDBName + ' -Q "EXECUTE [dbo].[DatabaseBackup] @Databases = ''ALL_DATABASES, -%DROPDB%'', @Directory = ''' + @URLBackupPath +''', @BackupType = ''LOG'', @ChangeBackupType = ''Y'', @Verify = ''Y'', @Compress = ''Y'', @CheckSum = ''Y'',' + CASE WHEN @intLogBackupExpireDays IS NOT NULL THEN ' @CleanupTime = ' + CAST(@intLogBackupExpireDays * 24 AS VARCHAR(10)) + ',' ELSE '' END + ' @LogToTable = ''Y''" -b' 
		SET @LogBackupDescription =N'Perform transaction log backup of all user and system databases to a UNC path with the exception of databases whose name is like ''%DROPDB%'''

		SET @MonthlyBackupJobName=N'DatabaseBackup UNC - FULL COPY ONLY - ALL_DATABASES_WITH_EXCEPTIONS'
		SET @MonthlyBackupJobCommand  =N'sqlcmd -E -S $(ESCAPE_SQUOTE(SRVR)) -d ' + @OlaDBName + ' -Q "EXECUTE [dbo].[DatabaseBackup] @Databases = ''ALL_DATABASES, -%DROPDB%'', @Directory = ''' + @URLBackupPath +''', @BackupType = ''FULL'', @CopyOnly = ''Y'', @Verify = ''Y'', @Compress = ''Y'', @CheckSum = ''Y'', @LogToTable = ''Y''" -b' 
		SET @MonthlyBackupDescription =N'Perform full copy only backup of all user and system databases to a UNC path with the exception of databases whose name is like ''%DROPDB%'''
    END


--We're going to generate and send an email containing scripts to restore all databases on this server ftw!
SET @SendDREmailCommand = 'EXEC master.dbo.usp_EmailRestoreScript ' + CASE WHEN @BlobCredential IS NOT NULL THEN '@BlobCredential = ''' + @BlobCredential + ''', ' ELSE '' END + '@profileName = ''' + @ProfileName + ''', @recipientAddress = ''' + @DRScriptEmail + ''''

--EXECUTE msdb.dbo.sp_add_jobstep @job_name = @FullBackupJobName, @step_name=N'Send DR restore script email', @step_id=2, @subsystem=N'TSQL', @command=@SendDREmailCommand, @database_name=N'master'

IF EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = @FullBackupJobName)
	EXEC msdb.dbo.sp_delete_job @job_name = @FullBackupJobName

 IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = @FullBackupJobName)
  BEGIN
    EXECUTE msdb.dbo.sp_add_job @job_name = @FullBackupJobName, @description = @FullBackupDescription, @owner_login_name = @JobOwner
    EXECUTE msdb.dbo.sp_add_jobstep @job_name = @FullBackupJobName, @step_name = @FullBackupJobName, @step_id=1, @subsystem = 'CMDEXEC', @command = @FullBackupJobCommand, @on_success_action=3
	EXECUTE msdb.dbo.sp_add_jobstep @job_name = @FullBackupJobName, @step_name=N'Send DR restore script email', @step_id=2, @subsystem=N'TSQL', @command=@SendDREmailCommand, @database_name=N'master'
    EXECUTE msdb.dbo.sp_add_jobserver @job_name = @FullBackupJobName
  END

IF EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = @DiffBackupJobName)
	EXEC msdb.dbo.sp_delete_job @job_name = @DiffBackupJobName

 IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = @DiffBackupJobName)
  BEGIN
    EXECUTE msdb.dbo.sp_add_job @job_name = @DiffBackupJobName, @description = @DiffBackupDescription, @owner_login_name = @JobOwner
    EXECUTE msdb.dbo.sp_add_jobstep @job_name = @DiffBackupJobName, @step_name = @DiffBackupJobName, @step_id=1, @subsystem = 'CMDEXEC', @command = @DiffBackupJobCommand, @on_success_action=3
    EXECUTE msdb.dbo.sp_add_jobstep @job_name = @DiffBackupJobName, @step_name=N'Send DR restore script email', @step_id=2, @subsystem=N'TSQL', @command=@SendDREmailCommand, @database_name=N'master'
	EXECUTE msdb.dbo.sp_add_jobserver @job_name = @DiffBackupJobName
  END

  IF EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = @LogBackupJobName)
	EXEC msdb.dbo.sp_delete_job @job_name = @LogBackupJobName

   IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = @LogBackupJobName)
  BEGIN
    EXECUTE msdb.dbo.sp_add_job @job_name = @LogBackupJobName, @description = @LogBackupDescription, @owner_login_name = @JobOwner
    EXECUTE msdb.dbo.sp_add_jobstep @job_name = @LogBackupJobName, @step_name = @LogBackupJobName, @step_id=1, @subsystem = 'CMDEXEC', @command = @LogBackupJobCommand, @on_success_action=3
	EXECUTE msdb.dbo.sp_add_jobstep @job_name = @LogBackupJobName, @step_name=N'Send DR restore script email', @step_id=2, @subsystem=N'TSQL', @command=@SendDREmailCommand, @database_name=N'master'
    EXECUTE msdb.dbo.sp_add_jobserver @job_name = @LogBackupJobName
  END

  IF EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = @MonthlyBackupJobName)
	EXEC msdb.dbo.sp_delete_job @job_name = @MonthlyBackupJobName

   IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = @MonthlyBackupJobName)
  BEGIN
    EXECUTE msdb.dbo.sp_add_job @job_name = @MonthlyBackupJobName, @description = @MonthlyBackupDescription, @owner_login_name = @JobOwner
    EXECUTE msdb.dbo.sp_add_jobstep @job_name = @MonthlyBackupJobName, @step_name = @MonthlyBackupJobName, @subsystem = 'CMDEXEC', @command = @MonthlyBackupJobCommand
    EXECUTE msdb.dbo.sp_add_jobserver @job_name = @MonthlyBackupJobName
  END

--Now let's add some schedules...

--Some name variables---
DECLARE 
	@LogScheduleName VARCHAR(255)
,	@DiffScheduleName VARCHAR(255)
,	@FullScheduleName VARCHAR(255)

SET @LogScheduleName = 'Every ' + CAST(@LogBackupFreqMins AS VARCHAR(5)) + ' minutes'
SET @DiffScheduleName = 'Daily at ' + CAST(@BackupTime AS VARCHAR(50)) + ' except ' + @FullBackupDay
SET @FullScheduleName = @FullBackupDay + ' at ' + CAST(@BackupTime AS VARCHAR(50))

--Log backups 
EXEC msdb.dbo.sp_add_jobschedule 
		@job_name = @LogBackupJobName
	,	@name = @LogScheduleName
	,	@enabled = 1
	,	@freq_type = 4
	,	@freq_interval = 1
	,	@freq_subday_type = 4
	,	@freq_subday_interval = @LogBackupFreqMins
	,	@freq_relative_interval = 0
	,	@freq_recurrence_factor = 1
	,	@active_start_date = 20141112
	,	@active_end_date = 99991231
	,	@active_start_time = 0
	,	@active_end_time = 235959

--Differential Backup
DECLARE @schedule_id int
EXEC msdb.dbo.sp_add_jobschedule @job_name = @DiffBackupJobName
	,	@name = @DiffScheduleName
	,	@enabled = 1
	,	@freq_type = 8
	,	@freq_interval = @intDiffBackupDays
	,	@freq_subday_type = 1
	,	@freq_subday_interval = 0
	,	@freq_relative_interval = 0
	,	@freq_recurrence_factor = 1
	,	@active_start_date = 20141112
	,	@active_end_date = 99991231
	,	@active_start_time = @intBackupTime
	,	@active_end_time = 235959

--Full Backup
EXEC msdb.dbo.sp_add_jobschedule @job_name = @FullBackupJobName
		, @name= @FullScheduleName
		,	@enabled=1
		,	@freq_type=8
		,	@freq_interval= @intFullBackupDay
		,	@freq_subday_type=1
		,	@freq_subday_interval=0
		,	@freq_relative_interval=0
		,	@freq_recurrence_factor=1
		,	@active_start_date=20141112
		,	@active_end_date=99991231
		,	@active_start_time= @intBackupTime
		,	@active_end_time=235959

EXEC msdb.dbo.sp_add_jobschedule @job_name = @MonthlyBackupJobName,
		@name=N'First Sunday every month 9:00 pm', 
		@enabled=1, 
		@freq_type=32, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=1, 
		@freq_recurrence_factor=1, 
		@active_start_date=20141112, 
		@active_end_date=99991231, 
		@active_start_time=190000, 
		@active_end_time=235959

--Now to configure notifications

--Add a DBA operator if it doesn't already exist

IF NOT EXISTS(SELECT * FROM [msdb].[dbo].[sysoperators] WHERE name = @OperatorName)
	EXEC msdb.dbo.sp_add_operator @name = @OperatorName, 
			@enabled=1, 
			@pager_days=0, 
			@email_address= @OperatorEmail


--Now assign the operator to the jobs
EXEC msdb.dbo.sp_update_job @job_name = @FullBackupJobName, 
		@notify_level_email=2, --On fail?
		@notify_level_netsend=2, 
		@notify_level_page=2, 
		@notify_email_operator_name= @OperatorName

EXEC msdb.dbo.sp_update_job @job_name = @DiffBackupJobName,
		@notify_level_email=2, 
		--@notify_level_netsend=2, 
		--@notify_level_page=2, 
		@notify_email_operator_name= @OperatorName

EXEC msdb.dbo.sp_update_job @job_name = @LogBackupJobName, 
		@notify_level_email=2, 
		--@notify_level_netsend=2, 
		--@notify_level_page=2, 
		@notify_email_operator_name= @OperatorName

EXEC msdb.dbo.sp_update_job @job_name = @MonthlyBackupJobName, 
		@notify_level_email=2, 
		--@notify_level_netsend=2, 
		--@notify_level_page=2, 
		@notify_email_operator_name= @OperatorName

	
END 

GO 

----------------------------------------------------------------------------------------------
 
 --Create Maintenance jobs

--The following stored proc is designed to create unscheduled & disabled jobs for maintenance of Production databases
--It is assumed that we want to manually schedule the maintenance of our production databases as performing index 
--	rebuilds on all of our prod databases at one time has not been a terribly clever move in the past. :)
--

--Also creates a job to update statistics on all user databases every night at 10pm (aest)


USE [master] 
GO 
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'sp_ScheduleMaintenance') 
EXEC ('CREATE PROC dbo.sp_ScheduleMaintenance AS SELECT ''stub version, to be replaced''') 
GO 

ALTER PROC [dbo].[sp_ScheduleMaintenance]
(
	@OlaDBName sysname --The database which contains the OLA Hallengren maintenance solution
,	@OperatorName NVARCHAR(255) --Operator which will receive alerts for the jobs
,	@OperatorEmail NVARCHAR(1024) --Email address of the operator above.  NB: Only effective if specified operator does not already exist.
)
AS
BEGIN

/*
Copyright (c) 2016 SQL Fairy http://sqlfairy.com.au

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

dbo.sp_ScheduleMaintenance
Generates separate indexOptimise and DatabaseIntegrityCheck jobs which call on Ola Hallengren’s IndexOptimize and DatabaseIntegrityCheck stored procedures respectively 
for each of your databases.

Provided by SQLFairy http://sqlfairy.com.au

For more info please visit http://www.sqlfairy.com.au/2016/06/backup-to-the-cloud-super-dev-ops-restore-powers-dr-and-more/
*/

--Let's get a list of the production databases we have
DECLARE @prodDatabases AS TABLE (	dbname sysname
								,	Id int
								,	blnProcessed BIT NULL)
								
INSERT INTO @prodDatabases
        ([dbname], Id)
SELECT name, [database_id] FROM sys.[databases]
WHERE name NOT IN ('master','tempdb','model','msdb')
--AND name LIKE '%Prod%' --If production databases are named in a particular way then you may wish to use this line

DECLARE @JobOwner NVARCHAR(255)

,		@IndexRebuildJobName sysname
,		@IndexRebuildDescription NVARCHAR(max)
,		@CheckDBJobName sysname
,		@CheckDBDescription NVARCHAR(max)


SET @JobOwner = 'SA'

SET @IndexRebuildJobName = 'IndexOptimise'
SET @IndexRebuildDescription = 'Performs index maintenance using Ola Hellengren maintenance solution http://ola.hallengren.com/'

SET @CheckDBJobName = 'DatabaseIntegrityCheck'
SET @CheckDBDescription = 'Performs database integrity checks using Ola Hellengren maintenance solution http://ola.hallengren.com/'

--Add a DBA operator if it doesn't already exist

IF NOT EXISTS(SELECT * FROM [msdb].[dbo].[sysoperators] WHERE name = @OperatorName)
	EXEC msdb.dbo.sp_add_operator @name=@OperatorName, 
			@enabled=1, 
			@pager_days=0, 
			@email_address= @OperatorEmail


--Now we'll rbar over our list of databases and configure new jobs if they don't exist.  These will be created as disabled with no schedule.

--For use inside our loops below
DECLARE @dbName sysname 
DECLARE @txtJobName sysname 
DECLARE @txtStepCommand NVARCHAR(max)
--

WHILE (SELECT COUNT(Id) FROM @prodDatabases WHERE blnProcessed IS NULL OR blnProcessed = 0) > 0
	BEGIN
		SET @dbName = (SELECT TOP 1 dbname FROM @prodDatabases WHERE blnProcessed IS NULL OR blnProcessed = 0 ORDER BY Id)
		SET @txtJobName = @IndexRebuildJobName + ' - ' + @dbName
		SET @txtStepCommand = 'sqlcmd -E -S $(ESCAPE_SQUOTE(SRVR)) -d ' +@OlaDBName +' -Q "EXECUTE [dbo].[IndexOptimize] @Databases = ''' + @dbName + ''',@UpdateStatistics=''ALL'',@OnlyModifiedStatistics=''Y'', @LogToTable = ''Y'', @FragmentationLevel1 = 5 ,@FragmentationLevel2 =40, @MaxDOP = 1, @SortInTempdb = ''Y''" -b'
		
		IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = @txtJobName)
		BEGIN
			EXECUTE msdb.dbo.sp_add_job @job_name = @txtJobName, @description = @IndexRebuildDescription, @owner_login_name = @JobOwner, @enabled = 0, @notify_level_email=2, @notify_email_operator_name= @OperatorName
			EXECUTE msdb.dbo.sp_add_jobstep @job_name = @txtJobName, @step_name = @IndexRebuildJobName, @subsystem = 'CMDEXEC', @command = @txtStepCommand
			EXECUTE msdb.dbo.sp_add_jobserver @job_name = @txtJobName
		END 
		UPDATE @prodDatabases SET [blnProcessed] = 1 WHERE [dbname] = @dbName
	END

---finished creating index rebuild jobs.  Now clear the processed flag and repeat for our integrity check jobs

UPDATE @prodDatabases SET [blnProcessed] = NULL

--Now do moar rbar to create our integrity check jobs

WHILE (SELECT COUNT(Id) FROM @prodDatabases WHERE blnProcessed IS NULL OR blnProcessed = 0) > 0
	BEGIN
		SET @dbName = (SELECT TOP 1 dbname FROM @prodDatabases WHERE blnProcessed IS NULL OR blnProcessed = 0 ORDER BY Id)
		SET @txtJobName = @CheckDBJobName + ' - ' + @dbName
		SET @txtStepCommand = 'sqlcmd -E -S $(ESCAPE_SQUOTE(SRVR)) -d ' +@OlaDBName +' -Q "EXECUTE [dbo].[DatabaseIntegrityCheck] @Databases = ''' + @dbName + ''',@LogToTable = ''Y''" -b'
		
		IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = @txtJobName)
		BEGIN
			EXECUTE msdb.dbo.sp_add_job @job_name = @txtJobName, @description = @CheckDBDescription, @owner_login_name = @JobOwner, @enabled = 0, @notify_level_email=2, @notify_email_operator_name= @OperatorName
			EXECUTE msdb.dbo.sp_add_jobstep @job_name = @txtJobName, @step_name = @CheckDBJobName, @subsystem = 'CMDEXEC', @command = @txtStepCommand
			EXECUTE msdb.dbo.sp_add_jobserver @job_name = @txtJobName
		END 
		UPDATE @prodDatabases SET [blnProcessed] = 1 WHERE [dbname] = @dbName
	END

--Now let's configure a job to rebuild statistics every night for all user databases

DECLARE @txtUpdateStatisticsJobName NVARCHAR(255)
DECLARE @txtUpdateStatisticsDescription NVARCHAR(max)
DECLARE @txtUpdateStatisticsCommand NVARCHAR(max)

SET @txtUpdateStatisticsJobName = 'Update Statistics - USER_DATABASES'
SET @txtUpdateStatisticsDescription = 'Rebuild statistics on all User databases using Ola Hellengren maintenance solution http://ola.hallengren.com/'
SET @txtUpdateStatisticsCommand = 'sqlcmd -E -S $(ESCAPE_SQUOTE(SRVR)) -d ' +@OlaDBName +' -Q "EXECUTE [dbo].[IndexOptimize] @Databases = ''USER_DATABASES'', @FragmentationLow = NULL, @FragmentationMedium = NULL, @FragmentationHigh = NULL, @UpdateStatistics=''ALL'',@OnlyModifiedStatistics=''Y'', @LogToTable = ''Y''" -b'
    
--sqlcmd -E -S $(ESCAPE_SQUOTE(SRVR)) -d DBA -Q "EXECUTE [dbo].[IndexOptimize] @Databases = 'USER_DATABASES', @FragmentationLow = NULL, @FragmentationMedium = NULL, @FragmentationHigh = NULL, @UpdateStatistics = 'ALL', @OnlyModifiedStatistics = 'Y', @LogToTable = 'Y'" -b	
	                                      
IF EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = @txtUpdateStatisticsJobName)
	EXEC msdb.dbo.sp_delete_job @job_name = @txtUpdateStatisticsJobName

 IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = @txtUpdateStatisticsJobName)
  BEGIN
    EXECUTE msdb.dbo.sp_add_job @job_name = @txtUpdateStatisticsJobName, @description = @txtUpdateStatisticsDescription, @owner_login_name = @JobOwner, @notify_level_email=2, @notify_email_operator_name= @OperatorName
    EXECUTE msdb.dbo.sp_add_jobstep @job_name = @txtUpdateStatisticsJobName, @step_name = @txtUpdateStatisticsJobName, @subsystem = 'CMDEXEC', @command = @txtUpdateStatisticsCommand
    EXECUTE msdb.dbo.sp_add_jobserver @job_name = @txtUpdateStatisticsJobName
  END

   EXECUTE msdb.dbo.sp_add_jobschedule @job_name = @txtUpdateStatisticsJobName, 
		@name=N'Daily 10pm', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20141206, 
		@active_end_date=99991231, 
		@active_start_time=220000, 
		@active_end_time=235959


END 

GO
-----------------------------------------------------


--usp_RestoreDB
--Stored procedure to automagically restore a backup of a DB.
--Uses RestoreGenerator to generate SQL Backup scripts
--Relies on database backups having been made to Azure Blob storage rather than attempting to connect to remote drive shares


USE [master] 
GO 
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'usp_RestoreDB') 
EXEC ('CREATE PROC dbo.usp_RestoreDB AS SELECT ''stub version, to be replaced''') 
GO 

ALTER PROC [dbo].[usp_RestoreDB]
(
	@SourceServer NVARCHAR(255)
,	@TargetServer NVARCHAR(255)
,	@SourceDB sysname
,	@TargetDB sysname
,	@TargetServerDataPath NVARCHAR(255)
,	@TargetServerLogPath	NVARCHAR(255)
,	@BlobCredential NVARCHAR(255) = NULL --Now optional so that we can support restore from UNC paths without this.
,	@BlobCredentialSecret NVARCHAR(255) = NULL	
,	@RecoverDB BIT = 1
,	@StopAt DATETIME = NULL
)
AS
BEGIN    

/*
Copyright (c) 2016 SQL Fairy http://sqlfairy.com.au

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

dbo.usp_RestoreDB – Allows for automagic restore of databases from servers which are being backed up to Azure.

Params...

@SourceServer NVARCHAR(255) [mandatory]
 Name of the SQL Server where the database was backed up

@TargetServer NVARCHAR(255) [mandatory]
 Name of the SQL Server where the database will be restored

@SourceDB sysname [mandatory]
 Name of the Database to be copied

@TargetDB sysname [mandatory]
 Name that the Database will be restored as

@TargetServerDataPath NVARCHAR(255) [mandatory]
 Disk path on the Target Server where the data file (mdf) will be restored

@TargetServerLogPath NVARCHAR(255) [mandatory]
 Disk path on the Target Server where the log file (ldf) will be restored

@BlobCredential NVARCHAR(255) [optional]
 Name of the Credential used on the Target Server to access the Blob Storage Account.  This is normally set to match the Azure Storage Account Name.

 NB: If the credential doesn’t already exist and a @BlobCredentialSecret has been supplied then a new credential will be created.  If the credential already exists then it will not be overwritten as this may interfere with other processes such as backup which rely on the existing credential.
 NB(2): This parameter is now optional but only if the database to be restored exists on a \\UNC path.

@BlobCredentialSecret NVARCHAR(255) [optional]
 This is the Storage Account Access Key (Primary or Secondary).  This value is only required if there is not already a credential containing this key on the server.  If supplied in conjunction with the @BlobCredential parameter and there is not already a matching credential on the server one will be created.  Existing credentials will not be overwritten.

@RecoverDB BIT [optional]
 Defaults to true.  This parameter dictates whether the database will be recovered or left in a restoring state.

@StopAt DATETIME [optional]
 Defaults to the current time.  This parameter can be used to restore to a particular point in time.  The time specified should of course be covered by existing backups.  The generated restore script will select backups to restore which correspond to the desired time and will roll forward only until the specified time.

Provided by SQLFairy http://sqlfairy.com.au

For more info please visit http://www.sqlfairy.com.au/2016/06/backup-to-the-cloud-super-dev-ops-restore-powers-dr-and-more/
*/

SET NOCOUNT ON

DECLARE @debug int = 55

--Let's check that we aren't trying to restore the same DB to the same server.  This is an arbitrary restriction to stop accidents.
IF @SourceServer = @TargetServer AND @SourceDB = @TargetDB RAISERROR(N'ERROR: Source and destination Databases are the same.  This is not supported by this utility', 1, 1) 	
IF @SourceServer = @TargetServer AND @SourceDB = @TargetDB RETURN

--First we need to establish whether the servers being referenced are local or remote and establish linked servers if required
--Also set some variables to indicate local or remote
DECLARE @SourceServerIsLocal BIT
DECLARE @TargetServerIsLocal BIT

DECLARE @serverNameLike NVARCHAR(255)
SET @serverNameLike = '%' + @@SERVERNAME +'%'

--Checking for the local servername inside the supplied servername strings
SELECT @SourceServerIsLocal = CASE WHEN @SourceServer LIKE @serverNameLike THEN 1 ELSE 0 END 
SELECT @TargetServerIsLocal = CASE WHEN @TargetServer LIKE @serverNameLike THEN 1 ELSE 0 END 

IF @debug > 0 PRINT 'SourceServerIsLocal ' + CAST(@SourceServerIsLocal AS NVARCHAR(1))
IF @debug  > 0 PRINT 'TargetServerIsLocal ' + CAST(@TargetServerIsLocal AS NVARCHAR(1))

--Let's upper case the servernames for S&G.
SET @SourceServer = UPPER(@SourceServer)
SET @TargetServer = UPPER(@TargetServer)

--ToDo: Consider moving to another stored proc
IF @SourceServerIsLocal = 0 AND NOT EXISTS (SELECT * FROM sys.[servers] WHERE name = @SourceServer)
	BEGIN
		EXEC master.dbo.sp_addlinkedserver @server = @SourceServer, @srvproduct=N'SQL Server'
		EXEC master.dbo.sp_serveroption @server=@SourceServer, @optname=N'collation compatible', @optvalue=N'false'
		EXEC master.dbo.sp_serveroption @server=@SourceServer, @optname=N'data access', @optvalue=N'true'
		EXEC master.dbo.sp_serveroption @server=@SourceServer, @optname=N'dist', @optvalue=N'false'
		EXEC master.dbo.sp_serveroption @server=@SourceServer, @optname=N'pub', @optvalue=N'false'
		EXEC master.dbo.sp_serveroption @server=@SourceServer, @optname=N'rpc', @optvalue=N'true'
		EXEC master.dbo.sp_serveroption @server=@SourceServer, @optname=N'rpc out', @optvalue=N'true'
		EXEC master.dbo.sp_serveroption @server=@SourceServer, @optname=N'sub', @optvalue=N'false'
		EXEC master.dbo.sp_serveroption @server=@SourceServer, @optname=N'connect timeout', @optvalue=N'0'
		EXEC master.dbo.sp_serveroption @server=@SourceServer, @optname=N'collation name', @optvalue=null
		EXEC master.dbo.sp_serveroption @server=@SourceServer, @optname=N'lazy schema validation', @optvalue=N'false'
		EXEC master.dbo.sp_serveroption @server=@SourceServer, @optname=N'query timeout', @optvalue=N'0'
		EXEC master.dbo.sp_serveroption @server=@SourceServer, @optname=N'use remote collation', @optvalue=N'true'
		EXEC master.dbo.sp_serveroption @server=@SourceServer, @optname=N'remote proc transaction promotion', @optvalue=N'false'
		--Add a login
		EXEC master.dbo.sp_addlinkedsrvlogin @rmtsrvname = @SourceServer, @locallogin = NULL , @useself = N'True'
	END

IF @TargetServerIsLocal= 0 AND NOT EXISTS (SELECT * FROM sys.[servers] WHERE name = @TargetServer)
	BEGIN
		EXEC master.dbo.sp_addlinkedserver @server = @TargetServer, @srvproduct=N'SQL Server'
		EXEC master.dbo.sp_serveroption @server=@TargetServer, @optname=N'collation compatible', @optvalue=N'false'
		EXEC master.dbo.sp_serveroption @server=@TargetServer, @optname=N'data access', @optvalue=N'true'
		EXEC master.dbo.sp_serveroption @server=@TargetServer, @optname=N'dist', @optvalue=N'false'
		EXEC master.dbo.sp_serveroption @server=@TargetServer, @optname=N'pub', @optvalue=N'false'
		EXEC master.dbo.sp_serveroption @server=@TargetServer, @optname=N'rpc', @optvalue=N'true'
		EXEC master.dbo.sp_serveroption @server=@TargetServer, @optname=N'rpc out', @optvalue=N'true'
		EXEC master.dbo.sp_serveroption @server=@TargetServer, @optname=N'sub', @optvalue=N'false'
		EXEC master.dbo.sp_serveroption @server=@TargetServer, @optname=N'connect timeout', @optvalue=N'0'
		EXEC master.dbo.sp_serveroption @server=@TargetServer, @optname=N'collation name', @optvalue=null
		EXEC master.dbo.sp_serveroption @server=@TargetServer, @optname=N'lazy schema validation', @optvalue=N'false'
		EXEC master.dbo.sp_serveroption @server=@TargetServer, @optname=N'query timeout', @optvalue=N'0'
		EXEC master.dbo.sp_serveroption @server=@TargetServer, @optname=N'use remote collation', @optvalue=N'true'
		EXEC master.dbo.sp_serveroption @server=@TargetServer, @optname=N'remote proc transaction promotion', @optvalue=N'false'
		--Add a login
		EXEC master.dbo.sp_addlinkedsrvlogin @rmtsrvname =@TargetServer, @locallogin = NULL , @useself = N'True'
	END

--Now let's validate that we don't already have a mirror of this database on the primary or secondary servers

DECLARE @SQL NVARCHAR(max)
SET @SQL = ''

--Now we can check to see if the database does exist on the Primary server and in a good state
SET @SQL = ''

--SQL to check that we have a backup history of some sort on the source server
SET @sql = '
SELECT top 1  @intBackupSetId =
		[backupset].[backup_set_id]
FROM    ' +
	CASE WHEN @SourceServerIsLocal = 0 THEN '[' + @SourceServer + '].' ELSE '' END +
		'[msdb].[dbo].[backupset] [backupset]
		 WHERE [backupset].[database_name] = ''' + @SourceDB + '''
'

DECLARE @intBackupSetId INT
 EXEC sp_executesql @SQL,N'@intBackupSetId int out', @intBackupSetId OUT
--IF @debug  > 0 PRINT @intDatabaseId
IF @intBackupSetId IS NULL RAISERROR(N'ERROR: Backup history for database not located on source server', 1, 1) 	
IF @intBackupSetId IS NULL RETURN --STOP RIGHT NOW as we can't find any backup history for the database

IF @debug > 1 AND @intBackupSetId IS NOT NULL PRINT 'Restore history for database ' + @SourceDB + ' successfully located on ' + @SourceServer

--SQL to check we don't already have this database on the target
--NB: We could support this with an overwrite option however it's an unusual use case for us and safer not to at the moment.
SET @sql = '
SELECT top 1  @intDatabaseId =
		[databases].[database_id]
FROM    ' +
	CASE WHEN @TargetServerIsLocal = 0 THEN '[' + @TargetServer + '].' ELSE '' END +
		'[master].[sys].[databases] [databases]
		 WHERE [databases].[name] = ''' + @TargetDB + ''''

DECLARE @intDatabaseId INT
 EXEC sp_executesql @SQL,N'@intDatabaseId int out', @intDatabaseId OUT
--IF @debug  > 0PRINT @intDatabaseId
IF @intDatabaseId IS NOT NULL RAISERROR(N'ERROR: A database with a matching name has been located on the target server.  This will need to be resolved manually', 1, 1) 	
IF @debug  > 0 AND @intDatabaseId IS NOT NULL PRINT	'WARNING: Database ' + @TargetDB + ' located on target server with id ' + CAST(@intdatabaseId AS nchar(10))
IF @intDatabaseId IS NOT NULL RETURN --STOP RIGHT NOW IF THE DATABASE IS FOUND ON THE SECONDARY

-------------------------------------------------------------------------------

--Okay... Let's restore the database FTW!!!1!

--Need to generate a restore script from the primary

DECLARE @runRestoreGeneSQL NVARCHAR(max)
SET @runRestoreGeneSQL = 
	CASE WHEN @SourceServerIsLocal = 0 THEN '[' + @SourceServer + '].' ELSE '' END +
	'[master].[dbo].[sp_RestoreGene]
		@Database=''' + @SourceDB +'''
	,	@TargetDatabase=''' + @TargetDB +'''
	,	@WithMoveDataFiles = ''' + @TargetServerDataPath + '''
	,	@WithMoveLogFile = ''' + @TargetServerLogPath + '''
	,	@Log_Reference = ''Restore databaase ' + @SourceDB + ' from ' + @SourceServer + ' to ' + @TargetServer + ' as ' + @TargetDB + '.''' 
	+ case when @BlobCredential is not null then ',	@BlobCredential = ''' + @BlobCredential + '''' + CHAR(10) ELSE '' END + --Don't include @blobCredential if it hasn't been supplied
	',	@WithRecovery = ' + CAST(@RecoverDB AS nvarchar(1)) +'
	,	@WithReplace = 0 ' +
CASE WHEN @StopAt IS NOT NULL THEN '	,	@StopAt = ''' + CAST(@StopAt AS NVARCHAR(30)) + '''' ELSE '' END --Only include the @StopAt parm if specified
IF @debug > 1 PRINT CHAR(10) + @runRestoreGeneSQL + CHAR(10)


DECLARE @RestoreRows AS TABLE
(	TSQL NVARCHAR(MAX)
,	BackupDate DATETIME
,	BackupDevice NVARCHAR(255)
,	Last_LSN NUMERIC(32) NULL
,	Databaase_Name sysname
,	SortSequence INT
,	blnProcessed BIT DEFAULT 0
)

INSERT INTO @RestoreRows
        ([TSQL]
        ,[BackupDate]
        ,[BackupDevice]
        ,[Last_LSN]
        ,[Databaase_Name]
        ,[SortSequence]
        )

EXEC sp_executesql @runRestoreGeneSQL

IF @debug > 2 SELECT * FROM @RestoreRows

----------We're supporting restore from a central UNC path now so check to see whether any http paths are included in the restore set...
DECLARE @RestoreIncludesBlobFiles BIT
IF (SELECT COUNT(*) FROM @RestoreRows WHERE BackupDevice LIKE 'http%') > 0 SET @RestoreIncludesBlobFiles = 1

--Now if we do require a blob credential we will check for it here
IF @RestoreIncludesBlobFiles = 1
	BEGIN
    	--Now that we've made it optional to supply an @BlobCredential so that we can handle restore from UNC \\Server\Share backups we need to check here if one has actually been supplied...
		IF @BlobCredential IS NULL 
			BEGIN
				RAISERROR(N'ERROR: Backup contains files in Azure blob storage and no Credential was supplied :(', 1, 1) 	
				RETURN --Can't continue past this point :(    	
            END
		
		--Now let's check that we have the specified credential on the target server so that the database can be restored
		--If we don't have it and we have been supplied with a secret then we can create the credential.
		--We're not going to update existing credentials with supplied parameters as that could cause backups to fail
		--	which means that if you supply some dud credentials the first time around it could cause some pain.  --Maybe reconsider this 'feature' later

		--We only need to check this on the destination server
		SET @SQL = ''

		--SQL to check if we already have a credential matching the supplied name
		SET @sql = '
		SELECT top 1  @intCredentialId =
				[credentials].[credential_id]
		FROM    ' +
			CASE WHEN @TargetServerIsLocal = 0 THEN '[' + @TargetServer + '].' ELSE '' END +
				'[master].[sys].[credentials] [credentials]
				 WHERE [credentials].[name] = ''' + @BlobCredential + ''''

		DECLARE @intCredentialId INT
		 EXEC sp_executesql @SQL,N'@intCredentialId int out', @intCredentialId OUT

		IF @intCredentialId IS NULL AND @BlobCredentialSecret IS NULL RAISERROR(N'ERROR: BlobCredential not located on secondary server and no BlobCredentialSecret Supplied', 1, 1) 	
		IF @debug  > 0 AND @intDatabaseId IS NOT NULL PRINT	'Database credential ' + @BlobCredential + ' located on secondary server'
		IF @intCredentialId IS NULL AND @BlobCredentialSecret IS NULL RETURN --STOP RIGHT NOW.  We need a credential secret

		IF @intCredentialId IS NULL AND @BlobCredentialSecret IS NOT NULL
		BEGIN
		--Geez this is gettig hairy(tm) !!!1!
		--NB: If you're reviewing this code the obtuse string construction and nested exec sp_executesql is required because exec will not accept a variable for the server name :(
			IF @TargetServerIsLocal = 1
			BEGIN
				SET @SQL= 'CREATE CREDENTIAL [' + @BlobCredential +'] WITH IDENTITY = N''' + @BlobCredential + ''', SECRET = N''' + @BlobCredentialSecret + ''''
				IF @debug > 1 PRINT @SQL
				EXEC sp_executesql @SQL
			END 
			ELSE BEGIN
	
				SET @SQL= 'CREATE CREDENTIAL [' + @BlobCredential +'] WITH IDENTITY = N''' + @BlobCredential + ''', SECRET = N''' + @BlobCredentialSecret + ''''
				PRINT @SQL

				DECLARE @outerSQL NVARCHAR(max)
				SET @outerSQL = '
				declare @innerSQL nvarchar(max)
				set @innerSQL = ''' + REPLACE(@SQL, '''', '''''') + '''
				EXEC ['+ @TargetServer + '].master.dbo.sp_executesql @innerSQL'
				IF @debug > 1 PRINT '@OuterSQL:'+ CHAR(10) + @outerSQL
				EXEC sp_executesql @outerSQL
			END
		END


    END

--------------------------------------------------------------------------------
IF @debug > 0 PRINT CHAR(10) + 'Made it through pre-flight checks'

--------------------------------------------------------------------------------


DECLARE @RestoreStatement NVARCHAR(max)

SET @RestoreStatement = '' --Can't append a null

WHILE (SELECT COUNT(SortSequence) FROM @RestoreRows WHERE blnProcessed = 0) > 0
BEGIN 
	SET @RestoreStatement = @RestoreStatement + (SELECT TOP 1 TSQL FROM @RestoreRows WHERE [@RestoreRows].[blnProcessed] = 0 ORDER BY [@RestoreRows].[SortSequence]) + CHAR(10) 
	UPDATE @RestoreRows SET [blnProcessed] = 1 WHERE [@RestoreRows].[SortSequence] = (SELECT TOP 1 [@RestoreRows].[SortSequence] FROM @RestoreRows WHERE [@RestoreRows].[blnProcessed] = 0 ORDER BY [@RestoreRows].[SortSequence])
END

IF @debug > 1 PRINT '--Restore Script to be run on ' + @TargetServer + CHAR(10) + @RestoreStatement + CHAR(10)


	IF @TargetServerIsLocal = 0
	BEGIN
	    DECLARE @outerRestoreSQL NVARCHAR(max)
		SET @outerSQL = '
		declare @innerRestoreSQL nvarchar(max)
		set @innerRestoreSQL = ''' + REPLACE(@RestoreStatement, '''', '''''') + '''
		EXEC ['+ @TargetServer + '].master.dbo.sp_executesql @innerRestoreSQL'
		IF @debug > 1 PRINT '@OuterRestoreSQL:'+ CHAR(10) + @outerSQL
		
		EXEC sp_executesql @outerSQL
	END 
	ELSE
		EXEC sp_executesql @RestoreStatement


END
GO 


------------------------------------------------------------------------------------------------------------------------


--usp_AutoMirror
--Stored procedure to automagically mirror databases.
--Uses RestoreGenerator to generate SQL Backup scripts
--Relies on database backups having been made to Azure Blob storage rather than attempting to connect to remote drive shares
--Assumes that mirroring endpoints have already been configured
--Assumes that there is already a full and transaction log backup(s) to restore (maybe we should add a check)
--Depends upon RestoreGene with additional support for Cloud Backups

USE [master] 
GO 
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'usp_AutoMirror') 
EXEC ('CREATE PROC dbo.usp_AutoMirror AS SELECT ''stub version, to be replaced''') 
GO 

ALTER PROC [dbo].[usp_AutoMirror]
(
	@PrimaryServer NVARCHAR(255)
,	@SecondaryServer NVARCHAR(255)
,	@WitnessServerEndpoint NVARCHAR(255)
,	@PrimaryServerEndpoint NVARCHAR(255)
,	@SecondaryServerEndpoint NVARCHAR(255)
,	@DatabaseName sysname
,	@SecondaryServerDataPath NVARCHAR(255)
,	@SecondaryServerLogPath	NVARCHAR(255)
,	@BlobCredential NVARCHAR(255) = NULL --Now optional so that we can support restore from UNC paths without this.
,	@BlobCredentialSecret NVARCHAR(255) = NULL

)
AS
BEGIN

/*
Copyright (c) 2016 SQL Fairy http://sqlfairy.com.au

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

dbo.usp_AutoMirror – An awesome and fun tool :) which uses a similar approach to usp_RestoreDB 
but takes things a step further by automatically mirroring databases.

@PrimaryServer NVARCHAR(255) [mandatory]
Name of the SQL Server where the database to be mirrored is located

@SecondaryServer NVARCHAR(255) [mandatory]
Name of the SQL Server where the database will be mirrored

@WitnessServerEndpoint NVARCHAR(255) [mandatory]
Path to the Witness server’s mirroring endpoint e.g. 'TCP://mywitness.sqlfairy.com.au:5022'

@PrimaryServerEndpoint NVARCHAR(255) [mandatory]
Path to the Primary server’s mirroring endpoint e.g. 'TCP://myPri.sqlfairy.com.au:5022'

@SecondaryServerEndpoint NVARCHAR(255) [mandatory]
Path to the Secondary server’s mirroring endpoint e.g. 'TCP://mySec.sqlfairy.com.au:5022'

@DatabaseName sysname [mandatory]
Name of the Database to be mirrored

@SecondaryServerDataPath NVARCHAR(255) [mandatory]
Disk path on the Secondary Server where the data file (mdf) will be restored

@SecondaryServerLogPath	NVARCHAR(255) [mandatory]
Disk path on the Secondary Server where the log file (ldf) will be restored

@BlobCredential	NVARCHAR(255) [optional]	
Name of the Credential used on the Secondary Server to access the Blob Storage Account.  
This is normally set to match the Azure Storage Account Name.

NB: If the credential doesn’t already exist and a @BlobCredentialSecret has been supplied then a new credential will be created.  If the credential already exists then it will not be overwritten as this may interfere with other processes such as backup which rely on the existing credential.
NB(2): This parameter is now optional but only if the database to be restored exists on a \\UNC path.

@BlobCredentialSecret NVARCHAR(255)	[optional]
This is the Storage Account Access Key (Primary or Secondary).  
This value is only required if there is not already a credential containing this key on the server.  
If supplied in conjunction with the @BlobCredential parameter and there is not already a matching credential on the server one will be created.  
Existing credentials will not be overwritten. 

Provided by SQLFairy http://sqlfairy.com.au

For more info please visit http://www.sqlfairy.com.au/2016/06/backup-to-the-cloud-super-dev-ops-restore-powers-dr-and-more/
*/

    
SET NOCOUNT ON

DECLARE @debug int = 2

--Let's check that we don't have the same values for @PrimaryServer and @SecondaryServer
IF @PrimaryServer = @SecondaryServer RAISERROR(N'ERROR: Primary and Secondary servers are the same.  That can''t work!', 1, 1) 	
IF @PrimaryServer = @SecondaryServer RETURN

--Let's also check that we have different endpoints for the primary secondary and witness
IF @PrimaryServerEndpoint = @SecondaryServerEndpoint OR @PrimaryServerEndpoint = @WitnessServerEndpoint OR @SecondaryServerEndpoint = @WitnessServerEndpoint
	RAISERROR(N'ERROR: Server endpoints are duplicated.  That can''t work!', 1, 1) 	
IF @PrimaryServerEndpoint = @SecondaryServerEndpoint OR @PrimaryServerEndpoint = @WitnessServerEndpoint OR @SecondaryServerEndpoint = @WitnessServerEndpoint RETURN
--First we need to establish whether the servers being referenced are local or remote and establish linked servers if required
--Also set some variables to indicate local or remote
DECLARE @PrimaryServerIsLocal BIT
DECLARE @SecondaryServerIsLocal BIT

DECLARE @serverNameLike NVARCHAR(255)
SET @serverNameLike = '%' + @@SERVERNAME +'%'

--Checking for the local servername inside the supplied servername strings
SELECT @PrimaryServerIsLocal = CASE WHEN @PrimaryServer LIKE @serverNameLike THEN 1 ELSE 0 END 
SELECT @SecondaryServerIsLocal = CASE WHEN @SecondaryServer LIKE @serverNameLike THEN 1 ELSE 0 END 

IF @debug > 0 PRINT 'PrimaryServerIsLocal ' + CAST(@PrimaryServerIsLocal AS NVARCHAR(1))
IF @debug  > 0 PRINT 'SeondaryServerIsLocal ' + CAST(@SecondaryServerIsLocal AS NVARCHAR(1))

--Let's upper case the servernames for S&G.
SET @PrimaryServer = UPPER(@PrimaryServer)
SET @SecondaryServer = UPPER(@SecondaryServer)

--ToDo: Consider moving to another stored proc
IF @PrimaryServerIsLocal = 0 AND NOT EXISTS (SELECT * FROM sys.[servers] WHERE name = @PrimaryServer)
	BEGIN
		EXEC master.dbo.sp_addlinkedserver @server = @PrimaryServer, @srvproduct=N'SQL Server'
		EXEC master.dbo.sp_serveroption @server=@PrimaryServer, @optname=N'collation compatible', @optvalue=N'false'
		EXEC master.dbo.sp_serveroption @server=@PrimaryServer, @optname=N'data access', @optvalue=N'true'
		EXEC master.dbo.sp_serveroption @server=@PrimaryServer, @optname=N'dist', @optvalue=N'false'
		EXEC master.dbo.sp_serveroption @server=@PrimaryServer, @optname=N'pub', @optvalue=N'false'
		EXEC master.dbo.sp_serveroption @server=@PrimaryServer, @optname=N'rpc', @optvalue=N'true'
		EXEC master.dbo.sp_serveroption @server=@PrimaryServer, @optname=N'rpc out', @optvalue=N'true'
		EXEC master.dbo.sp_serveroption @server=@PrimaryServer, @optname=N'sub', @optvalue=N'false'
		EXEC master.dbo.sp_serveroption @server=@PrimaryServer, @optname=N'connect timeout', @optvalue=N'0'
		EXEC master.dbo.sp_serveroption @server=@PrimaryServer, @optname=N'collation name', @optvalue=null
		EXEC master.dbo.sp_serveroption @server=@PrimaryServer, @optname=N'lazy schema validation', @optvalue=N'false'
		EXEC master.dbo.sp_serveroption @server=@PrimaryServer, @optname=N'query timeout', @optvalue=N'0'
		EXEC master.dbo.sp_serveroption @server=@PrimaryServer, @optname=N'use remote collation', @optvalue=N'true'
		EXEC master.dbo.sp_serveroption @server=@PrimaryServer, @optname=N'remote proc transaction promotion', @optvalue=N'false'
		--Add a login
		EXEC master.dbo.sp_addlinkedsrvlogin @rmtsrvname = @PrimaryServer, @locallogin = NULL , @useself = N'True'
	END

IF @SecondaryServerIsLocal= 0 AND NOT EXISTS (SELECT * FROM sys.[servers] WHERE name = @SecondaryServer)
	BEGIN
		EXEC master.dbo.sp_addlinkedserver @server = @SecondaryServer, @srvproduct=N'SQL Server'
		EXEC master.dbo.sp_serveroption @server=@SecondaryServer, @optname=N'collation compatible', @optvalue=N'false'
		EXEC master.dbo.sp_serveroption @server=@SecondaryServer, @optname=N'data access', @optvalue=N'true'
		EXEC master.dbo.sp_serveroption @server=@SecondaryServer, @optname=N'dist', @optvalue=N'false'
		EXEC master.dbo.sp_serveroption @server=@SecondaryServer, @optname=N'pub', @optvalue=N'false'
		EXEC master.dbo.sp_serveroption @server=@SecondaryServer, @optname=N'rpc', @optvalue=N'true'
		EXEC master.dbo.sp_serveroption @server=@SecondaryServer, @optname=N'rpc out', @optvalue=N'true'
		EXEC master.dbo.sp_serveroption @server=@SecondaryServer, @optname=N'sub', @optvalue=N'false'
		EXEC master.dbo.sp_serveroption @server=@SecondaryServer, @optname=N'connect timeout', @optvalue=N'0'
		EXEC master.dbo.sp_serveroption @server=@SecondaryServer, @optname=N'collation name', @optvalue=null
		EXEC master.dbo.sp_serveroption @server=@SecondaryServer, @optname=N'lazy schema validation', @optvalue=N'false'
		EXEC master.dbo.sp_serveroption @server=@SecondaryServer, @optname=N'query timeout', @optvalue=N'0'
		EXEC master.dbo.sp_serveroption @server=@SecondaryServer, @optname=N'use remote collation', @optvalue=N'true'
		EXEC master.dbo.sp_serveroption @server=@SecondaryServer, @optname=N'remote proc transaction promotion', @optvalue=N'false'
		--Add a login
		EXEC master.dbo.sp_addlinkedsrvlogin @rmtsrvname =@SecondaryServer, @locallogin = NULL , @useself = N'True'
	END

--Now let's validate that we don't already have a mirror of this database on the primary or secondary servers

DECLARE @SQL NVARCHAR(max)
SET @SQL = ''

--SQL to check for mirror on Primary server
SET @sql = '
SELECT top 1  @MirrorGuid =
		[database_mirroring].[mirroring_guid]
FROM    ' +
	CASE WHEN @PrimaryServerIsLocal = 0 THEN '[' + @PrimaryServer + '].' ELSE '' END +
		'[master].[sys].[database_mirroring] [database_mirroring]
        INNER JOIN ' +
		CASE WHEN @PrimaryServerIsLocal = 0 THEN '[' + @PrimaryServer + '].' ELSE '' END +
		'[master].[sys].[databases] [databases] ON [database_mirroring].[database_id] = [databases].[database_id] 
			and [database_mirroring].[mirroring_guid] IS NOT NULL
			and [databases].[name] = ''' + @DatabaseName + ''''

DECLARE @MirrorGuid UNIQUEIDENTIFIER
 EXEC sp_executesql @SQL,N'@MirrorGuid uniqueidentifier out', @MirrorGuid OUT
IF @debug  > 0 PRINT @MirrorGuid
IF @MirrorGuid IS NOT NULL RAISERROR(N'ERROR: Mirror already configured for database on primary server', 1, 1) 	

IF @MirrorGuid IS NOT NULL RETURN --STOP RIGHT NOW IF THE MIRROR IS ESTABLISHED ON EITHER SERVER

--Now check the secondary server...

SET @sql = '
SELECT top 1  @MirrorGuid = 
		[database_mirroring].[mirroring_guid]
FROM    ' +
	CASE WHEN @SecondaryServerIsLocal = 0 THEN '[' + @SecondaryServer + '].' ELSE '' END +
		'[master].[sys].[database_mirroring] [database_mirroring]
        INNER JOIN ' +
		CASE WHEN @SecondaryServerIsLocal = 0 THEN '[' + @SecondaryServer + '].' ELSE '' END +
		'[master].[sys].[databases] [databases] ON [database_mirroring].[database_id] = [databases].[database_id] 
			and [database_mirroring].[mirroring_guid] IS NOT NULL
			and [databases].[name] = ''' + @DatabaseName + ''''

--DECLARE @MirrorGuid NVARCHAR(255)
EXEC sp_executesql @SQL,N'@MirrorGuid uniqueidentifier out', @MirrorGuid OUT
IF @debug  > 0 PRINT @MirrorGuid
IF @MirrorGuid IS NOT null RAISERROR(N'ERROR: Mirror already configured for database on secondary server', 1, 1) 	

IF @MirrorGuid IS NOT NULL RETURN --STOP RIGHT NOW IF THE MIRROR IS ESTABLISHED ON EITHER SERVER


IF @debug  > 0 PRINT 'Database is not already mirrored on either server'


--Now we can check to see if the database does exist on the Primary server and in a good state
SET @SQL = ''

--SQL to check for healthy DB on Primary server
SET @sql = '
SELECT top 1  @intDatabaseId =
		[databases].[database_id]
FROM    ' +
	CASE WHEN @PrimaryServerIsLocal = 0 THEN '[' + @PrimaryServer + '].' ELSE '' END +
		'[master].[sys].[databases] [databases]
		 WHERE [databases].[state_desc] = ''ONLINE''
			and [databases].[user_access_desc] = ''MULTI_USER''
			and [databases].[name] = ''' + @DatabaseName + ''''

DECLARE @intDatabaseId INT
 EXEC sp_executesql @SQL,N'@intDatabaseId int out', @intDatabaseId OUT
--IF @debug  > 0PRINT @intDatabaseId
IF @intDatabaseId IS NULL RAISERROR(N'ERROR: Database is either not found, not online or not MULTI_USER on primary server', 1, 1) 	
IF @debug  > 0 AND @intDatabaseId IS NOT NULL PRINT	'Database located on primary server with id ' + CAST(@intdatabaseId AS nchar(10))
IF @intDatabaseId IS NULL RETURN --STOP RIGHT NOW IF THE DATABASE IS NOT FOUND ON THE PRIMARY

--Now we can check to see if the database does exist on the Primary server and in a good state
SET @SQL = ''

--SQL to check we don't already have this database on the secondary
--NB: We could check for it in a borked restoring state and drop but that's muy scary for now. :)
SET @sql = '
SELECT top 1  @intDatabaseId =
		[databases].[database_id]
FROM    ' +
	CASE WHEN @SecondaryServerIsLocal = 0 THEN '[' + @SecondaryServer + '].' ELSE '' END +
		'[master].[sys].[databases] [databases]
		 WHERE [databases].[name] = ''' + @DatabaseName + ''''

SET @intDatabaseId = NULL --just in case
 EXEC sp_executesql @SQL,N'@intDatabaseId int out', @intDatabaseId OUT
--IF @debug  > 0PRINT @intDatabaseId
IF @intDatabaseId IS NOT NULL RAISERROR(N'ERROR: A database with a matching name has been located on the secondary server.  This will need to be resolved manually', 1, 1) 	
IF @debug  > 0AND @intDatabaseId IS NOT NULL PRINT	'WARNING: Database ' + @DatabaseName + ' located on secondary server with id ' + CAST(@intdatabaseId AS nchar(10))
IF @intDatabaseId IS NOT NULL RETURN --STOP RIGHT NOW IF THE DATABASE IS FOUND ON THE SECONDARY

-------------------------------------------------------------------------------

--Okay... Let's restore the database to the mirror FTW!!!1!

--Need to generate a restore script from the primary

DECLARE @runRestoreGeneSQL NVARCHAR(max)
SET @runRestoreGeneSQL = 
	CASE WHEN @PrimaryServerIsLocal = 0 THEN '[' + @PrimaryServer + '].' ELSE '' END +
	'[master].[dbo].[sp_RestoreGene]
		@Database=''' + @DatabaseName +'''
	,	@TargetDatabase=''' + @DatabaseName +'''
	,	@WithMoveDataFiles = ''' + @SecondaryServerDataPath + '''
	,	@WithMoveLogFile = ''' + @SecondaryServerLogPath + '''
	,	@Log_Reference = ''Mirror databaase ' + @DatabaseName + 'from primary ' + @PrimaryServer + ' to ' + @SecondaryServer + '.''' 
	+ case when @BlobCredential is not null then ',	@BlobCredential = ''' + @BlobCredential + '''' + CHAR(10) ELSE '' END + --Don't include @blobCredential if it hasn't been supplied
	',	@WithRecovery = 0
	,	@WithReplace = 0
'

IF @debug > 1 PRINT CHAR(10) + @runRestoreGeneSQL + CHAR(10)


DECLARE @RestoreRows AS TABLE
(	TSQL NVARCHAR(MAX)
,	BackupDate DATETIME
,	BackupDevice NVARCHAR(255)
,	Last_LSN NUMERIC(32) NULL
,	Databaase_Name sysname
,	SortSequence INT
,	blnProcessed BIT DEFAULT 0
)

INSERT INTO @RestoreRows
        ([TSQL]
        ,[BackupDate]
        ,[BackupDevice]
        ,[Last_LSN]
        ,[Databaase_Name]
        ,[SortSequence]
        )

EXEC sp_executesql @runRestoreGeneSQL

IF @debug > 2 SELECT * FROM @RestoreRows

---------Check to see that there is at least one log file being restored.  We need this for mirroring.
IF (SELECT COUNT(*) FROM @RestoreRows WHERE TSQL LIKE '%RESTORE LOG%') = 0 
	BEGIN
    	RAISERROR(N'ERROR: No transaction log backups located.  Please ensure that the database to be backed up uses the FULL recovery model and that there is at least one transaction log backup.', 1, 1) 	
		RETURN --Can't continue past this point :(    	
    END

----------We're supporting restore from a central UNC path now so check to see whether any http paths are included in the restore set...
DECLARE @RestoreIncludesBlobFiles BIT
IF (SELECT COUNT(*) FROM @RestoreRows WHERE BackupDevice LIKE 'http%') > 0 SET @RestoreIncludesBlobFiles = 1

--Now if we do require a blob credential we will check for it here
IF @RestoreIncludesBlobFiles = 1
	BEGIN
    	--Now that we've made it optional to supply an @BlobCredential so that we can handle restore from UNC \\Server\Share backups we need to check here if one has actually been supplied...
		IF @BlobCredential IS NULL 
			BEGIN
				RAISERROR(N'ERROR: Backup contains files in Azure blob storage and no Credential was supplied :(', 1, 1) 	
				RETURN --Can't continue past this point :(    	
            END


		--Now let's check that we have the specified credential on the mirror server so that the database can be restored
		--If we don't have it and we have been supplied with a secret then we can create the credential.
		--We're not going to update existing credentials with supplied parameters as that could cause backups to fail
		--	which means that if you supply some dud credentials the first time around it could cause some pain.  --Maybe reconsider this 'feature' later

		--We only need to check this on the destination server
		SET @SQL = ''

		--SQL to check if we already have a credential matching the supplied name
		SET @sql = '
		SELECT top 1  @intCredentialId =
				[credentials].[credential_id]
		FROM    ' +
			CASE WHEN @SecondaryServerIsLocal = 0 THEN '[' + @SecondaryServer + '].' ELSE '' END +
				'[master].[sys].[credentials] [credentials]
				 WHERE [credentials].[name] = ''' + @BlobCredential + ''''

		DECLARE @intCredentialId INT
		 EXEC sp_executesql @SQL,N'@intCredentialId int out', @intCredentialId OUT

		IF @intCredentialId IS NULL AND @BlobCredentialSecret IS NULL RAISERROR(N'ERROR: BlobCredential not located on secondary server and no BlobCredentialSecret Supplied', 1, 1) 	
		IF @debug  > 0 AND @intDatabaseId IS NOT NULL PRINT	'Database credential ' + @BlobCredential + ' located on secondary server'
		IF @intCredentialId IS NULL AND @BlobCredentialSecret IS NULL RETURN --STOP RIGHT NOW.  We need a credential secret

		IF @intCredentialId IS NULL AND @BlobCredentialSecret IS NOT NULL
		BEGIN
		--Geez this is gettig hairy(tm) !!!1!
		--NB: If you're reviewing this code the obtuse string construction and nested exec sp_executesql is required because exec will not accept a variable for the server name :(
			IF @SecondaryServerIsLocal = 1
			BEGIN
				SET @SQL= 'CREATE CREDENTIAL [' + @BlobCredential +'] WITH IDENTITY = N''' + @BlobCredential + ''', SECRET = N''' + @BlobCredentialSecret + ''''
				IF @debug > 1 PRINT @SQL
				EXEC sp_executesql @SQL
			END 
			ELSE BEGIN
	
				SET @SQL= 'CREATE CREDENTIAL [' + @BlobCredential +'] WITH IDENTITY = N''' + @BlobCredential + ''', SECRET = N''' + @BlobCredentialSecret + ''''
				PRINT @SQL

				DECLARE @outerSQL NVARCHAR(max)
				SET @outerSQL = '
				declare @innerSQL nvarchar(max)
				set @innerSQL = ''' + REPLACE(@SQL, '''', '''''') + '''
				EXEC ['+ @SecondaryServer + '].master.dbo.sp_executesql @innerSQL'
				IF @debug > 1 PRINT '@OuterSQL:'+ CHAR(10) + @outerSQL
				EXEC sp_executesql @outerSQL
			END
		END

	END

--------------------------------------------------------------------------------
IF @debug > 0 PRINT CHAR(10) + 'Made it through pre-flight checks'
--------------------------------------------------------------------------------

DECLARE @RestoreStatement NVARCHAR(max)

SET @RestoreStatement = '' --Can't append a null

WHILE (SELECT COUNT(SortSequence) FROM @RestoreRows WHERE blnProcessed = 0) > 0
BEGIN 
	SET @RestoreStatement = @RestoreStatement + (SELECT TOP 1 TSQL FROM @RestoreRows WHERE [@RestoreRows].[blnProcessed] = 0 ORDER BY [@RestoreRows].[SortSequence]) + CHAR(10) 
	UPDATE @RestoreRows SET [blnProcessed] = 1 WHERE [@RestoreRows].[SortSequence] = (SELECT TOP 1 [@RestoreRows].[SortSequence] FROM @RestoreRows WHERE [@RestoreRows].[blnProcessed] = 0 ORDER BY [@RestoreRows].[SortSequence])
END

IF @debug > 1 PRINT '--Restore Script to be run on ' + @SecondaryServer + CHAR(10) + @RestoreStatement + CHAR(10)


	IF @SecondaryServerIsLocal = 0
	BEGIN
	    DECLARE @outerRestoreSQL NVARCHAR(max)
		SET @outerSQL = '
		declare @innerRestoreSQL nvarchar(max)
		set @innerRestoreSQL = ''' + REPLACE(@RestoreStatement, '''', '''''') + '''
		EXEC ['+ @SecondaryServer + '].master.dbo.sp_executesql @innerRestoreSQL'
		IF @debug > 1 PRINT '@OuterRestoreSQL:'+ CHAR(10) + @outerSQL
		
		EXEC sp_executesql @outerSQL
	END 
	ELSE
		EXEC sp_executesql @RestoreStatement

-----------------------------------------------
--Now as we frequently experience problems with additional transaction log backups happening while a long running 
--RESTORE is in progress we'll see if there are any additional backups to be restored and restore them as well.

DECLARE @MoreRestoreRows AS TABLE
(	TSQL NVARCHAR(MAX)
,	BackupDate DATETIME
,	BackupDevice NVARCHAR(255)
,	Last_LSN NUMERIC(32) NULL
,	Databaase_Name sysname
,	SortSequence INT
,	blnProcessed BIT DEFAULT 0
)

INSERT INTO @MoreRestoreRows
        ([TSQL]
        ,[BackupDate]
        ,[BackupDevice]
        ,[Last_LSN]
        ,[Databaase_Name]
        ,[SortSequence]
        )

EXEC sp_executesql @runRestoreGeneSQL

IF @debug > 2 SELECT * FROM @MoreRestoreRows


SET @RestoreStatement = ''

WHILE	(SELECT COUNT([MoreRestoreRows].[SortSequence]) FROM @MoreRestoreRows MoreRestoreRows
		LEFT OUTER JOIN @RestoreRows RestoreRows ON [MoreRestoreRows].[SortSequence] = [RestoreRows].[SortSequence]
		WHERE ([MoreRestoreRows].[blnProcessed] = 0 AND ([RestoreRows].[SortSequence] IS NULL OR [MoreRestoreRows].[SortSequence] = 1))
		) > 0

BEGIN
	SET @RestoreStatement = @RestoreStatement + 
	(
		SELECT TOP 1 [MoreRestoreRows].TSQL FROM @MoreRestoreRows MoreRestoreRows
		LEFT OUTER JOIN @RestoreRows RestoreRows ON [MoreRestoreRows].[SortSequence] = [RestoreRows].[SortSequence]
		WHERE ([MoreRestoreRows].[blnProcessed] = 0 AND ([RestoreRows].[SortSequence] IS NULL OR [MoreRestoreRows].[SortSequence] = 1))
		ORDER BY [MoreRestoreRows].[SortSequence]
	) + CHAR(10) 

	UPDATE @MoreRestoreRows SET [blnProcessed] = 1 WHERE [@MoreRestoreRows].[SortSequence] = 
	(SELECT TOP 1 [MoreRestoreRows].[SortSequence] FROM @MoreRestoreRows MoreRestoreRows
		LEFT OUTER JOIN @RestoreRows RestoreRows ON [MoreRestoreRows].[SortSequence] = [RestoreRows].[SortSequence]
		WHERE ([MoreRestoreRows].[blnProcessed] = 0 AND ([RestoreRows].[SortSequence] IS NULL OR [MoreRestoreRows].[SortSequence] = 1))
		ORDER BY [MoreRestoreRows].[SortSequence])
END

IF @debug > 1 PRINT 'Second Restore Statement:' + CHAR(10) + @RestoreStatement

	IF @SecondaryServerIsLocal = 0
	BEGIN
	    --DECLARE @outerRestoreSQL NVARCHAR(max)
		SET @outerSQL = '
		declare @innerRestoreSQL nvarchar(max)
		set @innerRestoreSQL = ''' + REPLACE(@RestoreStatement, '''', '''''') + '''
		EXEC ['+ @SecondaryServer + '].master.dbo.sp_executesql @innerRestoreSQL'
		IF @debug > 1 PRINT '@OuterRestoreSQL:'+ CHAR(10) + @outerSQL
		
		EXEC sp_executesql @outerSQL
	END 
	ELSE
		EXEC sp_executesql @RestoreStatement

--Now the database is in place.  Let's mirror it!

--We could check here to see if the restore process has worked correctly though there's not going to be a dramatic problem
--	with attempting to mirror the database if this failed.  The mirror will just fail.

DECLARE @mirrorStatementSecondary NVARCHAR(max)
DECLARE @mirrorStatementPrimary NVARCHAR(max)

SET @mirrorStatementSecondary = 'ALTER DATABASE [' + @DatabaseName +'] SET PARTNER = ''' + @PrimaryServerEndpoint +''';'
SET @mirrorStatementPrimary = 'ALTER DATABASE [' + @DatabaseName +'] SET PARTNER = ''' + @SecondaryServerEndpoint +''';' + CHAR(10)
SET @mirrorStatementPrimary = @mirrorStatementPrimary + 'ALTER DATABASE [' + @DatabaseName +'] SET WITNESS = ''' + @WitnessServerEndpoint +''';' + CHAR(10)
SET @mirrorStatementPrimary = @mirrorStatementPrimary + 'ALTER DATABASE [' + @DatabaseName +'] SET PARTNER TIMEOUT 30;'

DECLARE @outerMirrorSQL NVARCHAR(max)

---Execute on secondary
IF @SecondaryServerIsLocal = 0
	BEGIN
	    SET @outerMirrorSQL = '
		declare @innerMirrorSQL nvarchar(max)
		set @innerMirrorSQL = ''' + REPLACE(@mirrorStatementSecondary, '''', '''''') + '''
		EXEC ['+ @SecondaryServer + '].master.dbo.sp_executesql @innerMirrorSQL'
		IF @debug > 1 PRINT '@OuterMirrorSQL:'+ CHAR(10) + @outerMirrorSQL
		
		EXEC sp_executesql @outerMirrorSQL
	END 
	ELSE
		EXEC sp_executesql @mirrorStatementSecondary

--Execute on primary
IF @PrimaryServerIsLocal = 0
	BEGIN
	    SET @outerMirrorSQL = '
		declare @innerMirrorSQL nvarchar(max)
		set @innerMirrorSQL = ''' + REPLACE(@mirrorStatementPrimary, '''', '''''') + '''
		EXEC ['+ @PrimaryServer + '].master.dbo.sp_executesql @innerMirrorSQL'
		IF @debug > 1 PRINT '@OuterMirrorSQL:'+ CHAR(10) + @outerMirrorSQL
		
		EXEC sp_executesql @outerMirrorSQL
	END 
	ELSE
		EXEC sp_executesql @mirrorStatementPrimary

--------------------------------------------------
--Now we should perform a check to determine whether 
--the mirror has been successfully established
--------------------------------------------------
DECLARE @MirrorStatusQuery NVARCHAR(max)
SET @MirrorStatusQuery = '
SELECT
 [PrimaryDatabases].[name] AS [Database Name]
,[PrimaryMirroring].[mirroring_state_desc] AS [Mirroring State]
,[PrimaryMirroring].[mirroring_role_desc] AS [Mirroring Role]
,[PrimaryMirroring].[mirroring_safety_level_desc] AS [Mirror Safety Level]
,[PrimaryMirroring].[mirroring_partner_name]
,[PrimaryMirroring].[mirroring_partner_instance]
,[PrimaryMirroring].[mirroring_witness_name]
,[PrimaryMirroring].[mirroring_witness_state_desc]
,[PrimaryMirroring].[mirroring_connection_timeout]
FROM ' + 
CASE WHEN @PrimaryServerIsLocal = 0 THEN '[' + @PrimaryServer + '].' ELSE '' END +
'master.sys.[database_mirroring] PrimaryMirroring
INNER JOIN ' +
CASE WHEN @PrimaryServerIsLocal = 0 THEN '[' + @PrimaryServer + '].' ELSE '' END + 
'master.sys.[databases] PrimaryDatabases ON [PrimaryDatabases].[database_id] = [PrimaryMirroring].[database_id]
WHERE [PrimaryDatabases].[name] = ''' + @DatabaseName + ''''

IF @debug > 1 PRINT @MirrorStatusQuery

EXEC sp_executesql @MirrorStatusQuery 

END
GO 


-------------------------------------------------------------------------------------------------------------------------------