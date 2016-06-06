# SQLFairyBackupTools

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
