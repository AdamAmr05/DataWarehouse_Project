-- =============================================================================
-- MILESTONE 1 AUDIT SCRIPT  |  Bronze & Silver Layer Verification
-- Data Warehouse Project
-- =============================================================================
-- HOW IT WORKS:
--   1. Verifies bronze and silver schemas exist
--   2. Lists all tables in each schema
--   3. Shows row counts and sample data per table
--   4. Generates a UNIQUE STUDENT TOKEN based on:
--        - Database name
--        - Current SQL Server login
--        - Machine/host name
--        - Submission timestamp
--      This token is different for every student and every run.
--      Students CANNOT replicate each other's token by copying scripts.
-- =============================================================================

SET NOCOUNT ON;
PRINT '=============================================================================';
PRINT '           MILESTONE 1 SUBMISSION AUDIT  |  Data Warehouse Project';
PRINT '=============================================================================';
PRINT '';

-- =============================================================================
-- SECTION 0 :  STUDENT IDENTITY & ANTI-PLAGIARISM TOKEN
-- =============================================================================

DECLARE @db_name        NVARCHAR(128) = DB_NAME();
DECLARE @login_name     NVARCHAR(128) = SUSER_SNAME();
DECLARE @host_name      NVARCHAR(128) = HOST_NAME();
DECLARE @submit_time    NVARCHAR(30)  = CONVERT(NVARCHAR(30), GETDATE(), 120);

-- Build a deterministic-but-unique seed string and hash it
DECLARE @seed_string NVARCHAR(512) =
    @db_name + '|' + @login_name + '|' + @host_name + '|' + @submit_time;

-- SHA2_256 hash → convert to hex string → take first 32 chars as the token
DECLARE @raw_hash  VARBINARY(32) = HASHBYTES('SHA2_256', @seed_string);
DECLARE @token     NVARCHAR(64)  =
    UPPER(SUBSTRING(CONVERT(NVARCHAR(64), @raw_hash, 2), 1, 32));

PRINT '-------------------------------------------------------------';
PRINT ' STUDENT SUBMISSION RECORD';
PRINT '-------------------------------------------------------------';
PRINT ' Database    : ' + @db_name;
PRINT ' Login       : ' + @login_name;
PRINT ' Host        : ' + @host_name;
PRINT ' Submitted   : ' + @submit_time;
PRINT ' ';
PRINT ' ANTI-PLAGIARISM TOKEN:';
PRINT ' >>> ' + @token + ' <<<';
PRINT ' ';
PRINT ' NOTE: This token is unique to YOUR database, login, machine,';
PRINT '       and submission time. Any copy of this script run on a';
PRINT '       different environment will produce a different token.';
PRINT '-------------------------------------------------------------';
PRINT '';

-- =============================================================================
-- SECTION 1 :  SCHEMA VERIFICATION
-- =============================================================================

PRINT '=============================================================================';
PRINT ' SECTION 1 : SCHEMA CHECK';
PRINT '=============================================================================';

SELECT
    s.schema_id,
    s.name                          AS schema_name,
    p.name                          AS schema_owner,
    CASE
        WHEN s.name IN ('bronze','silver') THEN 'REQUIRED  ✓'
        ELSE 'extra'
    END                             AS status
FROM sys.schemas s
JOIN sys.database_principals p ON s.principal_id = p.principal_id
WHERE s.name IN ('bronze','silver')
   OR s.name NOT IN ('dbo','guest','INFORMATION_SCHEMA','sys',
                     'db_owner','db_accessadmin','db_securityadmin',
                     'db_ddladmin','db_backupoperator','db_datareader',
                     'db_datawriter','db_denydatareader','db_denydatawriter')
ORDER BY s.name;

-- Warn if a required schema is missing
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    PRINT '  *** WARNING: Schema [bronze] was NOT found! ***';
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
    PRINT '  *** WARNING: Schema [silver] was NOT found! ***';

PRINT '';

-- =============================================================================
-- SECTION 2 :  TABLE INVENTORY
-- =============================================================================

PRINT '=============================================================================';
PRINT ' SECTION 2 : TABLE INVENTORY  (bronze + silver)';
PRINT '=============================================================================';

SELECT
    s.name                          AS schema_name,
    t.name                          AS table_name,
    t.create_date                   AS created_at,
    t.modify_date                   AS last_modified,
    p.rows                          AS total_rows
FROM sys.tables        t
JOIN sys.schemas       s  ON t.schema_id     = s.schema_id
JOIN sys.partitions    p  ON t.object_id     = p.object_id
                          AND p.index_id    IN (0, 1)   -- heap or clustered
WHERE s.name IN ('bronze','silver')
ORDER BY s.name, t.name;

PRINT '';

-- =============================================================================
-- SECTION 3 :  COLUMN DEFINITIONS  (structure audit)
-- =============================================================================

PRINT '=============================================================================';
PRINT ' SECTION 3 : COLUMN DEFINITIONS';
PRINT '=============================================================================';

SELECT
    s.name                          AS schema_name,
    t.name                          AS table_name,
    c.column_id,
    c.name                          AS column_name,
    tp.name                         AS data_type,
    CASE tp.name
        WHEN 'nvarchar' THEN CAST(c.max_length/2 AS VARCHAR) + ' chars'
        WHEN 'varchar'  THEN CAST(c.max_length   AS VARCHAR) + ' chars'
        WHEN 'decimal'  THEN CAST(c.precision AS VARCHAR)
                              + ',' + CAST(c.scale AS VARCHAR)
        ELSE ''
    END                             AS size_info,
    CASE c.is_nullable
        WHEN 1 THEN 'NULL'
        ELSE        'NOT NULL'
    END                             AS nullable,
    CASE WHEN ic.object_id IS NOT NULL THEN 'PK' ELSE '' END AS is_pk
FROM sys.columns    c
JOIN sys.tables     t  ON c.object_id    = t.object_id
JOIN sys.schemas    s  ON t.schema_id    = s.schema_id
JOIN sys.types      tp ON c.user_type_id = tp.user_type_id
LEFT JOIN (
    SELECT ic2.object_id, ic2.column_id
    FROM sys.index_columns ic2
    JOIN sys.indexes       ix  ON ic2.object_id = ix.object_id
                              AND ic2.index_id  = ix.index_id
    WHERE ix.is_primary_key = 1
) ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE s.name IN ('bronze','silver')
ORDER BY s.name, t.name, c.column_id;

PRINT '';

-- =============================================================================
-- SECTION 4 :  ROW COUNTS PER TABLE  (quick health-check)
-- =============================================================================

PRINT '=============================================================================';
PRINT ' SECTION 4 : ROW COUNTS';
PRINT '=============================================================================';

-- Dynamic SQL builds one UNION ALL across all bronze/silver tables
DECLARE @count_sql  NVARCHAR(MAX) = N'';
DECLARE @tbl_name   NVARCHAR(128);
DECLARE @sch_name   NVARCHAR(128);
DECLARE @first_row  BIT = 1;

DECLARE tbl_cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT s.name, t.name
    FROM   sys.tables  t
    JOIN   sys.schemas s ON t.schema_id = s.schema_id
    WHERE  s.name IN ('bronze','silver')
    ORDER  BY s.name, t.name;

OPEN tbl_cur;
FETCH NEXT FROM tbl_cur INTO @sch_name, @tbl_name;

WHILE @@FETCH_STATUS = 0
BEGIN
    IF @first_row = 0
        SET @count_sql += N'
UNION ALL
';
    SET @count_sql +=
        N'SELECT ''' + @sch_name + ''' AS schema_name, '
                     + '''' + @tbl_name + ''' AS table_name, '
                     + 'COUNT(*) AS row_count '
                     + 'FROM [' + @sch_name + '].[' + @tbl_name + ']';
    SET @first_row = 0;
    FETCH NEXT FROM tbl_cur INTO @sch_name, @tbl_name;
END;

CLOSE      tbl_cur;
DEALLOCATE tbl_cur;

IF LEN(@count_sql) > 0
    EXEC sp_executesql @count_sql;
ELSE
    PRINT '  No tables found in bronze or silver schemas.';

PRINT '';

-- =============================================================================
-- SECTION 5 :  SAMPLE DATA  (top 10 rows per table)
-- =============================================================================

PRINT '=============================================================================';
PRINT ' SECTION 5 : SAMPLE DATA  (top 10 rows per table)';
PRINT '=============================================================================';

DECLARE @sample_sql NVARCHAR(MAX);

DECLARE smp_cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT s.name, t.name
    FROM   sys.tables  t
    JOIN   sys.schemas s ON t.schema_id = s.schema_id
    WHERE  s.name IN ('bronze','silver')
    ORDER  BY s.name, t.name;

OPEN smp_cur;
FETCH NEXT FROM smp_cur INTO @sch_name, @tbl_name;

WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT '';
    PRINT '--- [' + @sch_name + '].[' + @tbl_name + '] ---';

    SET @sample_sql =
        N'SELECT TOP 10 '
        + N'N''' + @sch_name + N'.' + @tbl_name + N''' AS [Table_Name], * '
        + N'FROM [' + @sch_name + N'].[' + @tbl_name + N']';
    EXEC sp_executesql @sample_sql;

    FETCH NEXT FROM smp_cur INTO @sch_name, @tbl_name;
END;

CLOSE      smp_cur;
DEALLOCATE smp_cur;

PRINT '';

-- =============================================================================
-- SECTION 6 :  DATA QUALITY SNAPSHOT  (NULLs per column)
-- =============================================================================

PRINT '=============================================================================';
PRINT ' SECTION 6 : NULL-VALUE AUDIT PER COLUMN';
PRINT '=============================================================================';

DECLARE @null_sql   NVARCHAR(MAX);
DECLARE @col_name   NVARCHAR(128);
DECLARE @col_parts  NVARCHAR(MAX);

DECLARE null_tbl_cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT s.name, t.name
    FROM   sys.tables  t
    JOIN   sys.schemas s ON t.schema_id = s.schema_id
    WHERE  s.name IN ('bronze','silver')
    ORDER  BY s.name, t.name;

OPEN null_tbl_cur;
FETCH NEXT FROM null_tbl_cur INTO @sch_name, @tbl_name;

WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT '';
    PRINT '--- [' + @sch_name + '].[' + @tbl_name + '] ---';

    -- Build SUM(CASE WHEN col IS NULL …) for every column
    SET @col_parts = N'';

    DECLARE col_cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT c.name
        FROM   sys.columns c
        JOIN   sys.tables  t2 ON c.object_id = t2.object_id
        JOIN   sys.schemas s2 ON t2.schema_id = s2.schema_id
        WHERE  s2.name = @sch_name AND t2.name = @tbl_name
        ORDER  BY c.column_id;

    OPEN col_cur;
    FETCH NEXT FROM col_cur INTO @col_name;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF LEN(@col_parts) > 0 SET @col_parts += N', ';
        SET @col_parts +=
            N'SUM(CASE WHEN [' + @col_name + N'] IS NULL THEN 1 ELSE 0 END)'
            + N' AS [' + @col_name + N'_nulls]';
        FETCH NEXT FROM col_cur INTO @col_name;
    END;

    CLOSE      col_cur;
    DEALLOCATE col_cur;

    IF LEN(@col_parts) > 0
    BEGIN
        SET @null_sql =
            N'SELECT ' + @col_parts
            + N' FROM [' + @sch_name + N'].[' + @tbl_name + N']';
        EXEC sp_executesql @null_sql;
    END;

    FETCH NEXT FROM null_tbl_cur INTO @sch_name, @tbl_name;
END;

CLOSE      null_tbl_cur;
DEALLOCATE null_tbl_cur;

PRINT '';

-- =============================================================================
-- SECTION 7 :  SILVER LAYER TRANSFORMATION CHECK
--              (verifies common cleaning expectations)
-- =============================================================================

PRINT '=============================================================================';
PRINT ' SECTION 7 : SILVER LAYER QUALITY CHECKS';
PRINT '=============================================================================';

-- 7a – Check that silver tables exist and have data
PRINT '';
PRINT '7a  Silver tables with zero rows (should be empty list after loading):';
SELECT
    s.name  AS schema_name,
    t.name  AS table_name,
    p.rows  AS row_count,
    'WARNING – empty table' AS note
FROM sys.tables     t
JOIN sys.schemas    s ON t.schema_id  = s.schema_id
JOIN sys.partitions p ON t.object_id  = p.object_id
                     AND p.index_id  IN (0,1)
WHERE s.name = 'silver'
  AND p.rows = 0;

-- 7b – Compare bronze vs silver row counts side by side
PRINT '';
PRINT '7b  Bronze vs Silver row-count comparison (matching table names):';

SELECT
    b.table_name,
    b.row_count  AS bronze_rows,
    sv.row_count AS silver_rows,
    sv.row_count - b.row_count AS difference,
    CASE
        WHEN sv.row_count = b.row_count THEN 'OK  – counts match'
        WHEN sv.row_count < b.row_count THEN 'NOTE – rows removed (dedup/filter)'
        ELSE                                 'CHECK – silver has MORE rows than bronze'
    END AS assessment
FROM (
    SELECT t.name AS table_name, p.rows AS row_count
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0,1)
    WHERE s.name = 'bronze'
) b
JOIN (
    SELECT t.name AS table_name, p.rows AS row_count
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0,1)
    WHERE s.name = 'silver'
) sv ON b.table_name = sv.table_name
ORDER BY b.table_name;

PRINT '';

-- =============================================================================
-- SECTION 8 :  FINAL SUBMISSION SUMMARY
-- =============================================================================

PRINT '=============================================================================';
PRINT ' SECTION 8 : FINAL SUBMISSION SUMMARY';
PRINT '=============================================================================';
PRINT '';

DECLARE @bronze_tables INT = (
    SELECT COUNT(*) FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'bronze');

DECLARE @silver_tables INT = (
    SELECT COUNT(*) FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'silver');

DECLARE @bronze_rows BIGINT = 0;
DECLARE @silver_rows BIGINT = 0;

SELECT @bronze_rows = SUM(p.rows)
FROM sys.tables t
JOIN sys.schemas s    ON t.schema_id = s.schema_id
JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0,1)
WHERE s.name = 'bronze';

SELECT @silver_rows = SUM(p.rows)
FROM sys.tables t
JOIN sys.schemas s    ON t.schema_id = s.schema_id
JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0,1)
WHERE s.name = 'silver';

PRINT '  Database          : ' + @db_name;
PRINT '  Student login     : ' + @login_name;
PRINT '  Host              : ' + @host_name;
PRINT '  Run timestamp     : ' + @submit_time;
PRINT '';
PRINT '  Bronze tables     : ' + CAST(@bronze_tables AS VARCHAR);
PRINT '  Bronze total rows : ' + CAST(ISNULL(@bronze_rows,0) AS VARCHAR);
PRINT '  Silver tables     : ' + CAST(@silver_tables AS VARCHAR);
PRINT '  Silver total rows : ' + CAST(ISNULL(@silver_rows,0) AS VARCHAR);
PRINT '';
PRINT '  ANTI-PLAGIARISM TOKEN : ' + @token;
PRINT '';