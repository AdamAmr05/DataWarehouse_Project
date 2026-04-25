/*

Script: Execute_and_check_procedure.sql
Purpose: Executes the Bronze load procedure and performs basic checks.
Run order: 4 of 4  (after Procedures_log_Bronze_layer.sql)
==========================================================

*/

USE DataWarehouse;
GO

-- ============================================================
-- STEP 1: Execute the Bronze Load
-- ============================================================
PRINT '======================================================';
PRINT 'STEP 1: Executing bronze.load_bronze';
PRINT '======================================================';

EXEC bronze.load_bronze;
GO

-- ============================================================
-- STEP 2: Load Log Summary
-- ============================================================
PRINT '======================================================';
PRINT 'STEP 2: Load Log Summary';
PRINT '======================================================';

SELECT
log_id,
table_name,
load_start_time,
load_end_time,
duration_seconds,
rows_inserted,
status,
error_message
FROM bronze.load_log
ORDER BY log_id;

-- ============================================================
-- STEP 3: Quick Data Spot Checks
-- ============================================================
PRINT '======================================================';
PRINT 'STEP 3: Spot Checks (Top 3 rows per key table)';
PRINT '======================================================';

SELECT TOP 3 * FROM bronze.ret_transaction_items;
SELECT TOP 3 * FROM bronze.ret_pos_transactions;
SELECT TOP 3 * FROM bronze.ret_customers;
SELECT TOP 3 * FROM bronze.ret_online_order_items;
SELECT TOP 3 * FROM bronze.ret_online_orders;
SELECT TOP 3 * FROM bronze.ret_deliveries;
