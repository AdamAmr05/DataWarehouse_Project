/*
===============================================================================
Stored Procedure: Procedures_log_Bronze_layer.sql
Purpose:  Truncates and reloads all Bronze tables from CSV source files.
          Logs every table load (success or failure) into bronze.load_log.
Run order: 3 of 4  (after ddl_bronze.sql)

IMPORTANT: Update the file paths in each BULK INSERT to match your machine.
           Default path assumes: C:\SQLDATA\datasets\
===============================================================================
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
        @start_time     DATETIME,
        @end_time       DATETIME,
        @rows           INT,
        @table_name     NVARCHAR(200),
        @proc_name      NVARCHAR(200) = 'bronze.load_bronze';

    PRINT '======================================================';
    PRINT 'Starting Bronze Layer Load';
    PRINT 'Procedure : bronze.load_bronze';
    PRINT 'Start Time: ' + CAST(GETDATE() AS NVARCHAR(50));
    PRINT '======================================================';

    BEGIN TRY

        PRINT '------------------------------------------------------';
        PRINT 'Section 1 of 5 — Core Dimension Tables';
        PRINT '------------------------------------------------------';

        -- ==================== CUSTOMERS ====================
        SET @table_name = 'bronze.ret_customers';
        SET @start_time = GETDATE();
        PRINT '>> Truncating: ' + @table_name;
        TRUNCATE TABLE bronze.ret_customers;
        PRINT '>> Loading:    ' + @table_name;
        BULK INSERT bronze.ret_customers
        FROM 'C:\sql\datasets\CUSTOMERS.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0d0a', TABLOCK, CODEPAGE = '65001');
        SET @rows = @@ROWCOUNT; SET @end_time = GETDATE();
        PRINT '>> Rows: ' + CAST(@rows AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's';
        INSERT INTO bronze.load_log VALUES (@proc_name,@table_name,@start_time,@end_time,DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

        -- ==================== EMPLOYEES ====================
        SET @table_name = 'bronze.ret_employees';
        SET @start_time = GETDATE();
        PRINT '>> Truncating: ' + @table_name;
        TRUNCATE TABLE bronze.ret_employees;
        PRINT '>> Loading:    ' + @table_name;
        BULK INSERT bronze.ret_employees
        FROM 'C:\sql\datasets\EMPLOYEES.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0d0a', TABLOCK, CODEPAGE = '65001');
        SET @rows = @@ROWCOUNT; SET @end_time = GETDATE();
        PRINT '>> Rows: ' + CAST(@rows AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's';
        INSERT INTO bronze.load_log VALUES (@proc_name,@table_name,@start_time,@end_time,DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

        -- ==================== STORES ====================
        SET @table_name = 'bronze.ret_stores';
        SET @start_time = GETDATE();
        PRINT '>> Truncating: ' + @table_name;
        TRUNCATE TABLE bronze.ret_stores;
        PRINT '>> Loading:    ' + @table_name;
        BULK INSERT bronze.ret_stores
        FROM 'C:\sql\datasets\STORES.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0d0a', TABLOCK, CODEPAGE = '65001');
        SET @rows = @@ROWCOUNT; SET @end_time = GETDATE();
        PRINT '>> Rows: ' + CAST(@rows AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's';
        INSERT INTO bronze.load_log VALUES (@proc_name,@table_name,@start_time,@end_time,DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

        -- ==================== REGISTERS ====================
        SET @table_name = 'bronze.ret_registers';
        SET @start_time = GETDATE();
        PRINT '>> Truncating: ' + @table_name;
        TRUNCATE TABLE bronze.ret_registers;
        PRINT '>> Loading:    ' + @table_name;
        BULK INSERT bronze.ret_registers
        FROM 'C:\sql\datasets\REGISTERS.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0d0a', TABLOCK, CODEPAGE = '65001');
        SET @rows = @@ROWCOUNT; SET @end_time = GETDATE();
        PRINT '>> Rows: ' + CAST(@rows AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's';
        INSERT INTO bronze.load_log VALUES (@proc_name,@table_name,@start_time,@end_time,DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

        -- ==================== PRODUCTS ====================
        SET @table_name = 'bronze.ret_products';
        SET @start_time = GETDATE();
        PRINT '>> Truncating: ' + @table_name;
        TRUNCATE TABLE bronze.ret_products;
        PRINT '>> Loading:    ' + @table_name;
        BULK INSERT bronze.ret_products
        FROM 'C:\sql\datasets\PRODUCTS.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0d0a', TABLOCK, CODEPAGE = '65001');
        SET @rows = @@ROWCOUNT; SET @end_time = GETDATE();
        PRINT '>> Rows: ' + CAST(@rows AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's';
        INSERT INTO bronze.load_log VALUES (@proc_name,@table_name,@start_time,@end_time,DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

        -- ==================== BRANDS ====================
        SET @table_name = 'bronze.ret_brands';
        SET @start_time = GETDATE();
        PRINT '>> Truncating: ' + @table_name;
        TRUNCATE TABLE bronze.ret_brands;
        PRINT '>> Loading:    ' + @table_name;
        BULK INSERT bronze.ret_brands
        FROM 'C:\sql\datasets\BRANDS.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0d0a', TABLOCK, CODEPAGE = '65001');
        SET @rows = @@ROWCOUNT; SET @end_time = GETDATE();
        PRINT '>> Rows: ' + CAST(@rows AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's';
        INSERT INTO bronze.load_log VALUES (@proc_name,@table_name,@start_time,@end_time,DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

        -- ==================== DEPARTMENTS ====================
        SET @table_name = 'bronze.ret_departments';
        SET @start_time = GETDATE();
        PRINT '>> Truncating: ' + @table_name;
        TRUNCATE TABLE bronze.ret_departments;
        PRINT '>> Loading:    ' + @table_name;
        BULK INSERT bronze.ret_departments
        FROM 'C:\sql\datasets\DEPARTMENTS.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0d0a', TABLOCK, CODEPAGE = '65001');
        SET @rows = @@ROWCOUNT; SET @end_time = GETDATE();
        PRINT '>> Rows: ' + CAST(@rows AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's';
        INSERT INTO bronze.load_log VALUES (@proc_name,@table_name,@start_time,@end_time,DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

        -- ==================== PROMOTIONS ====================
        SET @table_name = 'bronze.ret_promotions';
        SET @start_time = GETDATE();
        PRINT '>> Truncating: ' + @table_name;
        TRUNCATE TABLE bronze.ret_promotions;
        PRINT '>> Loading:    ' + @table_name;
        BULK INSERT bronze.ret_promotions
        FROM 'C:\sql\datasets\PROMOTIONS.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0d0a', TABLOCK, CODEPAGE = '65001');
        SET @rows = @@ROWCOUNT; SET @end_time = GETDATE();
        PRINT '>> Rows: ' + CAST(@rows AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's';
        INSERT INTO bronze.load_log VALUES (@proc_name,@table_name,@start_time,@end_time,DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

        PRINT '------------------------------------------------------';
        PRINT 'Section 2 of 5 — In-Store Sales (Schema 1)';
        PRINT '------------------------------------------------------';

        -- ==================== POS_TRANSACTIONS ====================
        SET @table_name = 'bronze.ret_pos_transactions';
        SET @start_time = GETDATE();
        PRINT '>> Truncating: ' + @table_name;
        TRUNCATE TABLE bronze.ret_pos_transactions;
        PRINT '>> Loading:    ' + @table_name;
        BULK INSERT bronze.ret_pos_transactions
        FROM 'C:\sql\datasets\POS_TRANSACTIONS.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0d0a', TABLOCK, CODEPAGE = '65001');
        SET @rows = @@ROWCOUNT; SET @end_time = GETDATE();
        PRINT '>> Rows: ' + CAST(@rows AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's';
        INSERT INTO bronze.load_log VALUES (@proc_name,@table_name,@start_time,@end_time,DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

        -- ==================== TRANSACTION_ITEMS ====================
        SET @table_name = 'bronze.ret_transaction_items';
        SET @start_time = GETDATE();
        PRINT '>> Truncating: ' + @table_name;
        TRUNCATE TABLE bronze.ret_transaction_items;
        PRINT '>> Loading:    ' + @table_name;
        BULK INSERT bronze.ret_transaction_items
        FROM 'C:\sql\datasets\TRANSACTION_ITEMS.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0d0a', TABLOCK, CODEPAGE = '65001');
        SET @rows = @@ROWCOUNT; SET @end_time = GETDATE();
        PRINT '>> Rows: ' + CAST(@rows AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's';
        INSERT INTO bronze.load_log VALUES (@proc_name,@table_name,@start_time,@end_time,DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

        PRINT '------------------------------------------------------';
        PRINT 'Section 3 of 5 — Online Sales (Schema 2)';
        PRINT '------------------------------------------------------';

        -- ==================== ONLINE_ORDERS ====================
        SET @table_name = 'bronze.ret_online_orders';
        SET @start_time = GETDATE();
        PRINT '>> Truncating: ' + @table_name;
        TRUNCATE TABLE bronze.ret_online_orders;
        PRINT '>> Loading:    ' + @table_name;
        BULK INSERT bronze.ret_online_orders
        FROM 'C:\sql\datasets\ONLINE_ORDERS.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0d0a', TABLOCK, CODEPAGE = '65001');
        SET @rows = @@ROWCOUNT; SET @end_time = GETDATE();
        PRINT '>> Rows: ' + CAST(@rows AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's';
        INSERT INTO bronze.load_log VALUES (@proc_name,@table_name,@start_time,@end_time,DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

        -- ==================== ONLINE_ORDER_ITEMS ====================
        SET @table_name = 'bronze.ret_online_order_items';
        SET @start_time = GETDATE();
        PRINT '>> Truncating: ' + @table_name;
        TRUNCATE TABLE bronze.ret_online_order_items;
        PRINT '>> Loading:    ' + @table_name;
        BULK INSERT bronze.ret_online_order_items
        FROM 'C:\sql\datasets\ONLINE_ORDER_ITEMS.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0d0a', TABLOCK);
        SET @rows = @@ROWCOUNT; SET @end_time = GETDATE();
        PRINT '>> Rows: ' + CAST(@rows AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's';
        INSERT INTO bronze.load_log VALUES (@proc_name,@table_name,@start_time,@end_time,DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

        -- ==================== WAREHOUSES ====================
        SET @table_name = 'bronze.ret_warehouses';
        SET @start_time = GETDATE();
        PRINT '>> Truncating: ' + @table_name;
        TRUNCATE TABLE bronze.ret_warehouses;
        PRINT '>> Loading:    ' + @table_name;
        BULK INSERT bronze.ret_warehouses
        FROM 'C:\sql\datasets\WAREHOUSES.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0d0a', TABLOCK, CODEPAGE = '65001');
        SET @rows = @@ROWCOUNT; SET @end_time = GETDATE();
        PRINT '>> Rows: ' + CAST(@rows AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's';
        INSERT INTO bronze.load_log VALUES (@proc_name,@table_name,@start_time,@end_time,DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

        -- ==================== PAYMENTS ====================
        SET @table_name = 'bronze.ret_payments';
        SET @start_time = GETDATE();
        PRINT '>> Truncating: ' + @table_name;
        TRUNCATE TABLE bronze.ret_payments;
        PRINT '>> Loading:    ' + @table_name;
        BULK INSERT bronze.ret_payments
        FROM 'C:\sql\datasets\PAYMENTS.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0d0a', TABLOCK, CODEPAGE = '65001');
        SET @rows = @@ROWCOUNT; SET @end_time = GETDATE();
        PRINT '>> Rows: ' + CAST(@rows AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's';
        INSERT INTO bronze.load_log VALUES (@proc_name,@table_name,@start_time,@end_time,DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

        PRINT '------------------------------------------------------';
        PRINT 'Section 4 of 5 — Inventory (Schema 3)';
        PRINT '------------------------------------------------------';

        -- ==================== INVENTORY ====================
        SET @table_name = 'bronze.ret_inventory';
        SET @start_time = GETDATE();
        PRINT '>> Truncating: ' + @table_name;
        TRUNCATE TABLE bronze.ret_inventory;
        PRINT '>> Loading:    ' + @table_name;
        BULK INSERT bronze.ret_inventory
        FROM 'C:\sql\datasets\INVENTORY.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0d0a', TABLOCK, CODEPAGE = '65001');
        SET @rows = @@ROWCOUNT; SET @end_time = GETDATE();
        PRINT '>> Rows: ' + CAST(@rows AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's';
        INSERT INTO bronze.load_log VALUES (@proc_name,@table_name,@start_time,@end_time,DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

        -- ==================== SUPPLIERS ====================
        SET @table_name = 'bronze.ret_suppliers';
        SET @start_time = GETDATE();
        PRINT '>> Truncating: ' + @table_name;
        TRUNCATE TABLE bronze.ret_suppliers;
        PRINT '>> Loading:    ' + @table_name;
        BULK INSERT bronze.ret_suppliers
        FROM 'C:\sql\datasets\SUPPLIERS.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0d0a', TABLOCK, CODEPAGE = '65001');
        SET @rows = @@ROWCOUNT; SET @end_time = GETDATE();
        PRINT '>> Rows: ' + CAST(@rows AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's';
        INSERT INTO bronze.load_log VALUES (@proc_name,@table_name,@start_time,@end_time,DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

        -- ==================== PRODUCT_SUPPLIERS ====================
        SET @table_name = 'bronze.ret_product_suppliers';
        SET @start_time = GETDATE();
        PRINT '>> Truncating: ' + @table_name;
        TRUNCATE TABLE bronze.ret_product_suppliers;
        PRINT '>> Loading:    ' + @table_name;
        BULK INSERT bronze.ret_product_suppliers
        FROM 'C:\sql\datasets\PRODUCT_SUPPLIERS.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0d0a', TABLOCK, CODEPAGE = '65001');
        SET @rows = @@ROWCOUNT; SET @end_time = GETDATE();
        PRINT '>> Rows: ' + CAST(@rows AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's';
        INSERT INTO bronze.load_log VALUES (@proc_name,@table_name,@start_time,@end_time,DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

        PRINT '------------------------------------------------------';
        PRINT 'Section 5 of 5 — Delivery & Logistics (Schema 4)';
        PRINT '------------------------------------------------------';

        -- ==================== DELIVERIES ====================
        SET @table_name = 'bronze.ret_deliveries';
        SET @start_time = GETDATE();
        PRINT '>> Truncating: ' + @table_name;
        TRUNCATE TABLE bronze.ret_deliveries;
        PRINT '>> Loading:    ' + @table_name;
        BULK INSERT bronze.ret_deliveries
        FROM 'C:\sql\datasets\DELIVERIES.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0d0a', TABLOCK, CODEPAGE = '65001');
        SET @rows = @@ROWCOUNT; SET @end_time = GETDATE();
        PRINT '>> Rows: ' + CAST(@rows AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's';
        INSERT INTO bronze.load_log VALUES (@proc_name,@table_name,@start_time,@end_time,DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

        -- ==================== DELIVERY_PROVIDERS ====================
        SET @table_name = 'bronze.ret_delivery_providers';
        SET @start_time = GETDATE();
        PRINT '>> Truncating: ' + @table_name;
        TRUNCATE TABLE bronze.ret_delivery_providers;
        PRINT '>> Loading:    ' + @table_name;
        BULK INSERT bronze.ret_delivery_providers
        FROM 'C:\sql\datasets\DELIVERY_PROVIDERS.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0d0a', TABLOCK, CODEPAGE = '65001');
        SET @rows = @@ROWCOUNT; SET @end_time = GETDATE();
        PRINT '>> Rows: ' + CAST(@rows AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's';
        INSERT INTO bronze.load_log VALUES (@proc_name,@table_name,@start_time,@end_time,DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

        -- ==================== DATA_QUALITY_REPORT ====================
        SET @table_name = 'bronze.ret_data_quality_report';
        SET @start_time = GETDATE();
        PRINT '>> Truncating: ' + @table_name;
        TRUNCATE TABLE bronze.ret_data_quality_report;
        PRINT '>> Loading:    ' + @table_name;
        BULK INSERT bronze.ret_data_quality_report
        FROM 'C:\sql\datasets\DATA_QUALITY_REPORT.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0d0a', TABLOCK, CODEPAGE = '65001');
        SET @rows = @@ROWCOUNT; SET @end_time = GETDATE();
        PRINT '>> Rows: ' + CAST(@rows AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 's';
        INSERT INTO bronze.load_log VALUES (@proc_name,@table_name,@start_time,@end_time,DATEDIFF(SECOND,@start_time,@end_time),@rows,'SUCCESS',NULL);

        PRINT '======================================================';
        PRINT 'Bronze Layer Load Completed Successfully';
        PRINT 'End Time: ' + CAST(GETDATE() AS NVARCHAR(50));
        PRINT '======================================================';

    END TRY

    BEGIN CATCH
        PRINT '!! ERROR OCCURRED ON TABLE: ' + ISNULL(@table_name,'UNKNOWN');
        PRINT '!! ' + ERROR_MESSAGE();

        INSERT INTO bronze.load_log
        VALUES (
            @proc_name,
            @table_name,
            @start_time,
            GETDATE(),
            NULL,
            NULL,
            'FAILED',
            ERROR_MESSAGE()
        );
    END CATCH
END;
GO

PRINT 'Procedure bronze.load_bronze created successfully.';