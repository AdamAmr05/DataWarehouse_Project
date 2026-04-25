/*
===============================================================================
DDL Script: ddl_bronze.sql
Purpose:    Drops and recreates all Bronze staging tables and the load log.
            Tables mirror source CSV structure exactly — no transformations.
Run order:  1 of 4  (after init_database.sql)
===============================================================================
*/

USE DataWarehouse;
GO

-- ============================================================
-- DROP EXISTING BRONZE TABLES
-- ============================================================

IF OBJECT_ID('bronze.ret_transaction_items',    'U') IS NOT NULL DROP TABLE bronze.ret_transaction_items;
IF OBJECT_ID('bronze.ret_pos_transactions',     'U') IS NOT NULL DROP TABLE bronze.ret_pos_transactions;
IF OBJECT_ID('bronze.ret_customers',            'U') IS NOT NULL DROP TABLE bronze.ret_customers;
IF OBJECT_ID('bronze.ret_employees',            'U') IS NOT NULL DROP TABLE bronze.ret_employees;
IF OBJECT_ID('bronze.ret_stores',               'U') IS NOT NULL DROP TABLE bronze.ret_stores;
IF OBJECT_ID('bronze.ret_registers',            'U') IS NOT NULL DROP TABLE bronze.ret_registers;
IF OBJECT_ID('bronze.ret_products',             'U') IS NOT NULL DROP TABLE bronze.ret_products;
IF OBJECT_ID('bronze.ret_brands',               'U') IS NOT NULL DROP TABLE bronze.ret_brands;
IF OBJECT_ID('bronze.ret_departments',          'U') IS NOT NULL DROP TABLE bronze.ret_departments;
IF OBJECT_ID('bronze.ret_promotions',           'U') IS NOT NULL DROP TABLE bronze.ret_promotions;
IF OBJECT_ID('bronze.ret_online_order_items',   'U') IS NOT NULL DROP TABLE bronze.ret_online_order_items;
IF OBJECT_ID('bronze.ret_online_orders',        'U') IS NOT NULL DROP TABLE bronze.ret_online_orders;
IF OBJECT_ID('bronze.ret_inventory',            'U') IS NOT NULL DROP TABLE bronze.ret_inventory;
IF OBJECT_ID('bronze.ret_suppliers',            'U') IS NOT NULL DROP TABLE bronze.ret_suppliers;
IF OBJECT_ID('bronze.ret_product_suppliers',    'U') IS NOT NULL DROP TABLE bronze.ret_product_suppliers;
IF OBJECT_ID('bronze.ret_warehouses',           'U') IS NOT NULL DROP TABLE bronze.ret_warehouses;
IF OBJECT_ID('bronze.ret_deliveries',           'U') IS NOT NULL DROP TABLE bronze.ret_deliveries;
IF OBJECT_ID('bronze.ret_delivery_providers',   'U') IS NOT NULL DROP TABLE bronze.ret_delivery_providers;
IF OBJECT_ID('bronze.ret_payments',             'U') IS NOT NULL DROP TABLE bronze.ret_payments;
IF OBJECT_ID('bronze.ret_data_quality_report',  'U') IS NOT NULL DROP TABLE bronze.ret_data_quality_report;
IF OBJECT_ID('bronze.load_log',                 'U') IS NOT NULL DROP TABLE bronze.load_log;
GO

-- ============================================================
-- TRANSACTION_ITEMS  (main fact — Schema 1)
-- Line_ID, Transaction_ID, Product_ID, Promotion_ID, Quantity, Unit_Price
-- ============================================================
CREATE TABLE bronze.ret_transaction_items (
    Line_ID         INT,
    Transaction_ID  NVARCHAR(50),
    Product_ID      INT,
    Promotion_ID    INT,            -- nullable: row 3 sample had no promo
    Quantity        INT,
    Unit_Price      DECIMAL(10,2)
);

-- ============================================================
-- POS_TRANSACTIONS  (header for Schema 1)
-- Transaction_ID, Store_ID, Register_ID, Employee_ID, Customer_ID, Transaction_Time
-- ============================================================
CREATE TABLE bronze.ret_pos_transactions (
    Transaction_ID      NVARCHAR(50),
    Store_ID            INT,
    Register_ID         INT,
    Employee_ID         INT,
    Customer_ID         INT,
    Transaction_Time    NVARCHAR(50)   -- loaded as string; cast in Silver
);

-- ============================================================
-- CUSTOMERS
-- Customer_ID, First_Name, Last_Name, Gender, City, Loyalty_Level, Email
-- ============================================================
CREATE TABLE bronze.ret_customers (
    Customer_ID     INT,
    First_Name      NVARCHAR(100),
    Last_Name       NVARCHAR(100),
    Gender          NVARCHAR(20),
    City            NVARCHAR(100),
    Loyalty_Level   NVARCHAR(50),
    Email           NVARCHAR(200)
);

-- ============================================================
-- EMPLOYEES
-- Employee_ID, Name, Gender, Position, Store_ID, Hire_Date
-- ============================================================
CREATE TABLE bronze.ret_employees (
    Employee_ID INT,
    Name        NVARCHAR(150),
    Gender      NVARCHAR(20),
    Position    NVARCHAR(100),
    Store_ID    INT,
    Hire_Date   NVARCHAR(50)    -- loaded as string; cast in Silver
);

-- ============================================================
-- STORES
-- Store_ID, Store_Name, City, State, Region, Opening_Date
-- ============================================================
CREATE TABLE bronze.ret_stores (
    Store_ID        INT,
    Store_Name      NVARCHAR(150),
    City            NVARCHAR(100),
    State           NVARCHAR(50),
    Region          NVARCHAR(100),
    Opening_Date    NVARCHAR(50)
);

-- ============================================================
-- REGISTERS
-- Register_ID, Store_ID, Register_Name/Label  (actual columns TBC from file)
-- Note: source file appeared identical to STORES in preview — adjust if needed
-- ============================================================
CREATE TABLE bronze.ret_registers (
    Register_ID     INT,
    Store_ID        INT,
    Register_Number  INT
);

-- ============================================================
-- PRODUCTS
-- Product_ID, SKU, Product_Name, Brand_ID, Department_ID, Package_Size
-- ============================================================
CREATE TABLE bronze.ret_products (
    Product_ID      INT,
    SKU             NVARCHAR(50),
    Product_Name    NVARCHAR(200),
    Brand_ID        INT,
    Department_ID   INT,
    Package_Size    NVARCHAR(100)
);

-- ============================================================
-- BRANDS
-- Brand_ID, Brand_Name
-- ============================================================
CREATE TABLE bronze.ret_brands (
    Brand_ID    INT,
    Brand_Name  NVARCHAR(100)
);

-- ============================================================
-- DEPARTMENTS
-- Department_ID, Department_Name
-- ============================================================
CREATE TABLE bronze.ret_departments (
    Department_ID   INT,
    Department_Name NVARCHAR(100)
);

-- ============================================================
-- PROMOTIONS
-- Promotion_ID, Promo_Type, Discount_Percent, Start_Date, End_Date
-- ============================================================
CREATE TABLE bronze.ret_promotions (
    Promotion_ID        INT,
    Promo_Type          NVARCHAR(100),
    Discount_Percent    DECIMAL(5,2),
    Start_Date          NVARCHAR(50),
    End_Date            NVARCHAR(50)
);

-- ============================================================
-- ONLINE_ORDER_ITEMS  (main fact — Schema 2)
-- Order_Item_ID, Order_ID, Product_ID, Promotion_ID, Quantity, Unit_Price
-- ============================================================
CREATE TABLE bronze.ret_online_order_items (
    Order_Item_ID   INT,
    Order_ID        INT,
    Product_ID      INT,
    Promotion_ID    INT,
    Quantity        INT,
    Unit_Price      DECIMAL(10,2)
);

-- ============================================================
-- ONLINE_ORDERS
-- Order_ID, Customer_ID, Warehouse_ID, Order_Time, Order_Status, Order_Total
-- ============================================================
CREATE TABLE bronze.ret_online_orders (
    Order_ID        INT,
    Customer_ID     INT,
    Warehouse_ID    INT,
    Order_Time      NVARCHAR(50),
    Order_Status    NVARCHAR(50),
    Order_Total     DECIMAL(10,2)
);

-- ============================================================
-- INVENTORY
-- Inventory_ID, Store_ID, Product_ID, Stock_Level, Last_Updated
-- ============================================================
CREATE TABLE bronze.ret_inventory (
    Inventory_ID    INT,
    Store_ID        INT,
    Product_ID      INT,
    Stock_Level     INT,
    Last_Updated    NVARCHAR(50)
);

-- ============================================================
-- SUPPLIERS
-- Supplier_ID, Supplier_Name, Country, Phone
-- ============================================================
CREATE TABLE bronze.ret_suppliers (
    Supplier_ID     INT,
    Supplier_Name   NVARCHAR(150),
    Country         NVARCHAR(100),
    Phone           NVARCHAR(50)
);

-- ============================================================
-- PRODUCT_SUPPLIERS
-- Product_ID, Supplier_ID, Supply_Price
-- ============================================================
CREATE TABLE bronze.ret_product_suppliers (
    Product_ID      INT,
    Supplier_ID     INT,
    Supply_Price    DECIMAL(10,2)
);

-- ============================================================
-- WAREHOUSES
-- Warehouse_ID, Warehouse_Name, City, State
-- ============================================================
CREATE TABLE bronze.ret_warehouses (
    Warehouse_ID    INT,
    Warehouse_Name  NVARCHAR(150),
    City            NVARCHAR(100),
    State           NVARCHAR(50)
);

-- ============================================================
-- DELIVERIES
-- Delivery_ID, Order_ID, Provider_ID, Ship_Date, Delivery_Date, Delivery_Status
-- ============================================================
CREATE TABLE bronze.ret_deliveries (
    Delivery_ID         INT,
    Order_ID            INT,
    Provider_ID         INT,
    Ship_Date           NVARCHAR(50),
    Delivery_Date       NVARCHAR(50),
    Delivery_Status     NVARCHAR(50)
);

-- ============================================================
-- DELIVERY_PROVIDERS
-- Provider_ID, Provider_Name, Phone
-- ============================================================
CREATE TABLE bronze.ret_delivery_providers (
    Provider_ID     INT,
    Provider_Name   NVARCHAR(100),
    Phone           NVARCHAR(50)
);

-- ============================================================
-- PAYMENTS
-- Payment_ID, Order_ID, Payment_Method, Payment_Amount, Payment_Time
-- ============================================================
CREATE TABLE bronze.ret_payments (
    Payment_ID          INT,
    Order_ID            INT,
    Payment_Method      NVARCHAR(100),
    Payment_Amount      DECIMAL(10,2),
    Payment_Time        NVARCHAR(50)
);

-- ============================================================
-- DATA_QUALITY_REPORT  (reference / metadata table)
-- Table_Name, Row_Count
-- ============================================================
CREATE TABLE bronze.ret_data_quality_report (
    Table_Name  NVARCHAR(100),
    Row_Count   INT
);

-- ============================================================
-- LOAD LOG TABLE
-- ============================================================
CREATE TABLE bronze.load_log (
    log_id              INT IDENTITY(1,1) PRIMARY KEY,
    procedure_name      NVARCHAR(200),
    table_name          NVARCHAR(200),
    load_start_time     DATETIME,
    load_end_time       DATETIME,
    duration_seconds    INT,
    rows_inserted       INT,
    status              NVARCHAR(20),
    error_message       NVARCHAR(MAX)
);
GO

PRINT 'All Bronze tables created successfully.';