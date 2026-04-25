/*
===============================================================================
DDL Script: ddl_silver.sql
Purpose:    Creates all Silver layer tables with proper data types,
            cleaned structures, and derived column placeholders.
            Silver reads from Bronze and applies all transformations.
Run order:  After ddl_bronze.sql
===============================================================================
*/

USE DataWarehouse;
GO

-- ============================================================
-- DROP EXISTING SILVER TABLES
-- ============================================================
IF OBJECT_ID('silver.customers',            'U') IS NOT NULL DROP TABLE silver.customers;
IF OBJECT_ID('silver.employees',            'U') IS NOT NULL DROP TABLE silver.employees;
IF OBJECT_ID('silver.stores',               'U') IS NOT NULL DROP TABLE silver.stores;
IF OBJECT_ID('silver.registers',            'U') IS NOT NULL DROP TABLE silver.registers;
IF OBJECT_ID('silver.products',             'U') IS NOT NULL DROP TABLE silver.products;
IF OBJECT_ID('silver.brands',               'U') IS NOT NULL DROP TABLE silver.brands;
IF OBJECT_ID('silver.departments',          'U') IS NOT NULL DROP TABLE silver.departments;
IF OBJECT_ID('silver.promotions',           'U') IS NOT NULL DROP TABLE silver.promotions;
IF OBJECT_ID('silver.pos_transactions',     'U') IS NOT NULL DROP TABLE silver.pos_transactions;
IF OBJECT_ID('silver.transaction_items',    'U') IS NOT NULL DROP TABLE silver.transaction_items;
IF OBJECT_ID('silver.online_orders',        'U') IS NOT NULL DROP TABLE silver.online_orders;
IF OBJECT_ID('silver.online_order_items',   'U') IS NOT NULL DROP TABLE silver.online_order_items;
IF OBJECT_ID('silver.inventory',            'U') IS NOT NULL DROP TABLE silver.inventory;
IF OBJECT_ID('silver.suppliers',            'U') IS NOT NULL DROP TABLE silver.suppliers;
IF OBJECT_ID('silver.product_suppliers',    'U') IS NOT NULL DROP TABLE silver.product_suppliers;
IF OBJECT_ID('silver.warehouses',           'U') IS NOT NULL DROP TABLE silver.warehouses;
IF OBJECT_ID('silver.deliveries',           'U') IS NOT NULL DROP TABLE silver.deliveries;
IF OBJECT_ID('silver.delivery_providers',   'U') IS NOT NULL DROP TABLE silver.delivery_providers;
IF OBJECT_ID('silver.payments',             'U') IS NOT NULL DROP TABLE silver.payments;
IF OBJECT_ID('silver.load_log',             'U') IS NOT NULL DROP TABLE silver.load_log;
GO

-- ============================================================
-- CUSTOMERS
-- Transformations: trim whitespace, standardize Gender,
--                  standardize Loyalty_Level, derived Full_Name
-- ============================================================
CREATE TABLE silver.customers (
    Customer_ID     INT             NOT NULL,
    First_Name      NVARCHAR(100)   NOT NULL,
    Last_Name       NVARCHAR(100)   NOT NULL,
    Full_Name       NVARCHAR(200)   NOT NULL,   -- derived: First_Name + ' ' + Last_Name
    Gender          NVARCHAR(10)    NOT NULL,   -- standardized: 'Male' / 'Female' / 'Unknown'
    City            NVARCHAR(100),
    Loyalty_Level   NVARCHAR(20),               -- standardized: 'Bronze'/'Silver'/'Gold'/'Platinum'
    Email           NVARCHAR(200)
);

-- ============================================================
-- EMPLOYEES
-- Transformations: trim Name, cast Hire_Date, standardize Gender,
--                  derived Tenure_Years
-- ============================================================
CREATE TABLE silver.employees (
    Employee_ID     INT             NOT NULL,
    Name            NVARCHAR(150)   NOT NULL,
    Gender          NVARCHAR(10)    NOT NULL,
    Position        NVARCHAR(100),
    Store_ID        INT,
    Hire_Date       DATE,
    Tenure_Years    INT                         -- derived: DATEDIFF(YEAR, Hire_Date, GETDATE())
);

-- ============================================================
-- STORES
-- Transformations: trim strings, cast Opening_Date
-- ============================================================
CREATE TABLE silver.stores (
    Store_ID        INT             NOT NULL,
    Store_Name      NVARCHAR(150)   NOT NULL,
    City            NVARCHAR(100),
    State           NVARCHAR(50),
    Region          NVARCHAR(100),
    Opening_Date    DATE
);

-- ============================================================
-- REGISTERS
-- Transformations: trim strings, validate Store_ID exists
-- ============================================================
CREATE TABLE silver.registers (
    Register_ID     INT             NOT NULL,
    Store_ID        INT             NOT NULL,
    Register_Number INT
);

-- ============================================================
-- PRODUCTS
-- Transformations: trim strings, join Brand_Name + Dept_Name (denormalized),
--                  handle NULL Package_Size
-- ============================================================
CREATE TABLE silver.products (
    Product_ID      INT             NOT NULL,
    SKU             NVARCHAR(50)    NOT NULL,
    Product_Name    NVARCHAR(200)   NOT NULL,
    Brand_ID        INT,
    Brand_Name      NVARCHAR(100),              -- enriched via join to brands
    Department_ID   INT,
    Department_Name NVARCHAR(100),              -- enriched via join to departments
    Package_Size    NVARCHAR(100)
);

-- ============================================================
-- BRANDS
-- Transformations: trim Brand_Name, remove duplicates
-- ============================================================
CREATE TABLE silver.brands (
    Brand_ID    INT             NOT NULL,
    Brand_Name  NVARCHAR(100)   NOT NULL
);

-- ============================================================
-- DEPARTMENTS
-- Transformations: trim Department_Name
-- ============================================================
CREATE TABLE silver.departments (
    Department_ID   INT             NOT NULL,
    Department_Name NVARCHAR(100)   NOT NULL
);

-- ============================================================
-- PROMOTIONS
-- Transformations: trim Promo_Type, cast Start/End dates,
--                  derived Promo_Duration_Days
-- ============================================================
CREATE TABLE silver.promotions (
    Promotion_ID        INT             NOT NULL,
    Promo_Type          NVARCHAR(100)   NOT NULL,
    Discount_Percent    DECIMAL(5,2)    NOT NULL,
    Start_Date          DATE,
    End_Date            DATE,
    Promo_Duration_Days INT                         -- derived: DATEDIFF(DAY, Start_Date, End_Date)
);

-- ============================================================
-- POS_TRANSACTIONS
-- Transformations: cast Transaction_Time to DATETIME,
--                  handle NULL Customer_ID (walk-in = 0)
-- ============================================================
CREATE TABLE silver.pos_transactions (
    Transaction_ID      NVARCHAR(50)    NOT NULL,
    Store_ID            INT             NOT NULL,
    Register_ID         INT,
    Employee_ID         INT,
    Customer_ID         INT,            -- NULL allowed (anonymous transaction)
    Transaction_Time    DATETIME        NOT NULL,
    Transaction_Date    DATE            NOT NULL    -- derived: CAST(Transaction_Time AS DATE)
);

-- ============================================================
-- TRANSACTION_ITEMS  (Silver fact — Schema 1)
-- Transformations: NULL Promotion_ID → 0 (no promo),
--                  derived Sales_Amount and Discount_Amount,
--                  derived Net_Sales_Amount
-- ============================================================
CREATE TABLE silver.transaction_items (
    Line_ID             INT             NOT NULL,
    Transaction_ID      NVARCHAR(50)    NOT NULL,
    Product_ID          INT             NOT NULL,
    Promotion_ID        INT             NOT NULL,   -- 0 = no promotion
    Quantity            INT             NOT NULL,
    Unit_Price          DECIMAL(10,2)   NOT NULL,
    Sales_Amount        DECIMAL(12,2)   NOT NULL,   -- derived: Quantity * Unit_Price
    Discount_Percent    DECIMAL(5,2),               -- looked up from silver.promotions
    Discount_Amount     DECIMAL(12,2),              -- derived: Sales_Amount * (Discount_Percent/100)
    Net_Sales_Amount    DECIMAL(12,2)               -- derived: Sales_Amount - Discount_Amount
);

-- ============================================================
-- ONLINE_ORDERS
-- Transformations: cast Order_Time, trim Order_Status,
--                  standardize Order_Status values
-- ============================================================
CREATE TABLE silver.online_orders (
    Order_ID        INT             NOT NULL,
    Customer_ID     INT             NOT NULL,
    Warehouse_ID    INT             NOT NULL,
    Order_Time      DATETIME        NOT NULL,
    Order_Date      DATE            NOT NULL,   -- derived: CAST(Order_Time AS DATE)
    Order_Status    NVARCHAR(50)    NOT NULL,   -- standardized: 'Delivered'/'Pending'/'Cancelled'
    Order_Total     DECIMAL(10,2)   NOT NULL
);

-- ============================================================
-- ONLINE_ORDER_ITEMS  (Silver fact — Schema 2)
-- Transformations: same derived columns as transaction_items
-- ============================================================
CREATE TABLE silver.online_order_items (
    Order_Item_ID       INT             NOT NULL,
    Order_ID            INT             NOT NULL,
    Product_ID          INT             NOT NULL,
    Promotion_ID        INT             NOT NULL,   -- 0 = no promotion
    Quantity            INT             NOT NULL,
    Unit_Price          DECIMAL(10,2)   NOT NULL,
    Sales_Amount        DECIMAL(12,2)   NOT NULL,
    Discount_Percent    DECIMAL(5,2),
    Discount_Amount     DECIMAL(12,2),
    Net_Sales_Amount    DECIMAL(12,2)
);

-- ============================================================
-- INVENTORY
-- Transformations: cast Last_Updated, validate Stock_Level >= 0,
--                  derived Reorder_Flag (1 if Stock_Level < 50)
-- ============================================================
CREATE TABLE silver.inventory (
    Inventory_ID    INT             NOT NULL,
    Store_ID        INT             NOT NULL,
    Product_ID      INT             NOT NULL,
    Stock_Level     INT             NOT NULL,
    Last_Updated    DATETIME,
    Reorder_Flag    TINYINT         NOT NULL    -- derived: CASE WHEN Stock_Level < 50 THEN 1 ELSE 0 END
);

-- ============================================================
-- SUPPLIERS
-- Transformations: trim Supplier_Name, Country, Phone
-- ============================================================
CREATE TABLE silver.suppliers (
    Supplier_ID     INT             NOT NULL,
    Supplier_Name   NVARCHAR(150)   NOT NULL,
    Country         NVARCHAR(100),
    Phone           NVARCHAR(50)
);

-- ============================================================
-- PRODUCT_SUPPLIERS
-- Transformations: validate Supply_Price > 0
-- ============================================================
CREATE TABLE silver.product_suppliers (
    Product_ID      INT             NOT NULL,
    Supplier_ID     INT             NOT NULL,
    Supply_Price    DECIMAL(10,2)   NOT NULL
);

-- ============================================================
-- WAREHOUSES
-- Transformations: trim strings
-- ============================================================
CREATE TABLE silver.warehouses (
    Warehouse_ID    INT             NOT NULL,
    Warehouse_Name  NVARCHAR(150)   NOT NULL,
    City            NVARCHAR(100),
    State           NVARCHAR(50)
);

-- ============================================================
-- DELIVERIES
-- Transformations: cast Ship_Date and Delivery_Date,
--                  standardize Delivery_Status,
--                  derived Days_to_Ship, Days_to_Deliver,
--                  Total_Delivery_Days, On_Time_Flag (SLA = 7 days)
-- ============================================================
CREATE TABLE silver.deliveries (
    Delivery_ID             INT             NOT NULL,
    Order_ID                INT             NOT NULL,
    Provider_ID             INT             NOT NULL,
    Ship_Date               DATETIME,
    Delivery_Date           DATETIME,
    Delivery_Status         NVARCHAR(50),           -- standardized
    Days_to_Ship            INT,                    -- derived: DATEDIFF(DAY, order_time, Ship_Date)
    Days_to_Deliver         INT,                    -- derived: DATEDIFF(DAY, Ship_Date, Delivery_Date)
    Total_Delivery_Days     INT,                    -- derived: Days_to_Ship + Days_to_Deliver
    On_Time_Flag            TINYINT                 -- derived: 1 if Total_Delivery_Days <= 7
);

-- ============================================================
-- DELIVERY_PROVIDERS
-- Transformations: trim strings
-- ============================================================
CREATE TABLE silver.delivery_providers (
    Provider_ID     INT             NOT NULL,
    Provider_Name   NVARCHAR(100)   NOT NULL,
    Phone           NVARCHAR(50)
);

-- ============================================================
-- PAYMENTS
-- Transformations: cast Payment_Time, trim Payment_Method,
--                  validate Payment_Amount > 0
-- ============================================================
CREATE TABLE silver.payments (
    Payment_ID          INT             NOT NULL,
    Order_ID            INT             NOT NULL,
    Payment_Method      NVARCHAR(100)   NOT NULL,
    Payment_Amount      DECIMAL(10,2)   NOT NULL,
    Payment_Time        DATETIME
);

-- ============================================================
-- SILVER LOAD LOG
-- ============================================================
CREATE TABLE silver.load_log (
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

PRINT 'All Silver tables created successfully.';