/*
======================================
Stored procedure: Load Bronze Layer (Source-> Bronze)
======================================
Script Purpose:
  This stored procedure loads data form crm & erp source to bronze schema.
  It performs the following action:
  - Truncate the bronze table before loading the data
  - Use BULK INSERT command to load data from csv files to bronze table.
Parameter:
this stored procedure does not accept any parameter or retur any value 

Usage example:
EXEC bronze.load_bronze;
=======================================================
*/


USE [DataWarehouse]
GO

/****** Object:  StoredProcedure [bronze].[load_bronze]    Script Date: 15-03-2026 20:09:08 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [bronze].[load_bronze]
AS
BEGIN
    DECLARE @start_time AS DATETIME, @end_time AS DATETIME, @batch_start_time AS DATETIME, @batch_end_time AS DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '============================';
        PRINT 'Loading Bronze Layer';
        PRINT '============================';
        PRINT '------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '------------------------';
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
        PRINT '>> Tnserting Table bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info FROM 'C:\Users\kshitij\Downloads\datasets\source_crm\cust_info.csv'
            WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> lOAD DURATION: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
        PRINT '-----------------------';
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
        PRINT '>> Inserting Table bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info FROM 'C:\Users\kshitij\Downloads\datasets\source_crm\prd_info.csv'
            WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT 'Load Time: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
        PRINT '------------';
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT '>> Inserting Table bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details FROM 'C:\Users\kshitij\Downloads\datasets\source_crm\sales_details.csv'
            WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT 'Load Time: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
        PRINT '------------';
        PRINT '------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '------------------------';
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;
        PRINT '>> Inserting Table bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12 FROM 'C:\Users\kshitij\Downloads\datasets\source_erp\cust_az12.csv'
            WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT 'Load Time: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
        PRINT '------------';
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT '>> Inserting Table bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101 FROM 'C:\Users\kshitij\Downloads\datasets\source_erp\loc_a101.csv'
            WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT 'Load Time: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
        PRINT '------------';
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        PRINT '>> Inserting Table bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2 FROM 'C:\Users\kshitij\Downloads\datasets\source_erp\px_cat_g1v2.csv'
            WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @end_time = GETDATE();
        PRINT 'Load Time: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
        PRINT '------------';
        SET @batch_end_time = GETDATE();
        PRINT 'Load Time: ' + CAST (DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
        PRINT '------------';
    END TRY
    BEGIN CATCH
        PRINT '================';
        PRINT 'Error Occured during Loading BRONZE Layer';
        PRINT 'Error Message' + ERROR_MESSAGE();
        PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
        PRINT '==============';
    END CATCH
END

GO


