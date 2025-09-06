/*
===========================================================
 Procedure: silver.load_silver
 Purpose  : Load and transform data from Bronze → Silver
===========================================================

 1. Truncates silver tables before loading.
 2. Cleans and standardizes data:
      - Trims text values.
      - Normalizes gender & marital status.
      - Validates dates, sets invalid ones to NULL.
      - Standardizes country codes & product lines.
      - Recalculates sales/price if missing or invalid.
 3. Ensures only the latest customer records are kept.
 4. Derives product keys, categories, and end dates.
 5. Logs start/end time for each table load.
 6. Catches errors and prints error messages.

 Result : Curated, consistent, and analysis-ready data
          stored in the Silver layer.
===========================================================
*/
/
  USE CASE
  EXEC silver.load_silver
/
  
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
BEGIN TRY
    DECLARE @start_time DATETIME, @end_time DATETIME,@startb_time DATETIME,@endb_time DATETIME
    SET @startb_time = GETDATE();
    PRINT '============================================';
	PRINT 'Loading the silver layer';
	PRINT '============================================';

	PRINT '---------------------------------------------';
	PRINT 'Loading CRM tables';
	PRINT'----------------------------------------------';
    
    SET @start_time = GETDATE()
    PRINT '>> Truncating Table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;
    PRINT '>> Inserting Data Into: silver.crm_cust_info';
    INSERT INTO silver.crm_cust_info(
	    cst_id ,
	    cst_key,
	    cst_firstname,
	    cst_lastname,
	    cst_marital_status,
	    cst_gndr ,
	    cst_create_date
    )

    SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
         WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
         ELSE 'n/a'
    END cst_marital_status,
    CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'MALE'
         WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'FEMALE'
         ELSE 'n/a'
    END cst_gndr,
    cst_create_date

    FROM
    (
    SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
    FROM bronze.crm_cust_info
    )t
    WHERE flag_last = 1;
    SET @end_time = GETDATE();
    PRINT 'Load time: ' + CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + 'seconds'

    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;
    PRINT '>> Inserting Data Into: silver.crm_prd_info';
    INSERT INTO silver.crm_prd_info(
	    prd_id,
	    cat_id,
	    prd_key,
	    prd_nm,
	    prd_cost,
	    prd_line,
	    prd_start_dt,
	    prd_end_dt
    )
    SELECT
    prd_id,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
    SUBSTRING(prd_key,7,len(prd_key)) AS prd_key,
    prd_nm,
    ISNULL(prd_cost,0) AS prd_cost,
    CASE UPPER(TRIM(prd_line))
         WHEN 'M' THEN 'Mountain'
         WHEN 'R' THEN 'Road'
         WHEN 'S' THEN 'Other Sales'
         WHEN 'T' THEN 'Touring'
         ELSE 'n/a'
    END prd_line,
    CAST(prd_start_dt as DATE) prd_start_dt,
    CAST(LEAD(prd_start_dt) OVER(Partition By prd_key Order by prd_start_dt)-1 as DATE) as prd_end_dt
    FROM bronze.crm_prd_info
    SET @end_time = GETDATE();
    PRINT 'Load Time: ' + CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + 'seconds'

    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;
    PRINT '>> Inserting Data Into: silver.crm_sales_details';
    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt ,
	    sls_ship_dt,
	    sls_due_dt,
	    sls_sales,
	    sls_quantity,
	    sls_price
    )

    SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt <= 0 or len(sls_order_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END sls_order_dt,
    CASE WHEN sls_ship_dt <= 0 or len(sls_ship_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
         END sls_ship_dt,
    CASE WHEN sls_due_dt <= 0 or len(sls_due_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
         END sls_due_dt,
    CASE WHEN sls_sales IS NULL OR sls_sales <=0 or sls_sales != ABS(sls_price) * (sls_quantity) 
         THEN ABS(sls_price) * (sls_quantity)
         ELSE sls_sales
    END sls_sales,
    sls_quantity,
    CASE WHEN sls_price IS NULL OR sls_price <= 0 
         THEN sls_sales / NULLIF(sls_quantity,0)
         ELSE sls_price
    END sls_price
    FROM bronze.crm_sales_details
    SET @end_time = GETDATE();
    
    PRINT 'Load Time: ' + CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + 'seconds'
    PRINT '---------------------------------------------';
	PRINT 'Loading ERP tables';
	PRINT'----------------------------------------------';
    
    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: silver.erp_LOC_A101';
    TRUNCATE TABLE silver.erp_LOC_A101;
    PRINT '>> Inserting Data Into: silver.erp_LOC_A101';
    INSERT INTO silver.erp_LOC_A101(
    CID,
    cntry)

    SELECT 
    REPLACE(CID,'-','') as CID,
    CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
         WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
         WHEN TRIM(cntry) = ' ' or cntry IS NULL THEN 'n/a'
         ELSE TRIM(cntry)
    END cntry
    FROM bronze.erp_LOC_A101
    SET @end_time = GETDATE();
    PRINT 'Load Time: ' + CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + 'seconds'


    SET @start_time = GETDATE();
    PRINT '>> Truncating Table:  silver.erp_CUST_AZ12';
    TRUNCATE TABLE  silver.erp_CUST_AZ12;
    PRINT '>> Inserting Data Into:  silver.erp_CUST_AZ12';
    INSERT INTO silver.erp_CUST_AZ12(
        CID,
        BDATE,
        GEN
    )
    SELECT 
    CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
         ELSE CID
    END CID,
    CASE WHEN BDATE > GETDATE() THEN NULL
         ELSE BDATE
    END BDATE,
    CASE WHEN UPPER(TRIM(GEN)) IN ('F','Female') THEN 'Female'
         WHEN UPPER(TRIM(GEN)) IN ( 'M','Male') THEN 'Male'
         ELSE 'n/a'
    END GEN
    FROM bronze.erp_CUST_AZ12
     SET @end_time = GETDATE();
    PRINT 'Load Time: ' + CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + 'seconds'

    
    SET @start_time = GETDATE()
    PRINT '>> Truncating Table: silver.erp_PX_CAT_G1V2';
    TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
    PRINT '>> Inserting Data Into: silver.erp_PX_CAT_G1V2';
    INSERT INTO silver.erp_PX_CAT_G1V2(
	    ID,
	    CAT,
	    SUBCAT,
	    MAINTENANCE
    )
    SELECT
    ID,
    CAT,
    SUBCAT,
    MAINTENANCE
    FROM bronze.erp_PX_CAT_G1V2
    SET @end_time = GETDATE();
    PRINT 'Load Time: ' + CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + 'seconds'
 PRINT '============================================';
 PRINT 'The silver layer has been loaded';
 PRINT '============================================';
 SET @endb_time = GETDATE(); 
 PRINT 'The entire Loading time of the table is:' + CAST(DATEDIFF(second,startb_date,endb_date) as NVARCHAR) + 'seconds'
   END TRY
    
BEGIN CATCH
PRINT '========================================================='
PRINT 'Error occured during bronze loading'
PRINT 'Error Message' + ERROR_MESSAGE();
PRINT '========================================================='
END CATCH
 
END


