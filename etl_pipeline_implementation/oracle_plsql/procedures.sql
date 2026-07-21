-- ============================================
-- ORACLE PL/SQL PROCEDURES
-- Data Warehouse Procedures
-- ============================================

-- ============================================
-- PROCEDURE 1: Load Fact Sales
-- ============================================

CREATE OR REPLACE PROCEDURE SP_LOAD_FACT_SALES (
    p_batch_id IN NUMBER,
    p_date_key IN NUMBER,
    p_source_system IN VARCHAR2 DEFAULT 'SPARK'
) AS
    v_records_processed NUMBER := 0;
    v_records_failed NUMBER := 0;
    v_error_message VARCHAR2(4000);
BEGIN
    -- Insert new records from staging to fact table
    INSERT INTO FACT_SALES (
        date_key,
        product_key,
        customer_key,
        store_key,
        promotion_key,
        quantity_sold,
        unit_price,
        gross_amount,
        discount_amount,
        net_amount,
        cost_amount,
        profit_amount,
        source_system,
        batch_id,
        created_at,
        updated_at
    )
    SELECT 
        s.date_key,
        s.product_key,
        s.customer_key,
        s.store_key,
        s.promotion_key,
        s.quantity_sold,
        s.unit_price,
        s.gross_amount,
        s.discount_amount,
        s.net_amount,
        s.cost_amount,
        s.profit_amount,
        p_source_system,
        p_batch_id,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    FROM STG_FACT_SALES s
    WHERE s.batch_id = p_batch_id
      AND s.date_key = p_date_key
      AND NOT EXISTS (
          SELECT 1 FROM FACT_SALES f 
          WHERE f.date_key = s.date_key 
            AND f.product_key = s.product_key 
            AND f.customer_key = s.customer_key 
            AND f.store_key = s.store_key
      );
    
    v_records_processed := SQL%ROWCOUNT;
    
    -- Log errors if any
    IF v_records_failed > 0 THEN
        v_error_message := 'Failed to load ' || v_records_failed || ' records';
        INSERT INTO METADATA_DATA_QUALITY (
            table_name,
            check_name,
            check_type,
            expected_value,
            actual_value,
            status,
            run_id
        ) VALUES (
            'FACT_SALES',
            'Load Validation',
            'Row Count',
            v_records_processed + v_records_failed,
            v_records_processed,
            CASE WHEN v_records_failed = 0 THEN 'PASSED' ELSE 'FAILED' END,
            (SELECT MAX(run_id) FROM METADATA_PIPELINE_RUN WHERE pipeline_name = 'daily_etl_pipeline')
        );
    END IF;
    
    COMMIT;
    
    -- Output results
    DBMS_OUTPUT.PUT_LINE('Records processed: ' || v_records_processed);
    DBMS_OUTPUT.PUT_LINE('Records failed: ' || v_records_failed);
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        v_error_message := SQLERRM;
        DBMS_OUTPUT.PUT_LINE('Error: ' || v_error_message);
        RAISE;
END SP_LOAD_FACT_SALES;
/


-- ============================================
-- PROCEDURE 2: Update Dimension with SCD Type 2
-- ============================================

CREATE OR REPLACE PROCEDURE SP_UPDATE_DIM_CUSTOMER_SCD2 (
    p_batch_id IN NUMBER
) AS
    CURSOR c_changed_records IS
        SELECT 
            s.customer_key,
            s.customer_id,
            s.customer_name,
            s.email,
            s.phone,
            s.gender,
            s.birth_date,
            s.age_group,
            s.address,
            s.city,
            s.state_province,
            s.country,
            s.postal_code,
            s.region_key,
            s.segment,
            s.customer_type,
            s.loyalty_tier,
            s.is_active,
            s.effective_date
        FROM STG_DIM_CUSTOMER s
        INNER JOIN DIM_CUSTOMER d 
            ON s.customer_id = d.customer_id 
            AND d.is_current = 'Y'
        WHERE s.batch_id = p_batch_id
          AND (
              s.customer_name != d.customer_name
              OR NVL(s.email, 'NULL') != NVL(d.email, 'NULL')
              OR NVL(s.phone, 'NULL') != NVL(d.phone, 'NULL')
              OR NVL(s.segment, 'NULL') != NVL(d.segment, 'NULL')
              OR NVL(s.address, 'NULL') != NVL(d.address, 'NULL')
          );
    
    v_count NUMBER := 0;
BEGIN
    -- Expire old records
    FOR rec IN c_changed_records LOOP
        UPDATE DIM_CUSTOMER
        SET 
            expiry_date = CURRENT_DATE,
            is_current = 'N',
            updated_at = CURRENT_TIMESTAMP
        WHERE customer_id = rec.customer_id
          AND is_current = 'Y';
        
        -- Insert new record
        INSERT INTO DIM_CUSTOMER (
            customer_id,
            customer_name,
            email,
            phone,
            gender,
            birth_date,
            age_group,
            address,
            city,
            state_province,
            country,
            postal_code,
            region_key,
            segment,
            customer_type,
            loyalty_tier,
            is_active,
            effective_date,
            expiry_date,
            is_current,
            created_at,
            updated_at
        ) VALUES (
            rec.customer_id,
            rec.customer_name,
            rec.email,
            rec.phone,
            rec.gender,
            rec.birth_date,
            rec.age_group,
            rec.address,
            rec.city,
            rec.state_province,
            rec.country,
            rec.postal_code,
            rec.region_key,
            rec.segment,
            rec.customer_type,
            rec.loyalty_tier,
            rec.is_active,
            CURRENT_DATE,
            TO_DATE('9999-12-31', 'YYYY-MM-DD'),
            'Y',
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        );
        
        v_count := v_count + 1;
    END LOOP;
    
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Updated ' || v_count || ' customer records with SCD Type 2');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
        RAISE;
END SP_UPDATE_DIM_CUSTOMER_SCD2;
/


-- ============================================
-- PROCEDURE 3: Calculate Sales Aggregations
-- ============================================

CREATE OR REPLACE PROCEDURE SP_CALCULATE_SALES_AGGREGATIONS (
    p_date_key IN NUMBER
) AS
BEGIN
    -- Calculate daily sales summary
    INSERT INTO AGG_DAILY_SALES_SUMMARY (
        date_key,
        total_transactions,
        total_quantity,
        total_gross_amount,
        total_discount,
        total_net_amount,
        total_cost,
        total_profit,
        avg_transaction_value,
        unique_customers,
        unique_products,
        created_at
    )
    SELECT 
        f.date_key,
        COUNT(DISTINCT f.sales_key) as total_transactions,
        SUM(f.quantity_sold) as total_quantity,
        SUM(f.gross_amount) as total_gross_amount,
        SUM(f.discount_amount) as total_discount,
        SUM(f.net_amount) as total_net_amount,
        SUM(f.cost_amount) as total_cost,
        SUM(f.profit_amount) as total_profit,
        AVG(f.net_amount) as avg_transaction_value,
        COUNT(DISTINCT f.customer_key) as unique_customers,
        COUNT(DISTINCT f.product_key) as unique_products,
        CURRENT_TIMESTAMP
    FROM FACT_SALES f
    WHERE f.date_key = p_date_key
    GROUP BY f.date_key;
    
    -- Calculate monthly sales summary
    INSERT INTO AGG_MONTHLY_SALES_SUMMARY (
        year,
        month_number,
        month_name,
        total_transactions,
        total_quantity,
        total_gross_amount,
        total_discount,
        total_net_amount,
        total_cost,
        total_profit,
        avg_transaction_value,
        unique_customers,
        created_at
    )
    SELECT 
        d.year,
        d.month_number,
        d.month_name,
        COUNT(DISTINCT f.sales_key) as total_transactions,
        SUM(f.quantity_sold) as total_quantity,
        SUM(f.gross_amount) as total_gross_amount,
        SUM(f.discount_amount) as total_discount,
        SUM(f.net_amount) as total_net_amount,
        SUM(f.cost_amount) as total_cost,
        SUM(f.profit_amount) as total_profit,
        AVG(f.net_amount) as avg_transaction_value,
        COUNT(DISTINCT f.customer_key) as unique_customers,
        CURRENT_TIMESTAMP
    FROM FACT_SALES f
    JOIN DIM_DATE d ON f.date_key = d.date_key
    WHERE d.year = (SELECT year FROM DIM_DATE WHERE date_key = p_date_key)
      AND d.month_number = (SELECT month_number FROM DIM_DATE WHERE date_key = p_date_key)
    GROUP BY d.year, d.month_number, d.month_name;
    
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Sales aggregations calculated for date_key: ' || p_date_key);
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
        RAISE;
END SP_CALCULATE_SALES_AGGREGATIONS;
/


-- ============================================
-- PROCEDURE 4: Data Quality Validation
-- ============================================

CREATE OR REPLACE PROCEDURE SP_VALIDATE_DATA_QUALITY (
    p_pipeline_name IN VARCHAR2,
    p_run_id IN NUMBER
) AS
    v_check_id NUMBER;
BEGIN
    -- Check 1: Null values in critical columns
    FOR rec IN (
        SELECT 'FACT_SALES' as table_name, 'NULL_DATE_KEY' as check_name, 
               COUNT(*) as null_count
        FROM FACT_SALES
        WHERE date_key IS NULL
        
        UNION ALL
        
        SELECT 'FACT_SALES', 'NULL_PRODUCT_KEY', COUNT(*)
        FROM FACT_SALES
        WHERE product_key IS NULL
        
        UNION ALL
        
        SELECT 'FACT_SALES', 'NULL_CUSTOMER_KEY', COUNT(*)
        FROM FACT_SALES
        WHERE customer_key IS NULL
        
        UNION ALL
        
        SELECT 'DIM_CUSTOMER', 'NULL_CUSTOMER_ID', COUNT(*)
        FROM DIM_CUSTOMER
        WHERE customer_id IS NULL AND is_current = 'Y'
    ) LOOP
        INSERT INTO METADATA_DATA_QUALITY (
            table_name,
            check_name,
            check_type,
            expected_value,
            actual_value,
            status,
            run_id
        ) VALUES (
            rec.table_name,
            rec.check_name,
            'NULL_CHECK',
            0,
            rec.null_count,
            CASE WHEN rec.null_count = 0 THEN 'PASSED' ELSE 'FAILED' END,
            p_run_id
        );
    END LOOP;
    
    -- Check 2: Referential integrity
    FOR rec IN (
        SELECT 'FACT_SALES_DATE_FK' as check_name, COUNT(*) as orphan_count
        FROM FACT_SALES f
        LEFT JOIN DIM_DATE d ON f.date_key = d.date_key
        WHERE d.date_key IS NULL
        
        UNION ALL
        
        SELECT 'FACT_SALES_PRODUCT_FK', COUNT(*)
        FROM FACT_SALES f
        LEFT JOIN DIM_PRODUCT p ON f.product_key = p.product_key
        WHERE p.product_key IS NULL
    ) LOOP
        INSERT INTO METADATA_DATA_QUALITY (
            table_name,
            check_name,
            check_type,
            expected_value,
            actual_value,
            status,
            run_id
        ) VALUES (
            'FACT_SALES',
            rec.check_name,
            'REFERENTIAL_INTEGRITY',
            0,
            rec.orphan_count,
            CASE WHEN rec.orphan_count = 0 THEN 'PASSED' ELSE 'FAILED' END,
            p_run_id
        );
    END LOOP;
    
    -- Check 3: Data freshness
    INSERT INTO METADATA_DATA_QUALITY (
        table_name,
        check_name,
        check_type,
        expected_value,
        actual_value,
        status,
        run_id
    ) VALUES (
        'FACT_SALES',
        'DATA_FRESHNESS',
        'DATE_CHECK',
        1,
        (SELECT COUNT(*) FROM FACT_SALES 
         WHERE date_key = TO_NUMBER(TO_CHAR(SYSDATE - 1, 'YYYYMMDD'))),
        CASE WHEN (SELECT COUNT(*) FROM FACT_SALES 
                   WHERE date_key = TO_NUMBER(TO_CHAR(SYSDATE - 1, 'YYYYMMDD'))) > 0 
             THEN 'PASSED' ELSE 'WARNING' END,
        p_run_id
    );
    
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Data quality validation completed');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
        RAISE;
END SP_VALIDATE_DATA_QUALITY;
/


-- ============================================
-- PROCEDURE 5: Refresh All Materialized Views
-- ============================================

CREATE OR REPLACE PROCEDURE SP_REFRESH_ALL_MATERIALIZED_VIEWS AS
BEGIN
    -- Refresh sales monthly summary
    DBMS_MVIEW.REFRESH('MV_SALES_MONTHLY_SUMMARY', 'C');
    
    -- Refresh customer analytics
    DBMS_MVIEW.REFRESH('MV_CUSTOMER_ANALYTICS', 'C');
    
    -- Refresh daily sales summary (if exists)
    BEGIN
        DBMS_MVIEW.REFRESH('AGG_DAILY_SALES_SUMMARY', 'C');
    EXCEPTION
        WHEN OTHERS THEN NULL;
    END;
    
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('All materialized views refreshed successfully');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error refreshing materialized views: ' || SQLERRM);
        RAISE;
END SP_REFRESH_ALL_MATERIALIZED_VIEWS;
/


-- ============================================
-- PROCEDURE 6: Clean Up Old Data
-- ============================================

CREATE OR REPLACE PROCEDURE SP_CLEANUP_OLD_DATA (
    p_retention_days IN NUMBER DEFAULT 365
) AS
    v_cutoff_date NUMBER;
BEGIN
    -- Calculate cutoff date
    v_cutoff_date := TO_NUMBER(TO_CHAR(SYSDATE - p_retention_days, 'YYYYMMDD'));
    
    -- Archive old fact data before deleting
    INSERT INTO FACT_SALES_ARCHIVE
    SELECT * FROM FACT_SALES
    WHERE date_key < v_cutoff_date;
    
    -- Delete old fact data
    DELETE FROM FACT_SALES
    WHERE date_key < v_cutoff_date;
    
    -- Delete old metadata
    DELETE FROM METADATA_PIPELINE_RUN
    WHERE run_date < SYSDATE - p_retention_days;
    
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('Cleaned up data older than ' || p_retention_days || ' days');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error cleaning up old data: ' || SQLERRM);
        RAISE;
END SP_CLEANUP_OLD_DATA;
/


-- ============================================
-- PROCEDURE 7: Generate Data Lineage
-- ============================================

CREATE OR REPLACE PROCEDURE SP_GENERATE_DATA_LINEAGE (
    p_pipeline_name IN VARCHAR2,
    p_run_id IN NUMBER,
    p_source_table IN VARCHAR2,
    p_target_table IN VARCHAR2,
    p_transformation IN VARCHAR2
) AS
BEGIN
    INSERT INTO METADATA_DATA_LINEAGE (
        source_table,
        target_table,
        transformation,
        pipeline_name,
        run_id,
        created_at
    ) VALUES (
        p_source_table,
        p_target_table,
        p_transformation,
        p_pipeline_name,
        p_run_id,
        CURRENT_TIMESTAMP
    );
    
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Data lineage recorded');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error generating data lineage: ' || SQLERRM);
        RAISE;
END SP_GENERATE_DATA_LINEAGE;
/


-- ============================================
-- PROCEDURE 8: Rebuild Indexes
-- ============================================

CREATE OR REPLACE PROCEDURE SP_REBUILD_INDEXES AS
BEGIN
    FOR idx IN (
        SELECT index_name, table_name, tablespace_name
        FROM user_indexes
        WHERE table_name IN (
            'FACT_SALES', 'FACT_REVENUE', 'DIM_CUSTOMER', 
            'DIM_PRODUCT', 'DIM_STORE', 'DIM_DATE'
        )
    ) LOOP
        BEGIN
            EXECUTE IMMEDIATE 'ALTER INDEX ' || idx.index_name || ' REBUILD';
            DBMS_OUTPUT.PUT_LINE('Rebuilt index: ' || idx.index_name);
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error rebuilding index ' || idx.index_name || ': ' || SQLERRM);
        END;
    END LOOP;
    
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Index rebuild completed');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
        RAISE;
END SP_REBUILD_INDEXES;
/


-- ============================================
-- PROCEDURE 9: Gather Statistics
-- ============================================

CREATE OR REPLACE PROCEDURE SP_GATHER_TABLE_STATISTICS AS
BEGIN
    -- Gather statistics for dimension tables
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname => USER,
        tabname => 'DIM_DATE',
        estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
        method_opt => 'FOR ALL COLUMNS SIZE AUTO',
        degree => DBMS_STATS.AUTO_DEGREE
    );
    
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname => USER,
        tabname => 'DIM_CUSTOMER',
        estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
        method_opt => 'FOR ALL COLUMNS SIZE AUTO',
        degree => DBMS_STATS.AUTO_DEGREE
    );
    
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname => USER,
        tabname => 'DIM_PRODUCT',
        estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
        method_opt => 'FOR ALL COLUMNS SIZE AUTO',
        degree => DBMS_STATS.AUTO_DEGREE
    );
    
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname => USER,
        tabname => 'DIM_STORE',
        estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
        method_opt => 'FOR ALL COLUMNS SIZE AUTO',
        degree => DBMS_STATS.AUTO_DEGREE
    );
    
    -- Gather statistics for fact tables
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname => USER,
        tabname => 'FACT_SALES',
        estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
        method_opt => 'FOR ALL COLUMNS SIZE AUTO',
        degree => DBMS_STATS.AUTO_DEGREE
    );
    
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname => USER,
        tabname => 'FACT_REVENUE',
        estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
        method_opt => 'FOR ALL COLUMNS SIZE AUTO',
        degree => DBMS_STATS.AUTO_DEGREE
    );
    
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Statistics gathered for all tables');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error gathering statistics: ' || SQLERRM);
        RAISE;
END SP_GATHER_TABLE_STATISTICS;
/


-- ============================================
-- PROCEDURE 10: End-to-End Pipeline Execution
-- ============================================

CREATE OR REPLACE PROCEDURE SP_RUN_DAILY_ETL_PIPELINE (
    p_date_key IN NUMBER
) AS
    v_run_id NUMBER;
    v_start_time TIMESTAMP := CURRENT_TIMESTAMP;
    v_status VARCHAR2(20) := 'SUCCESS';
    v_error_message VARCHAR2(4000);
    v_records_processed NUMBER := 0;
BEGIN
    -- Log pipeline start
    INSERT INTO METADATA_PIPELINE_RUN (
        pipeline_name,
        run_date,
        start_time,
        status
    ) VALUES (
        'daily_etl_pipeline',
        TO_DATE(TO_CHAR(p_date_key), 'YYYYMMDD'),
        v_start_time,
        'RUNNING'
    ) RETURNING run_id INTO v_run_id;
    
    COMMIT;
    
    BEGIN
        -- Step 1: Load dimension date (if needed)
        -- (Assumes DIM_DATE is already populated)
        
        -- Step 2: Load dimension tables
        -- (Handled by Spark jobs)
        
        -- Step 3: Load fact tables
        SP_LOAD_FACT_SALES(v_run_id, p_date_key);
        
        -- Step 4: Update SCD Type 2 dimensions
        SP_UPDATE_DIM_CUSTOMER_SCD2(v_run_id);
        
        -- Step 5: Calculate aggregations
        SP_CALCULATE_SALES_AGGREGATIONS(p_date_key);
        
        -- Step 6: Validate data quality
        SP_VALIDATE_DATA_QUALITY('daily_etl_pipeline', v_run_id);
        
        -- Step 7: Refresh materialized views
        SP_REFRESH_ALL_MATERIALIZED_VIEWS;
        
        -- Step 8: Gather statistics
        SP_GATHER_TABLE_STATISTICS;
        
        v_records_processed := (SELECT COUNT(*) FROM FACT_SALES WHERE date_key = p_date_key);
        
    EXCEPTION
        WHEN OTHERS THEN
            v_status := 'FAILED';
            v_error_message := SQLERRM;
    END;
    
    -- Update pipeline run log
    UPDATE METADATA_PIPELINE_RUN
    SET 
        end_time = CURRENT_TIMESTAMP,
        status = v_status,
        records_processed = v_records_processed,
        error_message = v_error_message
    WHERE run_id = v_run_id;
    
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('Pipeline completed with status: ' || v_status);
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Pipeline failed: ' || SQLERRM);
        RAISE;
END SP_RUN_DAILY_ETL_PIPELINE;
/


-- ============================================
-- Grant permissions
-- ============================================

GRANT EXECUTE ON SP_LOAD_FACT_SALES TO DATA_WAREHOUSE_USER;
GRANT EXECUTE ON SP_UPDATE_DIM_CUSTOMER_SCD2 TO DATA_WAREHOUSE_USER;
GRANT EXECUTE ON SP_CALCULATE_SALES_AGGREGATIONS TO DATA_WAREHOUSE_USER;
GRANT EXECUTE ON SP_VALIDATE_DATA_QUALITY TO DATA_WAREHOUSE_USER;
GRANT EXECUTE ON SP_REFRESH_ALL_MATERIALIZED_VIEWS TO DATA_WAREHOUSE_USER;
GRANT EXECUTE ON SP_CLEANUP_OLD_DATA TO DATA_WAREHOUSE_USER;
GRANT EXECUTE ON SP_GENERATE_DATA_LINEAGE TO DATA_WAREHOUSE_USER;
GRANT EXECUTE ON SP_REBUILD_INDEXES TO DATA_WAREHOUSE_USER;
GRANT EXECUTE ON SP_GATHER_TABLE_STATISTICS TO DATA_WAREHOUSE_USER;
GRANT EXECUTE ON SP_RUN_DAILY_ETL_PIPELINE TO DATA_WAREHOUSE_USER;

COMMIT;