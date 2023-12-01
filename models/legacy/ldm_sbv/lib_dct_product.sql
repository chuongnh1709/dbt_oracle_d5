/*
-- WF :  DIM_DCT_PRODUCT_MAP_SBV  --> DIM_DCT_PRODUCT_LOAD_SBV --> DIM_DCT_PRODUCT_MAP_LATE_SBV --> DIM_DCT_PRODUCT_LOAD_SBV
*/
---------------------------------------------------------------------------

create or replace PACKAGE Body                                  lib_dct_product IS

  -----------------------------------------------------------------
  -- dim_dct_product_map_sbv
  -- author : huyen.trank
  -- created : 23.04.2021
  -----------------------------------------------------------------

  v_dm2_err_addr      VARCHAR2(255)   := 'Errors - Notification - VN Data mart 2 <4908ab62.homecreditgroup.onmicrosoft.com@emea.teams.ms>';

  PROCEDURE dim_dct_product_map_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2) IS
    v_step        VARCHAR2(255);
    v_cnt         Integer;
    c_mapping     VARCHAR2(60) := 'DIM_DCT_PRODUCT_MAP_SBV';
    c_table_owner VARCHAR2(40) := 'LDM_SBV';
    c_table_name  VARCHAR2(40) := 'STM_PRODUCT';

  BEGIN

    v_step := 'Start log';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping);

    v_step := 'Truncate table ' || c_table_owner || '.' || c_table_name;
    LDM_SBV.truncate_table(c_table_owner, c_table_name);

    v_step := 'Insert data into LDM_SBV.STM_PRODUCT';

   Insert /*+APPEND */
    INTO LDM_SBV.STM_PRODUCT
      (
				  SKP_PRODUCT
				, CODE_SOURCE_SYSTEM
				, ID_SOURCE
				, DATE_EFFECTIVE
				, SKP_PROC_INSERTED
				, SKP_PROC_UPDATED
				, DTIME_INSERTED
				, DTIME_UPDATED
				, FLAG_DELETED
				, ID_PRODUCT
				, CODE_PRODUCT
				, NAME_PRODUCT
				, DATE_CREATION
				, NAME_VERSION_STATUS
				, CODE_PRODUCT_PROFILE
	  )
	select
                     NULL 															AS SKP_PRODUCT,
                     v_hom_code_source_system 										AS CODE_SOURCE_SYSTEM,
                     i.ID_SOURCE 													AS ID_SOURCE,
					 p_effective_date 												AS DATE_EFFECTIVE,
                     p_process_key 													AS skp_proc_inserted,
                     p_process_key  												AS skp_proc_updated,
					 sysdate														AS DTIME_INSERTED,
					 sysdate 														AS DTIME_UPDATED,
                     CASE
						WHEN i.code_change_type = v_code_change_type_del
						THEN v_flag_Y
						ELSE v_flag_N END										    AS FLAG_DELETED ,
					 i.id															AS ID_PRODUCT,
					 NVL(i.code, v_xna) 										    AS CODE_PRODUCT,
                     NVL(i.name, v_xna) 										    AS NAME_PRODUCT,
					 NVL(i.creation_date, d_def_value_date_future)  				AS DATE_CREATION,
					 i.version_status 												AS NAME_VERSION_STATUS,
					 i.product_profile_code 										AS CODE_PRODUCT_PROFILE
	from(
			select
                    to_char(product.id) 											AS ID_SOURCE,
					product.id,
                    product.code_change_type,
                    product.code,
					product.name,
					product.creation_date,
					product.version_status,
					product.product_profile_code
            from
                 owner_int.in_hom_product product
			where	  		 product.code_load_status IN ('OK', 'LOAD')
							and product.code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
							and product.date_effective_inserted = p_effective_date
                ) i
         ;

    v_cnt := SQL%ROWCOUNT;
    COMMIT;

    v_step := 'Write record count';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping,
                                           p_sel          => v_cnt,
                                           p_ins          => v_cnt);

    v_step := 'Calculate statistics';
    dbms_stats.gather_table_stats(ownname          => c_table_owner,
                                  tabname          => c_table_name,
                                  estimate_percent => dbms_stats.auto_sample_size,
                                  degree           => 4,
                                  granularity      => 'ALL',
                                  cascade          => True);

    v_step := 'Close log';
    owner_core.ms_global_log.write_map_end(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping,
                                           p_Status       => 'COMPLETE');

  Exception
    WHEN OTHERS THEN
      ROLLBACK;
      owner_core.etl_log_api.logend2(p_proc_key => p_process_key,
                                     p_log_name => c_mapping,
                                     p_Status   => 'FAILED',
                                     p_info     => v_step || ', SQLERRM = ' ||
                                                   substr(dbms_utility.format_error_stack, 1, 300));
      LDM_SBV_ETL.SP_SEND_NOTIFICATION (v_dm2_err_addr, 'LIB_DCT_PRODUCT.'|| c_mapping ,v_step || ', SQLERRM = ' || substr(dbms_utility.format_error_stack, 1, 300));

      raise_application_error(-20123,
                              'Error in module ' || c_mapping || ' (' || v_step || ')',
                              True);

  END dim_dct_product_map_sbv;


  -----------------------------------------------------------------
  -- dim_dct_product_map_sbv
  -- author : huyen.trank
  -- created : 23.04.2021
  -----------------------------------------------------------------
  PROCEDURE dim_dct_product_map_late_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2) IS
    v_step        VARCHAR2(255);
    v_cnt         Integer;
    c_mapping     VARCHAR2(60) := 'DIM_DCT_PRODUCT_MAP_LATE_SBV';
    c_table_owner VARCHAR2(40) := 'LDM_SBV';
    c_table_name  VARCHAR2(40) := 'STM_PRODUCT';

  BEGIN

    v_step := 'Start log';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping);

    v_step := 'Truncate table ' || c_table_owner || '.' || c_table_name;
    LDM_SBV.truncate_table(c_table_owner, c_table_name);

    v_step := 'Insert data into LDM_SBV.STM_PRODUCT';

   Insert /*+APPEND */
    INTO LDM_SBV.STM_PRODUCT
      (
				  SKP_PRODUCT
				, CODE_SOURCE_SYSTEM
				, ID_SOURCE
				, DATE_EFFECTIVE
				, SKP_PROC_INSERTED
				, SKP_PROC_UPDATED
				, DTIME_INSERTED
				, DTIME_UPDATED
				, FLAG_DELETED
				, ID_PRODUCT
				, CODE_PRODUCT
				, NAME_PRODUCT
				, DATE_CREATION
				, NAME_VERSION_STATUS
				, CODE_PRODUCT_PROFILE
	  )
	select
                     NULL 															AS SKP_PRODUCT,
                     v_hom_code_source_system 										AS CODE_SOURCE_SYSTEM,
                     i.ID_SOURCE 													AS ID_SOURCE,
					 p_effective_date 												AS DATE_EFFECTIVE,
                     p_process_key 													AS skp_proc_inserted,
                     p_process_key  												AS skp_proc_updated,
					 sysdate														AS DTIME_INSERTED,
					 sysdate 														AS DTIME_UPDATED,
                     CASE
						WHEN i.code_change_type = v_code_change_type_del
						THEN v_flag_Y
						ELSE v_flag_N END										    AS FLAG_DELETED ,
					 i.id															AS ID_PRODUCT,
					 NVL(i.code, v_xna) 										    AS CODE_PRODUCT,
                     NVL(i.name, v_xna) 										    AS NAME_PRODUCT,
					 NVL(i.creation_date, d_def_value_date_future)  				AS DATE_CREATION,
					 i.version_status 												AS NAME_VERSION_STATUS,
					 i.product_profile_code 										AS CODE_PRODUCT_PROFILE
	from(
			select
                    to_char(product.id) 											AS ID_SOURCE,
					product.id,
                    product.code_change_type,
                    product.code,
					product.name,
					product.creation_date,
					product.version_status,
					product.product_profile_code
            from
                 owner_int.in_hom_product product
			where	  		 product.code_load_status IN ('OK', 'LOAD')
							and product.code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
							and product.date_effective_inserted = p_effective_date + 1 and product.update_date < p_effective_date + 1
                ) i
         ;

    v_cnt := SQL%ROWCOUNT;
    COMMIT;

    v_step := 'Write record count';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping,
                                           p_sel          => v_cnt,
                                           p_ins          => v_cnt);

    v_step := 'Calculate statistics';
    dbms_stats.gather_table_stats(ownname          => c_table_owner,
                                  tabname          => c_table_name,
                                  estimate_percent => dbms_stats.auto_sample_size,
                                  degree           => 4,
                                  granularity      => 'ALL',
                                  cascade          => True);

    v_step := 'Close log';
    owner_core.ms_global_log.write_map_end(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping,
                                           p_Status       => 'COMPLETE');

  Exception
    WHEN OTHERS THEN
      ROLLBACK;
      owner_core.etl_log_api.logend2(p_proc_key => p_process_key,
                                     p_log_name => c_mapping,
                                     p_Status   => 'FAILED',
                                     p_info     => v_step || ', SQLERRM = ' ||
                                                   substr(dbms_utility.format_error_stack, 1, 300));
      LDM_SBV_ETL.SP_SEND_NOTIFICATION (v_dm2_err_addr, 'LIB_DCT_PRODUCT.'|| c_mapping ,v_step || ', SQLERRM = ' || substr(dbms_utility.format_error_stack, 1, 300));

      raise_application_error(-20123,
                              'Error in module ' || c_mapping || ' (' || v_step || ')',
                              True);

  END dim_dct_product_map_late_sbv;

   -----------------------------------------------------------------
  -- dim_dct_product_load_sbv
  -- author : huyen.trank
  -- created : 23.04.2020
  -----------------------------------------------------------------
  PROCEDURE dim_dct_product_load_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2) IS
    v_step        VARCHAR2(255);
    v_cnt         Integer;
    c_mapping     VARCHAR2(60) := 'DIM_DCT_PRODUCT_LOAD_SBV';
    c_table_owner VARCHAR2(40) := 'LDM_SBV';
    c_table_name  VARCHAR2(40) := 'DCT_PRODUCT';

  BEGIN

    v_step := 'Start log';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping);

    v_step := 'Merge data into LDM_SBV.DCT_PRODUCT';


    MERGE
	INTO LDM_SBV.DCT_PRODUCT t
    USING (SELECT
				SKP_PRODUCT
				, CODE_SOURCE_SYSTEM
				, ID_SOURCE
				, DATE_EFFECTIVE
				, SKP_PROC_INSERTED
				, SKP_PROC_UPDATED
				, DTIME_INSERTED
				, DTIME_UPDATED
				, FLAG_DELETED
				, ID_PRODUCT
				, CODE_PRODUCT
				, NAME_PRODUCT
				, DATE_CREATION
				, NAME_VERSION_STATUS
				, CODE_PRODUCT_PROFILE
             FROM LDM_SBV.STM_PRODUCT ) s
    ON (s.id_source = t.id_source)
    WHEN NOT MATCHED THEN
      Insert
		(		 SKP_PRODUCT
				, CODE_SOURCE_SYSTEM
				, ID_SOURCE
				, DATE_EFFECTIVE
				, SKP_PROC_INSERTED
				, SKP_PROC_UPDATED
				, DTIME_INSERTED
				, DTIME_UPDATED
				, FLAG_DELETED
				, ID_PRODUCT
				, CODE_PRODUCT
				, NAME_PRODUCT
				, DATE_CREATION
				, NAME_VERSION_STATUS
				, CODE_PRODUCT_PROFILE
		)
      VALUES
			(	 LDM_SBV.S_DCT_PRODUCT.nextval
				, s.CODE_SOURCE_SYSTEM
				, s.ID_SOURCE
				, s.DATE_EFFECTIVE
				, s.SKP_PROC_INSERTED
				, s.SKP_PROC_UPDATED
				, s.DTIME_INSERTED
				, s.DTIME_UPDATED
				, s.FLAG_DELETED
				, s.ID_PRODUCT
				, s.CODE_PRODUCT
				, s.NAME_PRODUCT
				, s.DATE_CREATION
				, s.NAME_VERSION_STATUS
				, s.CODE_PRODUCT_PROFILE
			)
    WHEN MATCHED THEN
      Update
         SET 	t.DATE_EFFECTIVE 			=	s.DATE_EFFECTIVE ,
				t.SKP_PROC_UPDATED 			=	s.SKP_PROC_UPDATED  ,
				t.DTIME_UPDATED				= 	s.DTIME_UPDATED,
				t.FLAG_DELETED 				=	s.FLAG_DELETED ,
				t.CODE_PRODUCT 				=	s.CODE_PRODUCT ,
				t.NAME_PRODUCT 				=	s.NAME_PRODUCT ,
				t.DATE_CREATION 			=	s.DATE_CREATION ,
				t.NAME_VERSION_STATUS 		=	s.NAME_VERSION_STATUS,
				t.CODE_PRODUCT_PROFILE 		=	s.CODE_PRODUCT_PROFILE
	;

    v_cnt := SQL%ROWCOUNT;
    COMMIT;

    v_step := 'Write record count';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping,
                                           p_sel          => v_cnt,
                                           p_mrg          => v_cnt);

	--Stats
  v_step := 'Calculate statistics';
  dbms_stats.gather_table_stats(ownname          => c_table_owner,
                                tabname          => c_table_name,
                                estimate_percent => dbms_stats.auto_sample_size,
                                degree           => 4,
                                granularity      => 'ALL',
                                cascade          => True);


   v_step := 'Close log';
    owner_core.ms_global_log.write_map_end(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping,
                                           p_Status       => 'COMPLETE');

  Exception
    WHEN OTHERS THEN
      ROLLBACK;
      owner_core.etl_log_api.logend2(p_proc_key => p_process_key,
                                     p_log_name => c_mapping,
                                     p_Status   => 'FAILED',
                                     p_info     => v_step || ', SQLERRM = ' ||
                                                  substr(dbms_utility.format_error_stack, 1, 300));
      LDM_SBV_ETL.SP_SEND_NOTIFICATION (v_dm2_err_addr, 'LIB_DCT_PRODUCT.'|| c_mapping ,v_step || ', SQLERRM = ' || substr(dbms_utility.format_error_stack, 1, 300));

      raise_application_error(-20123,
                              'Error in module ' || c_mapping || ' (' || v_step || ')',
                              True);

  END dim_dct_product_load_sbv;


  -----------------------------------------------------------------
  -- dim_dct_product_map_SCB
  -- author : huyen.trank
  -- created : 17.11.2021
  -----------------------------------------------------------------
  PROCEDURE dim_dct_product_map_scb(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2) IS
    v_step        VARCHAR2(255);
    v_cnt         Integer;
    c_mapping     VARCHAR2(60) := 'DIM_DCT_PRODUCT_MAP_SCB';
    c_table_owner VARCHAR2(40) := 'LDM_SBV';
    c_table_name  VARCHAR2(40) := 'STM_PRODUCT';
	v_from_dtime date:= to_date(to_char(p_effective_date,'YYYY-MM-DD') || ' ' || '18:00:00', 'YYYY-MM-DD HH24:MI:SS');
    v_to_dtime   date:= to_date(to_char(p_effective_date + 1,'YYYY-MM-DD') || ' ' || '18:00:00', 'YYYY-MM-DD HH24:MI:SS');

  BEGIN

    v_step := 'Start log';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping);

    v_step := 'Truncate table ' || c_table_owner || '.' || c_table_name;
    LDM_SBV.truncate_table(c_table_owner, c_table_name);

    v_step := 'Insert data into LDM_SBV.STM_PRODUCT';

   Insert /*+APPEND */
    INTO LDM_SBV.STM_PRODUCT
      (
				  SKP_PRODUCT
				, CODE_SOURCE_SYSTEM
				, ID_SOURCE
				, DATE_EFFECTIVE
				, SKP_PROC_INSERTED
				, SKP_PROC_UPDATED
				, DTIME_INSERTED
				, DTIME_UPDATED
				, FLAG_DELETED
				, ID_PRODUCT
				, CODE_PRODUCT
				, NAME_PRODUCT
				, DATE_CREATION
				, NAME_VERSION_STATUS
				, CODE_PRODUCT_PROFILE
	  )
	select
                     NULL 															AS SKP_PRODUCT,
                     v_hom_code_source_system 										AS CODE_SOURCE_SYSTEM,
                     i.ID_SOURCE 													AS ID_SOURCE,
					 p_effective_date 												AS DATE_EFFECTIVE,
                     p_process_key 													AS skp_proc_inserted,
                     p_process_key  												AS skp_proc_updated,
					 sysdate														AS DTIME_INSERTED,
					 sysdate 														AS DTIME_UPDATED,
                     CASE
						WHEN i.code_change_type = v_code_change_type_del
						THEN v_flag_Y
						ELSE v_flag_N END										    AS FLAG_DELETED ,
					 i.id															AS ID_PRODUCT,
					 NVL(i.code, v_xna) 										    AS CODE_PRODUCT,
                     NVL(i.name, v_xna) 										    AS NAME_PRODUCT,
					 NVL(i.creation_date, d_def_value_date_future)  				AS DATE_CREATION,
					 i.version_status 												AS NAME_VERSION_STATUS,
					 i.product_profile_code 										AS CODE_PRODUCT_PROFILE
	from(
			select
                    to_char(product.id) 											AS ID_SOURCE,
					product.id,
                    product.code_change_type,
                    product.code,
					product.name,
					product.creation_date,
					product.version_status,
					product.product_profile_code
            from
                 owner_int.in_hom_product product
			where	  		 product.code_load_status IN ('OK', 'LOAD')
							and product.code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
							and product.date_effective_inserted = p_effective_date +1
                ) i
         ;

    v_cnt := SQL%ROWCOUNT;
    COMMIT;

    v_step := 'Write record count';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping,
                                           p_sel          => v_cnt,
                                           p_ins          => v_cnt);

    v_step := 'Calculate statistics';
    dbms_stats.gather_table_stats(ownname          => c_table_owner,
                                  tabname          => c_table_name,
                                  estimate_percent => dbms_stats.auto_sample_size,
                                  degree           => 4,
                                  granularity      => 'ALL',
                                  cascade          => True);

    v_step := 'Close log';
    owner_core.ms_global_log.write_map_end(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping,
                                           p_Status       => 'COMPLETE');

  Exception
    WHEN OTHERS THEN
      ROLLBACK;
      owner_core.etl_log_api.logend2(p_proc_key => p_process_key,
                                     p_log_name => c_mapping,
                                     p_Status   => 'FAILED',
                                     p_info     => v_step || ', SQLERRM = ' ||
                                                   substr(dbms_utility.format_error_stack, 1, 300));
      LDM_SBV_ETL.SP_SEND_NOTIFICATION (v_dm2_err_addr, 'LIB_DCT_PRODUCT.'|| c_mapping ,v_step || ', SQLERRM = ' || substr(dbms_utility.format_error_stack, 1, 300));

      raise_application_error(-20123,
                              'Error in module ' || c_mapping || ' (' || v_step || ')',
                              True);

  END dim_dct_product_map_scb;

END lib_dct_product;