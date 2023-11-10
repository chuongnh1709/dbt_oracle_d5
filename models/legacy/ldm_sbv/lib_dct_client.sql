/*
-- WF : 
    DIM_CLIENT_MAP_SBV -> DIM_CLIENT_LOAD_SBV -> DIM_CLIENT_LATE_MAP_SBV -> DIM_CLIENT_LOAD_SBV
*/

CREATE OR REPLACE PACKAGE LDM_SBV_ETL.LIB_DCT_CLIENT IS

  -- global parameters
		v_default_code_source_system CONSTANT VARCHAR2(10) := 'SBV';
		v_hom_code_source_system     CONSTANT VARCHAR2(10) := 'HOM';
		v_cbus_code_source_system    CONSTANT VARCHAR2(10) := 'CBUS';
		v_xna                        CONSTANT VARCHAR2(10) := 'XNA';
		v_xap                        CONSTANT VARCHAR2(10) := 'XAP';
		n_minus_one                  CONSTANT BINARY_INTEGER := -1;
		v_flag_X                     CONSTANT VARCHAR2(1 CHAR)  := 'X';
		v_flag_Y                     CONSTANT VARCHAR2(1)  := 'Y';
		v_flag_N                     CONSTANT VARCHAR2(1)  := 'N';
		v_code_change_type_del       CONSTANT VARCHAR2(1)  := 'D';
		d_def_value_date_hist        CONSTANT DATE  := DATE '1000-01-01';
		d_def_value_date_future      CONSTANT DATE  := DATE '3000-01-01';
		d_def_value                  CONSTANT DATE  := DATE '3000-01-01';
		v_code_active                CONSTANT VARCHAR2(1 CHAR) := 'a';

  PROCEDURE dim_client_map_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2);

  PROCEDURE dim_client_load_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2);

  PROCEDURE dim_client_late_map_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2);


END lib_dct_client;
/

CREATE OR REPLACE PACKAGE BODY LDM_SBV_ETL.LIB_DCT_CLIENT IS

  -----------------------------------------------------------------
  -- dim_client_map_sbv
  -- author : huyen.trank
  -- created : 13.01.2022
  -----------------------------------------------------------------

  v_dm2_err_addr      VARCHAR2(255)   := 'Errors - Notification - VN Data mart 2 <4908ab62.homecreditgroup.onmicrosoft.com@emea.teams.ms>';

  PROCEDURE dim_client_map_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2) IS
    v_step        VARCHAR2(255);
    v_cnt         Integer;
    c_mapping     VARCHAR2(60) := 'DIM_CLIENT_MAP_SBV';
    c_table_owner VARCHAR2(40) := 'LDM_SBV';
    c_table_name  VARCHAR2(40) := 'STM_CLIENT';

  BEGIN

    v_step := 'Start log';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping);

    v_step := 'Truncate table ' || c_table_owner || '.' || c_table_name;
    LDM_SBV.truncate_table(c_table_owner, c_table_name);

    v_step := 'Insert data into LDM_SBV.STM_CLIENT';

   Insert /*+APPEND */
    INTO LDM_SBV.STM_CLIENT
      (
				SKP_CLIENT
				, CODE_SOURCE_SYSTEM
				, ID_SOURCE
				, DATE_EFFECTIVE
				, SKP_PROC_INSERTED
				, SKP_PROC_UPDATED
				, DTIME_INSERTED
				, DTIME_UPDATED
				, FLAG_DELETED
				, ID_CUID
	  )
	select
        NULL 															AS SKP_CLIENT,
        v_hom_code_source_system 										AS CODE_SOURCE_SYSTEM,
        i.ID_SOURCE 													AS ID_SOURCE,
        p_effective_date 												AS DATE_EFFECTIVE,
        p_process_key 													AS skp_proc_inserted,
        p_process_key  												AS skp_proc_updated,
        sysdate														AS DTIME_INSERTED,
        sysdate 														AS DTIME_UPDATED,
        CASE
        WHEN i.CODE_CHANGE_TYPE = V_CODE_CHANGE_TYPE_DEL
        THEN V_FLAG_Y
        ELSE V_FLAG_N END 											AS FLAG_DELETED,
        NVL(i.CUID,  n_minus_one ) 									as ID_CUID
	from(
			select
                     to_char(client.id) 									AS ID_SOURCE,
                     client.cuid	,
					 client.CODE_CHANGE_TYPE
            from
                 owner_int.in_hom_client client
			where	  			 client.code_load_status IN ('OK', 'LOAD')
							   and client.code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
							   and client.date_effective_inserted = p_effective_date
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
      LDM_SBV_ETL.SP_SEND_NOTIFICATION (v_dm2_err_addr, 'LIB_DCT_CLIENT.'|| c_mapping ,v_step || ', SQLERRM = ' || substr(dbms_utility.format_error_stack, 1, 300));

      raise_application_error(-20123,
                              'Error in module ' || c_mapping || ' (' || v_step || ')',
                              True);

  END dim_client_map_sbv;

   -----------------------------------------------------------------
  -- dim_client_load_sbv
  -- author : huyen.trank
  -- created : 13.01.2022
  -----------------------------------------------------------------
  PROCEDURE dim_client_load_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2) IS
    v_step        VARCHAR2(255);
    v_cnt         Integer;
    c_mapping     VARCHAR2(60) := 'DIM_CLIENT_LOAD_SBV';
    c_table_owner VARCHAR2(40) := 'LDM_SBV';
    c_table_name  VARCHAR2(40) := 'DCT_CLIENT';

  BEGIN

    v_step := 'Start log';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping);

    v_step := 'Merge data into LDM_SBV.DCT_CLIENT';


    MERGE
	INTO LDM_SBV.DCT_CLIENT t
    USING (SELECT
				SKP_CLIENT
				, CODE_SOURCE_SYSTEM
				, ID_SOURCE
				, DATE_EFFECTIVE
				, SKP_PROC_INSERTED
				, SKP_PROC_UPDATED
				, DTIME_INSERTED
				, DTIME_UPDATED
				, FLAG_DELETED
				, ID_CUID
             FROM LDM_SBV.STM_CLIENT ) s
    ON (s.id_source = t.id_source)
    WHEN NOT MATCHED THEN
      Insert
		(		SKP_CLIENT
				, CODE_SOURCE_SYSTEM
				, ID_SOURCE
				, DATE_EFFECTIVE
				, SKP_PROC_INSERTED
				, SKP_PROC_UPDATED
				, DTIME_INSERTED
				, DTIME_UPDATED
				, FLAG_DELETED
				, ID_CUID
		)
      VALUES
			(	 LDM_SBV.S_DCT_CLIENT.nextval
				,s.CODE_SOURCE_SYSTEM
				,s.ID_SOURCE
				,s.DATE_EFFECTIVE
				,s.SKP_PROC_INSERTED
				,s.SKP_PROC_UPDATED
				,s.DTIME_INSERTED
				,s.DTIME_UPDATED
				,s.FLAG_DELETED
				,s.ID_CUID
			)
    WHEN MATCHED THEN
      Update
         SET 	t.DATE_EFFECTIVE 			=	s.DATE_EFFECTIVE ,
				t.SKP_PROC_UPDATED 			=	s.SKP_PROC_UPDATED  ,
				t.DTIME_UPDATED				= 	s.DTIME_UPDATED,
				t.FLAG_DELETED 				=	s.FLAG_DELETED ,
				t.ID_CUID					=	s.ID_CUID
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
      LDM_SBV_ETL.SP_SEND_NOTIFICATION (v_dm2_err_addr, 'LIB_DCT_CLIENT.'|| c_mapping ,v_step || ', SQLERRM = ' || substr(dbms_utility.format_error_stack, 1, 300));

      raise_application_error(-20123,
                              'Error in module ' || c_mapping || ' (' || v_step || ')',
                              True);

  END dim_client_load_sbv;

  -----------------------------------------------------------------
  -- dim_client_late_map_sbv
  -- author : huyen.trank
  -- created : 27.01.2022
  -----------------------------------------------------------------
  PROCEDURE dim_client_late_map_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2) IS
    v_step        VARCHAR2(255);
    v_cnt         Integer;
    c_mapping     VARCHAR2(60) := 'DIM_CLIENT_LATE_MAP_SBV';
    c_table_owner VARCHAR2(40) := 'LDM_SBV';
    c_table_name  VARCHAR2(40) := 'STM_CLIENT';

  BEGIN

    v_step := 'Start log';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping);

    v_step := 'Truncate table ' || c_table_owner || '.' || c_table_name;
    LDM_SBV.truncate_table(c_table_owner, c_table_name);

    v_step := 'Insert data into LDM_SBV.STM_CLIENT';

   Insert /*+APPEND */
    INTO LDM_SBV.STM_CLIENT
      (
				SKP_CLIENT
				, CODE_SOURCE_SYSTEM
				, ID_SOURCE
				, DATE_EFFECTIVE
				, SKP_PROC_INSERTED
				, SKP_PROC_UPDATED
				, DTIME_INSERTED
				, DTIME_UPDATED
				, FLAG_DELETED
				, ID_CUID
	  )
	select
                     NULL 															AS SKP_CLIENT,
                     v_hom_code_source_system 										AS CODE_SOURCE_SYSTEM,
                     i.ID_SOURCE 													AS ID_SOURCE,
					 p_effective_date 												AS DATE_EFFECTIVE,
                     p_process_key 													AS skp_proc_inserted,
                     p_process_key  												AS skp_proc_updated,
					 sysdate														AS DTIME_INSERTED,
					 sysdate 														AS DTIME_UPDATED,
					 CASE
						WHEN i.CODE_CHANGE_TYPE = V_CODE_CHANGE_TYPE_DEL
						THEN V_FLAG_Y
						ELSE V_FLAG_N END 											AS FLAG_DELETED,
                     NVL(i.CUID,  n_minus_one ) 									as ID_CUID
	from(
			select
                     to_char(client.id) 									AS ID_SOURCE,
                     client.cuid	,
					 client.CODE_CHANGE_TYPE
            from
                 owner_int.in_hom_client client
			where	  			 client.code_load_status IN ('OK', 'LOAD')
							   and client.code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
							   and client.date_effective_inserted = p_effective_date + 1
							   and client.CREATION_DATE >= p_effective_date and  client.CREATION_DATE <  p_effective_date + 1
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
      LDM_SBV_ETL.SP_SEND_NOTIFICATION (v_dm2_err_addr, 'LIB_DCT_CLIENT.'|| c_mapping ,v_step || ', SQLERRM = ' || substr(dbms_utility.format_error_stack, 1, 300));

      raise_application_error(-20123,
                              'Error in module ' || c_mapping || ' (' || v_step || ')',
                              True);

  END dim_client_late_map_sbv;

END lib_dct_client;
/
