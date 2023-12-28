create or replace PACKAGE lib_ft_acc_online_new_tt IS


  -- global parameters
		v_bkng_code_source_system 	 CONSTANT VARCHAR2(10) := 'BNKG';
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

  PROCEDURE ft_faccon_new_map_book_sbv(p_process_key    IN NUMBER,
                               p_effective_date IN DATE,
                               p_data_type      IN VARCHAR2);

  PROCEDURE ft_faccon_new_load_book_sbv(p_process_key    IN NUMBER,
                                p_effective_date IN DATE,
                                p_data_type      IN VARCHAR2);

END lib_ft_acc_online_new_tt;

create or replace PACKAGE Body lib_ft_acc_online_new_tt IS

  -----------------------------------------------------------------
	-- ft_faccon_new_map_book_sbv
	-- author : huyen.trank
	-- created : 20.10.2020
	-- modified : 29.12.2020
  -----------------------------------------------------------------

  v_dm2_err_addr      VARCHAR2(255)   := 'Errors - Notification - VN Data mart 2 <4908ab62.homecreditgroup.onmicrosoft.com@emea.teams.ms>';

  PROCEDURE ft_faccon_new_map_book_sbv(p_process_key    IN NUMBER,
                               p_effective_date IN DATE,
                               p_data_type      IN VARCHAR2) IS
    v_step        VARCHAR2(255);
    v_cnt         Integer;
    c_mapping     VARCHAR2(60) := 'FT_FACCON_NEW_MAP_BOOK_SBV';
    c_table_owner VARCHAR2(40) := 'LDM_SBV';
    c_table_name  VARCHAR2(40) := 'STM_ACCOUNTING_ONLINE_NEW_TT';

  BEGIN

    v_step := 'Start log';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping);

     v_step := 'Truncate table ' || c_table_owner || '.' || c_table_name;
    LDM_SBV.truncate_table(c_table_owner, c_table_name);

    v_step := 'Insert data into LDM_SBV.stm_accounting_online_new_tt';

    Insert /*+ APPEND */
    INTO LDM_SBV.stm_accounting_online_new_tt
      (skf_accounting_online,
       code_source_system,
       id_source,
       date_effective,
       flag_deleted,
	   code_move_type,
       amt_accounted_value,
       date_accounted,
       date_accounting_move,
       dtime_acc_system_created,
       code_transaction_subtype,
	   code_credit_type,
	   code_owner,
	   text_contract_number,
	   text_account_move_desc,
       skp_contract,
	   id_contract,
	   id_client,
	   id_accounting_event,
	   date_creation,
	   skp_proc_inserted,
	   skp_proc_updated,
	   dtime_inserted,
	   dtime_updated,
       code_contract_term,
       name_status_acquisition,
	   AMT_ACCOUNTED_VALUE_R
       )
    with bkng_rank_info as (
    select id_accounting_event,
			max(case when type = 'CONTRACT_CODE' then value end) as text_contract_number,
			max(case when type = 'TARIFF_ITEM_TYPE_CODE' then value end) as code_transaction_subtype,
            max(case when type = 'CONTRACT_TERM' then value end) as code_contract_term,
            max(case when type = 'CLIENT_SEGMENT' then value end) as name_status_acquisition
    FROM owner_int.in_bkng_rank_001
    where type in ('CONTRACT_CODE', 'TARIFF_ITEM_TYPE_CODE', 'CONTRACT_TERM', 'CLIENT_SEGMENT')
		  and code_load_status IN ('OK', 'LOAD')
		  AND code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
		  AND date_effective = p_effective_date
    group by id_accounting_event
    )
      SELECT /*+ parallel(16) */ NULL AS skf_accounting_online,
             i.code_source_system AS code_source_system,
             i.id_source AS id_source,
             p_effective_date AS date_effective,
             i.flag_deleted AS flag_deleted,
			 nvl(i.code_move_type, v_xna) AS code_move_type,
			 i.amt_accounted_value AS amt_accounted_value,
			 nvl(i.date_accounted, d_def_value_date_hist) AS date_accounted,
			 nvl(i.date_accounting_move, d_def_value_date_hist) AS date_accounting_move,
			 nvl(i.dtime_acc_system_created, d_def_value_date_hist) AS dtime_acc_system_created,
			 nvl(i.CODE_TRANSACTION_SUBTYPE, v_xna) AS code_transaction_subtype,
			 i.code_credit_type,
			 nvl(i.code_owner, v_xna) AS code_owner,
			 nvl(i.text_contract_number, v_xna) AS	text_contract_number,
			 nvl(i.text_account_move_desc, v_xna) AS text_account_move_desc,
			 i.skp_contract as skp_contract,
			 i.id_contract as id_contract,
			 i.id_client as id_client,
			 i.id_accounting_event as id_accounting_event,
			 i.date_creation as date_creation,
			 p_process_key skp_proc_inserted,
			 p_process_key skp_proc_updated,
			 sysdate dtime_inserted,
			 sysdate dtime_updated,
             i.code_contract_term as code_contract_term,
             nvl(i.name_status_acquisition, v_xna) as name_status_acquisition,
			 i.amt_accounted_value_r
        FROM (SELECT    v_bkng_code_source_system as code_source_system,
                        to_char(b_mm.id) AS id_source,
                        CASE WHEN b_mm.code_change_type = v_code_change_type_del THEN v_flag_Y ELSE v_flag_N END AS FLAG_DELETED,
                        b_mm.type  as code_move_type,
                        b_mm.amount                        amt_accounted_value,
                        b_mm.accounting_date               date_accounted,
                        b_mm.move_date                     date_accounting_move,
                        b_mm.created                       dtime_acc_system_created,
						bkri.code_transaction_subtype    	code_transaction_subtype,
					    contract.code_credit_type,
						contract.code_credit_owner 	as		code_owner,
						bkri.text_contract_number 			text_contract_number,
						enum.value as 						text_account_move_desc,
						contract.skp_contract as skp_contract,
						contract.id_contract as 			id_contract,
						contract.id_client	 as				id_client,
						b_mm.id_accounting_event as id_accounting_event,
						contract.date_creation as date_creation,
                        bkri.code_contract_term as code_contract_term,
                        bkri.name_status_acquisition as name_status_acquisition,
						b_mm.amount_rounded                 amt_accounted_value_r
                    FROM owner_int.in_bkng_movement_002 b_mm
                     JOIN bkng_rank_info bkri ON bkri.id_accounting_event = b_mm.id_accounting_event
					 JOIN LDM_SBV.dct_contract contract on bkri.text_contract_number = contract.text_contract_number
                     LEFT JOIN owner_int.in_csd_enum_value enum on b_mm.type = enum.code and enum.ENUM_CODE = 'ACC_MOVE_TYPES'
                                             AND enum.ENUM_GROUP_ID = 'CUST'
											 AND enum.code_load_status IN ('OK', 'LOAD')
										     AND enum.code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
										     AND enum.date_effective_inserted = p_effective_date
				   WHERE
                       b_mm.code_load_status IN ('OK', 'LOAD')
                      AND b_mm.code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
                      AND b_mm.date_effective = p_effective_date
                      ) i;
        v_cnt :=  SQL%ROWCOUNT;
        COMMIT;
    /*THUAN.DANGT ADD ON 2022-08-12 resolving the missing move case , due to the contract have not created at that time  */
            Insert /*+APPEND */
            INTO Ldm_sbv.stm_accounting_online_new_tt
              (skf_accounting_online,
               code_source_system,
               id_source,
               date_effective,
               flag_deleted,
               code_move_type,
               amt_accounted_value,
               date_accounted,
               date_accounting_move,
               dtime_acc_system_created,
               code_transaction_subtype,
               code_credit_type,
               code_owner,
               text_contract_number,
               text_account_move_desc,
               skp_contract,
               id_contract,
               id_client,
               id_accounting_event,
               date_creation,
               skp_proc_inserted,
               skp_proc_updated,
               dtime_inserted,
               dtime_updated,
               code_contract_term,
               name_status_acquisition,
			   AMT_ACCOUNTED_VALUE_R
               )
        with  bkng_rank_info as (
            select id_accounting_event,
                    max(case when type = 'CONTRACT_CODE' then value end) as text_contract_number,
                    max(case when type = 'TARIFF_ITEM_TYPE_CODE' then value end) as code_transaction_subtype,
                    max(case when type = 'CONTRACT_TERM' then value end) as code_contract_term,
                    max(case when type = 'CLIENT_SEGMENT' then value end) as name_status_acquisition
            from owner_int.in_bkng_rank_001
            where type in ('CONTRACT_CODE', 'TARIFF_ITEM_TYPE_CODE', 'CONTRACT_TERM', 'CLIENT_SEGMENT')
                  and code_load_status IN ('OK', 'LOAD')
                  AND code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
                  AND date_effective = p_effective_date - 1
            group by id_accounting_event
            )
              SELECT NULL AS skf_accounting_online,
                     i.code_source_system AS code_source_system,
                     i.id_source AS id_source,
                     p_effective_date AS date_effective,
                     i.flag_deleted AS flag_deleted,
                     nvl(i.code_move_type, v_xna) AS code_move_type,
                     i.amt_accounted_value AS amt_accounted_value,
                     nvl(i.date_accounted, d_def_value_date_hist) AS date_accounted,
                     nvl(i.date_accounting_move, d_def_value_date_hist) AS date_accounting_move,
                     nvl(i.dtime_acc_system_created, d_def_value_date_hist) AS dtime_acc_system_created,
                     nvl(i.CODE_TRANSACTION_SUBTYPE, v_xna) AS code_transaction_subtype,
                     i.code_credit_type,
                     nvl(i.code_owner, v_xna) AS code_owner,
                     nvl(i.text_contract_number, v_xna) AS	text_contract_number,
                     nvl(i.text_account_move_desc, v_xna) AS text_account_move_desc,
                     i.skp_contract as skp_contract,
                     i.id_contract as id_contract,
                     i.id_client as id_client,
                     i.id_accounting_event as id_accounting_event,
                     i.date_creation as date_creation,
                     p_process_key skp_proc_inserted,
                     p_process_key skp_proc_updated,
                     sysdate dtime_inserted,
                     sysdate dtime_updated,
                     i.code_contract_term as code_contract_term,
                     nvl(i.name_status_acquisition, v_xna) as name_status_acquisition,
					 i.amt_accounted_value_r
                FROM (SELECT    v_bkng_code_source_system as code_source_system,
                                to_char(b_mm.id) AS id_source,
                                CASE WHEN b_mm.code_change_type = v_code_change_type_del THEN v_flag_Y ELSE v_flag_N END AS FLAG_DELETED,
                                b_mm.type  as code_move_type,
                                b_mm.amount                        amt_accounted_value,
                                b_mm.accounting_date               date_accounted,
                                b_mm.move_date                     date_accounting_move,
                                b_mm.created                       dtime_acc_system_created,
                                bkri.code_transaction_subtype    	code_transaction_subtype,
                                contract.code_credit_type,
                                contract.code_credit_owner 	as		code_owner,
                                bkri.text_contract_number 			text_contract_number,
                                enum.value as 						text_account_move_desc,
                                contract.skp_contract as skp_contract,
                                contract.id_contract as 			id_contract,
                                contract.id_client	 as				id_client,
                                b_mm.id_accounting_event as id_accounting_event,
                                contract.date_creation as date_creation,
                                bkri.code_contract_term as code_contract_term,
                                bkri.name_status_acquisition as name_status_acquisition,
								b_mm.amount_rounded                 amt_accounted_value_r
                            FROM owner_int.in_bkng_movement_002 b_mm
                             JOIN bkng_rank_info bkri ON bkri.id_accounting_event = b_mm.id_accounting_event
                             JOIN Ldm_sbv.dct_contract contract on bkri.text_contract_number = contract.text_contract_number
                             LEFT JOIN owner_int.in_csd_enum_value enum on b_mm.type = enum.code and enum.ENUM_CODE = 'ACC_MOVE_TYPES'
                                                     AND enum.ENUM_GROUP_ID = 'CUST'
                                                     AND enum.code_load_status IN ('OK', 'LOAD')
                                                     AND enum.code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
                                                     AND enum.date_effective_inserted = p_effective_date - 1
                           WHERE
                               b_mm.code_load_status IN ('OK', 'LOAD')
                              AND b_mm.code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
                              AND b_mm.date_effective = p_effective_date - 1
                               AND CONTRACT.DATE_EFFECTIVE  = p_effective_date
                              AND contract.DTIME_INSERTED > p_effective_date + 1
                              AND NOT EXISTS (
                                SELECT 1
                                  FROM Ldm_sbv.FT_ACCOUNTING_ONLINE_NEW_TT TT
                                 WHERE TT.ID_SOURCE = to_char(b_mm.id)
                              )
                             AND NOT EXISTS(
                                 SELECT 1
                                   FROM Ldm_sbv.stm_accounting_online_new_tt STM
                                  WHERE STM.id_source = to_char(b_mm.id)
                                )
                              ) i
                              ;
    /*THUAN.DANGT ADD ON 2022-08-12 resolving the missing move case , due to the contract have not created at that time -----------end */

    v_cnt := v_cnt + SQL%ROWCOUNT;
    COMMIT;

    v_step := 'Write record count';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping,
                                           p_sel          => v_cnt,
                                           p_ins          => v_cnt);

--    v_step := 'Calculate statistics';
--    dbms_stats.gather_table_stats(ownname          => c_table_owner,
--                                  tabname          => c_table_name,
--                                  estimate_percent => dbms_stats.auto_sample_size,
--                                  degree           => 4,
--                                  granularity      => 'ALL',
--                                  cascade          => True);

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
      LDM_SBV_ETL.SP_SEND_NOTIFICATION (v_dm2_err_addr, 'LIB_FT_ACC_ONLINE_NEW_TT.'|| c_mapping ,v_step || ', SQLERRM = ' || substr(dbms_utility.format_error_stack, 1, 300));

      raise_application_error(-20123,
                              'Error in module ' || c_mapping || ' (' || v_step || ')',
                              True);

  END ft_faccon_new_map_book_sbv;

  -----------------------------------------------------------------
	-- ft_faccon_new_load_book_sbv
	-- author : huyen.trank
	-- created : 20.10.2020
	-- modified : 29.12.2020
  -----------------------------------------------------------------
  PROCEDURE ft_faccon_new_load_book_sbv(p_process_key    IN NUMBER,
                                p_effective_date IN DATE,
                                p_data_type      IN VARCHAR2) IS
    v_step        VARCHAR2(255);
    v_cnt         Integer;
    c_mapping     VARCHAR2(60) := 'FT_FACCON_NEW_LOAD_BOOK_SBV';
    c_table_owner VARCHAR2(40) := 'LDM_SBV';
    c_table_name  VARCHAR2(40) := 'FT_ACCOUNTING_ONLINE_NEW_TT';

  BEGIN

    v_step := 'Start log';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping);

    v_step := 'Merge data into LDM_SBV.ft_accounting_online_new_tt';

    MERGE INTO LDM_SBV.ft_accounting_online_new_tt t
    USING (SELECT
          skf_accounting_online,
		   code_source_system,
		   id_source,
		   date_effective,
		   flag_deleted,
		   code_move_type,
		   amt_accounted_value,
		   date_accounted,
		   date_accounting_move,
		   dtime_acc_system_created,
		   code_transaction_subtype,
		   code_credit_type,
		   code_owner,
		   text_contract_number,
		   text_account_move_desc,
		   skp_contract,
		   id_contract,
		   id_client,
		   id_accounting_event,
		   date_creation,
		   skp_proc_inserted,
		   skp_proc_updated,
		   dtime_inserted,
		   dtime_updated,
           code_contract_term,
           name_status_acquisition,
		   AMT_ACCOUNTED_VALUE_R
        FROM LDM_SBV.stm_accounting_online_new_tt stc) s
    ON (s.id_source = t.id_source)
    WHEN NOT MATCHED THEN
      Insert
        (skf_accounting_online,
		   code_source_system,
		   id_source,
		   date_effective,
		   flag_deleted,
		   code_move_type,
		   amt_accounted_value,
		   date_accounted,
		   date_accounting_move,
		   dtime_acc_system_created,
		   code_transaction_subtype,
		   code_credit_type,
		   code_owner,
		   text_contract_number,
		   text_account_move_desc,
           skp_contract,
		   id_contract,
		   id_client,
		   id_accounting_event,
		   date_creation,
		   skp_proc_inserted,
		   skp_proc_updated,
		   dtime_inserted,
		   dtime_updated,
           code_contract_term,
           name_status_acquisition,
		   AMT_ACCOUNTED_VALUE_R
           )
      VALUES
        (LDM_SBV.S_FT_ACCOUNTING_ONLINE_NEW_TT.nextval,
			s.code_source_system,
			s.id_source,
			s.date_effective,
			s.flag_deleted,
			s.code_move_type,
			s.amt_accounted_value,
			s.date_accounted,
			s.date_accounting_move,
			s.dtime_acc_system_created,
			s.code_transaction_subtype,
			s.code_credit_type,
			s.code_owner,
			s.text_contract_number,
			s.text_account_move_desc,
			s.skp_contract,
			s.id_contract,
			s.id_client,
			s.id_accounting_event,
			s.date_creation,
			s.skp_proc_inserted,
			s.skp_proc_updated,
			s.dtime_inserted,
			s.dtime_updated,
            s.code_contract_term,
            s.name_status_acquisition,
			s.AMT_ACCOUNTED_VALUE_R
            )
    WHEN MATCHED THEN
      Update
         SET 	t.date_effective				= 	s.date_effective,
				t.flag_deleted 					= 	s.flag_deleted,
				t.code_move_type				= 	s.code_move_type,
				t.amt_accounted_value			= 	s.amt_accounted_value,
				t.date_accounted				=	s.date_accounted,
				t.date_accounting_move			=	s.date_accounting_move,
				t.dtime_acc_system_created		=	s.dtime_acc_system_created,
				t.code_transaction_subtype		= 	s.code_transaction_subtype,
				t.code_credit_type				=	s.code_credit_type,
				t.code_owner				    =	s.code_owner,
				t.text_contract_number			=	s.text_contract_number,
				t.text_account_move_desc		=	s.text_account_move_desc,
				t.skp_contract        			=   s.skp_contract,
				t.id_contract					=	s.id_contract,
				t.id_client						=	s.id_client,
				t.id_accounting_event			=	s.id_accounting_event,
				t.date_creation					=	s.date_creation,
				t.skp_proc_updated				=	s.skp_proc_updated,
				t.dtime_updated					=	s.dtime_updated,
                t.code_contract_term			=	s.code_contract_term,
                t.name_status_acquisition		=	s.name_status_acquisition,
				t.AMT_ACCOUNTED_VALUE_R 		=	s.AMT_ACCOUNTED_VALUE_R
                ;

    v_cnt := SQL%ROWCOUNT;
    COMMIT;

    v_step := 'Write record count';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping,
                                           p_sel          => v_cnt,
                                           p_mrg          => v_cnt);

--    v_step := 'Calculate statistics';
--    dbms_stats.gather_table_stats(ownname          => c_table_owner,
--                                  tabname          => c_table_name,
--                                  estimate_percent => dbms_stats.auto_sample_size,
--                                  degree           => 4,
--                                  granularity      => 'ALL',
--                                  cascade          => True);

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
      LDM_SBV_ETL.SP_SEND_NOTIFICATION (v_dm2_err_addr, 'LIB_FT_ACC_ONLINE_NEW_TT.'|| c_mapping ,v_step || ', SQLERRM = ' || substr(dbms_utility.format_error_stack, 1, 300));

      raise_application_error(-20123,
                              'Error in module ' || c_mapping || ' (' || v_step || ')',
                              True);

  END ft_faccon_new_load_book_sbv;

END lib_ft_acc_online_new_tt;