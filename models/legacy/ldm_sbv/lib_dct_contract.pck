/*
-- WF : 
    DIM_CONTRACT_MAP_SBV -> DIM_CONTRACT_UPDATE_CT_SBV -> DIM_CONTRACT_LOAD_SBV -> DIM_CONTR_LATE_MAP_SBV 
        -> DIM_CONTRACT_UPDATE_CT_SBV2 (DIM_CONTRACT_UPDATE_CT_SBV) 
            -> DIM_CONTRACT_LOAD_SBV2 (DIM_CONTRACT_LOAD_SBV)
*/
---------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE             LDM_SBV_ETL.lib_dct_contract IS

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

  PROCEDURE dim_contract_map_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2);

  PROCEDURE dim_contract_load_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2);

  PROCEDURE dim_contract_update_ct_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2);

 -- PROCEDURE dim_contract_stat_update_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2);

  PROCEDURE dim_contract_wo_update_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2);

  PROCEDURE dim_contract_wo_mm_update_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2);

  PROCEDURE dim_contract_sbv_update_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2);

  PROCEDURE dim_contract_ldm_sbv_scb_update_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2);

  PROCEDURE dim_contr_late_map_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2);

END lib_dct_contract;
/


CREATE OR REPLACE PACKAGE Body             LDM_SBV_ETL.lib_dct_contract IS

 -----------------------------------------------------------------
  -- dim_contract_map_sbv
  -- author : huyen.trank
  -- created : 26.10.2020
  -- modified : 29.12.2020
  -----------------------------------------------------------------
  
  v_dm2_err_addr      VARCHAR2(255)   := 'Errors - Notification - VN Data mart 2 <4908ab62.homecreditgroup.onmicrosoft.com@emea.teams.ms>';
  
  PROCEDURE dim_contract_map_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2) IS
    v_step        VARCHAR2(255);
    v_cnt         Integer;
    c_mapping     VARCHAR2(60) := 'DIM_CONTRACT_MAP_SBV';
    c_table_owner VARCHAR2(40) := 'LDM_SBV';
    c_table_name  VARCHAR2(40) := 'STM_CONTRACT';

  BEGIN

    v_step := 'Start log';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping);

    v_step := 'Truncate table ' || c_table_owner || '.' || c_table_name;
    LDM_SBV.truncate_table(c_table_owner, c_table_name);

    v_step := 'Insert data into LDM_SBV.STM_CONTRACT';

  Insert /*+APPEND */
    INTO LDM_SBV.STM_CONTRACT
      (
           SKP_CONTRACT
        , CODE_SOURCE_SYSTEM
        , ID_SOURCE
        , DATE_EFFECTIVE
        , FLAG_DELETED
        , SKP_PROC_INSERTED
        , SKP_PROC_UPDATED
        , DTIME_INSERTED
        , DTIME_UPDATED
        , ID_CONTRACT
        , ID_CLIENT
        , ID_PRODUCT
        , ID_CUID
        , DTIME_SIGNATURE_CONTRACT
        , CNT_INSTALMENT
        , DATE_FIRST_DUE
        , DTIME_DISBURSEMENT
        , CODE_CREDIT_STATUS
        , CODE_CREDIT_TYPE
        , TEXT_CONTRACT_NUMBER
        , DATE_CREATION
        , CODE_SBV_REPORT_RISK_GROUP
        , FLAG_RISK_GROUP_WO_MANUALLY
        , DATE_CONTRACT_WRITE_OFF
        , FLAG_WRITTEN_OFF
        , ID_SOURCE_SBV_LAST
        , FLAG_OBSERVATION_PERIOD_USED
        , CODE_CONTRACT_TERM
        , CODE_SALESROOM
        , NAME_SALESROOM
        , SALESROOM_ADDRESS
        , SALESROOM_PROVINCE
        , CODE_PRODUCT
        , RATE_INTEREST
        , RATE_INTEREST_INITIAL
        , AMT_CREDIT
        , AMT_CREDIT_TOTAL
        , CODE_ACCOUNTING_METHOD
        , NAME_PRODUCT
        , SKP_CREDIT_OWNER
        , CODE_CREDIT_OWNER
        , DTIME_PAIDOFF
        , DTIME_CLOSE
        , DTIME_ACTIVATION
      )

     with w_hom as
    (
        select
          hc.id id_contract
        , hc.code_change_type
        , to_number(client.id_source) id_client
        , product.id_product id_product
        , client.id_cuid
        , fp.terms cnt_instalment
        , product.code_product code_product
        , fp.interest_rate rate_interest
        , fp.net_credit_limit_amount
        , fp.net_credit_amount
        , fp.provided_credit_limit_amount
        , fp.credit_amount
        , case when hc.status = 'T' then d_def_value_date_future else fp.first_due_date end date_first_due
        , product_profile.code_accounting_method
--        , hs.code code_salesroom
--        , hs.name name_salesroom
--        , ha.house_number || ' - ' || ha.street_name || ' - ' || ha.town_value || ' - ' || hd.Name || ' - ' || hr.name salesroom_address
--        , hr.name salesroom_province
        , fsat.code_salesroom
        , fsat.name_salesroom
        , fsat.TEXT_SALESROOM_ADDRESS as salesroom_address
        , fsat.NAME_SALE_PROVINCE as salesroom_province
        , cst.dtime_signature_contract
        , hc.status code_credit_status
        , hc.contract_code text_contract_number
        , trunc(hc.creation_date) date_creation
        , hc.contract_type
        , product_profile.code_credit_type
         , product.name_product as name_product
         , credit_owner.skp_credit_owner
         , credit_owner.code_credit_owner
         , cst.dtime_paidoff
         , cst.dtime_close
         , cst.dtime_activation
        from owner_int.in_hom_contract hc
            JOIN owner_int.in_hom_deal deal ON hc.deal_id = deal.id
                              and deal.code_load_status IN ('OK', 'LOAD')
                              and deal.code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
                              and deal.date_effective_inserted = p_effective_date
            JOIN LDM_SBV.dct_client client ON deal.client_id = to_number(client.id_source)
            JOIN LDM_SBV.dct_product product on hc.product_id = product.id_product
            JOIN  LDM_SBV.dct_product_profile product_profile on product.code_product_profile = product_profile.code_product_profile
            join LDM_SBV.ft_salesroom_address_tt fsat on hc.salesroom_code = fsat.code_salesroom and fsat.flag_deleted = v_flag_N
--            join owner_int.vh_hom_salesroom hs on hc.salesroom_code = hs.code
--            join owner_int.vh_hom_salesroom2address s2a on hs.id = s2a.salesroom_id
--            join owner_int.vh_hom_address ha on s2a.address_id = ha.id
--            left join owner_int.vh_hom_district hd on ha.district_code = hd.code
--            join owner_int.vh_hom_region hr on ha.region_code = hr.code
            left JOIN owner_int.in_hom_financial_parameters fp on hc.id = fp.contract_id and fp.archived = 0
                              and fp.code_load_status IN ('OK', 'LOAD')
                              and fp.code_change_type IN ('X', 'I', 'U', 'M', 'N')
                              and fp.date_effective_inserted = p_effective_date
           left join
            (
                select
                    contract_id,
                    max(case when status = 'N' then creation_date end) as dtime_signature_contract,
                    max(case when status = 'L' then creation_date end) as dtime_paidoff,
                    max(case when status = 'A' then creation_date end) as dtime_activation,
                    max(case when status = 'K' then creation_date end) as dtime_close
                from
                owner_int.in_hom_contract_status_trans
                where             status in ('N', 'L', 'K', 'A')
                                  and transferred_manually = 0
                                  and code_load_status IN ('OK', 'LOAD')
                                  and code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
                                  and date_effective_inserted = p_effective_date
                group by  contract_id
            ) cst on cst.contract_id = fp.contract_id
             join(
                  select

                        osh.ID_CONTRACT as ID_CONTRACT
                        ,osh.skp_credit_owner
                        ,osh.CODE_CONTRACT_OWNER as code_credit_owner
                 from
                         LDM_SBV.STP_HOM_OWNERSHIP osh
                where osh.date_valid_to = d_def_value_date_future
            ) credit_owner on hc.id =   credit_owner.ID_CONTRACT
        where fsat.code_status = 'a'
            and     hc.code_load_status IN ('OK', 'LOAD')
            and hc.code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
            and hc.date_effective_inserted = p_effective_date
        group by
             hc.id
            , hc.code_change_type
            , client.id_source
            , product.id_product
            , client.id_cuid
            , fp.terms
            , fp.first_due_date
            , hc.status
            , hc.contract_code
            , hc.creation_date
            , fsat.code_salesroom
            , fsat.name_salesroom
            , fsat.TEXT_SALESROOM_ADDRESS
            , fsat.NAME_SALE_PROVINCE
--            , hs.code
--            , hs.name
--            , ha.house_number
--            , ha.street_name
--            , ha.town_value
--            , hd.Name
--            , hr.name
            , product.code_product
            , fp.interest_rate
            , fp.net_credit_limit_amount
            , fp.net_credit_amount
            , fp.provided_credit_limit_amount
            , fp.credit_amount
            , product_profile.code_accounting_method
            , hc.contract_type
            , product_profile.code_credit_type
            , cst.dtime_signature_contract
            , product.name_product
            , credit_owner.skp_credit_owner
            , credit_owner.code_credit_owner
            , cst.dtime_paidoff
            , cst.dtime_close
            , cst.dtime_activation
    )
, w_contract as
    (select
          wh.id_contract
        , wh.code_change_type
        , v_hom_code_source_system code_source_system
        , to_char(wh.id_contract) id_source
        , wh.id_client
        , wh.id_product
        , wh.id_cuid
        , wh.dtime_signature_contract
        , wh.cnt_instalment
        , wh.date_first_due
        , max(nvl(  case when op.TRANSACTION_TYPE = 'PTR' and op.status = 'P' then op.update_date
                         when op.TRANSACTION_TYPE = 'CL' and op.status = 'D' then op.update_date end,  d_def_value_date_hist)) as dtime_disbursement
        , wh.code_credit_status
        , wh.code_credit_type
        , wh.text_contract_number
        , wh.date_creation
        , wh.code_salesroom
        , wh.name_salesroom
        , wh.salesroom_address
        , wh.salesroom_province
        , wh.code_product
        , wh.rate_interest
        , NULL rate_interest_initial
        , wh.net_credit_limit_amount
        , wh.net_credit_amount
        , wh.provided_credit_limit_amount
        , wh.credit_amount
        , wh.code_accounting_method
        , wh.name_product
        , wh.skp_credit_owner
        , wh.code_credit_owner
        , wh.dtime_paidoff
        , wh.dtime_close
        , wh.dtime_activation
    from w_hom wh
        left join owner_int.in_hom_outgoing_payment op on wh.id_contract = op.contract_id
              and op.status in( 'D', 'P') and op.TRANSACTION_TYPE in ('PTR', 'CL')
              and op.code_load_status IN ('OK', 'LOAD')
              and op.code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
              and op.date_effective_inserted = p_effective_date
    where wh.contract_type <> 'REL'
    group by wh.id_contract
        , wh.code_change_type
        , wh.id_client
        , wh.id_product
        , wh.id_cuid
        , wh.dtime_signature_contract
        , wh.cnt_instalment
        , wh.date_first_due
        , wh.code_credit_status
        , wh.code_credit_type
        , wh.text_contract_number
        , wh.date_creation
        , wh.code_salesroom
        , wh.name_salesroom
        , wh.salesroom_address
        , wh.salesroom_province
        , wh.code_product
        , wh.rate_interest
        , wh.net_credit_limit_amount
        , wh.net_credit_amount
        , wh.provided_credit_limit_amount
        , wh.credit_amount
        , wh.code_accounting_method
        , wh.name_product
        , wh.skp_credit_owner
        , wh.code_credit_owner
        , wh.dtime_paidoff
        , wh.dtime_close
        , wh.dtime_activation
    union all
    select
        wh.id_contract
        , ac.code_change_type
        , v_cbus_code_source_system code_source_system
        , to_char(ac.id_contract) id_source
        , wh.id_client
        , wh.id_product
        , wh.id_cuid
        , wh.dtime_signature_contract
        , wh.cnt_instalment
        , wh.date_first_due
        , d_def_value_date_hist dtime_disbursement
        , wh.code_credit_status
        , wh.code_credit_type
        , wh.text_contract_number
        , trunc(ac.date_creation) as date_creation
        , wh.code_salesroom
        , wh.name_salesroom
        , wh.salesroom_address
        , wh.salesroom_province
        , wh.code_product
        , NULL rate_interest
        , ati.percentage_value as rate_interest_initial
        , wh.net_credit_limit_amount
        , wh.net_credit_amount
        , wh.provided_credit_limit_amount
        , wh.credit_amount
        , wh.code_accounting_method
        , wh.name_product
        , wh.skp_credit_owner
        , wh.code_credit_owner
        , wh.dtime_paidoff
        , wh.dtime_close
        , wh.dtime_activation
    from w_hom wh
        join LDM_SBV.stp_cbus_a_account ac on wh.text_contract_number = ac.text_contract_number
        left join owner_int.in_cbus_a_contract_snapshot cs on cs.contract_id = ac.id_contract AND cs.VALID_TO IS NULL
                      and cs.code_load_status IN ('OK', 'LOAD')
                      and cs.code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
                      and cs.date_effective_inserted = p_effective_date
        left JOIN  LDM_SBV.dct_cbus_a_tariff at ON cs.tariff_code = at.code_tariff and at.DATE_VALID_TO = date '3000-01-01'
        left join
        (
                    select  max(num_percentage_value) percentage_value,
                            id_tariff
                    from LDM_SBV.dct_cbus_a_tariff_item where code_tariff_item_type in ('INTEREST_CASH', 'INTEREST_CASHLESS')
                    group by id_tariff
        ) ati on  ati.id_tariff = at.id_tariff
    where wh.contract_type = 'REL'
    group by wh.id_contract
        , ac.code_change_type
        , ac.id_contract
        , wh.id_client
        , wh.id_product
        , wh.id_cuid
        , wh.dtime_signature_contract
        , wh.cnt_instalment
        , wh.date_first_due
        , wh.code_credit_status
        , wh.code_credit_type
        , wh.text_contract_number
        , ac.date_creation
        , wh.code_salesroom
        , wh.name_salesroom
        , wh.salesroom_address
        , wh.salesroom_province
        , wh.code_product
        , wh.net_credit_limit_amount
        , wh.net_credit_amount
        , wh.provided_credit_limit_amount
        , wh.credit_amount
        , wh.code_accounting_method
        , wh.name_product
        , ati.percentage_value
        , wh.skp_credit_owner
        , wh.code_credit_owner
        , wh.dtime_paidoff
        , wh.dtime_close
        , wh.dtime_activation
    )
      SELECT
             NULL AS SKP_CONTRACT,
             i.code_source_system                                         AS code_source_system,
             i.id_source                                                 AS id_source,
             p_effective_date                                             AS date_effective,
             i.flag_deleted                                             AS flag_deleted,
             p_process_key                                                 AS skp_proc_inserted,
             p_process_key                                              AS skp_proc_updated,
             sysdate                                                     AS dtime_inserted,
             sysdate                                                     AS dtime_updated,
             i.id_contract                                                AS id_contract,
             i.id_client                                                AS id_client,
             i.id_product                                                AS id_product,
             i.id_cuid                                                  AS id_cuid,
             nvl(i.dtime_signature_contract, d_def_value_date_future)     AS dtime_signature_contract,
             i.cnt_instalment                                             AS cnt_instalment,
             nvl(i.date_first_due, d_def_value_date_future)             AS date_first_due,
             nvl(i.dtime_disbursement, d_def_value_date_hist)             AS dtime_disbursement,
             nvl(i.code_credit_status, v_xna)                             AS code_credit_status,
              nvl(i.code_credit_type, v_xna)                               AS code_credit_type    ,
             nvl(i.text_contract_number, v_xna)                           AS text_contract_number    ,
             nvl(i.date_creation,  d_def_value_date_future)               AS date_creation    ,
             '1'                                                        AS CODE_SBV_REPORT_RISK_GROUP,
             v_flag_N                                                     AS FLAG_RISK_GROUP_WO_MANUALLY,
             d_def_value_date_future                                     AS DATE_CONTRACT_WRITE_OFF,
             v_flag_N                                                     AS FLAG_WRITTEN_OFF,
             v_xna                                                         AS ID_SOURCE_SBV_LAST,
             v_flag_N                                                   as FLAG_OBSERVATION_PERIOD_USED,
             nvl(i.code_contract_term, v_xna)                           AS code_contract_term
            , CODE_SALESROOM
            , NAME_SALESROOM
            , salesroom_address
            , SALESROOM_PROVINCE
            , code_product
            , rate_interest
            , rate_interest_initial
            , amt_credit
            , amt_credit_total
            , code_accounting_method
            , nvl(name_product, v_xna) as name_product
            , nvl(i.skp_credit_owner, n_minus_one) as skp_credit_owner
            , nvl(i.code_credit_owner, v_xna) as code_credit_owner
            , nvl(i.dtime_paidoff, d_def_value_date_future) as dtime_paidoff
            , nvl(i.dtime_close, d_def_value_date_future) as dtime_close
            , nvl(i.dtime_activation, d_def_value_date_future) as dtime_activation
        FROM (
            SELECT
                code_source_system
                , id_source
                , CASE WHEN code_change_type = v_code_change_type_del THEN v_flag_Y ELSE v_flag_N END AS FLAG_DELETED
                , id_contract
                , id_client
                , id_product
                , id_cuid
                , dtime_signature_contract
                , cnt_instalment
                , date_first_due
                , dtime_disbursement
                , code_credit_status
                , code_credit_type
                , CASE
                    WHEN code_credit_type IN ('COL','CAL')
                        AND nvl(cnt_instalment, 0) < 12 THEN 'SHORT'
                    WHEN code_credit_type = 'CAL'
                        AND nvl(cnt_instalment, 0) = 12 
                        AND (dtime_disbursement = d_def_value_date_future OR dtime_disbursement IS NULL) THEN 'MEDIUM'
                    WHEN code_credit_type = 'CAL'
                        AND nvl(cnt_instalment, 0) = 60 
                        AND (dtime_disbursement = d_def_value_date_future OR dtime_disbursement IS NULL) THEN 'LONG'    
                    WHEN code_credit_type = 'CAL'
                        AND nvl(cnt_instalment, 0) = 12
                        AND TRUNC(date_first_due, 'MM') <= TRUNC(dtime_disbursement, 'MM') THEN 'SHORT'
                    WHEN code_credit_type = 'CAL'
                        AND nvl(cnt_instalment, 0) = 12
                        AND TRUNC(date_first_due, 'MM') = TRUNC(ADD_MONTHS(dtime_disbursement, 1), 'MM')
                        AND to_number(to_char(date_first_due, 'DD')) <= to_number(to_char(dtime_disbursement, 'DD')) THEN 'SHORT'
                    WHEN code_credit_type = 'COL'
                        AND nvl(cnt_instalment, 0) = 12
                        AND TRUNC(date_first_due, 'MM') <= TRUNC(dtime_signature_contract, 'MM') THEN
                       'SHORT'
                    WHEN code_credit_type = 'COL'
                        AND nvl(cnt_instalment, 0) = 12
                        AND TRUNC(date_first_due, 'MM') = TRUNC(ADD_MONTHS(dtime_signature_contract, 1), 'MM')
                        AND to_number(to_char(date_first_due, 'DD')) <= to_number(to_char(dtime_signature_contract, 'DD')) THEN
                       'SHORT'
                    WHEN code_credit_type IN ('COL', 'CAL')
                        AND nvl(cnt_instalment, 0) > 12
                        AND nvl(cnt_instalment, 0) < 60 THEN
                       'MEDIUM'
                    WHEN code_credit_type = 'COL'
                        AND nvl(cnt_instalment, 0) = 12
                        AND TRUNC(date_first_due, 'MM') = TRUNC(ADD_MONTHS(dtime_signature_contract, 1), 'MM')
                        AND to_number(to_char(date_first_due, 'DD')) > to_number(to_char(dtime_signature_contract, 'DD')) THEN
                       'MEDIUM'
                    WHEN code_credit_type = 'COL'
                        AND nvl(cnt_instalment, 0) = 12
                        AND TRUNC(date_first_due, 'MM') >= TRUNC(ADD_MONTHS(dtime_signature_contract, 2), 'MM') THEN
                       'MEDIUM'
                    WHEN code_credit_type = 'COL'
                        AND nvl(cnt_instalment, 0) = 60
                        AND TRUNC(date_first_due, 'MM') <= TRUNC(dtime_signature_contract, 'MM') THEN
                       'MEDIUM'
                    WHEN code_credit_type = 'COL'
                        AND nvl(cnt_instalment, 0) = 60
                        AND TRUNC(date_first_due, 'MM') = TRUNC(ADD_MONTHS(dtime_signature_contract, 1), 'MM')
                        AND to_number(to_char(date_first_due, 'DD')) <= to_number(to_char(dtime_signature_contract, 'DD')) THEN
                       'MEDIUM'
                    WHEN code_credit_type = 'CAL'
                        AND nvl(cnt_instalment, 0) = 12
                        AND TRUNC(date_first_due, 'MM') = TRUNC(ADD_MONTHS(dtime_disbursement, 1), 'MM')
                        AND to_number(to_char(date_first_due, 'DD')) > to_number(to_char(dtime_disbursement, 'DD')) THEN 'MEDIUM'
                    WHEN code_credit_type = 'CAL'
                        AND nvl(cnt_instalment, 0) = 12
                        AND TRUNC(date_first_due, 'MM') >= TRUNC(ADD_MONTHS(dtime_disbursement, 2), 'MM') THEN 'MEDIUM'
                    WHEN code_credit_type = 'CAL'
                        AND nvl(cnt_instalment, 0) = 60
                        AND TRUNC(date_first_due, 'MM') <= TRUNC(dtime_disbursement, 'MM') THEN 'MEDIUM'
                    WHEN code_credit_type = 'CAL'
                        AND nvl(cnt_instalment, 0) = 60
                        AND TRUNC(date_first_due, 'MM') = TRUNC(ADD_MONTHS(dtime_disbursement, 1), 'MM')
                        AND to_number(to_char(date_first_due, 'DD')) <= to_number(to_char(dtime_disbursement, 'DD')) THEN 'MEDIUM'
                    WHEN code_credit_type IN ('COL', 'CAL')
                        AND nvl(cnt_instalment, 0) > 60 THEN 'LONG'
                    WHEN code_credit_type = 'COL'
                        AND nvl(cnt_instalment, 0) = 60
                        AND TRUNC(date_first_due, 'MM') = TRUNC(ADD_MONTHS(dtime_signature_contract, 1), 'MM')
                        AND to_number(to_char(date_first_due, 'DD')) > to_number(to_char(dtime_signature_contract, 'DD')) THEN
                       'LONG'
                    WHEN code_credit_type = 'COL'
                        AND nvl(cnt_instalment, 0) = 60
                        AND TRUNC(date_first_due, 'MM') >= TRUNC(ADD_MONTHS(dtime_signature_contract, 2), 'MM') THEN
                       'LONG'
                    WHEN code_credit_type = 'CAL'
                        AND nvl(cnt_instalment, 0) = 60
                        AND TRUNC(date_first_due, 'MM') = TRUNC(ADD_MONTHS(dtime_disbursement, 1), 'MM')
                        AND to_number(to_char(date_first_due, 'DD')) > to_number(to_char(dtime_disbursement, 'DD')) THEN 'LONG'
                    WHEN code_credit_type = 'CAL'
                        AND nvl(cnt_instalment, 0) = 60
                        AND TRUNC(date_first_due, 'MM') >= TRUNC(ADD_MONTHS(dtime_disbursement, 2), 'MM') THEN 'LONG'
                    WHEN code_credit_type = 'REV' THEN 'SHORT'
                    WHEN code_credit_status = 'T'
                        AND date_first_due = date '3000-01-01' THEN 'XNA'
                END AS code_contract_term
                , text_contract_number
                , date_creation
                , code_salesroom
                , name_salesroom
                , salesroom_address
                , Salesroom_Province
                , code_product
                , rate_interest
                , rate_interest_initial
                , case
                    when code_credit_type = 'REV' then net_credit_limit_amount
                    else net_credit_amount
                  end amt_credit
                , case
                    when code_credit_type = 'REV' then provided_credit_limit_amount
                    else credit_amount
                  end amt_credit_total
                , code_accounting_method
                , name_product
                ,skp_credit_owner
                ,code_credit_owner
                ,dtime_paidoff
                ,dtime_close
                ,dtime_activation
            from w_contract
            ) i;


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
      LDM_SBV_ETL.SP_SEND_NOTIFICATION (v_dm2_err_addr, 'LIB_DCT_CONTRACT.'|| c_mapping ,v_step || ', SQLERRM = ' || substr(dbms_utility.format_error_stack, 1, 300));

      raise_application_error(-20123,
                              'Error in module ' || c_mapping || ' (' || v_step || ')',
                              True);

  END dim_contract_map_sbv;

  -----------------------------------------------------------------
  -- dim_contract_load_sbv
  -- author : huyen.trank
   -- created : 26.10.2020
  -- modified : 29.12.2020
  -----------------------------------------------------------------
  PROCEDURE dim_contract_load_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2) IS
    v_step        VARCHAR2(255);
    v_cnt         Integer;
    c_mapping     VARCHAR2(60) := 'DIM_CONTRACT_LOAD_SBV';
    c_table_owner VARCHAR2(40) := 'LDM_SBV';
    c_table_name  VARCHAR2(40) := 'DCT_CONTRACT';

  BEGIN

    v_step := 'Start log';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping);

    v_step := 'Merge data into LDM_SBV.DCT_CONTRACT';

    MERGE
    INTO LDM_SBV.DCT_CONTRACT t
    USING (SELECT
                      SKP_CONTRACT
                    , CODE_SOURCE_SYSTEM
                    , ID_SOURCE
                    , DATE_EFFECTIVE
                    , FLAG_DELETED
                    , SKP_PROC_INSERTED
                    , SKP_PROC_UPDATED
                    , DTIME_INSERTED
                    , DTIME_UPDATED
                    , ID_CONTRACT
                    , ID_CLIENT
                    , ID_PRODUCT
                    , ID_CUID
                    , DTIME_SIGNATURE_CONTRACT
                    , CNT_INSTALMENT
                    , DATE_FIRST_DUE
                    , DTIME_DISBURSEMENT
                    , CODE_CREDIT_STATUS
                    , CODE_CREDIT_TYPE
                    , TEXT_CONTRACT_NUMBER
                    , DATE_CREATION
                    , CODE_SBV_REPORT_RISK_GROUP
                    , FLAG_RISK_GROUP_WO_MANUALLY
                    , DATE_CONTRACT_WRITE_OFF
                    , FLAG_WRITTEN_OFF
                    , ID_SOURCE_SBV_LAST
                    , FLAG_OBSERVATION_PERIOD_USED
                    , CODE_CONTRACT_TERM
                    , CODE_SALESROOM
                    , NAME_SALESROOM
                    , SALESROOM_ADDRESS
                    , SALESROOM_PROVINCE
                    , CODE_PRODUCT
                    , RATE_INTEREST
                    , RATE_INTEREST_INITIAL
                    , AMT_CREDIT
                    , AMT_CREDIT_TOTAL
                    , CODE_ACCOUNTING_METHOD
                    , NAME_PRODUCT
                    , SKP_CREDIT_OWNER
                    , CODE_CREDIT_OWNER
                    , DTIME_PAIDOFF
                    , DTIME_CLOSE
                    , DTIME_ACTIVATION
             FROM LDM_SBV.STM_CONTRACT
             where flag_deleted= v_flag_N
             ) s
    ON (s.id_source = t.id_source)
    WHEN NOT MATCHED THEN
      Insert
        (          SKP_CONTRACT
                , CODE_SOURCE_SYSTEM
                , ID_SOURCE
                , DATE_EFFECTIVE
                , FLAG_DELETED
                , SKP_PROC_INSERTED
                , SKP_PROC_UPDATED
                , DTIME_INSERTED
                , DTIME_UPDATED
                , ID_CONTRACT
                , ID_CLIENT
                , ID_PRODUCT
                , ID_CUID
                , DTIME_SIGNATURE_CONTRACT
                , CNT_INSTALMENT
                , DATE_FIRST_DUE
                , DTIME_DISBURSEMENT
                , CODE_CREDIT_STATUS
                , CODE_CREDIT_TYPE
                , TEXT_CONTRACT_NUMBER
                , DATE_CREATION
                , CODE_SBV_REPORT_RISK_GROUP
                , FLAG_RISK_GROUP_WO_MANUALLY
                , DATE_CONTRACT_WRITE_OFF
                , FLAG_WRITTEN_OFF
                , ID_SOURCE_SBV_LAST
                , FLAG_OBSERVATION_PERIOD_USED
                , CODE_CONTRACT_TERM
                , CODE_SALESROOM
                , NAME_SALESROOM
                , SALESROOM_ADDRESS
                , SALESROOM_PROVINCE
                , CODE_PRODUCT
                , RATE_INTEREST
                , RATE_INTEREST_INITIAL
                , AMT_CREDIT
                , AMT_CREDIT_TOTAL
                , CODE_ACCOUNTING_METHOD
                , NAME_PRODUCT
                , SKP_CREDIT_OWNER
                , CODE_CREDIT_OWNER
                , DTIME_PAIDOFF
                , DTIME_CLOSE
                , DTIME_ACTIVATION
                )
      VALUES
            (    LDM_SBV.S_DCT_CONTRACT.nextval
                ,s.CODE_SOURCE_SYSTEM
                ,s.ID_SOURCE
                ,s.DATE_EFFECTIVE
                ,s.FLAG_DELETED
                ,s.SKP_PROC_INSERTED
                ,s.SKP_PROC_UPDATED
                ,s.DTIME_INSERTED
                ,s.DTIME_UPDATED
                ,s.ID_CONTRACT
                ,s.ID_CLIENT
                ,s.ID_PRODUCT
                ,s.ID_CUID
                ,s.DTIME_SIGNATURE_CONTRACT
                ,s.CNT_INSTALMENT
                ,s.DATE_FIRST_DUE
                ,s.DTIME_DISBURSEMENT
                ,s.CODE_CREDIT_STATUS
                ,s.CODE_CREDIT_TYPE
                ,s.TEXT_CONTRACT_NUMBER
                ,s.DATE_CREATION
                ,s.CODE_SBV_REPORT_RISK_GROUP
                ,s.FLAG_RISK_GROUP_WO_MANUALLY
                ,s.DATE_CONTRACT_WRITE_OFF
                ,s.FLAG_WRITTEN_OFF
                ,s.ID_SOURCE_SBV_LAST
                ,s.FLAG_OBSERVATION_PERIOD_USED
                ,s.CODE_CONTRACT_TERM
                ,s.CODE_SALESROOM
                ,s.NAME_SALESROOM
                ,s.SALESROOM_ADDRESS
                ,s.SALESROOM_PROVINCE
                ,s.CODE_PRODUCT
                ,s.RATE_INTEREST
                ,s.RATE_INTEREST_INITIAL
                ,s.AMT_CREDIT
                ,s.AMT_CREDIT_TOTAL
                ,s.CODE_ACCOUNTING_METHOD
                ,s.NAME_PRODUCT
                ,s.SKP_CREDIT_OWNER
                ,s.CODE_CREDIT_OWNER
                ,s.DTIME_PAIDOFF
                ,s.DTIME_CLOSE
                ,s.DTIME_ACTIVATION
            )
    WHEN MATCHED THEN
      Update
         SET     t.DATE_EFFECTIVE             =    s.DATE_EFFECTIVE ,
                t.FLAG_DELETED                 =    s.FLAG_DELETED ,
                t.SKP_PROC_UPDATED             =    s.SKP_PROC_UPDATED  ,
                t.DTIME_UPDATED             =    s.DTIME_UPDATED     ,
                t.ID_CONTRACT                 =    s.ID_CONTRACT,
                t.ID_CLIENT                 =    s.ID_CLIENT,
                t.ID_PRODUCT                 =    s.ID_PRODUCT,
                t.ID_CUID                     =    s.ID_CUID,
                t.DTIME_SIGNATURE_CONTRACT     =    case when t.DTIME_SIGNATURE_CONTRACT = d_def_value_date_future
                                                then
                                                    s.DTIME_SIGNATURE_CONTRACT
                                                else
                                                    t.DTIME_SIGNATURE_CONTRACT
                                                end ,
                t.CNT_INSTALMENT            =    case when t.CNT_INSTALMENT is not null and s.CNT_INSTALMENT is null
                                                then t.CNT_INSTALMENT
                                                else
                                                     s.CNT_INSTALMENT
                                                end,
                t.DATE_FIRST_DUE             =    s.DATE_FIRST_DUE ,
                t.DTIME_DISBURSEMENT         =    case when t.DTIME_DISBURSEMENT = d_def_value_date_hist
                                                then s.DTIME_DISBURSEMENT
                                                else
                                                    t.DTIME_DISBURSEMENT
                                                end ,
                t.CODE_CREDIT_STATUS         =    s.CODE_CREDIT_STATUS ,
                t.CODE_CREDIT_TYPE            =    s.CODE_CREDIT_TYPE ,
                t.TEXT_CONTRACT_NUMBER        =    s.TEXT_CONTRACT_NUMBER ,
                t.DATE_CREATION                =    s.DATE_CREATION,
                t.CODE_CONTRACT_TERM                =    case when s.DTIME_DISBURSEMENT <> d_def_value_date_hist
                                                        then s.CODE_CONTRACT_TERM
--                                                        WHEN NVL(t.CODE_CONTRACT_TERM,v_xna) != NVL(s.CODE_CONTRACT_TERM,v_xna) AND NVL(s.CODE_CONTRACT_TERM,v_xna) != v_xna 
--                                                        THEN s.CODE_CONTRACT_TERM --THUAN.DANG 20210621 CBL-17575: USER ONLY CHANGE CNT_INSTALMENT AND THE CODE_CONTRACT_TERM COLUMN ALSO NEED TO CHANGE 
--                                                        THUAN.DANG 20220919 MOVING TO PACKAGE LIB_UPD_CONTRACT . WHEN USER EXECUTED LRES/PAYHOLD AND CHANGE CNT_INSTALMENT THEN THE CODE_CONTRACT_TERM WILL BE UPDATED
                                                        else
                                                            t.CODE_CONTRACT_TERM
                                                        end ,
                t.CODE_SALESROOM                    =    s.CODE_SALESROOM,
                t.NAME_SALESROOM                    =    s.NAME_SALESROOM,
                t.SALESROOM_ADDRESS                    =    s.SALESROOM_ADDRESS,
                t.SALESROOM_PROVINCE                =    s.SALESROOM_PROVINCE,
                t.CODE_PRODUCT                        =    s.CODE_PRODUCT,
                t.RATE_INTEREST                        =    case when t.RATE_INTEREST is not null and s.RATE_INTEREST is null
                                                        then t.RATE_INTEREST
                                                        else
                                                             s.RATE_INTEREST
                                                        end,
                t.RATE_INTEREST_INITIAL                =    case when t.RATE_INTEREST_INITIAL is not null and s.RATE_INTEREST_INITIAL is null
                                                        then t.RATE_INTEREST_INITIAL
                                                        else
                                                             s.RATE_INTEREST_INITIAL
                                                        end,
                t.AMT_CREDIT                        =    case when t.AMT_CREDIT is not null and s.AMT_CREDIT is null
                                                        then t.AMT_CREDIT
                                                        else
                                                             s.AMT_CREDIT
                                                        end,
                t.AMT_CREDIT_TOTAL                    =    case when t.AMT_CREDIT_TOTAL is not null and s.AMT_CREDIT_TOTAL is null
                                                        then t.AMT_CREDIT_TOTAL
                                                        else
                                                             s.AMT_CREDIT_TOTAL
                                                        end,
                t.CODE_ACCOUNTING_METHOD            =   s.CODE_ACCOUNTING_METHOD,
                t.NAME_PRODUCT                      =   s.NAME_PRODUCT,
                t.SKP_CREDIT_OWNER                  =   s.SKP_CREDIT_OWNER,
                t.CODE_CREDIT_OWNER                 =   s.CODE_CREDIT_OWNER,
                t.DTIME_PAIDOFF                     =   case when t.DTIME_PAIDOFF = d_def_value_date_future
                                                        then
                                                            s.DTIME_PAIDOFF
                                                        else
                                                            t.DTIME_PAIDOFF
                                                        end,
                t.DTIME_CLOSE                     =   case when t.DTIME_CLOSE = d_def_value_date_future or t.DTIME_CLOSE is null
                                                        then
                                                            s.DTIME_CLOSE
                                                        else
                                                            t.DTIME_CLOSE
                                                        end,
                t.DTIME_ACTIVATION                =   case when t.DTIME_ACTIVATION = d_def_value_date_future or t.DTIME_ACTIVATION is null
                                                        then
                                                            s.DTIME_ACTIVATION
                                                        else
                                                            t.DTIME_ACTIVATION
                                                        end                                                        
    ;

    v_cnt := SQL%ROWCOUNT;
    
    UPDATE ldm_sbv.dct_contract b
    SET (b.salesroom_address,b.salesroom_province) = 
        (
            SELECT text_salesroom_address,name_sale_province
            FROM ldm_sbv.ft_salesroom_address_tt a
            WHERE 
                code_status = 'a'
                AND a.code_salesroom = b.code_salesroom
                and a.flag_deleted = v_flag_N
        )
    WHERE b.salesroom_province = v_xna
    ;
  /*THUAN.DCANGT 20221128 ADD UPDATE CODE_CONTRACT_TERM CBL-18911 */
  UPDATE LDM_SBV.DCT_CONTRACT T 
    SET T.CODE_CONTRACT_TERM =
              (CASE
                WHEN T.CODE_CREDIT_TYPE IN ('COL', 'CAL')
                     AND NVL(CNT_INSTALMENT, 0) < 12 THEN
                 'SHORT'
                WHEN code_credit_type = 'CAL'
                    AND nvl(cnt_instalment, 0) = 12 
                    AND (dtime_disbursement = d_def_value_date_future OR dtime_disbursement IS NULL) THEN 'MEDIUM'
                WHEN code_credit_type = 'CAL'
                    AND nvl(cnt_instalment, 0) = 60 
                    AND (dtime_disbursement = d_def_value_date_future OR dtime_disbursement IS NULL) THEN 'LONG'    
                WHEN T.CODE_CREDIT_TYPE = 'CAL'
                     AND NVL(T.CNT_INSTALMENT, 0) = 12
                     AND TRUNC(T.DATE_FIRST_DUE, 'MM') <= TRUNC(T.DTIME_DISBURSEMENT, 'MM') THEN
                 'SHORT'
                WHEN T.CODE_CREDIT_TYPE = 'CAL'
                     AND NVL(T.CNT_INSTALMENT, 0) = 12
                     AND TRUNC(T.DATE_FIRST_DUE, 'MM') =
                     TRUNC(ADD_MONTHS(T.DTIME_DISBURSEMENT, 1), 'MM')
                     AND TO_NUMBER(TO_CHAR(T.DATE_FIRST_DUE, 'DD')) <=
                     TO_NUMBER(TO_CHAR(T.DTIME_DISBURSEMENT, 'DD')) THEN
                 'SHORT'
                WHEN T.CODE_CREDIT_TYPE = 'COL'
                     AND NVL(T.CNT_INSTALMENT, 0) = 12
                     AND TRUNC(T.DATE_FIRST_DUE, 'MM') <=
                     TRUNC(T.DTIME_SIGNATURE_CONTRACT, 'MM') THEN
                 'SHORT'
                WHEN T.CODE_CREDIT_TYPE = 'COL'
                     AND NVL(T.CNT_INSTALMENT, 0) = 12
                     AND TRUNC(T.DATE_FIRST_DUE, 'MM') =
                     TRUNC(ADD_MONTHS(T.DTIME_SIGNATURE_CONTRACT, 1), 'MM')
                     AND TO_NUMBER(TO_CHAR(T.DATE_FIRST_DUE, 'DD')) <=
                     TO_NUMBER(TO_CHAR(T.DTIME_SIGNATURE_CONTRACT, 'DD')) THEN
                 'SHORT'
                WHEN T.CODE_CREDIT_TYPE IN ('COL', 'CAL')
                     AND NVL(T.CNT_INSTALMENT, 0) > 12
                     AND NVL(T.CNT_INSTALMENT, 0) < 60 THEN
                 'MEDIUM'
                WHEN T.CODE_CREDIT_TYPE = 'COL'
                     AND NVL(T.CNT_INSTALMENT, 0) = 12
                     AND TRUNC(T.DATE_FIRST_DUE, 'MM') =
                     TRUNC(ADD_MONTHS(T.DTIME_SIGNATURE_CONTRACT, 1), 'MM')
                     AND TO_NUMBER(TO_CHAR(T.DATE_FIRST_DUE, 'DD')) >
                     TO_NUMBER(TO_CHAR(T.DTIME_SIGNATURE_CONTRACT, 'DD')) THEN
                 'MEDIUM'
                WHEN T.CODE_CREDIT_TYPE = 'COL'
                     AND NVL(T.CNT_INSTALMENT, 0) = 12
                     AND TRUNC(T.DATE_FIRST_DUE, 'MM') >=
                     TRUNC(ADD_MONTHS(T.DTIME_SIGNATURE_CONTRACT, 2), 'MM') THEN
                 'MEDIUM'
                WHEN T.CODE_CREDIT_TYPE = 'COL'
                     AND NVL(T.CNT_INSTALMENT, 0) = 60
                     AND TRUNC(T.DATE_FIRST_DUE, 'MM') <=
                     TRUNC(T.DTIME_SIGNATURE_CONTRACT, 'MM') THEN
                 'MEDIUM'
                WHEN T.CODE_CREDIT_TYPE = 'COL'
                     AND NVL(T.CNT_INSTALMENT, 0) = 60
                     AND TRUNC(T.DATE_FIRST_DUE, 'MM') =
                     TRUNC(ADD_MONTHS(T.DTIME_SIGNATURE_CONTRACT, 1), 'MM')
                     AND TO_NUMBER(TO_CHAR(T.DATE_FIRST_DUE, 'DD')) <=
                     TO_NUMBER(TO_CHAR(T.DTIME_SIGNATURE_CONTRACT, 'DD')) THEN
                 'MEDIUM'
                WHEN T.CODE_CREDIT_TYPE = 'CAL'
                     AND NVL(T.CNT_INSTALMENT, 0) = 12
                     AND TRUNC(T.DATE_FIRST_DUE, 'MM') =
                     TRUNC(ADD_MONTHS(T.DTIME_DISBURSEMENT, 1), 'MM')
                     AND TO_NUMBER(TO_CHAR(T.DATE_FIRST_DUE, 'DD')) >
                     TO_NUMBER(TO_CHAR(T.DTIME_DISBURSEMENT, 'DD')) THEN
                 'MEDIUM'
                WHEN T.CODE_CREDIT_TYPE = 'CAL'
                     AND NVL(T.CNT_INSTALMENT, 0) = 12
                     AND TRUNC(T.DATE_FIRST_DUE, 'MM') >=
                     TRUNC(ADD_MONTHS(T.DTIME_DISBURSEMENT, 2), 'MM') THEN
                 'MEDIUM'
                WHEN T.CODE_CREDIT_TYPE = 'CAL'
                     AND NVL(T.CNT_INSTALMENT, 0) = 60
                     AND
                     TRUNC(T.DATE_FIRST_DUE, 'MM') <= TRUNC(T.DTIME_DISBURSEMENT, 'MM') THEN
                 'MEDIUM'
                WHEN T.CODE_CREDIT_TYPE = 'CAL'
                     AND NVL(T.CNT_INSTALMENT, 0) = 60
                     AND TRUNC(T.DATE_FIRST_DUE, 'MM') =
                     TRUNC(ADD_MONTHS(T.DTIME_DISBURSEMENT, 1), 'MM')
                     AND TO_NUMBER(TO_CHAR(T.DATE_FIRST_DUE, 'DD')) <=
                     TO_NUMBER(TO_CHAR(T.DTIME_DISBURSEMENT, 'DD')) THEN
                 'MEDIUM'
                WHEN T.CODE_CREDIT_TYPE IN ('COL', 'CAL')
                     AND NVL(T.CNT_INSTALMENT, 0) > 60 THEN
                 'LONG'
                WHEN T.CODE_CREDIT_TYPE = 'COL'
                     AND NVL(T.CNT_INSTALMENT, 0) = 60
                     AND TRUNC(T.DATE_FIRST_DUE, 'MM') =
                     TRUNC(ADD_MONTHS(T.DTIME_SIGNATURE_CONTRACT, 1), 'MM')
                     AND TO_NUMBER(TO_CHAR(T.DATE_FIRST_DUE, 'DD')) >
                     TO_NUMBER(TO_CHAR(T.DTIME_SIGNATURE_CONTRACT, 'DD')) THEN
                 'LONG'
                WHEN T.CODE_CREDIT_TYPE = 'COL'
                     AND NVL(T.CNT_INSTALMENT, 0) = 60
                     AND TRUNC(T.DATE_FIRST_DUE, 'MM') >=
                     TRUNC(ADD_MONTHS(T.DTIME_SIGNATURE_CONTRACT, 2), 'MM') THEN
                 'LONG'
                WHEN T.CODE_CREDIT_TYPE = 'CAL'
                     AND NVL(T.CNT_INSTALMENT, 0) = 60
                     AND TRUNC(T.DATE_FIRST_DUE, 'MM') =
                     TRUNC(ADD_MONTHS(T.DTIME_DISBURSEMENT, 1), 'MM')
                     AND TO_NUMBER(TO_CHAR(T.DATE_FIRST_DUE, 'DD')) >
                     TO_NUMBER(TO_CHAR(T.DTIME_DISBURSEMENT, 'DD')) THEN
                 'LONG'
                WHEN T.CODE_CREDIT_TYPE = 'CAL'
                     AND NVL(T.CNT_INSTALMENT, 0) = 60
                     AND TRUNC(T.DATE_FIRST_DUE, 'MM') >=
                     TRUNC(ADD_MONTHS(T.DTIME_DISBURSEMENT, 2), 'MM') THEN
                 'LONG'
                WHEN T.CODE_CREDIT_TYPE = 'REV' THEN
                 'SHORT'
                WHEN T.CODE_CREDIT_STATUS = 'T'
                     AND T.DATE_FIRST_DUE = DATE '3000-01-01' THEN
                 'XNA'
              END)  
  WHERE EXISTS(
    SELECT 1
      FROM LDM_SBV.STM_CONTRACT STM
     WHERE STM.TEXT_CONTRACT_NUMBER = T.TEXT_CONTRACT_NUMBER
       AND STM.CODE_CREDIT_STATUS != 'T'
  )  ;
    /*THUAN.DCANGT 20221128 ADD UPDATE CODE_CONTRACT_TERM CBL-18911 END */          
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
      LDM_SBV_ETL.SP_SEND_NOTIFICATION (v_dm2_err_addr, 'LIB_DCT_CONTRACT.'|| c_mapping ,v_step || ', SQLERRM = ' || substr(dbms_utility.format_error_stack, 1, 300));

      raise_application_error(-20123,
                              'Error in module ' || c_mapping || ' (' || v_step || ')',
                              True);

  END dim_contract_load_sbv;

   -----------------------------------------------------------------
  -- dim_contract_update_ct_sbv
  -- author : huyen.trank
   -- created : 26.10.2020
  -- modified : 29.12.2020
  -----------------------------------------------------------------
  PROCEDURE dim_contract_update_ct_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2) IS
    v_step        VARCHAR2(255);
    v_cnt         Integer;
    c_mapping     VARCHAR2(60) := 'DIM_CONTRACT_UPDATE_CT_SBV';
    c_table_owner VARCHAR2(40) := 'LDM_SBV';
    c_table_name  VARCHAR2(40) := 'DCT_CONTRACT';

  BEGIN

    v_step := 'Start log';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping);

    v_step := 'Update data into LDM_SBV.DCT_CONTRACT for cash loan contract';

    MERGE
    INTO LDM_SBV.DCT_CONTRACT t
    USING (SELECT DISTINCT
                      SKP_CONTRACT
                    , CODE_SOURCE_SYSTEM
                    , ID_SOURCE
                    , DATE_EFFECTIVE
                    , FLAG_DELETED
                    , SKP_PROC_UPDATED
                    , DTIME_UPDATED
                    , DATE_CREATION
                    , CODE_CONTRACT_TERM
                    , DTIME_DISBURSEMENT
             FROM LDM_SBV.STM_CONTRACT
             WHERE code_credit_type = 'CAL' AND dtime_disbursement > d_def_value_date_hist
--                AND code_credit_status <> 'T'
                AND code_credit_status not in ('K', 'T')
                and FLAG_DELETED = v_flag_N
             ) s
    ON (s.id_source = t.id_source)
    WHEN MATCHED THEN
      Update
         SET
                t.DATE_EFFECTIVE        = s.DATE_EFFECTIVE,
                t.SKP_PROC_UPDATED        = s.SKP_PROC_UPDATED,
                t.DTIME_UPDATED            = s.DTIME_UPDATED,
                t.FLAG_DELETED            = s.FLAG_DELETED,
                t.code_contract_term    = s.code_contract_term ,
                t.DTIME_DISBURSEMENT     =    s.DTIME_DISBURSEMENT
        where t.code_credit_type = 'CAL' AND dtime_disbursement = d_def_value_date_hist
            and t.code_credit_status not in ('K', 'T')
    ;

    v_cnt := SQL%ROWCOUNT;
    COMMIT;

    v_step := 'Write record count';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping,
                                           p_sel          => v_cnt,
                                           p_mrg          => v_cnt);

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
      LDM_SBV_ETL.SP_SEND_NOTIFICATION (v_dm2_err_addr, 'LIB_DCT_CONTRACT.'|| c_mapping ,v_step || ', SQLERRM = ' || substr(dbms_utility.format_error_stack, 1, 300));

      raise_application_error(-20123,
                              'Error in module ' || c_mapping || ' (' || v_step || ')',
                              True);

  END dim_contract_update_ct_sbv;

-------------------------------------------------------------------
---- dim_contract_stat_update_sbv
---- author : hoa.trank
---- created : 14.12.2020
---- modified : 14.12.2020
-------------------------------------------------------------------
--  PROCEDURE dim_contract_stat_update_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2) IS
--    v_step        VARCHAR2(255);
--    v_cnt         Integer;
--    c_mapping     VARCHAR2(60) := 'DIM_CONTRACT_STAT_UPDATE_SBV';
--    c_table_owner VARCHAR2(40) := 'LDM_SBV';
--    c_table_name  VARCHAR2(40) := 'DCT_CONTRACT';
--
--  BEGIN
--
--    v_step := 'Start log';
--    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
--                                           p_mapping_name => c_mapping);
--
--    v_step := 'Merge data into LDM_SBV.dct_contract';
--
----    MERGE
----    INTO LDM_SBV.dct_contract t
----    USING (    SELECT     hc.id, hc.status as code_credit_status, row_number() over (partition by hc.id order by dtime_inserted desc) rwn
----            FROM owner_int.in_hom_contract hc
----            WHERE hc.code_load_status IN ('OK', 'LOAD')
----                and hc.code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
----                and hc.date_effective_inserted = p_effective_date
----    ) s
----    ON (s.id = t.id_contract and s.rwn = 1)
----    WHEN MATCHED THEN
----      UPDATE
----         SET t.code_credit_status             = s.code_credit_status,
----             t.date_effective                = p_effective_date,
----             t.skp_proc_updated              = p_process_key
----    ;
--    MERGE
--    INTO LDM_SBV.dct_contract t
--    USING (    select
--                T1.text_contract_number, t2.code_credit_status, t1.code_credit_type
--            from
--                LDM_SBV.dct_contract t1 join OWNER_DWH.dct_contract_extension t2
--            on t1.text_contract_number = t2.text_contract_number
--            where
--                t1.code_credit_status <> t2.code_credit_status
--                and t1.skp_credit_owner = -1
--                and t1.code_credit_status <> 'T'
--    ) s
--    ON (s.text_contract_number = t.text_contract_number)
--    WHEN MATCHED THEN
--      UPDATE
--         SET t.code_credit_status             = s.code_credit_status,
--             t.date_effective                = trunc(sysdate -1)
--    ;
--
--    v_cnt := SQL%ROWCOUNT;
--    COMMIT;
--
--    v_step := 'Write record count';
--    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
--                                           p_mapping_name => c_mapping,
--                                           p_sel          => v_cnt,
--                                           p_mrg          => v_cnt);
--
--    v_step := 'Close log';
--    owner_core.ms_global_log.write_map_end(p_process_key  => p_process_key,
--                                           p_mapping_name => c_mapping,
--                                           p_Status       => 'COMPLETE');
--
--  Exception
--    WHEN OTHERS THEN
--      ROLLBACK;
--      owner_core.etl_log_api.logend2(p_proc_key => p_process_key,
--                                     p_log_name => c_mapping,
--                                     p_Status   => 'FAILED',
--                                     p_info     => v_step || ', SQLERRM = ' ||
--                                                   substr(dbms_utility.format_error_stack, 1, 300));
--
--      raise_application_error(-20123,
--                              'Error in module ' || c_mapping || ' (' || v_step || ')',
--                              True);
--
--  END dim_contract_stat_update_sbv;

-----------------------------------------------------------------
-- dim_contract_wo_update_sbv
-- author : hoa.trank
-- created : 28.10.2020
-- modified : 28.10.2020
-----------------------------------------------------------------
  PROCEDURE dim_contract_wo_update_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2) IS
    v_step        VARCHAR2(255);
    v_cnt         Integer;
    c_mapping     VARCHAR2(60) := 'DIM_CONTRACT_WO_UPDATE_SBV';
    c_table_owner VARCHAR2(40) := 'LDM_SBV';
    c_table_name  VARCHAR2(40) := 'DCT_CONTRACT';

  BEGIN

    v_step := 'Start log';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping);

    v_step := 'Merge data into LDM_SBV.dct_contract';

    MERGE
    INTO LDM_SBV.dct_contract t
    USING (SELECT d.skp_contract,
                  d.date_creation,
                  d.id_source_sbv_last,
                  d.code_sbv_report_risk_group,
                  d.cnt_days_past_due,
                  d.date_contract_write_off,
                  case
                    when d.date_contract_write_off = d_def_value_date_future
                    then v_flag_n
                    else v_flag_y
                  end as flag_written_off
             FROM LDM_SBV.stt_sbv_risk_wo d) s
    ON (t.date_creation = s.date_creation AND t.skp_contract = s.skp_contract)
    WHEN MATCHED THEN
      Update
         SET t.code_sbv_report_risk_group     = s.code_sbv_report_risk_group,
             t.cnt_days_past_due             = s.cnt_days_past_due,
             t.code_maturity_status            = CASE WHEN s.cnt_days_past_due = 0 THEN 'IN DUE' ELSE 'OVER DUE' END,
             t.flag_written_off              = s.flag_written_off,
             t.date_contract_write_off         = s.date_contract_write_off,
             t.id_source_sbv_last            = substr(s.id_source_sbv_last, 1, INSTR(s.id_source_sbv_last,'.')-1) || '.' ||to_char(p_effective_date,'yyyymmdd'),
             t.date_effective                = p_effective_date,
             t.skp_proc_updated              = p_process_key;

    v_cnt := SQL%ROWCOUNT;
    COMMIT;

    v_step := 'Write record count';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping,
                                           p_sel          => v_cnt,
                                           p_mrg          => v_cnt);

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
      LDM_SBV_ETL.SP_SEND_NOTIFICATION (v_dm2_err_addr, 'LIB_DCT_CONTRACT.'|| c_mapping ,v_step || ', SQLERRM = ' || substr(dbms_utility.format_error_stack, 1, 300));

      raise_application_error(-20123,
                              'Error in module ' || c_mapping || ' (' || v_step || ')',
                              True);

  END dim_contract_wo_update_sbv;

-----------------------------------------------------------------
-- dim_contract_wo_mm_update_sbv
-- author : hoa.trank
-- created : 28.10.2020
-- modified : 28.10.2020
-----------------------------------------------------------------
  PROCEDURE dim_contract_wo_mm_update_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2) IS
    v_step        VARCHAR2(255);
    v_cnt         Integer;
    c_mapping     VARCHAR2(60) := 'DIM_CONTRACT_WO_MM_UPDATE_SBV';
    c_table_owner VARCHAR2(40) := 'LDM_SBV';
    c_table_name  VARCHAR2(40) := 'DCT_CONTRACT';

  BEGIN

    v_step := 'Start log';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping);

    v_step := 'Merge data into LDM_SBV.dct_contract';

    MERGE
    INTO LDM_SBV.dct_contract t
    USING (SELECT d.skp_contract,
                  d.date_creation,
                  d.id_source_sbv_last,
                  d.date_contract_write_off,
                  d.code_sbv_report_risk_group,
                  d.cnt_days_past_due,
                  case
                    when d.date_contract_write_off = d_def_value_date_future
                    then v_flag_n
                    else v_flag_y
                  end as flag_written_off,
                  d.flag_risk_group_wo_manually
             FROM LDM_SBV.stt_sbv_risk_wo_mm d) s
    ON (t.skp_contract = s.skp_contract AND t.date_creation = s.date_creation)
    WHEN MATCHED THEN
      Update
         SET t.code_sbv_report_risk_group      = s.code_sbv_report_risk_group,
             t.cnt_days_past_due              = s.cnt_days_past_due,
             t.code_maturity_status            = CASE WHEN s.cnt_days_past_due = 0 THEN 'IN DUE' ELSE 'OVER DUE' END,
             t.flag_risk_group_wo_manually     = s.flag_risk_group_wo_manually,
             t.date_contract_write_off         = s.date_contract_write_off,
             t.flag_written_off                = s.flag_written_off,
             t.id_source_sbv_last            = substr(s.id_source_sbv_last, 1, INSTR(s.id_source_sbv_last,'.')-1) || '.' ||to_char(p_effective_date,'yyyymmdd'),
             t.date_effective                  = p_effective_date,
             t.skp_proc_updated                = p_process_key;

    v_cnt := SQL%ROWCOUNT;
    COMMIT;

    v_step := 'Write record count';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping,
                                           p_sel          => v_cnt,
                                           p_mrg          => v_cnt);

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
      LDM_SBV_ETL.SP_SEND_NOTIFICATION (v_dm2_err_addr, 'LIB_DCT_CONTRACT.'|| c_mapping ,v_step || ', SQLERRM = ' || substr(dbms_utility.format_error_stack, 1, 300));

      raise_application_error(-20123,
                              'Error in module ' || c_mapping || ' (' || v_step || ')',
                              True);

  END dim_contract_wo_mm_update_sbv;

-----------------------------------------------------------------
-- dim_contract_sbv_update_sbv
-- author : hoa.trank
-- created : 04.11.2020
-- modified : 04.11.2020
-----------------------------------------------------------------
  PROCEDURE dim_contract_sbv_update_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2) IS
    v_step        VARCHAR2(255);
    v_cnt         Integer;
    c_mapping     VARCHAR2(60) := 'DIM_CONTRACT_SBV_UPDATE_SBV';
    c_table_owner VARCHAR2(40) := 'LDM_SBV';
    c_table_name  VARCHAR2(40) := 'DCT_CONTRACT';

  BEGIN

    v_step := 'Start log';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping);

    v_step := 'Merge data into LDM_SBV.dct_contract';

    MERGE
    INTO LDM_SBV.dct_contract t
    USING (SELECT t.skp_contract,
                  t.date_creation,
                  t.id_source,
                  t.code_sbv_report_risk_group,
                  t.cnt_days_past_due,
                  t.flag_observation_period_used
             FROM LDM_SBV.stc_sbv_risk_td t
             WHERE t.code_sbv_report_risk_group NOT IN ('98','99')) s
    ON (t.skp_contract = s.skp_contract AND t.date_creation = s.date_creation)
    WHEN MATCHED THEN
      Update
         SET t.code_sbv_report_risk_group      = s.code_sbv_report_risk_group,
             t.cnt_days_past_due              = s.cnt_days_past_due,
             t.code_maturity_status            = CASE WHEN s.cnt_days_past_due = 0 THEN 'IN DUE' ELSE 'OVER DUE' END,
             t.flag_observation_period_used = s.flag_observation_period_used,
             t.id_source_sbv_last            = s.id_source,
             t.skp_proc_updated                = p_process_key,
             t.date_effective                  = p_effective_date;

    v_cnt := SQL%ROWCOUNT;
    COMMIT;

    v_step := 'Write record count';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping,
                                           p_sel          => v_cnt,
                                           p_mrg          => v_cnt);

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
      LDM_SBV_ETL.SP_SEND_NOTIFICATION (v_dm2_err_addr, 'LIB_DCT_CONTRACT.'|| c_mapping ,v_step || ', SQLERRM = ' || substr(dbms_utility.format_error_stack, 1, 300));

      raise_application_error(-20123,
                              'Error in module ' || c_mapping || ' (' || v_step || ')',
                              True);

  END dim_contract_sbv_update_sbv;

  -----------------------------------------------------------------
  -- dim_contract_sbv_scb_update_sbv
  -- author : huy.lyb
  -- created : 13.06.2020
  -- modified : 13.06.2020
  -----------------------------------------------------------------
  PROCEDURE dim_contract_ldm_sbv_scb_update_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2) IS
    v_step        VARCHAR2(255);
    v_cnt         Integer;
    c_mapping     VARCHAR2(60) := 'DIM_CONTRACT_LDM_SBV_SCB_UPDATE_SBV';
    c_table_owner VARCHAR2(40) := 'LDM_SBV';
    c_table_name  VARCHAR2(40) := 'DCT_CONTRACT';

  BEGIN

    v_step := 'Start log';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping);

    v_step := 'Merge data into ldm_sbv.dct_contract';

    MERGE
    INTO ldm_sbv.dct_contract t
    USING (SELECT t.skp_contract,
                                t.date_creation,
                                t.id_source,
                                t.code_sbv_report_risk_group,
                                t.cnt_days_past_due
                 FROM ldm_sbv.stt_sbv_scb_risk_main t
                 WHERE t.code_sbv_report_risk_group NOT IN ('98','99')) s
    ON (t.skp_contract = s.skp_contract AND t.date_creation = s.date_creation)
    WHEN MATCHED THEN
      Update
         SET t.code_sbv_report_risk_group      = s.code_sbv_report_risk_group,
                 t.cnt_days_past_due          = s.cnt_days_past_due,
                 t.code_maturity_status        = CASE WHEN s.cnt_days_past_due = 0 THEN 'IN DUE' ELSE 'OVER DUE' END,
                 t.id_source_sbv_last        = s.id_source,
                 t.skp_proc_updated         = p_process_key,
                 t.date_effective           = p_effective_date;

    v_cnt := SQL%ROWCOUNT;
    COMMIT;

    v_step := 'Write record count';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping,
                                           p_sel          => v_cnt,
                                           p_mrg          => v_cnt);

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
      LDM_SBV_ETL.SP_SEND_NOTIFICATION (v_dm2_err_addr, 'LIB_DCT_CONTRACT.'|| c_mapping ,v_step || ', SQLERRM = ' || substr(dbms_utility.format_error_stack, 1, 300));

      raise_application_error(-20123,
                              'Error in module ' || c_mapping || ' (' || v_step || ')',
                              True);

  END dim_contract_ldm_sbv_scb_update_sbv;


  PROCEDURE dim_contr_late_map_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2) IS
    v_step        VARCHAR2(255);
    v_cnt         Integer;
    c_mapping     VARCHAR2(60) := 'DIM_CONTR_LATE_MAP_SBV';
    c_table_owner VARCHAR2(40) := 'LDM_SBV';
    c_table_name  VARCHAR2(40) := 'STM_CONTRACT';

  BEGIN

    v_step := 'Start log';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping);

    v_step := 'Truncate table ' || c_table_owner || '.' || c_table_name;
    LDM_SBV.truncate_table(c_table_owner, c_table_name);

    v_step := 'Insert data into LDM_SBV.STM_CONTRACT';

  Insert /*+APPEND */
    INTO LDM_SBV.STM_CONTRACT
      (
           SKP_CONTRACT
        , CODE_SOURCE_SYSTEM
        , ID_SOURCE
        , DATE_EFFECTIVE
        , FLAG_DELETED
        , SKP_PROC_INSERTED
        , SKP_PROC_UPDATED
        , DTIME_INSERTED
        , DTIME_UPDATED
        , ID_CONTRACT
        , ID_CLIENT
        , ID_PRODUCT
        , ID_CUID
        , DTIME_SIGNATURE_CONTRACT
        , CNT_INSTALMENT
        , DATE_FIRST_DUE
        , DTIME_DISBURSEMENT
        , CODE_CREDIT_STATUS
        , CODE_CREDIT_TYPE
        , TEXT_CONTRACT_NUMBER
        , DATE_CREATION
        , CODE_SBV_REPORT_RISK_GROUP
        , FLAG_RISK_GROUP_WO_MANUALLY
        , DATE_CONTRACT_WRITE_OFF
        , FLAG_WRITTEN_OFF
        , ID_SOURCE_SBV_LAST
        , FLAG_OBSERVATION_PERIOD_USED
        , CODE_CONTRACT_TERM
        , CODE_SALESROOM
        , NAME_SALESROOM
        , SALESROOM_ADDRESS
        , SALESROOM_PROVINCE
        , CODE_PRODUCT
        , RATE_INTEREST
        , RATE_INTEREST_INITIAL
        , AMT_CREDIT
        , AMT_CREDIT_TOTAL
        , CODE_ACCOUNTING_METHOD
        , NAME_PRODUCT
        , SKP_CREDIT_OWNER
        , CODE_CREDIT_OWNER
        , DTIME_PAIDOFF
        , DTIME_CLOSE
        , DTIME_ACTIVATION
      )

     with w_hom as
    (
        select
          hc.id id_contract
        , hc.code_change_type
        , to_number(client.id_source) id_client
        , product.id_product as id_product
        , client.id_cuid
        , fp.terms cnt_instalment
        , product.code_product as code_product
        , fp.interest_rate rate_interest
        , fp.net_credit_limit_amount
        , fp.net_credit_amount
        , fp.provided_credit_limit_amount
        , fp.credit_amount
        , case when hc.status = 'T' then d_def_value_date_future else fp.first_due_date end date_first_due
        , product_profile.code_accounting_method
        , fsat.code_salesroom
        , fsat.name_salesroom
        , fsat.TEXT_SALESROOM_ADDRESS as salesroom_address
        , fsat.NAME_SALE_PROVINCE as salesroom_province
--        , hs.code code_salesroom
--        , hs.name name_salesroom
--        , ha.house_number || ' - ' || ha.street_name || ' - ' || ha.town_value || ' - ' || hd.Name || ' - ' || hr.name salesroom_address
--        , hr.name salesroom_province
        , cst.dtime_signature_contract
        , hc.status code_credit_status
        , hc.contract_code text_contract_number
        , trunc(hc.creation_date) date_creation
        , hc.contract_type
        , product_profile.code_credit_type
        , product.name_product as name_product
        , credit_owner.skp_credit_owner
        , credit_owner.code_credit_owner
        ,  cst.dtime_paidoff
        ,  cst.dtime_close
        ,  cst.dtime_activation
        from owner_int.in_hom_contract hc
          join (
                SELECT HOM_DEAL.ID  , HOM_DEAL.CLIENT_ID
                FROM (
                        select max(id) over (partition by id order by date_effective_inserted desc) id ,
                                max(client_id) over (partition by id order by date_effective_inserted desc) as client_id
                        from owner_int.in_hom_deal
                        where  date_effective_inserted >= p_effective_date - 15 --in case of table in_hom_deal is not generate date_effective for contract insert/update after 22h30
                                and code_load_status IN ('OK', 'LOAD')
                                and code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
                    ) HOM_DEAL
                GROUP BY HOM_DEAL.ID  , HOM_DEAL.CLIENT_ID
            ) deal ON hc.deal_id = deal.id
            JOIN LDM_SBV.dct_client client ON deal.client_id = to_number(client.id_source)
            JOIN LDM_SBV.dct_product product on hc.product_id = product.id_product
            JOIN  LDM_SBV.dct_product_profile product_profile on product.code_product_profile = product_profile.code_product_profile
            join LDM_SBV.ft_salesroom_address_tt fsat on hc.salesroom_code = fsat.code_salesroom and fsat.flag_deleted = v_flag_N
--            join owner_int.vh_hom_salesroom hs on hc.salesroom_code = hs.code
--            join owner_int.vh_hom_salesroom2address s2a on hs.id = s2a.salesroom_id
--            join owner_int.vh_hom_address ha on s2a.address_id = ha.id
--            left join owner_int.vh_hom_district hd on ha.district_code = hd.code
--            join owner_int.vh_hom_region hr on ha.region_code = hr.code
            left JOIN owner_int.in_hom_financial_parameters fp on hc.id = fp.contract_id and fp.archived = 0
                and fp.code_load_status IN ('OK', 'LOAD')
                and fp.code_change_type IN ('X', 'I', 'U', /*'D',*/ 'M', 'N') --THUAN.DANG 20221028 REMOVE 'D' BECAUSE IT IS DUPLICATED WITH OTHER TYPES
                and fp.date_effective_inserted = p_effective_date + 1 and fp.update_date >= p_effective_date and fp.update_date < p_effective_date + 1
           left join
            (
                select
                    contract_id,
                    max(case when status = 'N' then creation_date end) as dtime_signature_contract,
                    max(case when status = 'L' then creation_date end) as dtime_paidoff,
                    max(case when status = 'A' then creation_date end) as dtime_activation,
                    max(case when status = 'K' then creation_date end) as dtime_close
                from
                owner_int.in_hom_contract_status_trans
                where             status in ('N', 'L', 'K','A')
                                  and transferred_manually = 0
                                  and code_load_status IN ('OK', 'LOAD')
                                  and code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
                                  and date_effective_inserted = p_effective_date + 1 and CREATION_DATE < p_effective_date + 1
                group by  contract_id
            ) cst on cst.contract_id = fp.contract_id
             join(
                 select

                        osh.ID_CONTRACT as ID_CONTRACT
                        ,osh.skp_credit_owner
                        ,osh.CODE_CONTRACT_OWNER as code_credit_owner
                 from
                         LDM_SBV.STP_HOM_OWNERSHIP osh
                 where osh.date_valid_to = d_def_value_date_future
            ) credit_owner on hc.id = credit_owner.ID_CONTRACT
        where  fsat.code_status = 'a'
            and     hc.code_load_status IN ('OK', 'LOAD')
            and hc.code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
            and hc.date_effective_inserted = p_effective_date + 1 and hc.update_date < p_effective_date + 1
        group by
             hc.id
            , hc.code_change_type
            , client.id_source
            , product.id_product
            , client.id_cuid
            , fp.terms
            , fp.first_due_date
            , hc.status
            , hc.contract_code
            , hc.creation_date
            , fsat.code_salesroom
            , fsat.name_salesroom
            , fsat.TEXT_SALESROOM_ADDRESS
            , fsat.NAME_SALE_PROVINCE
--            , hs.code
--            , hs.name
--            , ha.house_number
--            , ha.street_name
--            , ha.town_value
--            , hd.Name
--            , hr.name
            , product.code_product
            , fp.interest_rate
            , fp.net_credit_limit_amount
            , fp.net_credit_amount
            , fp.provided_credit_limit_amount
            , fp.credit_amount
            , product_profile.code_accounting_method
            , hc.contract_type
            , product_profile.code_credit_type
            , cst.dtime_signature_contract
            , product.name_product
            , credit_owner.skp_credit_owner
            , credit_owner.code_credit_owner
            , cst.dtime_paidoff
            , cst.dtime_close
            , cst.dtime_activation
    )
, w_contract as
    (select
          wh.id_contract
        , wh.code_change_type
        , v_hom_code_source_system code_source_system
        , to_char(wh.id_contract) id_source
        , wh.id_client
        , wh.id_product
        , wh.id_cuid
        , wh.dtime_signature_contract
        , wh.cnt_instalment
        , wh.date_first_due
        , max(nvl(  case when op.TRANSACTION_TYPE = 'PTR' and op.status = 'P' then op.update_date
                         when op.TRANSACTION_TYPE = 'CL' and op.status = 'D' then op.update_date end,  d_def_value_date_hist)) as dtime_disbursement
        , wh.code_credit_status
        , wh.code_credit_type
        , wh.text_contract_number
        , wh.date_creation
        , wh.code_salesroom
        , wh.name_salesroom
        , wh.salesroom_address
        , wh.salesroom_province
        , wh.code_product
        , wh.rate_interest
        , NULL rate_interest_initial
        , wh.net_credit_limit_amount
        , wh.net_credit_amount
        , wh.provided_credit_limit_amount
        , wh.credit_amount
        , wh.code_accounting_method
        , wh.name_product
        , wh.skp_credit_owner
        , wh.code_credit_owner
        , wh.dtime_paidoff
        , wh.dtime_close
        , wh.dtime_activation
    from w_hom wh
        left join owner_int.in_hom_outgoing_payment op on wh.id_contract = op.contract_id
          and op.status in( 'D', 'P') and op.TRANSACTION_TYPE in ('PTR', 'CL')
          and op.code_load_status IN ('OK', 'LOAD')
          and op.code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
          and op.date_effective_inserted = p_effective_date + 1 and op.update_date < p_effective_date + 1
    where wh.contract_type <> 'REL'
    group by wh.id_contract
        , wh.code_change_type
        , wh.id_client
        , wh.id_product
        , wh.id_cuid
        , wh.dtime_signature_contract
        , wh.cnt_instalment
        , wh.date_first_due
        , wh.code_credit_status
        , wh.code_credit_type
        , wh.text_contract_number
        , wh.date_creation
        , wh.code_salesroom
        , wh.name_salesroom
        , wh.salesroom_address
        , wh.salesroom_province
        , wh.code_product
        , wh.rate_interest
        , wh.net_credit_limit_amount
        , wh.net_credit_amount
        , wh.provided_credit_limit_amount
        , wh.credit_amount
        , wh.code_accounting_method
        , wh.name_product
        , wh.skp_credit_owner
        , wh.code_credit_owner
        , wh.dtime_paidoff
        , wh.dtime_close
        , wh.dtime_activation
    union all
    select
        wh.id_contract
        , wh.code_change_type
        , v_cbus_code_source_system code_source_system
        , to_char(ac.id_contract) id_source
        , wh.id_client
        , wh.id_product
        , wh.id_cuid
        , wh.dtime_signature_contract
        , wh.cnt_instalment
        , wh.date_first_due
        , d_def_value_date_hist dtime_disbursement
        , wh.code_credit_status
        , wh.code_credit_type
        , wh.text_contract_number
        , trunc(ac.date_creation) as date_creation
        , wh.code_salesroom
        , wh.name_salesroom
        , wh.salesroom_address
        , wh.salesroom_province
        , wh.code_product
        , NULL rate_interest
        , ati.percentage_value as rate_interest_initial
        , wh.net_credit_limit_amount
        , wh.net_credit_amount
        , wh.provided_credit_limit_amount
        , wh.credit_amount
        , wh.code_accounting_method
        , wh.name_product
        , wh.skp_credit_owner
        , wh.code_credit_owner
        , wh.dtime_paidoff
        , wh.dtime_close
        , wh.dtime_activation
    from w_hom wh
        join LDM_SBV.stp_cbus_a_account ac on wh.text_contract_number = ac.text_contract_number
        left join owner_int.in_cbus_a_contract_snapshot cs on cs.contract_id = ac.id_contract AND cs.VALID_TO IS NULL
                      and cs.code_load_status IN ('OK', 'LOAD')
                      and cs.code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
                        and cs.date_effective_inserted = p_effective_date + 1 and cs.CREATION_DATE < p_effective_date + 1
        left JOIN  LDM_SBV.dct_cbus_a_tariff at ON cs.tariff_code = at.code_tariff and at.DATE_VALID_TO = date '3000-01-01'
        left join
        (
                    select  max(num_percentage_value) percentage_value,
                            id_tariff
                    from LDM_SBV.dct_cbus_a_tariff_item where code_tariff_item_type in ('INTEREST_CASH', 'INTEREST_CASHLESS')
                    group by id_tariff
        ) ati on  ati.id_tariff = at.id_tariff
    where wh.contract_type = 'REL'
    group by wh.id_contract
        , wh.code_change_type
        , ac.id_contract
        , wh.id_client
        , wh.id_product
        , wh.id_cuid
        , wh.dtime_signature_contract
        , wh.cnt_instalment
        , wh.date_first_due
        , wh.code_credit_status
        , wh.code_credit_type
        , wh.text_contract_number
        , ac.date_creation
        , wh.code_salesroom
        , wh.name_salesroom
        , wh.salesroom_address
        , wh.salesroom_province
        , wh.code_product
        , wh.net_credit_limit_amount
        , wh.net_credit_amount
        , wh.provided_credit_limit_amount
        , wh.credit_amount
        , wh.code_accounting_method
        , wh.name_product
        , ati.percentage_value
        , wh.skp_credit_owner
        , wh.code_credit_owner
        , wh.dtime_paidoff
        , wh.dtime_close
        , wh.dtime_activation
    )
      SELECT
             NULL AS SKP_CONTRACT,
             i.code_source_system                                         AS code_source_system,
             i.id_source                                                 AS id_source,
             p_effective_date                                             AS date_effective,
             i.flag_deleted                                             AS flag_deleted,
             p_process_key                                                 AS skp_proc_inserted,
             p_process_key                                              AS skp_proc_updated,
             sysdate                                                     AS dtime_inserted,
             sysdate                                                     AS dtime_updated,
             i.id_contract                                                AS id_contract,
             i.id_client                                                AS id_client,
             i.id_product                                                AS id_product,
             i.id_cuid                                                  AS id_cuid,
             nvl(i.dtime_signature_contract, d_def_value_date_future)     AS dtime_signature_contract,
             i.cnt_instalment                                             AS cnt_instalment,
             nvl(i.date_first_due, d_def_value_date_future)             AS date_first_due,
             nvl(i.dtime_disbursement, d_def_value_date_hist)             AS dtime_disbursement,
             nvl(i.code_credit_status, v_xna)                             AS code_credit_status,
              nvl(i.code_credit_type, v_xna)                               AS code_credit_type    ,
             nvl(i.text_contract_number, v_xna)                           AS text_contract_number    ,
             nvl(i.date_creation,  d_def_value_date_future)               AS date_creation    ,
             '1'                                                        AS CODE_SBV_REPORT_RISK_GROUP,
             v_flag_N                                                     AS FLAG_RISK_GROUP_WO_MANUALLY,
             d_def_value_date_future                                     AS DATE_CONTRACT_WRITE_OFF,
             v_flag_N                                                     AS FLAG_WRITTEN_OFF,
             v_xna                                                         AS ID_SOURCE_SBV_LAST,
             v_flag_N                                                   as FLAG_OBSERVATION_PERIOD_USED,
             nvl(i.code_contract_term, v_xna)                           AS code_contract_term
            , CODE_SALESROOM
            , NAME_SALESROOM
            , salesroom_address
            , SALESROOM_PROVINCE
            , code_product
            , rate_interest
            , rate_interest_initial
            , amt_credit
            , amt_credit_total
            , code_accounting_method
            , nvl(name_product, v_xna) as name_product
            , nvl(i.skp_credit_owner, n_minus_one) as skp_credit_owner
            , nvl(i.code_credit_owner, v_xna) as code_credit_owner
            , nvl(i.dtime_paidoff, d_def_value_date_future) as dtime_paidoff
            , nvl(i.dtime_close, d_def_value_date_future) as dtime_close
            , nvl(i.dtime_activation, d_def_value_date_future) as dtime_activation
        FROM (
            SELECT
                code_source_system
                , id_source
                , CASE WHEN code_change_type = v_code_change_type_del THEN v_flag_Y ELSE v_flag_N END AS FLAG_DELETED
                , id_contract
                , id_client
                , id_product
                , id_cuid
                , dtime_signature_contract
                , cnt_instalment
                , date_first_due
                , dtime_disbursement
                , code_credit_status
                , code_credit_type
                ,CASE
                    WHEN code_credit_type IN ('COL','CAL')
                        AND nvl(cnt_instalment, 0) < 12 THEN 'SHORT'
                    WHEN code_credit_type = 'CAL'
                        AND nvl(cnt_instalment, 0) = 12 
                        AND (dtime_disbursement = d_def_value_date_future OR dtime_disbursement IS NULL) THEN 'MEDIUM'
                    WHEN code_credit_type = 'CAL'
                        AND nvl(cnt_instalment, 0) = 60 
                        AND (dtime_disbursement = d_def_value_date_future OR dtime_disbursement IS NULL) THEN 'LONG'    
                    WHEN code_credit_type = 'CAL'
                        AND nvl(cnt_instalment, 0) = 12
                        AND TRUNC(date_first_due, 'MM') <= TRUNC(dtime_disbursement, 'MM') THEN 'SHORT'
                    WHEN code_credit_type = 'CAL'
                        AND nvl(cnt_instalment, 0) = 12
                        AND TRUNC(date_first_due, 'MM') = TRUNC(ADD_MONTHS(dtime_disbursement, 1), 'MM')
                        AND to_number(to_char(date_first_due, 'DD')) <= to_number(to_char(dtime_disbursement, 'DD')) THEN 'SHORT'
                    WHEN code_credit_type = 'COL'
                        AND nvl(cnt_instalment, 0) = 12
                        AND TRUNC(date_first_due, 'MM') <= TRUNC(dtime_signature_contract, 'MM') THEN
                       'SHORT'
                    WHEN code_credit_type = 'COL'
                        AND nvl(cnt_instalment, 0) = 12
                        AND TRUNC(date_first_due, 'MM') = TRUNC(ADD_MONTHS(dtime_signature_contract, 1), 'MM')
                        AND to_number(to_char(date_first_due, 'DD')) <= to_number(to_char(dtime_signature_contract, 'DD')) THEN
                       'SHORT'
                    WHEN code_credit_type IN ('COL', 'CAL')
                        AND nvl(cnt_instalment, 0) > 12
                        AND nvl(cnt_instalment, 0) < 60 THEN
                       'MEDIUM'
                    WHEN code_credit_type = 'COL'
                        AND nvl(cnt_instalment, 0) = 12
                        AND TRUNC(date_first_due, 'MM') = TRUNC(ADD_MONTHS(dtime_signature_contract, 1), 'MM')
                        AND to_number(to_char(date_first_due, 'DD')) > to_number(to_char(dtime_signature_contract, 'DD')) THEN
                       'MEDIUM'
                    WHEN code_credit_type = 'COL'
                        AND nvl(cnt_instalment, 0) = 12
                        AND TRUNC(date_first_due, 'MM') >= TRUNC(ADD_MONTHS(dtime_signature_contract, 2), 'MM') THEN
                       'MEDIUM'
                    WHEN code_credit_type = 'COL'
                        AND nvl(cnt_instalment, 0) = 60
                        AND TRUNC(date_first_due, 'MM') <= TRUNC(dtime_signature_contract, 'MM') THEN
                       'MEDIUM'
                    WHEN code_credit_type = 'COL'
                        AND nvl(cnt_instalment, 0) = 60
                        AND TRUNC(date_first_due, 'MM') = TRUNC(ADD_MONTHS(dtime_signature_contract, 1), 'MM')
                        AND to_number(to_char(date_first_due, 'DD')) <= to_number(to_char(dtime_signature_contract, 'DD')) THEN
                       'MEDIUM'
                    WHEN code_credit_type = 'CAL'
                        AND nvl(cnt_instalment, 0) = 12
                        AND TRUNC(date_first_due, 'MM') = TRUNC(ADD_MONTHS(dtime_disbursement, 1), 'MM')
                        AND to_number(to_char(date_first_due, 'DD')) > to_number(to_char(dtime_disbursement, 'DD')) THEN 'MEDIUM'
                    WHEN code_credit_type = 'CAL'
                        AND nvl(cnt_instalment, 0) = 12
                        AND TRUNC(date_first_due, 'MM') >= TRUNC(ADD_MONTHS(dtime_disbursement, 2), 'MM') THEN 'MEDIUM'
                    WHEN code_credit_type = 'CAL'
                        AND nvl(cnt_instalment, 0) = 60
                        AND TRUNC(date_first_due, 'MM') <= TRUNC(dtime_disbursement, 'MM') THEN 'MEDIUM'
                    WHEN code_credit_type = 'CAL'
                        AND nvl(cnt_instalment, 0) = 60
                        AND TRUNC(date_first_due, 'MM') = TRUNC(ADD_MONTHS(dtime_disbursement, 1), 'MM')
                        AND to_number(to_char(date_first_due, 'DD')) <= to_number(to_char(dtime_disbursement, 'DD')) THEN 'MEDIUM'
                    WHEN code_credit_type IN ('COL', 'CAL')
                        AND nvl(cnt_instalment, 0) > 60 THEN 'LONG'
                    WHEN code_credit_type = 'COL'
                        AND nvl(cnt_instalment, 0) = 60
                        AND TRUNC(date_first_due, 'MM') = TRUNC(ADD_MONTHS(dtime_signature_contract, 1), 'MM')
                        AND to_number(to_char(date_first_due, 'DD')) > to_number(to_char(dtime_signature_contract, 'DD')) THEN
                       'LONG'
                    WHEN code_credit_type = 'COL'
                        AND nvl(cnt_instalment, 0) = 60
                        AND TRUNC(date_first_due, 'MM') >= TRUNC(ADD_MONTHS(dtime_signature_contract, 2), 'MM') THEN
                       'LONG'
                    WHEN code_credit_type = 'CAL'
                        AND nvl(cnt_instalment, 0) = 60
                        AND TRUNC(date_first_due, 'MM') = TRUNC(ADD_MONTHS(dtime_disbursement, 1), 'MM')
                        AND to_number(to_char(date_first_due, 'DD')) > to_number(to_char(dtime_disbursement, 'DD')) THEN 'LONG'
                    WHEN code_credit_type = 'CAL'
                        AND nvl(cnt_instalment, 0) = 60
                        AND TRUNC(date_first_due, 'MM') >= TRUNC(ADD_MONTHS(dtime_disbursement, 2), 'MM') THEN 'LONG'
                    WHEN code_credit_type = 'REV' THEN 'SHORT'
                    WHEN code_credit_status = 'T'
                        AND date_first_due = date '3000-01-01' THEN 'XNA'
                END AS code_contract_term
                , text_contract_number
                , date_creation
                , code_salesroom
                , name_salesroom
                , salesroom_address
                , Salesroom_Province
                , code_product
                , rate_interest
                , rate_interest_initial
                , case
                    when code_credit_type = 'REV' then net_credit_limit_amount
                    else net_credit_amount
                  end amt_credit
                , case
                    when code_credit_type = 'REV' then provided_credit_limit_amount
                    else credit_amount
                  end amt_credit_total
                , code_accounting_method
                , name_product
                ,skp_credit_owner
                ,code_credit_owner
                , dtime_paidoff
                , dtime_close
                , dtime_activation
            from w_contract
            ) i;


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
      LDM_SBV_ETL.SP_SEND_NOTIFICATION (v_dm2_err_addr, 'LIB_DCT_CONTRACT.'|| c_mapping ,v_step || ', SQLERRM = ' || substr(dbms_utility.format_error_stack, 1, 300));

      raise_application_error(-20123,
                              'Error in module ' || c_mapping || ' (' || v_step || ')',
                              True);

  END dim_contr_late_map_sbv;

END lib_dct_contract;
/