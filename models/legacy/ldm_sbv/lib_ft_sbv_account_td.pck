
CREATE OR REPLACE PACKAGE                                  LDM_SBV_ETL.lib_ft_sbv_account_td IS



-- global parameters
    v_default_code_source_system CONSTANT VARCHAR2(10) := 'SBV';
    v_xna                        CONSTANT VARCHAR2(10) := 'XNA';
    v_xap                        CONSTANT VARCHAR2(10) := 'XAP';
    v_hcvn                       CONSTANT VARCHAR2(10) := 'HCVN';
    n_minus_one                  CONSTANT BINARY_INTEGER := -1;
    v_flag_X                     CONSTANT VARCHAR2(1 CHAR)  := 'X';
    v_flag_Y                     CONSTANT VARCHAR2(1)  := 'Y';
    v_flag_N                     CONSTANT VARCHAR2(1)  := 'N';
    v_code_change_type_del       CONSTANT VARCHAR2(1)  := 'D';
    d_def_value_date_hist        CONSTANT DATE  := DATE '1000-01-01';
    d_def_value_date_future      CONSTANT DATE  := DATE '3000-01-01';
    v_date_accounted             CONSTANT DATE  := DATE '2021-02-28';
    v_fixed                      CONSTANT VARCHAR2(10) := 'FIXED';
    v_interest                   CONSTANT VARCHAR2(10) := 'Interest';


PROCEDURE ft_sbvaccdat_map_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2);

PROCEDURE ft_sbvaccdat_diff_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2);

PROCEDURE ft_sbvaccdat_old_load_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2);

PROCEDURE ft_sbvaccdat_new_load_sbv(p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2);

END lib_ft_sbv_account_td;
/


CREATE OR REPLACE PACKAGE Body                                                                 LDM_SBV_ETL.lib_ft_sbv_account_td IS
-----------------------------------------------------------------
-- author : nga.len
-- created : 26.10.2020
-- modified : 26.10.2020
-----------------------------------------------------------------

v_dm2_err_addr      VARCHAR2(255)   := 'Errors - Notification - VN Data mart 2 <4908ab62.homecreditgroup.onmicrosoft.com@emea.teams.ms>';

PROCEDURE ft_sbvaccdat_map_sbv (p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2)
IS
  v_step           VARCHAR2(255);
  v_cnt            INTEGER;
  c_mapping        VARCHAR2(60) := 'FT_SBVACCDAT_MAP_SBV';
  c_table_owner    VARCHAR2(40) := 'LDM_SBV';
  c_table_name     VARCHAR2(40) := 'STM_SBV_ACCOUNT_DATA_TD';
BEGIN
    v_step := 'Start log';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                           p_mapping_name => c_mapping);
    v_step := 'Truncate table ' || c_table_owner || '.' || c_table_name;
    LDM_SBV.truncate_table(c_table_owner, c_table_name);

    v_step := 'Insert data into LDM_SBV.stm_sbv_account_data_td';

    owner_core.lib_support_utils.alter_session_manual(4);

    INSERT /*+ APPEND */
    INTO LDM_SBV.stm_sbv_account_data_td t
        (
            skf_sbv_account_data,
            code_source_system,
            id_source,
            date_effective,
            skp_proc_inserted,
            skp_proc_updated,
            flag_deleted,
            code_credit_type,
            skp_contract,
            id_contract,
            id_client,
            text_contract_number,
            amt_acc101,
            amt_acc115,
            amt_acc116,
            amt_acc201,
            amt_acc209,
            amt_acc212,
            amt_acc213,
            amt_acc214,
            amt_acc215,
            amt_acc226,
            amt_acc277,
            amt_acc301,
            amt_acc302,
            amt_acc312,
            amt_acc326,
            amt_acc331,
            amt_acc377,
            amt_acc997,
            amt_acc176,
            amt_acc177,
            amt_acc178,
            date_valid_from,
            date_valid_to,
            amt_acc332,
            amt_acc605,
            amt_acc697,
            amt_acc541,
            amt_acc305,
            amt_acc542,
            amt_acc306,
            amt_acc282,
            amt_acc300,
            amt_acc319,
            amt_acc320,
            amt_acc349,
            amt_acc350,
            amt_acc357,
            amt_acc358,
            amt_acc359,
            amt_acc360,
            amt_acc372,
            amt_acc375,
            amt_acc376,
            amt_acc380,
            amt_acc383,
            amt_acc386,
            amt_acc388,
            amt_acc389,
            amt_acc390,
            amt_acc391,
            amt_acc392,
            amt_acc394,
            amt_acc395,
            amt_acc396,
            amt_acc397,
            amt_acc621,
            amt_acc625,
            amt_acc2282,
            amt_acc543,
            amt_acc544,
            amt_acc547,
            amt_acc308,
            amt_acc335,
            amt_acc353,
            amt_acc272,
            amt_acc219,
            amt_acc275,
            amt_acc181,
            amt_acc185,
            amt_acc183,
            amt_acc316,
            amt_acc382,
            amt_acc373,
            amt_acc273,
            amt_acc998,
            amt_acc303,
            amt_acc179,
            amt_acc546,
            amt_acc548,
            amt_acc500,
            amt_acc323,
            amt_acc549,
            amt_acc545,
            code_transaction_subtype,
            amt_acc5002,
            amt_acc5003,
            amt_acc800,
            amt_acc801,
            amt_acc802,
            amt_acc816,
            amt_acc701,
            amt_acc716,
            amt_acc702,
            amt_acc398,
            amt_acc307,
            amt_acc481,
            amt_acc483,
            amt_acc485,
            amt_acc496,
            amt_acc3310,
            amt_acc540,
            amt_acc703,
            amt_acc704,
            amt_acc803,
            amt_acc804,
            amt_acc3300,
            amt_acc078,
            amt_acc121,
            amt_acc123,
            amt_acc125,
            amt_acc91,
            amt_acc92,
            amt_acc93,
            amt_acc94,
            amt_acc95,
            amt_acc96,
            amt_acc99,
            amt_acc904,
            amt_acc333,
            amt_acc5006,
            amt_acc901,
            amt_acc902,
            amt_acc909,
            amt_acc920,
            amt_acc97,
            amt_acc318,
            amt_acc718,
            amt_acc818,
            amt_acc900,
            amt_acc903,
            amt_acc913,
            amt_acc914,
            amt_acc916,
            amt_acc918,
            amt_acc3501 ,
            amt_acc3502 ,
            amt_acc3504 ,
            amt_acc3500 ,
            amt_acc3503 ,
            amt_acc3516 ,
            amt_acc3518 ,
            text_comment,
            dtime_inserted,
            dtime_updated,
            flag_accrual, --20210223
            amt_acc328,
            amt_acc528,
            amt_acc338,
            amt_acc339,
            amt_acc211,
            amt_acc311,
            amt_acc175,
            AMT_ACC115_ROUNDED,
            AMT_ACC116_ROUNDED,
            AMT_ACC181_ROUNDED,
            AMT_ACC185_ROUNDED,
            AMT_ACC183_ROUNDED,
            AMT_ACC625_ROUNDED,
            AMT_ACC301_ROUNDED,
            AMT_ACC541_ROUNDED,
            AMT_ACC305_ROUNDED,
            AMT_ACC316_ROUNDED,
            AMT_ACC701_ROUNDED,
            AMT_ACC716_ROUNDED,
            AMT_ACC801_ROUNDED,
            AMT_ACC816_ROUNDED,
            AMT_ACC481_ROUNDED,
            AMT_ACC483_ROUNDED,
            AMT_ACC485_ROUNDED,
            AMT_ACC91_ROUNDED,
            AMT_ACC3501_ROUNDED,
            AMT_ACC901_ROUNDED,
            AMT_ACC916_ROUNDED,
            AMT_ACC3516_ROUNDED,
            AMT_ACC175_ROUNDED,
            AMT_ACC176_ROUNDED,
            AMT_ACC331_ROUNDED,
            AMT_ACC605_ROUNDED,
            AMT_ACC2282_ROUNDED,
            AMT_ACC282_ROUNDED,
            AMT_ACC702_ROUNDED,
            AMT_ACC802_ROUNDED,
            AMT_ACC302_ROUNDED,
            AMT_ACC306_ROUNDED,
            AMT_ACC542_ROUNDED,
            AMT_ACC92_ROUNDED,
            AMT_ACC3502_ROUNDED,
            AMT_ACC902_ROUNDED,
            AMT_ACC177_ROUNDED,
            AMT_ACC201_ROUNDED,
            AMT_ACC211_ROUNDED,
            AMT_ACC212_ROUNDED,
            AMT_ACC213_ROUNDED,
            AMT_ACC214_ROUNDED,
            AMT_ACC215_ROUNDED,
            AMT_ACC311_ROUNDED,
            AMT_ACC332_ROUNDED,
            AMT_ACC697_ROUNDED,
            AMT_ACC93_ROUNDED,
            AMT_ACC543_ROUNDED
         )
    with w_accmove as
    (
        SELECT skp_contract 
        FROM LDM_SBV.stm_accounting_online_new_tt 
        where code_owner = v_hcvn
        UNION
        SELECT skp_contract 
        FROM LDM_SBV.ft_accounting_online_new_tt
        WHERE date_accounted >= p_effective_date 
            and date_accounted < p_effective_date + 1
            and code_owner = v_hcvn
    ),
        -- Check a contract was fixed by HCI or not
    w_accmove_contract_type AS (
        SELECT a.skp_contract,
            CASE WHEN b.skp_contract IS NOT NULL
                 THEN v_fixed
                 ELSE v_xna
            END AS code_contract_fixed
        FROM w_accmove a
            LEFT JOIN (
                SELECT DISTINCT skp_contract
                FROM ldm_sbv.ft_neg_interest_hci_fixed_contracts
                WHERE code_fix_batch LIKE '%202301' OR code_fix_batch LIKE '%202302'
            ) b ON a.skp_contract = b.skp_contract
    ),
    w_accmove_data as (
    SELECT /*+ parallel(4) +*/
        incr.skp_contract
        ,code_move_type
        ,amt_accounted_value
        ,amt_accounted_value_r
        ,date_accounted
        ,date_accounting_move
        ,code_transaction_subtype
        ,id_contract
        ,flag_deleted
        ,code_credit_type
        ,text_contract_number
    FROM
        w_accmove_contract_type incr
            JOIN LDM_SBV.ft_accounting_online_new_tt acc ON acc.skp_contract = incr.skp_contract
    WHERE
        (incr.code_contract_fixed = v_xna OR (incr.code_contract_fixed = v_fixed  AND 
            code_move_type not in (
                      '201','211','212','213','214','215','302','311','332','697','542','306','177','92','93','902','543',
                      '2282','282','702','802','3502'
                      )))
        AND
        (
            acc.date_accounted < p_effective_date + 1
            and DTIME_ACC_SYSTEM_CREATED >= v_date_accounted + 1
            and DTIME_ACC_SYSTEM_CREATED < p_effective_date + 1
            and not 
            (
                code_move_type IN ('282','2282')
                and date_accounted = v_date_accounted
                and DTIME_ACC_SYSTEM_CREATED >= v_date_accounted + 1
                and DTIME_ACC_SYSTEM_CREATED < v_date_accounted + 2
            )
        )
        AND acc.FLAG_DELETED = 'N'
    UNION ALL
    SELECT
        incr.skp_contract
        ,code_move_type
        ,amt_accounted_value
        ,amt_accounted_value_r
        ,date_accounted
        ,date_accounting_move
        ,code_transaction_subtype
        ,id_contract
        ,flag_deleted
        ,code_credit_type
        ,text_contract_number
    FROM
        w_accmove_contract_type incr
        JOIN LDM_SBV.ft_accounting_online_new_tt acc ON acc.skp_contract = incr.skp_contract
    WHERE
        incr.code_contract_fixed = v_fixed
        AND acc.date_accounted > v_date_accounted
        AND acc.date_accounted < p_effective_date + 1
        and acc.FLAG_DELETED = 'N'
        AND code_move_type in (
          '201','211','212','213','214','215','302','311','332','697','542','306','177','92','93','902','543',
          '2282','282','702','802','3502'
        )
    )
    SELECT
        i.skf_sbv_account_data                         as skf_sbv_account_data,
            i.code_source_system                           AS code_source_system,
            i.id_source                                    AS id_source,
            i.date_effective                               AS date_effective,
            i.skp_proc_inserted                            AS skp_proc_inserted,
            i.skp_proc_updated                             AS skp_proc_updated,
            i.flag_deleted                                 AS flag_deleted,
            NVL(i.code_credit_type,v_xna)                  AS code_credit_type,
            i.skp_contract                                 as skp_contract,
            NVL(i.id_contract,n_minus_one)                 AS id_contract,
            NVL(i.id_client,n_minus_one)                   AS id_client,
            NVL(i.text_contract_number,v_xna)              AS text_contract_number,
            NVL(i.amt_acc101, 0)                           AS amt_acc101,
            NVL(i.amt_acc115, 0)                           AS amt_acc115,
            NVL(i.amt_acc116, 0)                           AS amt_acc116,
            NVL(i.amt_acc201, 0)                           AS amt_acc201,
            NVL(i.amt_acc209, 0)                           AS amt_acc209,
            NVL(i.amt_acc212, 0)                           AS amt_acc212,
            NVL(i.amt_acc213, 0)                           AS amt_acc213,
            NVL(i.amt_acc214, 0)                           AS amt_acc214,
            NVL(i.amt_acc215, 0)                           AS amt_acc215,
            NVL(i.amt_acc226, 0)                           AS amt_acc226,
            NVL(i.amt_acc277, 0)                           AS amt_acc277,
            NVL(i.amt_acc301, 0)                           AS amt_acc301,
            NVL(i.amt_acc302, 0)                           AS amt_acc302,
            NVL(i.amt_acc312, 0)                           AS amt_acc312,
            NVL(i.amt_acc326, 0)                           AS amt_acc326,
            NVL(i.amt_acc331, 0)                           AS amt_acc331,
            NVL(i.amt_acc377, 0)                           AS amt_acc377,
            NVL(i.amt_acc997, 0)                           AS amt_acc997,
            NVL(i.amt_acc176, 0)                           AS amt_acc176,
            NVL(i.amt_acc177, 0)                           AS amt_acc177,
            NVL(i.amt_acc178, 0)                           AS amt_acc178,
            NVL(i.date_valid_from,d_def_value_date_hist)   AS date_valid_from,
            NVL(i.date_valid_to,d_def_value_date_future)   AS date_valid_to,
            NVL(i.amt_acc332, 0)                           AS amt_acc332,
            NVL(i.amt_acc605, 0)                           AS amt_acc605,
            NVL(i.amt_acc697, 0)                           AS amt_acc697,
            NVL(i.amt_acc541, 0)                           AS amt_acc541,
            NVL(i.amt_acc305, 0)                           AS amt_acc305,
            NVL(i.amt_acc542, 0)                           AS amt_acc542,
            NVL(i.amt_acc306, 0)                           AS amt_acc306,
            NVL(i.amt_acc282, 0)                           AS amt_acc282,
            NVL(i.amt_acc300, 0)                           AS amt_acc300,
            NVL(i.amt_acc319, 0)                           AS amt_acc319,
            NVL(i.amt_acc320, 0)                           AS amt_acc320,
            NVL(i.amt_acc349, 0)                           AS amt_acc349,
            NVL(i.amt_acc350, 0)                           AS amt_acc350,
            NVL(i.amt_acc357, 0)                           AS amt_acc357,
            NVL(i.amt_acc358, 0)                           AS amt_acc358,
            NVL(i.amt_acc359, 0)                           AS amt_acc359,
            NVL(i.amt_acc360, 0)                           AS amt_acc360,
            NVL(i.amt_acc372, 0)                           AS amt_acc372,
            NVL(i.amt_acc375, 0)                           AS amt_acc375,
            NVL(i.amt_acc376, 0)                           AS amt_acc376,
            NVL(i.amt_acc380, 0)                           AS amt_acc380,
            NVL(i.amt_acc383, 0)                           AS amt_acc383,
            NVL(i.amt_acc386, 0)                           AS amt_acc386,
            NVL(i.amt_acc388, 0)                           AS amt_acc388,
            NVL(i.amt_acc389, 0)                           AS amt_acc389,
            NVL(i.amt_acc390, 0)                           AS amt_acc390,
            NVL(i.amt_acc391, 0)                           AS amt_acc391,
            NVL(i.amt_acc392, 0)                           AS amt_acc392,
            NVL(i.amt_acc394, 0)                           AS amt_acc394,
            NVL(i.amt_acc395, 0)                           AS amt_acc395,
            NVL(i.amt_acc396, 0)                           AS amt_acc396,
            NVL(i.amt_acc397, 0)                           AS amt_acc397,
            NVL(i.amt_acc621, 0)                           AS amt_acc621,
            NVL(i.amt_acc625, 0)                           AS amt_acc625,
            NVL(i.amt_acc2282, 0)                          AS amt_acc2282,
            NVL(i.amt_acc543, 0)                           AS amt_acc543,
            NVL(i.amt_acc544, 0)                           AS amt_acc544,
            NVL(i.amt_acc547, 0)                           AS amt_acc547,
            NVL(i.amt_acc308, 0)                           AS amt_acc308,
            NVL(i.amt_acc335, 0)                           AS amt_acc335,
            NVL(i.amt_acc353, 0)                           AS amt_acc353,
            NVL(i.amt_acc272, 0)                           AS amt_acc272,
            NVL(i.amt_acc219, 0)                           AS amt_acc219,
            NVL(i.amt_acc275, 0)                           AS amt_acc275,
            NVL(i.amt_acc181, 0)                           AS amt_acc181,
            NVL(i.amt_acc185, 0)                           AS amt_acc185,
            NVL(i.amt_acc183, 0)                           AS amt_acc183,
            NVL(i.amt_acc316, 0)                           AS amt_acc316,
            NVL(i.amt_acc382, 0)                           AS amt_acc382,
            NVL(i.amt_acc373, 0)                           AS amt_acc373,
            NVL(i.amt_acc273, 0)                           AS amt_acc273,
            NVL(i.amt_acc998, 0)                           AS amt_acc998,
            NVL(i.amt_acc303, 0)                           AS amt_acc303,
            NVL(i.amt_acc179, 0)                           AS amt_acc179,
            NVL(i.amt_acc546, 0)                           AS amt_acc546,
            NVL(i.amt_acc548, 0)                           AS amt_acc548,
            NVL(i.amt_acc500, 0)                           AS amt_acc500,
            NVL(i.amt_acc323, 0)                           AS amt_acc323,
            NVL(i.amt_acc549, 0)                           AS amt_acc549,
            NVL(i.amt_acc545, 0)                           AS amt_acc545,
            NVL(code_transaction_subtype, v_xna)           AS code_transaction_subtype,
            NVL(i.amt_acc5002, 0)                          AS amt_acc5002,
            NVL(i.amt_acc5003, 0)                          AS amt_acc5003,
            NVL(i.amt_acc800, 0)                           AS amt_acc800,
            NVL(i.amt_acc801, 0)                           AS amt_acc801,
            NVL(i.amt_acc802, 0)                           AS amt_acc802,
            NVL(i.amt_acc816, 0)                           AS amt_acc816,
            NVL(i.amt_acc701, 0)                           AS amt_acc701,
            NVL(i.amt_acc716, 0)                           AS amt_acc716,
            NVL(i.amt_acc702, 0)                           AS amt_acc702,
            NVL(i.amt_acc398, 0)                           AS amt_acc398,
            NVL(i.amt_acc307, 0)                           AS amt_acc307,
            NVL(i.amt_acc481, 0)                           AS amt_acc481,
            NVL(i.amt_acc483, 0)                           AS amt_acc483,
            NVL(i.amt_acc485, 0)                           AS amt_acc485,
            NVL(i.amt_acc496, 0)                           AS amt_acc496,
            NVL(i.amt_acc3310, 0)                          AS amt_acc3310,
            NVL(i.amt_acc540, 0)                           AS amt_acc540,
            NVL(i.amt_acc703, 0)                           AS amt_acc703,
            NVL(i.amt_acc704, 0)                           AS amt_acc704,
            NVL(i.amt_acc803, 0)                           AS amt_acc803,
            NVL(i.amt_acc804, 0)                           AS amt_acc804,
            NVL(i.amt_acc3300, 0)                          AS amt_acc3300,
            NVL(i.amt_acc078, 0)                           AS amt_acc078,
            NVL(i.amt_acc121, 0)                           AS amt_acc121,
            NVL(i.amt_acc123, 0)                           AS amt_acc123,
            NVL(i.amt_acc125, 0)                           AS amt_acc125,
            NVL(i.amt_acc91,  0)                           AS amt_acc91,
            NVL(i.amt_acc92,  0)                           AS amt_acc92,
            NVL(i.amt_acc93,  0)                           AS amt_acc93,
            NVL(i.amt_acc94,  0)                           AS amt_acc94,
            NVL(i.amt_acc95,  0)                           AS amt_acc95,
            NVL(i.amt_acc96,  0)                           AS amt_acc96,
            NVL(i.amt_acc99,  0)                           AS amt_acc99,
            NVL(i.amt_acc904,  0)                          AS amt_acc904,
            NVL(i.amt_acc333,  0)                          AS amt_acc333,
            NVL(i.amt_acc5006,  0)                         AS amt_acc5006,
            NVL(i.amt_acc901,  0)                          AS amt_acc901,
            NVL(i.amt_acc902,  0)                          AS amt_acc902,
            NVL(i.amt_acc909,  0)                          AS amt_acc909,
            NVL(i.amt_acc920,  0)                          AS amt_acc920,
            NVL(i.amt_acc97,  0)                           AS amt_acc97,
            NVL(i.amt_acc318,  0)                          AS amt_acc318,
            NVL(i.amt_acc718,  0)                          AS amt_acc718,
            NVL(i.amt_acc818,  0)                          AS amt_acc818,
            NVL(i.amt_acc900,  0)                          AS amt_acc900,
            NVL(i.amt_acc903,  0)                          AS amt_acc903,
            NVL(i.amt_acc913,  0)                          AS amt_acc913,
            NVL(i.amt_acc914,  0)                          AS amt_acc914,
            NVL(i.amt_acc916,  0)                          AS amt_acc916,
            NVL(i.amt_acc918,  0)                          AS amt_acc918,
            NVL(i.amt_acc3501,  0)                         AS amt_acc3501,
            NVL(i.amt_acc3502,  0)                         AS amt_acc3502,
            NVL(i.amt_acc3504,  0)                         AS amt_acc3504,
            NVL(i.amt_acc3500,  0)                         AS amt_acc3500,
            NVL(i.amt_acc3503,  0)                         AS amt_acc3503,
            NVL(i.amt_acc3516,  0)                         AS amt_acc3516,
            NVL(i.amt_acc3518,  0)                         AS amt_acc3518,
            NVL(text_comment, v_xna)                       AS text_comment,
            sysdate                                        AS dtime_inserted,
            sysdate                                        AS dtime_updated,
            flag_accrual, --20210223,
            NVL(i.amt_acc328,  0)                          AS amt_acc328,
            NVL(i.amt_acc528,  0)                          AS amt_acc528,
            NVL(i.amt_acc338,  0)                          AS amt_acc338,
            NVL(i.amt_acc339,  0)                          AS amt_acc339,
            NVL(i.amt_acc211,  0)                          AS amt_acc211,
            NVL(i.amt_acc311,  0)                          AS amt_acc311,
            NVL(i.amt_acc175, 0)                           AS amt_acc175,
            NVL(i.AMT_ACC115_ROUNDED,0)                    AS AMT_ACC115_ROUNDED,
            NVL(i.AMT_ACC116_ROUNDED,0)                    AS AMT_ACC116_ROUNDED,
            NVL(i.AMT_ACC181_ROUNDED,0)                    AS AMT_ACC181_ROUNDED,
            NVL(i.AMT_ACC185_ROUNDED,0)                    AS AMT_ACC185_ROUNDED,
            NVL(i.AMT_ACC183_ROUNDED,0)                    AS AMT_ACC183_ROUNDED,
            NVL(i.AMT_ACC625_ROUNDED,0)                    AS AMT_ACC625_ROUNDED,
            NVL(i.AMT_ACC301_ROUNDED,0)                    AS AMT_ACC301_ROUNDED,
            NVL(i.AMT_ACC541_ROUNDED,0)                    AS AMT_ACC541_ROUNDED,
            NVL(i.AMT_ACC305_ROUNDED,0)                    AS AMT_ACC305_ROUNDED,
            NVL(i.AMT_ACC316_ROUNDED,0)                    AS AMT_ACC316_ROUNDED,
            NVL(i.AMT_ACC701_ROUNDED,0)                    AS AMT_ACC701_ROUNDED,
            NVL(i.AMT_ACC716_ROUNDED,0)                    AS AMT_ACC716_ROUNDED,
            NVL(i.AMT_ACC801_ROUNDED,0)                    AS AMT_ACC801_ROUNDED,
            NVL(i.AMT_ACC816_ROUNDED,0)                    AS AMT_ACC816_ROUNDED,
            NVL(i.AMT_ACC481_ROUNDED,0)                    AS AMT_ACC481_ROUNDED,
            NVL(i.AMT_ACC483_ROUNDED,0)                    AS AMT_ACC483_ROUNDED,
            NVL(i.AMT_ACC485_ROUNDED,0)                    AS AMT_ACC485_ROUNDED,
            NVL(i.AMT_ACC91_ROUNDED,0)                     AS AMT_ACC91_ROUNDED,
            NVL(i.AMT_ACC3501_ROUNDED,0)                   AS AMT_ACC3501_ROUNDED,
            NVL(i.AMT_ACC901_ROUNDED,0)                    AS AMT_ACC901_ROUNDED,
            NVL(i.AMT_ACC916_ROUNDED,0)                    AS AMT_ACC916_ROUNDED,
            NVL(i.AMT_ACC3516_ROUNDED,0)                   AS AMT_ACC3516_ROUNDED,
            NVL(i.AMT_ACC175_ROUNDED,0)                    AS AMT_ACC175_ROUNDED,
            NVL(i.AMT_ACC176_ROUNDED,0)                    AS AMT_ACC176_ROUNDED,
            NVL(i.AMT_ACC331_ROUNDED,0)                    AS AMT_ACC331_ROUNDED,
            NVL(i.AMT_ACC605_ROUNDED,0)                    AS AMT_ACC605_ROUNDED,
            NVL(i.AMT_ACC2282_ROUNDED,0)                   AS AMT_ACC2282_ROUNDED,
            NVL(i.AMT_ACC282_ROUNDED,0)                    AS AMT_ACC282_ROUNDED,
            NVL(i.AMT_ACC702_ROUNDED,0)                    AS AMT_ACC702_ROUNDED,
            NVL(i.AMT_ACC802_ROUNDED,0)                    AS AMT_ACC802_ROUNDED,
            NVL(i.AMT_ACC302_ROUNDED,0)                    AS AMT_ACC302_ROUNDED,
            NVL(i.AMT_ACC306_ROUNDED,0)                    AS AMT_ACC306_ROUNDED,
            NVL(i.AMT_ACC542_ROUNDED,0)                    AS AMT_ACC542_ROUNDED,
            NVL(i.AMT_ACC92_ROUNDED,0)                     AS AMT_ACC92_ROUNDED,
            NVL(i.AMT_ACC3502_ROUNDED,0)                   AS AMT_ACC3502_ROUNDED,
            NVL(i.AMT_ACC902_ROUNDED,0)                    AS AMT_ACC902_ROUNDED,
            NVL(i.AMT_ACC177_ROUNDED,0)                    AS AMT_ACC177_ROUNDED,
            NVL(i.AMT_ACC201_ROUNDED,0)                    AS AMT_ACC201_ROUNDED,
            NVL(i.AMT_ACC211_ROUNDED,0)                    AS AMT_ACC211_ROUNDED,
            NVL(i.AMT_ACC212_ROUNDED,0)                    AS AMT_ACC212_ROUNDED,
            NVL(i.AMT_ACC213_ROUNDED,0)                    AS AMT_ACC213_ROUNDED,
            NVL(i.AMT_ACC214_ROUNDED,0)                    AS AMT_ACC214_ROUNDED,
            NVL(i.AMT_ACC215_ROUNDED,0)                    AS AMT_ACC215_ROUNDED,
            NVL(i.AMT_ACC311_ROUNDED,0)                    AS AMT_ACC311_ROUNDED,
            NVL(i.AMT_ACC332_ROUNDED,0)                    AS AMT_ACC332_ROUNDED,
            NVL(i.AMT_ACC697_ROUNDED,0)                    AS AMT_ACC697_ROUNDED,
            NVL(i.AMT_ACC93_ROUNDED,0)                     AS AMT_ACC93_ROUNDED,
            NVL(i.AMT_ACC543_ROUNDED,0)                    AS AMT_ACC543_ROUNDED
        FROM
            (SELECT
                NULL                                                                as skf_sbv_account_data,
                v_default_code_source_system                                        AS code_source_system,
                to_char(acc.id_contract)||'.'||to_char(p_effective_date,'yyyymmdd')||'.'||NVL(acc.code_transaction_subtype, v_xna)       AS ID_SOURCE,
                p_effective_date                                                    AS date_effective,
                p_process_key                                                       AS skp_proc_inserted,
                p_process_key                                                       AS skp_proc_updated,
                acc.flag_deleted                                                    AS flag_deleted,
                acc.skp_contract                                                    as skp_contract,
                acc.id_contract                                                     as id_contract,
                pro.id_client                                                       as id_client,
                acc.code_credit_type                                                AS code_credit_type,
                acc.text_contract_number                                            AS text_contract_number,
                sum(decode(acc.code_move_type, '101', acc.amt_accounted_value, 0))  AS amt_acc101,
                sum(decode(acc.code_move_type, '115', acc.amt_accounted_value, 0))  AS amt_acc115,
                sum(decode(acc.code_move_type, '116', acc.amt_accounted_value, 0))  AS amt_acc116,
                sum(decode(acc.code_move_type, '201', acc.amt_accounted_value, 0))  AS amt_acc201,
                sum(decode(acc.code_move_type, '209', acc.amt_accounted_value, 0))  AS amt_acc209,
                sum(decode(acc.code_move_type, '212', acc.amt_accounted_value, 0))  AS amt_acc212,
                sum(decode(acc.code_move_type, '213', acc.amt_accounted_value, 0))  AS amt_acc213,
                sum(decode(acc.code_move_type, '214', acc.amt_accounted_value, 0))  AS amt_acc214,
                sum(decode(acc.code_move_type, '215', acc.amt_accounted_value, 0))  AS amt_acc215,
                sum(decode(acc.code_move_type, '226', acc.amt_accounted_value, 0))  AS amt_acc226,
                sum(decode(acc.code_move_type, '277', acc.amt_accounted_value, 0))  AS amt_acc277,
                sum(decode(acc.code_move_type, '301', acc.amt_accounted_value, 0))  AS amt_acc301,
                sum(decode(acc.code_move_type, '302', acc.amt_accounted_value, 0))  AS amt_acc302,
                sum(decode(acc.code_move_type, '312', acc.amt_accounted_value, 0))  AS amt_acc312,
                sum(decode(acc.code_move_type, '326', acc.amt_accounted_value, 0))  AS amt_acc326,
                sum(decode(acc.code_move_type, '331', acc.amt_accounted_value, 0))  AS amt_acc331,
                sum(decode(acc.code_move_type, '377', acc.amt_accounted_value, 0))  AS amt_acc377,
                sum(decode(acc.code_move_type, '997', acc.amt_accounted_value, 0))  AS amt_acc997,
                sum(decode(acc.code_move_type, '176', acc.amt_accounted_value, 0))  AS amt_acc176,
                sum(decode(acc.code_move_type, '177', acc.amt_accounted_value, 0))  AS amt_acc177,
                sum(decode(acc.code_move_type, '178', acc.amt_accounted_value, 0))  AS amt_acc178,
                p_effective_date                                                    AS date_valid_from,
                d_def_value_date_future                                             AS date_valid_to,
                SUM(DECODE(acc.code_move_type, '332', acc.amt_accounted_value, 0))  AS amt_acc332,
                SUM(DECODE(acc.code_move_type, '605', acc.amt_accounted_value, 0))  AS amt_acc605,
                SUM(DECODE(acc.code_move_type, '697', acc.amt_accounted_value, 0))  AS amt_acc697,
                SUM(DECODE(acc.code_move_type, '541', acc.amt_accounted_value, 0))  AS amt_acc541,
                SUM(DECODE(acc.code_move_type, '305', acc.amt_accounted_value, 0))  AS amt_acc305,
                SUM(DECODE(acc.code_move_type, '542', acc.amt_accounted_value, 0))  AS amt_acc542,
                SUM(DECODE(acc.code_move_type, '306', acc.amt_accounted_value, 0))  AS amt_acc306,
                SUM(DECODE(acc.code_move_type, '282', acc.amt_accounted_value, 0))  AS amt_acc282,
                SUM(DECODE(acc.code_move_type, '300', acc.amt_accounted_value, 0))  AS amt_acc300,
                SUM(DECODE(acc.code_move_type, '319', acc.amt_accounted_value, 0))  AS amt_acc319,
                SUM(DECODE(acc.code_move_type, '320', acc.amt_accounted_value, 0))  AS amt_acc320,
                SUM(DECODE(acc.code_move_type, '349', acc.amt_accounted_value, 0))  AS amt_acc349,
                SUM(DECODE(acc.code_move_type, '350', acc.amt_accounted_value, 0))  AS amt_acc350,
                SUM(DECODE(acc.code_move_type, '357', acc.amt_accounted_value, 0))  AS amt_acc357,
                SUM(DECODE(acc.code_move_type, '358', acc.amt_accounted_value, 0))  AS amt_acc358,
                SUM(DECODE(acc.code_move_type, '359', acc.amt_accounted_value, 0))  AS amt_acc359,
                SUM(DECODE(acc.code_move_type, '360', acc.amt_accounted_value, 0))  AS amt_acc360,
                SUM(DECODE(acc.code_move_type, '372', acc.amt_accounted_value, 0))  AS amt_acc372,
                SUM(DECODE(acc.code_move_type, '375', acc.amt_accounted_value, 0))  AS amt_acc375,
                SUM(DECODE(acc.code_move_type, '376', acc.amt_accounted_value, 0))  AS amt_acc376,
                SUM(DECODE(acc.code_move_type, '380', acc.amt_accounted_value, 0))  AS amt_acc380,
                SUM(DECODE(acc.code_move_type, '383', acc.amt_accounted_value, 0))  AS amt_acc383,
                SUM(DECODE(acc.code_move_type, '386', acc.amt_accounted_value, 0))  AS amt_acc386,
                SUM(DECODE(acc.code_move_type, '388', acc.amt_accounted_value, 0))  AS amt_acc388,
                SUM(DECODE(acc.code_move_type, '389', acc.amt_accounted_value, 0))  AS amt_acc389,
                SUM(DECODE(acc.code_move_type, '390', acc.amt_accounted_value, 0))  AS amt_acc390,
                SUM(DECODE(acc.code_move_type, '391', acc.amt_accounted_value, 0))  AS amt_acc391,
                SUM(DECODE(acc.code_move_type, '392', acc.amt_accounted_value, 0))  AS amt_acc392,
                SUM(DECODE(acc.code_move_type, '394', acc.amt_accounted_value, 0))  AS amt_acc394,
                SUM(DECODE(acc.code_move_type, '395', acc.amt_accounted_value, 0))  AS amt_acc395,
                SUM(DECODE(acc.code_move_type, '396', acc.amt_accounted_value, 0))  AS amt_acc396,
                SUM(DECODE(acc.code_move_type, '397', acc.amt_accounted_value, 0))  AS amt_acc397,
                SUM(DECODE(acc.code_move_type, '621', acc.amt_accounted_value, 0))  AS amt_acc621,
                SUM(DECODE(acc.code_move_type, '625', acc.amt_accounted_value, 0))  AS amt_acc625,
                SUM(DECODE(acc.code_move_type, '2282', acc.amt_accounted_value, 0)) AS amt_acc2282,
                SUM(DECODE(acc.code_move_type, '543', acc.amt_accounted_value, 0))  AS amt_acc543,
                SUM(DECODE(acc.code_move_type, '544', acc.amt_accounted_value, 0))  AS amt_acc544,
                SUM(DECODE(acc.code_move_type, '547', acc.amt_accounted_value, 0))  AS amt_acc547,
                SUM(DECODE(acc.code_move_type, '308', acc.amt_accounted_value, 0))  AS amt_acc308,
                SUM(DECODE(acc.code_move_type, '335', acc.amt_accounted_value, 0))  AS amt_acc335,
                SUM(DECODE(acc.code_move_type, '353', acc.amt_accounted_value, 0))  AS amt_acc353,
                SUM(DECODE(acc.code_move_type, '272', acc.amt_accounted_value, 0))  AS amt_acc272,
                SUM(DECODE(acc.code_move_type, '219', acc.amt_accounted_value, 0))  AS amt_acc219,
                SUM(DECODE(acc.code_move_type, '275', acc.amt_accounted_value, 0))  AS amt_acc275,
                SUM(DECODE(acc.code_move_type, '181', acc.amt_accounted_value, 0))  AS amt_acc181,
                SUM(DECODE(acc.code_move_type, '185', acc.amt_accounted_value, 0))  AS amt_acc185,
                SUM(DECODE(acc.code_move_type, '183', acc.amt_accounted_value, 0))  AS amt_acc183,
                SUM(DECODE(acc.code_move_type, '316', acc.amt_accounted_value, 0))  AS amt_acc316,
                SUM(DECODE(acc.code_move_type, '382', acc.amt_accounted_value, 0))  AS amt_acc382,
                SUM(DECODE(acc.code_move_type, '373', acc.amt_accounted_value, 0))  AS amt_acc373,
                SUM(DECODE(acc.code_move_type, '273', acc.amt_accounted_value, 0))  AS amt_acc273,
                SUM(DECODE(acc.code_move_type, '998', acc.amt_accounted_value, 0))  AS amt_acc998,
                SUM(DECODE(acc.code_move_type, '303', acc.amt_accounted_value, 0))  AS amt_acc303,
                SUM(DECODE(acc.code_move_type, '179', acc.amt_accounted_value, 0))  AS amt_acc179,
                SUM(DECODE(acc.code_move_type, '546', acc.amt_accounted_value, 0))  AS amt_acc546,
                SUM(DECODE(acc.code_move_type, '548', acc.amt_accounted_value, 0))  AS amt_acc548,
                SUM(DECODE(acc.code_move_type, '500', acc.amt_accounted_value, 0))  AS amt_acc500,
                SUM(DECODE(acc.code_move_type, '323', acc.amt_accounted_value, 0))  AS amt_acc323,
                SUM(DECODE(acc.code_move_type, '549', acc.amt_accounted_value, 0))  AS amt_acc549,
                SUM(DECODE(acc.code_move_type, '545', acc.amt_accounted_value, 0))  AS amt_acc545,
                acc.CODE_TRANSACTION_SUBTYPE                                        as CODE_TRANSACTION_SUBTYPE,
                SUM(DECODE(acc.code_move_type, '5002', acc.amt_accounted_value, 0)) AS amt_acc5002,
                SUM(DECODE(acc.code_move_type, '5003', acc.amt_accounted_value, 0)) AS amt_acc5003,
                SUM(DECODE(acc.code_move_type, '800', acc.amt_accounted_value, 0))  AS amt_acc800,
                SUM(DECODE(acc.code_move_type, '801', acc.amt_accounted_value, 0))  AS amt_acc801,
                SUM(DECODE(acc.code_move_type, '802', acc.amt_accounted_value, 0))  AS amt_acc802,
                SUM(DECODE(acc.code_move_type, '816', acc.amt_accounted_value, 0))  AS amt_acc816,
                SUM(DECODE(acc.code_move_type, '701', acc.amt_accounted_value, 0))  AS amt_acc701,
                SUM(DECODE(acc.code_move_type, '716', acc.amt_accounted_value, 0))  AS amt_acc716,
                SUM(DECODE(acc.code_move_type, '702', acc.amt_accounted_value, 0))  AS amt_acc702,
                SUM(DECODE(acc.code_move_type, '398', acc.amt_accounted_value, 0))  AS amt_acc398,
                SUM(DECODE(acc.code_move_type, '307', acc.amt_accounted_value, 0))  AS amt_acc307,
                SUM(DECODE(acc.code_move_type, '481', acc.amt_accounted_value, 0))  AS amt_acc481,
                SUM(DECODE(acc.code_move_type, '483', acc.amt_accounted_value, 0))  AS amt_acc483,
                SUM(DECODE(acc.code_move_type, '485', acc.amt_accounted_value, 0))  AS amt_acc485,
                SUM(DECODE(acc.code_move_type, '496', acc.amt_accounted_value, 0))  AS amt_acc496,
                SUM(DECODE(acc.code_move_type, '3310', acc.amt_accounted_value, 0)) AS amt_acc3310,
                SUM(DECODE(acc.code_move_type, '540', acc.amt_accounted_value, 0))  AS amt_acc540,
                SUM(DECODE(acc.code_move_type, '703', acc.amt_accounted_value, 0))  AS amt_acc703,
                SUM(DECODE(acc.code_move_type, '704', acc.amt_accounted_value, 0))  AS amt_acc704,
                SUM(DECODE(acc.code_move_type, '803', acc.amt_accounted_value, 0))  AS amt_acc803,
                SUM(DECODE(acc.code_move_type, '804', acc.amt_accounted_value, 0))  AS amt_acc804,
                SUM(DECODE(acc.code_move_type, '3300', acc.amt_accounted_value, 0)) AS amt_acc3300,
                SUM(DECODE(acc.code_move_type, '078', acc.amt_accounted_value, 0))  AS amt_acc078,
                SUM(DECODE(acc.code_move_type, '121', acc.amt_accounted_value, 0))  AS amt_acc121,
                SUM(DECODE(acc.code_move_type, '123', acc.amt_accounted_value, 0))  AS amt_acc123,
                SUM(DECODE(acc.code_move_type, '125', acc.amt_accounted_value, 0))  AS amt_acc125,
                SUM(DECODE(acc.code_move_type, '91', acc.amt_accounted_value, 0))   AS amt_acc91,
                SUM(DECODE(acc.code_move_type, '92', acc.amt_accounted_value, 0))   AS amt_acc92,
                SUM(DECODE(acc.code_move_type, '93', acc.amt_accounted_value, 0))   AS amt_acc93,
                SUM(DECODE(acc.code_move_type, '94', acc.amt_accounted_value, 0))   AS amt_acc94,
                SUM(DECODE(acc.code_move_type, '95', acc.amt_accounted_value, 0))   AS amt_acc95,
                SUM(DECODE(acc.code_move_type, '96', acc.amt_accounted_value, 0))   AS amt_acc96,
                SUM(DECODE(acc.code_move_type, '99', acc.amt_accounted_value, 0))   AS amt_acc99,
                SUM(DECODE(acc.code_move_type, '904', acc.amt_accounted_value, 0))  AS amt_acc904,
                SUM(DECODE(acc.code_move_type, '333', acc.amt_accounted_value, 0))  AS amt_acc333,
                SUM(DECODE(acc.code_move_type, '5006', acc.amt_accounted_value, 0)) AS amt_acc5006,
                SUM(DECODE(acc.code_move_type, '901', acc.amt_accounted_value, 0))  AS amt_acc901,
                SUM(DECODE(acc.code_move_type, '902', acc.amt_accounted_value, 0))  AS amt_acc902,
                SUM(DECODE(acc.code_move_type, '909', acc.amt_accounted_value, 0))  AS amt_acc909,
                SUM(DECODE(acc.code_move_type, '920', acc.amt_accounted_value, 0))  AS amt_acc920,
                SUM(DECODE(acc.code_move_type, '97', acc.amt_accounted_value, 0))   AS amt_acc97,
                SUM(DECODE(acc.code_move_type, '318', acc.amt_accounted_value, 0))  AS amt_acc318,
                SUM(DECODE(acc.code_move_type, '718', acc.amt_accounted_value, 0))  AS amt_acc718,
                SUM(DECODE(acc.code_move_type, '818', acc.amt_accounted_value, 0))  AS amt_acc818,
                SUM(DECODE(acc.code_move_type, '900', acc.amt_accounted_value, 0))  AS amt_acc900,
                SUM(DECODE(acc.code_move_type, '903', acc.amt_accounted_value, 0))  AS amt_acc903,
                SUM(DECODE(acc.code_move_type, '913', acc.amt_accounted_value, 0))  AS amt_acc913,
                SUM(DECODE(acc.code_move_type, '914', acc.amt_accounted_value, 0))  AS amt_acc914,
                SUM(DECODE(acc.code_move_type, '916', acc.amt_accounted_value, 0))  AS amt_acc916,
                SUM(DECODE(acc.code_move_type, '918', acc.amt_accounted_value, 0))  AS amt_acc918,
                SUM(DECODE(acc.code_move_type, '3501', acc.amt_accounted_value, 0)) AS amt_acc3501,
                SUM(DECODE(acc.code_move_type, '3502', acc.amt_accounted_value, 0)) AS amt_acc3502,
                SUM(DECODE(acc.code_move_type, '3504', acc.amt_accounted_value, 0)) AS amt_acc3504,
                SUM(DECODE(acc.code_move_type, '3500', acc.amt_accounted_value, 0)) AS amt_acc3500,
                SUM(DECODE(acc.code_move_type, '3503', acc.amt_accounted_value, 0)) AS amt_acc3503,
                SUM(DECODE(acc.code_move_type, '3516', acc.amt_accounted_value, 0)) AS amt_acc3516,
                SUM(DECODE(acc.code_move_type, '3518', acc.amt_accounted_value, 0)) AS amt_acc3518,
                NULL as text_comment,
                max(case when acc.code_move_type in ('2282', '282', '201', '211', '212', '213', '214', '215')
                            and acc.DATE_ACCOUNTED = last_day(add_months(p_effective_date, -1))
                            and acc.DATE_ACCOUNTING_MOVE = last_day(add_months(p_effective_date, -1))
                            and p_effective_date = trunc(p_effective_date, 'MM')
                        then 1
                        else 0
                    end) flag_accrual ,--20210223,
                SUM(DECODE(acc.code_move_type, '328', acc.amt_accounted_value, 0))  AS amt_acc328,
                SUM(DECODE(acc.code_move_type, '528', acc.amt_accounted_value, 0))  AS amt_acc528,
                SUM(DECODE(acc.code_move_type, '338', acc.amt_accounted_value, 0))  AS amt_acc338,
                SUM(DECODE(acc.code_move_type, '339', acc.amt_accounted_value, 0))  AS amt_acc339,
                SUM(DECODE(acc.code_move_type, '211', acc.amt_accounted_value, 0))  AS amt_acc211,
                SUM(DECODE(acc.code_move_type, '311', acc.amt_accounted_value, 0))  AS amt_acc311,
                sum(decode(acc.code_move_type, '175', acc.amt_accounted_value, 0))  AS amt_acc175,
                SUM(DECODE(acc.code_move_type, '115', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC115_ROUNDED,
                SUM(DECODE(acc.code_move_type, '116', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC116_ROUNDED,
                SUM(DECODE(acc.code_move_type, '181', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC181_ROUNDED,
                SUM(DECODE(acc.code_move_type, '185', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC185_ROUNDED,
                SUM(DECODE(acc.code_move_type, '183', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC183_ROUNDED,
                SUM(DECODE(acc.code_move_type, '625', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC625_ROUNDED,
                SUM(DECODE(acc.code_move_type, '301', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC301_ROUNDED,
                SUM(DECODE(acc.code_move_type, '541', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC541_ROUNDED,
                SUM(DECODE(acc.code_move_type, '305', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC305_ROUNDED,
                SUM(DECODE(acc.code_move_type, '316', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC316_ROUNDED,
                SUM(DECODE(acc.code_move_type, '701', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC701_ROUNDED,
                SUM(DECODE(acc.code_move_type, '716', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC716_ROUNDED,
                SUM(DECODE(acc.code_move_type, '801', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC801_ROUNDED,
                SUM(DECODE(acc.code_move_type, '816', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC816_ROUNDED,
                SUM(DECODE(acc.code_move_type, '481', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC481_ROUNDED,
                SUM(DECODE(acc.code_move_type, '483', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC483_ROUNDED,
                SUM(DECODE(acc.code_move_type, '485', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC485_ROUNDED,
                SUM(DECODE(acc.code_move_type, '91', acc.AMT_ACCOUNTED_VALUE_R, 0))  AS AMT_ACC91_ROUNDED,
                SUM(DECODE(acc.code_move_type, '3501', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC3501_ROUNDED,
                SUM(DECODE(acc.code_move_type, '901', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC901_ROUNDED,
                SUM(DECODE(acc.code_move_type, '916', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC916_ROUNDED,
                SUM(DECODE(acc.code_move_type, '3516', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC3516_ROUNDED,
                SUM(DECODE(acc.code_move_type, '175', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC175_ROUNDED,
                SUM(DECODE(acc.code_move_type, '176', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC176_ROUNDED,
                SUM(DECODE(acc.code_move_type, '331', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC331_ROUNDED,
                SUM(DECODE(acc.code_move_type, '605', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC605_ROUNDED,
                SUM(DECODE(acc.code_move_type, '2282', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC2282_ROUNDED,
                SUM(DECODE(acc.code_move_type, '282', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC282_ROUNDED,
                SUM(DECODE(acc.code_move_type, '702', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC702_ROUNDED,
                SUM(DECODE(acc.code_move_type, '802', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC802_ROUNDED,
                SUM(DECODE(acc.code_move_type, '302', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC302_ROUNDED,
                SUM(DECODE(acc.code_move_type, '306', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC306_ROUNDED,
                SUM(DECODE(acc.code_move_type, '542', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC542_ROUNDED,
                SUM(DECODE(acc.code_move_type, '92', acc.AMT_ACCOUNTED_VALUE_R, 0))  AS AMT_ACC92_ROUNDED,
                SUM(DECODE(acc.code_move_type, '3502', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC3502_ROUNDED,
                SUM(DECODE(acc.code_move_type, '902', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC902_ROUNDED,
                SUM(DECODE(acc.code_move_type, '177', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC177_ROUNDED,
                SUM(DECODE(acc.code_move_type, '201', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC201_ROUNDED,
                SUM(DECODE(acc.code_move_type, '211', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC211_ROUNDED,
                SUM(DECODE(acc.code_move_type, '212', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC212_ROUNDED,
                SUM(DECODE(acc.code_move_type, '213', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC213_ROUNDED,
                SUM(DECODE(acc.code_move_type, '214', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC214_ROUNDED,
                SUM(DECODE(acc.code_move_type, '215', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC215_ROUNDED,
                SUM(DECODE(acc.code_move_type, '311', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC311_ROUNDED,
                SUM(DECODE(acc.code_move_type, '332', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC332_ROUNDED,
                SUM(DECODE(acc.code_move_type, '697', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC697_ROUNDED,
                SUM(DECODE(acc.code_move_type, '93', acc.AMT_ACCOUNTED_VALUE_R, 0))  AS AMT_ACC93_ROUNDED,
                SUM(DECODE(acc.code_move_type, '543', acc.AMT_ACCOUNTED_VALUE_R, 0)) AS AMT_ACC543_ROUNDED
            FROM
                w_accmove_data acc
                JOIN LDM_SBV.dct_contract pro on acc.skp_contract = pro.skp_contract
            WHERE
                pro.code_credit_owner = v_hcvn
            GROUP BY acc.code_credit_type,
                acc.text_contract_number,
                acc.skp_contract,
                acc.id_contract,
                acc.flag_deleted,
                pro.id_client,
                acc.code_transaction_subtype
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
                                  cascade          => TRUE);

    v_step := 'Close log';
    owner_core.ms_global_log.write_map_end(p_process_key  => p_process_key,
                                          p_mapping_name => c_mapping,
                                          p_status       => 'COMPLETE');
  EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        owner_core.etl_log_api.logend2(p_proc_key => p_process_key,
                                      p_log_name => c_mapping,
                                      p_status   => 'FAILED',
                                      p_info     => v_step || ', SQLERRM = ' || substr(dbms_utility.format_error_stack, 1, 300));
        LDM_SBV_ETL.SP_SEND_NOTIFICATION (v_dm2_err_addr, 'LIB_FT_SBV_ACCOUNT_TD.'|| c_mapping ,v_step || ', SQLERRM = ' || substr(dbms_utility.format_error_stack, 1, 300));
       
        raise_application_error(-20123, 'Error in module '||c_mapping||' ('||v_step||')', TRUE);
END ft_sbvaccdat_map_sbv;

PROCEDURE ft_sbvaccdat_diff_sbv (p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2)
IS
  v_step           VARCHAR2(255);
  v_cnt            INTEGER;
  c_mapping        VARCHAR2(60) := 'FT_SBVACCDAT_DIFF_SBV';
  c_table_owner    VARCHAR2(40) := 'LDM_SBV';
  c_table_name     VARCHAR2(40) := 'STC_SBV_ACCOUNT_DATA_TD';
BEGIN
    v_step := 'Start log';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                          p_mapping_name => c_mapping);

    v_step := 'Truncate table ' || c_table_owner || '.' || c_table_name;
    LDM_SBV.truncate_table(c_table_owner, c_table_name);

    v_step := 'Insert data into LDM_SBV.stc_sbv_account_data_td';

    owner_core.lib_support_utils.alter_session_manual(4);

    INSERT /*+ APPEND */ INTO LDM_SBV.stc_sbv_account_data_td t
        (
            skf_sbv_account_data,
            code_source_system,
            id_source,
            date_effective,
            skp_proc_inserted,
            skp_proc_updated,
            flag_deleted,
            code_credit_type,
            skp_contract,
            id_contract,
            id_client,
            text_contract_number,
            amt_acc101,
            amt_acc115,
            amt_acc116,
            amt_acc201,
            amt_acc209,
            amt_acc212,
            amt_acc213,
            amt_acc214,
            amt_acc215,
            amt_acc226,
            amt_acc277,
            amt_acc301,
            amt_acc302,
            amt_acc312,
            amt_acc326,
            amt_acc331,
            amt_acc377,
            amt_acc997,
            amt_acc176,
            amt_acc177,
            amt_acc178,
            date_valid_from,
            date_valid_to,
            code_change_type,
            id_source_previous,
            amt_acc332,
            amt_acc605,
            amt_acc697,
            amt_acc541,
            amt_acc305,
            amt_acc542,
            amt_acc306,
            amt_acc282,
            amt_acc300,
            amt_acc319,
            amt_acc320,
            amt_acc349,
            amt_acc350,
            amt_acc357,
            amt_acc358,
            amt_acc359,
            amt_acc360,
            amt_acc372,
            amt_acc375,
            amt_acc376,
            amt_acc380,
            amt_acc383,
            amt_acc386,
            amt_acc388,
            amt_acc389,
            amt_acc390,
            amt_acc391,
            amt_acc392,
            amt_acc394,
            amt_acc395,
            amt_acc396,
            amt_acc397,
            amt_acc621,
            amt_acc625,
            amt_acc2282,
            amt_acc543,
            amt_acc544,
            amt_acc547,
            amt_acc308,
            amt_acc335,
            amt_acc353,
            amt_acc272,
            amt_acc219,
            amt_acc275,
            amt_acc181,
            amt_acc185,
            amt_acc183,
            amt_acc316,
            amt_acc382,
            amt_acc373,
            amt_acc273,
            amt_acc998,
            amt_acc303,
            amt_acc179,
            amt_acc546,
            amt_acc548,
            amt_acc500,
            amt_acc323,
            amt_acc549,
            amt_acc545,
            code_transaction_subtype,
            amt_acc5002,
            amt_acc5003,
            amt_acc800,
            amt_acc801,
            amt_acc802,
            amt_acc816,
            amt_acc701,
            amt_acc716,
            amt_acc702,
            amt_acc398,
            amt_acc307,
            amt_acc481,
            amt_acc483,
            amt_acc485,
            amt_acc496,
            amt_acc3310,
            amt_acc540,
            amt_acc703,
            amt_acc704,
            amt_acc803,
            amt_acc804,
            amt_acc3300,
            amt_acc078,
            amt_acc121,
            amt_acc123,
            amt_acc125,
            amt_acc91,
            amt_acc92,
            amt_acc93,
            amt_acc94,
            amt_acc95,
            amt_acc96,
            amt_acc99,
            amt_acc904,
            amt_acc333,
            amt_acc5006,
            amt_acc901,
            amt_acc902,
            amt_acc909,
            amt_acc920,
            amt_acc97,
            amt_acc318,
            amt_acc718,
            amt_acc818,
            amt_acc900,
            amt_acc903,
            amt_acc913,
            amt_acc914,
            amt_acc916,
            amt_acc918,
            amt_acc3501 ,
            amt_acc3502 ,
            amt_acc3504 ,
            amt_acc3500 ,
            amt_acc3503 ,
            amt_acc3516 ,
            amt_acc3518 ,
            text_comment,
            dtime_inserted,
            dtime_updated,
            id_source_his, --20210215
            flag_accrual, --20210223
            amt_acc328,
            amt_acc528,
            amt_acc338,
            amt_acc339,
            amt_acc211,
            amt_acc311,
            amt_acc175,
            AMT_ACC115_ROUNDED,
            AMT_ACC116_ROUNDED,
            AMT_ACC181_ROUNDED,
            AMT_ACC185_ROUNDED,
            AMT_ACC183_ROUNDED,
            AMT_ACC625_ROUNDED,
            AMT_ACC301_ROUNDED,
            AMT_ACC541_ROUNDED,
            AMT_ACC305_ROUNDED,
            AMT_ACC316_ROUNDED,
            AMT_ACC701_ROUNDED,
            AMT_ACC716_ROUNDED,
            AMT_ACC801_ROUNDED,
            AMT_ACC816_ROUNDED,
            AMT_ACC481_ROUNDED,
            AMT_ACC483_ROUNDED,
            AMT_ACC485_ROUNDED,
            AMT_ACC91_ROUNDED,
            AMT_ACC3501_ROUNDED,
            AMT_ACC901_ROUNDED,
            AMT_ACC916_ROUNDED,
            AMT_ACC3516_ROUNDED,
            AMT_ACC175_ROUNDED,
            AMT_ACC176_ROUNDED,
            AMT_ACC331_ROUNDED,
            AMT_ACC605_ROUNDED,
            AMT_ACC2282_ROUNDED,
            AMT_ACC282_ROUNDED,
            AMT_ACC702_ROUNDED,
            AMT_ACC802_ROUNDED,
            AMT_ACC302_ROUNDED,
            AMT_ACC306_ROUNDED,
            AMT_ACC542_ROUNDED,
            AMT_ACC92_ROUNDED,
            AMT_ACC3502_ROUNDED,
            AMT_ACC902_ROUNDED,
            AMT_ACC177_ROUNDED,
            AMT_ACC201_ROUNDED,
            AMT_ACC211_ROUNDED,
            AMT_ACC212_ROUNDED,
            AMT_ACC213_ROUNDED,
            AMT_ACC214_ROUNDED,
            AMT_ACC215_ROUNDED,
            AMT_ACC311_ROUNDED,
            AMT_ACC332_ROUNDED,
            AMT_ACC697_ROUNDED,
            AMT_ACC93_ROUNDED,
            AMT_ACC543_ROUNDED
        )
    with vw_sbv_account_data_hist as
        (
        SELECT
                    stm.code_source_system as code_source_system,
                    stm.id_source as id_source,
                    stm.date_effective as date_effective,
                    stm.skp_proc_inserted as skp_proc_inserted,
                    stm.skp_proc_updated as skp_proc_updated,
                    stm.flag_deleted as flag_deleted,
                    stm.code_credit_type as code_credit_type,
                    stm.skp_contract as skp_contract,
                    stm.id_contract as id_contract,
                    stm.id_client as id_client,
                    stm.text_contract_number as text_contract_number,
                    stm.amt_acc101 + nvl(c.amt_acc101,0) as amt_acc101,
                    stm.amt_acc115 + nvl(c.amt_acc115,0) as amt_acc115,
                    stm.amt_acc116 + nvl(c.amt_acc116,0) as amt_acc116,
--                    stm.amt_acc201 + nvl(c.amt_acc201,0) + nvl(dfi.amt_acc201,0) + nvl(dfi2528.amt_acc201,0) as amt_acc201,
                    stm.amt_acc201 + nvl(c.amt_acc201,0) + nvl(dfi.amt_acc201,0) as amt_acc201,
                    stm.amt_acc209 + nvl(c.amt_acc209,0) as amt_acc209,
--                    stm.amt_acc212 + nvl(c.amt_acc212,0) + nvl(dfi.amt_acc212,0) + nvl(dfi2528.amt_acc212,0) as amt_acc212,
                    stm.amt_acc212 + nvl(c.amt_acc212,0) + nvl(dfi.amt_acc212,0) as amt_acc212,
--                    stm.amt_acc213 + nvl(c.amt_acc213,0) + nvl(dfi.amt_acc213,0) + nvl(dfi2528.amt_acc213,0) as amt_acc213,
                    stm.amt_acc213 + nvl(c.amt_acc213,0) + nvl(dfi.amt_acc213,0) as amt_acc213,
                    stm.amt_acc214 + nvl(c.amt_acc214,0) + nvl(dfi.amt_acc214,0) as amt_acc214,
--                    stm.amt_acc215 + nvl(c.amt_acc215,0) + nvl(dfi.amt_acc215,0) + nvl(dfi2528.amt_acc215,0) as amt_acc215,
                    stm.amt_acc215 + nvl(c.amt_acc215,0) + nvl(dfi.amt_acc215,0) amt_acc215,
                    stm.amt_acc226 + nvl(c.amt_acc226,0) as amt_acc226,
                    stm.amt_acc277 + nvl(c.amt_acc277,0) as amt_acc277,
--                    stm.amt_acc301 + nvl(c.amt_acc301,0) as amt_acc301,
                    stm.amt_acc301 + nvl(c.amt_acc301,0) + nvl(dfp.amt_acc301,0) as amt_acc301,
                    stm.amt_acc302 + nvl(c.amt_acc302,0) + nvl(dfi.amt_acc302,0) as amt_acc302,
--                    stm.amt_acc302 + nvl(c.amt_acc302,0) as amt_acc302,
                    stm.amt_acc312 + nvl(c.amt_acc312,0) as amt_acc312,
                    stm.amt_acc326 + nvl(c.amt_acc326,0) as amt_acc326,
                    stm.amt_acc331 + nvl(c.amt_acc331,0) as amt_acc331,
                    stm.amt_acc377 + nvl(c.amt_acc377,0) as amt_acc377,
                    stm.amt_acc997 + nvl(c.amt_acc997,0) as amt_acc997,
                    stm.amt_acc176 + nvl(c.amt_acc176,0) as amt_acc176,
                    stm.amt_acc177 + nvl(c.amt_acc177,0) as amt_acc177,
                    stm.amt_acc178 + nvl(c.amt_acc178,0) as amt_acc178,
                    stm.date_valid_from as date_valid_from ,
                    stm.date_valid_to  as date_valid_to,
                    stm.amt_acc332 + nvl(c.amt_acc332,0) as amt_acc332,
                    stm.amt_acc605 + nvl(c.amt_acc605,0) as amt_acc605,
                    stm.amt_acc697 + nvl(c.amt_acc697,0) as amt_acc697,
                    stm.amt_acc541 + nvl(c.amt_acc541,0) as amt_acc541,
                    stm.amt_acc305 + nvl(c.amt_acc305,0) as amt_acc305,
                    stm.amt_acc542 + nvl(c.amt_acc542,0) as amt_acc542,
                    stm.amt_acc306 + nvl(c.amt_acc306,0) as amt_acc306,
--                    stm.amt_acc282 + nvl(c.amt_acc282,0) as amt_acc282,
                    stm.amt_acc282 + nvl(c.amt_acc282,0) + nvl(dfi.amt_acc282,0) as amt_acc282,
                    stm.amt_acc300 + nvl(c.amt_acc300,0) as amt_acc300,
                    stm.amt_acc319 + nvl(c.amt_acc319,0) as amt_acc319,
                    stm.amt_acc320 + nvl(c.amt_acc320,0) as amt_acc320,
                    stm.amt_acc349 + nvl(c.amt_acc349,0) as amt_acc349,
                    stm.amt_acc350 + nvl(c.amt_acc350,0) as amt_acc350,
                    stm.amt_acc357 + nvl(c.amt_acc357,0) as amt_acc357,
                    stm.amt_acc358 + nvl(c.amt_acc358,0) as amt_acc358,
                    stm.amt_acc359 + nvl(c.amt_acc359,0) as amt_acc359,
                    stm.amt_acc360 + nvl(c.amt_acc360,0) as amt_acc360,
                    stm.amt_acc372 + nvl(c.amt_acc372,0) as amt_acc372,
                    stm.amt_acc375 + nvl(c.amt_acc375,0) as amt_acc375,
                    stm.amt_acc376 + nvl(c.amt_acc376,0) as amt_acc376,
                    stm.amt_acc380 + nvl(c.amt_acc380,0) as amt_acc380,
                    stm.amt_acc383 + nvl(c.amt_acc383,0) as amt_acc383,
                    stm.amt_acc386 + nvl(c.amt_acc386,0) as amt_acc386,
                    stm.amt_acc388 + nvl(c.amt_acc388,0) as amt_acc388,
                    stm.amt_acc389 + nvl(c.amt_acc389,0) as amt_acc389,
                    stm.amt_acc390 + nvl(c.amt_acc390,0) as amt_acc390,
                    stm.amt_acc391 + nvl(c.amt_acc391,0) as amt_acc391,
                    stm.amt_acc392 + nvl(c.amt_acc392,0) as amt_acc392,
                    stm.amt_acc394 + nvl(c.amt_acc394,0) as amt_acc394,
                    stm.amt_acc395 + nvl(c.amt_acc395,0) as amt_acc395,
                    stm.amt_acc396 + nvl(c.amt_acc396,0) as amt_acc396,
                    stm.amt_acc397 + nvl(c.amt_acc397,0) as amt_acc397,
                    stm.amt_acc621 + nvl(c.amt_acc621,0) as amt_acc621,
                    stm.amt_acc625 + nvl(c.amt_acc625,0) as amt_acc625,
                    stm.amt_acc2282 + nvl(c.amt_acc2282,0) + nvl(dfi.amt_acc2282,0) as amt_acc2282,
                    stm.amt_acc543 + nvl(c.amt_acc543,0) as amt_acc543,
                    stm.amt_acc544 + nvl(c.amt_acc544,0) as amt_acc544,
                    stm.amt_acc547 + nvl(c.amt_acc547,0) as amt_acc547,
                    stm.amt_acc308 + nvl(c.amt_acc308,0) as amt_acc308,
                    stm.amt_acc335 + nvl(c.amt_acc335,0) as amt_acc335,
                    stm.amt_acc353 + nvl(c.amt_acc353,0) as amt_acc353,
                    stm.amt_acc272 + nvl(c.amt_acc272,0) as amt_acc272,
                    stm.amt_acc219 + nvl(c.amt_acc219,0) as amt_acc219,
                    stm.amt_acc275 + nvl(c.amt_acc275,0) as amt_acc275,
                    stm.amt_acc181 + nvl(c.amt_acc181,0) as amt_acc181,
                    stm.amt_acc185 + nvl(c.amt_acc185,0) as amt_acc185,
                    stm.amt_acc183 + nvl(c.amt_acc183,0) as amt_acc183,
                    stm.amt_acc316 + nvl(c.amt_acc316,0) as amt_acc316,
                    stm.amt_acc382 + nvl(c.amt_acc382,0) as amt_acc382,
                    stm.amt_acc373 + nvl(c.amt_acc373,0) as amt_acc373,
                    stm.amt_acc273 + nvl(c.amt_acc273,0) as amt_acc273,
                    stm.amt_acc998 + nvl(c.amt_acc998,0) as amt_acc998,
                    stm.amt_acc303 + nvl(c.amt_acc303,0) as amt_acc303,
                    stm.amt_acc179 + nvl(c.amt_acc179,0) as amt_acc179,
                    stm.amt_acc546 + nvl(c.amt_acc546,0) as amt_acc546,
                    stm.amt_acc548 + nvl(c.amt_acc548,0) as amt_acc548,
                    stm.amt_acc500 + nvl(c.amt_acc500,0) as amt_acc500,
                    stm.amt_acc323 + nvl(c.amt_acc323,0) as amt_acc323,
                    stm.amt_acc549 + nvl(c.amt_acc549,0) as amt_acc549,
                    stm.amt_acc545 + nvl(c.amt_acc545,0) as amt_acc545,
                    stm.code_transaction_subtype as code_transaction_subtype,
                    stm.amt_acc5002 + nvl(c.amt_acc5002,0) as amt_acc5002,
                    stm.amt_acc5003 + nvl(c.amt_acc5003,0) as amt_acc5003,
                    stm.amt_acc800 + nvl(c.amt_acc800,0) as amt_acc800,
--                    stm.amt_acc801 + nvl(c.amt_acc801,0) as amt_acc801,
                    stm.amt_acc801 + nvl(c.amt_acc801,0) + nvl(dfp.amt_acc801,0) as amt_acc801,
--                    stm.amt_acc802 + nvl(c.amt_acc802,0) + nvl(dfi2528.amt_acc802,0) as amt_acc802,
                    stm.amt_acc802 + nvl(c.amt_acc802,0) as amt_acc802,
                    stm.amt_acc816 + nvl(c.amt_acc816,0) as amt_acc816,
--                    stm.amt_acc701 + nvl(c.amt_acc701,0) as amt_acc701,
                    stm.amt_acc701 + nvl(c.amt_acc701,0) + nvl(dfp.amt_acc701,0) as amt_acc701,
                    stm.amt_acc716 + nvl(c.amt_acc716,0) as amt_acc716,
                    stm.amt_acc702 + nvl(c.amt_acc702,0) + nvl(dfi.amt_acc702,0) as amt_acc702,
--                    stm.amt_acc702 + nvl(c.amt_acc702,0) as amt_acc702,
                    stm.amt_acc398 + nvl(c.amt_acc398,0) as amt_acc398,
                    stm.amt_acc307 + nvl(c.amt_acc307,0) as amt_acc307,
                    stm.amt_acc481 + nvl(c.amt_acc481,0) as amt_acc481,
                    stm.amt_acc483 + nvl(c.amt_acc483,0) as amt_acc483,
                    stm.amt_acc485 + nvl(c.amt_acc485,0) as amt_acc485,
                    stm.amt_acc496 + nvl(c.amt_acc496,0) as amt_acc496,
                    stm.amt_acc3310 + nvl(c.amt_acc3310,0) as amt_acc3310,
                    stm.amt_acc540 + nvl(c.amt_acc540,0) as amt_acc540,
                    stm.amt_acc703 + nvl(c.amt_acc703,0) as amt_acc703,
                    stm.amt_acc704 + nvl(c.amt_acc704,0) as amt_acc704,
                    stm.amt_acc803 + nvl(c.amt_acc803,0) as amt_acc803,
                    stm.amt_acc804 + nvl(c.amt_acc804,0) as amt_acc804,
                    stm.amt_acc3300 + nvl(c.amt_acc3300,0) as amt_acc3300,
                    stm.amt_acc078 + nvl(c.amt_acc078,0) as amt_acc078,
                    stm.amt_acc121 + nvl(c.amt_acc121,0) as amt_acc121,
                    stm.amt_acc123 + nvl(c.amt_acc123,0) as amt_acc123,
                    stm.amt_acc125 + nvl(c.amt_acc125,0) as amt_acc125,
                    stm.amt_acc91 + nvl(c.amt_acc91,0) as amt_acc91,
                    stm.amt_acc92 + nvl(c.amt_acc92,0) as amt_acc92,
                    stm.amt_acc93 + nvl(c.amt_acc93,0) as amt_acc93,
                    stm.amt_acc94 + nvl(c.amt_acc94,0) as amt_acc94,
                    stm.amt_acc95 + nvl(c.amt_acc95,0) as amt_acc95,
                    stm.amt_acc96 + nvl(c.amt_acc96,0) as amt_acc96,
                    stm.amt_acc99 + nvl(c.amt_acc99,0) as amt_acc99,
                    stm.amt_acc904 + nvl(c.amt_acc904,0) as amt_acc904,
                    stm.amt_acc333 + nvl(c.amt_acc333,0) as amt_acc333,
                    stm.amt_acc5006 + nvl(c.amt_acc5006,0) as amt_acc5006,
                    stm.amt_acc901 + nvl(c.amt_acc901,0) as amt_acc901,
                    stm.amt_acc902 + nvl(c.amt_acc902,0) as amt_acc902,
                    stm.amt_acc909 + nvl(c.amt_acc909,0) as amt_acc909,
                    stm.amt_acc920 + nvl(c.amt_acc920,0) as amt_acc920,
                    stm.amt_acc97 + nvl(c.amt_acc97,0) as amt_acc97,
                    stm.amt_acc318 + nvl(c.amt_acc318,0) as amt_acc318,
                    stm.amt_acc718 + nvl(c.amt_acc718,0) as amt_acc718,
                    stm.amt_acc818 + nvl(c.amt_acc818,0) as amt_acc818,
                    stm.amt_acc900 + nvl(c.amt_acc900,0) as amt_acc900,
                    stm.amt_acc903 + nvl(c.amt_acc903,0) as amt_acc903,
                    stm.amt_acc913 + nvl(c.amt_acc913,0) as amt_acc913,
                    stm.amt_acc914 + nvl(c.amt_acc914,0) as amt_acc914,
                    stm.amt_acc916 + nvl(c.amt_acc916,0) as amt_acc916,
                    stm.amt_acc918 + nvl(c.amt_acc918,0) as amt_acc918,
                    stm.amt_acc3501 + nvl(c.amt_acc3501,0) as amt_acc3501,
                    stm.amt_acc3502 + nvl(c.amt_acc3502,0) as amt_acc3502,
                    stm.amt_acc3504 + nvl(c.amt_acc3504,0) as amt_acc3504,
                    stm.amt_acc3500 + nvl(c.amt_acc3500,0) as amt_acc3500,
                    stm.amt_acc3503 + nvl(c.amt_acc3503,0) as amt_acc3503,
                    stm.amt_acc3516 + nvl(c.amt_acc3516,0) as amt_acc3516,
                    stm.amt_acc3518 + nvl(c.amt_acc3518,0) as amt_acc3518,
                    stm.text_comment as text_comment,
                    sysdate as dtime_inserted,
                    sysdate as dtime_updated,
                    c.id_source as id_source_his, --20210215
                    flag_accrual, --20210223
                    stm.amt_acc328 as amt_acc328,
                    stm.amt_acc528 as amt_acc528,
                    stm.amt_acc338 as amt_acc338,
                    stm.amt_acc339 as amt_acc339,
--                    stm.amt_acc211 + nvl(dfi.amt_acc211, 0) as amt_acc211,
                    stm.amt_acc211 + nvl(c.amt_acc211,0) as amt_acc211, 
--                    stm.amt_acc211 as amt_acc211,
                    stm.amt_acc311 as amt_acc311,
                    stm.amt_acc175 as amt_acc175,                    
                    stm.amt_acc115_rounded + round(nvl(c.amt_acc115,0)) as amt_acc115_rounded,
                    stm.amt_acc116_rounded + round(nvl(c.amt_acc116,0)) as amt_acc116_rounded,
                    stm.amt_acc181_rounded + round(nvl(c.amt_acc181,0)) as amt_acc181_rounded,
                    stm.amt_acc185_rounded + round(nvl(c.amt_acc185,0)) as amt_acc185_rounded,                    
                    stm.amt_acc183_rounded + round(nvl(c.amt_acc183,0)) as amt_acc183_rounded,
                    stm.amt_acc625_rounded + round(nvl(c.amt_acc625,0)) as amt_acc625_rounded,
                    stm.amt_acc301_rounded + round(nvl(c.amt_acc301,0)) + round(nvl(dfp.amt_acc301,0)) as amt_acc301_rounded,
                    stm.amt_acc541_rounded + round(nvl(c.amt_acc541,0)) as amt_acc541_rounded,
                    stm.amt_acc305_rounded + round(nvl(c.amt_acc305,0)) as amt_acc305_rounded,
                    stm.amt_acc316_rounded + round(nvl(c.amt_acc316,0)) as amt_acc316_rounded,
                    stm.amt_acc801_rounded + round(nvl(c.amt_acc801,0)) + round(nvl(dfp.amt_acc801,0)) as amt_acc801_rounded,
                    stm.amt_acc816_rounded + round(nvl(c.amt_acc816,0)) as amt_acc816_rounded,
                    stm.amt_acc701_rounded + round(nvl(c.amt_acc701,0)) + round(nvl(dfp.amt_acc701,0)) as amt_acc701_rounded,
                    stm.amt_acc716_rounded + round(nvl(c.amt_acc716,0)) as amt_acc716_rounded,
                    stm.amt_acc481_rounded + round(nvl(c.amt_acc481,0)) as amt_acc481_rounded,
                    stm.amt_acc483_rounded + round(nvl(c.amt_acc483,0)) as amt_acc483_rounded,
                    stm.amt_acc485_rounded + round(nvl(c.amt_acc485,0)) as amt_acc485_rounded,
                    stm.amt_acc3501_rounded + round(nvl(c.amt_acc3501,0)) as amt_acc3501_rounded,
                    stm.amt_acc91_rounded + round(nvl(c.amt_acc91,0)) as amt_acc91_rounded,
                    stm.amt_acc901_rounded + round(nvl(c.amt_acc901,0)) as amt_acc901_rounded,
                    stm.amt_acc916_rounded + round(nvl(c.amt_acc916,0)) as amt_acc916_rounded,
                    stm.amt_acc3516_rounded + round(nvl(c.amt_acc3516,0)) as amt_acc3516_rounded,
                    stm.amt_acc175_rounded as amt_acc175_rounded,
                    stm.amt_acc176_rounded + round(nvl(c.amt_acc176,0)) as amt_acc176_rounded,
                    stm.amt_acc331_rounded + round(nvl(c.amt_acc331,0)) as amt_acc331_rounded,
                    stm.amt_acc605_rounded + round(nvl(c.amt_acc605,0)) as amt_acc605_rounded,
                    stm.amt_acc2282_rounded + round(nvl(c.amt_acc2282,0)) + round(nvl(dfi.amt_acc2282,0)) as amt_acc2282_rounded,
                    stm.amt_acc282_rounded + round(nvl(c.amt_acc282,0)) + round(nvl(dfi.amt_acc282,0)) as amt_acc282_rounded,
                    stm.amt_acc702_rounded + round(nvl(c.amt_acc702,0)) + round(nvl(dfi.amt_acc702,0)) as amt_acc702_rounded,
                    stm.amt_acc802_rounded + round(nvl(c.amt_acc802,0)) as amt_acc802_rounded,
                    stm.amt_acc302_rounded + round(nvl(c.amt_acc302,0)) + round(nvl(dfi.amt_acc302,0)) as amt_acc302_rounded,
                    stm.amt_acc306_rounded + round(nvl(c.amt_acc306,0)) as amt_acc306_rounded,
                    stm.amt_acc92_rounded + round(nvl(c.amt_acc92,0)) as amt_acc92_rounded,
                    stm.amt_acc542_rounded + round(nvl(c.amt_acc542,0)) as amt_acc542_rounded,
                    stm.amt_acc3502_rounded + round(nvl(c.amt_acc3502,0)) as amt_acc3502_rounded,
                    stm.amt_acc902_rounded + round(nvl(c.amt_acc902,0)) as amt_acc902_rounded,
                    stm.amt_acc177_rounded + round(nvl(c.amt_acc177,0)) as amt_acc177_rounded,
                    stm.amt_acc201_rounded + round(nvl(c.amt_acc201,0)) + round(nvl(dfi.amt_acc201,0)) as amt_acc201_rounded,
                    stm.amt_acc211_rounded + round(nvl(c.amt_acc211,0)) as amt_acc211_rounded, 
                    stm.amt_acc212_rounded + round(nvl(c.amt_acc212,0)) + round(nvl(dfi.amt_acc212,0)) as amt_acc212_rounded,
                    stm.amt_acc213_rounded + round(nvl(c.amt_acc213,0)) + round(nvl(dfi.amt_acc213,0)) as amt_acc213_rounded,
                    stm.amt_acc214_rounded + round(nvl(c.amt_acc214,0)) + round(nvl(dfi.amt_acc214,0)) as amt_acc214_rounded,
                    stm.amt_acc215_rounded + round(nvl(c.amt_acc215,0)) + round(nvl(dfi.amt_acc215,0)) as amt_acc215_rounded,
                    stm.amt_acc311_rounded as amt_acc311_rounded,
                    stm.amt_acc332_rounded + round(nvl(c.amt_acc332,0)) as amt_acc332_rounded,
                    stm.amt_acc93_rounded + round(nvl(c.amt_acc93,0)) as amt_acc93_rounded,
                    stm.amt_acc697_rounded + round(nvl(c.amt_acc697,0)) as amt_acc697_rounded,
                    stm.amt_acc543_rounded + round(nvl(c.amt_acc543,0)) as amt_acc543_rounded
                FROM LDM_SBV.stm_sbv_account_data_td stm
                    LEFT JOIN LDM_SBV.ft_sbv_account_data_hist c ON stm.skp_contract = c.skp_contract
--                                                              and stm.code_credit_type = c.code_credit_type
                                                              AND stm.code_transaction_subtype = c.code_transaction_subtype
                    --20210430
                    left join (select skp_contract
                                    , code_transaction_subtype
                                    , sum(AMT_ACC201) AMT_ACC201
                                    , sum(AMT_ACC212) AMT_ACC212
                                    , sum(AMT_ACC213) AMT_ACC213
                                    , sum(AMT_ACC214) AMT_ACC214
                                    , sum(AMT_ACC215) AMT_ACC215
                                    , sum(AMT_ACC282) AMT_ACC282
                                    , sum(AMT_ACC702) AMT_ACC702
                                    , sum(AMT_ACC302) AMT_ACC302
                                    , sum(AMT_ACC2282) AMT_ACC2282
--                                    , sum(AMT_ACC211) AMT_ACC211
                                from LDM_SBV.df_interest
                                where date_effective <= p_effective_date
                                group by skp_contract
                                    , code_transaction_subtype) dfi on stm.skp_contract = dfi.skp_contract
                                                                    AND stm.code_transaction_subtype = dfi.code_transaction_subtype
                    left join LDM_SBV.df_principal dfp on stm.skp_contract = dfp.skp_contract
                                                    AND stm.code_transaction_subtype = dfp.code_transaction_subtype
--                --20210709
--                    left join LDM_SBV.df_interest2528 dfi2528 on stm.skp_contract = dfi2528.skp_contract
--                                                    AND stm.code_transaction_subtype = dfi2528.code_transaction_subtype
               )
      SELECT
            LDM_SBV.S_FT_SBV_ACCOUNT_DATA_TD.nextval,
            i.code_source_system,
            i.id_source,
            i.date_effective,
            i.skp_proc_inserted,
            i.skp_proc_updated,
            i.flag_deleted,
            i.code_credit_type,
            i.skp_contract,
            i.id_contract,
            i.id_client,
            i.text_contract_number,
            i.amt_acc101,
            i.amt_acc115,
            i.amt_acc116,
            i.amt_acc201,
            i.amt_acc209,
            i.amt_acc212,
            i.amt_acc213,
            i.amt_acc214,
            i.amt_acc215,
            i.amt_acc226,
            i.amt_acc277,
            i.amt_acc301,
            i.amt_acc302,
            i.amt_acc312,
            i.amt_acc326,
            i.amt_acc331,
            i.amt_acc377,
            i.amt_acc997,
            i.amt_acc176,
            i.amt_acc177,
            i.amt_acc178,
            i.date_valid_from ,
            i.date_valid_to ,
            CASE WHEN c.id_source IS NULL THEN 'I' ELSE 'U' END code_change_type ,
            CASE WHEN c.id_source IS NOT NULL THEN c.id_source END id_source_previous ,
            i.amt_acc332,
            i.amt_acc605,
            i.amt_acc697,
            i.amt_acc541,
            i.amt_acc305,
            i.amt_acc542,
            i.amt_acc306,
            i.amt_acc282,
            i.amt_acc300,
            i.amt_acc319,
            i.amt_acc320,
            i.amt_acc349,
            i.amt_acc350,
            i.amt_acc357,
            i.amt_acc358,
            i.amt_acc359,
            i.amt_acc360,
            i.amt_acc372,
            i.amt_acc375,
            i.amt_acc376,
            i.amt_acc380,
            i.amt_acc383,
            i.amt_acc386,
            i.amt_acc388,
            i.amt_acc389,
            i.amt_acc390,
            i.amt_acc391,
            i.amt_acc392,
            i.amt_acc394,
            i.amt_acc395,
            i.amt_acc396,
            i.amt_acc397,
            i.amt_acc621,
            i.amt_acc625,
            i.amt_acc2282,
            i.amt_acc543,
            i.amt_acc544,
            i.amt_acc547,
            i.amt_acc308,
            i.amt_acc335,
            i.amt_acc353,
            i.amt_acc272,
            i.amt_acc219,
            i.amt_acc275,
            i.amt_acc181,
            i.amt_acc185,
            i.amt_acc183,
            i.amt_acc316,
            i.amt_acc382,
            i.amt_acc373,
            i.amt_acc273,
            i.amt_acc998,
            i.amt_acc303,
            i.amt_acc179,
            i.amt_acc546,
            i.amt_acc548,
            i.amt_acc500,
            i.amt_acc323,
            i.amt_acc549,
            i.amt_acc545,
            i.code_transaction_subtype ,
            i.amt_acc5002,
            i.amt_acc5003,
            i.amt_acc800,
            i.amt_acc801,
            i.amt_acc802,
            i.amt_acc816,
            i.amt_acc701,
            i.amt_acc716,
            i.amt_acc702,
            i.amt_acc398,
            i.amt_acc307,
            i.amt_acc481,
            i.amt_acc483,
            i.amt_acc485,
            i.amt_acc496,
            i.amt_acc3310,
            i.amt_acc540,
            i.amt_acc703,
            i.amt_acc704,
            i.amt_acc803,
            i.amt_acc804,
            i.amt_acc3300,
            i.amt_acc078,
            i.amt_acc121,
            i.amt_acc123,
            i.amt_acc125,
            i.amt_acc91,
            i.amt_acc92,
            i.amt_acc93,
            i.amt_acc94,
            i.amt_acc95,
            i.amt_acc96,
            i.amt_acc99,
            i.amt_acc904,
            i.amt_acc333,
            i.amt_acc5006,
            i.amt_acc901,
            i.amt_acc902,
            i.amt_acc909,
            i.amt_acc920,
            i.amt_acc97,
            i.amt_acc318,
            i.amt_acc718,
            i.amt_acc818,
            i.amt_acc900,
            i.amt_acc903,
            i.amt_acc913,
            i.amt_acc914,
            i.amt_acc916,
            i.amt_acc918,
            i.amt_acc3501 ,
            i.amt_acc3502 ,
            i.amt_acc3504 ,
            i.amt_acc3500 ,
            i.amt_acc3503 ,
            i.amt_acc3516 ,
            i.amt_acc3518 ,
            i.text_comment,
            i.dtime_inserted,
            i.dtime_updated,
            i.id_source_his, --20210215
            flag_accrual, --20210223
            i.amt_acc328 ,
            i.amt_acc528,
            i.amt_acc338,
            i.amt_acc339,
            i.amt_acc211,
            i.amt_acc311,
            i.amt_acc175,
            i.AMT_ACC115_ROUNDED,
            i.AMT_ACC116_ROUNDED,
            i.AMT_ACC181_ROUNDED,
            i.AMT_ACC185_ROUNDED,
            i.AMT_ACC183_ROUNDED,
            i.AMT_ACC625_ROUNDED,
            i.AMT_ACC301_ROUNDED,
            i.AMT_ACC541_ROUNDED,
            i.AMT_ACC305_ROUNDED,
            i.AMT_ACC316_ROUNDED,
            i.AMT_ACC701_ROUNDED,
            i.AMT_ACC716_ROUNDED,
            i.AMT_ACC801_ROUNDED,
            i.AMT_ACC816_ROUNDED,
            i.AMT_ACC481_ROUNDED,
            i.AMT_ACC483_ROUNDED,
            i.AMT_ACC485_ROUNDED,
            i.AMT_ACC91_ROUNDED,
            i.AMT_ACC3501_ROUNDED,
            i.AMT_ACC901_ROUNDED,
            i.AMT_ACC916_ROUNDED,
            i.AMT_ACC3516_ROUNDED,
            i.AMT_ACC175_ROUNDED,
            i.AMT_ACC176_ROUNDED,
            i.AMT_ACC331_ROUNDED,
            i.AMT_ACC605_ROUNDED,
            i.AMT_ACC2282_ROUNDED,
            i.AMT_ACC282_ROUNDED,
            i.AMT_ACC702_ROUNDED,
            i.AMT_ACC802_ROUNDED,
            i.AMT_ACC302_ROUNDED,
            i.AMT_ACC306_ROUNDED,
            i.AMT_ACC542_ROUNDED,
            i.AMT_ACC92_ROUNDED,
            i.AMT_ACC3502_ROUNDED,
            i.AMT_ACC902_ROUNDED,
            i.AMT_ACC177_ROUNDED,
            i.AMT_ACC201_ROUNDED,
            i.AMT_ACC211_ROUNDED,
            i.AMT_ACC212_ROUNDED,
            i.AMT_ACC213_ROUNDED,
            i.AMT_ACC214_ROUNDED,
            i.AMT_ACC215_ROUNDED,
            i.AMT_ACC311_ROUNDED,
            i.AMT_ACC332_ROUNDED,
            i.AMT_ACC697_ROUNDED,
            i.AMT_ACC93_ROUNDED,
            i.AMT_ACC543_ROUNDED
      FROM vw_sbv_account_data_hist i
        LEFT JOIN LDM_SBV.ft_sbv_account_data_td c ON i.skp_contract = c.skp_contract
                                                  and i.code_credit_type = c.code_credit_type
                                                  AND i.code_transaction_subtype =  c.code_transaction_subtype
                                                  AND c.date_valid_to = d_def_value_date_future
      WHERE c.id_source IS NULL
            OR (
            NVL(i.flag_deleted,v_flag_X)                       <> NVL(c.flag_deleted,v_flag_X)
--            OR NVL(i.code_credit_type,v_xna)                  <> NVL(c.code_credit_type,v_xna)
--            OR NVL(i.skp_contract, n_minus_one)             <> NVL(c.skp_contract, n_minus_one)
--            OR NVL(i.id_contract, n_minus_one)              <> NVL(c.id_contract, n_minus_one)
--            OR NVL(i.id_client, n_minus_one)                <> NVL(c.id_client, n_minus_one)
--            OR NVL(i.text_contract_number,v_xna)              <> NVL(c.text_contract_number,v_xna)
            OR NVL(i.amt_acc101, n_minus_one)               <> NVL(c.amt_acc101, n_minus_one)
            OR NVL(i.amt_acc115, n_minus_one)               <> NVL(c.amt_acc115, n_minus_one)
            OR NVL(i.amt_acc116, n_minus_one)               <> NVL(c.amt_acc116, n_minus_one)
            OR NVL(i.amt_acc201, n_minus_one)               <> NVL(c.amt_acc201, n_minus_one)
            OR NVL(i.amt_acc209, n_minus_one)               <> NVL(c.amt_acc209, n_minus_one)
            OR NVL(i.amt_acc212, n_minus_one)               <> NVL(c.amt_acc212, n_minus_one)
            OR NVL(i.amt_acc213, n_minus_one)               <> NVL(c.amt_acc213, n_minus_one)
            OR NVL(i.amt_acc214, n_minus_one)               <> NVL(c.amt_acc214, n_minus_one)
            OR NVL(i.amt_acc215, n_minus_one)               <> NVL(c.amt_acc215, n_minus_one)
            OR NVL(i.amt_acc226, n_minus_one)               <> NVL(c.amt_acc226, n_minus_one)
            OR NVL(i.amt_acc277, n_minus_one)               <> NVL(c.amt_acc277, n_minus_one)
            OR NVL(i.amt_acc301, n_minus_one)               <> NVL(c.amt_acc301, n_minus_one)
            OR NVL(i.amt_acc302, n_minus_one)               <> NVL(c.amt_acc302, n_minus_one)
            OR NVL(i.amt_acc312, n_minus_one)               <> NVL(c.amt_acc312, n_minus_one)
            OR NVL(i.amt_acc326, n_minus_one)               <> NVL(c.amt_acc326, n_minus_one)
            OR NVL(i.amt_acc331, n_minus_one)               <> NVL(c.amt_acc331, n_minus_one)
            OR NVL(i.amt_acc377, n_minus_one)               <> NVL(c.amt_acc377, n_minus_one)
            OR NVL(i.amt_acc997, n_minus_one)               <> NVL(c.amt_acc997, n_minus_one)
            OR NVL(i.amt_acc176, n_minus_one)               <> NVL(c.amt_acc176, n_minus_one)
            OR NVL(i.amt_acc177, n_minus_one)               <> NVL(c.amt_acc177, n_minus_one)
            OR NVL(i.amt_acc178, n_minus_one)               <> NVL(c.amt_acc178, n_minus_one)
            OR NVL(i.amt_acc332, n_minus_one)               <> NVL(c.amt_acc332, n_minus_one)
            OR NVL(i.amt_acc605, n_minus_one)               <> NVL(c.amt_acc605, n_minus_one)
            OR NVL(i.amt_acc697, n_minus_one)               <> NVL(c.amt_acc697, n_minus_one)
            OR NVL(i.amt_acc541, n_minus_one)               <> NVL(c.amt_acc541, n_minus_one)
            OR NVL(i.amt_acc305, n_minus_one)               <> NVL(c.amt_acc305, n_minus_one)
            OR NVL(i.amt_acc542, n_minus_one)               <> NVL(c.amt_acc542, n_minus_one)
            OR NVL(i.amt_acc306, n_minus_one)               <> NVL(c.amt_acc306, n_minus_one)
            OR NVL(i.amt_acc282, n_minus_one)               <> NVL(c.amt_acc282, n_minus_one)
            OR NVL(i.amt_acc300, n_minus_one)               <> NVL(c.amt_acc300, n_minus_one)
            OR NVL(i.amt_acc319, n_minus_one)               <> NVL(c.amt_acc319, n_minus_one)
            OR NVL(i.amt_acc320, n_minus_one)               <> NVL(c.amt_acc320, n_minus_one)
            OR NVL(i.amt_acc349, n_minus_one)               <> NVL(c.amt_acc349, n_minus_one)
            OR NVL(i.amt_acc350, n_minus_one)               <> NVL(c.amt_acc350, n_minus_one)
            OR NVL(i.amt_acc357, n_minus_one)               <> NVL(c.amt_acc357, n_minus_one)
            OR NVL(i.amt_acc358, n_minus_one)               <> NVL(c.amt_acc358, n_minus_one)
            OR NVL(i.amt_acc359, n_minus_one)               <> NVL(c.amt_acc359, n_minus_one)
            OR NVL(i.amt_acc360, n_minus_one)               <> NVL(c.amt_acc360, n_minus_one)
            OR NVL(i.amt_acc372, n_minus_one)               <> NVL(c.amt_acc372, n_minus_one)
            OR NVL(i.amt_acc375, n_minus_one)               <> NVL(c.amt_acc375, n_minus_one)
            OR NVL(i.amt_acc376, n_minus_one)               <> NVL(c.amt_acc376, n_minus_one)
            OR NVL(i.amt_acc380, n_minus_one)               <> NVL(c.amt_acc380, n_minus_one)
            OR NVL(i.amt_acc383, n_minus_one)               <> NVL(c.amt_acc383, n_minus_one)
            OR NVL(i.amt_acc386, n_minus_one)               <> NVL(c.amt_acc386, n_minus_one)
            OR NVL(i.amt_acc388, n_minus_one)               <> NVL(c.amt_acc388, n_minus_one)
            OR NVL(i.amt_acc389, n_minus_one)               <> NVL(c.amt_acc389, n_minus_one)
            OR NVL(i.amt_acc390, n_minus_one)               <> NVL(c.amt_acc390, n_minus_one)
            OR NVL(i.amt_acc391, n_minus_one)               <> NVL(c.amt_acc391, n_minus_one)
            OR NVL(i.amt_acc392, n_minus_one)               <> NVL(c.amt_acc392, n_minus_one)
            OR NVL(i.amt_acc394, n_minus_one)               <> NVL(c.amt_acc394, n_minus_one)
            OR NVL(i.amt_acc395, n_minus_one)               <> NVL(c.amt_acc395, n_minus_one)
            OR NVL(i.amt_acc396, n_minus_one)               <> NVL(c.amt_acc396, n_minus_one)
            OR NVL(i.amt_acc397, n_minus_one)               <> NVL(c.amt_acc397, n_minus_one)
            OR NVL(i.amt_acc621, n_minus_one)               <> NVL(c.amt_acc621, n_minus_one)
            OR NVL(i.amt_acc625, n_minus_one)               <> NVL(c.amt_acc625, n_minus_one)
            OR NVL(i.amt_acc2282, n_minus_one)              <> NVL(c.amt_acc2282, n_minus_one)
            OR NVL(i.amt_acc543, n_minus_one)               <> NVL(c.amt_acc543, n_minus_one)
            OR NVL(i.amt_acc544, n_minus_one)               <> NVL(c.amt_acc544, n_minus_one)
            OR NVL(i.amt_acc547, n_minus_one)               <> NVL(c.amt_acc547, n_minus_one)
            OR NVL(i.amt_acc308, n_minus_one)               <> NVL(c.amt_acc308, n_minus_one)
            OR NVL(i.amt_acc335, n_minus_one)               <> NVL(c.amt_acc335, n_minus_one)
            OR NVL(i.amt_acc353, n_minus_one)               <> NVL(c.amt_acc353, n_minus_one)
            OR NVL(i.amt_acc272, n_minus_one)               <> NVL(c.amt_acc272, n_minus_one)
            OR NVL(i.amt_acc219, n_minus_one)               <> NVL(c.amt_acc219, n_minus_one)
            OR NVL(i.amt_acc275, n_minus_one)               <> NVL(c.amt_acc275, n_minus_one)
            OR NVL(i.amt_acc181, n_minus_one)               <> NVL(c.amt_acc181, n_minus_one)
            OR NVL(i.amt_acc185, n_minus_one)               <> NVL(c.amt_acc185, n_minus_one)
            OR NVL(i.amt_acc183, n_minus_one)               <> NVL(c.amt_acc183, n_minus_one)
            OR NVL(i.amt_acc316, n_minus_one)               <> NVL(c.amt_acc316, n_minus_one)
            OR NVL(i.amt_acc382, n_minus_one)               <> NVL(c.amt_acc382, n_minus_one)
            OR NVL(i.amt_acc373, n_minus_one)               <> NVL(c.amt_acc373, n_minus_one)
            OR NVL(i.amt_acc273, n_minus_one)               <> NVL(c.amt_acc273, n_minus_one)
            OR NVL(i.amt_acc998, n_minus_one)               <> NVL(c.amt_acc998, n_minus_one)
            OR NVL(i.amt_acc303, n_minus_one)               <> NVL(c.amt_acc303, n_minus_one)
            OR NVL(i.amt_acc179, n_minus_one)               <> NVL(c.amt_acc179, n_minus_one)
            OR NVL(i.amt_acc546, n_minus_one)               <> NVL(c.amt_acc546, n_minus_one)
            OR NVL(i.amt_acc548, n_minus_one)               <> NVL(c.amt_acc548, n_minus_one)
            OR NVL(i.amt_acc500, n_minus_one)               <> NVL(c.amt_acc500, n_minus_one)
            OR NVL(i.amt_acc323, n_minus_one)               <> NVL(c.amt_acc323, n_minus_one)
            OR NVL(i.amt_acc549, n_minus_one)               <> NVL(c.amt_acc549, n_minus_one)
            OR NVL(i.amt_acc545, n_minus_one)               <> NVL(c.amt_acc545, n_minus_one)
--            OR NVL(i.code_transaction_subtype, v_xna) <> NVL(c.code_transaction_subtype, v_xna)
            OR NVL(i.amt_acc5002, n_minus_one)              <> NVL(c.amt_acc5002, n_minus_one)
            OR NVL(i.amt_acc5003, n_minus_one)              <> NVL(c.amt_acc5003, n_minus_one)
            OR NVL(i.amt_acc800, n_minus_one)               <> NVL(c.amt_acc800, n_minus_one)
            OR NVL(i.amt_acc801, n_minus_one)               <> NVL(c.amt_acc801, n_minus_one)
            OR NVL(i.amt_acc802, n_minus_one)               <> NVL(c.amt_acc802, n_minus_one)
            OR NVL(i.amt_acc816, n_minus_one)               <> NVL(c.amt_acc816, n_minus_one)
            OR NVL(i.amt_acc701, n_minus_one)               <> NVL(c.amt_acc701, n_minus_one)
            OR NVL(i.amt_acc716, n_minus_one)               <> NVL(c.amt_acc716, n_minus_one)
            OR NVL(i.amt_acc702, n_minus_one)               <> NVL(c.amt_acc702, n_minus_one)
            OR NVL(i.amt_acc398, n_minus_one)               <> NVL(c.amt_acc398, n_minus_one)
            OR NVL(i.amt_acc307, n_minus_one)               <> NVL(c.amt_acc307, n_minus_one)
            OR NVL(i.amt_acc481, n_minus_one)               <> NVL(c.amt_acc481, n_minus_one)
            OR NVL(i.amt_acc483, n_minus_one)               <> NVL(c.amt_acc483, n_minus_one)
            OR NVL(i.amt_acc485, n_minus_one)               <> NVL(c.amt_acc485, n_minus_one)
            OR NVL(i.amt_acc496, n_minus_one)               <> NVL(c.amt_acc496, n_minus_one)
            OR NVL(i.amt_acc3310, n_minus_one)              <> NVL(c.amt_acc3310, n_minus_one)
            OR NVL(i.amt_acc540, n_minus_one)               <> NVL(c.amt_acc540, n_minus_one)
            OR NVL(i.amt_acc703, n_minus_one)               <> NVL(c.amt_acc703, n_minus_one)
            OR NVL(i.amt_acc704, n_minus_one)               <> NVL(c.amt_acc704, n_minus_one)
            OR NVL(i.amt_acc803, n_minus_one)               <> NVL(c.amt_acc803, n_minus_one)
            OR NVL(i.amt_acc804, n_minus_one)               <> NVL(c.amt_acc804, n_minus_one)
            OR NVL(i.amt_acc3300, n_minus_one)              <> NVL(c.amt_acc3300, n_minus_one)
            OR NVL(i.amt_acc078, n_minus_one)               <> NVL(c.amt_acc078, n_minus_one)
            OR NVL(i.amt_acc121, n_minus_one)               <> NVL(c.amt_acc121, n_minus_one)
            OR NVL(i.amt_acc123, n_minus_one)               <> NVL(c.amt_acc123, n_minus_one)
            OR NVL(i.amt_acc125, n_minus_one)               <> NVL(c.amt_acc125, n_minus_one)
            OR NVL(i.amt_acc91, n_minus_one)               <> NVL(c.amt_acc91, n_minus_one)
            OR NVL(i.amt_acc92, n_minus_one)               <> NVL(c.amt_acc92, n_minus_one)
            OR NVL(i.amt_acc93, n_minus_one)               <> NVL(c.amt_acc93, n_minus_one)
            OR NVL(i.amt_acc94, n_minus_one)               <> NVL(c.amt_acc94, n_minus_one)
            OR NVL(i.amt_acc95, n_minus_one)               <> NVL(c.amt_acc95, n_minus_one)
            OR NVL(i.amt_acc96, n_minus_one)               <> NVL(c.amt_acc96, n_minus_one)
            OR NVL(i.amt_acc99, n_minus_one)               <> NVL(c.amt_acc99, n_minus_one)
            OR NVL(i.amt_acc904, n_minus_one)               <> NVL(c.amt_acc904, n_minus_one)
            OR NVL(i.amt_acc333, n_minus_one)               <> NVL(c.amt_acc333, n_minus_one)
            OR NVL(i.amt_acc5006, n_minus_one)               <> NVL(c.amt_acc5006, n_minus_one)
            OR NVL(i.amt_acc901, n_minus_one)               <> NVL(c.amt_acc901, n_minus_one)
            OR NVL(i.amt_acc902, n_minus_one)               <> NVL(c.amt_acc902, n_minus_one)
            OR NVL(i.amt_acc909, n_minus_one)               <> NVL(c.amt_acc909, n_minus_one)
            OR NVL(i.amt_acc920, n_minus_one)               <> NVL(c.amt_acc920, n_minus_one)
            OR NVL(i.amt_acc97, n_minus_one)                <> NVL(c.amt_acc97, n_minus_one)
            OR NVL(i.amt_acc318, n_minus_one)               <> NVL(c.amt_acc318, n_minus_one)
            OR NVL(i.amt_acc718, n_minus_one)               <> NVL(c.amt_acc718, n_minus_one)
            OR NVL(i.amt_acc818, n_minus_one)               <> NVL(c.amt_acc818, n_minus_one)
            OR NVL(i.amt_acc900, n_minus_one)               <> NVL(c.amt_acc900, n_minus_one)
            OR NVL(i.amt_acc903, n_minus_one)               <> NVL(c.amt_acc903, n_minus_one)
            OR NVL(i.amt_acc913, n_minus_one)               <> NVL(c.amt_acc913, n_minus_one)
            OR NVL(i.amt_acc914, n_minus_one)               <> NVL(c.amt_acc914, n_minus_one)
            OR NVL(i.amt_acc916, n_minus_one)               <> NVL(c.amt_acc916, n_minus_one)
            OR NVL(i.amt_acc918, n_minus_one)               <> NVL(c.amt_acc918, n_minus_one)
            OR NVL(i.amt_acc3501, n_minus_one)               <> NVL(c.amt_acc3501, n_minus_one)
            OR NVL(i.amt_acc3502, n_minus_one)               <> NVL(c.amt_acc3502, n_minus_one)
            OR NVL(i.amt_acc3504, n_minus_one)               <> NVL(c.amt_acc3504, n_minus_one)
            OR NVL(i.amt_acc3500, n_minus_one)               <> NVL(c.amt_acc3500, n_minus_one)
            OR NVL(i.amt_acc3503, n_minus_one)               <> NVL(c.amt_acc3503, n_minus_one)
            OR NVL(i.amt_acc3516, n_minus_one)               <> NVL(c.amt_acc3516, n_minus_one)
            OR NVL(i.amt_acc3518, n_minus_one)               <> NVL(c.amt_acc3518, n_minus_one)
            OR NVL(i.amt_acc328, n_minus_one)               <> NVL(c.amt_acc328, n_minus_one)
            OR NVL(i.amt_acc528, n_minus_one)               <> NVL(c.amt_acc528, n_minus_one)
            OR NVL(i.amt_acc338, n_minus_one)               <> NVL(c.amt_acc338, n_minus_one)
            OR NVL(i.amt_acc339, n_minus_one)               <> NVL(c.amt_acc339, n_minus_one)
            OR NVL(i.amt_acc211, n_minus_one)               <> NVL(c.amt_acc211, n_minus_one)
            OR NVL(i.amt_acc311, n_minus_one)               <> NVL(c.amt_acc311, n_minus_one)
            OR NVL(i.amt_acc175, n_minus_one)               <> NVL(c.amt_acc175, n_minus_one)
            OR NVL(i.AMT_ACC115_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC115_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC116_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC116_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC181_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC181_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC185_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC185_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC183_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC183_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC625_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC625_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC301_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC301_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC541_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC541_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC305_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC305_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC316_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC316_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC701_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC701_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC716_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC716_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC801_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC801_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC816_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC816_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC481_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC481_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC483_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC483_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC485_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC485_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC91_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC91_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC3501_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC3501_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC901_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC901_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC916_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC916_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC3516_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC3516_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC175_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC175_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC176_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC176_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC331_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC331_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC605_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC605_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC2282_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC2282_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC282_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC282_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC702_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC702_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC802_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC802_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC302_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC302_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC306_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC306_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC542_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC542_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC92_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC92_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC3502_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC3502_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC902_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC902_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC177_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC177_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC201_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC201_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC211_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC211_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC212_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC212_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC213_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC213_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC214_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC214_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC215_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC215_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC311_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC311_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC332_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC332_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC697_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC697_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC93_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC93_ROUNDED, n_minus_one)
            OR NVL(i.AMT_ACC543_ROUNDED, n_minus_one) <>  NVL(c.AMT_ACC543_ROUNDED, n_minus_one)
   );

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
                                  cascade          => TRUE);

----20210215
--    --Update flag_his_update for records having incremental data
--    merge /*+ append */ into LDM_SBV.ft_sbv_account_data_hist des
--    using (select id_source_his
--           from LDM_SBV.stc_sbv_account_data_td
--           where id_source_his is not null) src on (des.id_source = src.id_source_his)
--    when matched then update set
--        des.flag_his_update = 1
--    ;
--    commit;

    v_step := 'Close log';
    owner_core.ms_global_log.write_map_end(p_process_key  => p_process_key,
                                          p_mapping_name => c_mapping,
                                          p_status       => 'COMPLETE');
  EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        owner_core.etl_log_api.logend2(p_proc_key => p_process_key,
                                      p_log_name => c_mapping,
                                      p_status   => 'FAILED',
                                      p_info     => v_step || ', SQLERRM = ' || substr(dbms_utility.format_error_stack, 1, 300));
        LDM_SBV_ETL.SP_SEND_NOTIFICATION (v_dm2_err_addr, 'LIB_FT_SBV_ACCOUNT_TD.'|| c_mapping ,v_step || ', SQLERRM = ' || substr(dbms_utility.format_error_stack, 1, 300));

        raise_application_error(-20123, 'Error in module '||c_mapping||' ('||v_step||')', TRUE);
END ft_sbvaccdat_diff_sbv;

PROCEDURE ft_sbvaccdat_old_load_sbv (p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2)
IS
  v_step           VARCHAR2(255);
  v_cnt            INTEGER;
  c_mapping        VARCHAR2(60) := 'FT_SBVACCDAT_OLD_LOAD_SBV';
  c_table_owner    VARCHAR2(40) := 'LDM_SBV';
  c_table_name     VARCHAR2(40) := 'FT_SBV_ACCOUNT_DATA_TD';

BEGIN

    v_step := 'Start log';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                          p_mapping_name => c_mapping);

    v_step := 'Merge data into LDM_SBV.ft_sbv_account_data_td';
    MERGE /*+ append */ INTO LDM_SBV.ft_sbv_account_data_td t
    USING (SELECT src.id_contract, src.id_source_previous, src.date_valid_from, src.flag_accrual
            FROM LDM_SBV.stc_sbv_account_data_td src
           WHERE src.code_change_type = 'U') s
    ON (--s.id_contract           = t.id_contract    AND
    s.id_source_previous           = t.id_source)
    WHEN MATCHED THEN UPDATE SET
--        t.date_valid_to = case when s.flag_accrual = 1 and t.date_valid_from < last_day(add_months(p_effective_date, -1)) then p_effective_date - 2
--                                when s.flag_accrual = 1 and t.date_valid_from = last_day(add_months(p_effective_date, -1)) then t.date_valid_to
--                            else s.date_valid_from - 1
--                          end,
        t.date_valid_to = s.date_valid_from - 1,
        t.date_effective = p_effective_date,
        t.skp_proc_updated = p_process_key,
        t.dtime_updated = sysdate
    where t.date_valid_to = d_def_value_date_future;

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
                                          p_status       => 'COMPLETE');

  EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        owner_core.etl_log_api.logend2(p_proc_key => p_process_key,
                                      p_log_name => c_mapping,
                                      p_status   => 'FAILED',
                                      p_info     => v_step || ', SQLERRM = ' || substr(dbms_utility.format_error_stack, 1, 300));
        LDM_SBV_ETL.SP_SEND_NOTIFICATION (v_dm2_err_addr, 'LIB_FT_SBV_ACCOUNT_TD.'|| c_mapping ,v_step || ', SQLERRM = ' || substr(dbms_utility.format_error_stack, 1, 300));

        raise_application_error(-20123, 'Error in module '||c_mapping||' ('||v_step||')', TRUE);
END ft_sbvaccdat_old_load_sbv;

PROCEDURE ft_sbvaccdat_new_load_sbv (p_process_key IN NUMBER, p_effective_date IN DATE, p_data_type IN VARCHAR2)
IS
  v_step           VARCHAR2(255);
  v_cnt            INTEGER;
  c_mapping        VARCHAR2(60) := 'FT_SBVACCDAT_NEW_LOAD_SBV';
  c_table_owner    VARCHAR2(40) := 'LDM_SBV';
  c_table_name     VARCHAR2(40) := 'FT_SBV_ACCOUNT_DATA_TD';

BEGIN

    v_step := 'Start log';
    owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                          p_mapping_name => c_mapping);

    v_step := 'Merge data into LDM_SBV.ft_sbv_account_data_td';

    INSERT INTO LDM_SBV.ft_sbv_account_data_td t (
                skf_sbv_account_data,
                code_source_system,
                id_source,
                date_effective,
                skp_proc_inserted,
                skp_proc_updated,
                flag_deleted,
                code_credit_type,
                skp_contract,
                id_contract,
                id_client,
                text_contract_number,
                amt_acc101,
                amt_acc115,
                amt_acc116,
                amt_acc201,
                amt_acc209,
                amt_acc212,
                amt_acc213,
                amt_acc214,
                amt_acc215,
                amt_acc226,
                amt_acc277,
                amt_acc301,
                amt_acc302,
                amt_acc312,
                amt_acc326,
                amt_acc331,
                amt_acc377,
                amt_acc997,
                amt_acc176,
                amt_acc177,
                amt_acc178,
                date_valid_from,
                date_valid_to,
                amt_acc332,
                amt_acc605,
                amt_acc697,
                amt_acc541,
                amt_acc305,
                amt_acc542,
                amt_acc306,
                amt_acc282,
                amt_acc300,
                amt_acc319,
                amt_acc320,
                amt_acc349,
                amt_acc350,
                amt_acc357,
                amt_acc358,
                amt_acc359,
                amt_acc360,
                amt_acc372,
                amt_acc375,
                amt_acc376,
                amt_acc380,
                amt_acc383,
                amt_acc386,
                amt_acc388,
                amt_acc389,
                amt_acc390,
                amt_acc391,
                amt_acc392,
                amt_acc394,
                amt_acc395,
                amt_acc396,
                amt_acc397,
                amt_acc621,
                amt_acc625,
                amt_acc2282,
                amt_acc543,
                amt_acc544,
                amt_acc547,
                amt_acc308,
                amt_acc335,
                amt_acc353,
                amt_acc272,
                amt_acc219,
                amt_acc275,
                amt_acc181,
                amt_acc185,
                amt_acc183,
                amt_acc316,
                amt_acc382,
                amt_acc373,
                amt_acc273,
                amt_acc998,
                amt_acc303,
                amt_acc179,
                amt_acc546,
                amt_acc548,
                amt_acc500,
                amt_acc323,
                amt_acc549,
                amt_acc545,
                code_transaction_subtype,
                amt_acc5002,
                amt_acc5003,
                amt_acc800,
                amt_acc801,
                amt_acc802,
                amt_acc816,
                amt_acc701,
                amt_acc716,
                amt_acc702,
                amt_acc398,
                amt_acc307,
                amt_acc481,
                amt_acc483,
                amt_acc485,
                amt_acc496,
                amt_acc3310,
                amt_acc540,
                amt_acc703,
                amt_acc704,
                amt_acc803,
                amt_acc804,
                amt_acc3300,
                amt_acc078,
                amt_acc121,
                amt_acc123,
                amt_acc125,
                amt_acc91,
                amt_acc92,
                amt_acc93,
                amt_acc94,
                amt_acc95,
                amt_acc96,
                amt_acc99,
                amt_acc904,
                amt_acc333,
                amt_acc5006,
                amt_acc901,
                amt_acc902,
                amt_acc909,
                amt_acc920,
                amt_acc97,
                amt_acc318,
                amt_acc718,
                amt_acc818,
                amt_acc900,
                amt_acc903,
                amt_acc913,
                amt_acc914,
                amt_acc916,
                amt_acc918,
                amt_acc3501 ,
                amt_acc3502 ,
                amt_acc3504 ,
                amt_acc3500 ,
                amt_acc3503 ,
                amt_acc3516 ,
                amt_acc3518 ,
                text_comment,
                dtime_inserted,
                dtime_updated,
                amt_acc328 ,
                amt_acc528 ,
                amt_acc338,
                amt_acc339,
                amt_acc211,
                amt_acc311,
                amt_acc175,
                AMT_ACC115_ROUNDED,
                AMT_ACC116_ROUNDED,
                AMT_ACC181_ROUNDED,
                AMT_ACC185_ROUNDED,
                AMT_ACC183_ROUNDED,
                AMT_ACC625_ROUNDED,
                AMT_ACC301_ROUNDED,
                AMT_ACC541_ROUNDED,
                AMT_ACC305_ROUNDED,
                AMT_ACC316_ROUNDED,
                AMT_ACC701_ROUNDED,
                AMT_ACC716_ROUNDED,
                AMT_ACC801_ROUNDED,
                AMT_ACC816_ROUNDED,
                AMT_ACC481_ROUNDED,
                AMT_ACC483_ROUNDED,
                AMT_ACC485_ROUNDED,
                AMT_ACC91_ROUNDED,
                AMT_ACC3501_ROUNDED,
                AMT_ACC901_ROUNDED,
                AMT_ACC916_ROUNDED,
                AMT_ACC3516_ROUNDED,
                AMT_ACC175_ROUNDED,
                AMT_ACC176_ROUNDED,
                AMT_ACC331_ROUNDED,
                AMT_ACC605_ROUNDED,
                AMT_ACC2282_ROUNDED,
                AMT_ACC282_ROUNDED,
                AMT_ACC702_ROUNDED,
                AMT_ACC802_ROUNDED,
                AMT_ACC302_ROUNDED,
                AMT_ACC306_ROUNDED,
                AMT_ACC542_ROUNDED,
                AMT_ACC92_ROUNDED,
                AMT_ACC3502_ROUNDED,
                AMT_ACC902_ROUNDED,
                AMT_ACC177_ROUNDED,
                AMT_ACC201_ROUNDED,
                AMT_ACC211_ROUNDED,
                AMT_ACC212_ROUNDED,
                AMT_ACC213_ROUNDED,
                AMT_ACC214_ROUNDED,
                AMT_ACC215_ROUNDED,
                AMT_ACC311_ROUNDED,
                AMT_ACC332_ROUNDED,
                AMT_ACC697_ROUNDED,
                AMT_ACC93_ROUNDED,
                AMT_ACC543_ROUNDED
  )
  SELECT
        ssad.skf_sbv_account_data,
        ssad.code_source_system,
        ssad.id_source,
        ssad.date_effective,
        ssad.skp_proc_inserted,
        ssad.skp_proc_updated,
        ssad.flag_deleted,
        ssad.code_credit_type,
        ssad.skp_contract,
        ssad.id_contract,
        ssad.id_client,
        ssad.text_contract_number,
        ssad.amt_acc101,
        ssad.amt_acc115,
        ssad.amt_acc116,
        ssad.amt_acc201,
        ssad.amt_acc209,
        ssad.amt_acc212,
        ssad.amt_acc213,
        ssad.amt_acc214,
        ssad.amt_acc215,
        ssad.amt_acc226,
        ssad.amt_acc277,
        ssad.amt_acc301,
        ssad.amt_acc302,
        ssad.amt_acc312,
        ssad.amt_acc326,
        ssad.amt_acc331,
        ssad.amt_acc377,
        ssad.amt_acc997,
        ssad.amt_acc176,
        ssad.amt_acc177,
        ssad.amt_acc178,
        ssad.date_valid_from,
        ssad.date_valid_to,
        ssad.amt_acc332,
        ssad.amt_acc605,
        ssad.amt_acc697,
        ssad.amt_acc541,
        ssad.amt_acc305,
        ssad.amt_acc542,
        ssad.amt_acc306,
        ssad.amt_acc282,
        ssad.amt_acc300,
        ssad.amt_acc319,
        ssad.amt_acc320,
        ssad.amt_acc349,
        ssad.amt_acc350,
        ssad.amt_acc357,
        ssad.amt_acc358,
        ssad.amt_acc359,
        ssad.amt_acc360,
        ssad.amt_acc372,
        ssad.amt_acc375,
        ssad.amt_acc376,
        ssad.amt_acc380,
        ssad.amt_acc383,
        ssad.amt_acc386,
        ssad.amt_acc388,
        ssad.amt_acc389,
        ssad.amt_acc390,
        ssad.amt_acc391,
        ssad.amt_acc392,
        ssad.amt_acc394,
        ssad.amt_acc395,
        ssad.amt_acc396,
        ssad.amt_acc397,
        ssad.amt_acc621,
        ssad.amt_acc625,
        ssad.amt_acc2282,
        ssad.amt_acc543,
        ssad.amt_acc544,
        ssad.amt_acc547,
        ssad.amt_acc308,
        ssad.amt_acc335,
        ssad.amt_acc353,
        ssad.amt_acc272,
        ssad.amt_acc219,
        ssad.amt_acc275,
        ssad.amt_acc181,
        ssad.amt_acc185,
        ssad.amt_acc183,
        ssad.amt_acc316,
        ssad.amt_acc382,
        ssad.amt_acc373,
        ssad.amt_acc273,
        ssad.amt_acc998,
        ssad.amt_acc303,
        ssad.amt_acc179,
        ssad.amt_acc546,
        ssad.amt_acc548,
        ssad.amt_acc500,
        ssad.amt_acc323,
        ssad.amt_acc549,
        ssad.amt_acc545,
        ssad.code_transaction_subtype,
        ssad.amt_acc5002,
        ssad.amt_acc5003,
        ssad.amt_acc800,
        ssad.amt_acc801,
        ssad.amt_acc802,
        ssad.amt_acc816,
        ssad.amt_acc701,
        ssad.amt_acc716,
        ssad.amt_acc702,
        ssad.amt_acc398,
        ssad.amt_acc307,
        ssad.amt_acc481,
        ssad.amt_acc483,
        ssad.amt_acc485,
        ssad.amt_acc496,
        ssad.amt_acc3310,
        ssad.amt_acc540,
        ssad.amt_acc703,
        ssad.amt_acc704,
        ssad.amt_acc803,
        ssad.amt_acc804,
        ssad.amt_acc3300,
        ssad.amt_acc078,
        ssad.amt_acc121,
        ssad.amt_acc123,
        ssad.amt_acc125,
        ssad.amt_acc91,
        ssad.amt_acc92,
        ssad.amt_acc93,
        ssad.amt_acc94,
        ssad.amt_acc95,
        ssad.amt_acc96,
        ssad.amt_acc99,
        ssad.amt_acc904,
        ssad.amt_acc333,
        ssad.amt_acc5006,
        ssad.amt_acc901,
        ssad.amt_acc902,
        ssad.amt_acc909,
        ssad.amt_acc920,
        ssad.amt_acc97,
        ssad.amt_acc318,
        ssad.amt_acc718,
        ssad.amt_acc818,
        ssad.amt_acc900,
        ssad.amt_acc903,
        ssad.amt_acc913,
        ssad.amt_acc914,
        ssad.amt_acc916,
        ssad.amt_acc918,
        ssad.amt_acc3501 ,
        ssad.amt_acc3502 ,
        ssad.amt_acc3504 ,
        ssad.amt_acc3500 ,
        ssad.amt_acc3503 ,
        ssad.amt_acc3516 ,
        ssad.amt_acc3518 ,
        ssad.text_comment,
        ssad.dtime_inserted,
        ssad.dtime_updated,
        ssad.amt_acc328 ,
        ssad.amt_acc528 ,
        ssad.amt_acc338,
        ssad.amt_acc339,
        ssad.amt_acc211,
        ssad.amt_acc311,
        ssad.amt_acc175,
        ssad.AMT_ACC115_ROUNDED,
        ssad.AMT_ACC116_ROUNDED,
        ssad.AMT_ACC181_ROUNDED,
        ssad.AMT_ACC185_ROUNDED,
        ssad.AMT_ACC183_ROUNDED,
        ssad.AMT_ACC625_ROUNDED,
        ssad.AMT_ACC301_ROUNDED,
        ssad.AMT_ACC541_ROUNDED,
        ssad.AMT_ACC305_ROUNDED,
        ssad.AMT_ACC316_ROUNDED,
        ssad.AMT_ACC701_ROUNDED,
        ssad.AMT_ACC716_ROUNDED,
        ssad.AMT_ACC801_ROUNDED,
        ssad.AMT_ACC816_ROUNDED,
        ssad.AMT_ACC481_ROUNDED,
        ssad.AMT_ACC483_ROUNDED,
        ssad.AMT_ACC485_ROUNDED,
        ssad.AMT_ACC91_ROUNDED,
        ssad.AMT_ACC3501_ROUNDED,
        ssad.AMT_ACC901_ROUNDED,
        ssad.AMT_ACC916_ROUNDED,
        ssad.AMT_ACC3516_ROUNDED,
        ssad.AMT_ACC175_ROUNDED,
        ssad.AMT_ACC176_ROUNDED,
        ssad.AMT_ACC331_ROUNDED,
        ssad.AMT_ACC605_ROUNDED,
        ssad.AMT_ACC2282_ROUNDED,
        ssad.AMT_ACC282_ROUNDED,
        ssad.AMT_ACC702_ROUNDED,
        ssad.AMT_ACC802_ROUNDED,
        ssad.AMT_ACC302_ROUNDED,
        ssad.AMT_ACC306_ROUNDED,
        ssad.AMT_ACC542_ROUNDED,
        ssad.AMT_ACC92_ROUNDED,
        ssad.AMT_ACC3502_ROUNDED,
        ssad.AMT_ACC902_ROUNDED,
        ssad.AMT_ACC177_ROUNDED,
        ssad.AMT_ACC201_ROUNDED,
        ssad.AMT_ACC211_ROUNDED,
        ssad.AMT_ACC212_ROUNDED,
        ssad.AMT_ACC213_ROUNDED,
        ssad.AMT_ACC214_ROUNDED,
        ssad.AMT_ACC215_ROUNDED,
        ssad.AMT_ACC311_ROUNDED,
        ssad.AMT_ACC332_ROUNDED,
        ssad.AMT_ACC697_ROUNDED,
        ssad.AMT_ACC93_ROUNDED,
        ssad.AMT_ACC543_ROUNDED
    FROM LDM_SBV.stc_sbv_account_data_td ssad
--    where ssad.flag_accrual = 0
    ;

    v_cnt := SQL%ROWCOUNT;
    COMMIT;

    if trunc(p_effective_date, 'MM') = p_effective_date then
        INSERT INTO LDM_SBV.ft_sbv_account_data_td t (
                    skf_sbv_account_data,
                    code_source_system,
                    id_source,
                    date_effective,
                    skp_proc_inserted,
                    skp_proc_updated,
                    flag_deleted,
                    code_credit_type,
                    skp_contract,
                    id_contract,
                    id_client,
                    text_contract_number,
                    amt_acc101,
                    amt_acc115,
                    amt_acc116,
                    amt_acc201,
                    amt_acc209,
                    amt_acc212,
                    amt_acc213,
                    amt_acc214,
                    amt_acc215,
                    amt_acc226,
                    amt_acc277,
                    amt_acc301,
                    amt_acc302,
                    amt_acc312,
                    amt_acc326,
                    amt_acc331,
                    amt_acc377,
                    amt_acc997,
                    amt_acc176,
                    amt_acc177,
                    amt_acc178,
                    date_valid_from,
                    date_valid_to,
                    amt_acc332,
                    amt_acc605,
                    amt_acc697,
                    amt_acc541,
                    amt_acc305,
                    amt_acc542,
                    amt_acc306,
                    amt_acc282,
                    amt_acc300,
                    amt_acc319,
                    amt_acc320,
                    amt_acc349,
                    amt_acc350,
                    amt_acc357,
                    amt_acc358,
                    amt_acc359,
                    amt_acc360,
                    amt_acc372,
                    amt_acc375,
                    amt_acc376,
                    amt_acc380,
                    amt_acc383,
                    amt_acc386,
                    amt_acc388,
                    amt_acc389,
                    amt_acc390,
                    amt_acc391,
                    amt_acc392,
                    amt_acc394,
                    amt_acc395,
                    amt_acc396,
                    amt_acc397,
                    amt_acc621,
                    amt_acc625,
                    amt_acc2282,
                    amt_acc543,
                    amt_acc544,
                    amt_acc547,
                    amt_acc308,
                    amt_acc335,
                    amt_acc353,
                    amt_acc272,
                    amt_acc219,
                    amt_acc275,
                    amt_acc181,
                    amt_acc185,
                    amt_acc183,
                    amt_acc316,
                    amt_acc382,
                    amt_acc373,
                    amt_acc273,
                    amt_acc998,
                    amt_acc303,
                    amt_acc179,
                    amt_acc546,
                    amt_acc548,
                    amt_acc500,
                    amt_acc323,
                    amt_acc549,
                    amt_acc545,
                    code_transaction_subtype,
                    amt_acc5002,
                    amt_acc5003,
                    amt_acc800,
                    amt_acc801,
                    amt_acc802,
                    amt_acc816,
                    amt_acc701,
                    amt_acc716,
                    amt_acc702,
                    amt_acc398,
                    amt_acc307,
                    amt_acc481,
                    amt_acc483,
                    amt_acc485,
                    amt_acc496,
                    amt_acc3310,
                    amt_acc540,
                    amt_acc703,
                    amt_acc704,
                    amt_acc803,
                    amt_acc804,
                    amt_acc3300,
                    amt_acc078,
                    amt_acc121,
                    amt_acc123,
                    amt_acc125,
                    amt_acc91,
                    amt_acc92,
                    amt_acc93,
                    amt_acc94,
                    amt_acc95,
                    amt_acc96,
                    amt_acc99,
                    amt_acc904,
                    amt_acc333,
                    amt_acc5006,
                    amt_acc901,
                    amt_acc902,
                    amt_acc909,
                    amt_acc920,
                    amt_acc97,
                    amt_acc318,
                    amt_acc718,
                    amt_acc818,
                    amt_acc900,
                    amt_acc903,
                    amt_acc913,
                    amt_acc914,
                    amt_acc916,
                    amt_acc918,
                    amt_acc3501 ,
                    amt_acc3502 ,
                    amt_acc3504 ,
                    amt_acc3500 ,
                    amt_acc3503 ,
                    amt_acc3516 ,
                    amt_acc3518 ,
                    text_comment,
                    dtime_inserted,
                    dtime_updated,
                    amt_acc328,
                    amt_acc528 ,
                    amt_acc338,
                    amt_acc339,
                    amt_acc211,
                    amt_acc311,
                    amt_acc175,
                    AMT_ACC115_ROUNDED,
                    AMT_ACC116_ROUNDED,
                    AMT_ACC181_ROUNDED,
                    AMT_ACC185_ROUNDED,
                    AMT_ACC183_ROUNDED,
                    AMT_ACC625_ROUNDED,
                    AMT_ACC301_ROUNDED,
                    AMT_ACC541_ROUNDED,
                    AMT_ACC305_ROUNDED,
                    AMT_ACC316_ROUNDED,
                    AMT_ACC701_ROUNDED,
                    AMT_ACC716_ROUNDED,
                    AMT_ACC801_ROUNDED,
                    AMT_ACC816_ROUNDED,
                    AMT_ACC481_ROUNDED,
                    AMT_ACC483_ROUNDED,
                    AMT_ACC485_ROUNDED,
                    AMT_ACC91_ROUNDED,
                    AMT_ACC3501_ROUNDED,
                    AMT_ACC901_ROUNDED,
                    AMT_ACC916_ROUNDED,
                    AMT_ACC3516_ROUNDED,
                    AMT_ACC175_ROUNDED,
                    AMT_ACC176_ROUNDED,
                    AMT_ACC331_ROUNDED,
                    AMT_ACC605_ROUNDED,
                    AMT_ACC2282_ROUNDED,
                    AMT_ACC282_ROUNDED,
                    AMT_ACC702_ROUNDED,
                    AMT_ACC802_ROUNDED,
                    AMT_ACC302_ROUNDED,
                    AMT_ACC306_ROUNDED,
                    AMT_ACC542_ROUNDED,
                    AMT_ACC92_ROUNDED,
                    AMT_ACC3502_ROUNDED,
                    AMT_ACC902_ROUNDED,
                    AMT_ACC177_ROUNDED,
                    AMT_ACC201_ROUNDED,
                    AMT_ACC211_ROUNDED,
                    AMT_ACC212_ROUNDED,
                    AMT_ACC213_ROUNDED,
                    AMT_ACC214_ROUNDED,
                    AMT_ACC215_ROUNDED,
                    AMT_ACC311_ROUNDED,
                    AMT_ACC332_ROUNDED,
                    AMT_ACC697_ROUNDED,
                    AMT_ACC93_ROUNDED,
                    AMT_ACC543_ROUNDED
        )
        SELECT
            LDM_SBV.S_FT_SBV_ACCOUNT_DATA_TD.nextval skf_sbv_account_data,
            ssad.code_source_system,
            replace(ssad.id_source, substr(ssad.id_source, instr(ssad.id_source, '.', 1, 1) + 1, 8), to_char(p_effective_date - 1,'yyyymmdd')) || '.ac' id_source,
            ssad.date_effective,
            ssad.skp_proc_inserted,
            ssad.skp_proc_updated,
            ssad.flag_deleted,
            ssad.code_credit_type,
            ssad.skp_contract,
            ssad.id_contract,
            ssad.id_client,
            ssad.text_contract_number,
            t.amt_acc101,
            t.amt_acc115,
            t.amt_acc116,
            ssad.amt_acc201,
            t.amt_acc209,
            ssad.amt_acc212,
            ssad.amt_acc213,
            ssad.amt_acc214,
            ssad.amt_acc215,
            t.amt_acc226,
            t.amt_acc277,
            t.amt_acc301,
            t.amt_acc302,
            t.amt_acc312,
            t.amt_acc326,
            t.amt_acc331,
            t.amt_acc377,
            t.amt_acc997,
            t.amt_acc176,
            t.amt_acc177,
            t.amt_acc178,
            p_effective_date -1 date_valid_from,
            p_effective_date -1 date_valid_to,
            t.amt_acc332,
            t.amt_acc605,
            t.amt_acc697,
            t.amt_acc541,
            t.amt_acc305,
            t.amt_acc542,
            t.amt_acc306,
            ssad.amt_acc282,
            t.amt_acc300,
            t.amt_acc319,
            t.amt_acc320,
            t.amt_acc349,
            t.amt_acc350,
            t.amt_acc357,
            t.amt_acc358,
            t.amt_acc359,
            t.amt_acc360,
            t.amt_acc372,
            t.amt_acc375,
            t.amt_acc376,
            t.amt_acc380,
            t.amt_acc383,
            t.amt_acc386,
            t.amt_acc388,
            t.amt_acc389,
            t.amt_acc390,
            t.amt_acc391,
            t.amt_acc392,
            t.amt_acc394,
            t.amt_acc395,
            t.amt_acc396,
            t.amt_acc397,
            t.amt_acc621,
            t.amt_acc625,
            ssad.amt_acc2282,
            t.amt_acc543,
            t.amt_acc544,
            t.amt_acc547,
            t.amt_acc308,
            t.amt_acc335,
            t.amt_acc353,
            t.amt_acc272,
            t.amt_acc219,
            t.amt_acc275,
            t.amt_acc181,
            t.amt_acc185,
            t.amt_acc183,
            t.amt_acc316,
            t.amt_acc382,
            t.amt_acc373,
            t.amt_acc273,
            t.amt_acc998,
            t.amt_acc303,
            t.amt_acc179,
            t.amt_acc546,
            t.amt_acc548,
            t.amt_acc500,
            t.amt_acc323,
            t.amt_acc549,
            t.amt_acc545,
            t.code_transaction_subtype,
            t.amt_acc5002,
            t.amt_acc5003,
            t.amt_acc800,
            t.amt_acc801,
            t.amt_acc802,
            t.amt_acc816,
            t.amt_acc701,
            t.amt_acc716,
            t.amt_acc702,
            t.amt_acc398,
            t.amt_acc307,
            t.amt_acc481,
            t.amt_acc483,
            t.amt_acc485,
            t.amt_acc496,
            t.amt_acc3310,
            t.amt_acc540,
            t.amt_acc703,
            t.amt_acc704,
            t.amt_acc803,
            t.amt_acc804,
            t.amt_acc3300,
            t.amt_acc078,
            t.amt_acc121,
            t.amt_acc123,
            t.amt_acc125,
            t.amt_acc91,
            t.amt_acc92,
            t.amt_acc93,
            t.amt_acc94,
            t.amt_acc95,
            t.amt_acc96,
            t.amt_acc99,
            t.amt_acc904,
            t.amt_acc333,
            t.amt_acc5006,
            t.amt_acc901,
            t.amt_acc902,
            t.amt_acc909,
            t.amt_acc920,
            t.amt_acc97,
            t.amt_acc318,
            t.amt_acc718,
            t.amt_acc818,
            t.amt_acc900,
            t.amt_acc903,
            t.amt_acc913,
            t.amt_acc914,
            t.amt_acc916,
            t.amt_acc918,
            t.amt_acc3501 ,
            t.amt_acc3502 ,
            t.amt_acc3504 ,
            t.amt_acc3500 ,
            t.amt_acc3503 ,
            t.amt_acc3516 ,
            t.amt_acc3518 ,
            t.text_comment,
            ssad.dtime_inserted,
            ssad.dtime_updated,
            t.amt_acc328 ,
            t.amt_acc528 ,
            t.amt_acc338,
            t.amt_acc339,
            t.amt_acc211,
            t.amt_acc311,
            t.amt_acc175,
            t.AMT_ACC115_ROUNDED,
            t.AMT_ACC116_ROUNDED,
            t.AMT_ACC181_ROUNDED,
            t.AMT_ACC185_ROUNDED,
            t.AMT_ACC183_ROUNDED,
            t.AMT_ACC625_ROUNDED,
            t.AMT_ACC301_ROUNDED,
            t.AMT_ACC541_ROUNDED,
            t.AMT_ACC305_ROUNDED,
            t.AMT_ACC316_ROUNDED,
            t.AMT_ACC701_ROUNDED,
            t.AMT_ACC716_ROUNDED,
            t.AMT_ACC801_ROUNDED,
            t.AMT_ACC816_ROUNDED,
            t.AMT_ACC481_ROUNDED,
            t.AMT_ACC483_ROUNDED,
            t.AMT_ACC485_ROUNDED,
            t.AMT_ACC91_ROUNDED,
            t.AMT_ACC3501_ROUNDED,
            t.AMT_ACC901_ROUNDED,
            t.AMT_ACC916_ROUNDED,
            t.AMT_ACC3516_ROUNDED,
            t.AMT_ACC175_ROUNDED,
            t.AMT_ACC176_ROUNDED,
            t.AMT_ACC331_ROUNDED,
            t.AMT_ACC605_ROUNDED,
            ssad.AMT_ACC2282_ROUNDED,
            ssad.AMT_ACC282_ROUNDED,
            t.AMT_ACC702_ROUNDED,
            t.AMT_ACC802_ROUNDED,
            t.AMT_ACC302_ROUNDED,
            t.AMT_ACC306_ROUNDED,
            t.AMT_ACC542_ROUNDED,
            t.AMT_ACC92_ROUNDED,
            t.AMT_ACC3502_ROUNDED,
            t.AMT_ACC902_ROUNDED,
            t.AMT_ACC177_ROUNDED,
            ssad.AMT_ACC201_ROUNDED,
            t.AMT_ACC211_ROUNDED,
            ssad.AMT_ACC212_ROUNDED,
            ssad.AMT_ACC213_ROUNDED,
            ssad.AMT_ACC214_ROUNDED,
            ssad.AMT_ACC215_ROUNDED,
            t.AMT_ACC311_ROUNDED,
            t.AMT_ACC332_ROUNDED,
            t.AMT_ACC697_ROUNDED,
            t.AMT_ACC93_ROUNDED,
            t.AMT_ACC543_ROUNDED

        FROM LDM_SBV.stc_sbv_account_data_td ssad
            JOIN LDM_SBV.ft_sbv_account_data_td t ON ssad.id_source_previous = t.id_source
        where ssad.flag_accrual = 1
            --and ssad.id_source_previous is null
        ;

        merge into LDM_SBV.ft_sbv_account_data_td des
        using (SELECT skp_contract, id_source_previous, amt_acc282, amt_acc2282
                , amt_acc201, amt_acc211, amt_acc212, amt_acc213, amt_acc214, amt_acc215
              FROM LDM_SBV.stc_sbv_account_data_td
              where flag_accrual = 1
                and id_source_previous is not null
              ) src on (src.skp_contract = des.skp_contract
--                    and src.code_transaction_subtype = des.code_transaction_subtype
                    and src.id_source_previous = des.id_source
                    )
        when matched then update set
            des.skp_proc_updated = p_process_key
            , des.dtime_updated = sysdate
            , des.date_valid_to =  p_effective_date - 2 -- Closed old record

        ;
        commit;
    end if;

  v_step := 'Write record count';
  owner_core.ms_global_log.write_map_cnt(p_process_key  => p_process_key,
                                         p_mapping_name => c_mapping,
                                         p_sel          => v_cnt,
                                         p_ins          => v_cnt);

  v_step := 'Close log';
  owner_core.ms_global_log.write_map_end(p_process_key  => p_process_key,
                                         p_mapping_name => c_mapping,
                                         p_status       => 'COMPLETE');

  EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        owner_core.etl_log_api.logend2(p_proc_key => p_process_key,
                                      p_log_name => c_mapping,
                                      p_status   => 'FAILED',
                                      p_info     => v_step || ', SQLERRM = ' || substr(dbms_utility.format_error_stack, 1, 300));
        LDM_SBV_ETL.SP_SEND_NOTIFICATION (v_dm2_err_addr, 'LIB_FT_SBV_ACCOUNT_TD.'|| c_mapping ,v_step || ', SQLERRM = ' || substr(dbms_utility.format_error_stack, 1, 300));

        raise_application_error(-20123, 'Error in module '||c_mapping||' ('||v_step||')', TRUE);

END ft_sbvaccdat_new_load_sbv;

END lib_ft_sbv_account_td;
/