BEGIN
    EXECUTE IMMEDIATE '
    CREATE TABLE LDM_SBV.DCT_CLIENT
    (
      SKP_CLIENT          NUMBER(38)                NOT NULL,
      CODE_SOURCE_SYSTEM  VARCHAR2(10 CHAR)         NOT NULL,
      ID_SOURCE           VARCHAR2(30 CHAR)         NOT NULL,
      DATE_EFFECTIVE      DATE                      NOT NULL,
      DTIME_INSERTED      DATE                      NOT NULL,
      DTIME_UPDATED       DATE                      NOT NULL,
      FLAG_DELETED        VARCHAR2(1 CHAR)          NOT NULL,
      ID_CUID             NUMBER(38)                NOT NULL
    )
    TABLESPACE LDM_SBV_DATA
    PARTITION BY RANGE (DATE_EFFECTIVE)
    INTERVAL (NUMTOYMINTERVAL(1,''MONTH''))
    STORE IN (LDM_CBE_DATA) 
    (
        PARTITION p8 values LESS THAN (TO_DATE(''01-09-2023'',''DD-MM-YYYY''))
       ,PARTITION p9 values LESS THAN (TO_DATE(''01-10-2023'',''DD-MM-YYYY''))
       ,PARTITION p10 values LESS THAN (TO_DATE(''01-11-2023'',''DD-MM-YYYY''))
       ,PARTITION p11 values LESS THAN (TO_DATE(''01-12-2023'',''DD-MM-YYYY''))
       ,PARTITION p12 values LESS THAN (TO_DATE(''01-01-2024'',''DD-MM-YYYY''))
       ,PARTITION p13 values LESS THAN (TO_DATE(''01-02-2024'',''DD-MM-YYYY''))
       ,PARTITION p14 values LESS THAN (TO_DATE(''01-03-2024'',''DD-MM-YYYY''))
       ,PARTITION p15 values LESS THAN (TO_DATE(''01-04-2024'',''DD-MM-YYYY''))
       ,PARTITION p16 values LESS THAN (TO_DATE(''01-05-2024'',''DD-MM-YYYY''))
       ,PARTITION p17 values LESS THAN (TO_DATE(''01-06-2024'',''DD-MM-YYYY''))
       ,PARTITION p18 values LESS THAN (TO_DATE(''01-07-2024'',''DD-MM-YYYY''))
    )
';

EXCEPTION
    WHEN OTHERS THEN
        IF (SQLCODE = -955)
        THEN
            Dbms_output.Put_line('Already done, do nothing');
        ELSE
            RAISE;
        END IF;
END;
/

COMMENT ON COLUMN LDM_SBV.DCT_CLIENT.SKP_CLIENT IS 'Primary surrogate key identifying record in the SKP_CLIENT table.';
COMMENT ON COLUMN LDM_SBV.DCT_CLIENT.CODE_SOURCE_SYSTEM IS 'Code of the source system.';
COMMENT ON COLUMN LDM_SBV.DCT_CLIENT.ID_SOURCE IS 'Identification of record in the source system.';
COMMENT ON COLUMN LDM_SBV.DCT_CLIENT.DATE_EFFECTIVE IS 'Effective (business) date the record was last modified.';
COMMENT ON COLUMN LDM_SBV.DCT_CLIENT.SKP_PROC_INSERTED IS 'Key of the process which inserted the record.';
COMMENT ON COLUMN LDM_SBV.DCT_CLIENT.SKP_PROC_UPDATED IS 'Key of the process which last updated the record.';
COMMENT ON COLUMN LDM_SBV.DCT_CLIENT.DTIME_INSERTED IS 'Date when record was created.';
COMMENT ON COLUMN LDM_SBV.DCT_CLIENT.DTIME_UPDATED IS 'Date (and time) when record was updated for the last time.';
COMMENT ON COLUMN LDM_SBV.DCT_CLIENT.FLAG_DELETED IS 'Flag marking records deleted from source system.';
COMMENT ON COLUMN LDM_SBV.DCT_CLIENT.ID_CUID IS 'Client universal ID in PIF';

BEGIN
  EXECUTE IMMEDIATE '
    CREATE UNIQUE INDEX LDM_SBV.PK_CLIENT ON LDM_SBV.DCT_CLIENT
    (SKP_CLIENT) 
    TABLESPACE LDM_SBV_IDX
';
EXCEPTION
  WHEN OTHERS THEN
    IF (SQLCODE IN (-955, -2261, -2260, -1408))
    THEN
      Dbms_output.Put_line('Already done, do nothing');
    ELSE
      RAISE;
    END IF;
END;
/

BEGIN
  EXECUTE IMMEDIATE '
    CREATE UNIQUE INDEX LDM_SBV.UK_CLIENT ON LDM_SBV.DCT_CLIENT
    (ID_SOURCE) 
    TABLESPACE LDM_SBV_IDX
';
EXCEPTION
  WHEN OTHERS THEN
    IF (SQLCODE IN (-955, -2261, -2260, -1408))
    THEN
      Dbms_output.Put_line('Already done, do nothing');
    ELSE
      RAISE;
    END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE '
    ALTER TABLE LDM_SBV.DCT_CLIENT 
        ADD CONSTRAINT PK_CLIENT
        PRIMARY KEY (SKP_CLIENT)
        USING INDEX TABLESPACE LDM_SBV_IDX';
EXCEPTION
    WHEN OTHERS THEN
        IF (SQLCODE IN (-955, -2261, -2260))
        THEN
            Dbms_output.Put_line('Already done, do nothing');
        ELSE
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE '
    ALTER TABLE LDM_SBV.DCT_CLIENT 
        ADD CONSTRAINT UK_CLIENT 
        UNIQUE (ID_SOURCE)
        USING INDEX TABLESPACE LDM_SBV_IDX';
EXCEPTION
    WHEN OTHERS THEN
        IF (SQLCODE IN (-955, -2261, -2260))
        THEN
            Dbms_output.Put_line('Already done, do nothing');
        ELSE
            RAISE;
        END IF;
END;
/
