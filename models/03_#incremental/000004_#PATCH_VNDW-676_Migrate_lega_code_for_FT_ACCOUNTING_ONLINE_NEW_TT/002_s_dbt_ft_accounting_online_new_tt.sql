BEGIN
    EXECUTE IMMEDIATE '
    CREATE SEQUENCE LDM_SBV.S_DBT_FT_ACCOUNTING_ONLINE_NEW_TT START WITH 1 
    INCREMENT BY 1 
    CACHE 1000
    NOCYCLE';
EXCEPTION
    WHEN OTHERS THEN
        IF (SQLCODE IN (-955))
        THEN
            Dbms_output.Put_line('Already done, do nothing');
        ELSE
            RAISE;
        END IF;
END;
/
