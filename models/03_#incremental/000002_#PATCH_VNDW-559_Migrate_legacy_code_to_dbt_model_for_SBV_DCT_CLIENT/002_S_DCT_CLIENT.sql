-- sửa lại thành LDM_SBV sau khi đổi kết nối vào schema LDM_SBV 
BEGIN
    EXECUTE IMMEDIATE '
    CREATE SEQUENCE LDM_FIN.S_DCT_CLIENT START WITH 1 
    INCREMENT BY 1 
    CACHE 200
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
