CREATE OR REPLACE PROCEDURE DWH.PRC_LOADPLAN
AS
    CURSOR c1 (vCONTEXT_CODE VARCHAR2)
    IS
        SELECT CONTEXT_CODE,
               LOAD_PLAN_NAME,
               START_DATE,
               END_DATE,
               STATUS,
               RN,
               TODAY_DUR,
               AVGDUR,
               DURATION,
               AVG_DUR
          FROM DWH.LOADPLAN
         WHERE CONTEXT_CODE = vCONTEXT_CODE;

    CURSOR c2 IS
        SELECT DISTINCT CONTEXT_CODE
          FROM DWH.LOADPLAN;

    cur                c1%ROWTYPE;

    myfrom             VARCHAR2 (30) := 'ugur.erol@dwh.com';
    myto               VARCHAR2 (30) := 'ugur.erol@dwh.com';
    mysubject          VARCHAR2 (100);
    mytime             VARCHAR2 (50);
    mymessage          VARCHAR2 (32767)
        := 'CONTEXT_CODE LOAD_PLAN_NAME START_DATE END_DATE STATUS RN DURATION AVG_DUR';
    i                  NUMBER;
    bgcolor            VARCHAR2 (10);
    tmp_CONTEXT_CODE   VARCHAR2 (35 CHAR);
BEGIN
    i := 0;
    mytime := TO_CHAR (SYSDATE, 'DD-Mon-YYYY hh24:mi');
    mysubject := 'Top SQL Cost';

    mymessage :=
        '<html>
<head>
<title>CK</title>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
</head>
<body bgcolor="#FFFFFF" leftmargin="1" topmargin="1" marginwidth="1" marginheight="1" style="font-family:''Verdana''">

';


    FOR cur2 IN c2
    LOOP
        tmp_CONTEXT_CODE := cur2.CONTEXT_CODE;
        mymessage :=
               mymessage
            || '
        <table id="Table_01" width="900" height="100" border="1" cellpadding="1" cellspacing="1">
        <tr> 
            <td align="center"><b>CONTEXT_CODE</b></td>
            <td align="center"><b>LOAD_PLAN_NAME</b></td>
            <td align="center"><b>SESSION_START_DATE</b></td>
            <td align="center"><b>SESSION_END_DATE</b></td>
            <td align="center"><b>STATUS</b></td>
            <td align="center"><b>RN</b></td>
            <td align="center"><b>SESS_DURATION</b></td>
            <td align="center"><b>AVG_DURATION</b></td>
        </tr>';


        FOR cur IN c1 (tmp_CONTEXT_CODE)
        LOOP
            IF     TRIM (UPPER (cur.STATUS)) = 'DONE'
               AND (cur.TODAY_DUR < cur.AVGDUR + 1000)
            THEN
                bgcolor := '#66FF66';                        -- done  -- green
            ELSIF     TRIM (UPPER (cur.STATUS)) = 'DONE'
                  AND (cur.TODAY_DUR >= cur.AVGDUR + 1000)
            THEN
                bgcolor := '#FFFF55'; -- done, extend  --if today_dur > 1000 sec then yellow
            ELSIF     TRIM (UPPER (cur.STATUS)) = 'RUNNING'
                  AND (cur.TODAY_DUR < cur.AVGDUR + 1000)
            THEN
                bgcolor := '#99FF99';           -- contiune...  -- light green
            ELSIF     TRIM (UPPER (cur.STATUS)) = 'RUNNING'
                  AND (cur.TODAY_DUR >= cur.AVGDUR + 1000)
            THEN
                bgcolor := '#FFFF77';     -- contiune, extend  -- light yellow
            ELSIF TRIM (UPPER (cur.STATUS)) = 'ERROR'
            THEN
                bgcolor := '#FC9191';                           --error  --red
            ELSE
                bgcolor := '#66FFFF';                 --waiting  --ligtht blue
            END IF;


            EXIT WHEN c1%NOTFOUND;

            mymessage :=
                   mymessage
                || '<tr><td style="border: none;" align="center" bgcolor ="'
                || bgcolor
                || '">'
                || cur.CONTEXT_CODE
                || '</td><td style="border: none;" align="center" bgcolor ="'
                || bgcolor
                || '">'
                || cur.LOAD_PLAN_NAME
                || '</td><td style="border: none;" align="center" bgcolor ="'
                || bgcolor
                || '">'
                || cur.START_DATE
                || '</td><td style="border: none;" align="center" bgcolor ="'
                || bgcolor
                || '">'
                || cur.END_DATE
                || '</td><td style="border: none;" align="center" bgcolor ="'
                || bgcolor
                || '">'
                || cur.STATUS
                || '</td><td style="border: none;" align="center" bgcolor ="'
                || bgcolor
                || '">'
                || cur.RN
                || '</td><td style="border: none;" align="center" bgcolor ="'
                || bgcolor
                || '">'
                || cur.DURATION
                || '</td><td style="border: none;" align="center" bgcolor ="'
                || bgcolor
                || '">'
                || cur.AVG_DUR
                || '</td></tr>';
        END LOOP;

        mymessage := mymessage || '</table> <p style="font-size:200%;"></p>';

        i := i + 1;
    END LOOP;


    mymessage := mymessage || '</body> </html>';

    IF i > 0
    THEN
        UTL_MAIL.send (
            sender       => 'ugur.erol@dwh.com',
            recipients   => 'ugur.erol@dwh.com',
            cc           =>
                'ugur.erol0@dwh.com; ugur.erol1@dwh.com; ugur.erol2@dwh.com',
            subject      =>
                'Load Plan (' || TO_CHAR (SYSDATE, 'DD.MM.YYYY') || ')',
            mime_type    => 'text/html;charset=windows-1254',
            MESSAGE      => mymessage);
    ELSE
        NULL;
    END IF;
END;
/
