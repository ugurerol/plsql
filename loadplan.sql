WITH
    ugr
    AS
        (SELECT LPI_RUN.I_LP_INST,
                LPI_RUN.LOAD_PLAN_NAME,
                LPI_RUN.CONTEXT_CODE,
                MIN (LPI_RUN.START_DATE)
                    OVER (PARTITION BY LPI_RUN.I_LP_INST)
                    AS START_DATE,                          --min. START_DATE,
                CASE
                    WHEN TRIM (LPI_RUN.STATUS) = 'R'
                    THEN
                        TO_DATE ('', 'dd/mm/yyyy hh24:mi:ss')
                    ELSE
                        LPI_RUN.END_DATE
                END
                    AS END_DATE,
                LPI_RUN.STATUS,
                MAX (LPI_RUN.NB_RUN)
                    OVER (PARTITION BY LPI_RUN.I_LP_INST
                          ORDER BY LPI_RUN.NB_RUN DESC)
                    AS RN,                                       --LAST_STATUS
                ROW_NUMBER ()
                    OVER (PARTITION BY LPI_RUN.I_LP_INST
                          ORDER BY NB_RUN DESC)
                    AS RN_DUMMY,                              --DUMMY_VARIABLE
                LPI_RUN.NB_RUN
                    NB_RUN,
                LPI_RUN.START_DATE
                    START_DATE_DUMMY,
                SUM (LPI_RUN.DURATION) OVER (PARTITION BY LPI_RUN.I_LP_INST)
                    AS DURATION,
                LPI_RUN.ERROR_MESSAGE
           FROM ODI_REPO_NAME.SNP_LPI_RUN LPI_RUN)
  SELECT ugr.CONTEXT_CODE,
         ugr.LOAD_PLAN_NAME,
         ugr.START_DATE,
         ugr.END_DATE,
         CASE
             WHEN TRIM (ugr.STATUS) = 'D'
             THEN
                 'Done'
             WHEN TRIM (ugr.STATUS) = 'E'
             THEN
                 'ERROR'
             WHEN ----- The reason we do this is to make "Status = Error" while other jobs are running when any job gets an error in parallel.-----
                  (    (ROUND (
                            TO_NUMBER (
                                  (  SYSDATE
                                   - (SELECT ugr.START_DATE_DUMMY
                                        FROM ugr
                                       WHERE     1 = 1
                                             AND ugr.RN = ugr.NB_RUN
                                             AND LAGG2.I_LP_INST =
                                                 ugr.I_LP_INST))
                                * 24
                                * 60
                                * 60),
                            0) >=
                        ROUND (
                            TO_NUMBER ((SYSDATE - LAGG2.MAXX) * 24 * 60 * 60),
                            0)) ----  Sysdate - Run Restart >= Sysdate - Error
                   AND (    TRIM (ugr.STATUS) = 'R'
                        AND TRIM (LAGG2.SESS_STATUS) = 'E'))
             THEN
                 'ERROR'
             WHEN TRIM (ugr.STATUS) = 'R'
             THEN
                 'Running'
             WHEN TRIM (ugr.STATUS) = 'W'
             THEN
                 'Waiting'
             WHEN TRIM (ugr.STATUS) = 'M'
             THEN
                 'Warning'
             ELSE
                 ugr.STATUS
         END
             AS STATUS,
         ugr.RN,
         ROUND (
             TO_NUMBER (
                 CASE
                     WHEN TRIM (ugr.STATUS) = 'R' AND ugr.NB_RUN = 1
                     THEN
                         ((SYSDATE - ugr.START_DATE) * 24 * 60 * 60)
                     WHEN TRIM (ugr.STATUS) = 'R' AND ugr.NB_RUN != 1
                     THEN
                           ugr.DURATION
                         + ((SYSDATE - ugr.START_DATE_DUMMY) * 24 * 60 * 60)
                     ELSE
                         ugr.DURATION
                 END),
             0)
             AS TODAY_DUR,
         ROUND (AVG1.AVG_DURATION, 0)
             AVGDUR,
         CASE
             -- for prevent "1hh : 60mi"
             WHEN (    (MOD (
                            (TO_NUMBER (
                                 CEIL (
                                       ROUND (
                                           TO_NUMBER (
                                               CASE
                                                   WHEN     TRIM (ugr.STATUS) =
                                                            'R'
                                                        AND ugr.NB_RUN = 1
                                                   THEN
                                                       (  (  SYSDATE
                                                           - ugr.START_DATE)
                                                        * 24
                                                        * 60
                                                        * 60)
                                                   WHEN     TRIM (ugr.STATUS) =
                                                            'R'
                                                        AND ugr.NB_RUN != 1
                                                   THEN
                                                         ugr.DURATION
                                                       + (  (  SYSDATE
                                                             - ugr.START_DATE_DUMMY)
                                                          * 24
                                                          * 60
                                                          * 60)
                                                   ELSE
                                                       ugr.DURATION
                                               END),
                                           0)
                                     / 60))),
                            60) =
                        0)
                   AND (TO_NUMBER (
                            CASE
                                WHEN TRIM (ugr.STATUS) = 'R' AND ugr.NB_RUN = 1
                                THEN
                                    ((SYSDATE - ugr.START_DATE) * 24 * 60 * 60)
                                WHEN     TRIM (ugr.STATUS) = 'R'
                                     AND ugr.NB_RUN != 1
                                THEN
                                      ugr.DURATION
                                    + (  (SYSDATE - ugr.START_DATE_DUMMY)
                                       * 24
                                       * 60
                                       * 60)
                                ELSE -- If the LP gets an error without working at all, let the else work.
                                    ugr.DURATION
                            END) NOT IN
                            (0,
                             3600,
                             7200,
                             10800,
                             14400,
                             18000,
                             21600,
                             25200,
                             28800,
                             32400,
                             36000,
                             39600,
                             43200,
                             46800,
                             50400,
                             54000,
                             57600,
                             61200,
                             64800,
                             72000)))
             THEN
                    ROUND (
                        TO_NUMBER (
                            CASE
                                WHEN TRIM (ugr.STATUS) = 'R' AND ugr.NB_RUN = 1
                                THEN
                                    (  TO_NUMBER (
                                           FLOOR (
                                                 (  ((  (  SYSDATE
                                                         - ugr.START_DATE)
                                                      * 24
                                                      * 60
                                                      * 60))
                                                  / 60)
                                               / 60))
                                     + 1)
                                WHEN     TRIM (ugr.STATUS) = 'R'
                                     AND ugr.NB_RUN != 1
                                THEN
                                    (  TO_NUMBER (
                                           FLOOR (
                                                 (  (  ugr.DURATION
                                                     + (  (  SYSDATE
                                                           - ugr.START_DATE_DUMMY)
                                                        * 24
                                                        * 60
                                                        * 60))
                                                  / 60)
                                               / 60))
                                     + 1)
                                ELSE
                                    (  TO_NUMBER (
                                           FLOOR (((ugr.DURATION) / 60) / 60))
                                     + 1)
                            END),
                        0)
                 || 'hh : '
                 || 0
                 || 'mi'
             ELSE
                    ROUND (
                        TO_NUMBER (
                            CASE
                                WHEN TRIM (ugr.STATUS) = 'R' AND ugr.NB_RUN = 1
                                THEN
                                    FLOOR (
                                          (  ((  (SYSDATE - ugr.START_DATE)
                                               * 24
                                               * 60
                                               * 60))
                                           / 60)
                                        / 60)
                                WHEN     TRIM (ugr.STATUS) = 'R'
                                     AND ugr.NB_RUN != 1
                                THEN
                                    FLOOR (
                                          (  (  ugr.DURATION
                                              + (  (  SYSDATE
                                                    - ugr.START_DATE_DUMMY)
                                                 * 24
                                                 * 60
                                                 * 60))
                                           / 60)
                                        / 60)
                                ELSE
                                    FLOOR (((ugr.DURATION) / 60) / 60)
                            END),
                        0)
                 || 'hh : '
                 || ROUND (
                        TO_NUMBER (
                            CASE
                                WHEN TRIM (ugr.STATUS) = 'R' AND ugr.NB_RUN = 1
                                THEN
                                    CEIL (
                                            (  (SYSDATE - ugr.START_DATE)
                                             * 24
                                             * 60
                                             * 60)
                                          / 60
                                        - (  (FLOOR (
                                                    (  (  (  SYSDATE
                                                           - ugr.START_DATE)
                                                        * 24
                                                        * 60
                                                        * 60)
                                                     / 60)
                                                  / 60))
                                           * 60))
                                WHEN     TRIM (ugr.STATUS) = 'R'
                                     AND ugr.NB_RUN != 1
                                THEN
                                    CEIL (
                                            (  ugr.DURATION
                                             + (  (  SYSDATE
                                                   - ugr.START_DATE_DUMMY)
                                                * 24
                                                * 60
                                                * 60))
                                          / 60
                                        - (  (FLOOR (
                                                    (  (  ugr.DURATION
                                                        + (  (  SYSDATE
                                                              - ugr.START_DATE_DUMMY)
                                                           * 24
                                                           * 60
                                                           * 60))
                                                     / 60)
                                                  / 60))
                                           * 60))
                                ELSE
                                    CEIL (
                                          ugr.DURATION / 60
                                        - (  FLOOR ((ugr.DURATION / 60) / 60)
                                           * 60))
                            END),
                        0)
                 || 'mi'
         END
             AS DURATION,
         CASE
             WHEN (TO_NUMBER (
                       CEIL (
                             AVG1.AVG_DURATION / 60
                           - ((FLOOR ((AVG1.AVG_DURATION / 60) / 60)) * 60))) =
                   60)
             THEN
                    (TO_NUMBER (FLOOR ((AVG1.AVG_DURATION / 60) / 60)) + 1)
                 || 'hh : '
                 || 0
                 || 'mi'
             ELSE
                    (FLOOR ((AVG1.AVG_DURATION / 60) / 60))
                 || 'hh : '
                 || CEIL (
                          AVG1.AVG_DURATION / 60
                        - ((FLOOR ((AVG1.AVG_DURATION / 60) / 60)) * 60))
                 || 'mi'
         END
             AS AVG_DUR,
         CASE WHEN TRIM (ugr.STATUS) = 'E' THEN '' ELSE LAGG.SESSIONS END
             AS SESSIONS,
         CASE WHEN TRIM (ugr.STATUS) = 'E' THEN '' ELSE LAGG.SESS_DUR END
             AS SESS_DUR,
         SD_AVG_ugr.SD_AVG,
         NVL (ugr.ERROR_MESSAGE, '')
             ERROR_MESSAGE
    FROM ugr
         LEFT JOIN
         (  SELECT LPI_STEP_LOG.I_LP_INST,
                   LISTAGG (SESS1.SESS_NAME, '; ')
                       WITHIN GROUP (ORDER BY LPI_STEP_LOG.I_LP_INST)
                       SESSIONS,
                   (   CEIL (
                           ROUND ((SYSDATE - MIN (SESS1.SESS_BEG)) * 24 * 60, 2))
                    || ' mi')
                       AS SESS_DUR
              FROM ODI_REPO_NAME.SNP_LPI_STEP_LOG LPI_STEP_LOG,
                   ODI_REPO_NAME.SNP_SESSION   SESS1
             WHERE     1 = 1
                   AND SESS1.SESS_NO = LPI_STEP_LOG.SESS_NO       --INNER_JOIN
                   AND SESS1.SESS_STATUS = 'R'
          GROUP BY LPI_STEP_LOG.I_LP_INST) LAGG
             ON ugr.I_LP_INST = LAGG.I_LP_INST
         LEFT JOIN
         (  SELECT DISTINCT
                   -- we can get more than one error at the same time in a parallel
                   -- so we added "distinct"
                   LPI_STEP_LOG.I_LP_INST,
                   SESS1.SESS_STATUS,
                   MAX (SESS1.SESS_BEG)     MAXX
              FROM ODI_REPO_NAME.SNP_LPI_STEP_LOG LPI_STEP_LOG,
                   ODI_REPO_NAME.SNP_SESSION   SESS1
             WHERE     1 = 1
                   AND SESS1.SESS_NO = LPI_STEP_LOG.SESS_NO       --INNER_JOIN
                   AND SESS1.SESS_STATUS = 'E'
          GROUP BY LPI_STEP_LOG.I_LP_INST, SESS1.SESS_STATUS) LAGG2
             ON ugr.I_LP_INST = LAGG2.I_LP_INST
         LEFT JOIN
         (SELECT SD_AVG1.I_LP_INST, SD_AVG1.SD_AVG
            FROM (  SELECT SESSIONS_R.I_LP_INST,
                           SESSIONS_R.SESS_NAME,
                           (   CEIL (ROUND (AVG (SESSIONS_D.SESS_DUR / 60), 0))
                            || ' mi')
                               SD_AVG,
                           ROW_NUMBER ()
                               OVER (PARTITION BY SESSIONS_R.I_LP_INST
                                     ORDER BY AVG (SESSIONS_D.SESS_DUR) DESC)
                               RN_MAX
                      FROM ODI_REPO_NAME.SNP_SESSION SESSIONS_D,
                           (SELECT LPI_STEP_LOG.I_LP_INST,
                                   LPI_STEP_LOG.I_LP_STEP,
                                   SESS1.SESS_NAME,
                                   SESS1.CONTEXT_CODE
                              FROM ODI_REPO_NAME.SNP_LPI_STEP_LOG LPI_STEP_LOG,
                                   ODI_REPO_NAME.SNP_SESSION   SESS1
                             WHERE     1 = 1
                                   AND SESS1.SESS_NO = LPI_STEP_LOG.SESS_NO --INNER_JOIN
                                   AND SESS1.SESS_STATUS = 'R') SESSIONS_R
                     WHERE     1 = 1
                           AND SESSIONS_D.SESS_NAME = SESSIONS_R.SESS_NAME
                           AND SESSIONS_D.CONTEXT_CODE = SESSIONS_R.CONTEXT_CODE
                           AND SESSIONS_D.SESS_STATUS = 'D'
                  GROUP BY SESSIONS_R.I_LP_INST, SESSIONS_R.SESS_NAME) SD_AVG1
           WHERE 1 = 1 AND SD_AVG1.RN_MAX = 1) SD_AVG_ugr
             ON ugr.I_LP_INST = SD_AVG_ugr.I_LP_INST
         LEFT JOIN
         (  SELECT ugr.CONTEXT_CODE,
                   ugr.LOAD_PLAN_NAME,
                   (SUM (ugr.DURATION) / COUNT (DISTINCT (ugr.I_LP_INST)))
                       AVG_DURATION
              FROM ugr
             WHERE 1 = 1 AND ugr.STATUS = 'D'
          GROUP BY ugr.LOAD_PLAN_NAME, ugr.CONTEXT_CODE) AVG1 --Average of complete Load Plans
             ON     AVG1.LOAD_PLAN_NAME = ugr.LOAD_PLAN_NAME
                AND AVG1.CONTEXT_CODE = ugr.CONTEXT_CODE
   WHERE     1 = 1
         AND ugr.RN_DUMMY = 1
         AND TO_NUMBER (TO_CHAR (ugr.START_DATE, 'YYYYMMDDHH24')) >=
             TO_NUMBER (TO_CHAR (SYSDATE - 1, 'YYYYMMDD') || '23') -- changable
ORDER BY ugr.START_DATE
