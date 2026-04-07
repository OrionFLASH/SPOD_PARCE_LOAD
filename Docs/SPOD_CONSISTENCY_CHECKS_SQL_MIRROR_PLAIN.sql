WITH
dim_group_raw AS (
    SELECT CONTEST_CODE, GROUP_CODE, GROUP_VALUE FROM spod_dq.t_group
),
dim_group_contest_code AS (
    SELECT DISTINCT CONTEST_CODE FROM dim_group_raw
),
dim_group_contest_group_pair AS (
    SELECT DISTINCT CONTEST_CODE, GROUP_CODE FROM dim_group_raw
),
dim_reward_link_raw AS (
    SELECT CONTEST_CODE, GROUP_CODE, REWARD_CODE FROM spod_dq.t_reward_link
),
dim_reward_link_reward_code AS (
    SELECT DISTINCT REWARD_CODE FROM dim_reward_link_raw
),
dim_reward_link_contest_group_pair AS (
    SELECT DISTINCT CONTEST_CODE, GROUP_CODE FROM dim_reward_link_raw
),
dim_contest_code AS (
    SELECT DISTINCT CONTEST_CODE FROM spod_dq.t_contest_data
),
dim_indicator_contest_code AS (
    SELECT DISTINCT CONTEST_CODE FROM spod_dq.t_indicator
),
dim_reward_code AS (
    SELECT DISTINCT REWARD_CODE FROM spod_dq.t_reward
),
dim_schedule_contest_tournament AS (
    SELECT CONTEST_CODE, TOURNAMENT_CODE FROM spod_dq.t_tournament_schedule
),
dim_schedule_tournament_contest_pair AS (
    SELECT DISTINCT TOURNAMENT_CODE, CONTEST_CODE FROM dim_schedule_contest_tournament
),
base_schedule_ref AS (
    SELECT
        s.CONTEST_CODE,
        cd.CONTEST_CODE AS ref_contest_data,
        ind.CONTEST_CODE AS ref_indicator,
        grp.CONTEST_CODE AS ref_group
    FROM dim_schedule_contest_tournament s
    LEFT JOIN dim_contest_code cd
        ON cd.CONTEST_CODE = s.CONTEST_CODE
    LEFT JOIN dim_indicator_contest_code ind
        ON ind.CONTEST_CODE = s.CONTEST_CODE
    LEFT JOIN dim_group_contest_code grp
        ON grp.CONTEST_CODE = s.CONTEST_CODE
),

v_ref_1_1 AS (
    SELECT
        CAST(g.CONTEST_CODE AS STRING) AS detail_key,
        'GROUP.CONTEST_CODE отсутствует в CONTEST-DATA' AS detail_message
    FROM dim_group_raw g
    LEFT JOIN dim_contest_code c
        ON c.CONTEST_CODE = g.CONTEST_CODE
    WHERE g.CONTEST_CODE IS NOT NULL
      AND TRIM(CAST(g.CONTEST_CODE AS STRING)) <> ''
      AND c.CONTEST_CODE IS NULL
),

v_ref_1_2 AS (
    SELECT
        CAST(i.CONTEST_CODE AS STRING) AS detail_key,
        'INDICATOR.CONTEST_CODE отсутствует в CONTEST-DATA' AS detail_message
    FROM spod_dq.t_indicator i
    LEFT JOIN dim_contest_code c
        ON c.CONTEST_CODE = i.CONTEST_CODE
    WHERE i.CONTEST_CODE IS NOT NULL
      AND TRIM(CAST(i.CONTEST_CODE AS STRING)) <> ''
      AND c.CONTEST_CODE IS NULL
),

v_ref_1_3 AS (
    SELECT
        CAST(rl.CONTEST_CODE AS STRING) AS detail_key,
        'REWARD-LINK.CONTEST_CODE отсутствует в CONTEST-DATA' AS detail_message
    FROM dim_reward_link_raw rl
    LEFT JOIN dim_contest_code c
        ON c.CONTEST_CODE = rl.CONTEST_CODE
    WHERE rl.CONTEST_CODE IS NOT NULL
      AND TRIM(CAST(rl.CONTEST_CODE AS STRING)) <> ''
      AND c.CONTEST_CODE IS NULL
),

v_ref_2 AS (
    SELECT
        CAST(rl.REWARD_CODE AS STRING) AS detail_key,
        'REWARD-LINK.REWARD_CODE отсутствует в REWARD' AS detail_message
    FROM dim_reward_link_raw rl
    LEFT JOIN dim_reward_code r
        ON r.REWARD_CODE = rl.REWARD_CODE
    WHERE rl.REWARD_CODE IS NOT NULL
      AND TRIM(CAST(rl.REWARD_CODE AS STRING)) <> ''
      AND r.REWARD_CODE IS NULL
),

v_ref_9 AS (
    SELECT
        CAST(e.ORG_UNIT_CODE AS STRING) AS detail_key,
        'EMPLOYEE.ORG_UNIT_CODE отсутствует в ORG_UNIT_V20' AS detail_message
    FROM spod_dq.t_employee e
    LEFT JOIN spod_dq.t_org_unit_v20 o
        ON o.ORG_UNIT_CODE = e.ORG_UNIT_CODE
    WHERE e.ORG_UNIT_CODE IS NOT NULL
      AND TRIM(CAST(e.ORG_UNIT_CODE AS STRING)) <> ''
      AND o.ORG_UNIT_CODE IS NULL
),

v_ref_scenario_1 AS (
    SELECT
        CAST(b.CONTEST_CODE AS STRING) AS detail_key,
        'TOURNAMENT-SCHEDULE.CONTEST_CODE отсутствует в CONTEST-DATA' AS detail_message
    FROM base_schedule_ref b
    WHERE b.CONTEST_CODE IS NOT NULL
      AND TRIM(CAST(b.CONTEST_CODE AS STRING)) <> ''
      AND b.ref_contest_data IS NULL
),

v_ref_scenario_16 AS (
    SELECT
        CAST(b.CONTEST_CODE AS STRING) AS detail_key,
        'TOURNAMENT-SCHEDULE.CONTEST_CODE отсутствует в INDICATOR' AS detail_message
    FROM base_schedule_ref b
    WHERE b.CONTEST_CODE IS NOT NULL
      AND TRIM(CAST(b.CONTEST_CODE AS STRING)) <> ''
      AND b.ref_indicator IS NULL
),

v_ref_scenario_20 AS (
    SELECT
        CAST(b.CONTEST_CODE AS STRING) AS detail_key,
        'TOURNAMENT-SCHEDULE.CONTEST_CODE отсутствует в GROUP' AS detail_message
    FROM base_schedule_ref b
    WHERE b.CONTEST_CODE IS NOT NULL
      AND TRIM(CAST(b.CONTEST_CODE AS STRING)) <> ''
      AND b.ref_group IS NULL
),

v_ref_contest_data_group AS (
    SELECT
        CAST(c.CONTEST_CODE AS STRING) AS detail_key,
        'CONTEST-DATA.CONTEST_CODE отсутствует в GROUP' AS detail_message
    FROM spod_dq.t_contest_data c
    LEFT JOIN dim_group_contest_code g
        ON g.CONTEST_CODE = c.CONTEST_CODE
    WHERE c.CONTEST_CODE IS NOT NULL
      AND TRIM(CAST(c.CONTEST_CODE AS STRING)) <> ''
      AND g.CONTEST_CODE IS NULL
),

v_ref_indicator_group AS (
    SELECT
        CAST(i.CONTEST_CODE AS STRING) AS detail_key,
        'INDICATOR.CONTEST_CODE отсутствует в GROUP' AS detail_message
    FROM spod_dq.t_indicator i
    LEFT JOIN dim_group_contest_code g
        ON g.CONTEST_CODE = i.CONTEST_CODE
    WHERE i.CONTEST_CODE IS NOT NULL
      AND TRIM(CAST(i.CONTEST_CODE AS STRING)) <> ''
      AND g.CONTEST_CODE IS NULL
),

v_ref_report_contest_data AS (
    SELECT
        CAST(r.CONTEST_CODE AS STRING) AS detail_key,
        'REPORT.CONTEST_CODE отсутствует в CONTEST-DATA' AS detail_message
    FROM spod_dq.t_report r
    LEFT JOIN dim_contest_code c
        ON c.CONTEST_CODE = r.CONTEST_CODE
    WHERE r.CONTEST_CODE IS NOT NULL
      AND TRIM(CAST(r.CONTEST_CODE AS STRING)) <> ''
      AND c.CONTEST_CODE IS NULL
),

v_ref_reward_reward_link AS (
    SELECT
        CAST(rw.REWARD_CODE AS STRING) AS detail_key,
        'REWARD.REWARD_CODE отсутствует в REWARD-LINK' AS detail_message
    FROM spod_dq.t_reward rw
    LEFT JOIN dim_reward_link_reward_code rl
        ON rl.REWARD_CODE = rw.REWARD_CODE
    WHERE rw.REWARD_CODE IS NOT NULL
      AND TRIM(CAST(rw.REWARD_CODE AS STRING)) <> ''
      AND rl.REWARD_CODE IS NULL
),

v_comp_5 AS (
    SELECT
        CONCAT_WS('|', CAST(rl.CONTEST_CODE AS STRING), CAST(rl.GROUP_CODE AS STRING)) AS detail_key,
        'Пара CONTEST_CODE+GROUP_CODE из REWARD-LINK отсутствует в GROUP' AS detail_message
    FROM dim_reward_link_raw rl
    LEFT JOIN dim_group_contest_group_pair g
        ON g.CONTEST_CODE = rl.CONTEST_CODE
       AND g.GROUP_CODE = rl.GROUP_CODE
    WHERE (rl.CONTEST_CODE IS NOT NULL AND TRIM(CAST(rl.CONTEST_CODE AS STRING)) <> '')
      AND (rl.GROUP_CODE IS NOT NULL AND TRIM(CAST(rl.GROUP_CODE AS STRING)) <> '')
      AND g.CONTEST_CODE IS NULL
),

v_comp_grp_rl AS (
    SELECT
        CONCAT_WS('|', CAST(g.CONTEST_CODE AS STRING), CAST(g.GROUP_CODE AS STRING)) AS detail_key,
        'Пара из GROUP отсутствует в REWARD-LINK' AS detail_message
    FROM dim_group_raw g
    LEFT JOIN dim_reward_link_contest_group_pair rl
        ON rl.CONTEST_CODE = g.CONTEST_CODE
       AND rl.GROUP_CODE = g.GROUP_CODE
    WHERE (g.CONTEST_CODE IS NOT NULL AND TRIM(CAST(g.CONTEST_CODE AS STRING)) <> '')
      AND (g.GROUP_CODE IS NOT NULL AND TRIM(CAST(g.GROUP_CODE AS STRING)) <> '')
      AND rl.CONTEST_CODE IS NULL
),

v_comp_rep_sch AS (
    SELECT
        CONCAT_WS('|', CAST(r.TOURNAMENT_CODE AS STRING), CAST(r.CONTEST_CODE AS STRING)) AS detail_key,
        'Пара из REPORT отсутствует в TOURNAMENT-SCHEDULE' AS detail_message
    FROM spod_dq.t_report r
    LEFT JOIN dim_schedule_tournament_contest_pair s
        ON s.TOURNAMENT_CODE = r.TOURNAMENT_CODE
       AND s.CONTEST_CODE = r.CONTEST_CODE
    WHERE (r.TOURNAMENT_CODE IS NOT NULL AND TRIM(CAST(r.TOURNAMENT_CODE AS STRING)) <> '')
      AND (r.CONTEST_CODE IS NOT NULL AND TRIM(CAST(r.CONTEST_CODE AS STRING)) <> '')
      AND s.TOURNAMENT_CODE IS NULL
),

v_uq_3 AS (
    SELECT
        CONCAT_WS('|', CAST(x.CONTEST_CODE AS STRING), CAST(x.GROUP_CODE AS STRING), CAST(x.GROUP_VALUE AS STRING)) AS detail_key,
        CONCAT('Дубликат по (CONTEST_CODE, GROUP_CODE, GROUP_VALUE): строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT CONTEST_CODE, GROUP_CODE, GROUP_VALUE, COUNT(*) AS cnt
        FROM dim_group_raw
        GROUP BY CONTEST_CODE, GROUP_CODE, GROUP_VALUE
        HAVING COUNT(*) > 1
    ) x
),

v_uq_4 AS (
    SELECT
        CONCAT_WS('|', CAST(x.CONTEST_CODE AS STRING), CAST(x.GROUP_CODE AS STRING), CAST(x.REWARD_CODE AS STRING)) AS detail_key,
        CONCAT('Дубликат по (CONTEST_CODE, GROUP_CODE, REWARD_CODE): строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT CONTEST_CODE, GROUP_CODE, REWARD_CODE, COUNT(*) AS cnt
        FROM dim_reward_link_raw
        GROUP BY CONTEST_CODE, GROUP_CODE, REWARD_CODE
        HAVING COUNT(*) > 1
    ) x
),

v_uq_contest_data AS (
    SELECT
        CAST(x.CONTEST_CODE AS STRING) AS detail_key,
        CONCAT('Дубликат CONTEST_CODE: строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT CONTEST_CODE, COUNT(*) AS cnt
        FROM spod_dq.t_contest_data
        GROUP BY CONTEST_CODE
        HAVING COUNT(*) > 1
    ) x
),

v_uq_ind1 AS (
    SELECT
        CONCAT_WS('|', CAST(x.CONTEST_CODE AS STRING), CAST(x.INDICATOR_ADD_CALC_TYPE AS STRING), CAST(x.INDICATOR_CODE AS STRING)) AS detail_key,
        CONCAT('Дубликат ключа индикатора: строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT CONTEST_CODE, INDICATOR_ADD_CALC_TYPE, INDICATOR_CODE, COUNT(*) AS cnt
        FROM spod_dq.t_indicator
        GROUP BY CONTEST_CODE, INDICATOR_ADD_CALC_TYPE, INDICATOR_CODE
        HAVING COUNT(*) > 1
    ) x
),

v_uq_ind_n AS (
    SELECT
        CAST(x.N AS STRING) AS detail_key,
        CONCAT('Дубликат N: строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT N, COUNT(*) AS cnt
        FROM spod_dq.t_indicator
        GROUP BY N
        HAVING COUNT(*) > 1
    ) x
),

v_uq_report AS (
    SELECT
        CONCAT_WS('|', CAST(x.MANAGER_PERSON_NUMBER AS STRING), CAST(x.TOURNAMENT_CODE AS STRING), CAST(x.CONTEST_CODE AS STRING)) AS detail_key,
        CONCAT('Дубликат ключа отчёта: строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT MANAGER_PERSON_NUMBER, TOURNAMENT_CODE, CONTEST_CODE, COUNT(*) AS cnt
        FROM spod_dq.t_report
        GROUP BY MANAGER_PERSON_NUMBER, TOURNAMENT_CODE, CONTEST_CODE
        HAVING COUNT(*) > 1
    ) x
),

v_uq_reward AS (
    SELECT
        CAST(x.REWARD_CODE AS STRING) AS detail_key,
        CONCAT('Дубликат REWARD_CODE: строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT REWARD_CODE, COUNT(*) AS cnt
        FROM spod_dq.t_reward
        GROUP BY REWARD_CODE
        HAVING COUNT(*) > 1
    ) x
),

v_uq_rl2 AS (
    SELECT
        CONCAT_WS('|', CAST(x.CONTEST_CODE AS STRING), CAST(x.REWARD_CODE AS STRING)) AS detail_key,
        CONCAT('Дубликат (CONTEST_CODE, REWARD_CODE): строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT CONTEST_CODE, REWARD_CODE, COUNT(*) AS cnt
        FROM dim_reward_link_raw
        GROUP BY CONTEST_CODE, REWARD_CODE
        HAVING COUNT(*) > 1
    ) x
),

v_uq_rl_r AS (
    SELECT
        CAST(x.REWARD_CODE AS STRING) AS detail_key,
        CONCAT('Дубликат REWARD_CODE в REWARD-LINK: строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT REWARD_CODE, COUNT(*) AS cnt
        FROM dim_reward_link_raw
        GROUP BY REWARD_CODE
        HAVING COUNT(*) > 1
    ) x
),

v_uq_sch2 AS (
    SELECT
        CONCAT_WS('|', CAST(x.TOURNAMENT_CODE AS STRING), CAST(x.CONTEST_CODE AS STRING)) AS detail_key,
        CONCAT('Дубликат (TOURNAMENT_CODE, CONTEST_CODE): строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT TOURNAMENT_CODE, CONTEST_CODE, COUNT(*) AS cnt
        FROM dim_schedule_contest_tournament
        GROUP BY TOURNAMENT_CODE, CONTEST_CODE
        HAVING COUNT(*) > 1
    ) x
),

v_uq_sch1 AS (
    SELECT
        CAST(x.TOURNAMENT_CODE AS STRING) AS detail_key,
        CONCAT('Дубликат TOURNAMENT_CODE: строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT TOURNAMENT_CODE, COUNT(*) AS cnt
        FROM dim_schedule_contest_tournament
        GROUP BY TOURNAMENT_CODE
        HAVING COUNT(*) > 1
    ) x
),

v_uq_org AS (
    SELECT
        CAST(x.ORG_UNIT_CODE AS STRING) AS detail_key,
        CONCAT('Дубликат ORG_UNIT_CODE: строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT ORG_UNIT_CODE, COUNT(*) AS cnt
        FROM spod_dq.t_org_unit_v20
        GROUP BY ORG_UNIT_CODE
        HAVING COUNT(*) > 1
    ) x
),

v_uq_tb_gosb AS (
    SELECT
        CONCAT_WS('|', CAST(x.TB_CODE AS STRING), CAST(x.GOSB_CODE AS STRING)) AS detail_key,
        CONCAT('Дубликат (TB_CODE, GOSB_CODE): строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT TB_CODE, GOSB_CODE, COUNT(*) AS cnt
        FROM spod_dq.t_org_unit_v20
        GROUP BY TB_CODE, GOSB_CODE
        HAVING COUNT(*) > 1
    ) x
),

v_uq_ur AS (
    SELECT
        CAST(x.RULE_NUM AS STRING) AS detail_key,
        CONCAT('Дубликат RULE_NUM: строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT RULE_NUM, COUNT(*) AS cnt
        FROM spod_dq.t_user_role
        GROUP BY RULE_NUM
        HAVING COUNT(*) > 1
    ) x
),

v_uq_ursb AS (
    SELECT
        CAST(x.RULE_NUM AS STRING) AS detail_key,
        CONCAT('Дубликат RULE_NUM (SB): строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT RULE_NUM, COUNT(*) AS cnt
        FROM spod_dq.t_user_role_sb
        GROUP BY RULE_NUM
        HAVING COUNT(*) > 1
    ) x
),

v_uq_emp_p AS (
    SELECT
        CAST(x.PERSON_NUMBER AS STRING) AS detail_key,
        CONCAT('Дубликат PERSON_NUMBER: строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT PERSON_NUMBER, COUNT(*) AS cnt
        FROM spod_dq.t_employee
        GROUP BY PERSON_NUMBER
        HAVING COUNT(*) > 1
    ) x
),

v_uq_emp_pa AS (
    SELECT
        CAST(x.PERSON_NUMBER_ADD AS STRING) AS detail_key,
        CONCAT('Дубликат PERSON_NUMBER_ADD: строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT PERSON_NUMBER_ADD, COUNT(*) AS cnt
        FROM spod_dq.t_employee
        GROUP BY PERSON_NUMBER_ADD
        HAVING COUNT(*) > 1
    ) x
),

v_uq_emp_kpk AS (
    SELECT
        CONCAT_WS('|', CAST(x.POSITION_NAME AS STRING), CAST(x.KPK_CODE AS STRING), CAST(x.ORG_UNIT_CODE AS STRING)) AS detail_key,
        CONCAT('Дубликат (КПК, KPK_CODE, ORG_UNIT_CODE): строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT POSITION_NAME, KPK_CODE, ORG_UNIT_CODE, COUNT(*) AS cnt
        FROM spod_dq.t_employee
        WHERE POSITION_NAME = 'КПК'
          AND KPK_CODE IS NOT NULL
          AND TRIM(CAST(KPK_CODE AS STRING)) NOT IN ('', '-')
        GROUP BY POSITION_NAME, KPK_CODE, ORG_UNIT_CODE
        HAVING COUNT(*) > 1
    ) x
),

v_fl_org AS (
    SELECT
        CONCAT_WS(':', CAST(ORG_UNIT_CODE AS STRING), 'TB_FULL_NAME') AS detail_key,
        CONCAT('Длина TB_FULL_NAME=', CAST(LENGTH(CAST(TB_FULL_NAME AS STRING)) AS STRING), ' > 100') AS detail_message
    FROM spod_dq.t_org_unit_v20
    WHERE LENGTH(CAST(TB_FULL_NAME AS STRING)) > 100
    UNION ALL
    SELECT
        CONCAT_WS(':', CAST(ORG_UNIT_CODE AS STRING), 'GOSB_NAME'),
        CONCAT('Длина GOSB_NAME=', CAST(LENGTH(CAST(GOSB_NAME AS STRING)) AS STRING), ' > 100')
    FROM spod_dq.t_org_unit_v20
    WHERE LENGTH(CAST(GOSB_NAME AS STRING)) > 100
    UNION ALL
    SELECT
        CONCAT_WS(':', CAST(ORG_UNIT_CODE AS STRING), 'GOSB_SHORT_NAME'),
        CONCAT('Длина GOSB_SHORT_NAME=', CAST(LENGTH(CAST(GOSB_SHORT_NAME AS STRING)) AS STRING), ' > 20')
    FROM spod_dq.t_org_unit_v20
    WHERE LENGTH(CAST(GOSB_SHORT_NAME AS STRING)) > 20
),

v_fl_emp AS (
    SELECT
        CONCAT('PERSON_NUMBER=', CAST(PERSON_NUMBER AS STRING)) AS detail_key,
        CONCAT('Длина PERSON_NUMBER=', CAST(LENGTH(CAST(PERSON_NUMBER AS STRING)) AS STRING), ' (ожидается 20)') AS detail_message
    FROM spod_dq.t_employee
    WHERE LENGTH(CAST(PERSON_NUMBER AS STRING)) <> 20
    UNION ALL
    SELECT
        CONCAT('PERSON_NUMBER_ADD=', CAST(PERSON_NUMBER_ADD AS STRING)),
        CONCAT('Длина PERSON_NUMBER_ADD=', CAST(LENGTH(CAST(PERSON_NUMBER_ADD AS STRING)) AS STRING), ' (ожидается 20)')
    FROM spod_dq.t_employee
    WHERE LENGTH(CAST(PERSON_NUMBER_ADD AS STRING)) <> 20
),

v_fl_rep AS (
    SELECT
        CONCAT_WS('|', CAST(MANAGER_PERSON_NUMBER AS STRING), CAST(TOURNAMENT_CODE AS STRING), CAST(CONTEST_CODE AS STRING)) AS detail_key,
        CONCAT('Длина MANAGER_PERSON_NUMBER=', CAST(LENGTH(CAST(MANAGER_PERSON_NUMBER AS STRING)) AS STRING), ' (ожидается 20)') AS detail_message
    FROM spod_dq.t_report
    WHERE LENGTH(CAST(MANAGER_PERSON_NUMBER AS STRING)) <> 20
),

chk_summary AS (
    SELECT (SELECT COUNT(*) FROM v_ref_1_1) AS violation_count
    UNION ALL SELECT (SELECT COUNT(*) FROM v_ref_1_2)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_ref_1_3)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_ref_2)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_ref_9)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_ref_scenario_1)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_ref_scenario_16)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_ref_scenario_20)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_ref_contest_data_group)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_ref_indicator_group)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_ref_report_contest_data)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_ref_reward_reward_link)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_comp_5)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_comp_grp_rl)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_comp_rep_sch)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_uq_3)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_uq_4)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_uq_contest_data)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_uq_ind1)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_uq_ind_n)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_uq_report)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_uq_reward)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_uq_rl2)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_uq_rl_r)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_uq_sch2)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_uq_sch1)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_uq_org)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_uq_tb_gosb)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_uq_ur)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_uq_ursb)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_uq_emp_p)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_uq_emp_pa)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_uq_emp_kpk)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_fl_org)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_fl_emp)
    UNION ALL SELECT (SELECT COUNT(*) FROM v_fl_rep)
),

chk_detail AS (
    SELECT detail_key, detail_message FROM v_ref_1_1
    UNION ALL SELECT detail_key, detail_message FROM v_ref_1_2
    UNION ALL SELECT detail_key, detail_message FROM v_ref_1_3
    UNION ALL SELECT detail_key, detail_message FROM v_ref_2
    UNION ALL SELECT detail_key, detail_message FROM v_ref_9
    UNION ALL SELECT detail_key, detail_message FROM v_ref_scenario_1
    UNION ALL SELECT detail_key, detail_message FROM v_ref_scenario_16
    UNION ALL SELECT detail_key, detail_message FROM v_ref_scenario_20
    UNION ALL SELECT detail_key, detail_message FROM v_ref_contest_data_group
    UNION ALL SELECT detail_key, detail_message FROM v_ref_indicator_group
    UNION ALL SELECT detail_key, detail_message FROM v_ref_report_contest_data
    UNION ALL SELECT detail_key, detail_message FROM v_ref_reward_reward_link
    UNION ALL SELECT detail_key, detail_message FROM v_comp_5
    UNION ALL SELECT detail_key, detail_message FROM v_comp_grp_rl
    UNION ALL SELECT detail_key, detail_message FROM v_comp_rep_sch
    UNION ALL SELECT detail_key, detail_message FROM v_uq_3
    UNION ALL SELECT detail_key, detail_message FROM v_uq_4
    UNION ALL SELECT detail_key, detail_message FROM v_uq_contest_data
    UNION ALL SELECT detail_key, detail_message FROM v_uq_ind1
    UNION ALL SELECT detail_key, detail_message FROM v_uq_ind_n
    UNION ALL SELECT detail_key, detail_message FROM v_uq_report
    UNION ALL SELECT detail_key, detail_message FROM v_uq_reward
    UNION ALL SELECT detail_key, detail_message FROM v_uq_rl2
    UNION ALL SELECT detail_key, detail_message FROM v_uq_rl_r
    UNION ALL SELECT detail_key, detail_message FROM v_uq_sch2
    UNION ALL SELECT detail_key, detail_message FROM v_uq_sch1
    UNION ALL SELECT detail_key, detail_message FROM v_uq_org
    UNION ALL SELECT detail_key, detail_message FROM v_uq_tb_gosb
    UNION ALL SELECT detail_key, detail_message FROM v_uq_ur
    UNION ALL SELECT detail_key, detail_message FROM v_uq_ursb
    UNION ALL SELECT detail_key, detail_message FROM v_uq_emp_p
    UNION ALL SELECT detail_key, detail_message FROM v_uq_emp_pa
    UNION ALL SELECT detail_key, detail_message FROM v_uq_emp_kpk
    UNION ALL SELECT detail_key, detail_message FROM v_fl_org
    UNION ALL SELECT detail_key, detail_message FROM v_fl_emp
    UNION ALL SELECT detail_key, detail_message FROM v_fl_rep
)

SELECT
    1 AS result_order,
    CAST('SUMMARY' AS STRING) AS report_section,
    CAST(CASE WHEN s.violation_count = 0 THEN 1 ELSE 0 END AS BIGINT) AS passed,
    CAST(s.violation_count AS BIGINT) AS violation_count,
    CAST(NULL AS STRING) AS detail_key,
    CAST(NULL AS STRING) AS detail_message
FROM chk_summary s

UNION ALL

SELECT
    2 AS result_order,
    CAST('DETAIL' AS STRING) AS report_section,
    CAST(NULL AS BIGINT) AS passed,
    CAST(NULL AS BIGINT) AS violation_count,
    d.detail_key,
    d.detail_message
FROM chk_detail d

ORDER BY result_order, detail_key
;
