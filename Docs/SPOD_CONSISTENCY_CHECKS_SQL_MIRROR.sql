-- =============================================================================
-- SPOD_PROM: зеркало проверок консистентности в SQL (referential, composite,
--            unique с простым scope, field_length, field_format).
--
-- Назначение: отдельный файл, не подключается к Python-пайплайну. Сверка с
--   правилами из config.json → consistency_checks.rules (типы без JSON).
--
-- Диалект ориентир: Hive / Spark SQL (RLIKE, CONCAT_WS). Для PostgreSQL замените:
--   RLIKE  → ~  (regex match)
--   CONCAT_WS(x,a,b) → a || x || b или concat_ws
--
-- ВАЖНО — ЗАМЕНИТЕ под вашу БД:
--   1) Схему: глобально замените `spod_dq` на ваш каталог/схему.
--   2) Имена таблиц в блоке «СООТВЕТСТВИЕ ЛИСТ EXCEL → ТАБЛИЦА».
--
-- Формат результата (один запрос в конце файла):
--   report_section = 'SUMMARY' — по одной строке на проверку:
--     passed = 1 если нарушений нет, 0 если есть (как в примере game_dq: ok/consistent);
--     violation_count — число строк/ключей с нарушением.
--   report_section = 'DETAIL' — только нарушения: check_id, detail_key, detail_message.
--   Сначала идут все строки SUMMARY (result_order=1), затем все DETAIL (result_order=2).
--   Чтобы взять только сводку: WHERE report_section = 'SUMMARY'.
--   Чтобы взять только детали: WHERE report_section = 'DETAIL'.
--
-- Ограничения (в SQL намеренно НЕ переносятся — остаются в программе):
--   json_field_equals_column, json_field_in_column, json_priority_unique_per_contest_link,
--   csv_columns_count по сырому CSV.
--
-- -----------------------------------------------------------------------------
-- Как устроена работа скрипта (один запрос от ключевого слова WITH до «;» в конце)
-- -----------------------------------------------------------------------------
-- 1) Выполнение в СУБД: вы отдаёте движку (Hive/Spark SQL и т.п.) целиком один
--    оператор SELECT с общим блоком WITH — это не «несколько команд», а одна
--    логическая команда, которая строит временные именованные подзапросы (CTE).
--
-- 2) Сначала вычисляются все CTE вида v_* — в каждом только строки-НАРУШИТЕЛИ
--    (или пустой набор, если проверка пройдена). Формат строки везде одинаковый:
--    check_id, check_type, detail_key (краткий идентификатор проблемы),
--    detail_message (текст для человека).
--
-- 3) CTE chk_summary не читает таблицы заново: для каждой проверки берётся
--    COUNT(*) из соответствующего v_* — сколько записей с нарушением. Отсюда
--    violation_count и флаг passed (1 = нарушений нет, 0 = есть).
--
-- 4) CTE chk_detail склеивает (UNION ALL) все v_* в один длинный список — это
--    все детальные строки по всем проверкам сразу (удобно фильтровать по check_id).
--
-- 5) Внешний SELECT после chk_detail объединяет две «плоскости» результата:
--    строки SUMMARY (сводка) и строки DETAIL (детали) через UNION ALL; колонки
--    выровнены типами (где не применимо — NULL). ORDER BY сначала выводит сводку,
--    потом детали; внутри секции — по check_id и detail_key.
--
-- 6) Типичный сценарий: сохранить результат во временную таблицу или смотреть
--    в IDE; для «только упавшие проверки» — фильтр report_section='SUMMARY'
--    AND passed=0.
--
-- -----------------------------------------------------------------------------
-- Соответствие исходному коду проекта SPOD_PROM
-- -----------------------------------------------------------------------------
-- Источник правил: config.json → объект "consistency_checks" → массив "rules".
--   Поле "id" у правила совпадает с литералом check_id в этом SQL (строка в кавычках).
--   Поле "name" — человекочитаемое название; ниже у каждого CTE оно процитировано в «ёлочках».
--   Поле "type" — тип обработчика (referential, referential_composite, unique, …).
--
-- Реализация в коде: src/consistency_checks.py
--   — run_all_consistency_checks() читает rules и по type вызывает проверки;
--   — referential / referential_composite — ссылочная целостность по листам;
--   — unique — _run_unique_check; field_length — _run_field_length_check;
--   — field_format — _run_field_format_check;
--   — типы json_field_*, json_priority_* в SQL не отражены (нужен разбор JSON в СУБД).
-- Сводный лист CONSISTENCY и колонки на листах строятся из тех же id/name (build_consistency_summary_df).
--
-- В этом файле нет правил с id: ref_contest_data_indicator, ref_group_indicator (в config enabled: false),
-- нет json/reward_add_data_*, reward_parent_*, reward_priority_*, csv_columns_count — см. комментарий в конце файла.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- ЗАМЕНА: каталог/схема, где лежат витрины, соответствующие листам выгрузки SPOD.
-- -----------------------------------------------------------------------------
-- Ниже везде: замените строку `spod_dq` на вашу схему (поиск/замена в редакторе).

-- =============================================================================
-- СООТВЕТСТВИЕ ЛИСТ EXCEL (config) → ИМЯ ТАБЛИЦЫ В ПРИМЕРЕ (ЗАМЕНИТЕ!)
-- =============================================================================
-- CONTEST-DATA          → spod_dq.t_contest_data
-- GROUP                 → spod_dq.t_group
-- INDICATOR             → spod_dq.t_indicator
-- REWARD-LINK           → spod_dq.t_reward_link
-- REWARD                → spod_dq.t_reward
-- TOURNAMENT-SCHEDULE   → spod_dq.t_tournament_schedule
-- ORG_UNIT_V20          → spod_dq.t_org_unit_v20
-- EMPLOYEE              → spod_dq.t_employee
-- REPORT                → spod_dq.t_report
-- USER_ROLE             → spod_dq.t_user_role
-- USER_ROLE SB          → spod_dq.t_user_role_sb
-- =============================================================================

-- #############################################################################
-- ЕДИНЫЙ ОТЧЁТ: сводка (passed 1/0) + детали нарушений в одном SELECT.
-- Логика проверок совпадает с прежними отдельными запросами (объединены в CTE).
-- #############################################################################

-- Ключевое слово WITH начинает цепочку CTE; запятая между CTE означает
-- «вычисли предыдущий псевдоним, затем следующий». Итоговый SELECT внизу файла
-- читает только chk_summary и chk_detail (они уже ссылаются на все v_*).

WITH
-- ---------------------------------------------------------------------------
-- A. REFERENTIAL — ссылочная целостность «значение в колонке A должно быть в справочнике B»
-- ---------------------------------------------------------------------------
-- Приём: LEFT JOIN фактовой таблицы с справочником по ключу; в WHERE оставляем
-- строки, где в справочнике нет совпадения (правая часть NULL), но слева значение
-- считается «заполненным» (не NULL и не пустая строка после TRIM). Каждая такая
-- строка — одно нарушение и попадёт в DETAIL; COUNT по этому набору — в SUMMARY.
-- У каждого CTE: первая строка комментария — config rules[].id и name; тип — для consistency_checks.py.

-- id "1.1" | name «Все CONTEST_CODE из GROUP существуют в CONTEST-DATE» | type referential
v_ref_1_1 AS (
    SELECT
        '1.1' AS check_id,
        'referential' AS check_type,
        CAST(g.CONTEST_CODE AS STRING) AS detail_key,
        'GROUP.CONTEST_CODE отсутствует в CONTEST-DATA' AS detail_message
    FROM spod_dq.t_group g
    LEFT JOIN spod_dq.t_contest_data c
        ON c.CONTEST_CODE = g.CONTEST_CODE
    WHERE g.CONTEST_CODE IS NOT NULL
      AND TRIM(CAST(g.CONTEST_CODE AS STRING)) <> ''
      AND c.CONTEST_CODE IS NULL
),

-- id "1.2" | name «Все CONTEST_CODE из INDICATOR существуют в CONTEST-DATE» | type referential
v_ref_1_2 AS (
    SELECT
        '1.2' AS check_id,
        'referential' AS check_type,
        CAST(i.CONTEST_CODE AS STRING) AS detail_key,
        'INDICATOR.CONTEST_CODE отсутствует в CONTEST-DATA' AS detail_message
    FROM spod_dq.t_indicator i
    LEFT JOIN spod_dq.t_contest_data c
        ON c.CONTEST_CODE = i.CONTEST_CODE
    WHERE i.CONTEST_CODE IS NOT NULL
      AND TRIM(CAST(i.CONTEST_CODE AS STRING)) <> ''
      AND c.CONTEST_CODE IS NULL
),

-- id "1.3" | name «Все CONTEST_CODE из REWARD-LINK существуют в CONTEST-DATE» | type referential
v_ref_1_3 AS (
    SELECT
        '1.3' AS check_id,
        'referential' AS check_type,
        CAST(rl.CONTEST_CODE AS STRING) AS detail_key,
        'REWARD-LINK.CONTEST_CODE отсутствует в CONTEST-DATA' AS detail_message
    FROM spod_dq.t_reward_link rl
    LEFT JOIN spod_dq.t_contest_data c
        ON c.CONTEST_CODE = rl.CONTEST_CODE
    WHERE rl.CONTEST_CODE IS NOT NULL
      AND TRIM(CAST(rl.CONTEST_CODE AS STRING)) <> ''
      AND c.CONTEST_CODE IS NULL
),

-- id "2" | name «Все REWARD_CODE из REWARD-LINK существуют в REWARD» | type referential
v_ref_2 AS (
    SELECT
        '2' AS check_id,
        'referential' AS check_type,
        CAST(rl.REWARD_CODE AS STRING) AS detail_key,
        'REWARD-LINK.REWARD_CODE отсутствует в REWARD' AS detail_message
    FROM spod_dq.t_reward_link rl
    LEFT JOIN spod_dq.t_reward r
        ON r.REWARD_CODE = rl.REWARD_CODE
    WHERE rl.REWARD_CODE IS NOT NULL
      AND TRIM(CAST(rl.REWARD_CODE AS STRING)) <> ''
      AND r.REWARD_CODE IS NULL
),

-- id "9" | name «Все ORG_UNIT_CODE из EMPLOYEE существуют в ORG_UNIT_V20» | type referential
v_ref_9 AS (
    SELECT
        '9' AS check_id,
        'referential' AS check_type,
        CAST(e.ORG_UNIT_CODE AS STRING) AS detail_key,
        'EMPLOYEE.ORG_UNIT_CODE отсутствует в ORG_UNIT_V20' AS detail_message
    FROM spod_dq.t_employee e
    LEFT JOIN spod_dq.t_org_unit_v20 o
        ON o.ORG_UNIT_CODE = e.ORG_UNIT_CODE
    WHERE e.ORG_UNIT_CODE IS NOT NULL
      AND TRIM(CAST(e.ORG_UNIT_CODE AS STRING)) <> ''
      AND o.ORG_UNIT_CODE IS NULL
),

-- Сценарные проверки: TOURNAMENT-SCHEDULE → разные справочники (как в config).

-- id "scenario_1" | name «Все CONTEST_CODE из TOURNAMENT-SHEDULE существуют в CONTEST-DATE» | type referential
v_ref_scenario_1 AS (
    SELECT
        'scenario_1' AS check_id,
        'referential' AS check_type,
        CAST(s.CONTEST_CODE AS STRING) AS detail_key,
        'TOURNAMENT-SCHEDULE.CONTEST_CODE отсутствует в CONTEST-DATA' AS detail_message
    FROM spod_dq.t_tournament_schedule s
    LEFT JOIN spod_dq.t_contest_data c
        ON c.CONTEST_CODE = s.CONTEST_CODE
    WHERE s.CONTEST_CODE IS NOT NULL
      AND TRIM(CAST(s.CONTEST_CODE AS STRING)) <> ''
      AND c.CONTEST_CODE IS NULL
),

-- id "scenario_16" | name «Все CONTEST_CODE из TOURNAMENT-SCHEDULE существуют в INDICATOR» | type referential
v_ref_scenario_16 AS (
    SELECT
        'scenario_16' AS check_id,
        'referential' AS check_type,
        CAST(s.CONTEST_CODE AS STRING) AS detail_key,
        'TOURNAMENT-SCHEDULE.CONTEST_CODE отсутствует в INDICATOR' AS detail_message
    FROM spod_dq.t_tournament_schedule s
    LEFT JOIN (
        SELECT DISTINCT CONTEST_CODE FROM spod_dq.t_indicator
    ) i ON i.CONTEST_CODE = s.CONTEST_CODE
    WHERE s.CONTEST_CODE IS NOT NULL
      AND TRIM(CAST(s.CONTEST_CODE AS STRING)) <> ''
      AND i.CONTEST_CODE IS NULL
),

-- id "scenario_20" | name «Все CONTEST_CODE из TOURNAMENT-SCHEDULE существуют в GROUP» | type referential
v_ref_scenario_20 AS (
    SELECT
        'scenario_20' AS check_id,
        'referential' AS check_type,
        CAST(s.CONTEST_CODE AS STRING) AS detail_key,
        'TOURNAMENT-SCHEDULE.CONTEST_CODE отсутствует в GROUP' AS detail_message
    FROM spod_dq.t_tournament_schedule s
    LEFT JOIN (
        SELECT DISTINCT CONTEST_CODE FROM spod_dq.t_group
    ) g ON g.CONTEST_CODE = s.CONTEST_CODE
    WHERE s.CONTEST_CODE IS NOT NULL
      AND TRIM(CAST(s.CONTEST_CODE AS STRING)) <> ''
      AND g.CONTEST_CODE IS NULL
),

-- id "ref_contest_data_group" | name «Все CONTEST_CODE из CONTEST-DATE существуют в GROUP» | type referential
v_ref_contest_data_group AS (
    SELECT
        'ref_contest_data_group' AS check_id,
        'referential' AS check_type,
        CAST(c.CONTEST_CODE AS STRING) AS detail_key,
        'CONTEST-DATA.CONTEST_CODE отсутствует в GROUP' AS detail_message
    FROM spod_dq.t_contest_data c
    LEFT JOIN (
        SELECT DISTINCT CONTEST_CODE FROM spod_dq.t_group
    ) g ON g.CONTEST_CODE = c.CONTEST_CODE
    WHERE c.CONTEST_CODE IS NOT NULL
      AND TRIM(CAST(c.CONTEST_CODE AS STRING)) <> ''
      AND g.CONTEST_CODE IS NULL
),

-- id "ref_indicator_group" | name «Все CONTEST_CODE из INDICATOR существуют в GROUP» | type referential
v_ref_indicator_group AS (
    SELECT
        'ref_indicator_group' AS check_id,
        'referential' AS check_type,
        CAST(i.CONTEST_CODE AS STRING) AS detail_key,
        'INDICATOR.CONTEST_CODE отсутствует в GROUP' AS detail_message
    FROM spod_dq.t_indicator i
    LEFT JOIN (
        SELECT DISTINCT CONTEST_CODE FROM spod_dq.t_group
    ) g ON g.CONTEST_CODE = i.CONTEST_CODE
    WHERE i.CONTEST_CODE IS NOT NULL
      AND TRIM(CAST(i.CONTEST_CODE AS STRING)) <> ''
      AND g.CONTEST_CODE IS NULL
),

-- id "ref_report_contest_data" | name «Все CONTEST_CODE из REPORT существуют в CONTEST-DATE» | type referential
v_ref_report_contest_data AS (
    SELECT
        'ref_report_contest_data' AS check_id,
        'referential' AS check_type,
        CAST(r.CONTEST_CODE AS STRING) AS detail_key,
        'REPORT.CONTEST_CODE отсутствует в CONTEST-DATA' AS detail_message
    FROM spod_dq.t_report r
    LEFT JOIN spod_dq.t_contest_data c
        ON c.CONTEST_CODE = r.CONTEST_CODE
    WHERE r.CONTEST_CODE IS NOT NULL
      AND TRIM(CAST(r.CONTEST_CODE AS STRING)) <> ''
      AND c.CONTEST_CODE IS NULL
),

-- id "ref_reward_reward_link" | name «Все REWARD_CODE из REWARD существуют в REWARD-LINK» | type referential
v_ref_reward_reward_link AS (
    SELECT
        'ref_reward_reward_link' AS check_id,
        'referential' AS check_type,
        CAST(rw.REWARD_CODE AS STRING) AS detail_key,
        'REWARD.REWARD_CODE отсутствует в REWARD-LINK' AS detail_message
    FROM spod_dq.t_reward rw
    LEFT JOIN (
        SELECT DISTINCT REWARD_CODE FROM spod_dq.t_reward_link
    ) rl ON rl.REWARD_CODE = rw.REWARD_CODE
    WHERE rw.REWARD_CODE IS NOT NULL
      AND TRIM(CAST(rw.REWARD_CODE AS STRING)) <> ''
      AND rl.REWARD_CODE IS NULL
),

-- ---------------------------------------------------------------------------
-- B. REFERENTIAL_COMPOSITE — целостность по составному ключу (две и более колонок)
-- ---------------------------------------------------------------------------
-- Отличие от раздела A: JOIN задаётся по паре/набору полей; «осиротевшие» комбинации
-- ищутся тем же LEFT JOIN + проверка NULL на стороне справочника.

-- id "5" | name «Все пары CONTEST_CODE, GROUP_CODE из REWARD-LINK существуют в GROUP» | type referential_composite
v_comp_5 AS (
    SELECT
        '5' AS check_id,
        'referential_composite' AS check_type,
        CONCAT_WS('|', CAST(rl.CONTEST_CODE AS STRING), CAST(rl.GROUP_CODE AS STRING)) AS detail_key,
        'Пара CONTEST_CODE+GROUP_CODE из REWARD-LINK отсутствует в GROUP' AS detail_message
    FROM spod_dq.t_reward_link rl
    LEFT JOIN spod_dq.t_group g
        ON g.CONTEST_CODE = rl.CONTEST_CODE
       AND g.GROUP_CODE = rl.GROUP_CODE
    WHERE (rl.CONTEST_CODE IS NOT NULL AND TRIM(CAST(rl.CONTEST_CODE AS STRING)) <> '')
      AND (rl.GROUP_CODE IS NOT NULL AND TRIM(CAST(rl.GROUP_CODE AS STRING)) <> '')
      AND g.CONTEST_CODE IS NULL
),

-- id "ref_composite_group_reward_link" | name «Все пары CONTEST_CODE, GROUP_CODE из GROUP существуют в REWARD-LINK» | type referential_composite
v_comp_grp_rl AS (
    SELECT
        'ref_composite_group_reward_link' AS check_id,
        'referential_composite' AS check_type,
        CONCAT_WS('|', CAST(g.CONTEST_CODE AS STRING), CAST(g.GROUP_CODE AS STRING)) AS detail_key,
        'Пара из GROUP отсутствует в REWARD-LINK' AS detail_message
    FROM spod_dq.t_group g
    LEFT JOIN spod_dq.t_reward_link rl
        ON rl.CONTEST_CODE = g.CONTEST_CODE
       AND rl.GROUP_CODE = g.GROUP_CODE
    WHERE (g.CONTEST_CODE IS NOT NULL AND TRIM(CAST(g.CONTEST_CODE AS STRING)) <> '')
      AND (g.GROUP_CODE IS NOT NULL AND TRIM(CAST(g.GROUP_CODE AS STRING)) <> '')
      AND rl.CONTEST_CODE IS NULL
),

-- id "ref_composite_report_schedule" | name «Все пары TOURNAMENT_CODE, CONTEST_CODE из REPORT существуют в TOURNAMENT-SCHEDULE» | type referential_composite
v_comp_rep_sch AS (
    SELECT
        'ref_composite_report_schedule' AS check_id,
        'referential_composite' AS check_type,
        CONCAT_WS('|', CAST(r.TOURNAMENT_CODE AS STRING), CAST(r.CONTEST_CODE AS STRING)) AS detail_key,
        'Пара из REPORT отсутствует в TOURNAMENT-SCHEDULE' AS detail_message
    FROM spod_dq.t_report r
    LEFT JOIN spod_dq.t_tournament_schedule s
        ON s.TOURNAMENT_CODE = r.TOURNAMENT_CODE
       AND s.CONTEST_CODE = r.CONTEST_CODE
    WHERE (r.TOURNAMENT_CODE IS NOT NULL AND TRIM(CAST(r.TOURNAMENT_CODE AS STRING)) <> '')
      AND (r.CONTEST_CODE IS NOT NULL AND TRIM(CAST(r.CONTEST_CODE AS STRING)) <> '')
      AND s.TOURNAMENT_CODE IS NULL
),

-- ---------------------------------------------------------------------------
-- C. UNIQUE — уникальность бизнес-ключа (дубликаты в таблице)
-- ---------------------------------------------------------------------------
-- Логика: внутренний подзапрос группирует по ключу и оставляет только группы с
-- COUNT(*) > 1; внешний SELECT добавляет check_id и человекочитаемое detail_*.
-- В DETAIL одна строка = один дублирующийся ключ (не каждая физическая строка Excel).

-- id "3" | name «В GROUP нет дублей по составному полю CONTEST_CODE, GROUP_CODE, GROUP_VALUE» | type unique
v_uq_3 AS (
    SELECT
        '3' AS check_id,
        'unique' AS check_type,
        CONCAT_WS('|', CAST(x.CONTEST_CODE AS STRING), CAST(x.GROUP_CODE AS STRING), CAST(x.GROUP_VALUE AS STRING)) AS detail_key,
        CONCAT('Дубликат по (CONTEST_CODE, GROUP_CODE, GROUP_VALUE): строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT CONTEST_CODE, GROUP_CODE, GROUP_VALUE, COUNT(*) AS cnt
        FROM spod_dq.t_group
        GROUP BY CONTEST_CODE, GROUP_CODE, GROUP_VALUE
        HAVING COUNT(*) > 1
    ) x
),

-- id "4" | name «В REWARD-LINK нет дублей по составному полю CONTEST_CODE, GROUP_CODE, REWARD_CODE» | type unique
v_uq_4 AS (
    SELECT
        '4' AS check_id,
        'unique' AS check_type,
        CONCAT_WS('|', CAST(x.CONTEST_CODE AS STRING), CAST(x.GROUP_CODE AS STRING), CAST(x.REWARD_CODE AS STRING)) AS detail_key,
        CONCAT('Дубликат по (CONTEST_CODE, GROUP_CODE, REWARD_CODE): строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT CONTEST_CODE, GROUP_CODE, REWARD_CODE, COUNT(*) AS cnt
        FROM spod_dq.t_reward_link
        GROUP BY CONTEST_CODE, GROUP_CODE, REWARD_CODE
        HAVING COUNT(*) > 1
    ) x
),

-- id "unique_contest_data" | name «В CONTEST-DATA нет дублей по полю CONTEST_CODE» | type unique
v_uq_contest_data AS (
    SELECT
        'unique_contest_data' AS check_id,
        'unique' AS check_type,
        CAST(x.CONTEST_CODE AS STRING) AS detail_key,
        CONCAT('Дубликат CONTEST_CODE: строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT CONTEST_CODE, COUNT(*) AS cnt
        FROM spod_dq.t_contest_data
        GROUP BY CONTEST_CODE
        HAVING COUNT(*) > 1
    ) x
),

-- id "unique_indicator_1" | name «В INDICATOR нет дублей по составному полю CONTEST_CODE, INDICATOR_ADD_CALC_TYPE, INDICATOR_CODE» | type unique
v_uq_ind1 AS (
    SELECT
        'unique_indicator_1' AS check_id,
        'unique' AS check_type,
        CONCAT_WS('|', CAST(x.CONTEST_CODE AS STRING), CAST(x.INDICATOR_ADD_CALC_TYPE AS STRING), CAST(x.INDICATOR_CODE AS STRING)) AS detail_key,
        CONCAT('Дубликат ключа индикатора: строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT CONTEST_CODE, INDICATOR_ADD_CALC_TYPE, INDICATOR_CODE, COUNT(*) AS cnt
        FROM spod_dq.t_indicator
        GROUP BY CONTEST_CODE, INDICATOR_ADD_CALC_TYPE, INDICATOR_CODE
        HAVING COUNT(*) > 1
    ) x
),

-- id "unique_indicator_n" | name «В INDICATOR нет дублей по полю N» | type unique
v_uq_ind_n AS (
    SELECT
        'unique_indicator_n' AS check_id,
        'unique' AS check_type,
        CAST(x.N AS STRING) AS detail_key,
        CONCAT('Дубликат N: строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT N, COUNT(*) AS cnt
        FROM spod_dq.t_indicator
        GROUP BY N
        HAVING COUNT(*) > 1
    ) x
),

-- id "unique_report" | name «В REPORT нет дублей по составному полю MANAGER_PERSON_NUMBER, TOURNAMENT_CODE, CONTEST_CODE» | type unique
v_uq_report AS (
    SELECT
        'unique_report' AS check_id,
        'unique' AS check_type,
        CONCAT_WS('|', CAST(x.MANAGER_PERSON_NUMBER AS STRING), CAST(x.TOURNAMENT_CODE AS STRING), CAST(x.CONTEST_CODE AS STRING)) AS detail_key,
        CONCAT('Дубликат ключа отчёта: строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT MANAGER_PERSON_NUMBER, TOURNAMENT_CODE, CONTEST_CODE, COUNT(*) AS cnt
        FROM spod_dq.t_report
        GROUP BY MANAGER_PERSON_NUMBER, TOURNAMENT_CODE, CONTEST_CODE
        HAVING COUNT(*) > 1
    ) x
),

-- id "unique_reward" | name «В REWARD нет дублей по полю REWARD_CODE» | type unique
v_uq_reward AS (
    SELECT
        'unique_reward' AS check_id,
        'unique' AS check_type,
        CAST(x.REWARD_CODE AS STRING) AS detail_key,
        CONCAT('Дубликат REWARD_CODE: строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT REWARD_CODE, COUNT(*) AS cnt
        FROM spod_dq.t_reward
        GROUP BY REWARD_CODE
        HAVING COUNT(*) > 1
    ) x
),

-- id "unique_reward_link_2" | name «В REWARD-LINK нет дублей по составному полю CONTEST_CODE, REWARD_CODE» | type unique
v_uq_rl2 AS (
    SELECT
        'unique_reward_link_2' AS check_id,
        'unique' AS check_type,
        CONCAT_WS('|', CAST(x.CONTEST_CODE AS STRING), CAST(x.REWARD_CODE AS STRING)) AS detail_key,
        CONCAT('Дубликат (CONTEST_CODE, REWARD_CODE): строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT CONTEST_CODE, REWARD_CODE, COUNT(*) AS cnt
        FROM spod_dq.t_reward_link
        GROUP BY CONTEST_CODE, REWARD_CODE
        HAVING COUNT(*) > 1
    ) x
),

-- id "unique_reward_link_reward" | name «В REWARD-LINK нет дублей по полю REWARD_CODE» | type unique
v_uq_rl_r AS (
    SELECT
        'unique_reward_link_reward' AS check_id,
        'unique' AS check_type,
        CAST(x.REWARD_CODE AS STRING) AS detail_key,
        CONCAT('Дубликат REWARD_CODE в REWARD-LINK: строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT REWARD_CODE, COUNT(*) AS cnt
        FROM spod_dq.t_reward_link
        GROUP BY REWARD_CODE
        HAVING COUNT(*) > 1
    ) x
),

-- id "unique_schedule_2" | name «В TOURNAMENT-SCHEDULE нет дублей по составному полю TOURNAMENT_CODE, CONTEST_CODE» | type unique
v_uq_sch2 AS (
    SELECT
        'unique_schedule_2' AS check_id,
        'unique' AS check_type,
        CONCAT_WS('|', CAST(x.TOURNAMENT_CODE AS STRING), CAST(x.CONTEST_CODE AS STRING)) AS detail_key,
        CONCAT('Дубликат (TOURNAMENT_CODE, CONTEST_CODE): строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT TOURNAMENT_CODE, CONTEST_CODE, COUNT(*) AS cnt
        FROM spod_dq.t_tournament_schedule
        GROUP BY TOURNAMENT_CODE, CONTEST_CODE
        HAVING COUNT(*) > 1
    ) x
),

-- id "unique_schedule_1" | name «В TOURNAMENT-SCHEDULE нет дублей по полю TOURNAMENT_CODE» | type unique
v_uq_sch1 AS (
    SELECT
        'unique_schedule_1' AS check_id,
        'unique' AS check_type,
        CAST(x.TOURNAMENT_CODE AS STRING) AS detail_key,
        CONCAT('Дубликат TOURNAMENT_CODE: строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT TOURNAMENT_CODE, COUNT(*) AS cnt
        FROM spod_dq.t_tournament_schedule
        GROUP BY TOURNAMENT_CODE
        HAVING COUNT(*) > 1
    ) x
),

-- id "unique_org_unit" | name «В ORG_UNIT_V20 нет дублей по полю ORG_UNIT_CODE» | type unique
v_uq_org AS (
    SELECT
        'unique_org_unit' AS check_id,
        'unique' AS check_type,
        CAST(x.ORG_UNIT_CODE AS STRING) AS detail_key,
        CONCAT('Дубликат ORG_UNIT_CODE: строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT ORG_UNIT_CODE, COUNT(*) AS cnt
        FROM spod_dq.t_org_unit_v20
        GROUP BY ORG_UNIT_CODE
        HAVING COUNT(*) > 1
    ) x
),

-- id "unique_tb_gosb" | name «В ORG_UNIT_V20 нет дублей по составному полю TB_CODE, GOSB_CODE» | type unique
v_uq_tb_gosb AS (
    SELECT
        'unique_tb_gosb' AS check_id,
        'unique' AS check_type,
        CONCAT_WS('|', CAST(x.TB_CODE AS STRING), CAST(x.GOSB_CODE AS STRING)) AS detail_key,
        CONCAT('Дубликат (TB_CODE, GOSB_CODE): строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT TB_CODE, GOSB_CODE, COUNT(*) AS cnt
        FROM spod_dq.t_org_unit_v20
        GROUP BY TB_CODE, GOSB_CODE
        HAVING COUNT(*) > 1
    ) x
),

-- id "unique_user_role" | name «В USER_ROLE нет дублей по полю RULE_NUM» | type unique
v_uq_ur AS (
    SELECT
        'unique_user_role' AS check_id,
        'unique' AS check_type,
        CAST(x.RULE_NUM AS STRING) AS detail_key,
        CONCAT('Дубликат RULE_NUM: строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT RULE_NUM, COUNT(*) AS cnt
        FROM spod_dq.t_user_role
        GROUP BY RULE_NUM
        HAVING COUNT(*) > 1
    ) x
),

-- id "unique_user_role_sb" | name «В USER_ROLE SB нет дублей по полю RULE_NUM» | type unique (лист "USER_ROLE SB" → t_user_role_sb)
v_uq_ursb AS (
    SELECT
        'unique_user_role_sb' AS check_id,
        'unique' AS check_type,
        CAST(x.RULE_NUM AS STRING) AS detail_key,
        CONCAT('Дубликат RULE_NUM (SB): строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT RULE_NUM, COUNT(*) AS cnt
        FROM spod_dq.t_user_role_sb
        GROUP BY RULE_NUM
        HAVING COUNT(*) > 1
    ) x
),

-- id "unique_employee_person" | name «В EMPLOYEE нет дублей по полю PERSON_NUMBER» | type unique
v_uq_emp_p AS (
    SELECT
        'unique_employee_person' AS check_id,
        'unique' AS check_type,
        CAST(x.PERSON_NUMBER AS STRING) AS detail_key,
        CONCAT('Дубликат PERSON_NUMBER: строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT PERSON_NUMBER, COUNT(*) AS cnt
        FROM spod_dq.t_employee
        GROUP BY PERSON_NUMBER
        HAVING COUNT(*) > 1
    ) x
),

-- id "unique_employee_person_add" | name «В EMPLOYEE нет дублей по полю PERSON_NUMBER_ADD» | type unique
v_uq_emp_pa AS (
    SELECT
        'unique_employee_person_add' AS check_id,
        'unique' AS check_type,
        CAST(x.PERSON_NUMBER_ADD AS STRING) AS detail_key,
        CONCAT('Дубликат PERSON_NUMBER_ADD: строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT PERSON_NUMBER_ADD, COUNT(*) AS cnt
        FROM spod_dq.t_employee
        GROUP BY PERSON_NUMBER_ADD
        HAVING COUNT(*) > 1
    ) x
),

-- id "unique_employee_kpk_gosb" | name «В EMPLOYEE нет дублей по POSITION_NAME, KPK_CODE, ORG_UNIT_CODE среди строк с POSITION_NAME=КПК и непустым KPK_CODE» | type unique
v_uq_emp_kpk AS (
    SELECT
        'unique_employee_kpk_gosb' AS check_id,
        'unique' AS check_type,
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

-- ---------------------------------------------------------------------------
-- D. FIELD_LENGTH — ограничение длины строки после приведения к STRING
-- ---------------------------------------------------------------------------
-- В выборку попадают только строки таблицы, где LENGTH(...) не укладывается в лимит
-- или (для табельных номеров) длина не равна ожидаемой. RLIKE здесь не используется.

-- id "field_length_org_unit" | name «Поле TB_FULL_NAME в ORG_UNIT_V20 должно быть <=100; Поле GOSB_NAME в ORG_UNIT_V20 должно быть <=100; Поле GOSB_SHORT_NAME в ORG_UNIT_V20 должно быть <=20» | type field_length (_run_field_length_check)
v_fl_org AS (
    SELECT
        'field_length_org_unit' AS check_id,
        'field_length' AS check_type,
        CONCAT_WS(':', CAST(ORG_UNIT_CODE AS STRING), 'TB_FULL_NAME') AS detail_key,
        CONCAT('Длина TB_FULL_NAME=', CAST(LENGTH(CAST(TB_FULL_NAME AS STRING)) AS STRING), ' > 100') AS detail_message
    FROM spod_dq.t_org_unit_v20
    WHERE LENGTH(CAST(TB_FULL_NAME AS STRING)) > 100
    UNION ALL
    SELECT
        'field_length_org_unit', 'field_length',
        CONCAT_WS(':', CAST(ORG_UNIT_CODE AS STRING), 'GOSB_NAME'),
        CONCAT('Длина GOSB_NAME=', CAST(LENGTH(CAST(GOSB_NAME AS STRING)) AS STRING), ' > 100')
    FROM spod_dq.t_org_unit_v20
    WHERE LENGTH(CAST(GOSB_NAME AS STRING)) > 100
    UNION ALL
    SELECT
        'field_length_org_unit', 'field_length',
        CONCAT_WS(':', CAST(ORG_UNIT_CODE AS STRING), 'GOSB_SHORT_NAME'),
        CONCAT('Длина GOSB_SHORT_NAME=', CAST(LENGTH(CAST(GOSB_SHORT_NAME AS STRING)) AS STRING), ' > 20')
    FROM spod_dq.t_org_unit_v20
    WHERE LENGTH(CAST(GOSB_SHORT_NAME AS STRING)) > 20
),

-- id "field_length_employee" | name «Поле PERSON_NUMBER в EMPLOYEE должно быть =20; PERSON_NUMBER_ADD =20» | type field_length
v_fl_emp AS (
    SELECT
        'field_length_employee' AS check_id,
        'field_length' AS check_type,
        CONCAT('PERSON_NUMBER=', CAST(PERSON_NUMBER AS STRING)) AS detail_key,
        CONCAT('Длина PERSON_NUMBER=', CAST(LENGTH(CAST(PERSON_NUMBER AS STRING)) AS STRING), ' (ожидается 20)') AS detail_message
    FROM spod_dq.t_employee
    WHERE LENGTH(CAST(PERSON_NUMBER AS STRING)) <> 20
    UNION ALL
    SELECT
        'field_length_employee', 'field_length',
        CONCAT('PERSON_NUMBER_ADD=', CAST(PERSON_NUMBER_ADD AS STRING)),
        CONCAT('Длина PERSON_NUMBER_ADD=', CAST(LENGTH(CAST(PERSON_NUMBER_ADD AS STRING)) AS STRING), ' (ожидается 20)')
    FROM spod_dq.t_employee
    WHERE LENGTH(CAST(PERSON_NUMBER_ADD AS STRING)) <> 20
),

-- id "field_length_report" | name «Поле MANAGER_PERSON_NUMBER в REPORT должно быть =20» | type field_length
v_fl_rep AS (
    SELECT
        'field_length_report' AS check_id,
        'field_length' AS check_type,
        CONCAT_WS('|', CAST(MANAGER_PERSON_NUMBER AS STRING), CAST(TOURNAMENT_CODE AS STRING), CAST(CONTEST_CODE AS STRING)) AS detail_key,
        CONCAT('Длина MANAGER_PERSON_NUMBER=', CAST(LENGTH(CAST(MANAGER_PERSON_NUMBER AS STRING)) AS STRING), ' (ожидается 20)') AS detail_message
    FROM spod_dq.t_report
    WHERE LENGTH(CAST(MANAGER_PERSON_NUMBER AS STRING)) <> 20
),

-- ---------------------------------------------------------------------------
-- E. FIELD_FORMAT — соответствие строки шаблону (RLIKE в Hive/Spark)
-- ---------------------------------------------------------------------------
-- Проверяются только непустые значения (после TRIM). Даты — упрощённый шаблон
-- YYYY-MM-DD без проверки календарной корректности; при необходимости замените на to_date().

-- id "format_report_contest_date" | name «Формат поля CONTEST_DATE в REPORT должен быть: YYYY-MM-DD» | type field_format (_run_field_format_check)
v_ff_rep_contest_date AS (
    SELECT
        'format_report_contest_date' AS check_id,
        'field_format' AS check_type,
        CONCAT('CONTEST_DATE=', CAST(CONTEST_DATE AS STRING)) AS detail_key,
        'Неверный формат даты YYYY-MM-DD' AS detail_message
    FROM spod_dq.t_report
    WHERE CONTEST_DATE IS NOT NULL
      AND TRIM(CAST(CONTEST_DATE AS STRING)) <> ''
      AND NOT (CAST(CONTEST_DATE AS STRING) RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$')
),

-- id "format_contest_data_create_dt" | name «Формат поля CREATE_DT в CONTEST-DATE должен быть: YYYY-MM-DD» | type field_format
v_ff_cd_create AS (
    SELECT
        'format_contest_data_create_dt' AS check_id,
        'field_format' AS check_type,
        CONCAT('CREATE_DT=', CAST(CREATE_DT AS STRING)) AS detail_key,
        'Неверный формат даты YYYY-MM-DD' AS detail_message
    FROM spod_dq.t_contest_data
    WHERE CREATE_DT IS NOT NULL
      AND TRIM(CAST(CREATE_DT AS STRING)) <> ''
      AND NOT (CAST(CREATE_DT AS STRING) RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$')
),

-- id "format_contest_data_close_dt" | name «Формат поля CLOSE_DT в CONTEST-DATE должен быть: YYYY-MM-DD (учесть вариант 4000-01-01)» | type field_format
v_ff_cd_close AS (
    SELECT
        'format_contest_data_close_dt' AS check_id,
        'field_format' AS check_type,
        CONCAT('CLOSE_DT=', CAST(CLOSE_DT AS STRING)) AS detail_key,
        'Неверный формат даты YYYY-MM-DD (или не спец. 4000-01-01)' AS detail_message
    FROM spod_dq.t_contest_data
    WHERE CLOSE_DT IS NOT NULL
      AND TRIM(CAST(CLOSE_DT AS STRING)) <> ''
      AND TRIM(CAST(CLOSE_DT AS STRING)) <> '4000-01-01'
      AND NOT (CAST(CLOSE_DT AS STRING) RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$')
),

-- id "format_schedule_start_dt" | name «Формат поля START_DT в TOURNAMENT-SCHEDULE должен быть: YYYY-MM-DD» | type field_format
v_ff_sch_start AS (
    SELECT
        'format_schedule_start_dt' AS check_id,
        'field_format' AS check_type,
        CONCAT('START_DT=', CAST(START_DT AS STRING)) AS detail_key,
        'Неверный формат даты YYYY-MM-DD' AS detail_message
    FROM spod_dq.t_tournament_schedule
    WHERE START_DT IS NOT NULL
      AND TRIM(CAST(START_DT AS STRING)) <> ''
      AND NOT (CAST(START_DT AS STRING) RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$')
),

-- id "format_schedule_end_dt" | name «Формат поля END_DT в TOURNAMENT-SCHEDULE должен быть: YYYY-MM-DD (учесть вариант 4000-01-01)» | type field_format
v_ff_sch_end AS (
    SELECT
        'format_schedule_end_dt' AS check_id,
        'field_format' AS check_type,
        CONCAT('END_DT=', CAST(END_DT AS STRING)) AS detail_key,
        'Неверный формат даты YYYY-MM-DD (или не спец. 4000-01-01)' AS detail_message
    FROM spod_dq.t_tournament_schedule
    WHERE END_DT IS NOT NULL
      AND TRIM(CAST(END_DT AS STRING)) <> ''
      AND TRIM(CAST(END_DT AS STRING)) <> '4000-01-01'
      AND NOT (CAST(END_DT AS STRING) RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$')
),

-- id "format_schedule_result_dt" | name «Формат поля RESULT_DT в TOURNAMENT-SCHEDULE должен быть: YYYY-MM-DD если присутствует» | type field_format
v_ff_sch_result AS (
    SELECT
        'format_schedule_result_dt' AS check_id,
        'field_format' AS check_type,
        CONCAT('RESULT_DT=', CAST(RESULT_DT AS STRING)) AS detail_key,
        'Неверный формат даты YYYY-MM-DD' AS detail_message
    FROM spod_dq.t_tournament_schedule
    WHERE RESULT_DT IS NOT NULL
      AND TRIM(CAST(RESULT_DT AS STRING)) <> ''
      AND NOT (CAST(RESULT_DT AS STRING) RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$')
),

-- id "format_report_plan_value" | name «Формат поля PLAN_VALUE в REPORT должен быть: 0.00000 (...)» | type field_format
v_ff_rep_plan AS (
    SELECT
        'format_report_plan_value' AS check_id,
        'field_format' AS check_type,
        CONCAT('PLAN_VALUE=', CAST(PLAN_VALUE AS STRING)) AS detail_key,
        'Ожидается decimal с 5 знаками после точки' AS detail_message
    FROM spod_dq.t_report
    WHERE PLAN_VALUE IS NOT NULL
      AND TRIM(CAST(PLAN_VALUE AS STRING)) <> ''
      AND NOT (CAST(PLAN_VALUE AS STRING) RLIKE '^-?[0-9]+\\.[0-9]{5}$')
),

-- id "format_report_fact_value" | name «Формат поля FACT_VALUE в REPORT должен быть: 0.00000 (...)» | type field_format
v_ff_rep_fact AS (
    SELECT
        'format_report_fact_value' AS check_id,
        'field_format' AS check_type,
        CONCAT('FACT_VALUE=', CAST(FACT_VALUE AS STRING)) AS detail_key,
        'Ожидается decimal с 5 знаками после точки' AS detail_message
    FROM spod_dq.t_report
    WHERE FACT_VALUE IS NOT NULL
      AND TRIM(CAST(FACT_VALUE AS STRING)) <> ''
      AND NOT (CAST(FACT_VALUE AS STRING) RLIKE '^-?[0-9]+\\.[0-9]{5}$')
),

-- id "format_report_manager_person_number" | name «Формат поля MANAGER_PERSON_NUMBER в REPORT должен быть: 20 цифр с лидирующими нулями» | type field_format
v_ff_rep_mgr_pn AS (
    SELECT
        'format_report_manager_person_number' AS check_id,
        'field_format' AS check_type,
        CAST(MANAGER_PERSON_NUMBER AS STRING) AS detail_key,
        'MANAGER_PERSON_NUMBER: ожидается ровно 20 цифр' AS detail_message
    FROM spod_dq.t_report
    WHERE MANAGER_PERSON_NUMBER IS NOT NULL
      AND TRIM(CAST(MANAGER_PERSON_NUMBER AS STRING)) <> ''
      AND NOT (CAST(MANAGER_PERSON_NUMBER AS STRING) RLIKE '^[0-9]{20}$')
),

-- id "format_employee_person_number" | name «Формат поля PERSON_NUMBER в EMPLOYEE должен быть: 20 цифр с лидирующими нулями» | type field_format
v_ff_emp_pn AS (
    SELECT
        'format_employee_person_number' AS check_id,
        'field_format' AS check_type,
        CAST(PERSON_NUMBER AS STRING) AS detail_key,
        'PERSON_NUMBER: ожидается ровно 20 цифр' AS detail_message
    FROM spod_dq.t_employee
    WHERE PERSON_NUMBER IS NOT NULL
      AND TRIM(CAST(PERSON_NUMBER AS STRING)) <> ''
      AND NOT (CAST(PERSON_NUMBER AS STRING) RLIKE '^[0-9]{20}$')
),

-- id "format_employee_person_number_add" | name «Формат поля PERSON_NUMBER_ADD в EMPLOYEE должен быть: 20 цифр с лидирующими нулями» | type field_format
v_ff_emp_pna AS (
    SELECT
        'format_employee_person_number_add' AS check_id,
        'field_format' AS check_type,
        CAST(PERSON_NUMBER_ADD AS STRING) AS detail_key,
        'PERSON_NUMBER_ADD: ожидается ровно 20 цифр' AS detail_message
    FROM spod_dq.t_employee
    WHERE PERSON_NUMBER_ADD IS NOT NULL
      AND TRIM(CAST(PERSON_NUMBER_ADD AS STRING)) <> ''
      AND NOT (CAST(PERSON_NUMBER_ADD AS STRING) RLIKE '^[0-9]{20}$')
),

-- ---------------------------------------------------------------------------
-- chk_summary — сводная таблица по всем проверкам
-- ---------------------------------------------------------------------------
-- Условия «что считать нарушением» заданы один раз в CTE v_*; здесь для каждой проверки
-- только скалярный подзапрос (SELECT COUNT(*) FROM v_...), дающий violation_count.
-- passed (1/0) вычисляется ниже во внешнем SELECT. Конкретная оптимизация (один проход
-- по таблицам или несколько) зависит от планировщика вашей СУБД.
-- Каждая строка UNION ALL ниже соответствует одному правилу из config consistency_checks.rules
-- с тем же id (порядок строк здесь может отличаться от порядка объектов в JSON — ориентир по id).
chk_summary AS (
    SELECT '1.1' AS check_id, 'referential' AS check_type, (SELECT COUNT(*) FROM v_ref_1_1) AS violation_count
    UNION ALL SELECT '1.2', 'referential', (SELECT COUNT(*) FROM v_ref_1_2)
    UNION ALL SELECT '1.3', 'referential', (SELECT COUNT(*) FROM v_ref_1_3)
    UNION ALL SELECT '2', 'referential', (SELECT COUNT(*) FROM v_ref_2)
    UNION ALL SELECT '9', 'referential', (SELECT COUNT(*) FROM v_ref_9)
    UNION ALL SELECT 'scenario_1', 'referential', (SELECT COUNT(*) FROM v_ref_scenario_1)
    UNION ALL SELECT 'scenario_16', 'referential', (SELECT COUNT(*) FROM v_ref_scenario_16)
    UNION ALL SELECT 'scenario_20', 'referential', (SELECT COUNT(*) FROM v_ref_scenario_20)
    UNION ALL SELECT 'ref_contest_data_group', 'referential', (SELECT COUNT(*) FROM v_ref_contest_data_group)
    UNION ALL SELECT 'ref_indicator_group', 'referential', (SELECT COUNT(*) FROM v_ref_indicator_group)
    UNION ALL SELECT 'ref_report_contest_data', 'referential', (SELECT COUNT(*) FROM v_ref_report_contest_data)
    UNION ALL SELECT 'ref_reward_reward_link', 'referential', (SELECT COUNT(*) FROM v_ref_reward_reward_link)
    UNION ALL SELECT '5', 'referential_composite', (SELECT COUNT(*) FROM v_comp_5)
    UNION ALL SELECT 'ref_composite_group_reward_link', 'referential_composite', (SELECT COUNT(*) FROM v_comp_grp_rl)
    UNION ALL SELECT 'ref_composite_report_schedule', 'referential_composite', (SELECT COUNT(*) FROM v_comp_rep_sch)
    UNION ALL SELECT '3', 'unique', (SELECT COUNT(*) FROM v_uq_3)
    UNION ALL SELECT '4', 'unique', (SELECT COUNT(*) FROM v_uq_4)
    UNION ALL SELECT 'unique_contest_data', 'unique', (SELECT COUNT(*) FROM v_uq_contest_data)
    UNION ALL SELECT 'unique_indicator_1', 'unique', (SELECT COUNT(*) FROM v_uq_ind1)
    UNION ALL SELECT 'unique_indicator_n', 'unique', (SELECT COUNT(*) FROM v_uq_ind_n)
    UNION ALL SELECT 'unique_report', 'unique', (SELECT COUNT(*) FROM v_uq_report)
    UNION ALL SELECT 'unique_reward', 'unique', (SELECT COUNT(*) FROM v_uq_reward)
    UNION ALL SELECT 'unique_reward_link_2', 'unique', (SELECT COUNT(*) FROM v_uq_rl2)
    UNION ALL SELECT 'unique_reward_link_reward', 'unique', (SELECT COUNT(*) FROM v_uq_rl_r)
    UNION ALL SELECT 'unique_schedule_2', 'unique', (SELECT COUNT(*) FROM v_uq_sch2)
    UNION ALL SELECT 'unique_schedule_1', 'unique', (SELECT COUNT(*) FROM v_uq_sch1)
    UNION ALL SELECT 'unique_org_unit', 'unique', (SELECT COUNT(*) FROM v_uq_org)
    UNION ALL SELECT 'unique_tb_gosb', 'unique', (SELECT COUNT(*) FROM v_uq_tb_gosb)
    UNION ALL SELECT 'unique_user_role', 'unique', (SELECT COUNT(*) FROM v_uq_ur)
    UNION ALL SELECT 'unique_user_role_sb', 'unique', (SELECT COUNT(*) FROM v_uq_ursb)
    UNION ALL SELECT 'unique_employee_person', 'unique', (SELECT COUNT(*) FROM v_uq_emp_p)
    UNION ALL SELECT 'unique_employee_person_add', 'unique', (SELECT COUNT(*) FROM v_uq_emp_pa)
    UNION ALL SELECT 'unique_employee_kpk_gosb', 'unique', (SELECT COUNT(*) FROM v_uq_emp_kpk)
    UNION ALL SELECT 'field_length_org_unit', 'field_length', (SELECT COUNT(*) FROM v_fl_org)
    UNION ALL SELECT 'field_length_employee', 'field_length', (SELECT COUNT(*) FROM v_fl_emp)
    UNION ALL SELECT 'field_length_report', 'field_length', (SELECT COUNT(*) FROM v_fl_rep)
    UNION ALL SELECT 'format_report_contest_date', 'field_format', (SELECT COUNT(*) FROM v_ff_rep_contest_date)
    UNION ALL SELECT 'format_contest_data_create_dt', 'field_format', (SELECT COUNT(*) FROM v_ff_cd_create)
    UNION ALL SELECT 'format_contest_data_close_dt', 'field_format', (SELECT COUNT(*) FROM v_ff_cd_close)
    UNION ALL SELECT 'format_schedule_start_dt', 'field_format', (SELECT COUNT(*) FROM v_ff_sch_start)
    UNION ALL SELECT 'format_schedule_end_dt', 'field_format', (SELECT COUNT(*) FROM v_ff_sch_end)
    UNION ALL SELECT 'format_schedule_result_dt', 'field_format', (SELECT COUNT(*) FROM v_ff_sch_result)
    UNION ALL SELECT 'format_report_plan_value', 'field_format', (SELECT COUNT(*) FROM v_ff_rep_plan)
    UNION ALL SELECT 'format_report_fact_value', 'field_format', (SELECT COUNT(*) FROM v_ff_rep_fact)
    UNION ALL SELECT 'format_report_manager_person_number', 'field_format', (SELECT COUNT(*) FROM v_ff_rep_mgr_pn)
    UNION ALL SELECT 'format_employee_person_number', 'field_format', (SELECT COUNT(*) FROM v_ff_emp_pn)
    UNION ALL SELECT 'format_employee_person_number_add', 'field_format', (SELECT COUNT(*) FROM v_ff_emp_pna)
),

-- ---------------------------------------------------------------------------
-- chk_detail — единый поток всех нарушений (построчно)
-- ---------------------------------------------------------------------------
-- UNION ALL склеивает наборы строк из всех v_*; если проверка чистая, её CTE пустой и
-- в итог не даёт строк. Удобно искать по check_id (= rules[].id в config) или экспортировать в файл расследования.
chk_detail AS (
    SELECT check_id, check_type, detail_key, detail_message FROM v_ref_1_1
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_ref_1_2
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_ref_1_3
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_ref_2
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_ref_9
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_ref_scenario_1
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_ref_scenario_16
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_ref_scenario_20
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_ref_contest_data_group
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_ref_indicator_group
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_ref_report_contest_data
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_ref_reward_reward_link
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_comp_5
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_comp_grp_rl
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_comp_rep_sch
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_uq_3
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_uq_4
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_uq_contest_data
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_uq_ind1
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_uq_ind_n
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_uq_report
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_uq_reward
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_uq_rl2
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_uq_rl_r
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_uq_sch2
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_uq_sch1
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_uq_org
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_uq_tb_gosb
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_uq_ur
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_uq_ursb
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_uq_emp_p
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_uq_emp_pa
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_uq_emp_kpk
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_fl_org
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_fl_emp
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_fl_rep
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_ff_rep_contest_date
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_ff_cd_create
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_ff_cd_close
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_ff_sch_start
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_ff_sch_end
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_ff_sch_result
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_ff_rep_plan
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_ff_rep_fact
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_ff_rep_mgr_pn
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_ff_emp_pn
    UNION ALL SELECT check_id, check_type, detail_key, detail_message FROM v_ff_emp_pna
)

-- ---------------------------------------------------------------------------
-- Итоговый SELECT — «два отчёта в одной таблице результата»
-- ---------------------------------------------------------------------------
-- Первая часть UNION ALL: только сводка (passed, violation_count); detail_key/detail_message NULL.
-- Вторая часть: только детали; passed и violation_count NULL, чтобы типы колонок совпадали.
-- result_order гарантирует, что при ORDER BY сначала пойдут все SUMMARY, затем все DETAIL.
SELECT
    1 AS result_order,
    CAST('SUMMARY' AS STRING) AS report_section,
    s.check_id,
    s.check_type,
    CAST(CASE WHEN s.violation_count = 0 THEN 1 ELSE 0 END AS BIGINT) AS passed,
    CAST(s.violation_count AS BIGINT) AS violation_count,
    CAST(NULL AS STRING) AS detail_key,
    CAST(NULL AS STRING) AS detail_message
FROM chk_summary s

-- UNION ALL не убирает дубликаты и не сортирует; просто дописывает строки второй выборки к первой.
UNION ALL

SELECT
    2 AS result_order,
    CAST('DETAIL' AS STRING) AS report_section,
    d.check_id,
    d.check_type,
    CAST(NULL AS BIGINT) AS passed,
    CAST(NULL AS BIGINT) AS violation_count,
    d.detail_key,
    d.detail_message
FROM chk_detail d

-- Сортировка: сначала весь блок сводки, затем детали; внутри — по идентификатору проверки и ключу строки.
ORDER BY result_order, check_id, detail_key
;
-- Конец единого запроса (одна команда для клиента СУБД). Дальше — только поясняющие комментарии вне SQL.

-- =============================================================================
-- ПРИМЕЧАНИЕ (соответствие config.json consistency_checks.rules):
--   В SQL не включены правила с enabled: false:
--     id "ref_contest_data_indicator" — «Все CONTEST_CODE из CONTEST-DATE существуют в INDICATOR»;
--     id "ref_group_indicator" — «Все CONTEST_CODE из GROUP существуют в INDICATOR».
--   Не перенесены типы json_field_equals_column, json_field_in_column,
--     json_priority_unique_per_contest_link (id: reward_add_data_badge, reward_add_data_label,
--     reward_add_data_badge_n, reward_parent_in_reward_code, reward_priority_unique_per_contest) —
--     нужен разбор JSON в СУБД или отдельный слой, см. consistency_checks.py.
--   Проверка csv_columns_count задаётся отдельно в конфиге, не дублируется в этом SQL.
-- =============================================================================
