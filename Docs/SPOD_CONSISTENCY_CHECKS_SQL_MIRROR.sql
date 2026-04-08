-- =============================================================================
-- SPOD_PROM: зеркало проверок консистентности в SQL (referential, composite,
--            unique с простым scope, field_length).
-- Правила type=field_format в СУБД не дублируются — остаются в Python (consistency_checks.py).
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
--   report_section = 'DETAIL' — только нарушения: detail_key, detail_message.
--   Сначала идут все строки SUMMARY (result_order=1), затем все DETAIL (result_order=2).
--   Чтобы взять только сводку: WHERE report_section = 'SUMMARY'.
--   Чтобы взять только детали: WHERE report_section = 'DETAIL'.
--
-- Ограничения (в SQL намеренно НЕ переносятся — остаются в программе):
--   field_format (все правила format_*), json_field_equals_column, json_field_in_column,
--   json_priority_unique_per_contest_link, csv_columns_count по сырому CSV.
--
-- -----------------------------------------------------------------------------
-- Как устроена работа скрипта (один запрос от ключевого слова WITH до «;» в конце)
-- -----------------------------------------------------------------------------
-- 1) Выполнение в СУБД: вы отдаёте движку (Hive/Spark SQL и т.п.) целиком один
--    оператор SELECT с общим блоком WITH — это не «несколько команд», а одна
--    логическая команда, которая строит временные именованные подзапросы (CTE).
--
-- 2) В начале WITH вычисляются CTE dim_* и base_schedule_ref: компактные справочники
--    и один проход по расписанию для scenario_1/16/20 — меньше повторных чтений
--    t_group, t_reward_link, t_tournament_schedule, t_contest_data там, где ключи совпадают
--    с семантикой JOIN к полным таблицам (см. комментарий у dim_group_raw).
-- 2b) Затем CTE v_* — в каждом только строки-НАРУШИТЕЛИ (или пустой набор). Формат:
--    detail_key, detail_message.
--
-- 3) CTE chk_summary: для каждой проверки COUNT(*) из соответствующего v_* —
--    violation_count и далее passed. Промежуточные dim_* при повторных ссылках
--    часто переиспользуются планировщиком (Spark чаще, чем старый Hive).
--
-- 4) CTE chk_detail склеивает (UNION ALL) все v_* в один длинный список —
--    все детальные строки по всем проверкам сразу.
--
-- 5) Внешний SELECT после chk_detail объединяет две «плоскости» результата:
--    строки SUMMARY (сводка) и строки DETAIL (детали) через UNION ALL; колонки
--    выровнены типами (где не применимо — NULL). ORDER BY сначала выводит сводку,
--    потом детали; внутри секции — по detail_key.
--
-- 6) Типичный сценарий: сохранить результат во временную таблицу или смотреть
--    в IDE; для «только упавшие проверки» — фильтр report_section='SUMMARY'
--    AND passed=0.
--
-- -----------------------------------------------------------------------------
-- Соответствие исходному коду проекта SPOD_PROM
-- -----------------------------------------------------------------------------
-- Источник правил: config.json → объект "consistency_checks" → массив "rules".
--   Поле "id" у правила соответствует секции проверки в этом SQL (см. комментарии у CTE).
--   Поле "name" — человекочитаемое название; ниже у каждого CTE оно процитировано в «ёлочках».
--   Поле "type" — тип обработчика (referential, referential_composite, unique, …).
--
-- Реализация в коде: src/consistency_checks.py
--   — run_all_consistency_checks() читает rules и по type вызывает проверки;
--   — referential / referential_composite — ссылочная целостность по листам;
--   — unique — _run_unique_check; field_length — _run_field_length_check;
--   — field_format — _run_field_format_check (в этом SQL-файле не зеркалируется);
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
-- Краткий глоссарий по конструкциям SQL в этом файле (для чтения кода ниже)
-- ---------------------------------------------------------------------------
-- SELECT …        — перечень колонок результата (константы, поля таблиц, выражения).
-- FROM …          — источник строк (таблица или имя CTE); алиас (g, rl) сокращает имя.
-- LEFT JOIN …     — присоединить справа; если пары по ON нет, колонки справа = NULL.
-- ON …            — условие совпадения строк левой и правой частей JOIN.
-- WHERE …         — отбор строк после JOIN; AND объединяет условия (все должны выполняться).
-- GROUP BY …      — свёртка в группы с одинаковым ключом; часто с COUNT(*) в SELECT.
-- HAVING …        — фильтр уже по группам (например COUNT(*) > 1 = есть дубликаты).
-- UNION ALL       — склеить два набора строк без удаления дубликатов.
-- CAST(x AS T)    — приведение типа (часто к STRING для TRIM/CONCAT/RLIKE).
-- TRIM(s)         — убрать пробелы по краям строки.
-- LENGTH(s)       — длина строки в символах (после CAST в STRING).
-- RLIKE 'шаблон'  — проверка на соответствие регулярному выражению (Hive/Spark).
-- CONCAT / CONCAT_WS — склейка строк; CONCAT_WS(разделитель, a, b, …).
-- DISTINCT        — уникальные комбинации выбранных колонок.
-- (SELECT …)      — скалярный подзапрос, например COUNT(*) для одной проверки.
-- ---------------------------------------------------------------------------
-- dim_* / base_* — оптимизация: меньше повторных сканов одних и тех же таблиц
-- ---------------------------------------------------------------------------
-- Справочники по ключам совпадают с JOIN к полным таблицам: «ключ есть», если
-- в исходной таблице есть хотя бы одна строка с таким значением (DISTINCT).
-- dim_group_raw / dim_reward_link_raw — полные набора строк для последующих JOIN
-- без второго чтения той же таблицы в composite и (где уместно) referential.
-- base_schedule_ref — один проход по расписанию + три справочника для scenario_1/16/20.
-- Все строки GROUP (нужны и для ссылок по парам, и для unique по тройке).
dim_group_raw AS (
    SELECT CONTEST_CODE, GROUP_CODE, GROUP_VALUE FROM spod_dq.t_group
),
-- Множество кодов конкурсов, встречающихся в GROUP (для referential по CONTEST_CODE).
dim_group_contest_code AS (
    SELECT DISTINCT CONTEST_CODE FROM dim_group_raw
),
-- Множество пар (CONTEST_CODE, GROUP_CODE) в GROUP (для composite).
dim_group_contest_group_pair AS (
    SELECT DISTINCT CONTEST_CODE, GROUP_CODE FROM dim_group_raw
),
-- Все строки REWARD-LINK (для нескольких проверок без повторного чтения файла/таблицы).
dim_reward_link_raw AS (
    SELECT CONTEST_CODE, GROUP_CODE, REWARD_CODE FROM spod_dq.t_reward_link
),
-- Уникальные REWARD_CODE, присутствующие в связях (справочник «коды в REWARD-LINK»).
dim_reward_link_reward_code AS (
    SELECT DISTINCT REWARD_CODE FROM dim_reward_link_raw
),
-- Уникальные пары (CONTEST_CODE, GROUP_CODE) в REWARD-LINK.
dim_reward_link_contest_group_pair AS (
    SELECT DISTINCT CONTEST_CODE, GROUP_CODE FROM dim_reward_link_raw
),
-- Уникальные CONTEST_CODE в CONTEST-DATA (справочник конкурсов).
dim_contest_code AS (
    SELECT DISTINCT CONTEST_CODE FROM spod_dq.t_contest_data
),
-- Уникальные CONTEST_CODE в INDICATOR.
dim_indicator_contest_code AS (
    SELECT DISTINCT CONTEST_CODE FROM spod_dq.t_indicator
),
-- Уникальные REWARD_CODE в справочнике REWARD.
dim_reward_code AS (
    SELECT DISTINCT REWARD_CODE FROM spod_dq.t_reward
),
-- Из расписания только два столбца (меньше данных, чем полная строка расписания).
dim_schedule_contest_tournament AS (
    SELECT CONTEST_CODE, TOURNAMENT_CODE FROM spod_dq.t_tournament_schedule
),
-- Уникальные пары (TOURNAMENT_CODE, CONTEST_CODE) в расписании.
dim_schedule_tournament_contest_pair AS (
    SELECT DISTINCT TOURNAMENT_CODE, CONTEST_CODE FROM dim_schedule_contest_tournament
),
-- Одна строка на строку расписания + три флага наличия кода в CONTEST-DATA / INDICATOR / GROUP.
base_schedule_ref AS (
    SELECT
        s.CONTEST_CODE,                                    -- код конкурса из расписания
        cd.CONTEST_CODE AS ref_contest_data,              -- NULL, если кода нет в CONTEST-DATA
        ind.CONTEST_CODE AS ref_indicator,                -- NULL, если кода нет среди INDICATOR
        grp.CONTEST_CODE AS ref_group                     -- NULL, если кода нет среди GROUP
    FROM dim_schedule_contest_tournament s               -- строки расписания (два поля)
    LEFT JOIN dim_contest_code cd                        -- есть ли CONTEST_CODE в конкурсах
        ON cd.CONTEST_CODE = s.CONTEST_CODE
    LEFT JOIN dim_indicator_contest_code ind             -- есть ли CONTEST_CODE в индикаторах
        ON ind.CONTEST_CODE = s.CONTEST_CODE
    LEFT JOIN dim_group_contest_code grp                 -- есть ли CONTEST_CODE в группах
        ON grp.CONTEST_CODE = s.CONTEST_CODE
),

-- ---------------------------------------------------------------------------
-- A. REFERENTIAL — ссылочная целостность «значение в колонке A должно быть в справочнике B»
-- ---------------------------------------------------------------------------
-- Приём: LEFT JOIN фактовой таблицы с справочником по ключу; в WHERE оставляем
-- строки, где в справочнике нет совпадения (правая часть NULL), но слева значение
-- считается «заполненным» (не NULL и не пустая строка после TRIM). Каждая такая
-- строка — одно нарушение и попадёт в DETAIL; COUNT по этому набору — в SUMMARY.
-- У каждого CTE: первая строка комментария — config rules[].id и name; тип — для consistency_checks.py.
-- Построчные пояснения к SELECT / FROM / JOIN / WHERE см. в v_ref_1_1 и v_ref_9; остальные v_ref_* — тот же шаблон.

-- id "ref_group_contest_code_in_contest_data" | name «Все CONTEST_CODE из GROUP существуют в CONTEST-DATE» | type referential (раньше 1.1)
v_ref_1_1 AS (
    SELECT
        CAST(g.CONTEST_CODE AS STRING) AS detail_key,                   -- значение, вызвавшее нарушение
        'GROUP.CONTEST_CODE отсутствует в CONTEST-DATA' AS detail_message
    FROM dim_group_raw g                                               -- факт: каждая строка GROUP
    LEFT JOIN dim_contest_code c                                        -- справочник: коды из CONTEST-DATA
        ON c.CONTEST_CODE = g.CONTEST_CODE                              -- совпадение по CONTEST_CODE
    WHERE g.CONTEST_CODE IS NOT NULL                                    -- слева код не NULL
      AND TRIM(CAST(g.CONTEST_CODE AS STRING)) <> ''                    -- и не пустая строка/пробелы
      AND c.CONTEST_CODE IS NULL                                        -- в справочнике пары не нашлось
),

-- id "ref_indicator_contest_code_in_contest_data" | name «Все CONTEST_CODE из INDICATOR существуют в CONTEST-DATE» | type referential (раньше 1.2)
v_ref_1_2 AS (
    SELECT
        CAST(i.CONTEST_CODE AS STRING) AS detail_key,
        'INDICATOR.CONTEST_CODE отсутствует в CONTEST-DATA' AS detail_message
    FROM spod_dq.t_indicator i                                          -- факт: строки INDICATOR
    LEFT JOIN dim_contest_code c
        ON c.CONTEST_CODE = i.CONTEST_CODE
    WHERE i.CONTEST_CODE IS NOT NULL
      AND TRIM(CAST(i.CONTEST_CODE AS STRING)) <> ''
      AND c.CONTEST_CODE IS NULL
),

-- id "ref_reward_link_contest_code_in_contest_data" | name «Все CONTEST_CODE из REWARD-LINK существуют в CONTEST-DATE» | type referential (раньше 1.3)
v_ref_1_3 AS (
    SELECT
        CAST(rl.CONTEST_CODE AS STRING) AS detail_key,
        'REWARD-LINK.CONTEST_CODE отсутствует в CONTEST-DATA' AS detail_message
    FROM dim_reward_link_raw rl                                         -- факт: строки REWARD-LINK
    LEFT JOIN dim_contest_code c
        ON c.CONTEST_CODE = rl.CONTEST_CODE
    WHERE rl.CONTEST_CODE IS NOT NULL
      AND TRIM(CAST(rl.CONTEST_CODE AS STRING)) <> ''
      AND c.CONTEST_CODE IS NULL
),

-- id "ref_reward_link_reward_code_in_reward" | name «Все REWARD_CODE из REWARD-LINK существуют в REWARD» | type referential (раньше 2)
v_ref_2 AS (
    SELECT
        CAST(rl.REWARD_CODE AS STRING) AS detail_key,
        'REWARD-LINK.REWARD_CODE отсутствует в REWARD' AS detail_message
    FROM dim_reward_link_raw rl
    LEFT JOIN dim_reward_code r                                         -- множество кодов наград в REWARD
        ON r.REWARD_CODE = rl.REWARD_CODE
    WHERE rl.REWARD_CODE IS NOT NULL
      AND TRIM(CAST(rl.REWARD_CODE AS STRING)) <> ''
      AND r.REWARD_CODE IS NULL
),

-- id "ref_employee_org_unit_code_in_org_unit_v20" | name «Все ORG_UNIT_CODE из EMPLOYEE существуют в ORG_UNIT_V20» | type referential (раньше 9)
v_ref_9 AS (
    SELECT
        CAST(e.ORG_UNIT_CODE AS STRING) AS detail_key,
        'EMPLOYEE.ORG_UNIT_CODE отсутствует в ORG_UNIT_V20' AS detail_message
    FROM spod_dq.t_employee e                                           -- факт: сотрудники
    LEFT JOIN spod_dq.t_org_unit_v20 o                                  -- справочник подразделений
        ON o.ORG_UNIT_CODE = e.ORG_UNIT_CODE                            -- связь по коду подразделения
    WHERE e.ORG_UNIT_CODE IS NOT NULL                                   -- у сотрудника код задан
      AND TRIM(CAST(e.ORG_UNIT_CODE AS STRING)) <> ''                   -- не считаем пустым после обрезки пробелов
      AND o.ORG_UNIT_CODE IS NULL                                       -- в ORG_UNIT_V20 строки с таким кодом нет
),

-- Сценарные проверки: TOURNAMENT-SCHEDULE → разные справочники (как в config).

-- id "scenario_1" | name «Все CONTEST_CODE из TOURNAMENT-SHEDULE существуют в CONTEST-DATE» | type referential
-- (строки из base_schedule_ref — один проход по расписанию для 1 / 16 / 20)
v_ref_scenario_1 AS (
    SELECT
        CAST(b.CONTEST_CODE AS STRING) AS detail_key,
        'TOURNAMENT-SCHEDULE.CONTEST_CODE отсутствует в CONTEST-DATA' AS detail_message
    FROM base_schedule_ref b                                             -- уже посчитанные JOIN к трём справочникам
    WHERE b.CONTEST_CODE IS NOT NULL
      AND TRIM(CAST(b.CONTEST_CODE AS STRING)) <> ''
      AND b.ref_contest_data IS NULL                                    -- колонка из JOIN с dim_contest_code пустая
),

-- id "scenario_16" | name «Все CONTEST_CODE из TOURNAMENT-SCHEDULE существуют в INDICATOR» | type referential
v_ref_scenario_16 AS (
    SELECT
        CAST(b.CONTEST_CODE AS STRING) AS detail_key,
        'TOURNAMENT-SCHEDULE.CONTEST_CODE отсутствует в INDICATOR' AS detail_message
    FROM base_schedule_ref b
    WHERE b.CONTEST_CODE IS NOT NULL
      AND TRIM(CAST(b.CONTEST_CODE AS STRING)) <> ''
      AND b.ref_indicator IS NULL                                       -- нет такого CONTEST_CODE в INDICATOR
),

-- id "scenario_20" | name «Все CONTEST_CODE из TOURNAMENT-SCHEDULE существуют в GROUP» | type referential
v_ref_scenario_20 AS (
    SELECT
        CAST(b.CONTEST_CODE AS STRING) AS detail_key,
        'TOURNAMENT-SCHEDULE.CONTEST_CODE отсутствует в GROUP' AS detail_message
    FROM base_schedule_ref b
    WHERE b.CONTEST_CODE IS NOT NULL
      AND TRIM(CAST(b.CONTEST_CODE AS STRING)) <> ''
      AND b.ref_group IS NULL                                           -- нет такого CONTEST_CODE в GROUP
),

-- id "ref_contest_data_group" | name «Все CONTEST_CODE из CONTEST-DATE существуют в GROUP» | type referential
v_ref_contest_data_group AS (
    SELECT
        CAST(c.CONTEST_CODE AS STRING) AS detail_key,
        'CONTEST-DATA.CONTEST_CODE отсутствует в GROUP' AS detail_message
    FROM spod_dq.t_contest_data c                                       -- факт: конкурсы
    LEFT JOIN dim_group_contest_code g                                  -- коды конкурсов, встречающиеся в GROUP
        ON g.CONTEST_CODE = c.CONTEST_CODE
    WHERE c.CONTEST_CODE IS NOT NULL
      AND TRIM(CAST(c.CONTEST_CODE AS STRING)) <> ''
      AND g.CONTEST_CODE IS NULL
),

-- id "ref_indicator_group" | name «Все CONTEST_CODE из INDICATOR существуют в GROUP» | type referential
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

-- id "ref_report_contest_data" | name «Все CONTEST_CODE из REPORT существуют в CONTEST-DATE» | type referential
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

-- id "ref_reward_reward_link" | name «Все REWARD_CODE из REWARD существуют в REWARD-LINK» | type referential
v_ref_reward_reward_link AS (
    SELECT
        CAST(rw.REWARD_CODE AS STRING) AS detail_key,
        'REWARD.REWARD_CODE отсутствует в REWARD-LINK' AS detail_message
    FROM spod_dq.t_reward rw                                             -- факт: справочник наград
    LEFT JOIN dim_reward_link_reward_code rl                              -- какие REWARD_CODE встречаются в связях
        ON rl.REWARD_CODE = rw.REWARD_CODE
    WHERE rw.REWARD_CODE IS NOT NULL
      AND TRIM(CAST(rw.REWARD_CODE AS STRING)) <> ''
      AND rl.REWARD_CODE IS NULL
),

-- ---------------------------------------------------------------------------
-- B. REFERENTIAL_COMPOSITE — целостность по составному ключу (две и более колонок)
-- ---------------------------------------------------------------------------
-- Отличие от раздела A: JOIN задаётся по паре/набору полей; «осиротевшие» комбинации
-- ищутся тем же LEFT JOIN + проверка NULL на стороне справочника.

-- id "ref_composite_reward_link_pair_in_group" | name «Все пары CONTEST_CODE, GROUP_CODE из REWARD-LINK существуют в GROUP» | type referential_composite (раньше 5)
v_comp_5 AS (
    SELECT
        CONCAT_WS('|', CAST(rl.CONTEST_CODE AS STRING), CAST(rl.GROUP_CODE AS STRING)) AS detail_key,  -- ключ нарушения: пара через |
        'Пара CONTEST_CODE+GROUP_CODE из REWARD-LINK отсутствует в GROUP' AS detail_message
    FROM dim_reward_link_raw rl                                          -- каждая связь награда–группа
    LEFT JOIN dim_group_contest_group_pair g                              -- все допустимые пары из GROUP
        ON g.CONTEST_CODE = rl.CONTEST_CODE                               -- совпадение по двум полям
       AND g.GROUP_CODE = rl.GROUP_CODE
    WHERE (rl.CONTEST_CODE IS NOT NULL AND TRIM(CAST(rl.CONTEST_CODE AS STRING)) <> '')
      AND (rl.GROUP_CODE IS NOT NULL AND TRIM(CAST(rl.GROUP_CODE AS STRING)) <> '')
      AND g.CONTEST_CODE IS NULL                                         -- пары в GROUP не нашлось
),

-- id "ref_composite_group_reward_link" | name «Все пары CONTEST_CODE, GROUP_CODE из GROUP существуют в REWARD-LINK» | type referential_composite
v_comp_grp_rl AS (
    SELECT
        CONCAT_WS('|', CAST(g.CONTEST_CODE AS STRING), CAST(g.GROUP_CODE AS STRING)) AS detail_key,
        'Пара из GROUP отсутствует в REWARD-LINK' AS detail_message
    FROM dim_group_raw g                                                 -- каждая строка GROUP
    LEFT JOIN dim_reward_link_contest_group_pair rl                       -- пары (конкурс, группа) из REWARD-LINK
        ON rl.CONTEST_CODE = g.CONTEST_CODE
       AND rl.GROUP_CODE = g.GROUP_CODE
    WHERE (g.CONTEST_CODE IS NOT NULL AND TRIM(CAST(g.CONTEST_CODE AS STRING)) <> '')
      AND (g.GROUP_CODE IS NOT NULL AND TRIM(CAST(g.GROUP_CODE AS STRING)) <> '')
      AND rl.CONTEST_CODE IS NULL                                        -- в связях такой пары нет
),

-- id "ref_composite_report_schedule" | name «Все пары TOURNAMENT_CODE, CONTEST_CODE из REPORT существуют в TOURNAMENT-SCHEDULE» | type referential_composite
v_comp_rep_sch AS (
    SELECT
        CONCAT_WS('|', CAST(r.TOURNAMENT_CODE AS STRING), CAST(r.CONTEST_CODE AS STRING)) AS detail_key,
        'Пара из REPORT отсутствует в TOURNAMENT-SCHEDULE' AS detail_message
    FROM spod_dq.t_report r                                              -- строки отчёта
    LEFT JOIN dim_schedule_tournament_contest_pair s                      -- пары турнир+конкурс в расписании
        ON s.TOURNAMENT_CODE = r.TOURNAMENT_CODE
       AND s.CONTEST_CODE = r.CONTEST_CODE
    WHERE (r.TOURNAMENT_CODE IS NOT NULL AND TRIM(CAST(r.TOURNAMENT_CODE AS STRING)) <> '')
      AND (r.CONTEST_CODE IS NOT NULL AND TRIM(CAST(r.CONTEST_CODE AS STRING)) <> '')
      AND s.TOURNAMENT_CODE IS NULL                                      -- в расписании пары нет
),

-- ---------------------------------------------------------------------------
-- C. UNIQUE — уникальность бизнес-ключа (дубликаты в таблице)
-- ---------------------------------------------------------------------------
-- Логика: внутренний подзапрос (алиас x) — GROUP BY по бизнес-ключу, HAVING COUNT(*)>1
-- оставляет только ключи, по которым больше одной строки в таблице. Внешний SELECT
-- формирует detail_key / detail_message из полей x.
-- В DETAIL одна строка = один дублирующийся ключ (не каждая физическая строка Excel).
-- Остальные v_uq_* ниже устроены так же, меняются таблица, ключ и тексты.

-- id "unique_group_contest_code_group_code_group_value" | name «В GROUP нет дублей по составному полю CONTEST_CODE, GROUP_CODE, GROUP_VALUE» | type unique (раньше 3)
v_uq_3 AS (
    SELECT
        CONCAT_WS('|', CAST(x.CONTEST_CODE AS STRING), CAST(x.GROUP_CODE AS STRING), CAST(x.GROUP_VALUE AS STRING)) AS detail_key,
        CONCAT('Дубликат по (CONTEST_CODE, GROUP_CODE, GROUP_VALUE): строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        -- Подсчёт строк на каждую тройку ключа; HAVING отсекает уникальные ключи
        SELECT CONTEST_CODE, GROUP_CODE, GROUP_VALUE, COUNT(*) AS cnt
        FROM dim_group_raw
        GROUP BY CONTEST_CODE, GROUP_CODE, GROUP_VALUE
        HAVING COUNT(*) > 1
    ) x
),

-- id "unique_reward_link_contest_code_group_code_reward_code" | name «В REWARD-LINK нет дублей по составному полю CONTEST_CODE, GROUP_CODE, REWARD_CODE» | type unique (раньше 4)
v_uq_4 AS (
    SELECT
        CONCAT_WS('|', CAST(x.CONTEST_CODE AS STRING), CAST(x.GROUP_CODE AS STRING), CAST(x.REWARD_CODE AS STRING)) AS detail_key,
        CONCAT('Дубликат по (CONTEST_CODE, GROUP_CODE, REWARD_CODE): строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        -- Шаблон unique: см. комментарии у v_uq_3 (GROUP BY ключа, HAVING COUNT(*)>1)
        SELECT CONTEST_CODE, GROUP_CODE, REWARD_CODE, COUNT(*) AS cnt
        FROM dim_reward_link_raw
        GROUP BY CONTEST_CODE, GROUP_CODE, REWARD_CODE
        HAVING COUNT(*) > 1
    ) x
),

-- id "unique_contest_data" | name «В CONTEST-DATA нет дублей по полю CONTEST_CODE» | type unique
v_uq_contest_data AS (
    SELECT
        CAST(x.CONTEST_CODE AS STRING) AS detail_key,
        CONCAT('Дубликат CONTEST_CODE: строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        -- unique: GROUP BY одного ключа, HAVING COUNT(*)>1 (подробно — у v_uq_3)
        SELECT CONTEST_CODE, COUNT(*) AS cnt
        FROM spod_dq.t_contest_data
        GROUP BY CONTEST_CODE
        HAVING COUNT(*) > 1
    ) x
),

-- id "unique_indicator_1" | name «В INDICATOR нет дублей по составному полю CONTEST_CODE, INDICATOR_ADD_CALC_TYPE, INDICATOR_CODE» | type unique
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

-- id "unique_indicator_n" | name «В INDICATOR нет дублей по полю N» | type unique
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

-- id "unique_report" | name «В REPORT нет дублей по составному полю MANAGER_PERSON_NUMBER, TOURNAMENT_CODE, CONTEST_CODE» | type unique
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

-- id "unique_reward" | name «В REWARD нет дублей по полю REWARD_CODE» | type unique
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

-- id "unique_reward_link_2" | name «В REWARD-LINK нет дублей по составному полю CONTEST_CODE, REWARD_CODE» | type unique
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

-- id "unique_reward_link_reward" | name «В REWARD-LINK нет дублей по полю REWARD_CODE» | type unique
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

-- id "unique_schedule_2" | name «В TOURNAMENT-SCHEDULE нет дублей по составному полю TOURNAMENT_CODE, CONTEST_CODE» | type unique
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

-- id "unique_schedule_1" | name «В TOURNAMENT-SCHEDULE нет дублей по полю TOURNAMENT_CODE» | type unique
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

-- id "unique_org_unit" | name «В ORG_UNIT_V20 нет дублей по полю ORG_UNIT_CODE» | type unique
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

-- id "unique_tb_gosb" | name «В ORG_UNIT_V20 нет дублей по составному полю TB_CODE, GOSB_CODE» | type unique
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

-- id "unique_user_role" | name «В USER_ROLE нет дублей по полю RULE_NUM» | type unique
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

-- id "unique_user_role_sb" | name «В USER_ROLE SB нет дублей по полю RULE_NUM» | type unique (лист "USER_ROLE SB" → t_user_role_sb)
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

-- id "unique_employee_person" | name «В EMPLOYEE нет дублей по полю PERSON_NUMBER» | type unique
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

-- id "unique_employee_person_add" | name «В EMPLOYEE нет дублей по полю PERSON_NUMBER_ADD» | type unique
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

-- id "unique_employee_kpk_gosb" | name «В EMPLOYEE нет дублей по POSITION_NAME, KPK_CODE, ORG_UNIT_CODE среди строк с POSITION_NAME=КПК и непустым KPK_CODE» | type unique
v_uq_emp_kpk AS (
    SELECT
        CONCAT_WS('|', CAST(x.POSITION_NAME AS STRING), CAST(x.KPK_CODE AS STRING), CAST(x.ORG_UNIT_CODE AS STRING)) AS detail_key,
        CONCAT('Дубликат (КПК, KPK_CODE, ORG_UNIT_CODE): строк=', CAST(x.cnt AS STRING)) AS detail_message
    FROM (
        SELECT POSITION_NAME, KPK_CODE, ORG_UNIT_CODE, COUNT(*) AS cnt
        FROM spod_dq.t_employee
        WHERE POSITION_NAME = 'КПК'                                    -- область проверки: только должность КПК
          AND KPK_CODE IS NOT NULL
          AND TRIM(CAST(KPK_CODE AS STRING)) NOT IN ('', '-')            -- KPK_CODE считается заполненным
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
-- Три ветки UNION ALL: одна таблица, разные поля и лимиты длины (каждая ветка — свой WHERE).
v_fl_org AS (
    SELECT
        CONCAT_WS(':', CAST(ORG_UNIT_CODE AS STRING), 'TB_FULL_NAME') AS detail_key,
        CONCAT('Длина TB_FULL_NAME=', CAST(LENGTH(CAST(TB_FULL_NAME AS STRING)) AS STRING), ' > 100') AS detail_message
    FROM spod_dq.t_org_unit_v20
    WHERE LENGTH(CAST(TB_FULL_NAME AS STRING)) > 100                     -- длина строки после приведения к STRING
    UNION ALL                                                              -- объединить строки нарушений по полям
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

-- id "field_length_employee" | name «Поле PERSON_NUMBER в EMPLOYEE должно быть =20; PERSON_NUMBER_ADD =20» | type field_length
v_fl_emp AS (
    SELECT
        CONCAT('PERSON_NUMBER=', CAST(PERSON_NUMBER AS STRING)) AS detail_key,
        CONCAT('Длина PERSON_NUMBER=', CAST(LENGTH(CAST(PERSON_NUMBER AS STRING)) AS STRING), ' (ожидается 20)') AS detail_message
    FROM spod_dq.t_employee
    WHERE LENGTH(CAST(PERSON_NUMBER AS STRING)) <> 20                    -- строго 20 символов
    UNION ALL
    SELECT
        CONCAT('PERSON_NUMBER_ADD=', CAST(PERSON_NUMBER_ADD AS STRING)),
        CONCAT('Длина PERSON_NUMBER_ADD=', CAST(LENGTH(CAST(PERSON_NUMBER_ADD AS STRING)) AS STRING), ' (ожидается 20)')
    FROM spod_dq.t_employee
    WHERE LENGTH(CAST(PERSON_NUMBER_ADD AS STRING)) <> 20
),

-- id "field_length_report" | name «Поле MANAGER_PERSON_NUMBER в REPORT должно быть =20» | type field_length
v_fl_rep AS (
    SELECT
        CONCAT_WS('|', CAST(MANAGER_PERSON_NUMBER AS STRING), CAST(TOURNAMENT_CODE AS STRING), CAST(CONTEST_CODE AS STRING)) AS detail_key,
        CONCAT('Длина MANAGER_PERSON_NUMBER=', CAST(LENGTH(CAST(MANAGER_PERSON_NUMBER AS STRING)) AS STRING), ' (ожидается 20)') AS detail_message
    FROM spod_dq.t_report
    WHERE LENGTH(CAST(MANAGER_PERSON_NUMBER AS STRING)) <> 20
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
-- Шаблон строки: скаляр (SELECT COUNT(*) FROM v_*) = число нарушений.
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

-- ---------------------------------------------------------------------------
-- chk_detail — единый поток всех нарушений (построчно)
-- ---------------------------------------------------------------------------
-- UNION ALL склеивает наборы строк из всех v_*; если проверка чистая, её CTE пустой и
-- в итог не даёт строк. Каждая ветка: две колонки из одного CTE нарушений.
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

-- ---------------------------------------------------------------------------
-- Итоговый SELECT — «два отчёта в одной таблице результата»
-- ---------------------------------------------------------------------------
-- Первая часть UNION ALL: только сводка (passed, violation_count); detail_key/detail_message NULL.
-- Вторая часть: только детали; passed и violation_count NULL, чтобы типы колонок совпадали.
-- result_order гарантирует, что при ORDER BY сначала пойдут все SUMMARY, затем все DETAIL.
SELECT
    1 AS result_order,                                                 -- сортировка: сначала блок сводки
    CAST('SUMMARY' AS STRING) AS report_section,                       -- метка типа строки результата
    CAST(CASE WHEN s.violation_count = 0 THEN 1 ELSE 0 END AS BIGINT) AS passed,  -- 1 = без нарушений, 0 = есть
    CAST(s.violation_count AS BIGINT) AS violation_count,               -- сколько строк нарушений по правилу
    CAST(NULL AS STRING) AS detail_key,                                  -- в сводке деталей нет
    CAST(NULL AS STRING) AS detail_message
FROM chk_summary s

-- UNION ALL не убирает дубликаты и не сортирует; просто дописывает строки второй выборки к первой.
UNION ALL

SELECT
    2 AS result_order,                                                 -- после всех SUMMARY идут DETAIL
    CAST('DETAIL' AS STRING) AS report_section,
    CAST(NULL AS BIGINT) AS passed,                                     -- в деталях флаги сводки не заполняем
    CAST(NULL AS BIGINT) AS violation_count,
    d.detail_key,                                                      -- краткий ключ проблемной записи
    d.detail_message                                                   -- пояснение для аналитика
FROM chk_detail d

-- ORDER BY: сначала все строки с result_order=1 (SUMMARY), затем result_order=2 (DETAIL);
-- внутри — по detail_key для устойчивого порядка строк.
ORDER BY result_order, detail_key
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
--   Не перенесён тип field_format (все id format_*): проверка форматов полей выполняется только в Python.
--   Проверка csv_columns_count задаётся отдельно в конфиге, не дублируется в этом SQL.
-- =============================================================================
