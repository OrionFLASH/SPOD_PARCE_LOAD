/* зеркало catalog.json — sync_web_fill_catalog.py */
window.PARAM_REVIEW_CATALOG = {
  "version": 2,
  "generated_at": "2026-08-16T17:32:07Z",
  "source": "schema + field_meta + web-edit (BUSINESS_BLOCK list KMMMB) + JSON arrays CONTEST_PERIOD / FILTER_PERIOD_ARR / INDICATOR_FILTER / SCHEDULE TARGET_TYPE + json_required + table JSON column shells",
  "sections": [
    {
      "id": "CONTEST",
      "title": "CONTEST",
      "menu_label": "CONTEST",
      "intro": "Таблица / лист CONTEST-DATA — плоские колонки конкурса",
      "kind": "table",
      "parent": null,
      "sheet": "CONTEST-DATA",
      "fields": [
        {
          "n": 1,
          "key": "CONTEST_CODE",
          "status": "[v]",
          "label": "Код конкурса",
          "description": "Уникальный код конкурса (ключ связей). Пример: 01_2025-0_11-1_1",
          "kind": "text",
          "variants": [],
          "default": "",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 2,
          "key": "FULL_NAME",
          "status": "[v]",
          "label": "Название конкурса",
          "description": "Отображаемое название конкурса/турнира (на странице Турниры и Детальная карточка турнира).",
          "kind": "text",
          "variants": [],
          "default": "",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 3,
          "key": "CREATE_DT",
          "status": "[v]",
          "label": "Дата создания конкурса",
          "description": "Дата начала действия конкурса. Почти всегда = начало года.",
          "kind": "date",
          "variants": [],
          "default": "2026-01-01",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 4,
          "key": "CLOSE_DT",
          "status": "[v]",
          "label": "Срок действия конкурса",
          "description": "Дата окончания действия конкурса; Почти всегда 4000-01-01 = без срока. формат: YYYY-MM-DD",
          "kind": "date",
          "variants": [],
          "default": "4000-01-01",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 5,
          "key": "BUSINESS_STATUS",
          "status": "[v]",
          "label": "Бизнес-статус",
          "description": "Статус работы конкурса Активный или Архивный (значение по умолчанию:АКТИВНЫЙ)",
          "kind": "dropdown",
          "variants": [
            "АКТИВНЫЙ",
            "АРХИВНЫЙ"
          ],
          "default": "АКТИВНЫЙ",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 6,
          "key": "CONTEST_TYPE",
          "status": "[v]",
          "label": "Тип конкурса",
          "description": "ТУРНИРНЫЙ (соревнование \"будь лучше других\") (разыгрываем от 1 до 3 сезонных наград Золото Серебро Бронза)  |  ИНДИВИДУАЛЬНЫЙ  |  ИНДИВИДУАЛЬНЫЙ НАКОПИТЕЛЬНЫЙ (режим \"достигни результат\", получи одну награду).",
          "kind": "dropdown",
          "variants": [
            "ТУРНИРНЫЙ",
            "ИНДИВИДУАЛЬНЫЙ",
            "ИНДИВИДУАЛЬНЫЙ НАКОПИТЕЛЬНЫЙ"
          ],
          "default": "ТУРНИРНЫЙ",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 7,
          "key": "CONTEST_DESCRIPTION",
          "status": "[v]",
          "label": "Описание турнира",
          "description": "Текст описания для конкурса/турнира для траницы Детальная карточка турнира",
          "kind": "text",
          "variants": [],
          "default": "",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 8,
          "key": "CONTEST_FEATURE",
          "status": "[v]",
          "label": "CONTEST_FEATURE (JSON)",
          "description": "Колонка CONTEST_FEATURE: JSON-объект особенностей конкурса. Пустота ячейки — флаг «можно пусто». Ключи — в разделе CONTEST_FEATURE.",
          "kind": "json",
          "variants": [],
          "default": "",
          "allow_empty": false,
          "json_target": "CONTEST_FEATURE",
          "note": "Колонка CSV с SPOD-JSON. Редактируются подпись, описание, заметка и «можно пусто». Ключи — в дочернем разделе JSON."
        },
        {
          "n": 9,
          "key": "SHOW_INDICATOR",
          "status": "[v]",
          "label": "Единицы измерения показателя",
          "description": "Единица/подпись индикатора: шт.  |  Факт  |  %  |  … на списке показателей подпись к единицам данных",
          "kind": "dropdown_custom",
          "variants": [
            "%",
            "шт.",
            "Факт",
            "балл",
            "Темп %",
            "Ранг %%",
            "Ср. балл",
            "млн руб.",
            "К-во, шт.",
            "категория",
            "тыс. руб.",
            "Сумма, руб.",
            "Факт, млн руб.",
            "Сумма, млн руб.",
            "сборы, млн руб.",
            "Сумма, тыс. руб.",
            "Интегральный ранг",
            "Прирост, млн руб.",
            "Прирост, тыс. руб.",
            "Комиссия, тыс. руб.",
            "нетто-притоки, млн руб."
          ],
          "default": "Факт",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 10,
          "key": "PRODUCT_GROUP",
          "status": "[v]",
          "label": "Группа продукта",
          "description": "Группа продукта (общее направление)",
          "kind": "text",
          "variants": [],
          "default": "",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 11,
          "key": "PRODUCT",
          "status": "[v]",
          "label": "Продукт",
          "description": "Продукт / тематика конкурса.",
          "kind": "text",
          "variants": [],
          "default": "",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 12,
          "key": "CONTEST_SUBJECT",
          "status": "[v]",
          "label": "Кто соревнуется",
          "description": "Субъект конкурса (подразделение, сотрудник). Уровень группировки результатов.",
          "kind": "dropdown",
          "variants": [
            "EMPLOYEE",
            "UNIT"
          ],
          "default": "EMPLOYEE",
          "allow_empty": false,
          "json_target": "",
          "note": "",
          "variant_labels": [
            "Сотрудники",
            "Подразделения"
          ]
        },
        {
          "n": 13,
          "key": "FACTOR_MARK_TYPE",
          "status": "[v]",
          "label": "Как выбираем победителей",
          "description": "Способ выбора победителей: достиг показателя, сделал больше других или меньше других — меньше, например, для ранга",
          "kind": "dropdown",
          "variants": [
            "CRITERION",
            "RATING_MAX",
            "RATING_MIN",
            "GAIN"
          ],
          "default": "CRITERION",
          "allow_empty": false,
          "json_target": "",
          "note": "",
          "variant_labels": [
            "Достигни",
            "Больше - лучше",
            "Меньше - лучше",
            "Больше в X раз"
          ]
        },
        {
          "n": 14,
          "key": "CONTEST_INDICATOR_METHOD",
          "status": "[v]",
          "label": "Метод индикатора",
          "description": "Метод расчета показателя конкурса: интегральный (по умолчанию) / отношение агрегированных значений.",
          "kind": "dropdown",
          "variants": [
            "INTEGRAL",
            "RELATION"
          ],
          "default": "INTEGRAL",
          "allow_empty": false,
          "json_target": "",
          "note": "",
          "variant_labels": [
            "Интегральный",
            "Отношение агрегатов"
          ]
        },
        {
          "n": 15,
          "key": "CONTEST_FACTOR_METHOD",
          "status": "[v]",
          "label": "Метод расчета показателя",
          "description": "Способ расчета показателя. FACT — ручные данные; остальные — автоматические турниры (прирост / run rate).",
          "kind": "dropdown",
          "variants": [
            "FACT",
            "FACT0-FACT1",
            "RUN_RATE",
            "RUN_RATE-FACT1",
            "FACT0-RUN_RATE1_DOWN",
            "RUN_RATE/FACT1",
            "FACT0/RUN_RATE1_DOWN"
          ],
          "default": "FACT",
          "allow_empty": false,
          "json_target": "",
          "note": "",
          "variant_labels": [
            "Факт",
            "Прирост",
            "Run rate",
            "Run rate прирост",
            "Run rate отклонение",
            "Run rate % прироста",
            "Run rate % отклонения"
          ]
        },
        {
          "n": 16,
          "key": "PLAN_METHOD_CODE",
          "status": "[v]",
          "label": "Как вычисляется план",
          "description": "Как задаётся план: не задан / предустановленное значение (по умолчанию) / зависит от прошлого периода.",
          "kind": "dropdown",
          "variants": [
            "NOT_USED",
            "PRESET_VALUE",
            "DEPENDS_PREVIOUS_PERIOD"
          ],
          "default": "PRESET_VALUE",
          "allow_empty": false,
          "json_target": "",
          "note": "",
          "variant_labels": [
            "План не задан",
            "Предустановленное",
            "От прошлого периода"
          ]
        },
        {
          "n": 17,
          "key": "PLAN_MOD_METOD",
          "status": "[v]",
          "label": "Метод модификации плана",
          "description": "Модификатор плана от прошлого периода: умножить на коэффициент (по умолчанию) или добавить число.",
          "kind": "dropdown",
          "variants": [
            "MULTIPLIER",
            "APPEND"
          ],
          "default": "MULTIPLIER",
          "allow_empty": false,
          "json_target": "",
          "note": "",
          "variant_labels": [
            "× коэффициент",
            "+ число к прошлому"
          ]
        },
        {
          "n": 18,
          "key": "PLAN_MOD_VALUE",
          "status": "[v]",
          "label": "Значение плана",
          "description": "Значение планового показателя (0, 1, 1000, …)",
          "kind": "number",
          "variants": [],
          "default": "0",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 19,
          "key": "FACTOR_MATCH",
          "status": "[v]",
          "label": "Символ сравнения с планом",
          "description": "Вид сравнения показателя с планом для определения участников",
          "kind": "dropdown",
          "variants": [
            "=",
            ">",
            ">=",
            "<",
            "<="
          ],
          "default": ">=",
          "allow_empty": false,
          "json_target": "",
          "note": "",
          "variant_labels": [
            "Равно",
            "Больше",
            "Больше или равно",
            "Меньше",
            "Меньше или равно"
          ]
        },
        {
          "n": 20,
          "key": "CONTEST_PERIOD",
          "status": "[v]",
          "label": "Настройка периода",
          "description": "Колонка CONTEST_PERIOD: JSON-массив периодов. Пустота ячейки — флаг «можно пусто». Элементы — в разделе CONTEST_PERIOD.",
          "kind": "json",
          "variants": [],
          "default": "",
          "allow_empty": false,
          "json_target": "CONTEST_PERIOD",
          "note": "Колонка CSV с SPOD-JSON. Редактируются подпись, описание, заметка и «можно пусто». Ключи — в дочернем разделе JSON."
        },
        {
          "n": 21,
          "key": "TARGET_TYPE",
          "status": "[v]",
          "label": "Среда конкурса",
          "description": "Выбор среды конкурсной",
          "kind": "dropdown",
          "variants": [
            "ПРОМ",
            "ТЕСТ"
          ],
          "default": "ПРОМ",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 22,
          "key": "SOURCE_UPD_FREQUENCY",
          "status": "[v]",
          "label": "Частота обновления источника",
          "description": "Частота обновления источника данных в источника (не используется)",
          "kind": "dropdown_custom",
          "variants": [
            "1",
            "7",
            "14"
          ],
          "default": "7",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 23,
          "key": "CALC_TYPE",
          "status": "[v]",
          "label": "Тип расчёта показателя",
          "description": "Тип расчёта: 0 — промышленный расчет / 1 — ручной расчет (не используется)",
          "kind": "dropdown",
          "variants": [
            "0",
            "1"
          ],
          "default": "1",
          "allow_empty": false,
          "json_target": "",
          "note": "",
          "variant_labels": [
            "Промышленный расчет",
            "Ручные данные"
          ]
        },
        {
          "n": 24,
          "key": "FACT_POST_PROCESSING",
          "status": "[v]",
          "label": "Постобработка факта",
          "description": "Постобработка факта: процентили, уровень группы или число участников с лучшим результатом. Можно не указывать.",
          "kind": "dropdown",
          "variants": [
            "PERCENTILE",
            "PERCENTILE_DOWN",
            "PERCENTILE_UPEST",
            "PERCENTILE_UP",
            "SPECIAL_INDICATOR_1",
            "COUNT_BIGGER"
          ],
          "default": "",
          "allow_empty": true,
          "json_target": "",
          "note": "",
          "variant_labels": [
            "% «лучше чем»",
            "% «попал в»",
            "% «лучше меня»",
            "% «не хуже»",
            "Уровень группы",
            "Счётчик лучших"
          ]
        },
        {
          "n": 25,
          "key": "BUSINESS_BLOCK",
          "status": "[v]",
          "label": "Бизнес-блок конкурса",
          "description": "Бизнес-блок конкурса и его участников",
          "kind": "list",
          "variants": [
            "KMMMB",
            "KMKKSB",
            "AKMKKSB",
            "CSM"
          ],
          "default": "KMMMB",
          "allow_empty": false,
          "json_target": "",
          "note": "",
          "variant_labels": [
            "КМ ММБ",
            "КМ ККСБ",
            "АКМ ККСБ",
            "Руководитель по внедрению проектов"
          ]
        }
      ]
    },
    {
      "id": "CONTEST_FEATURE",
      "title": "CONTEST_FEATURE",
      "menu_label": "CONTEST_FEATURE",
      "intro": "JSON-колонка CONTEST_FEATURE внутри таблицы CONTEST",
      "kind": "json",
      "parent": "CONTEST",
      "sheet": "CONTEST-DATA",
      "fields": [
        {
          "n": 25,
          "key": "CONTEST_FEATURE.vid",
          "status": "[v]",
          "label": "Среда конкурса",
          "description": "Опредедяем среду для конкурса (по умолчанию ПРОМ)",
          "kind": "dropdown",
          "variants": [
            "ПРОМ",
            "ТЕСТ"
          ],
          "default": "ПРОМ",
          "allow_empty": false,
          "json_target": "CONTEST_FEATURE.vid",
          "note": "ПРОМ (как TARGET_TYPE).",
          "variant_labels": [
            "Промышленный",
            "Тестовый"
          ],
          "json_required": true
        },
        {
          "n": 26,
          "key": "CONTEST_FEATURE.accuracy",
          "status": "[v]",
          "label": "Округление до...",
          "description": "Точность/разрядность: 0  |  1  |  2 . (число знаков после запятой для отображения)",
          "kind": "dropdown",
          "variants": [
            "0",
            "1",
            "2"
          ],
          "default": "0",
          "allow_empty": false,
          "json_target": "CONTEST_FEATURE.accuracy",
          "note": "",
          "variant_labels": [
            "# ##",
            "# ##.#",
            "# ##.##"
          ],
          "json_required": true
        },
        {
          "n": 27,
          "key": "CONTEST_FEATURE.capacity",
          "status": "[v]",
          "label": "Приведение к млн / тыс.",
          "description": "Приведение отображаемого показателя к млн, к тыс.",
          "kind": "dropdown",
          "variants": [
            "MILLIONS",
            "THOUSANDS"
          ],
          "default": "",
          "allow_empty": true,
          "json_target": "CONTEST_FEATURE.capacity",
          "note": "",
          "variant_labels": [
            "в миллионах",
            "в тысячах"
          ],
          "json_required": true
        },
        {
          "n": 28,
          "key": "CONTEST_FEATURE.masking",
          "status": "[v]",
          "label": "masking",
          "description": "Всегда ставами в Нет (N)",
          "kind": "dropdown",
          "variants": [
            "Y",
            "N"
          ],
          "default": "N",
          "allow_empty": false,
          "json_target": "CONTEST_FEATURE.masking",
          "note": "N (часто N).",
          "variant_labels": [
            "Да",
            "Нет"
          ],
          "json_required": true
        },
        {
          "n": 29,
          "key": "CONTEST_FEATURE.minNumber",
          "status": "[v]",
          "label": "Минимум участников",
          "description": "Мин. число участников на уровне чтобы считать победителей (исключаем соревнование сам с собой): 1  |  2  |  3.",
          "kind": "dropdown",
          "variants": [
            "1",
            "2",
            "3"
          ],
          "default": "1",
          "allow_empty": false,
          "json_target": "CONTEST_FEATURE.minNumber",
          "note": "",
          "json_required": true
        },
        {
          "n": 30,
          "key": "CONTEST_FEATURE.momentRewarding",
          "status": "[v]",
          "label": "Когда выбираем победителей",
          "description": "Момент награждения после закрытия турнира / во время турнира",
          "kind": "dropdown",
          "variants": [
            "AFTER",
            "DURIN"
          ],
          "default": "AFTER",
          "allow_empty": false,
          "json_target": "CONTEST_FEATURE.momentRewarding",
          "note": "DURIN (после закрытия турнира / во время турнира)",
          "variant_labels": [
            "По завершению",
            "Во время"
          ],
          "json_required": true
        },
        {
          "n": 31,
          "key": "CONTEST_FEATURE.typeRewarding",
          "status": "[v]",
          "label": "Сколько наград получает",
          "description": "Вручаем одну из 3 наград или все",
          "kind": "dropdown",
          "variants": [
            "one",
            "all"
          ],
          "default": "one",
          "allow_empty": false,
          "json_target": "CONTEST_FEATURE.typeRewarding",
          "note": "",
          "variant_labels": [
            "Вручаем одну",
            "Вручаем все"
          ],
          "json_required": true
        },
        {
          "n": 32,
          "key": "CONTEST_FEATURE.avatarShow",
          "status": "[v]",
          "label": "Показывать аватар?",
          "description": "Управление показом фотографий участников",
          "kind": "dropdown",
          "variants": [
            "Y",
            "N"
          ],
          "default": "Y",
          "allow_empty": false,
          "json_target": "CONTEST_FEATURE.avatarShow",
          "note": "N.",
          "variant_labels": [
            "Да",
            "Нет"
          ],
          "json_required": true
        },
        {
          "n": 33,
          "key": "CONTEST_FEATURE.tournamentTeam",
          "status": "[v]",
          "label": "Признак командного конкурса",
          "description": "При выборе командного турнира соревнуются не сотрудники, а команды (КПК/ Отдел/ ГОСБ/ ТБ)",
          "kind": "dropdown",
          "variants": [
            "Y",
            "N"
          ],
          "default": "N",
          "allow_empty": false,
          "json_target": "CONTEST_FEATURE.tournamentTeam",
          "note": "N.",
          "variant_labels": [
            "Да",
            "Нет"
          ],
          "json_required": true
        },
        {
          "n": 34,
          "key": "CONTEST_FEATURE.persomanNumberVisible",
          "status": "[v]",
          "label": "Видимость для сотрудника",
          "description": "Если указаны табельные, то только эти сотрудники увидят турнир",
          "kind": "list",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "CONTEST_FEATURE.persomanNumberVisible",
          "note": "",
          "json_required": true
        },
        {
          "n": 35,
          "key": "CONTEST_FEATURE.persomanNumberHidden",
          "status": "[v]",
          "label": "Скрытие для сотрудников",
          "description": "Если указаны табельные, то эти сотрудники не увидят турнир",
          "kind": "list",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "CONTEST_FEATURE.persomanNumberHidden",
          "note": "",
          "json_required": true
        },
        {
          "n": 36,
          "key": "CONTEST_FEATURE.tournamentStartMailing",
          "status": "[v]",
          "label": "Письма о старте турнира",
          "description": "В дату старта турнира участникам из вертикали придет письмо с уведомлением о старте турнира",
          "kind": "dropdown",
          "variants": [
            "Y",
            "N"
          ],
          "default": "Y",
          "allow_empty": false,
          "json_target": "CONTEST_FEATURE.tournamentStartMailing",
          "note": "",
          "variant_labels": [
            "Да",
            "Нет"
          ],
          "json_required": true
        },
        {
          "n": 37,
          "key": "CONTEST_FEATURE.tournamentEndMailing",
          "status": "[v]",
          "label": "Письмо о завершении турнира",
          "description": "Когда турнир закроется и подведут итоги участникам не победившим придет письмо о закрытии турнира",
          "kind": "dropdown",
          "variants": [
            "Y",
            "N"
          ],
          "default": "Y",
          "allow_empty": false,
          "json_target": "CONTEST_FEATURE.tournamentEndMailing",
          "note": "",
          "variant_labels": [
            "Да",
            "Нет"
          ],
          "json_required": true
        },
        {
          "n": 38,
          "key": "CONTEST_FEATURE.tournamentLikeMailing",
          "status": "[v]",
          "label": "Письмо о лайке",
          "description": "Рассылка писем о лайках на новости с участием сотрудника",
          "kind": "dropdown",
          "variants": [
            "Y",
            "N"
          ],
          "default": "Y",
          "allow_empty": false,
          "json_target": "CONTEST_FEATURE.tournamentLikeMailing",
          "note": "",
          "variant_labels": [
            "Да",
            "Нет"
          ],
          "json_required": true
        },
        {
          "n": 39,
          "key": "CONTEST_FEATURE.tournamentListMailing",
          "status": "[w]",
          "label": "Письмо о...",
          "description": "описание готовится (по умолчанию пусто)",
          "kind": "list",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "CONTEST_FEATURE.tournamentListMailing",
          "note": "",
          "json_required": true
        },
        {
          "n": 40,
          "key": "CONTEST_FEATURE.tournamentRewardingMailing",
          "status": "[v]",
          "label": "Письмо о награждении",
          "description": "РАссылка письма получателям награды после ее вручения",
          "kind": "dropdown",
          "variants": [
            "Y",
            "N"
          ],
          "default": "Y",
          "allow_empty": false,
          "json_target": "CONTEST_FEATURE.tournamentRewardingMailing",
          "note": "N.",
          "variant_labels": [
            "Да",
            "Нет"
          ],
          "json_required": true
        },
        {
          "n": 41,
          "key": "CONTEST_FEATURE.feature",
          "status": "[v]",
          "label": "Особенности конкурса",
          "description": "Тексты особенностей турнира. Показываем в детальной карточке турнира, можно указать несколько",
          "kind": "list",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "CONTEST_FEATURE.feature",
          "note": "",
          "json_required": true
        },
        {
          "n": 42,
          "key": "CONTEST_FEATURE.businessBlock",
          "status": "[v]",
          "label": "Бизнес-блок",
          "description": "Бизнес блок конкурса и его участников",
          "kind": "list",
          "variants": [
            "KMMMB",
            "KMKKSB",
            "AKMKKSB",
            "CSM"
          ],
          "default": "KMMMB",
          "allow_empty": false,
          "json_target": "CONTEST_FEATURE.businessBlock",
          "note": "",
          "variant_labels": [
            "КМ ММБ",
            "КМ ККСБ",
            "АКМ ККСБ",
            "Руководитель по внедрению проектов"
          ],
          "json_required": true
        },
        {
          "n": 43,
          "key": "CONTEST_FEATURE.helpCodeList",
          "status": "[v]",
          "label": "Код окна с описанием показателя",
          "description": "Коды для вывода окна с дополнительным описанием конкурса (доступно в детальной карточке турнира)",
          "kind": "list",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "CONTEST_FEATURE.helpCodeList",
          "note": "",
          "json_required": true
        },
        {
          "n": 44,
          "key": "CONTEST_FEATURE.preferences",
          "status": "[v]",
          "label": "Преференции за победу",
          "description": "Преференции за победу в турнире если предусмотрены (отображаем в детальной карточке турнира)",
          "kind": "list",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "CONTEST_FEATURE.preferences",
          "note": "",
          "json_required": true
        },
        {
          "n": 45,
          "key": "CONTEST_FEATURE.tbVisible",
          "status": "[v]",
          "label": "Видимость для ТБ",
          "description": "Только эти ТБ (если указаны) увидят конкурс",
          "kind": "list",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "CONTEST_FEATURE.tbVisible",
          "note": "",
          "json_required": true
        },
        {
          "n": 46,
          "key": "CONTEST_FEATURE.tbHidden",
          "status": "[v]",
          "label": "Скрытие для ТБ",
          "description": "Конкурс видят все ТБ, кроме указанных здесь",
          "kind": "list",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "CONTEST_FEATURE.tbHidden",
          "note": "",
          "json_required": true
        },
        {
          "n": 47,
          "key": "CONTEST_FEATURE.gosbVisible",
          "status": "[v]",
          "label": "Видимость для ГОСБ",
          "description": "Только эти ГОСБ (если указаны) увидят конкурс",
          "kind": "list",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "CONTEST_FEATURE.gosbVisible",
          "note": "",
          "json_required": true
        },
        {
          "n": 48,
          "key": "CONTEST_FEATURE.gosbHidden",
          "status": "[v]",
          "label": "Скрытие для ГОСБ",
          "description": "Конкурс видят все ГОСБ, кроме указанных здесь",
          "kind": "list",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "CONTEST_FEATURE.gosbHidden",
          "note": "",
          "json_required": true
        }
      ],
      "column": "CONTEST_FEATURE"
    },
    {
      "id": "CONTEST_PERIOD",
      "title": "CONTEST_PERIOD",
      "menu_label": "CONTEST_PERIOD",
      "intro": "JSON-массив в колонке CONTEST_PERIOD (лист CONTEST). Элемент: period_code (число), criterion_mark_type (оператор), criterion_mark_value (число). В CSV: массив в кавычках поля; ключи/строки в \"\"\"…\"\"\", числа без кавычек.",
      "kind": "json_array",
      "parent": "CONTEST",
      "sheet": "CONTEST-DATA",
      "column": "CONTEST_PERIOD",
      "fields": [
        {
          "n": 120,
          "status": "[w]",
          "variants": [
            "0",
            "1",
            "-1"
          ],
          "default": "0",
          "allow_empty": false,
          "json_target": "CONTEST_PERIOD[].period_code",
          "note": "",
          "description": "Номер/код периода в массиве CONTEST_PERIOD (часто 0, 1; бывает -1).",
          "kind": "dropdown",
          "label": "Код периода",
          "key": "CONTEST_PERIOD.period_code",
          "json_required": true
        },
        {
          "n": 121,
          "status": "[v]",
          "variants": [
            ">",
            ">=",
            "<",
            "<=",
            "="
          ],
          "default": ">",
          "allow_empty": false,
          "json_target": "CONTEST_PERIOD[].criterion_mark_type",
          "note": "",
          "description": "Оператор сравнения критерия периода.",
          "kind": "dropdown",
          "label": "Сравнение",
          "key": "CONTEST_PERIOD.criterion_mark_type",
          "variant_labels": [
            "Больше",
            "Больше или равно",
            "Меньше",
            "Меньше или равно",
            "Равно"
          ],
          "json_required": true
        },
        {
          "n": 122,
          "status": "[v]",
          "variants": [],
          "default": "0",
          "allow_empty": false,
          "json_target": "CONTEST_PERIOD[].criterion_mark_value",
          "note": "",
          "description": "Числовое значение порога",
          "kind": "number",
          "label": "Порог критерия",
          "key": "CONTEST_PERIOD.criterion_mark_value",
          "json_required": true
        }
      ]
    },
    {
      "id": "REWARD",
      "title": "REWARD",
      "menu_label": "REWARD",
      "intro": "Таблица / лист REWARD — плоские колонки награды (в форме слоты BADGE)",
      "kind": "table",
      "parent": null,
      "sheet": "REWARD",
      "fields": [
        {
          "n": 49,
          "key": "REWARD_CODE",
          "status": "[v]",
          "label": "Код награды",
          "description": "Уникальный код награды, напр. r_01_2025-0_11-1_1_1.",
          "kind": "text",
          "variants": [],
          "default": "",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 50,
          "key": "REWARD_TYPE",
          "status": "[v]",
          "label": "Тип награды",
          "description": "Для этой формы всегда BADGE.",
          "kind": "dropdown",
          "variants": [
            "BADGE",
            "LABEL",
            "ITEM",
            "CRISTAL"
          ],
          "default": "BADGE",
          "allow_empty": false,
          "json_target": "",
          "note": "",
          "variant_labels": [
            "Награда",
            "Метка",
            "Товар",
            "Кристалл"
          ]
        },
        {
          "n": 51,
          "key": "FULL_NAME",
          "status": "[v]",
          "label": "Название награды",
          "description": "Краткое название бейджа для показа в списке наград и детальной карточке награды (достижения)",
          "kind": "text",
          "variants": [],
          "default": "",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 52,
          "key": "REWARD_DESCRIPTION",
          "status": "[v]",
          "label": "Описание награды",
          "description": "Полное описание награды показываем в детальной карточке награды (достижения)",
          "kind": "text",
          "variants": [],
          "default": "",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 53,
          "key": "REWARD_CONDITION",
          "status": "[v]",
          "label": "Условия получения награды",
          "description": "Условия получения награды (1-победа в конкурсе, 2 - участие в конкурсе)",
          "kind": "dropdown",
          "variants": [
            "1",
            "2"
          ],
          "default": "1",
          "allow_empty": false,
          "json_target": "",
          "note": "",
          "variant_labels": [
            "победа",
            "участие"
          ]
        },
        {
          "n": 54,
          "key": "REWARD_COST",
          "status": "[v]",
          "label": "Стоимость награды",
          "description": "Сколько кристаллов заработает участник за получение награды (целое число, по умолчанию 5).",
          "kind": "number",
          "variants": [],
          "default": "5",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 55,
          "key": "REWARD_ADD_DATA",
          "status": "[v]",
          "label": "REWARD_ADD_DATA (JSON)",
          "description": "Колонка REWARD_ADD_DATA: JSON-объект доп. данных награды. Пустота ячейки — флаг «можно пусто». Ключи — в разделе REWARD_ADD_DATA.",
          "kind": "json",
          "variants": [],
          "default": "",
          "allow_empty": false,
          "json_target": "REWARD_ADD_DATA",
          "note": "Колонка CSV с SPOD-JSON. Редактируются подпись, описание, заметка и «можно пусто». Ключи — в дочернем разделе JSON."
        }
      ]
    },
    {
      "id": "REWARD_ADD_DATA",
      "title": "REWARD_ADD_DATA",
      "menu_label": "REWARD_ADD_DATA",
      "intro": "JSON-колонка REWARD_ADD_DATA внутри таблицы REWARD",
      "kind": "json",
      "parent": "REWARD",
      "sheet": "REWARD",
      "fields": [
        {
          "n": 55,
          "key": "REWARD_ADD_DATA.nftFlg",
          "status": "[v]",
          "label": "Признак NFT",
          "description": "Ставить метку на награду / турнир?",
          "kind": "dropdown",
          "variants": [
            "Y",
            "N"
          ],
          "default": "N",
          "allow_empty": false,
          "json_target": "REWARD_ADD_DATA.nftFlg",
          "note": "N (обычно N).",
          "variant_labels": [
            "Да",
            "Нет"
          ],
          "json_required": true
        },
        {
          "n": 56,
          "key": "REWARD_ADD_DATA.outstanding",
          "status": "[v]",
          "label": "Выпуск новостей",
          "description": "При получении награды выходит новость с поздравлением а Сообществе",
          "kind": "dropdown",
          "variants": [
            "Y",
            "N"
          ],
          "default": "N",
          "allow_empty": false,
          "json_target": "REWARD_ADD_DATA.outstanding",
          "note": "N.",
          "variant_labels": [
            "Да",
            "Нет"
          ],
          "json_required": true
        },
        {
          "n": 57,
          "key": "REWARD_ADD_DATA.rewardRule",
          "status": "[v]",
          "label": "Правило получения",
          "description": "Текст правила получения награды для отображения в детальной карточке награды",
          "kind": "text",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "REWARD_ADD_DATA.rewardRule",
          "note": "",
          "json_required": true
        },
        {
          "n": 58,
          "key": "REWARD_ADD_DATA.rewardAgainGlobal",
          "status": "[v]",
          "label": "Повтор в другом турнире",
          "description": "Можно ли награду получить больше одного раза в разных турнирах",
          "kind": "dropdown",
          "variants": [
            "Y",
            "N"
          ],
          "default": "N",
          "allow_empty": false,
          "json_target": "REWARD_ADD_DATA.rewardAgainGlobal",
          "note": "N.",
          "variant_labels": [
            "Да",
            "Нет"
          ],
          "json_required": true
        },
        {
          "n": 59,
          "key": "REWARD_ADD_DATA.rewardAgainTournament",
          "status": "[v]",
          "label": "Повтор в текущем турнире",
          "description": "Можно ли награду получить больше одного раза в одном турнире (не используем всегда Нет (N))",
          "kind": "dropdown",
          "variants": [
            "Y",
            "N"
          ],
          "default": "N",
          "allow_empty": false,
          "json_target": "REWARD_ADD_DATA.rewardAgainTournament",
          "note": "N (часто N).",
          "variant_labels": [
            "Да",
            "Нет"
          ],
          "json_required": true
        },
        {
          "n": 60,
          "key": "REWARD_ADD_DATA.hidden",
          "status": "[v]",
          "label": "Скрыть награду",
          "description": "Скрывает награду во всех местах",
          "kind": "dropdown",
          "variants": [
            "Y",
            "N"
          ],
          "default": "N",
          "allow_empty": false,
          "json_target": "REWARD_ADD_DATA.hidden",
          "note": "N.",
          "variant_labels": [
            "Да",
            "Нет"
          ],
          "json_required": true
        },
        {
          "n": 61,
          "key": "REWARD_ADD_DATA.fileName",
          "status": "[v]",
          "label": "Техническое имя награды",
          "description": "Техническое наименование группы наград",
          "kind": "text",
          "variants": [],
          "default": "",
          "allow_empty": false,
          "json_target": "REWARD_ADD_DATA.fileName",
          "note": "",
          "json_required": true
        },
        {
          "n": 62,
          "key": "REWARD_ADD_DATA.teamNews",
          "status": "[v]",
          "label": "Текст групповой новости",
          "description": "Текст новости с выпуском по шаблону, когда победителей более одного",
          "kind": "text",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "REWARD_ADD_DATA.teamNews",
          "note": "",
          "json_required": true
        },
        {
          "n": 63,
          "key": "REWARD_ADD_DATA.singleNews",
          "status": "[v]",
          "label": "Текст одиночной новости",
          "description": "Текст новости с выпуском по шаблону, когда победитель один",
          "kind": "text",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "REWARD_ADD_DATA.singleNews",
          "note": "",
          "json_required": true
        },
        {
          "n": 64,
          "key": "REWARD_ADD_DATA.masterBadge",
          "status": "[v]",
          "label": "Выбор мастер бейджа",
          "description": "Признак является ли данная награда основной для вручения (Награда - Да (Y) / Турнир - Нет (N))",
          "kind": "dropdown",
          "variants": [
            "Y",
            "N"
          ],
          "default": "Y",
          "allow_empty": false,
          "json_target": "REWARD_ADD_DATA.masterBadge",
          "note": "N. (Y — для награды / N — для турнира)",
          "variant_labels": [
            "Да",
            "Нет"
          ],
          "json_required": true
        },
        {
          "n": 65,
          "key": "REWARD_ADD_DATA.parentRewardCode",
          "status": "[v]",
          "label": "Код Мастер-беджа",
          "description": "Код родительской (Мастер) награды. (Награда = коду награды / Турнир = коду сезонной награды)",
          "kind": "text",
          "variants": [],
          "default": "",
          "allow_empty": false,
          "json_target": "REWARD_ADD_DATA.parentRewardCode",
          "note": "",
          "json_required": true
        },
        {
          "n": 66,
          "key": "REWARD_ADD_DATA.priority",
          "status": "[v]",
          "label": "Приоритет награды",
          "description": "Признак отнесения вида награды к Золоту/Серебру/Бронзе",
          "kind": "dropdown",
          "variants": [
            "1",
            "2",
            "3"
          ],
          "default": "1",
          "allow_empty": false,
          "json_target": "REWARD_ADD_DATA.priority",
          "note": "",
          "variant_labels": [
            "Золото",
            "Серебро",
            "Бронза"
          ],
          "json_required": true
        },
        {
          "n": 67,
          "key": "REWARD_ADD_DATA.recommendationLevel",
          "status": "[v]",
          "label": "Уровень для рекомендаций",
          "description": "Уровень для отбора в рекомендательную систему",
          "kind": "dropdown",
          "variants": [
            "BANK",
            "TB",
            "GOSB",
            "NON"
          ],
          "default": "BANK",
          "allow_empty": false,
          "json_target": "REWARD_ADD_DATA.recommendationLevel",
          "note": "",
          "variant_labels": [
            "Страна",
            "Тер. банк",
            "ГОСБ",
            "нет"
          ],
          "json_required": true
        },
        {
          "n": 68,
          "key": "REWARD_ADD_DATA.refreshOldNews",
          "status": "[v]",
          "label": "Обновление новости",
          "description": "Обновлять старые новости по этой награде или выпускать новую каждый раз",
          "kind": "dropdown",
          "variants": [
            "Y",
            "N"
          ],
          "default": "N",
          "allow_empty": false,
          "json_target": "REWARD_ADD_DATA.refreshOldNews",
          "note": "",
          "variant_labels": [
            "Да",
            "Нет"
          ],
          "json_required": true
        },
        {
          "n": 69,
          "key": "REWARD_ADD_DATA.tournamentTeam",
          "status": "[v]",
          "label": "Коммандная награда",
          "description": "Командный режим награды",
          "kind": "dropdown",
          "variants": [
            "Y",
            "N"
          ],
          "default": "N",
          "allow_empty": false,
          "json_target": "REWARD_ADD_DATA.tournamentTeam",
          "note": "",
          "variant_labels": [
            "Да",
            "Нет"
          ],
          "json_required": true
        },
        {
          "n": 70,
          "key": "REWARD_ADD_DATA.seasonItem",
          "status": "[v]",
          "label": "Код сезона товара",
          "description": "Код сезона для сезонной награды и товара",
          "kind": "dropdown",
          "variants": [
            "SEASON_mmb_2026",
            "SEASON_2026_1",
            "SEASON_akm_2026",
            "SEASON_csm_2026",
            "SEASON_m_2026_1"
          ],
          "default": "SEASON_mmb_2026",
          "allow_empty": false,
          "json_target": "REWARD_ADD_DATA.seasonItem",
          "note": "",
          "variant_labels": [
            "Сезон 2026 (ММБ)",
            "Сезон 2026 (ККСБ)",
            "Сезон 2026 (АКМ)",
            "Сезон 2026  (CSM)",
            "Сезон 2026 (МНС)"
          ],
          "json_required": true
        },
        {
          "n": 71,
          "key": "REWARD_ADD_DATA.newsType",
          "status": "[v]",
          "label": "Тип выпускаемых новостей",
          "description": "Как формируются новости о награде (AI-генерация / Создание по шаблону)",
          "kind": "dropdown",
          "variants": [
            "AIPROMPT",
            "TEMPLATE"
          ],
          "default": "AIPROMPT",
          "allow_empty": false,
          "json_target": "REWARD_ADD_DATA.newsType",
          "note": "TEMPLATE. (генерит ИИ / по шаблону)",
          "variant_labels": [
            "AI-генерация",
            "Шаблон"
          ],
          "json_required": true
        },
        {
          "n": 72,
          "key": "REWARD_ADD_DATA.winCriterion",
          "status": "[v]",
          "label": "Критерий победы для новости",
          "description": "Текст критерия победы для AI-создания новостей",
          "kind": "text",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "REWARD_ADD_DATA.winCriterion",
          "note": "",
          "json_required": true
        },
        {
          "n": 73,
          "key": "REWARD_ADD_DATA.preferences",
          "status": "[v]",
          "label": "Преференции за победу",
          "description": "Преференции за получение награды если предусмотрены (отображаем в детальной карточке Бейджа (награды))",
          "kind": "list",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "REWARD_ADD_DATA.preferences",
          "note": "",
          "json_required": true
        },
        {
          "n": 74,
          "key": "REWARD_ADD_DATA.feature",
          "status": "[v]",
          "label": "Особенности награды",
          "description": "Особенности награды для показа в Детальной карточке бейджа",
          "kind": "list",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "REWARD_ADD_DATA.feature",
          "note": "",
          "json_required": true
        },
        {
          "n": 75,
          "key": "REWARD_ADD_DATA.businessBlock",
          "status": "[v]",
          "label": "Бизнес-блок награды",
          "description": "Бизнес блок награды и участников кто может претендовать",
          "kind": "list",
          "variants": [
            "KMMMB",
            "KMKKSB",
            "AKMKKSB",
            "CSM"
          ],
          "default": "KMMMB",
          "allow_empty": true,
          "json_target": "REWARD_ADD_DATA.businessBlock",
          "note": "",
          "variant_labels": [
            "КМ ММБ",
            "КМ ККСБ",
            "АКМ ККСБ",
            "Руководитель по внедрению проектов"
          ],
          "json_required": true
        },
        {
          "n": 76,
          "key": "REWARD_ADD_DATA.helpCodeList",
          "status": "[v]",
          "label": "Код окна с описанием показателя",
          "description": "Коды для вывода окна с дополнительным описанием конкурса (доступно в детальной карточке награды)",
          "kind": "list",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "REWARD_ADD_DATA.helpCodeList",
          "note": "",
          "json_required": true
        },
        {
          "n": 77,
          "key": "REWARD_ADD_DATA.hiddenRewardList",
          "status": "[v]",
          "label": "Скрыть в списках",
          "description": "Скрывает награду в списках",
          "kind": "dropdown",
          "variants": [
            "Y",
            "N"
          ],
          "default": "N",
          "allow_empty": false,
          "json_target": "REWARD_ADD_DATA.hiddenRewardList",
          "note": "N.",
          "variant_labels": [
            "Да",
            "Нет"
          ],
          "json_required": true
        }
      ],
      "column": "REWARD_ADD_DATA"
    },
    {
      "id": "TABLE:REWARD-LINK",
      "title": "REWARD-LINK",
      "menu_label": "REWARD-LINK",
      "intro": "Таблица / лист REWARD-LINK",
      "kind": "table",
      "parent": null,
      "sheet": "REWARD-LINK",
      "fields": [
        {
          "n": 78,
          "key": "CONTEST_CODE",
          "status": "[v]",
          "label": "Код конкурса",
          "description": "Уникальный код конкурса (ключ связей). Пример: 01_2025-0_11-1_1",
          "kind": "text",
          "variants": [],
          "default": "",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 79,
          "key": "GROUP_CODE",
          "status": "[v]",
          "label": "Код уровня награды",
          "description": "Выбор уровня на котором выбираем победителей (среди кого соревнуемся)",
          "kind": "dropdown",
          "variants": [
            "BANK",
            "TB",
            "GOSB",
            "GROUPING"
          ],
          "default": "BANK",
          "allow_empty": false,
          "json_target": "",
          "note": "",
          "variant_labels": [
            "Вся страна",
            "Тер. банк",
            "ГОСБ",
            "Группа"
          ]
        },
        {
          "n": 80,
          "key": "REWARD_CODE",
          "status": "[v]",
          "label": "Код награды",
          "description": "Код награды для связи конкурса с уровнем",
          "kind": "text",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "",
          "note": ""
        }
      ]
    },
    {
      "id": "TABLE:GROUP",
      "title": "GROUP",
      "menu_label": "GROUP",
      "intro": "Таблица / лист GROUP",
      "kind": "table",
      "parent": null,
      "sheet": "GROUP",
      "fields": [
        {
          "n": 81,
          "key": "CONTEST_CODE",
          "status": "[v]",
          "label": "Код конкурса",
          "description": "Уникальный код конкурса (ключ связей). Пример: 01_2025-0_11-1_1",
          "kind": "text",
          "variants": [],
          "default": "",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 82,
          "key": "GROUP_CODE",
          "status": "[v]",
          "label": "Код уровня награды",
          "description": "Выбор уровня на котором выбираем победителей (среди кого соревнуемся)",
          "kind": "dropdown",
          "variants": [
            "BANK",
            "TB",
            "GOSB",
            "GROUPING"
          ],
          "default": "BANK",
          "allow_empty": false,
          "json_target": "",
          "note": "",
          "variant_labels": [
            "Вся страна",
            "Тер. банк",
            "ГОСБ",
            "Группа"
          ]
        },
        {
          "n": 83,
          "key": "GROUP_VALUE",
          "status": "[v]",
          "label": "Фильтр по уровням",
          "description": "Отдельные настройки победителей для выбранного уровня",
          "kind": "dropdown_custom",
          "variants": [
            "*",
            "38",
            "40",
            "44",
            "18",
            "42",
            "70",
            "54",
            "55",
            "16",
            "13",
            "52"
          ],
          "default": "*",
          "allow_empty": false,
          "json_target": "GROUP_VALUE",
          "note": "",
          "variant_labels": [
            "Все",
            "МБ",
            "СРБ",
            "СибБ",
            "ББ",
            "ВВБ",
            "ДВБ",
            "ПБ",
            "СЗБ",
            "УБ",
            "ЦЧБ",
            "ЮЗБ"
          ]
        },
        {
          "n": 84,
          "key": "GET_CALC_METHOD",
          "status": "[v]",
          "label": "Способ выбора победителей",
          "description": "Каким образом выбираем победителей турника",
          "kind": "dropdown",
          "variants": [
            "1",
            "2",
            "3",
            "0"
          ],
          "default": "2",
          "allow_empty": false,
          "json_target": "",
          "note": "",
          "variant_labels": [
            "процент от",
            "первые Х",
            "достигни",
            "не вручаем"
          ]
        },
        {
          "n": 85,
          "key": "GET_CALC_CRITERION",
          "status": "[v]",
          "label": "Критерий Золото",
          "description": "Сколько лучших выбираем для уровня Золото",
          "kind": "number",
          "variants": [],
          "default": "0",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 86,
          "key": "ADD_CALC_CRITERION",
          "status": "[v]",
          "label": "Критерий Серебро",
          "description": "Сколько лучших выбираем для уровня Серебро",
          "kind": "number",
          "variants": [],
          "default": "0",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 87,
          "key": "ADD_CALC_CRITERION_2",
          "status": "[v]",
          "label": "Критерий Бронза",
          "description": "Сколько лучших выбираем для уровня Бронза",
          "kind": "number",
          "variants": [],
          "default": "0",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 88,
          "key": "BASE_CALC_CODE",
          "status": "[v]",
          "label": "Код уровня награды",
          "description": "Выбор уровня на котором выбираем победителей (среди кого соревнуемся)",
          "kind": "dropdown",
          "variants": [
            "BANK",
            "TB",
            "GOSB",
            "GROUPING"
          ],
          "default": "",
          "allow_empty": true,
          "json_target": "",
          "note": "",
          "variant_labels": [
            "Вся страна",
            "Тер. банк",
            "ГОСБ",
            "Группа"
          ]
        }
      ]
    },
    {
      "id": "TABLE:INDICATOR",
      "title": "INDICATOR",
      "menu_label": "INDICATOR",
      "intro": "Таблица / лист INDICATOR",
      "kind": "table",
      "parent": null,
      "sheet": "INDICATOR",
      "fields": [
        {
          "n": 89,
          "key": "CONTEST_CODE",
          "status": "[v]",
          "label": "Код конкурса",
          "description": "Уникальный код конкурса (ключ связей). Пример: 01_2025-0_11-1_1",
          "kind": "text",
          "variants": [],
          "default": "",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 90,
          "key": "INDICATOR_CALC_TYPE",
          "status": "[v]",
          "label": "Тип индикатора",
          "description": "Тип показателя (сейчас всегда используем 1-Расчетный",
          "kind": "dropdown",
          "variants": [
            "1",
            "2",
            "3"
          ],
          "default": "1",
          "allow_empty": false,
          "json_target": "",
          "note": "",
          "variant_labels": [
            "Расчетный",
            "Абсолютный",
            "Относительный"
          ]
        },
        {
          "n": 91,
          "key": "INDICATOR_ADD_CALC_TYPE",
          "status": "[v]",
          "label": "Классификация показателя",
          "description": "Классификация показателя для определения расчета (числитель / знаменатель)",
          "kind": "dropdown",
          "variants": [
            "NUMERATOR",
            "DIVIDER"
          ],
          "default": "",
          "allow_empty": true,
          "json_target": "",
          "note": "DIVIDER",
          "variant_labels": [
            "Числитель",
            "Знаменатель"
          ]
        },
        {
          "n": 92,
          "key": "FULL_NAME",
          "status": "[v]",
          "label": "Имя индикатора",
          "description": "Наименование показателя = коду показателя и не используется",
          "kind": "text",
          "variants": [],
          "default": "",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 93,
          "key": "INDICATOR_CODE",
          "status": "[v]",
          "label": "Код показателя",
          "description": "Код показателя для расчётов. Только список (свой вариант нельзя). По умолчанию WAIT.",
          "kind": "dropdown",
          "variants": [
            "WAIT",
            "PPO_IN",
            "PPO_ALL",
            "PULMIS_BALANCE_OUT_RUB",
            "PULMIS_BALANCE_OUT",
            "PULMIS_SDO_IN_RUB",
            "PULMIS_SDO_IN",
            "PULMIS_INCOME",
            "PULMIS_INCOME_RUB",
            "PULMIS_AGRMNT_AMT_RUB",
            "PULMIS_CUSTOMER_ID",
            "LEAGUE",
            "SUPERCUP",
            "INCOME",
            "PFIMIS_INCOME",
            "PFIMIS_VOLUME",
            "PFIMIS_INCOME_SOFT",
            "PFIMIS_CUSTOMER_ID",
            "PFIMIS_DEAL_CNT",
            "PFIMIS_DEAL_ID",
            "CC360_CLIENT_VOLUM_CHPDP_M",
            "CC360_CLIENT_VOLUM_FOT_M",
            "INSURANCEMIS_AGENT_COMMISION",
            "INSURANCEMIS_BANK_COMMISION",
            "INSURANCEMIS_COMMISION",
            "INSURANCE_AMMOUNT",
            "EFFICIENCYARSKKSB_EFF",
            "EFFICIENCYARSKKSB_OD_YEAR",
            "EFFICIENCYARSKKSB_OD_YEAR_APPG",
            "EFFICIENCYARSKKSB_OD_QUARTER_APPG",
            "EFFICIENCYARSKKSB_OD_YEAR_GROWTH",
            "EFFICIENCYARSKKSB_OD_YEAR_TEMP",
            "EFFICIENCYARSKKSB_OD_QUARTER_GROWTH",
            "EFFICIENCYARSKKSB_OD_QUARTER_TEMP",
            "EFFICIENCYARS_OVERBONUS",
            "EFFICIENCYARS_OVERBONUS_YEAR",
            "EFFICIENCYARS_OVERBONUS_YEAR_APPG",
            "EFFICIENCYARS_OVERBONUS_QUARTER_APPG",
            "EFFICIENCYARS_OVERBONUS_YEAR_GROWTH",
            "EFFICIENCYARS_OVERBONUS_YEAR_TEMP",
            "EFFICIENCYARS_OVERBONUS_QUARTER_GROWTH",
            "EFFICIENCYARS_OVERBONUS_QUARTER_TEMP",
            "TRUSTLEVELCC360_STAR_COUNT",
            "TRUSTLEVELCC360_STAR_START_COUNT",
            "TRUSTLEVELCC360_LEVEL0_COUNT",
            "TRUSTLEVELCC360_LEVEL3_COUNT",
            "TRUSTLEVELCC360_LEVEL4_COUNT",
            "TRUSTLEVELCC360_LEVEL5_COUNT",
            "FUNNELARS_ACTIVE_DEAL_ID",
            "FUNNELARS_ACTIVE_DEAL_MARGIN",
            "FUNNELARS_ACTIVE_DEAL_CHOD",
            "FUNNELARS_ACTIVE_CUSTOMER_ID",
            "COMPASARS_KKP_ID",
            "CC360_NKD_DETAIL_CHKD",
            "CC360_NKD_DETAIL_CHKD_PLAN",
            "KANBANARS_OFFER_VALUE",
            "KANBANARS_STAGE_VALUE",
            "KANBANARS_STAGE_INC",
            "KANBANARS_OFFER_INC",
            "KANBANARS_STAGE_AMOUNT",
            "KANBANARS_DEAL_AMOUNT",
            "KANBANARS_DEAL_NUM",
            "KANBANARS_OFFER_VALUE_VKS",
            "KANBANARS_STAGE_VALUE_VKS",
            "KANBANARS_STAGE_INC_VKS",
            "KANBANARS_OFFER_INC_VKS",
            "KANBANARS_STAGE_AMOUNT_VKS",
            "KANBANARS_DEAL_AMOUNT_VKS",
            "KANBANARS_DEAL_NUM_VKS",
            "KANBANARS_OFFER_VALUE_VKO",
            "KANBANARS_STAGE_VALUE_VKO",
            "KANBANARS_STAGE_INC_VKO",
            "KANBANARS_OFFER_INC_VKO",
            "KANBANARS_STAGE_AMOUNT_VKO",
            "KANBANARS_DEAL_AMOUNT_VKO",
            "KANBANARS_DEAL_NUM_VKO",
            "WD"
          ],
          "default": "WAIT",
          "allow_empty": false,
          "json_target": "",
          "note": "",
          "variant_labels": [
            "Ручной",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            ""
          ]
        },
        {
          "n": 94,
          "key": "INDICATOR_AGG_FUNCTION",
          "status": "[v]",
          "label": "Функция агрегации",
          "description": "Функция агрегации показателя.",
          "kind": "dropdown",
          "variants": [
            "SUM",
            "MAX",
            "MIN",
            "AVG",
            "COUNT",
            "COUNT_DISTINCT",
            "COUNT_DISTINCT_CUSTOMER",
            "COUNT_DISTINCT_DEAL",
            "LAST_VALUE"
          ],
          "default": "SUM",
          "allow_empty": false,
          "json_target": "",
          "note": "",
          "variant_labels": [
            "Сумма",
            "Максимум",
            "Минимум",
            "Среднее",
            "Количество",
            "Уник. индикаторы",
            "Уник. клиенты",
            "Уник. договоры",
            "Последнее по дате"
          ]
        },
        {
          "n": 95,
          "key": "INDICATOR_WEIGHT",
          "status": "[v]",
          "label": "Множитель показателя",
          "description": "Способ расчета показателя (множитель)",
          "kind": "dropdown",
          "variants": [
            "1",
            "-1",
            "1000"
          ],
          "default": "1",
          "allow_empty": false,
          "json_target": "",
          "note": "",
          "variant_labels": [
            "Положительный",
            "Отрицательный",
            "Множитель x1000"
          ]
        },
        {
          "n": 96,
          "key": "INDICATOR_OBJECT",
          "status": "[v]",
          "label": "Объект конкурса",
          "description": "Параметр для определения объекта конкурса (для группировки в рамках одного сотрудника (поиск лучшей сделки) - для типа показателя - 2- Абсолютный показатель)",
          "kind": "text",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "",
          "note": ""
        },
        {
          "n": 97,
          "key": "INDICATOR_MARK_TYPE",
          "status": "[v]",
          "label": "Как выбираем победителей",
          "description": "Способ выбора победителей: достиг показателя, сделал больше других или меньше других — меньше, например, для ранга",
          "kind": "dropdown",
          "variants": [
            "CRITERION",
            "RATING",
            "GAIN"
          ],
          "default": "RATING",
          "allow_empty": false,
          "json_target": "",
          "note": "",
          "variant_labels": [
            "Достигни",
            "Рейтинг",
            "Больше в X раз"
          ]
        },
        {
          "n": 98,
          "key": "INDICATOR_MATCH",
          "status": "[v]",
          "label": "Символ сравнения с планом",
          "description": "Вид сравнения показателя с планом для определения участников",
          "kind": "dropdown",
          "variants": [
            "=",
            ">",
            ">=",
            "<",
            "<=",
            "MAX",
            "MIN",
            "X2",
            "X3",
            "X4"
          ],
          "default": "MAX",
          "allow_empty": false,
          "json_target": "",
          "note": "",
          "variant_labels": [
            "Равно",
            "Больше",
            "Больше или равно",
            "Меньше",
            "Меньше или равно",
            "Максимум",
            "Минимум",
            "Больше в 2 раза",
            "Больше в 3 раза",
            "Больше в 4 раза"
          ]
        },
        {
          "n": 99,
          "key": "INDICATOR_VALUE",
          "status": "[v]",
          "label": "Значение плана",
          "description": "Значение планового показателя (0, 1, 1000, …)",
          "kind": "number",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "",
          "note": ""
        },
        {
          "n": 100,
          "key": "CONTEST_CRITERION",
          "status": "[v]",
          "label": "Ограничения сделки",
          "description": "Параметры для ограничения выбора сделки (не используется)",
          "kind": "text",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "",
          "note": ""
        },
        {
          "n": 102,
          "key": "CONTESTANT_SELECTION",
          "status": "[v]",
          "label": "Как определять ВКО",
          "description": "Способ определения участника на данных источника",
          "kind": "dropdown",
          "variants": [
            "0",
            "1"
          ],
          "default": "0",
          "allow_empty": false,
          "json_target": "",
          "note": "1",
          "variant_labels": [
            "На дату операции",
            "На конец турнира"
          ]
        },
        {
          "n": 103,
          "key": "CALC_TYPE",
          "status": "[v]",
          "label": "Тип расчёта показателя",
          "description": "Тип расчёта: 0 — промышленный расчет / 1 — ручной расчет (не используется)",
          "kind": "dropdown",
          "variants": [
            "0",
            "1"
          ],
          "default": "1",
          "allow_empty": false,
          "json_target": "",
          "note": "1",
          "variant_labels": [
            "Промышленный расчет",
            "Ручные данные"
          ]
        },
        {
          "n": 104,
          "key": "N",
          "status": "[v]",
          "label": "Уникальный ID строки",
          "description": "Уникальный во всем файле настроек ID (порядковый номер)",
          "kind": "number",
          "variants": [],
          "default": "",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 138,
          "key": "INDICATOR_FILTER",
          "status": "[v]",
          "label": "Фильтры индикатора",
          "description": "Для автоматических турниров настройка фильтров для источника",
          "kind": "json",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "INDICATOR_FILTER",
          "note": "Колонка CSV с SPOD-JSON. «Можно пусто» — допустима ли пустая ячейка колонки целиком."
        }
      ]
    },
    {
      "id": "INDICATOR_FILTER",
      "title": "INDICATOR_FILTER",
      "menu_label": "INDICATOR_FILTER",
      "intro": "JSON-массив фильтров в колонке INDICATOR_FILTER. Элемент: filtered_attribute_code/type/match + condition (массив строк) или value/dt.",
      "kind": "json_array",
      "parent": "TABLE:INDICATOR",
      "sheet": "INDICATOR",
      "column": "INDICATOR_FILTER",
      "fields": [
        {
          "n": 129,
          "status": "[v]",
          "variants": [
            "segment",
            "segment_mk",
            "tb",
            "product_group",
            "product",
            "is_manual_correct",
            "is_cva_product",
            "action_code",
            "stage",
            "deal_type",
            "coa_type_id",
            "customer_segment",
            "e2e_product_code",
            "kkp_status_code",
            "deal_is_msh",
            "ccy_code",
            "category",
            "coa_open_dt",
            "offer_close_date",
            "polis_dt"
          ],
          "default": "segment",
          "allow_empty": false,
          "json_target": "INDICATOR_FILTER[].filtered_attribute_code",
          "note": "",
          "description": "Код поля фильтра",
          "kind": "dropdown_custom",
          "label": "Код фильтра",
          "key": "INDICATOR_FILTER.filtered_attribute_code",
          "json_required": true
        },
        {
          "n": 130,
          "status": "[v]",
          "variants": [
            "STRING",
            "DATE",
            "INTEGER",
            "DECIMAL (38,12)"
          ],
          "default": "STRING",
          "allow_empty": false,
          "json_target": "INDICATOR_FILTER[].filtered_attribute_type",
          "note": "",
          "description": "Тип данных в фильтре",
          "kind": "dropdown_custom",
          "label": "Тип атрибута",
          "key": "INDICATOR_FILTER.filtered_attribute_type",
          "variant_labels": [
            "Строка",
            "Дата",
            "Целое число",
            "Дробное число"
          ],
          "json_required": true
        },
        {
          "n": 131,
          "status": "[v]",
          "variants": [
            "IN",
            "NOT_IN",
            ">=",
            ">",
            "<=",
            "<",
            "="
          ],
          "default": "IN",
          "allow_empty": false,
          "json_target": "INDICATOR_FILTER[].filtered_attribute_match",
          "note": "",
          "description": "Оператор сравнения для фильтра",
          "kind": "dropdown",
          "label": "Оператор сравнения",
          "key": "INDICATOR_FILTER.filtered_attribute_match",
          "variant_labels": [
            "Входит",
            "Не входит",
            "Больше или равно",
            "Больше",
            "Меньше или равно",
            "Меньше",
            "Равно"
          ],
          "json_required": true
        },
        {
          "n": 132,
          "status": "[v]",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "INDICATOR_FILTER[].filtered_attribute_condition",
          "note": "",
          "description": "Массив значений фильма в виде строк",
          "kind": "list",
          "label": "Значение фильтра строки",
          "key": "INDICATOR_FILTER.filtered_attribute_condition",
          "json_required": false
        },
        {
          "n": 133,
          "status": "[v]",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "INDICATOR_FILTER[].filtered_attribute_value",
          "note": "",
          "description": "Значение для фильтра числового",
          "kind": "number",
          "label": "Значение фильтра числа",
          "key": "INDICATOR_FILTER.filtered_attribute_value",
          "json_required": false
        },
        {
          "n": 134,
          "status": "[v]",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "INDICATOR_FILTER[].filtered_attribute_dt",
          "note": "",
          "description": "Значение для фильтра даты",
          "kind": "date",
          "label": "Значение фильтра даты",
          "key": "INDICATOR_FILTER.filtered_attribute_dt",
          "json_required": false
        }
      ]
    },
    {
      "id": "TABLE:SCHEDULE",
      "title": "SCHEDULE",
      "menu_label": "SCHEDULE",
      "intro": "Таблица / лист TOURNAMENT-SCHEDULE",
      "kind": "table",
      "parent": null,
      "sheet": "TOURNAMENT-SCHEDULE",
      "fields": [
        {
          "n": 105,
          "key": "TOURNAMENT_CODE",
          "status": "[v]",
          "label": "Код турнира",
          "description": "Код для турнира в рамках конкурса",
          "kind": "text",
          "variants": [],
          "default": "",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 106,
          "key": "PERIOD_TYPE",
          "status": "[v]",
          "label": "Тип периодичность турнира",
          "description": "Текстовое описание периода турнира",
          "kind": "dropdown_custom",
          "variants": [
            "турнир года",
            "турнир 1 полугодия",
            "турнир 2 полугодия",
            "турнир 1 квартала",
            "турнир 2 квартала",
            "турнир 3 квартала",
            "турнир 4 квартала",
            "турнир января",
            "турнир февраля",
            "турнир марта",
            "турнир апреля",
            "турнир мая",
            "турнир июня",
            "турнир июля",
            "турнир августа",
            "турнир сентября",
            "турнир октября",
            "турнир ноября",
            "турнир декабря",
            "произвольный"
          ],
          "default": "произвольный",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 107,
          "key": "START_DT",
          "status": "[v]",
          "label": "Дата начала турнира",
          "description": "Дата старта турнира",
          "kind": "date",
          "variants": [],
          "default": "",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 108,
          "key": "END_DT",
          "status": "[v]",
          "label": "Дата конца турнира",
          "description": "Дата окончания турнира",
          "kind": "date",
          "variants": [],
          "default": "",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 109,
          "key": "RESULT_DT",
          "status": "[v]",
          "label": "Дата подведения итога",
          "description": "Дата подведения итогов турнира и вручения наград",
          "kind": "date",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "",
          "note": ""
        },
        {
          "n": 110,
          "key": "PLAN_PERIOD_START_DT",
          "status": "[v]",
          "label": "Дата старта периода плана",
          "description": "Дата старта для периода, где определяется плановое значение",
          "kind": "date",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "",
          "note": ""
        },
        {
          "n": 111,
          "key": "PLAN_PERIOD_END_DT",
          "status": "[v]",
          "label": "Дата конца периода плана",
          "description": "Дата конца для периода, где определяется плановое значение",
          "kind": "date",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "",
          "note": ""
        },
        {
          "n": 112,
          "key": "CRITERION_MARK_TYPE",
          "status": "[v]",
          "label": "Критерий участия",
          "description": "Критерий участия для перида конкурса. Оператор сравнения",
          "kind": "dropdown",
          "variants": [
            "=",
            ">",
            ">=",
            "<",
            "<="
          ],
          "default": ">=",
          "allow_empty": false,
          "json_target": "",
          "note": "",
          "variant_labels": [
            "Равно",
            "Больше",
            "Больше или равно",
            "Меньше",
            "Меньше или равно"
          ]
        },
        {
          "n": 113,
          "key": "CRITERION_MARK_VALUE",
          "status": "[v]",
          "label": "Критерий отбора периода",
          "description": "Значение критерий для участия перида конкурса",
          "kind": "number",
          "variants": [],
          "default": "",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 115,
          "key": "TOURNAMENT_STATUS",
          "status": "[v]",
          "label": "Статус турнира",
          "description": "Выбор статуса активности турнира",
          "kind": "dropdown",
          "variants": [
            "АКТИВНЫЙ",
            "ЗАВЕРШЕН",
            "ОТМЕНЕН",
            "ПОДВЕДЕНИЕ ИТОГОВ",
            "УДАЛЕН"
          ],
          "default": "АКТИВНЫЙ",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 116,
          "key": "CONTEST_CODE",
          "status": "[v]",
          "label": "Код конкурса",
          "description": "Уникальный код конкурса (ключ связей). Пример: 01_2025-0_11-1_1",
          "kind": "text",
          "variants": [],
          "default": "",
          "allow_empty": false,
          "json_target": "",
          "note": ""
        },
        {
          "n": 118,
          "key": "CALC_TYPE",
          "status": "[v]",
          "label": "Тип расчёта показателя",
          "description": "Тип расчёта: 0 — промышленный расчет / 1 — ручной расчет",
          "kind": "dropdown",
          "variants": [
            "0",
            "1"
          ],
          "default": "1",
          "allow_empty": false,
          "json_target": "",
          "note": "",
          "variant_labels": [
            "Промышленный расчет",
            "Ручные данные"
          ]
        },
        {
          "n": 119,
          "key": "TRN_INDICATOR_FILTER",
          "status": "[w]",
          "label": "TRN_INDICATOR_FILTER",
          "description": "Описание готовится... по умолчанию не заполняется",
          "kind": "text",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "TRN_INDICATOR_FILTER",
          "note": ""
        },
        {
          "n": 139,
          "key": "TARGET_TYPE",
          "status": "[v]",
          "label": "Настройка сезонов",
          "description": "Содержит настройку сезонов в JSON",
          "kind": "json",
          "variants": [],
          "default": "",
          "allow_empty": false,
          "json_target": "TARGET_TYPE",
          "note": "Колонка CSV с SPOD-JSON. «Можно пусто» — допустима ли пустая ячейка колонки целиком."
        },
        {
          "n": 140,
          "key": "FILTER_PERIOD_ARR",
          "status": "[v]",
          "label": "Настройки правовых периодов",
          "description": "Содержит настройки для периода плана если их более двух",
          "kind": "json",
          "variants": [],
          "default": "",
          "allow_empty": false,
          "json_target": "FILTER_PERIOD_ARR",
          "note": "Колонка CSV с SPOD-JSON. «Можно пусто» — допустима ли пустая ячейка колонки целиком."
        }
      ]
    },
    {
      "id": "SCHEDULE_TARGET_TYPE",
      "title": "TARGET_TYPE (SCHEDULE)",
      "menu_label": "TARGET_TYPE",
      "intro": "JSON-объект в колонке TARGET_TYPE листа SCHEDULE: {\"\"\"seasonCode\"\"\": \"\"\"SEASON_…\"\"\"}. Не путать с TARGET_TYPE конкурса (ПРОМ/ТЕСТ).",
      "kind": "json",
      "parent": "TABLE:SCHEDULE",
      "sheet": "TOURNAMENT-SCHEDULE",
      "column": "TARGET_TYPE",
      "fields": [
        {
          "n": 123,
          "status": "[v]",
          "variants": [
            "SEASON_mmb_2026",
            "SEASON_2026_1",
            "SEASON_akm_2026",
            "SEASON_csm_2026",
            "SEASON_m_2026_1",
            "NON"
          ],
          "default": "SEASON_mmb_2026",
          "allow_empty": true,
          "json_target": "TARGET_TYPE.seasonCode",
          "note": "",
          "description": "Сезон рейтинга / начисления кристаллов (seasonCode).",
          "kind": "dropdown_custom",
          "label": "Код сезона",
          "key": "TARGET_TYPE.seasonCode",
          "variant_labels": [
            "Сезон 2026 (ММБ)",
            "Сезон 2026 (ККСБ)",
            "Сезон 2026 (АКМ)",
            "Сезон 2026 (CSM)",
            "Сезон 2026 (МНС)",
            ""
          ],
          "json_required": false
        }
      ]
    },
    {
      "id": "FILTER_PERIOD_ARR",
      "title": "FILTER_PERIOD_ARR",
      "menu_label": "FILTER_PERIOD_ARR",
      "intro": "JSON-массив в колонке FILTER_PERIOD_ARR (SCHEDULE). Элемент: period_code, start_dt, end_dt; опционально criterion_mark_type / criterion_mark_value. Даты и строки в \"\"\"…\"\"\", числа без кавычек.",
      "kind": "json_array",
      "parent": "TABLE:SCHEDULE",
      "sheet": "TOURNAMENT-SCHEDULE",
      "column": "FILTER_PERIOD_ARR",
      "fields": [
        {
          "n": 124,
          "status": "[v]",
          "variants": [],
          "default": "1",
          "allow_empty": false,
          "json_target": "FILTER_PERIOD_ARR[].period_code",
          "note": "",
          "description": "Код периода в обратном порядке от текущего",
          "kind": "number",
          "label": "Код периода",
          "key": "FILTER_PERIOD_ARR.period_code",
          "json_required": true
        },
        {
          "n": 125,
          "status": "[v]",
          "variants": [],
          "default": "",
          "allow_empty": false,
          "json_target": "FILTER_PERIOD_ARR[].start_dt",
          "note": "",
          "description": "Дата начала анализируемого периода",
          "kind": "date",
          "label": "Дата начала",
          "key": "FILTER_PERIOD_ARR.start_dt",
          "json_required": true
        },
        {
          "n": 126,
          "status": "[v]",
          "variants": [],
          "default": "",
          "allow_empty": false,
          "json_target": "FILTER_PERIOD_ARR[].end_dt",
          "note": "",
          "description": "Дата конца анализируемого периода",
          "kind": "date",
          "label": "Дата конца",
          "key": "FILTER_PERIOD_ARR.end_dt",
          "json_required": true
        },
        {
          "n": 127,
          "status": "[v]",
          "variants": [
            ">",
            ">=",
            "<",
            "<=",
            "="
          ],
          "default": "",
          "allow_empty": true,
          "json_target": "FILTER_PERIOD_ARR[].criterion_mark_type",
          "note": "",
          "description": "Оператор; можно не заполнять.",
          "kind": "dropdown",
          "label": "Оператор сравнения",
          "key": "FILTER_PERIOD_ARR.criterion_mark_type",
          "variant_labels": [
            "Больше",
            "Больше или равно",
            "Меньше",
            "Меньше или равно",
            "Равно"
          ],
          "json_required": false
        },
        {
          "n": 128,
          "status": "[v]",
          "variants": [],
          "default": "",
          "allow_empty": true,
          "json_target": "FILTER_PERIOD_ARR[].criterion_mark_value",
          "note": "",
          "description": "Пороховое значение",
          "kind": "number",
          "label": "Значение порога",
          "key": "FILTER_PERIOD_ARR.criterion_mark_value",
          "json_required": true
        }
      ]
    }
  ],
  "exported_at": "2026-08-17T13:42:19.465681Z"
};
