# -*- coding: utf-8 -*-
"""Описания полей и допустимые значения для Excel-формы BADGE (по выгрузкам SPOD)."""

from __future__ import annotations

from typing import Dict, List, Optional

# Y/N и 0/1 — частые флаги
_YN: List[str] = ["Y", "N"]
_01: List[str] = ["0", "1"]

# Выпадающие списки: ключ формы (колонка A) → значения
DROPDOWN_VALUES: Dict[str, List[str]] = {
    # CONTEST
    "BUSINESS_STATUS": ["АКТИВНЫЙ", "АРХИВНЫЙ"],
    "CONTEST_TYPE": [
        "ТУРНИРНЫЙ",
        "ИНДИВИДУАЛЬНЫЙ",
        "ИНДИВИДУАЛЬНЫЙ НАКОПИТЕЛЬНЫЙ",
    ],
    "SHOW_INDICATOR": [
        "%",
        "%%",
        "пт.",
        "шт.",
        "Факт",
        "балл",
        "Темп %",
        "Ранг %%",
        "ФЛ, шт.",
        "клиенты",
        "Ср. балл",
        "млн руб.",
        "К-во, шт.",
        "категория",
        "тыс. руб.",
        "Анкет, шт.",
        "Сумма, руб.",
        "Пакеты услуг",
        "Договора, шт.",
        "Сумма УС, шт.",
        "Процент (х100)",
        "Факт, млн руб.",
        "Сумма, млн руб.",
        "сборы, млн руб.",
        "Сумма, тыс. руб.",
        "Интегральный ранг",
        "Прирост, млн руб.",
        "Прирост, тыс. руб.",
        "Комиссия, тыс. руб.",
        "Прирост ОСЗ, млн руб.",
        "нетто-притоки, млн руб.",
    ],
    "CONTEST_SUBJECT": ["EMPLOYEE"],
    "FACTOR_MARK_TYPE": ["CRITERION", "RATING_MAX", "RATING_MIN"],
    "CONTEST_INDICATOR_METHOD": ["INTEGRAL", "RELATION"],
    "CONTEST_FACTOR_METHOD": [
        "FACT",
        "FACT0-FACT1",
        "FACT0-RUN_RATE1_DOWN",
        "RUN_RATE",
    ],
    "PLAN_METHOD_CODE": ["DEPENDS_PREVIOUS_PERIOD", "PRESET_VALUE"],
    "PLAN_MOD_METOD": ["MULTIPLIER"],
    "FACTOR_MATCH": ["=", ">", ">="],
    "TARGET_TYPE": ["ПРОМ", "ТЕСТ"],
    "SOURCE_UPD_FREQUENCY": ["1", "7", "10"],
    "CALC_TYPE": _01,
    "PRODUCT_GROUP": [
        "DTaaS",
        "ВЭД, нац рынки, хедж",
        "Гарантии",
        "ДГР кредитные продукты",
        "ЕФС",
        "Команда",
        "Кредиты",
        "Лизинг",
        "Пассивы, РКО",
        "Продукты УБ в канале СБ1",
        "Сервисные задачи",
        "Системные",
        "Спец проекты",
        "Статусные",
        "Страхование",
        "ТФиДО",
        "ФОТ",
        "Факторинг",
        "Эквайринг",
        "Экосистема",
        "Эффективность",
    ],
    "BUSINESS_BLOCK": [
        "KMKKSB",
        "MNS",
        "KMSB1",
        "AKMKKSB",
        "SERVICEMEN",
        "KMFACTORING",
        "IMUB",
        "RNUB",
        "RSB1",
        "CSM",
    ],
    # FEATURE.*
    "FEATURE.vid": ["ПРОМ", "ТЕСТ"],
    "FEATURE.accuracy": ["0", "1", "2", "3", "5"],
    "FEATURE.capacity": ["MILLIONS", "THOUSANDS"],
    "FEATURE.masking": _YN,
    "FEATURE.minNumber": ["0", "1", "2", "3"],
    "FEATURE.momentRewarding": ["AFTER", "DURIN"],
    "FEATURE.typeRewarding": ["one", "all"],
    "FEATURE.avatarShow": _YN,
    "FEATURE.tournamentTeam": _YN,
    "FEATURE.tournamentStartMailing": _YN,
    "FEATURE.tournamentEndMailing": _YN,
    "FEATURE.tournamentLikeMailing": _YN,
    "FEATURE.tournamentRewardingMailing": _YN,
    # REWARD flat / ADD
    "REWARD_TYPE": ["BADGE"],
    "REWARD_COST": ["0", "2", "3", "4", "5", "6", "7", "8", "10", "14"],
    "ADD.nftFlg": _YN,
    "ADD.outstanding": _YN,
    "ADD.rewardAgainGlobal": _YN,
    "ADD.rewardAgainTournament": _YN,
    "ADD.hidden": _YN,
    "ADD.masterBadge": _YN,
    "ADD.refreshOldNews": _YN,
    "ADD.tournamentTeam": _YN,
    "ADD.hiddenRewardList": _YN,
    "ADD.priority": ["1", "2", "3"],
    "ADD.recommendationLevel": ["BANK", "TB", "GOSB", "NON"],
    "ADD.newsType": ["AIPROMPT", "TEMPLATE"],
    "ADD.businessBlock": [
        "KMKKSB",
        "MNS",
        "KMSB1",
        "AKMKKSB",
        "SERVICEMEN",
        "KMFACTORING",
        "IMUB",
        "RNUB",
        "RSB1",
        "CSM",
    ],
}

# Описания KV-полей (колонка D): что это и какие значения
FIELD_DESCRIPTIONS: Dict[str, str] = {
    "CONTEST_CODE": (
        "Уникальный код конкурса (ключ связей). Пример: 01_2025-0_11-1_1"
    ),
    "FULL_NAME": "Отображаемое название конкурса/турнира (на секции CONTEST).",
    "CREATE_DT": "Дата начала YYYY-MM-DD.",
    "CLOSE_DT": "Дата окончания YYYY-MM-DD; 4000-01-01 = без срока.",
    "BUSINESS_STATUS": "Статус: АКТИВНЫЙ | АРХИВНЫЙ.",
    "CONTEST_TYPE": (
        "ТУРНИРНЫЙ (до 3 BADGE) | ИНДИВИДУАЛЬНЫЙ | ИНДИВИДУАЛЬНЫЙ НАКОПИТЕЛЬНЫЙ (1 BADGE)."
    ),
    "CONTEST_DESCRIPTION": "Текст описания для UI/админки.",
    "SHOW_INDICATOR": (
        "Единица/подпись индикатора: шт. | Факт | % | … (см. выпадающий список)."
    ),
    "PRODUCT_GROUP": "Группа продукта из классификатора (см. список).",
    "PRODUCT": "Продукт / тематика конкурса (свободный текст).",
    "CONTEST_SUBJECT": "Предмет конкурса. Обычно: EMPLOYEE.",
    "FACTOR_MARK_TYPE": "CRITERION | RATING_MAX | RATING_MIN.",
    "CONTEST_INDICATOR_METHOD": "INTEGRAL | RELATION.",
    "CONTEST_FACTOR_METHOD": "FACT | FACT0-FACT1 | FACT0-RUN_RATE1_DOWN | RUN_RATE.",
    "PLAN_METHOD_CODE": "DEPENDS_PREVIOUS_PERIOD | PRESET_VALUE.",
    "PLAN_MOD_METOD": "Модификатор плана. Обычно: MULTIPLIER.",
    "PLAN_MOD_VALUE": "Число модификатора (0, 1, 1000, …).",
    "FACTOR_MATCH": "Сравнение фактора: = | > | >=.",
    "TARGET_TYPE": "Среда конкурса: ПРОМ | ТЕСТ.",
    "SOURCE_UPD_FREQUENCY": "Частота обновления источника: 1 | 7 | 10 (дни).",
    "CALC_TYPE": "Тип расчёта: 0 | 1.",
    "FACT_POST_PROCESSING": "Постобработка факта (код/флаг; часто пусто).",
    "BUSINESS_BLOCK": (
        "Бизнес-блок(и) через ; . Примеры: KMKKSB; MNS; KMSB1; AKMKKSB."
    ),
    "CONTEST_PERIOD": "Периоды через ; или пусто → []. Обычно пусто.",
    # FEATURE
    "FEATURE.vid": "Среда в фиче: ПРОМ | ТЕСТ (как TARGET_TYPE).",
    "FEATURE.accuracy": "Точность/разрядность: 0 | 1 | 2 | 3 | 5.",
    "FEATURE.capacity": "Масштаб: пусто | MILLIONS | THOUSANDS.",
    "FEATURE.masking": "Маскирование: Y | N (часто N).",
    "FEATURE.minNumber": "Мин. число: 0 | 1 | 2 | 3.",
    "FEATURE.momentRewarding": "Момент награждения: AFTER | DURIN (как в SPOD).",
    "FEATURE.typeRewarding": "Кому бейдж: one | all.",
    "FEATURE.avatarShow": "Показ аватара: Y | N.",
    "FEATURE.tournamentTeam": "Командный турнир: Y | N.",
    "FEATURE.persomanNumberVisible": "Табельные видимые через ; (с ведущими нулями).",
    "FEATURE.persomanNumberHidden": "Табельные скрытые через ; .",
    "FEATURE.tournamentStartMailing": "Рассылка старта: Y | N.",
    "FEATURE.tournamentEndMailing": "Рассылка финиша: Y | N.",
    "FEATURE.tournamentLikeMailing": "Рассылка лайков: Y | N.",
    "FEATURE.tournamentListMailing": "Список рассылки через ; .",
    "FEATURE.tournamentRewardingMailing": "Рассылка награждения: Y | N.",
    "FEATURE.feature": "Тексты фич UI через ; (каждый элемент массива).",
    "FEATURE.businessBlock": "Блоки в FEATURE через ; (как BUSINESS_BLOCK).",
    "FEATURE.helpCodeList": "Коды подсказок через ; .",
    "FEATURE.preferences": "Предпочтения через ; .",
    "FEATURE.tbVisible": "Коды ТБ видимые через ; .",
    "FEATURE.tbHidden": "Коды ТБ скрытые через ; .",
    "FEATURE.gosbVisible": "Коды ГОСБ видимые через ; .",
    "FEATURE.gosbHidden": "Коды ГОСБ скрытые через ; .",
    # REWARD
    "REWARD_CODE": "Уникальный код награды, напр. r_01_2025-0_11-1_1_1.",
    "REWARD_TYPE": "Для этой формы всегда BADGE.",
    "REWARD_FULL_NAME": "Краткое название бейджа (колонка FULL_NAME в REWARD).",
    "REWARD_DESCRIPTION": "Полное описание награды.",
    "REWARD_CONDITION": "Класс/код условия начисления (часто пусто или код).",
    "REWARD_COST": "Стоимость в у.е. (часто 0…14).",
    "ADD.nftFlg": "NFT-флаг: Y | N (обычно N).",
    "ADD.outstanding": "Выдающийся: Y | N.",
    "ADD.rewardRule": "Текст правила получения бейджа.",
    "ADD.rewardAgainGlobal": "Повтор глобально: Y | N.",
    "ADD.rewardAgainTournament": "Повтор в турнире: Y | N (часто N).",
    "ADD.hidden": "Скрыт: Y | N.",
    "ADD.fileName": "Имя файла арта/иконки (код); часто пусто.",
    "ADD.teamNews": "Текст командной новости (шаблон с [Имя] и т.п.).",
    "ADD.singleNews": "Текст индивидуальной новости.",
    "ADD.masterBadge": "Мастер-бейдж: Y | N.",
    "ADD.parentRewardCode": "Код родительской награды (если есть).",
    "ADD.priority": "Приоритет слота: 1 | 2 | 3.",
    "ADD.recommendationLevel": "Уровень: BANK | TB | GOSB | NON.",
    "ADD.refreshOldNews": "Обновлять старые новости: Y | N.",
    "ADD.tournamentTeam": "Командный режим награды: Y | N.",
    "ADD.seasonItem": "Код сезонного ITEM (если связан).",
    "ADD.newsType": "Тип новости: AIPROMPT | TEMPLATE.",
    "ADD.winCriterion": "Текст критерия победы.",
    "ADD.preferences": "Предпочтения (строка).",
    "ADD.feature": "Фичи награды через ; .",
    "ADD.businessBlock": "Блоки награды через ; .",
    "ADD.helpCodeList": "Коды help через ; .",
    "ADD.hiddenRewardList": "Скрыт в списке наград: Y | N.",
}

# Подсказки для колонок таблиц (строка #HINT под заголовком)
TABLE_COLUMN_HINTS: Dict[str, Dict[str, str]] = {
    "REWARD-LINK": {
        "CONTEST_CODE": "Код конкурса (= CONTEST_CODE на листе)",
        "GROUP_CODE": "BANK | TB | GOSB | GROUPING",
        "REWARD_CODE": "Код BADGE из слота",
    },
    "GROUP": {
        "CONTEST_CODE": "Код конкурса",
        "GROUP_CODE": "BANK | TB | GOSB | GROUPING",
        "GROUP_VALUE": "* или [код] / JSON",
        "GET_CALC_METHOD": "1 | 2 | 3",
        "GET_CALC_CRITERION": "Число/порог",
        "ADD_CALC_CRITERION": "Число/порог",
        "ADD_CALC_CRITERION_2": "Число/порог",
        "BASE_CALC_CODE": "BANK | TB | GOSB | GROUPING",
    },
    "INDICATOR": {
        "CONTEST_CODE": "Код конкурса",
        "INDICATOR_CALC_TYPE": "Обычно 1",
        "INDICATOR_ADD_CALC_TYPE": "NUMERATOR | DIVIDER",
        "FULL_NAME": "Имя индикатора",
        "INDICATOR_CODE": "Код (WAIT, RATING, …)",
        "INDICATOR_AGG_FUNCTION": "SUM | MAX | COUNT_DISTINCT | …",
        "INDICATOR_WEIGHT": "1 | -1 | 1000",
        "INDICATOR_OBJECT": "Часто пусто",
        "INDICATOR_MARK_TYPE": "CRITERION | GAIN | RATING",
        "INDICATOR_MATCH": "= | >= | MAX | MIN | X2…",
        "INDICATOR_VALUE": "Порог/константа",
        "CONTEST_CRITERION": "Часто пусто",
        "INDICATOR_FILTER": "SPOD-JSON фильтр или пусто",
        "CONTESTANT_SELECTION": "0 | 1",
        "CALC_TYPE": "0 | 1",
        "N": "Параметр N",
    },
    "SCHEDULE": {
        "TOURNAMENT_CODE": "Код слота расписания",
        "PERIOD_TYPE": "Текст периода (турнир месяца, …)",
        "START_DT": "YYYY-MM-DD",
        "END_DT": "YYYY-MM-DD",
        "RESULT_DT": "YYYY-MM-DD",
        "PLAN_PERIOD_START_DT": "YYYY-MM-DD",
        "PLAN_PERIOD_END_DT": "YYYY-MM-DD",
        "CRITERION_MARK_TYPE": "> | >=",
        "CRITERION_MARK_VALUE": "Число (0, 50000, …)",
        "FILTER_PERIOD_ARR": "JSON или пусто",
        "TOURNAMENT_STATUS": "АКТИВНЫЙ | ЗАВЕРШЕН | ОТМЕНЕН | ПОДВЕДЕНИЕ ИТОГОВ | УДАЛЕН",
        "CONTEST_CODE": "Код конкурса",
        "TARGET_TYPE": "JSON seasonCode или пусто",
        "CALC_TYPE": "0 | 1",
        "TRN_INDICATOR_FILTER": "Часто пусто",
    },
}

# Выпадающие для заголовков таблиц (имя колонки → список); применяются ко всем ячейкам колонки данных
TABLE_DROPDOWNS: Dict[str, Dict[str, List[str]]] = {
    "REWARD-LINK": {
        "GROUP_CODE": ["BANK", "TB", "GOSB", "GROUPING"],
    },
    "GROUP": {
        "GROUP_CODE": ["BANK", "TB", "GOSB", "GROUPING"],
        "BASE_CALC_CODE": ["BANK", "TB", "GOSB", "GROUPING"],
        "GET_CALC_METHOD": ["1", "2", "3"],
    },
    "INDICATOR": {
        "INDICATOR_CALC_TYPE": ["1"],
        "INDICATOR_ADD_CALC_TYPE": ["NUMERATOR", "DIVIDER"],
        "INDICATOR_AGG_FUNCTION": [
            "SUM",
            "MAX",
            "COUNT_DISTINCT",
            "COUNT_DISTINCT_CUSTOMER",
            "COUNT_DISTINCT_DEAL",
        ],
        "INDICATOR_WEIGHT": ["1", "-1", "1000"],
        "INDICATOR_MARK_TYPE": ["CRITERION", "GAIN", "RATING"],
        "INDICATOR_MATCH": ["=", ">=", "MAX", "MIN", "X2", "X3", "X4"],
        "CONTESTANT_SELECTION": _01,
        "CALC_TYPE": _01,
    },
    "SCHEDULE": {
        "CRITERION_MARK_TYPE": [">", ">="],
        "TOURNAMENT_STATUS": [
            "АКТИВНЫЙ",
            "ЗАВЕРШЕН",
            "ОТМЕНЕН",
            "ПОДВЕДЕНИЕ ИТОГОВ",
            "УДАЛЕН",
        ],
        "CALC_TYPE": _01,
    },
}


# --- Типы ввода (цвет столбца «Значение») ---
# dropdown = выбор из списка; text = свободный ввод; list = несколько через ;
# json = JSON как в SPOD; date = дата YYYY-MM-DD

INPUT_KIND_COLORS: Dict[str, str] = {
    "dropdown": "#C6EFCE",
    "text": "#FFF2CC",
    "list": "#FCE4D6",
    "json": "#F5B7B1",
    "date": "#DDEBF7",
}

INPUT_KIND_LABELS: Dict[str, str] = {
    "dropdown": "Выбор из списка",
    "text": "Свободный ввод",
    "list": "Несколько через ;",
    "json": "JSON (как в SPOD)",
    "date": "Дата YYYY-MM-DD",
}

# Порядок легенды на листе
INPUT_KIND_ORDER: List[str] = ["dropdown", "text", "list", "json", "date"]

# KV-поля с датой
_DATE_FORM_KEYS: frozenset = frozenset({"CREATE_DT", "CLOSE_DT"})

# Табличные колонки с JSON (или смешанным JSON/*/[код])
TABLE_JSON_COLUMNS: Dict[str, frozenset] = {
    "GROUP": frozenset({"GROUP_VALUE"}),
    "INDICATOR": frozenset({"INDICATOR_FILTER"}),
    "SCHEDULE": frozenset(
        {"FILTER_PERIOD_ARR", "TARGET_TYPE", "TRN_INDICATOR_FILTER"}
    ),
}

TABLE_DATE_COLUMNS: Dict[str, frozenset] = {
    "SCHEDULE": frozenset(
        {
            "START_DT",
            "END_DT",
            "RESULT_DT",
            "PLAN_PERIOD_START_DT",
            "PLAN_PERIOD_END_DT",
        }
    ),
}


def _list_form_keys() -> frozenset:
    """Ключи формы, которые при импорте собираются в JSON-массив через ;."""
    from src.contest_badge_form import schema as _schema

    keys = {k for k, _ in _schema.CONTEST_ARRAY_FIELDS}
    for leaf, _label, kind in _schema.CONTEST_FEATURE_FIELDS:
        if kind == "list":
            keys.add(f"FEATURE.{leaf}")
    for leaf, _label, kind in _schema.REWARD_ADD_DATA_FIELDS:
        if kind == "list":
            keys.add(f"ADD.{leaf}")
    return frozenset(keys)


def input_kind_for_kv(
    form_key: str,
    *,
    schema_kind: Optional[str] = None,
    has_dropdown: bool = False,
) -> str:
    """Тип ввода для KV-поля (колонка C)."""
    if schema_kind == "list" or form_key in _list_form_keys():
        return "list"
    if form_key in _DATE_FORM_KEYS or (
        form_key.endswith("_DT") and form_key not in DROPDOWN_VALUES
    ):
        return "date"
    if has_dropdown or form_key in DROPDOWN_VALUES:
        return "dropdown"
    return "text"


def input_kind_for_table_col(table_key: str, col_name: str) -> str:
    """Тип ввода для ячейки таблицы."""
    if col_name in TABLE_JSON_COLUMNS.get(table_key, frozenset()):
        return "json"
    if col_name in TABLE_DATE_COLUMNS.get(table_key, frozenset()) or (
        col_name.endswith("_DT")
        and col_name not in (TABLE_DROPDOWNS.get(table_key) or {})
    ):
        return "date"
    if col_name in (TABLE_DROPDOWNS.get(table_key) or {}):
        return "dropdown"
    return "text"


def merge_dropdowns(
    config_dropdowns: Optional[Dict[str, List[str]]] = None,
) -> Dict[str, List[str]]:
    """Базовые списки + оверрайд из конфига."""
    out: Dict[str, List[str]] = {k: list(v) for k, v in DROPDOWN_VALUES.items()}
    if config_dropdowns:
        for key, values in config_dropdowns.items():
            if values:
                out[str(key)] = [str(x) for x in values]
    return out


def description_for(key: str, *, in_badge_slot: bool = False) -> str:
    """Текст подсказки для ключа формы."""
    if in_badge_slot and key == "FULL_NAME":
        return FIELD_DESCRIPTIONS.get(
            "REWARD_FULL_NAME",
            "Краткое название бейджа.",
        )
    if key in FIELD_DESCRIPTIONS:
        return FIELD_DESCRIPTIONS[key]
    return (
        "Заполните ячейку значения (цвет = тип ввода, см. легенду). "
        "Если есть список — выберите из выпадающего."
    )
