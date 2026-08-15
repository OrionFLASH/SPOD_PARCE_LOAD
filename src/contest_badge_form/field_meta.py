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
    "FACTOR_MATCH": ["=", ">", ">=", "<", "<="],
    "TARGET_TYPE": ["ПРОМ", "ТЕСТ"],
    "SOURCE_UPD_FREQUENCY": ["1", "7", "10"],
    "CALC_TYPE": _01,
    # PRODUCT_GROUP — свободный текст (без выпадающего списка)
    "BUSINESS_BLOCK": [
        "KMMMB",
        "KMKKSB",
        "CSM",
        "AKMKKSB",
    ],
    # FEATURE.*
    "FEATURE.vid": ["ПРОМ", "ТЕСТ"],
    "FEATURE.accuracy": ["0", "1", "2"],
    "FEATURE.capacity": ["MILLIONS", "THOUSANDS"],
    "FEATURE.masking": _YN,
    "FEATURE.minNumber": ["1", "2", "3"],
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
        "KMMMB",
        "KMKKSB",
        "CSM",
        "AKMKKSB",
    ],
}

# Описания KV-полей (колонка D): что это и какие значения
FIELD_DESCRIPTIONS: Dict[str, str] = {
    "CONTEST_CODE": (
        "Уникальный код конкурса (ключ связей). Пример: 01_2025-0_11-1_1"
    ),
    "FULL_NAME": (
        "Отображаемое название конкурса/турнира "
        "(на странице Турниры/Детальная карточка турнира)."
    ),
    "CREATE_DT": "Дата начала YYYY-MM-DD. Почти всегда начало года",
    "CLOSE_DT": "Дата окончания YYYY-MM-DD; 4000-01-01 = без срока.",
    "BUSINESS_STATUS": "Статус: АКТИВНЫЙ | АРХИВНЫЙ. (Всегда ставим АКТИВНЫЙ)",
    "CONTEST_TYPE": (
        'ТУРНИРНЫЙ (соревнование "будь лучше других") '
        "(разыгрываем от 1 до 3 сезонных наград Золото Серебро Бронза) | "
        "ИНДИВИДУАЛЬНЫЙ | ИНДИВИДУАЛЬНЫЙ НАКОПИТЕЛЬНЫЙ "
        '(режим "достигни результат", получи одну награду).'
    ),
    "CONTEST_DESCRIPTION": (
        "Текст описания для конкурса/турнира "
        "(на странице Детальная карточка турнира показываем)."
    ),
    "SHOW_INDICATOR": (
        "Единица/подпись индикатора: шт. | Факт | % | … "
        "на списке показателей подпись к единицам данных"
    ),
    "PRODUCT_GROUP": "Группа продукта (общее направление)",
    "PRODUCT": "Продукт / тематика конкурса.",
    "CONTEST_SUBJECT": (
        "Кто участник конкурса. Обычно: EMPLOYEE (сотрудники)."
    ),
    "FACTOR_MARK_TYPE": (
        "CRITERION | RATING_MAX | RATING_MIN. "
        "(способ выбора победителей: достиг показателя, сделал больше других "
        "или меньше других — меньше, например, для ранга)"
    ),
    "CONTEST_INDICATOR_METHOD": (
        "INTEGRAL | RELATION. Метод расчета показателя "
        "(фактический / расчетный)"
    ),
    "CONTEST_FACTOR_METHOD": (
        "FACT | FACT0-FACT1 | FACT0-RUN_RATE1_DOWN | RUN_RATE. "
        "(для автоматических турниров способ расчета на данных источников)"
    ),
    "PLAN_METHOD_CODE": (
        "DEPENDS_PREVIOUS_PERIOD | PRESET_VALUE. "
        "(Метод расчета планового показателя: из прошлого периода / "
        "фиксированное значение)"
    ),
    "PLAN_MOD_METOD": "Модификатор плана. Обычно: MULTIPLIER.",
    "PLAN_MOD_VALUE": "Значение планового показателя (0, 1, 1000, …).",
    "FACTOR_MATCH": "Сравнение фактора: = | > | >= | < | <=.",
    "TARGET_TYPE": "Среда конкурса: ПРОМ | ТЕСТ.",
    "SOURCE_UPD_FREQUENCY": (
        "Частота обновления источника: 1 | 7 | 10 (дни). (не используется)"
    ),
    "CALC_TYPE": (
        "Тип расчёта: 0 | 1. (не используется) "
        "0 — промышленный расчет / 1 — ручной расчет"
    ),
    "FACT_POST_PROCESSING": (
        "Постобработка факта (код/флаг; часто пусто). "
        "Правило постобработки показателя конкурса. "
        "PERCENTILE — вычисление перцентиля от фактического показателя конкурса"
    ),
    "BUSINESS_BLOCK": (
        "Бизнес-блок(и) через ; . Примеры: KMMMB, KMKKSB, CSM, AKMKKSB."
    ),
    "CONTEST_PERIOD": "Периоды через ; или пусто → []. Обычно пусто.",
    # FEATURE
    "FEATURE.vid": "Среда конкурса: ПРОМ | ТЕСТ (как TARGET_TYPE).",
    "FEATURE.accuracy": (
        "Точность/разрядность: 0 | 1 | 2 . "
        "(число знаков после запятой для отображения)"
    ),
    "FEATURE.capacity": (
        "Масштаб: пусто | MILLIONS | THOUSANDS. "
        "(приведение отображаемого показателя к млн, к тыс.)"
    ),
    "FEATURE.masking": "Маскирование: Y | N (часто N).",
    "FEATURE.minNumber": (
        "Мин. число участников чтобы считать победителей "
        "(исключаем соревнование сам с собой): 1 | 2 | 3."
    ),
    "FEATURE.momentRewarding": (
        "Момент награждения: AFTER | DURIN "
        "(после закрытия турнира / во время турнира)"
    ),
    "FEATURE.typeRewarding": "Вручаем одну из 3 наград или все (one | all).",
    "FEATURE.avatarShow": "Показ аватара: Y | N.",
    "FEATURE.tournamentTeam": "Командный турнир: Y | N.",
    "FEATURE.persomanNumberVisible": (
        "Если указаны табельные, то только эти сотрудники увидят турнир"
    ),
    "FEATURE.persomanNumberHidden": (
        "Если указаны табельные, то эти сотрудники НЕ увидят турнир"
    ),
    "FEATURE.tournamentStartMailing": "Рассылка старта: Y | N.",
    "FEATURE.tournamentEndMailing": "Рассылка финиша: Y | N.",
    "FEATURE.tournamentLikeMailing": "Рассылка лайков: Y | N.",
    "FEATURE.tournamentListMailing": "Список рассылки через ; .",
    "FEATURE.tournamentRewardingMailing": "Рассылка награждения: Y | N.",
    "FEATURE.feature": (
        "Тексты особенностей турнира. Показываем в детальной карточке турнира"
    ),
    "FEATURE.businessBlock": "Блоки в FEATURE через ; (как BUSINESS_BLOCK).",
    "FEATURE.helpCodeList": "Коды для вывода окна с доп описанием конкурса",
    "FEATURE.preferences": (
        "Преференции за получение награды если предусмотрены"
    ),
    "FEATURE.tbVisible": "Коды ТБ видимые через ; .",
    "FEATURE.tbHidden": "Коды ТБ скрытые через ; .",
    "FEATURE.gosbVisible": "Коды ГОСБ видимые через ; .",
    "FEATURE.gosbHidden": "Коды ГОСБ скрытые через ; .",
    # REWARD
    "REWARD_CODE": "Уникальный код награды, напр. r_01_2025-0_11-1_1_1.",
    "REWARD_TYPE": "Для этой формы всегда BADGE.",
    "REWARD_FULL_NAME": "Краткое название бейджа",
    "REWARD_DESCRIPTION": "Полное описание награды.",
    "REWARD_CONDITION": "Класс/код условия начисления (часто пусто или код).",
    "REWARD_COST": "Стоимость в кристаллах (часто 0…14).",
    "ADD.nftFlg": "NFT-флаг: Y | N (обычно N).",
    "ADD.outstanding": "Выдающийся: Y | N.",
    "ADD.rewardRule": "Текст правила получения бейджа.",
    "ADD.rewardAgainGlobal": "Повтор глобально: Y | N.",
    "ADD.rewardAgainTournament": "Повтор в турнире: Y | N (часто N).",
    "ADD.hidden": "Скрыт: Y | N.",
    "ADD.fileName": "Имя файла арта/иконки (код); часто пусто.",
    "ADD.teamNews": "Текст командной новости (шаблон с [Имя] и т.п.).",
    "ADD.singleNews": "Текст индивидуальной новости.",
    "ADD.masterBadge": "Мастер-бейдж: Y | N. (Y — для награды / N — для турнира)",
    "ADD.parentRewardCode": "Код родительской награды (если есть).",
    "ADD.priority": "Приоритет слота: 1 | 2 | 3.",
    "ADD.recommendationLevel": "Уровень: BANK | TB | GOSB | NON.",
    "ADD.refreshOldNews": "Обновлять старые новости: Y | N.",
    "ADD.tournamentTeam": "Командный режим награды: Y | N.",
    "ADD.seasonItem": "Код сезонного ITEM (если связан).",
    "ADD.newsType": (
        "Тип новости: AIPROMPT | TEMPLATE. (генерит ИИ / по шаблону)"
    ),
    "ADD.winCriterion": "Текст критерия победы для ИИ создания новости",
    "ADD.preferences": "Преференции если предусмотрены за награду",
    "ADD.feature": "Особенности награды через ; . (показываем в Награде)",
    "ADD.businessBlock": "Блоки награды через ; .",
    "ADD.helpCodeList": "Коды help через ; .",
    "ADD.hiddenRewardList": "Скрыт в списке наград: Y | N.",
}

# Значения по умолчанию в BLANK (колонка C / ячейка предзаполнена).
# Пустая строка в словаре не нужна — отсутствие ключа = не предзаполнять.
FIELD_DEFAULTS: Dict[str, str] = {
    "BUSINESS_STATUS": "АКТИВНЫЙ",
    "CONTEST_TYPE": "ТУРНИРНЫЙ",
    "CLOSE_DT": "4000-01-01",
    "CONTEST_SUBJECT": "EMPLOYEE",
    "TARGET_TYPE": "ПРОМ",
    "SOURCE_UPD_FREQUENCY": "1",
    "CALC_TYPE": "0",
    "PLAN_MOD_METOD": "MULTIPLIER",
    "FEATURE.vid": "ПРОМ",
    "FEATURE.masking": "N",
    "FEATURE.avatarShow": "Y",
    "FEATURE.tournamentTeam": "N",
    "REWARD_TYPE": "BADGE",
    "ADD.nftFlg": "N",
    "ADD.outstanding": "N",
    "ADD.rewardAgainGlobal": "N",
    "ADD.rewardAgainTournament": "N",
    "ADD.hidden": "N",
    "ADD.masterBadge": "N",
    "ADD.refreshOldNews": "N",
    "ADD.tournamentTeam": "N",
    "ADD.hiddenRewardList": "N",
}

# Можно ли оставить пустым при заполнении формы.
# Если ключа нет — считаем, что пусто допустимо (да).
FIELD_ALLOW_EMPTY: Dict[str, bool] = {
    "CONTEST_CODE": False,
    "FULL_NAME": False,
    "CREATE_DT": False,
    "BUSINESS_STATUS": False,
    "CONTEST_TYPE": False,
    "REWARD_CODE": False,
    "REWARD_TYPE": False,
    "REWARD.FULL_NAME": False,
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
        "START_DT": "Дата старта турнира",
        "END_DT": "Дата окончания турнира",
        "RESULT_DT": "Дата подведения итогов турнира",
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
# dropdown = выбор из списка; text = свободный текст; number = число;
# list = массив значений (в форме через ;); json = JSON {[ ]}; date = YYYY-MM-DD

INPUT_KIND_COLORS: Dict[str, str] = {
    "dropdown": "#C6EFCE",
    "text": "#FFF2CC",
    "number": "#E8DAEF",
    "list": "#FCE4D6",
    "json": "#F5B7B1",
    "date": "#DDEBF7",
}

INPUT_KIND_LABELS: Dict[str, str] = {
    "dropdown": "Выбор из списка",
    "text": "Свободный текст",
    "number": "Число",
    "list": "Массив значений",
    "json": "JSON формат {[ ]}",
    "date": "Дата (формат YYYY-MM-DD)",
}

# Порядок легенды на листе
INPUT_KIND_ORDER: List[str] = ["dropdown", "text", "number", "list", "json", "date"]

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
    from src.contest_badge_form.catalog_loader import field_overlay

    ov = field_overlay(form_key)
    if ov and ov.get("kind"):
        return str(ov["kind"])
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
    from src.contest_badge_form.catalog_loader import table_overlay

    ov = table_overlay(table_key, col_name)
    if ov and ov.get("kind"):
        return str(ov["kind"])
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
    """Базовые списки + таблицы + catalog.json + оверрайд из конфига."""
    from src.contest_badge_form.catalog_loader import (
        dropdown_overrides_from_catalog,
        table_dropdown_overrides_from_catalog,
    )

    out: Dict[str, List[str]] = {k: list(v) for k, v in DROPDOWN_VALUES.items()}
    for table_key, col_map in TABLE_DROPDOWNS.items():
        for col_name, values in col_map.items():
            out[f"TBL:{table_key}:{col_name}"] = list(values)
    for key, values in dropdown_overrides_from_catalog().items():
        if values:
            out[str(key)] = [str(x) for x in values]
    for table_key, col_map in table_dropdown_overrides_from_catalog().items():
        for col_name, values in col_map.items():
            if values:
                out[f"TBL:{table_key}:{col_name}"] = [str(x) for x in values]
    if config_dropdowns:
        for key, values in config_dropdowns.items():
            if values:
                out[str(key)] = [str(x) for x in values]
    return out


def label_for(key: str, fallback: str = "", *, in_badge_slot: bool = False) -> str:
    """Подпись поля: catalog.json → fallback (schema)."""
    from src.contest_badge_form.catalog_loader import field_overlay

    lookup = key
    if in_badge_slot and key == "FULL_NAME":
        lookup = "REWARD.FULL_NAME"
        ov = field_overlay(lookup) or field_overlay("FULL_NAME")
    else:
        ov = field_overlay(lookup)
    if ov and ov.get("label"):
        return str(ov["label"])
    return fallback


def description_for(key: str, *, in_badge_slot: bool = False) -> str:
    """Текст подсказки для ключа формы."""
    from src.contest_badge_form.catalog_loader import field_overlay

    if in_badge_slot and key == "FULL_NAME":
        ov = field_overlay("REWARD.FULL_NAME") or field_overlay(key)
        if ov and ov.get("description"):
            return str(ov["description"])
        return FIELD_DESCRIPTIONS.get(
            "REWARD_FULL_NAME",
            "Краткое название бейджа.",
        )
    ov = field_overlay(key)
    if ov and ov.get("description"):
        return str(ov["description"])
    if key in FIELD_DESCRIPTIONS:
        return FIELD_DESCRIPTIONS[key]
    return (
        "Заполните ячейку значения (цвет = тип ввода, см. легенду). "
        "Если есть список — выберите из выпадающего."
    )


def default_for(key: str, *, in_badge_slot: bool = False) -> str:
    """Значение по умолчанию для BLANK (пусто = не предзаполнять)."""
    from src.contest_badge_form.catalog_loader import field_overlay

    if in_badge_slot and key == "FULL_NAME":
        ov = field_overlay("REWARD.FULL_NAME") or field_overlay(key)
        if ov is not None:
            return str(ov.get("default") or "")
        return FIELD_DEFAULTS.get("REWARD.FULL_NAME", "")
    ov = field_overlay(key)
    if ov is not None:
        return str(ov.get("default") or "")
    return FIELD_DEFAULTS.get(key, "")


def allow_empty_for(key: str, *, in_badge_slot: bool = False) -> bool:
    """Допустимо ли пустое значение (по умолчанию да)."""
    from src.contest_badge_form.catalog_loader import field_overlay

    if in_badge_slot and key == "FULL_NAME":
        ov = field_overlay("REWARD.FULL_NAME") or field_overlay(key)
        if ov is not None and "allow_empty" in ov:
            return bool(ov["allow_empty"])
        return FIELD_ALLOW_EMPTY.get("REWARD.FULL_NAME", False)
    ov = field_overlay(key)
    if ov is not None and "allow_empty" in ov:
        return bool(ov["allow_empty"])
    if key in FIELD_ALLOW_EMPTY:
        return FIELD_ALLOW_EMPTY[key]
    return True


def table_hint_for(table_key: str, col_name: str) -> str:
    """Подсказка колонки таблицы: catalog → TABLE_COLUMN_HINTS."""
    from src.contest_badge_form.catalog_loader import table_overlay

    ov = table_overlay(table_key, col_name)
    if ov and ov.get("description"):
        return str(ov["description"])
    return TABLE_COLUMN_HINTS.get(table_key, {}).get(col_name, "значение")


def json_pack_target(form_key: str) -> str:
    """Куда упаковывается поле при импорте в SPOD; пусто = плоская колонка."""
    if form_key.startswith("FEATURE."):
        return "CONTEST_FEATURE"
    if form_key.startswith("ADD."):
        return "REWARD_ADD_DATA"
    return ""


def json_pack_target_table(table_key: str, col_name: str) -> str:
    """Метка JSON для колонки таблицы (ячейка целиком в формате SPOD-JSON)."""
    if col_name in TABLE_JSON_COLUMNS.get(table_key, frozenset()):
        return "ячейка JSON"
    return ""
