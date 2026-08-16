# -*- coding: utf-8 -*-
"""Схема полей Excel-формы BADGE и лимиты наград по CONTEST_TYPE."""

from __future__ import annotations

from typing import Any, Dict, List, Sequence, Tuple

# Плоские колонки CONTEST (без CONTEST_FEATURE — раскрывается в листья)
CONTEST_FLAT_FIELDS: List[Tuple[str, str]] = [
    ("CONTEST_CODE", "Код конкурса"),
    ("FULL_NAME", "Название конкурса"),
    ("CREATE_DT", "Дата создания конкурса"),
    ("CLOSE_DT", "Дата закрытия"),
    ("BUSINESS_STATUS", "Бизнес-статус"),
    ("CONTEST_TYPE", "Тип конкурса"),
    ("CONTEST_DESCRIPTION", "Описание турнира"),
    ("SHOW_INDICATOR", "Отображаемое название единиц показателя"),
    ("PRODUCT_GROUP", "Группа продукта"),
    ("PRODUCT", "Продукт"),
    ("CONTEST_SUBJECT", "Кто соревнуется"),
    ("FACTOR_MARK_TYPE", "Принцип отбора победителей"),
    ("CONTEST_INDICATOR_METHOD", "Метод индикатора"),
    ("CONTEST_FACTOR_METHOD", "Метод расчета показателя"),
    ("PLAN_METHOD_CODE", "Как вычисляется план"),
    ("PLAN_MOD_METOD", "Метод модификации плана"),
    ("PLAN_MOD_VALUE", "Значение плана"),
    ("FACTOR_MATCH", "Символ сравнения с планом"),
    ("TARGET_TYPE", "Среда конкурса"),
    ("SOURCE_UPD_FREQUENCY", "Частота обновления источника"),
    ("CALC_TYPE", "Тип расчёта"),
    ("FACT_POST_PROCESSING", "Постобработка факта"),
]

# Массивы на уровне CONTEST (в форме — через ;)
CONTEST_ARRAY_FIELDS: List[Tuple[str, str]] = [
    ("BUSINESS_BLOCK", "Бизнес-блок (через ;)"),
    ("CONTEST_PERIOD", "Периоды расчета конкурса (через ;)"),
]

# Листья CONTEST_FEATURE (BADGE / турнирный сценарий)
CONTEST_FEATURE_FIELDS: List[Tuple[str, str, str]] = [
    # leaf, label, kind: str | list | raw (ключ каталога: CONTEST_FEATURE.<leaf>)
    ("vid", "Среда конкурса", "str"),
    ("accuracy", "Округление до...", "str"),
    ("capacity", "Приведение к млн / тыс.", "str"),
    ("masking", "masking", "str"),
    ("minNumber", "minNumber", "str"),
    ("momentRewarding", "momentRewarding", "str"),
    ("typeRewarding", "typeRewarding", "str"),
    ("avatarShow", "avatarShow", "str"),
    ("tournamentTeam", "tournamentTeam", "str"),
    ("persomanNumberVisible", "persomanNumberVisible (через ;)", "list"),
    ("persomanNumberHidden", "persomanNumberHidden (через ;)", "list"),
    ("tournamentStartMailing", "tournamentStartMailing", "str"),
    ("tournamentEndMailing", "tournamentEndMailing", "str"),
    ("tournamentLikeMailing", "tournamentLikeMailing", "str"),
    ("tournamentListMailing", "tournamentListMailing (через ;)", "list"),
    ("tournamentRewardingMailing", "tournamentRewardingMailing", "str"),
    ("feature", "feature (через ;)", "list"),
    ("businessBlock", "businessBlock (через ;)", "list"),
    ("helpCodeList", "helpCodeList (через ;)", "list"),
    ("preferences", "preferences (через ;)", "list"),
    ("tbVisible", "tbVisible (через ;)", "list"),
    ("tbHidden", "tbHidden (через ;)", "list"),
    ("gosbVisible", "gosbVisible (через ;)", "list"),
    ("gosbHidden", "gosbHidden (через ;)", "list"),
]

REWARD_FLAT_FIELDS: List[Tuple[str, str]] = [
    ("REWARD_CODE", "Код награды"),
    ("REWARD_TYPE", "Тип награды (BADGE)"),
    ("FULL_NAME", "Название награды"),
    ("REWARD_DESCRIPTION", "Описание награды"),
    ("REWARD_CONDITION", "Условие награды"),
    ("REWARD_COST", "Стоимость"),
]

# Whitelist REWARD_ADD_DATA для BADGE (ключ каталога: REWARD_ADD_DATA.<leaf>)
REWARD_ADD_DATA_FIELDS: List[Tuple[str, str, str]] = [
    ("nftFlg", "Признак NFT", "str"),
    ("outstanding", "Выпуск новостей", "str"),
    ("rewardRule", "Правило получения", "str"),
    ("rewardAgainGlobal", "Повтор в другом турнире", "str"),
    ("rewardAgainTournament", "Повтор в текущем турнире", "str"),
    ("hidden", "Скрыт", "str"),
    ("fileName", "Имя файла", "str"),
    ("teamNews", "Командная новость", "str"),
    ("singleNews", "Индивидуальная новость", "str"),
    ("masterBadge", "Мастер-бейдж", "str"),
    ("parentRewardCode", "Код родительской награды", "str"),
    ("priority", "Приоритет слота", "str"),
    ("recommendationLevel", "Уровень рекомендации", "str"),
    ("refreshOldNews", "Обновлять старые новости", "str"),
    ("tournamentTeam", "Командный режим", "str"),
    ("seasonItem", "Сезонный ITEM", "str"),
    ("newsType", "Тип новости", "str"),
    ("winCriterion", "Критерий победы", "str"),
    ("preferences", "preferences (через ;)", "list"),
    ("feature", "feature (через ;)", "list"),
    ("businessBlock", "businessBlock (через ;)", "list"),
    ("helpCodeList", "helpCodeList (через ;)", "list"),
    ("hiddenRewardList", "Скрыт в списке наград", "str"),
]

REWARD_LINK_COLUMNS: List[str] = ["CONTEST_CODE", "GROUP_CODE", "REWARD_CODE"]

GROUP_COLUMNS: List[str] = [
    "CONTEST_CODE",
    "GROUP_CODE",
    "GROUP_VALUE",
    "GET_CALC_METHOD",
    "GET_CALC_CRITERION",
    "ADD_CALC_CRITERION",
    "ADD_CALC_CRITERION_2",
    "BASE_CALC_CODE",
]

INDICATOR_COLUMNS: List[str] = [
    "CONTEST_CODE",
    "INDICATOR_CALC_TYPE",
    "INDICATOR_ADD_CALC_TYPE",
    "FULL_NAME",
    "INDICATOR_CODE",
    "INDICATOR_AGG_FUNCTION",
    "INDICATOR_WEIGHT",
    "INDICATOR_OBJECT",
    "INDICATOR_MARK_TYPE",
    "INDICATOR_MATCH",
    "INDICATOR_VALUE",
    "CONTEST_CRITERION",
    "INDICATOR_FILTER",
    "CONTESTANT_SELECTION",
    "CALC_TYPE",
    "N",
]

SCHEDULE_COLUMNS: List[str] = [
    "TOURNAMENT_CODE",
    "PERIOD_TYPE",
    "START_DT",
    "END_DT",
    "RESULT_DT",
    "PLAN_PERIOD_START_DT",
    "PLAN_PERIOD_END_DT",
    "CRITERION_MARK_TYPE",
    "CRITERION_MARK_VALUE",
    "FILTER_PERIOD_ARR",
    "TOURNAMENT_STATUS",
    "CONTEST_CODE",
    "TARGET_TYPE",
    "CALC_TYPE",
    "TRN_INDICATOR_FILTER",
]

# Имена листов SPOD при импорте
SPOD_SHEET_NAMES: Dict[str, str] = {
    "contest": "CONTEST-DATA",
    "reward": "REWARD",
    "reward_link": "REWARD-LINK",
    "group": "GROUP",
    "indicator": "INDICATOR",
    "schedule": "TOURNAMENT-SCHEDULE",
}

FORM_TOKENS = frozenset(
    {
        "contest_badge_form_export",
        "contest_badge_form_import",
        "contest_badge_form_blank",
    }
)


def max_badge_slots(contest_type: str) -> int:
    """Лимит слотов BADGE по типу конкурса."""
    t = (contest_type or "").strip().upper()
    if t in {"ИНДИВИДУАЛЬНЫЙ", "ИНДИВИДУАЛЬНЫЙ НАКОПИТЕЛЬНЫЙ"}:
        return 1
    if t == "ТУРНИРНЫЙ":
        return 3
    # Неизвестный тип — как турнир (до 3), с предупреждением на стороне вызывающего
    return 3


def expected_badge_count_note(contest_type: str) -> str:
    """Человекочитаемое описание лимита."""
    n = max_badge_slots(contest_type)
    t = (contest_type or "").strip() or "?"
    if n == 1:
        return f"{t}: ровно 1 BADGE"
    return f"{t}: до {n} BADGE"


def empty_add_data_template() -> Dict[str, Any]:
    """Шаблон ADD_DATA со всеми whitelist-ключами."""
    out: Dict[str, Any] = {}
    for key, _label, kind in REWARD_ADD_DATA_FIELDS:
        out[key] = [] if kind == "list" else ""
    return out


def empty_feature_template() -> Dict[str, Any]:
    """Шаблон CONTEST_FEATURE."""
    out: Dict[str, Any] = {}
    for key, _label, kind in CONTEST_FEATURE_FIELDS:
        out[key] = [] if kind == "list" else ""
    return out


def table_columns_for(marker: str) -> Sequence[str]:
    """Колонки таблицы формы по маркеру ``#TABLE:…``."""
    key = marker.strip().upper().replace("#TABLE:", "")
    mapping = {
        "REWARD-LINK": REWARD_LINK_COLUMNS,
        "REWARD_LINK": REWARD_LINK_COLUMNS,
        "GROUP": GROUP_COLUMNS,
        "INDICATOR": INDICATOR_COLUMNS,
        "SCHEDULE": SCHEDULE_COLUMNS,
        "TOURNAMENT-SCHEDULE": SCHEDULE_COLUMNS,
    }
    return mapping.get(key, [])
