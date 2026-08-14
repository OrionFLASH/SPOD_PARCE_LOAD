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
    # key, label, kind: str | list | raw
    ("vid", "FEATURE.Среда конкурса", "str"),
    ("accuracy", "FEATURE.Округление до...", "str"),
    ("capacity", "FEATURE.Приведение к млн / тыс.", "str"),
    ("masking", "FEATURE.masking", "str"),
    ("minNumber", "FEATURE.minNumber", "str"),
    ("momentRewarding", "FEATURE.momentRewarding", "str"),
    ("typeRewarding", "FEATURE.typeRewarding", "str"),
    ("avatarShow", "FEATURE.avatarShow", "str"),
    ("tournamentTeam", "FEATURE.tournamentTeam", "str"),
    ("persomanNumberVisible", "FEATURE.persomanNumberVisible (через ;)", "list"),
    ("persomanNumberHidden", "FEATURE.persomanNumberHidden (через ;)", "list"),
    ("tournamentStartMailing", "FEATURE.tournamentStartMailing", "str"),
    ("tournamentEndMailing", "FEATURE.tournamentEndMailing", "str"),
    ("tournamentLikeMailing", "FEATURE.tournamentLikeMailing", "str"),
    ("tournamentListMailing", "FEATURE.tournamentListMailing (через ;)", "list"),
    ("tournamentRewardingMailing", "FEATURE.tournamentRewardingMailing", "str"),
    ("feature", "FEATURE.feature (через ;)", "list"),
    ("businessBlock", "FEATURE.businessBlock (через ;)", "list"),
    ("helpCodeList", "FEATURE.helpCodeList (через ;)", "list"),
    ("preferences", "FEATURE.preferences (через ;)", "list"),
    ("tbVisible", "FEATURE.tbVisible (через ;)", "list"),
    ("tbHidden", "FEATURE.tbHidden (через ;)", "list"),
    ("gosbVisible", "FEATURE.gosbVisible (через ;)", "list"),
    ("gosbHidden", "FEATURE.gosbHidden (через ;)", "list"),
]

REWARD_FLAT_FIELDS: List[Tuple[str, str]] = [
    ("REWARD_CODE", "Код награды"),
    ("REWARD_TYPE", "Тип награды (BADGE)"),
    ("FULL_NAME", "Название награды"),
    ("REWARD_DESCRIPTION", "Описание награды"),
    ("REWARD_CONDITION", "Условие награды"),
    ("REWARD_COST", "Стоимость"),
]

# Whitelist ADD_DATA для BADGE
REWARD_ADD_DATA_FIELDS: List[Tuple[str, str, str]] = [
    ("nftFlg", "ADD.nftFlg", "str"),
    ("outstanding", "ADD.outstanding", "str"),
    ("rewardRule", "ADD.rewardRule", "str"),
    ("rewardAgainGlobal", "ADD.rewardAgainGlobal", "str"),
    ("rewardAgainTournament", "ADD.rewardAgainTournament", "str"),
    ("hidden", "ADD.hidden", "str"),
    ("fileName", "ADD.fileName", "str"),
    ("teamNews", "ADD.teamNews", "str"),
    ("singleNews", "ADD.singleNews", "str"),
    ("masterBadge", "ADD.masterBadge", "str"),
    ("parentRewardCode", "ADD.parentRewardCode", "str"),
    ("priority", "ADD.priority", "str"),
    ("recommendationLevel", "ADD.recommendationLevel", "str"),
    ("refreshOldNews", "ADD.refreshOldNews", "str"),
    ("tournamentTeam", "ADD.tournamentTeam", "str"),
    ("seasonItem", "ADD.seasonItem", "str"),
    ("newsType", "ADD.newsType", "str"),
    ("winCriterion", "ADD.winCriterion", "str"),
    ("preferences", "ADD.preferences", "str"),
    ("feature", "ADD.feature (через ;)", "list"),
    ("businessBlock", "ADD.businessBlock (через ;)", "list"),
    ("helpCodeList", "ADD.helpCodeList (через ;)", "list"),
    ("hiddenRewardList", "ADD.hiddenRewardList", "str"),
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
