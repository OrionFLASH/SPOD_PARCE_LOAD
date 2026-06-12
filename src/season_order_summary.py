# -*- coding: utf-8 -*-
"""
Сводный лист заказов по группам сезона (SEASON_*) и прочим ITEM из REWARD.

Строит таблицу: заказано по ORDER, остаток itemAmount, признак «ЗАКОНЧИЛСЯ»,
пороги из REWARD_ADD_DATA и счётчики менеджеров на листе RATING.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

from src.reward_item_catalog import (
    build_item_catalog_from_reward_df,
    item_accessible_for_manager,
    rules_for_matrix_column,
)
from src import rating_item_matrix as rim

# Колонки с целым числом и выравниванием по центру (см. column_formats в config.json)
NUMERIC_CENTER_COLUMNS: Tuple[str, ...] = (
    "Всего товаров",
    "Заказано",
    "Остаток",
    "Мин. рейтинг BANK",
    "Мин. рейтинг TB",
    "Мин. рейтинг GOSB",
    "Мин. кристаллов",
    "ignoreConditions (кол-во)",
    "КМ: условия выполнены",
    "КМ: без 2 заказов в группе",
    "КМ: не закончился и не 2 в группе",
    "КМ: все ограничения кроме исчерпания",
)

SECTION_OTHER_LABEL = "— Прочие товары (вне групп SEASON) —"


def _defaults_cfg() -> Dict[str, Any]:
    return {
        "enabled": True,
        "sheet_name": "ORDER-SEASON-SUMMARY",
        "sheet_rating": "RATING",
        "sheet_order": "ORDER",
        "sheet_reward": "REWARD",
        "sheet_list_rewards": "LIST-REWARDS",
        "use_item_order_groups": True,
        "include_other_items": True,
        "stock_status_ended": "ЗАКОНЧИЛСЯ",
        "section_other_label": SECTION_OTHER_LABEL,
    }


def merge_season_summary_config(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Слияние блока season_order_summary и rating_item_matrix (общие колонки)."""
    base = _defaults_cfg()
    if not isinstance(raw, dict):
        return base
    block = raw.get("season_order_summary") if "season_order_summary" in raw else raw
    if not isinstance(block, dict):
        block = {}
    for k, v in block.items():
        base[k] = v
    rim_cfg = raw.get("rating_item_matrix") if "rating_item_matrix" in raw else None
    if rim_cfg is None and "item_order_groups" in raw:
        rim_cfg = raw
    if isinstance(rim_cfg, dict):
        for k in (
            "sheet_order",
            "sheet_reward",
            "sheet_rating",
            "sheet_list_rewards",
            "order_employee_col",
            "order_product_col",
            "order_status_col",
            "order_status_exclude",
            "rating_employee_col",
            "country_rank_col",
            "tb_rank_col",
            "gosb_rank_col",
            "crystals_col",
            "list_rewards_employee_col",
            "list_rewards_code_col",
            "reward_type_col",
            "reward_code_col",
            "reward_add_data_col",
            "item_amount_scope",
            "item_order_groups",
        ):
            if k in rim_cfg:
                base[k] = rim_cfg[k]
    return base


def _code_to_group_map(groups: List[Dict[str, Any]]) -> Dict[str, str]:
    """REWARD_CODE → id группы (SEASON_…)."""
    out: Dict[str, str] = {}
    for grp in groups:
        gid = str(grp.get("id") or "").strip()
        if not gid:
            continue
        for code in grp.get("codes") or []:
            c = rim._norm_str(code)
            if c:
                out[c] = gid
    return out


def _group_by_id(groups: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {str(g.get("id") or "").strip(): g for g in groups if str(g.get("id") or "").strip()}


def _threshold_int(val: Any) -> Optional[int]:
    """Порог для Excel: целое > 0 или пусто (None)."""
    if val is None:
        return None
    try:
        f = float(val)
        if f <= 0:
            return None
        return int(f) if f == int(f) else int(round(f))
    except (TypeError, ValueError):
        return None


def _format_code_list(codes: List[str]) -> str:
    items = [rim._norm_str(c) for c in codes if rim._norm_str(c)]
    if not items:
        return ""
    return "; ".join(items)


def _manager_rows_from_rating(
    rating_df: pd.DataFrame,
    cfg: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Один проход по RATING: поля для проверки доступности."""
    emp_c = rim._resolve_column(rating_df, cfg.get("rating_employee_col"), rim._DEFAULT_RATING_EMP)
    country_c = rim._resolve_column(rating_df, cfg.get("country_rank_col"), rim._DEFAULT_COUNTRY_RANK)
    tb_c = rim._resolve_column(rating_df, cfg.get("tb_rank_col"), rim._DEFAULT_TB_RANK)
    gosb_c = rim._resolve_column(rating_df, cfg.get("gosb_rank_col"), rim._DEFAULT_GOSB_RANK)
    cry_c = rim._resolve_column(rating_df, cfg.get("crystals_col"), rim._DEFAULT_CRYSTALS)
    if not emp_c:
        return []

    rows: List[Dict[str, Any]] = []
    for _, row in rating_df.iterrows():
        emp = rim._norm_str(row.get(emp_c))
        if not emp:
            continue
        rows.append(
            {
                "emp": emp,
                "rank_country": rim._as_float(row.get(country_c)) if country_c else None,
                "rank_tb": rim._as_float(row.get(tb_c)) if tb_c else None,
                "rank_gosb": rim._as_float(row.get(gosb_c)) if gosb_c else None,
                "crystals": rim._as_float(row.get(cry_c)) if cry_c else None,
            }
        )
    return rows


def _load_list_rewards_by_emp(
    sheets_data: Dict[str, Any],
    cfg: Dict[str, Any],
) -> Dict[str, Set[str]]:
    slr = cfg.get("sheet_list_rewards") or "LIST-REWARDS"
    out: Dict[str, Set[str]] = {}
    if slr not in sheets_data or sheets_data[slr] is None:
        return out
    lr_df = sheets_data[slr][0]
    if not isinstance(lr_df, pd.DataFrame):
        return out
    le = rim._resolve_column(lr_df, cfg.get("list_rewards_employee_col"), rim._DEFAULT_LIST_RW_EMP)
    lc = rim._resolve_column(lr_df, cfg.get("list_rewards_code_col"), rim._DEFAULT_LIST_RW_CODE)
    if not le or not lc:
        return out
    for _, lrow in lr_df.iterrows():
        e = rim._norm_str(lrow.get(le))
        c = rim._norm_str(lrow.get(lc))
        if e and c:
            out.setdefault(e, set()).add(c)
    return out


def _count_managers_for_code(
    code: str,
    rules: Dict[str, Any],
    managers: List[Dict[str, Any]],
    counts_by_emp: Dict[str, Dict[str, int]],
    order_by_emp: Dict[str, Set[str]],
    rewards_by_emp: Dict[str, Set[str]],
    grp: Optional[Dict[str, Any]],
    *,
    limit_amt: Optional[int],
    ordered_total: int,
) -> Tuple[int, int, int, int]:
    """Счётчики КМ для одного кода товара."""
    stock_ended = limit_amt is not None and ordered_total >= limit_amt
    cnt_conditions = 0
    cnt_excl_group = 0
    cnt_stock_and_group = 0
    cnt_full = 0
    group_list = [grp] if grp else []

    for m in managers:
        emp = m["emp"]
        count_by_code = counts_by_emp.get(emp, {})
        blocked = rim._blocked_codes_for_row(count_by_code, group_list) if group_list else set()
        group_blocked = code in blocked
        order_codes = order_by_emp.get(emp, set())
        rw_codes = rewards_by_emp.get(emp, set())

        cond_ok = item_accessible_for_manager(
            rules,
            rank_country=m["rank_country"],
            rank_tb=m["rank_tb"],
            rank_gosb=m["rank_gosb"],
            crystals=m["crystals"],
            order_product_codes=order_codes,
            list_reward_codes=rw_codes,
            manager_tab=emp,
        )

        if cond_ok:
            cnt_conditions += 1
        if cond_ok and not group_blocked:
            cnt_excl_group += 1
            cnt_full += 1
        if cond_ok and not stock_ended and not group_blocked:
            cnt_stock_and_group += 1

    return cnt_conditions, cnt_excl_group, cnt_stock_and_group, cnt_full


def _build_row_for_code(
    code: str,
    catalog: Dict[str, Dict[str, Any]],
    *,
    group_id: str,
    grp: Optional[Dict[str, Any]],
    global_ordered: Dict[str, int],
    managers: List[Dict[str, Any]],
    counts_by_emp: Dict[str, Dict[str, int]],
    order_by_emp: Dict[str, Set[str]],
    rewards_by_emp: Dict[str, Set[str]],
    ended_label: str,
) -> Dict[str, Any]:
    """Одна строка сводки для REWARD_CODE."""
    rules = rules_for_matrix_column(code, catalog)
    full_name = rim._norm_str(rules.get("FULL_NAME"))
    limit_amt = rim._item_amount_limit(rules)
    ordered_total = int(global_ordered.get(code, 0))

    total_val: Optional[int] = limit_amt if limit_amt is not None else None
    remainder_val: Optional[int] = None
    if limit_amt is not None:
        remainder_val = limit_amt - ordered_total

    stock_ended = limit_amt is not None and ordered_total >= limit_amt
    status_cell = ended_label if stock_ended else ""

    er = rules.get("employeeRating") or {}
    mn = er.get("minRating") or {}
    gc = rules.get("getCondition") or {}
    r_codes = [x for x in (gc.get("rewardCode") or []) if rim._norm_str(x)]
    nonr_codes = [x for x in (gc.get("nonRewardCode") or []) if rim._norm_str(x)]
    ignore_tabs = [rim._norm_str(x) for x in (rules.get("ignoreConditions") or []) if rim._norm_str(x)]

    cnt_conditions, cnt_excl_group, cnt_stock_and_group, cnt_full = _count_managers_for_code(
        code,
        rules,
        managers,
        counts_by_emp,
        order_by_emp,
        rewards_by_emp,
        grp,
        limit_amt=limit_amt,
        ordered_total=ordered_total,
    )

    ignore_cnt: Optional[int] = len(ignore_tabs) if ignore_tabs else None

    return {
        "Код награды": code,
        "Наименование товара": full_name,
        "Группа сезона": group_id,
        "Всего товаров": total_val,
        "Заказано": ordered_total,
        "Остаток": remainder_val,
        "Статус наличия": status_cell,
        "Мин. рейтинг BANK": _threshold_int(mn.get("minRatingBANK")),
        "Мин. рейтинг TB": _threshold_int(mn.get("minRatingTB")),
        "Мин. рейтинг GOSB": _threshold_int(mn.get("minRatingGOSB")),
        "Мин. кристаллов": _threshold_int(er.get("minCrystalEarnedTotal")),
        "Ограничение rewardCode": _format_code_list(r_codes) if r_codes else "",
        "Ограничение nonRewardCode": _format_code_list(nonr_codes) if nonr_codes else "",
        "ignoreConditions (кол-во)": ignore_cnt,
        "ignoreConditions (табельные)": _format_code_list(ignore_tabs),
        "КМ: условия выполнены": cnt_conditions,
        "КМ: без 2 заказов в группе": cnt_excl_group,
        "КМ: не закончился и не 2 в группе": cnt_stock_and_group,
        "КМ: все ограничения кроме исчерпания": cnt_full,
    }


def _section_separator_row(label: str) -> Dict[str, Any]:
    """Строка-разделитель секции (только текст в первой колонке)."""
    return {
        "Код награды": label,
        "Наименование товара": "",
        "Группа сезона": "",
        "Всего товаров": None,
        "Заказано": None,
        "Остаток": None,
        "Статус наличия": "",
        "Мин. рейтинг BANK": None,
        "Мин. рейтинг TB": None,
        "Мин. рейтинг GOSB": None,
        "Мин. кристаллов": None,
        "Ограничение rewardCode": "",
        "Ограничение nonRewardCode": "",
        "ignoreConditions (кол-во)": None,
        "ignoreConditions (табельные)": "",
        "КМ: условия выполнены": None,
        "КМ: без 2 заказов в группе": None,
        "КМ: не закончился и не 2 в группе": None,
        "КМ: все ограничения кроме исчерпания": None,
    }


def _all_item_codes_from_catalog(catalog: Dict[str, Dict[str, Any]]) -> List[str]:
    return sorted(catalog.keys())


def build_season_order_summary_sheet(
    sheets_data: Dict[str, Any],
    cfg: Optional[Dict[str, Any]] = None,
) -> Optional[Tuple[pd.DataFrame, Dict[str, Any]]]:
    """
    Формирует лист сводки: сначала коды из item_order_groups, затем прочие ITEM.
    Возвращает (DataFrame, params) или None.
    """
    cfg = merge_season_summary_config(cfg)
    if not cfg.get("enabled"):
        return None

    sheet_out = str(cfg.get("sheet_name") or "ORDER-SEASON-SUMMARY")
    sr = cfg.get("sheet_rating") or "RATING"
    so = cfg.get("sheet_order") or "ORDER"
    rw = cfg.get("sheet_reward") or "REWARD"

    for name in (sr, so, rw):
        if name not in sheets_data or sheets_data[name] is None:
            logging.warning(f"[season_order_summary] Нет листа «{name}» — сводка пропущена")
            return None

    rating_df = sheets_data[sr][0]
    order_df = sheets_data[so][0]
    reward_df = sheets_data[rw][0]
    if not all(isinstance(x, pd.DataFrame) for x in (rating_df, order_df, reward_df)):
        return None

    groups = rim._parse_item_order_groups(cfg) if cfg.get("use_item_order_groups", True) else []
    code_to_group = _code_to_group_map(groups)
    group_map = _group_by_id(groups)

    order_df = rim._filter_order_dataframe(order_df, cfg)
    emp_o = rim._resolve_column(order_df, cfg.get("order_employee_col"), rim._DEFAULT_ORDER_EMP)
    prod_o = rim._resolve_column(order_df, cfg.get("order_product_col"), rim._DEFAULT_ORDER_PROD)
    if not emp_o or not prod_o:
        logging.warning("[season_order_summary] Не найдены колонки ORDER")
        return None

    counts_by_emp = rim._order_counts_by_employee(order_df, emp_o, prod_o)
    global_ordered = rim._global_order_count_by_code(counts_by_emp)

    rtc = rim._resolve_column(reward_df, cfg.get("reward_type_col"), ["REWARD_TYPE"])
    rcc = rim._resolve_column(reward_df, cfg.get("reward_code_col"), ["REWARD_CODE"])
    adc = str(cfg.get("reward_add_data_col") or "REWARD_ADD_DATA")
    if adc not in reward_df.columns:
        adc = "REWARD_ADD_DATA"
    catalog: Dict[str, Dict[str, Any]] = {}
    if rtc and rcc:
        catalog = build_item_catalog_from_reward_df(reward_df, rtc, rcc, add_data_col=adc)
    if not catalog:
        logging.warning("[season_order_summary] Каталог ITEM пуст")
        return None

    rewards_by_emp = _load_list_rewards_by_emp(sheets_data, cfg)
    managers = _manager_rows_from_rating(rating_df, cfg)
    if not managers:
        logging.warning("[season_order_summary] Нет строк RATING с табельным — счётчики КМ = 0")

    order_by_emp: Dict[str, Set[str]] = {}
    for emp_key, code_counts in counts_by_emp.items():
        order_by_emp[emp_key] = {c for c, n in code_counts.items() if n > 0}

    ended_label = str(cfg.get("stock_status_ended") or "ЗАКОНЧИЛСЯ")
    result_rows: List[Dict[str, Any]] = []

    for code in sorted(code_to_group.keys()):
        group_id = code_to_group[code]
        grp = group_map.get(group_id)
        result_rows.append(
            _build_row_for_code(
                code,
                catalog,
                group_id=group_id,
                grp=grp,
                global_ordered=global_ordered,
                managers=managers,
                counts_by_emp=counts_by_emp,
                order_by_emp=order_by_emp,
                rewards_by_emp=rewards_by_emp,
                ended_label=ended_label,
            )
        )

    if cfg.get("include_other_items", True):
        in_group: Set[str] = set(code_to_group.keys())
        other_codes = [c for c in _all_item_codes_from_catalog(catalog) if c not in in_group]
        if other_codes:
            if result_rows:
                label = str(cfg.get("section_other_label") or SECTION_OTHER_LABEL)
                result_rows.append(_section_separator_row(label))
            for code in other_codes:
                result_rows.append(
                    _build_row_for_code(
                        code,
                        catalog,
                        group_id="",
                        grp=None,
                        global_ordered=global_ordered,
                        managers=managers,
                        counts_by_emp=counts_by_emp,
                        order_by_emp=order_by_emp,
                        rewards_by_emp=rewards_by_emp,
                        ended_label=ended_label,
                    )
                )

    if not result_rows:
        logging.warning("[season_order_summary] Нет строк для сводки")
        return None

    df_out = pd.DataFrame(result_rows)
    params = {
        "sheet": sheet_out,
        "max_col_width": 50,
        "freeze": "A2",
        "col_width_mode": "AUTO",
        "min_col_width": 10,
    }
    n_season = len(code_to_group)
    in_group_set: Set[str] = set(code_to_group.keys())
    n_other = (
        sum(1 for c in catalog if c not in in_group_set)
        if cfg.get("include_other_items", True)
        else 0
    )
    logging.info(
        "[season_order_summary] Лист «%s»: %s строк (SEASON: %s, прочие ITEM: %s)",
        sheet_out,
        len(df_out),
        n_season,
        n_other,
    )
    return df_out, params


def apply_season_order_summary(
    sheets_data: Dict[str, Any],
    cfg: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """Добавляет сводный лист в sheets_data; возвращает имя листа."""
    built = build_season_order_summary_sheet(sheets_data, cfg)
    if built is None:
        return None
    df_out, params = built
    sheet_name = str(params.get("sheet") or "ORDER-SEASON-SUMMARY")
    sheets_data[sheet_name] = (df_out, params)
    return sheet_name
