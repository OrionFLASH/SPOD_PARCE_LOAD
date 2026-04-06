# -*- coding: utf-8 -*-
"""
Каталог товаров ITEM с листа REWARD: разбор REWARD_ADD_DATA (JSON) по структуре ToDo SPOD.

Используется для проверки доступности товара менеджеру и раскраски матрицы на листе RATING.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from src.json_utils import safe_json_loads


def normalize_reward_add_data_string(raw: Any) -> str:
    """
    Приведение строки ячейки к виду, пригодному для JSON.parse:
    тройные кавычки -> обычные; снятие внешних кавычек у всей строки.
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return ""
    s = str(raw).strip()
    if not s or s in {"-", "None", "null"}:
        return ""
    s = s.replace('"""', '"')
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        s = s[1:-1].strip()
    return s


def _to_num0(x: Any) -> float:
    """Число для порогов; пусто -> 0."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace(",", ".")
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def _norm(x: Any) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    return str(x).strip()


def _collect_business_block(val: Any, out: List[str]) -> None:
    """Извлечение строк из businessBlock (массив скаляров или объектов)."""
    if val is None:
        return
    if isinstance(val, list):
        for it in val:
            if isinstance(it, (str, int, float)) and not (isinstance(it, float) and pd.isna(it)):
                t = _norm(it)
                if t:
                    out.append(t)
            elif isinstance(it, dict):
                for v in it.values():
                    t = _norm(v)
                    if t:
                        out.append(t)
    elif isinstance(val, str) and val.strip():
        out.append(val.strip())


def _get_condition_blocks(gc: Any) -> List[dict]:
    """getCondition в данных может быть dict или list[dict]."""
    if gc is None:
        return []
    if isinstance(gc, dict):
        return [gc]
    if isinstance(gc, list):
        return [x for x in gc if isinstance(x, dict)]
    return []


def _collect_reward_codes_from_gc(gc_root: Any) -> List[str]:
    out: List[str] = []
    for g in _get_condition_blocks(gc_root):
        rewards = g.get("rewards")
        if not isinstance(rewards, list):
            continue
        for r in rewards:
            if isinstance(r, dict) and r.get("rewardCode") is not None:
                t = _norm(r.get("rewardCode"))
                if t:
                    out.append(t)
    return out


def _collect_non_reward_codes_from_gc(gc_root: Any) -> List[str]:
    out: List[str] = []
    for g in _get_condition_blocks(gc_root):
        nr = g.get("nonRewards")
        if not isinstance(nr, list):
            continue
        for item in nr:
            if isinstance(item, dict) and item.get("nonRewardCode") is not None:
                t = _norm(item.get("nonRewardCode"))
                if t:
                    out.append(t)
    return out


def _merge_employee_rating(data: dict) -> Dict[str, Any]:
    """
    employeeRating с корня ADD_DATA и из каждого элемента getCondition (последнее непустое перекрывает).
    """
    merged: Dict[str, Any] = {}
    er = data.get("employeeRating")
    if isinstance(er, dict):
        merged.update(er)
    for g in _get_condition_blocks(data.get("getCondition")):
        inner = g.get("employeeRating")
        if isinstance(inner, dict):
            merged.update(inner)
    return merged


def _empty_item_record(reward_code: str, full_name: str) -> Dict[str, Any]:
    return {
        "REWARD_CODE": reward_code,
        "FULL_NAME": full_name,
        "businessBlock": [],
        "getCondition": {"rewardCode": [], "nonRewardCode": []},
        "employeeRating": {
            "minRating": {"minRatingBANK": 0.0, "minRatingTB": 0.0, "minRatingGOSB": 0.0},
            "minCrystalEarnedTotal": 0.0,
            "seasonCode": "",
        },
        "itemAmount": None,
    }


def parse_reward_add_data_object(data: Any, reward_code: str, full_name: str) -> Dict[str, Any]:
    """Разбор уже распарсенного JSON-объекта ADD_DATA в запись каталога."""
    rec = _empty_item_record(reward_code, full_name)
    if not isinstance(data, dict):
        return rec

    bb: List[str] = []
    _collect_business_block(data.get("businessBlock"), bb)
    rec["businessBlock"] = bb

    gc = data.get("getCondition")
    rec["getCondition"]["rewardCode"] = _collect_reward_codes_from_gc(gc)
    rec["getCondition"]["nonRewardCode"] = _collect_non_reward_codes_from_gc(gc)

    er = _merge_employee_rating(data)
    rec["employeeRating"]["minRating"]["minRatingBANK"] = _to_num0(er.get("minRatingBANK"))
    rec["employeeRating"]["minRating"]["minRatingTB"] = _to_num0(er.get("minRatingTB"))
    rec["employeeRating"]["minRating"]["minRatingGOSB"] = _to_num0(er.get("minRatingGOSB"))
    rec["employeeRating"]["minCrystalEarnedTotal"] = _to_num0(er.get("minCrystalEarnedTotal"))
    rec["employeeRating"]["seasonCode"] = _norm(er.get("seasonCode"))

    if data.get("itemAmount") is not None:
        ia = _to_num0(data.get("itemAmount"))
        rec["itemAmount"] = int(ia) if ia == int(ia) else ia
    return rec


def build_item_catalog_from_reward_df(
    reward_df: pd.DataFrame,
    reward_type_col: str,
    reward_code_col: str,
    full_name_col: str = "FULL_NAME",
    add_data_col: str = "REWARD_ADD_DATA",
) -> Dict[str, Dict[str, Any]]:
    """
    Словарь REWARD_CODE -> запись каталога только для строк с REWARD_TYPE = ITEM.
    При дубликате кода последняя строка перезаписывает предыдущую.
    """
    catalog: Dict[str, Dict[str, Any]] = {}
    if reward_type_col not in reward_df.columns or reward_code_col not in reward_df.columns:
        logging.warning(
            f"[reward_item_catalog] Нет колонок «{reward_type_col}» / «{reward_code_col}» на листе REWARD"
        )
        return catalog

    has_add = add_data_col in reward_df.columns
    has_fn = full_name_col in reward_df.columns

    mask = reward_df[reward_type_col].astype(str).str.strip().str.upper() == "ITEM"
    sub = reward_df.loc[mask]
    for _, row in sub.iterrows():
        code = _norm(row.get(reward_code_col))
        if not code:
            continue
        fn = _norm(row.get(full_name_col)) if has_fn else ""
        raw_cell = row.get(add_data_col) if has_add else None
        s = normalize_reward_add_data_string(raw_cell)
        parsed: Any = None
        if s:
            parsed = safe_json_loads(s)
        if parsed is None and isinstance(raw_cell, dict):
            parsed = raw_cell
        if not isinstance(parsed, dict):
            parsed = {}
        catalog[code] = parse_reward_add_data_object(parsed, code, fn)

    logging.info(f"[reward_item_catalog] Записей ITEM в каталоге: {len(catalog)}")
    return catalog


def rules_for_matrix_column(
    match_code: str,
    catalog: Dict[str, Dict[str, Any]],
    *,
    min_bank: Optional[float] = None,
    min_tb: Optional[float] = None,
    min_gosb: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Правила доступности для колонки матрицы: из каталога по REWARD_CODE или
    синтетическая запись из плоских порогов строки REWARD (если JSON не разобран).
    """
    if match_code in catalog:
        return catalog[match_code]
    rec = parse_reward_add_data_object({}, match_code, "")
    if min_bank is not None:
        rec["employeeRating"]["minRating"]["minRatingBANK"] = float(min_bank)
    if min_tb is not None:
        rec["employeeRating"]["minRating"]["minRatingTB"] = float(min_tb)
    if min_gosb is not None:
        rec["employeeRating"]["minRating"]["minRatingGOSB"] = float(min_gosb)
    return rec


def item_accessible_for_manager(
    rules: Dict[str, Any],
    *,
    rank_country: Optional[float],
    rank_tb: Optional[float],
    rank_gosb: Optional[float],
    crystals: Optional[float],
    order_product_codes: Set[str],
    list_reward_codes: Set[str],
) -> bool:
    """
    Все критерии ToDo для доступности товара менеджеру (True = доступен, зелёная ячейка).

    minRating*: учитываются только значения > 0; место в рейтинге должно быть <= порога.
    minCrystalEarnedTotal: если > 0, кристаллы >= порога.
    rewardCode: если список не пуст, каждый код должен быть в list_reward_codes.
    nonRewardCode: если список не пуст, ни один код не должен быть в order_product_codes.
    """
    er = rules.get("employeeRating") or {}
    mn = (er.get("minRating") or {})

    t_bank = float(mn.get("minRatingBANK") or 0)
    if t_bank > 0:
        if rank_country is None or rank_country > t_bank:
            return False

    t_tb = float(mn.get("minRatingTB") or 0)
    if t_tb > 0:
        if rank_tb is None or rank_tb > t_tb:
            return False

    t_g = float(mn.get("minRatingGOSB") or 0)
    if t_g > 0:
        if rank_gosb is None or rank_gosb > t_g:
            return False

    mc = float(er.get("minCrystalEarnedTotal") or 0)
    if mc > 0:
        if crystals is None or crystals < mc:
            return False

    gc = rules.get("getCondition") or {}
    r_codes = [x for x in (gc.get("rewardCode") or []) if _norm(x)]
    if r_codes:
        owned = {_norm(x) for x in list_reward_codes}
        for rc in r_codes:
            if _norm(rc) not in owned:
                return False

    nonr = [x for x in (gc.get("nonRewardCode") or []) if _norm(x)]
    if nonr:
        ordered = {_norm(x) for x in order_product_codes}
        for nr in nonr:
            if _norm(nr) in ordered:
                return False

    return True
