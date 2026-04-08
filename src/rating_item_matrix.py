# -*- coding: utf-8 -*-
"""
Матрица на листе RATING: колонки по наградам ITEM из REWARD, счётчики по листу ORDER, подсветка ячеек.

Выполняется после merge_fields_advanced (ожидаются развёрнутые колонки ADD_DATA => ... на REWARD).
"""

from __future__ import annotations

import logging
import unicodedata
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Union

import numpy as np
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import PatternFill

from src.reward_item_catalog import (
    build_item_catalog_from_reward_df,
    item_accessible_for_manager,
    rules_for_matrix_column,
)


def _as_float(x: Any) -> Optional[float]:
    """Преобразование значения ячейки в float; None если не число или пусто."""
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    if isinstance(x, str):
        s = x.strip().replace(",", ".")
        if not s or s == "-":
            return None
        try:
            return float(s)
        except ValueError:
            return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _norm_str(x: Any) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    return str(x).strip()


# Запасные имена колонок для типичных выгрузок (русские заголовки и англ. поля gamification)
_DEFAULT_ORDER_EMP: List[str] = [
    "Табельный номер",
    "PERSON_NUMBER",
    "PERSON_NUMBER_ADD",
    "personNumber",
    "EMPLOYEE_NUMBER",
]
_DEFAULT_ORDER_PROD: List[str] = [
    "Код товара",
    "REWARD_CODE",
    "ITEM_CODE",
    "PRODUCT_CODE",
    "rewardCode",
    "REWARD",
]
_DEFAULT_RATING_EMP: List[str] = _DEFAULT_ORDER_EMP
_DEFAULT_COUNTRY_RANK: List[str] = [
    "Место в рейтинге по стране",
    "RANK_COUNTRY",
    "COUNTRY_RANK",
    "PLACE_COUNTRY",
    "countryRatingPlace",
    "ratingPlaceCountry",
]
_DEFAULT_TB_RANK: List[str] = [
    "Место в рейтинге ТБ",
    "RANK_TB",
    "TB_RANK",
    "PLACE_TB",
    "tbRatingPlace",
    "ratingPlaceTB",
]
_DEFAULT_GOSB_RANK: List[str] = [
    "Место в рейтинге ГОСБ",
    "RANK_GOSB",
    "GOSB_RANK",
    "PLACE_GOSB",
    "gosbRatingPlace",
    "ratingPlaceGOSB",
]
# Лист LIST-REWARDS: табельный и код награды (русские заголовки и запасные англ. имена)
_DEFAULT_LIST_RW_EMP: List[str] = [
    "Табельный номер сотрудника",
    "PERSON_NUMBER",
    "PERSON_NUMBER_ADD",
    "personNumber",
    "Табельный номер",
    "EMPLOYEE_NUMBER",
]
_DEFAULT_LIST_RW_CODE: List[str] = [
    "Код награды",
    "REWARD_CODE",
    "rewardCode",
    "Код",
]
_DEFAULT_CRYSTALS: List[str] = [
    "Количество кристаллов",
    "CRYSTAL_COUNT",
    "crystalsEarnedTotal",
    "crystalEarnedTotal",
]


def _norm_header(s: str) -> str:
    """Нормализация имени столбца для сопоставления (BOM, NFKC, регистр)."""
    t = unicodedata.normalize("NFKC", str(s)).strip()
    if t.startswith("\ufeff"):
        t = t.lstrip("\ufeff").strip()
    return t.casefold()


def _build_header_index(df: pd.DataFrame) -> Dict[str, str]:
    """casefold-ключ -> исходное имя столбца в DataFrame."""
    out: Dict[str, str] = {}
    for c in df.columns:
        out[_norm_header(str(c))] = c
    return out


def _resolve_column(df: pd.DataFrame, names: Union[str, List[str], None], defaults: List[str]) -> Optional[str]:
    """
    Первое найденное имя столбца: сначала варианты из конфига (строка или список), затем defaults.
    Сравнение по нормализованным строкам и точному вхождению в columns.
    """
    candidates: List[str] = []
    if names is not None:
        if isinstance(names, str) and names.strip():
            candidates.append(names.strip())
        elif isinstance(names, list):
            candidates.extend(str(x).strip() for x in names if str(x).strip())
    candidates.extend(d for d in defaults if d not in candidates)

    idx = _build_header_index(df)
    for want in candidates:
        wn = _norm_header(want)
        if wn in idx:
            resolved = idx[wn]
            if want != resolved:
                logging.info(f"[rating_item_matrix] Столбец «{want}» сопоставлен с «{resolved}»")
            return resolved
        if want in df.columns:
            return want
    return None


def _find_column_by_fragments(df: pd.DataFrame, fragments: List[str]) -> Optional[str]:
    """Первый столбец, в имени которого встречаются все подстроки (без учёта регистра)."""
    fr = [f.casefold() for f in fragments]
    for c in df.columns:
        s = str(c).casefold()
        if all(f in s for f in fr):
            return c
    return None


def _make_unique_col_name(base: str, existing: set) -> str:
    """Уникальное имя колонки в Excel (дубликаты REWARD_CODE)."""
    name = base.strip() or "ITEM"
    if name not in existing:
        existing.add(name)
        return name
    k = 2
    while f"{name}__{k}" in existing:
        k += 1
    out = f"{name}__{k}"
    existing.add(out)
    return out


def _collect_item_reward_specs(
    reward_df: pd.DataFrame,
    rating_df: pd.DataFrame,
    cfg: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Строки REWARD с REWARD_TYPE == ITEM: код, сезон, пороги рейтинга из развёрнутых колонок.
    """
    rtc = cfg.get("reward_type_col") or "REWARD_TYPE"
    rcc = cfg.get("reward_code_col") or "REWARD_CODE"
    exact_season = cfg.get("col_season_code") or "ADD_DATA => getCondition => employeeRating => seasonCode"
    exact_bank = cfg.get("col_min_rating_bank") or "ADD_DATA => getCondition => employeeRating => minRatingBANK"
    exact_tb = cfg.get("col_min_rating_tb") or "ADD_DATA => getCondition => employeeRating => minRatingTB"
    exact_gosb = cfg.get("col_min_rating_gosb") or "ADD_DATA => getCondition => employeeRating => minRatingGOSB"

    def _metric_col(exact: str, frags: List[str]) -> Optional[str]:
        if exact in reward_df.columns:
            return exact
        found = _find_column_by_fragments(reward_df, frags)
        if found:
            logging.info(f"[rating_item_matrix] Порог рейтинга: вместо «{exact}» используется «{found}»")
        return found

    c_season = _metric_col(exact_season, ["employeeRating", "seasonCode"])
    c_bank = _metric_col(exact_bank, ["employeeRating", "minRatingBANK"])
    c_tb = _metric_col(exact_tb, ["employeeRating", "minRatingTB"])
    c_gosb = _metric_col(exact_gosb, ["employeeRating", "minRatingGOSB"])

    rtc_res = _resolve_column(reward_df, rtc, ["REWARD_TYPE", "reward_type", "RewardType"])
    rcc_res = _resolve_column(reward_df, rcc, ["REWARD_CODE", "reward_code", "RewardCode"])
    if rtc_res is None or rcc_res is None:
        logging.warning(
            f"[rating_item_matrix] На листе REWARD не найдены колонки типа/кода награды "
            f"(ожидались «{rtc}», «{rcc}»). Заголовки (первые 40): {list(reward_df.columns)[:40]}"
        )
        return []

    mask = reward_df[rtc_res].astype(str).str.strip().str.upper() == "ITEM"
    sub = reward_df.loc[mask].copy()
    if sub.empty:
        logging.info(
            f"[rating_item_matrix] Нет строк REWARD с типом ITEM (колонка «{rtc_res}»). "
            f"Уникальные значения типа (до 15): {reward_df[rtc_res].astype(str).str.strip().unique()[:15].tolist()}"
        )
        return []

    # Имена новых колонок не должны пересекаться с уже существующими на RATING
    existing_names: set = set(str(c) for c in rating_df.columns)
    rows: List[Dict[str, Any]] = []
    for _, row in sub.iterrows():
        code = _norm_str(row.get(rcc_res))
        if not code:
            continue
        season = _norm_str(row.get(c_season)) if c_season and c_season in sub.columns else ""
        col_name = code
        if season:
            col_name = f"{code} ({season})"
        col_name = _make_unique_col_name(col_name, existing_names)
        rows.append(
            {
                "col_name": col_name,
                "match_code": code,
                "min_bank": _as_float(row.get(c_bank)) if c_bank and c_bank in sub.columns else None,
                "min_tb": _as_float(row.get(c_tb)) if c_tb and c_tb in sub.columns else None,
                "min_gosb": _as_float(row.get(c_gosb)) if c_gosb and c_gosb in sub.columns else None,
                "sort_season": season or "",
            }
        )

    rows.sort(key=lambda r: (_norm_str(r["match_code"]).lower(), r["sort_season"].lower()))
    return rows


def apply_rating_item_matrix_enrichment(
    sheets_data: Dict[str, Any],
    cfg: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Добавляет на лист RATING колонки со счётчиками заказов по кодам ITEM (аналог СЧЁТЕСЛИМН).

    Возвращает метаданные для последующей подсветки или None, если шаг пропущен.
    """
    if not cfg or not bool(cfg.get("enabled")):
        return None

    sr = cfg.get("sheet_rating") or "RATING"
    so = cfg.get("sheet_order") or "ORDER"
    rw = cfg.get("sheet_reward") or "REWARD"

    for key, name in (("rating", sr), ("order", so), ("reward", rw)):
        if name not in sheets_data or sheets_data[name] is None:
            logging.warning(f"[rating_item_matrix] Нет листа «{name}» в данных — шаг пропущен")
            return None

    rating_t = sheets_data[sr]
    order_t = sheets_data[so]
    reward_t = sheets_data[rw]
    rating_df = rating_t[0]
    order_df = order_t[0]
    if not isinstance(rating_df, pd.DataFrame) or not isinstance(order_df, pd.DataFrame):
        return None

    emp_o = _resolve_column(order_df, cfg.get("order_employee_col"), _DEFAULT_ORDER_EMP)
    prod_o = _resolve_column(order_df, cfg.get("order_product_col"), _DEFAULT_ORDER_PROD)
    emp_r = _resolve_column(rating_df, cfg.get("rating_employee_col"), _DEFAULT_RATING_EMP)
    country_c = _resolve_column(rating_df, cfg.get("country_rank_col"), _DEFAULT_COUNTRY_RANK)
    tb_c = _resolve_column(rating_df, cfg.get("tb_rank_col"), _DEFAULT_TB_RANK)
    gosb_c = _resolve_column(rating_df, cfg.get("gosb_rank_col"), _DEFAULT_GOSB_RANK)

    if emp_o is None or prod_o is None or emp_r is None:
        logging.warning(
            f"[rating_item_matrix] Не удалось сопоставить обязательные столбцы ORDER/RATING. "
            f"ORDER: сотрудник={emp_o!r}, товар/код={prod_o!r}; RATING: сотрудник={emp_r!r}. "
            f"Колонки ORDER (до 30): {list(order_df.columns)[:30]}; RATING (до 30): {list(rating_df.columns)[:30]}"
        )
        return None

    if not any((country_c, tb_c, gosb_c)):
        logging.warning(
            f"[rating_item_matrix] Нет ни одного столбца места в рейтинге — "
            f"country={country_c!r}, tb={tb_c!r}, gosb={gosb_c!r}. "
            "Критерии minRating* по отсутствующим колонкам будут считаться невыполненными (красная ячейка), если порог > 0."
        )
    elif not (country_c and tb_c and gosb_c):
        logging.info(
            f"[rating_item_matrix] Часть столбцов рейтинга не найдена (country={country_c!r}, tb={tb_c!r}, gosb={gosb_c!r}) — "
            "доступность считается только по найденным местам в рейтинге."
        )

    specs = _collect_item_reward_specs(reward_t[0], rating_df, cfg)
    if not specs:
        logging.warning(
            "[rating_item_matrix] Список ITEM-наград пуст — колонки на RATING не добавлялись "
            "(проверьте REWARD_TYPE=ITEM и разворот ADD_DATA на листе REWARD)."
        )
        return None

    rating_df = rating_df.copy()
    prod_order_series = order_df[prod_o].map(_norm_str)

    thresholds: Dict[str, Dict[str, Optional[float]]] = {}
    added: List[str] = []

    for sp in specs:
        code = sp["match_code"]
        cname = sp["col_name"]
        mask = prod_order_series == code
        sub = order_df.loc[mask]
        emp_sub = sub[emp_o].map(_norm_str)
        vc = emp_sub.value_counts(dropna=False)
        cnt_dict: Dict[str, int] = {str(k): int(v) for k, v in vc.items()}
        emp_r_norm = rating_df[emp_r].map(_norm_str)
        vals = emp_r_norm.map(lambda e: int(cnt_dict.get(e, 0)))
        rating_df[cname] = vals.where(vals > 0, np.nan)
        added.append(cname)
        thresholds[cname] = {
            "bank": sp["min_bank"],
            "tb": sp["min_tb"],
            "gosb": sp["min_gosb"],
        }

    reward_df0 = reward_t[0]
    rtc_res2 = _resolve_column(
        reward_df0, cfg.get("reward_type_col"), ["REWARD_TYPE", "reward_type", "RewardType"]
    )
    rcc_res2 = _resolve_column(
        reward_df0, cfg.get("reward_code_col"), ["REWARD_CODE", "reward_code", "RewardCode"]
    )
    adc = str(cfg.get("reward_add_data_col") or "REWARD_ADD_DATA")
    if adc not in reward_df0.columns:
        adc = "REWARD_ADD_DATA"
    if rtc_res2 is None or rcc_res2 is None:
        catalog = {}
        logging.warning("[rating_item_matrix] Каталог ITEM из REWARD не построен: нет колонок типа/кода награды")
    else:
        catalog = build_item_catalog_from_reward_df(
            reward_df0,
            rtc_res2,
            rcc_res2,
            full_name_col="FULL_NAME",
            add_data_col=adc,
        )

    order_by_emp: Dict[str, Set[str]] = defaultdict(set)
    for _, orow in order_df.iterrows():
        e = _norm_str(orow.get(emp_o))
        p = _norm_str(orow.get(prod_o))
        if e and p:
            order_by_emp[e].add(p)

    slr = cfg.get("sheet_list_rewards") or "LIST-REWARDS"
    rewards_by_emp: Dict[str, Set[str]] = defaultdict(set)
    if slr in sheets_data and sheets_data[slr] is not None:
        lr_df = sheets_data[slr][0]
        if isinstance(lr_df, pd.DataFrame):
            le = _resolve_column(lr_df, cfg.get("list_rewards_employee_col"), _DEFAULT_LIST_RW_EMP)
            lc = _resolve_column(lr_df, cfg.get("list_rewards_code_col"), _DEFAULT_LIST_RW_CODE)
            if le and lc:
                for _, lrow in lr_df.iterrows():
                    e2 = _norm_str(lrow.get(le))
                    c2 = _norm_str(lrow.get(lc))
                    if e2 and c2:
                        rewards_by_emp[e2].add(c2)
            else:
                logging.warning(
                    f"[rating_item_matrix] Лист «{slr}»: не найдены колонки табельного/кода награды — "
                    f"критерий rewardCode для всех товаров считается невыполненным, если список кодов не пуст."
                )
    else:
        logging.info(f"[rating_item_matrix] Лист «{slr}» отсутствует — множество наград сотрудника пустое.")

    cry_c = _resolve_column(rating_df, cfg.get("crystals_col"), _DEFAULT_CRYSTALS)

    accessibility_cells: List[Dict[str, Any]] = []
    for pos, (_, row) in enumerate(rating_df.iterrows()):
        emp_key = _norm_str(row.get(emp_r))
        rc = _as_float(row.get(country_c)) if country_c else None
        rt = _as_float(row.get(tb_c)) if tb_c else None
        rg = _as_float(row.get(gosb_c)) if gosb_c else None
        cry = _as_float(row.get(cry_c)) if cry_c else None
        order_codes = order_by_emp.get(emp_key, set())
        rw_codes = rewards_by_emp.get(emp_key, set())
        excel_row = pos + 2
        for sp in specs:
            rules = rules_for_matrix_column(
                sp["match_code"],
                catalog,
                min_bank=sp.get("min_bank"),
                min_tb=sp.get("min_tb"),
                min_gosb=sp.get("min_gosb"),
            )
            ok = item_accessible_for_manager(
                rules,
                rank_country=rc,
                rank_tb=rt,
                rank_gosb=rg,
                crystals=cry,
                order_product_codes=order_codes,
                list_reward_codes=rw_codes,
                manager_tab=emp_key or None,
            )
            accessibility_cells.append(
                {"row_excel": excel_row, "col_name": sp["col_name"], "ok": ok}
            )

    sheets_data[sr] = (rating_df, rating_t[1])
    logging.info(
        f"[rating_item_matrix] Лист «{sr}»: добавлено колонок ITEM-матрицы: {len(added)}"
    )

    return {
        "sheet_rating": sr,
        "added_columns": added,
        "thresholds": thresholds,
        "country_rank_col": country_c,
        "tb_rank_col": tb_c,
        "gosb_rank_col": gosb_c,
        "accessibility_cells": accessibility_cells,
        "fill_accessibility_ok": (cfg.get("fill_accessibility_ok") or "C6EFCE").lstrip("#"),
        "fill_accessibility_fail": (cfg.get("fill_accessibility_fail") or "FFC7CE").lstrip("#"),
        # Раскраска по полной доступности (зелёный / красный), без старой тройной подсветки по minRating
        "skip_colors": False,
    }


def _header_col_map(ws: Any) -> Dict[str, int]:
    """Имя заголовка (строка 1) -> индекс столбца openpyxl (1-based)."""
    m: Dict[str, int] = {}
    for c in range(1, ws.max_column + 1):
        v = ws.cell(row=1, column=c).value
        if v is not None:
            m[str(v).strip()] = c
    return m


def apply_rating_item_matrix_colors(
    xlsx_path: str,
    meta: Dict[str, Any],
    cfg: Dict[str, Any],
) -> None:
    """Подсветка ячеек матрицы на сохранённом файле Excel (после write_to_excel)."""
    if not cfg or not bool(cfg.get("enabled")):
        return
    if meta.get("skip_colors"):
        logging.info("[rating_item_matrix] Подсветка отключена флагом skip_colors")
        return
    cells = meta.get("accessibility_cells") or []
    if not cells:
        logging.warning("[rating_item_matrix] Нет предвычисленных ячеек доступности — подсветка пропущена")
        return
    try:
        wb = load_workbook(xlsx_path)
    except OSError as e:
        logging.warning(f"[rating_item_matrix] Не удалось открыть файл для подсветки: {e}")
        return

    sn = meta.get("sheet_rating") or "RATING"
    if sn not in wb.sheetnames:
        wb.close()
        return
    ws = wb[sn]
    hmap = _header_col_map(ws)

    def col_for(name: str) -> Optional[int]:
        return hmap.get(str(name).strip())

    ok_hex = meta.get("fill_accessibility_ok") or "C6EFCE"
    fail_hex = meta.get("fill_accessibility_fail") or "FFC7CE"
    fill_ok = PatternFill(fill_type="solid", start_color=ok_hex, end_color=ok_hex)
    fill_fail = PatternFill(fill_type="solid", start_color=fail_hex, end_color=fail_hex)

    n_applied = 0
    for item in cells:
        r = int(item.get("row_excel") or 0)
        cname = item.get("col_name")
        ok = bool(item.get("ok"))
        if r < 2 or not cname:
            continue
        ci = col_for(str(cname))
        if ci is None:
            continue
        ws.cell(row=r, column=ci).fill = fill_ok if ok else fill_fail
        n_applied += 1

    wb.save(xlsx_path)
    wb.close()
    logging.info(
        f"[rating_item_matrix] Подсветка доступности ITEM (зелёный/красный): {n_applied} ячеек в {xlsx_path}"
    )
