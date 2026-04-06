# -*- coding: utf-8 -*-
"""
Матрица на листе RATING: колонки по наградам ITEM из REWARD, счётчики по листу ORDER, подсветка ячеек.

Выполняется после merge_fields_advanced (ожидаются развёрнутые колонки ADD_DATA => ... на REWARD).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import PatternFill


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
    c_season = cfg.get("col_season_code") or "ADD_DATA => getCondition => employeeRating => seasonCode"
    c_bank = cfg.get("col_min_rating_bank") or "ADD_DATA => getCondition => employeeRating => minRatingBANK"
    c_tb = cfg.get("col_min_rating_tb") or "ADD_DATA => getCondition => employeeRating => minRatingTB"
    c_gosb = cfg.get("col_min_rating_gosb") or "ADD_DATA => getCondition => employeeRating => minRatingGOSB"

    need = [rtc, rcc]
    for c in need:
        if c not in reward_df.columns:
            logging.warning(f"[rating_item_matrix] На листе REWARD нет колонки «{c}»")
            return []

    mask = reward_df[rtc].astype(str).str.strip().str.upper() == "ITEM"
    sub = reward_df.loc[mask].copy()
    if sub.empty:
        logging.info("[rating_item_matrix] Нет строк REWARD с REWARD_TYPE=ITEM")
        return []

    # Имена новых колонок не должны пересекаться с уже существующими на RATING
    existing_names: set = set(str(c) for c in rating_df.columns)
    rows: List[Dict[str, Any]] = []
    for _, row in sub.iterrows():
        code = _norm_str(row.get(rcc))
        if not code:
            continue
        season = _norm_str(row.get(c_season)) if c_season in sub.columns else ""
        col_name = code
        if season:
            col_name = f"{code} ({season})"
        col_name = _make_unique_col_name(col_name, existing_names)
        rows.append(
            {
                "col_name": col_name,
                "match_code": code,
                "min_bank": _as_float(row.get(c_bank)) if c_bank in sub.columns else None,
                "min_tb": _as_float(row.get(c_tb)) if c_tb in sub.columns else None,
                "min_gosb": _as_float(row.get(c_gosb)) if c_gosb in sub.columns else None,
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

    emp_o = cfg.get("order_employee_col") or "Табельный номер"
    prod_o = cfg.get("order_product_col") or "Код товара"
    emp_r = cfg.get("rating_employee_col") or "Табельный номер"
    country_c = cfg.get("country_rank_col") or "Место в рейтинге по стране"
    tb_c = cfg.get("tb_rank_col") or "Место в рейтинге ТБ"
    gosb_c = cfg.get("gosb_rank_col") or "Место в рейтинге ГОСБ"

    for col, label in (
        (emp_o, "ORDER"),
        (prod_o, "ORDER"),
        (emp_r, "RATING"),
        (country_c, "RATING"),
        (tb_c, "RATING"),
        (gosb_c, "RATING"),
    ):
        df_ref = order_df if label == "ORDER" else rating_df
        if col not in df_ref.columns:
            logging.warning(f"[rating_item_matrix] Нет колонки «{col}» на листе {label}")
            return None

    specs = _collect_item_reward_specs(reward_t[0], rating_df, cfg)
    if not specs:
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
        "fill_country": cfg.get("fill_country_ok") or "C6EFCE",
        "fill_tb": cfg.get("fill_tb_ok") or "FFEB9C",
        "fill_gosb": cfg.get("fill_gosb_ok") or "BDD7EE",
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

    c_country = col_for(meta["country_rank_col"])
    c_tb = col_for(meta["tb_rank_col"])
    c_gosb = col_for(meta["gosb_rank_col"])
    if not c_country or not c_tb or not c_gosb:
        logging.warning("[rating_item_matrix] Подсветка: не найдены колонки рейтинга в файле")
        wb.close()
        return

    fill_country = PatternFill(
        fill_type="solid",
        start_color=meta.get("fill_country", "C6EFCE"),
        end_color=meta.get("fill_country", "C6EFCE"),
    )
    fill_tb = PatternFill(
        fill_type="solid",
        start_color=meta.get("fill_tb", "FFEB9C"),
        end_color=meta.get("fill_tb", "FFEB9C"),
    )
    fill_gosb = PatternFill(
        fill_type="solid",
        start_color=meta.get("fill_gosb", "BDD7EE"),
        end_color=meta.get("fill_gosb", "BDD7EE"),
    )

    thr_all: Dict[str, Dict[str, Optional[float]]] = meta.get("thresholds") or {}
    added = meta.get("added_columns") or []

    for r in range(2, ws.max_row + 1):
        v_country = _as_float(ws.cell(row=r, column=c_country).value)
        v_tb = _as_float(ws.cell(row=r, column=c_tb).value)
        v_gosb = _as_float(ws.cell(row=r, column=c_gosb).value)

        for ac in added:
            ci = col_for(ac)
            if ci is None:
                continue
            cell = ws.cell(row=r, column=ci)
            raw_v = cell.value
            if raw_v is None or raw_v == "":
                continue
            if isinstance(raw_v, (int, float)) and float(raw_v) == 0:
                continue

            tinfo = thr_all.get(ac) or {}
            t_b = tinfo.get("bank")
            t_t = tinfo.get("tb")
            t_g = tinfo.get("gosb")

            chosen: Optional[PatternFill] = None
            if t_b is not None and v_country is not None and v_country <= t_b:
                chosen = fill_country
            elif t_t is not None and v_tb is not None and v_tb <= t_t:
                chosen = fill_tb
            elif t_g is not None and v_gosb is not None and v_gosb <= t_g:
                chosen = fill_gosb

            if chosen is not None:
                cell.fill = chosen

    wb.save(xlsx_path)
    wb.close()
    logging.info(f"[rating_item_matrix] Подсветка матрицы применена к файлу: {xlsx_path}")
