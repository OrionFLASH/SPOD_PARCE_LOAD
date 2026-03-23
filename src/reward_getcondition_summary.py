# -*- coding: utf-8 -*-
"""
Формирование сводной текстовой колонки на листе REWARD по кодам из getCondition:
nonRewards[i].nonRewardCode и rewards[j].rewardCode с подстановкой FULL_NAME и seasonItem
целевой награды (по справочнику из того же листа), аналогично формуле Excel СЦЕПИТЬ/ВПР.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


def _cell_str(val: Any) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    s = str(val).strip()
    return s if s not in ("", "nan", "None") else ""


def _build_code_lookup(df: pd.DataFrame, season_col: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Строит отображение REWARD_CODE -> FULL_NAME и REWARD_CODE -> seasonItem
    по всем строкам листа REWARD (как ВПР по справочнику наград).
    """
    code_to_full: Dict[str, str] = {}
    code_to_season: Dict[str, str] = {}
    if "REWARD_CODE" not in df.columns:
        return code_to_full, code_to_season
    full_col = "FULL_NAME" if "FULL_NAME" in df.columns else None
    for _, row in df.iterrows():
        code = _cell_str(row.get("REWARD_CODE"))
        if not code:
            continue
        if full_col:
            code_to_full[code] = _cell_str(row.get(full_col))
        else:
            code_to_full[code] = ""
        if season_col in df.columns:
            code_to_season[code] = _cell_str(row.get(season_col))
        else:
            code_to_season[code] = ""
    return code_to_full, code_to_season


def _collect_indexed_columns(
    columns: List[str],
    prefix: str,
    kind: str,
) -> List[Tuple[int, str]]:
    """
    Находит пары (индекс [i], фактическое имя колонки) для полей вида
    ``{prefix} => getCondition => nonRewards => [i] => nonRewardCode``
    или ``... => rewards => [i] => rewardCode``.

    Допускаются лишние пробелы вокруг ``=>`` (иногда в заголовках CSV/Excel
    появляются двойные пробелы — жёсткое совпадение теряло колонки).
    """
    pfx = (prefix or "").strip()
    if kind == "nonRewards":
        pat = re.compile(
            re.escape(pfx)
            + r"\s*=>\s*getCondition\s*=>\s*nonRewards\s*=>\s*\[(\d+)\]\s*=>\s*nonRewardCode\s*$",
            re.IGNORECASE,
        )
    else:
        pat = re.compile(
            re.escape(pfx)
            + r"\s*=>\s*getCondition\s*=>\s*rewards\s*=>\s*\[(\d+)\]\s*=>\s*rewardCode\s*$",
            re.IGNORECASE,
        )
    # Индекс -> первое встретившееся имя колонки (порядок колонок в df сохраняем по возрастанию i)
    by_idx: Dict[int, str] = {}
    for c in columns:
        cs = (c or "").strip()
        m = pat.match(cs)
        if m:
            i = int(m.group(1))
            if i not in by_idx:
                by_idx[i] = cs
    return [(i, by_idx[i]) for i in sorted(by_idx.keys())]


def add_reward_getcondition_summary_column(
    df_reward: pd.DataFrame,
    *,
    prefix: str = "ADD_DATA",
    column_name: str = "Сводка: nonRewards и rewards (getCondition)",
) -> pd.DataFrame:
    """
    Добавляет в конец DataFrame колонку с многострочным текстом:
    для каждого непустого nonRewardCode / rewardCode — строка
    ``[код] FULL_NAME {seasonItem}`` (перенос строки между блоками).

    Args:
        df_reward: лист REWARD после разворота JSON (колонки с префиксом ``prefix``).
        prefix: префикс разворота из json_columns (для REWARD обычно ``ADD_DATA``).
        column_name: имя итоговой колонки.

    Returns:
        Копия DataFrame с колонкой сводки (в конце). Колонка добавляется всегда:
        при отсутствии полей getCondition ячейки остаются пустыми — так колонка
        видна в Excel и в списке заголовков.
    """
    # seasonItem: как в flatten — с одним пробелом вокруг =>; при отсутствии — ищем гибко
    season_col_exact = f"{prefix.strip()} => seasonItem"
    season_col = season_col_exact if season_col_exact in df_reward.columns else None
    if season_col is None:
        _sc_pat = re.compile(
            re.escape(prefix.strip()) + r"\s*=>\s*seasonItem\s*$",
            re.IGNORECASE,
        )
        for _c in df_reward.columns:
            if _sc_pat.match((_c or "").strip()):
                season_col = _c
                break
    if season_col is None:
        season_col = season_col_exact  # для _build_code_lookup — колонки не будет

    non_entries = _collect_indexed_columns(list(df_reward.columns), prefix, "nonRewards")
    rew_entries = _collect_indexed_columns(list(df_reward.columns), prefix, "rewards")
    if not non_entries and not rew_entries:
        logging.info(
            "[REWARD getCondition summary] Колонки nonRewards/rewards не найдены по шаблону — "
            "добавлена пустая колонка сводки (проверьте префикс json_columns.REWARD и разворот JSON)."
        )

    code_to_full, code_to_season = _build_code_lookup(df_reward, season_col)

    def build_one_row(row: pd.Series) -> str:
        lines: List[str] = []
        for _i, col_nm in non_entries:
            code = _cell_str(row.get(col_nm))
            if not code:
                continue
            full = code_to_full.get(code, "")
            sea = code_to_season.get(code, "")
            lines.append(f"[{code}] {full} {{{sea}}}")
        for _j, col_nm in rew_entries:
            code = _cell_str(row.get(col_nm))
            if not code:
                continue
            full = code_to_full.get(code, "")
            sea = code_to_season.get(code, "")
            lines.append(f"[{code}] {full} {{{sea}}}")
        return "\n".join(lines)

    out = df_reward.copy()
    out[column_name] = out.apply(build_one_row, axis=1)
    logging.info(
        f"[REWARD getCondition summary] Добавлена колонка «{column_name}» "
        f"(nonRewards: {len(non_entries)} полей, rewards: {len(rew_entries)} полей)"
    )
    return out
