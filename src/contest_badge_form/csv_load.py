# -*- coding: utf-8 -*-
"""Загрузка CSV листов SPOD для формы BADGE."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# Логические ключи → имя листа в input_files
SHEET_KEY_TO_INPUT: Dict[str, str] = {
    "contest": "CONTEST-DATA",
    "reward": "REWARD",
    "reward_link": "REWARD-LINK",
    "group": "GROUP",
    "indicator": "INDICATOR",
    "schedule": "TOURNAMENT-SCHEDULE",
}


def _input_files_for_block(cfg: Dict[str, Any], block: str) -> List[Dict[str, Any]]:
    """Список input_files для блока (объект по блокам или плоский список)."""
    raw = cfg.get("input_files")
    if isinstance(raw, dict):
        for key in (block, block.upper(), block.lower()):
            if key in raw and isinstance(raw[key], list):
                return list(raw[key])
        # один раздел
        if len(raw) == 1:
            only = next(iter(raw.values()))
            if isinstance(only, list):
                return list(only)
        return []
    if isinstance(raw, list):
        return list(raw)
    return []


def resolve_sheet_file(
    project_base_dir: str,
    cfg: Dict[str, Any],
    block: str,
    sheet_name: str,
) -> Optional[str]:
    """Абсолютный путь к CSV листа ``sheet_name`` из input_files блока."""
    paths = cfg.get("paths") or {}
    dir_input = str(paths.get("input") or "IN")
    for file_conf in _input_files_for_block(cfg, block):
        if str(file_conf.get("sheet") or "") != sheet_name:
            continue
        subdir = str(file_conf.get("subdir") or f"{block}/SPOD")
        filename = str(file_conf.get("file") or "")
        if not filename:
            continue
        path = os.path.join(project_base_dir, dir_input, subdir, filename)
        if os.path.isfile(path):
            return path
        logging.warning(
            "[contest_badge_form] Файл из конфига не найден: %s", path
        )
        return path
    return None


def load_spod_frames(
    project_base_dir: str,
    cfg: Dict[str, Any],
    block: str,
) -> Dict[str, pd.DataFrame]:
    """
    Загрузить DataFrame'ы по ключам contest/reward/….
    Пустой DataFrame, если файл отсутствует.
    """
    frames: Dict[str, pd.DataFrame] = {}
    for key, sheet_name in SHEET_KEY_TO_INPUT.items():
        path = resolve_sheet_file(project_base_dir, cfg, block, sheet_name)
        if not path or not os.path.isfile(path):
            logging.warning(
                "[contest_badge_form] Нет CSV для листа %s (ключ %s)",
                sheet_name,
                key,
            )
            frames[key] = pd.DataFrame()
            continue
        df = pd.read_csv(
            path,
            sep=";",
            dtype=str,
            keep_default_na=False,
            encoding="utf-8-sig",
        )
        frames[key] = df
        logging.info(
            "[contest_badge_form] Загружен %s → %s (%s строк)",
            sheet_name,
            path,
            len(df),
        )
    return frames


def filter_by_contest(
    df: pd.DataFrame, contest_code: str, col: str = "CONTEST_CODE"
) -> pd.DataFrame:
    """Строки с данным CONTEST_CODE."""
    if df.empty or col not in df.columns:
        return pd.DataFrame(columns=df.columns)
    return df[df[col].astype(str) == str(contest_code)].copy()


def rewards_for_contest(
    reward_df: pd.DataFrame,
    link_df: pd.DataFrame,
    contest_code: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Награды и связи конкурса.
    Возвращает (rewards_df, links_df).
    """
    links = filter_by_contest(link_df, contest_code)
    if links.empty or "REWARD_CODE" not in links.columns:
        return pd.DataFrame(columns=reward_df.columns), links
    codes = set(links["REWARD_CODE"].astype(str))
    if reward_df.empty or "REWARD_CODE" not in reward_df.columns:
        return pd.DataFrame(columns=reward_df.columns), links
    rewards = reward_df[reward_df["REWARD_CODE"].astype(str).isin(codes)].copy()
    return rewards, links
