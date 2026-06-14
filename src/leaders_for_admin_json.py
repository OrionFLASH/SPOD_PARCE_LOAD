# -*- coding: utf-8 -*-
"""
Разбор leadersForAdmin JSON: претенденты на награду по divisionRatings.
"""
from __future__ import annotations

import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any, DefaultDict, Dict, Iterable, Mapping, Optional, Sequence, Set, Tuple

from src.manager_stats import normalize_tab_number

DEFAULT_PRETENDER_CATEGORIES: Tuple[str, ...] = (
    "Серебро",
    "Бронза",
    "Вы в лидерах",
)


def _norm_code(value: Any) -> str:
    """Нормализация кода турнира/строки для сравнения."""
    return str(value or "").strip()


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def resolve_leaders_for_admin_json_path(
    catalog_cfg: Mapping[str, Any],
    *,
    paths_cfg: Optional[Mapping[str, Any]] = None,
) -> Optional[Path]:
    """Путь к JSON leadersForAdmin в IN/<subdir>/ (имя файла из config)."""
    if catalog_cfg.get("leaders_for_admin_json_enabled") is False:
        return None
    filename = str(catalog_cfg.get("leaders_for_admin_json_file") or "").strip()
    if not filename:
        return None
    subdir = str(catalog_cfg.get("leaders_for_admin_json_subdir") or "JS").strip()
    input_rel = str((paths_cfg or {}).get("input") or "IN").strip() or "IN"
    base = Path(input_rel)
    if not base.is_absolute():
        base = _project_root() / base
    return base / subdir / filename if subdir else base / filename


def pretender_categories_from_config(catalog_cfg: Mapping[str, Any]) -> Set[str]:
    """Категории ratingCategoryName, означающие претендента на награду."""
    raw = catalog_cfg.get("leaders_for_admin_pretender_categories")
    if isinstance(raw, list) and raw:
        return {str(x).strip() for x in raw if str(x).strip()}
    return set(DEFAULT_PRETENDER_CATEGORIES)


def _leader_is_pretender(
    leader: Mapping[str, Any],
    pretender_categories: Set[str],
) -> bool:
    ratings = leader.get("divisionRatings")
    if not isinstance(ratings, list):
        return False
    for block in ratings:
        if not isinstance(block, dict):
            continue
        name = str(block.get("ratingCategoryName") or "").strip()
        if name in pretender_categories:
            return True
    return False


def _count_pretender_hits_in_leader(
    leader: Mapping[str, Any],
    pretender_categories: Set[str],
) -> int:
    """Сколько блоков divisionRatings с категорией претендента у одного leader."""
    ratings = leader.get("divisionRatings")
    if not isinstance(ratings, list):
        return 0
    n = 0
    for block in ratings:
        if not isinstance(block, dict):
            continue
        name = str(block.get("ratingCategoryName") or "").strip()
        if name in pretender_categories:
            n += 1
    return n


def _extract_leaders_from_tournament_entry(entry: Any) -> Iterable[Mapping[str, Any]]:
    """Leaders из одной записи массива по ключу турнира в JSON."""
    if not isinstance(entry, list) or not entry:
        return []
    root = entry[0]
    if not isinstance(root, dict):
        return []
    body = root.get("body")
    if not isinstance(body, dict):
        return []
    tournament = body.get("tournament")
    if isinstance(tournament, dict):
        leaders = tournament.get("leaders")
        if isinstance(leaders, list):
            return [x for x in leaders if isinstance(x, dict)]
    badge = body.get("badge")
    if isinstance(badge, dict):
        leaders = badge.get("leaders")
        if isinstance(leaders, list):
            return [x for x in leaders if isinstance(x, dict)]
    return []


def parse_leaders_for_admin_pretender_counts(
    json_path: Path,
    *,
    tournament_codes: Optional[Set[str]] = None,
    pretender_categories: Optional[Set[str]] = None,
    pad_width: int = 20,
) -> Dict[str, Dict[str, int]]:
    """
    tab → count по каждому TOURNAMENT_CODE из JSON.

    Учитываются только турниры из tournament_codes (если задано).
    Счётчик — сумма попаданий в pretender_categories по divisionRatings.
    """
    categories = pretender_categories or set(DEFAULT_PRETENDER_CATEGORIES)
    if not json_path.is_file():
        logging.warning("[manager_stats] leadersForAdmin JSON не найден: %s", json_path)
        return {}

    try:
        with json_path.open(encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        logging.warning("[manager_stats] leadersForAdmin JSON: ошибка чтения %s: %s", json_path, exc)
        return {}

    if not isinstance(data, dict):
        logging.warning("[manager_stats] leadersForAdmin JSON: ожидался объект верхнего уровня")
        return {}

    leaders_filter = {_norm_code(c) for c in tournament_codes if _norm_code(c)} if tournament_codes else None
    result: DefaultDict[str, DefaultDict[str, int]] = defaultdict(lambda: defaultdict(int))
    for tournament_id, entry in data.items():
        tid = str(tournament_id or "").strip()
        if not tid:
            continue
        if leaders_filter is not None and tid not in leaders_filter:
            continue
        for leader in _extract_leaders_from_tournament_entry(entry):
            tab = normalize_tab_number(leader.get("employeeNumber"), pad_width)
            if not tab:
                continue
            hits = _count_pretender_hits_in_leader(leader, categories)
            if hits > 0:
                result[tid][tab] += hits

    if leaders_filter is not None:
        for tid in leaders_filter:
            if tid in data and tid not in result:
                result[tid] = {}
    return {tid: dict(tabs) for tid, tabs in result.items()}
