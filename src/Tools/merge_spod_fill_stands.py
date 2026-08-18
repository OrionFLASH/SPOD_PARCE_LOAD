# -*- coding: utf-8 -*-
"""Слияние снимков web-fill PROM + PSI с метками стенда ``stands``."""

from __future__ import annotations

import copy
import logging
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

logger = logging.getLogger(__name__)

STAND_PROM = "PROM"
STAND_PSI = "PSI"
STAND_IFT = "IFT"
STANDS_ORDER: Tuple[str, ...] = (STAND_PROM, STAND_PSI, STAND_IFT)
PROJECT_JSON_VERSION_STANDS = 5

RowKeyFn = Callable[[Dict[str, Any]], Any]


def _deepcopy_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return copy.deepcopy(row)


def _strip_stands(value: Any) -> Any:
    """Убрать поле stands для сравнения данных."""
    if isinstance(value, dict):
        return {k: _strip_stands(v) for k, v in value.items() if k != "stands"}
    if isinstance(value, list):
        return [_strip_stands(v) for v in value]
    return value


def _rows_differ(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    return _strip_stands(a) != _strip_stands(b)


def _sort_stands(values: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for stand in STANDS_ORDER:
        if stand in values and stand not in seen:
            seen.add(stand)
            out.append(stand)
    for stand in values:
        if stand not in seen:
            seen.add(stand)
            out.append(stand)
    return out


def group_row_key(row: Dict[str, Any]) -> Tuple[str, str, str]:
    """CONTEST_CODE + GROUP_CODE + GROUP_VALUE — GROUP_CODE не уникален внутри конкурса."""
    return (
        str(row.get("CONTEST_CODE") or "").strip(),
        str(row.get("GROUP_CODE") or "").strip(),
        str(row.get("GROUP_VALUE") or "").strip(),
    )


def indicator_row_key(row: Dict[str, Any]) -> Tuple[str, str, str]:
    return (
        str(row.get("CONTEST_CODE") or "").strip(),
        str(row.get("INDICATOR_ADD_CALC_TYPE") or "").strip(),
        str(row.get("INDICATOR_CODE") or "").strip(),
    )


def schedule_row_key(row: Dict[str, Any]) -> str:
    return str(row.get("TOURNAMENT_CODE") or "").strip()


def link_row_key(row: Dict[str, Any], contest_code: str) -> Tuple[str, str, str]:
    return (
        contest_code,
        str(row.get("GROUP_CODE") or "").strip(),
        str(row.get("REWARD_CODE") or "").strip(),
    )


def annotate_stand_tags(payload: Dict[str, Any], stand: str) -> Dict[str, Any]:
    """Проставить ``stands: [stand]`` на конкурс и все строки данных."""
    tags = [stand]
    for item in payload.get("contests") or []:
        item["stands"] = list(tags)
        data = item.get("data")
        if not isinstance(data, dict):
            continue
        contest = data.get("contest")
        if isinstance(contest, dict):
            contest["stands"] = list(tags)
        for key in ("group", "indicator", "schedule", "reward_link"):
            rows = data.get(key)
            if not isinstance(rows, list):
                continue
            for row in rows:
                if isinstance(row, dict):
                    row["stands"] = list(tags)
        badges = data.get("badges")
        if isinstance(badges, list):
            for badge in badges:
                if isinstance(badge, dict):
                    badge["stands"] = list(tags)
    payload["version"] = PROJECT_JSON_VERSION_STANDS
    manifest = list(payload.get("standsManifest") or [])
    if stand not in manifest:
        manifest.append(stand)
    payload["standsManifest"] = _sort_stands(manifest)
    return payload


def contest_stands_union(data: Dict[str, Any]) -> List[str]:
    """Union меток по карточке конкурса и всем строкам."""
    found: Set[str] = set()
    contest = data.get("contest")
    if isinstance(contest, dict):
        found.update(contest.get("stands") or [])
    for key in ("group", "indicator", "schedule", "reward_link", "badges"):
        rows = data.get(key)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if isinstance(row, dict):
                found.update(row.get("stands") or [])
    return _sort_stands(found)


def _index_by_key(
    rows: Sequence[Dict[str, Any]], key_fn: RowKeyFn
) -> Tuple[Dict[Any, Dict[str, Any]], List[Any]]:
    mapping: Dict[Any, Dict[str, Any]] = {}
    order: List[Any] = []
    for row in rows:
        key = key_fn(row)
        if key not in mapping:
            order.append(key)
        mapping[key] = row
    return mapping, order


def merge_row_lists(
    prom_rows: Sequence[Dict[str, Any]],
    psi_rows: Sequence[Dict[str, Any]],
    key_fn: RowKeyFn,
    *,
    contest_code: str = "",
    table_name: str = "",
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Объединить строки двух стендов. При overlap — данные PROM, ``stands`` по наличию.
    Возвращает (строки, предупреждения о расхождениях PSI от PROM).
    """
    prom_map, prom_order = _index_by_key(list(prom_rows), key_fn)
    psi_map, psi_order = _index_by_key(list(psi_rows), key_fn)
    out: List[Dict[str, Any]] = []
    warnings: List[str] = []

    for key in prom_order:
        prom_row = prom_map[key]
        in_psi = key in psi_map
        row = _deepcopy_row(prom_row)
        row["stands"] = _sort_stands([STAND_PROM, STAND_PSI] if in_psi else [STAND_PROM])
        if in_psi and _rows_differ(prom_row, psi_map[key]):
            warnings.append(
                f"{contest_code}.{table_name}{key!r}: overlap — данные PROM, PSI отличается"
            )
        out.append(row)

    for key in psi_order:
        if key in prom_map:
            continue
        row = _deepcopy_row(psi_map[key])
        row["stands"] = [STAND_PSI]
        out.append(row)

    return out, warnings


def _badges_by_link(
    data: Dict[str, Any], contest_code: str
) -> Dict[Tuple[str, str, str], Dict[str, Any]]:
    links = data.get("reward_link") or []
    badges = data.get("badges") or []
    out: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for idx, link in enumerate(links):
        if not isinstance(link, dict):
            continue
        if idx >= len(badges) or not isinstance(badges[idx], dict):
            continue
        out[link_row_key(link, contest_code)] = badges[idx]
    return out


def merge_contest_data(
    prom_data: Optional[Dict[str, Any]],
    psi_data: Optional[Dict[str, Any]],
    *,
    contest_code: str,
) -> Tuple[Dict[str, Any], List[str]]:
    """Слить ``data`` одного конкурса. Приоритет полей PROM."""
    warnings: List[str] = []
    prom_data = prom_data or {}
    psi_data = psi_data or {}
    has_prom = bool(prom_data)
    has_psi = bool(psi_data)

    if has_prom:
        contest = _deepcopy_row(prom_data.get("contest") or {})
        feature = _deepcopy_row(prom_data.get("feature") or {})
        contest_period = copy.deepcopy(prom_data.get("contestPeriod") or [])
    else:
        contest = _deepcopy_row(psi_data.get("contest") or {})
        feature = _deepcopy_row(psi_data.get("feature") or {})
        contest_period = copy.deepcopy(psi_data.get("contestPeriod") or [])

    card_stands = _sort_stands(
        [STAND_PROM, STAND_PSI] if has_prom and has_psi else [STAND_PROM if has_prom else STAND_PSI]
    )
    if isinstance(contest, dict):
        contest["stands"] = card_stands
        if has_prom and has_psi:
            psi_contest = psi_data.get("contest") or {}
            if _rows_differ(contest, psi_contest):
                warnings.append(
                    f"{contest_code}.contest: overlap — данные PROM, PSI отличается"
                )

    merged: Dict[str, Any] = {
        "contest": contest,
        "feature": feature,
        "contestPeriod": contest_period,
    }

    for table_name, key_fn in (
        ("group", group_row_key),
        ("indicator", indicator_row_key),
        ("schedule", schedule_row_key),
    ):
        rows, w = merge_row_lists(
            prom_data.get(table_name) or [],
            psi_data.get(table_name) or [],
            key_fn,
            contest_code=contest_code,
            table_name=table_name + " ",
        )
        merged[table_name] = rows
        warnings.extend(w)

    merged_links, w_links = merge_row_lists(
        prom_data.get("reward_link") or [],
        psi_data.get("reward_link") or [],
        lambda row: link_row_key(row, contest_code),
        contest_code=contest_code,
        table_name="reward_link ",
    )
    merged["reward_link"] = merged_links
    warnings.extend(w_links)

    prom_badges = _badges_by_link(prom_data, contest_code)
    psi_badges = _badges_by_link(psi_data, contest_code)
    merged_badges: List[Dict[str, Any]] = []
    for link in merged_links:
        lk = link_row_key(link, contest_code)
        link_stands = link.get("stands") or []
        in_prom = STAND_PROM in link_stands
        in_psi = STAND_PSI in link_stands
        if in_prom and lk in prom_badges:
            badge = _deepcopy_row(prom_badges[lk])
        elif in_psi and lk in psi_badges:
            badge = _deepcopy_row(psi_badges[lk])
        elif lk in prom_badges:
            badge = _deepcopy_row(prom_badges[lk])
        elif lk in psi_badges:
            badge = _deepcopy_row(psi_badges[lk])
        else:
            badge = {"flat": {}, "add": {}}
            warnings.append(f"{contest_code}.badges{lk!r}: нет данных награды")
        badge["stands"] = _sort_stands(link_stands)
        if in_prom and in_psi and lk in prom_badges and lk in psi_badges:
            if _rows_differ(prom_badges[lk], psi_badges[lk]):
                warnings.append(
                    f"{contest_code}.badges{lk!r}: overlap — flat/add PROM, PSI отличается"
                )
        merged_badges.append(badge)
    merged["badges"] = merged_badges

    return merged, warnings


def _contest_index(payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for item in payload.get("contests") or []:
        data = item.get("data") or {}
        contest = data.get("contest") or {}
        code = str(contest.get("CONTEST_CODE") or "").strip()
        if code:
            out[code] = item
    return out


def merge_fill_projects(
    prom_payload: Dict[str, Any],
    psi_payload: Dict[str, Any],
    *,
    title: str = "Все конкурсы PROM+PSI (merge, приоритет PROM)",
) -> Tuple[Dict[str, Any], List[str]]:
    """Слить два снимка fill в один ``block: MERGED``."""
    prom_by = _contest_index(prom_payload)
    psi_by = _contest_index(psi_payload)
    all_codes: List[str] = []
    seen: Set[str] = set()
    for item in prom_payload.get("contests") or []:
        code = str(((item.get("data") or {}).get("contest") or {}).get("CONTEST_CODE") or "").strip()
        if code and code not in seen:
            seen.add(code)
            all_codes.append(code)
    for item in psi_payload.get("contests") or []:
        code = str(((item.get("data") or {}).get("contest") or {}).get("CONTEST_CODE") or "").strip()
        if code and code not in seen:
            seen.add(code)
            all_codes.append(code)

    contests: List[Dict[str, Any]] = []
    warnings: List[str] = []
    for code in all_codes:
        prom_item = prom_by.get(code)
        psi_item = psi_by.get(code)
        prom_data = (prom_item or {}).get("data") if prom_item else None
        psi_data = (psi_item or {}).get("data") if psi_item else None
        merged_data, w = merge_contest_data(
            prom_data if isinstance(prom_data, dict) else None,
            psi_data if isinstance(psi_data, dict) else None,
            contest_code=code,
        )
        warnings.extend(w)
        name = str((merged_data.get("contest") or {}).get("FULL_NAME") or code)
        source_item = prom_item or psi_item or {}
        contests.append(
            {
                "id": source_item.get("id") or ("ex_" + code.replace("-", "_").replace(".", "_")),
                "name": name,
                "stands": contest_stands_union(merged_data),
                "data": merged_data,
            }
        )

    merged: Dict[str, Any] = {
        "version": PROJECT_JSON_VERSION_STANDS,
        "block": "MERGED",
        "standsManifest": _sort_stands([STAND_PROM, STAND_PSI]),
        "title": title,
        "source": (
            f"merge · PROM: {prom_payload.get('source', '')} · PSI: {psi_payload.get('source', '')}"
        ),
        "saved_at": prom_payload.get("saved_at") or psi_payload.get("saved_at"),
        "catalog_stamp": prom_payload.get("catalog_stamp") or psi_payload.get("catalog_stamp"),
        "activeContest": 0,
        "contests": contests,
    }
    if warnings:
        logger.warning("Merge: %s предупреждений о расхождениях PSI↔PROM", len(warnings))
        for msg in warnings[:20]:
            logger.warning("%s", msg)
        if len(warnings) > 20:
            logger.warning("… ещё %s", len(warnings) - 20)
    return merged, warnings


def verify_prom_preserved(
    prom_payload: Dict[str, Any],
    merged_payload: Dict[str, Any],
) -> List[str]:
    """
    PROM должен полностью попасть в merged: каждый конкурс и каждая PROM-строка
    с теми же данными (без учёта stands).
    """
    errors: List[str] = []
    prom_by = _contest_index(prom_payload)
    merged_by = _contest_index(merged_payload)

    missing = sorted(set(prom_by) - set(merged_by))
    if missing:
        errors.append("merged: нет PROM-конкурсов: " + ", ".join(missing[:20]))

    for code in sorted(set(prom_by) & set(merged_by)):
        prom_data = prom_by[code].get("data") or {}
        merged_data = merged_by[code].get("data") or {}

        prom_card = _strip_stands(prom_data.get("contest") or {})
        merged_card = _strip_stands(merged_data.get("contest") or {})
        if prom_card != merged_card:
            errors.append(f"{code}.contest: карточка PROM изменена в merged")

        merged_prom_rows = {
            group_row_key(r): r
            for r in (merged_data.get("group") or [])
            if STAND_PROM in (r.get("stands") or [])
        }
        for row in prom_data.get("group") or []:
            key = group_row_key(row)
            if key not in merged_prom_rows:
                errors.append(f"{code}.group{key!r}: PROM-строка не найдена в merged")
            elif _strip_stands(row) != _strip_stands(merged_prom_rows[key]):
                errors.append(f"{code}.group{key!r}: данные PROM изменены в merged")

        merged_prom_ind = {
            indicator_row_key(r): r
            for r in (merged_data.get("indicator") or [])
            if STAND_PROM in (r.get("stands") or [])
        }
        for row in prom_data.get("indicator") or []:
            key = indicator_row_key(row)
            if key not in merged_prom_ind:
                errors.append(f"{code}.indicator{key!r}: PROM-строка не найдена в merged")
            elif _strip_stands(row) != _strip_stands(merged_prom_ind[key]):
                errors.append(f"{code}.indicator{key!r}: данные PROM изменены в merged")

        merged_prom_sch = {
            schedule_row_key(r): r
            for r in (merged_data.get("schedule") or [])
            if STAND_PROM in (r.get("stands") or [])
        }
        for row in prom_data.get("schedule") or []:
            key = schedule_row_key(row)
            if key not in merged_prom_sch:
                errors.append(f"{code}.schedule{key!r}: PROM-строка не найдена в merged")
            elif _strip_stands(row) != _strip_stands(merged_prom_sch[key]):
                errors.append(f"{code}.schedule{key!r}: данные PROM изменены в merged")

        prom_links = prom_data.get("reward_link") or []
        prom_badges = prom_data.get("badges") or []
        merged_links = merged_data.get("reward_link") or []
        merged_badges = merged_data.get("badges") or []
        merged_link_map = {
            link_row_key(r, code): (r, merged_badges[i] if i < len(merged_badges) else None)
            for i, r in enumerate(merged_links)
            if isinstance(r, dict)
        }
        for idx, link in enumerate(prom_links):
            if not isinstance(link, dict):
                continue
            lk = link_row_key(link, code)
            if lk not in merged_link_map:
                errors.append(f"{code}.reward_link{lk!r}: PROM-link не найден в merged")
                continue
            m_link, m_badge = merged_link_map[lk]
            if _strip_stands(link) != _strip_stands(m_link):
                errors.append(f"{code}.reward_link{lk!r}: данные PROM изменены в merged")
            if idx < len(prom_badges) and isinstance(m_badge, dict):
                if _strip_stands(prom_badges[idx]) != _strip_stands(m_badge):
                    errors.append(f"{code}.badges{lk!r}: данные PROM изменены в merged")

    return errors
