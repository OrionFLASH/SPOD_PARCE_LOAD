# -*- coding: utf-8 -*-
"""Собрать снимки web-fill из CSV PROM SPOD (файлы из CONFIG_RUN_INPUT.json).

Выход: common/examples/web-fill/{curated,badges,contests}/ — не каталоги edit/fill и не сохранения UI.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.config_loader import (  # noqa: E402
    default_config_path,
    load_config_dict,
    resolve_project_base_dir,
)
from src.contest_badge_form.csv_load import resolve_sheet_file  # noqa: E402
from src.contest_badge_form.spod_json import (  # noqa: E402
    form_cell_from_list,
    parse_spod_json,
)

logger = logging.getLogger(__name__)

EXAMPLES_ROOT = ROOT / "common" / "examples" / "web-fill"
DIR_CURATED = EXAMPLES_ROOT / "curated"
DIR_BADGES = EXAMPLES_ROOT / "badges"
DIR_CONTESTS = EXAMPLES_ROOT / "contests"
CATALOG = ROOT / "common" / "web-fill" / "catalog.json"
DEFAULT_BLOCK = "PROM"

# Секции каталога fill/edit → лист input_files в CONFIG_RUN_INPUT.json
CATALOG_SECTION_TO_SHEET: Dict[str, str] = {
    "CONTEST": "CONTEST-DATA",
    "CONTEST_FEATURE": "CONTEST-DATA",
    "CONTEST_PERIOD": "CONTEST-DATA",
    "REWARD": "REWARD",
    "REWARD_ADD_DATA": "REWARD",
    "TABLE:REWARD-LINK": "REWARD-LINK",
    "TABLE:GROUP": "GROUP",
    "TABLE:INDICATOR": "INDICATOR",
    "INDICATOR_FILTER": "INDICATOR",
    "TABLE:SCHEDULE": "TOURNAMENT-SCHEDULE",
    "SCHEDULE_TARGET_TYPE": "TOURNAMENT-SCHEDULE",
    "FILTER_PERIOD_ARR": "TOURNAMENT-SCHEDULE",
}
SHEET_TO_TABLE_KEY: Dict[str, str] = {
    "CONTEST-DATA": "contest",
    "GROUP": "group",
    "INDICATOR": "indicator",
    "REWARD": "reward",
    "REWARD-LINK": "reward_link",
    "TOURNAMENT-SCHEDULE": "schedule",
}

CONTEST_FLAT_KEYS = [
    "CONTEST_CODE",
    "FULL_NAME",
    "CREATE_DT",
    "CLOSE_DT",
    "BUSINESS_STATUS",
    "CONTEST_TYPE",
    "CONTEST_DESCRIPTION",
    "SHOW_INDICATOR",
    "PRODUCT_GROUP",
    "PRODUCT",
    "CONTEST_SUBJECT",
    "FACTOR_MARK_TYPE",
    "CONTEST_INDICATOR_METHOD",
    "CONTEST_FACTOR_METHOD",
    "PLAN_METHOD_CODE",
    "PLAN_MOD_METOD",
    "PLAN_MOD_VALUE",
    "FACTOR_MATCH",
    "TARGET_TYPE",
    "SOURCE_UPD_FREQUENCY",
    "CALC_TYPE",
    "BUSINESS_BLOCK",
    "FACT_POST_PROCESSING",
]
CONTEST_LIST_KEYS = {"BUSINESS_BLOCK"}
LINK_COLS = ["CONTEST_CODE", "GROUP_CODE", "REWARD_CODE"]
GROUP_COLS = [
    "CONTEST_CODE",
    "GROUP_CODE",
    "GROUP_VALUE",
    "GET_CALC_METHOD",
    "GET_CALC_CRITERION",
    "ADD_CALC_CRITERION",
    "ADD_CALC_CRITERION_2",
    "BASE_CALC_CODE",
]
IND_COLS = [
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
    "CONTESTANT_SELECTION",
    "CALC_TYPE",
    "N",
]
SCH_COLS = [
    "TOURNAMENT_CODE",
    "PERIOD_TYPE",
    "START_DT",
    "END_DT",
    "RESULT_DT",
    "PLAN_PERIOD_START_DT",
    "PLAN_PERIOD_END_DT",
    "CRITERION_MARK_TYPE",
    "CRITERION_MARK_VALUE",
    "TOURNAMENT_STATUS",
    "CONTEST_CODE",
    "CALC_TYPE",
    "TRN_INDICATOR_FILTER",
]
REWARD_FLAT_KEYS = [
    "REWARD_CODE",
    "REWARD_TYPE",
    "FULL_NAME",
    "REWARD_DESCRIPTION",
    "REWARD_CONDITION",
    "REWARD_COST",
]
FEATURE_LIST_LEAVES = {
    "persomanNumberVisible",
    "persomanNumberHidden",
    "tournamentListMailing",
    "feature",
    "businessBlock",
    "helpCodeList",
    "preferences",
    "tbVisible",
    "tbHidden",
    "gosbVisible",
    "gosbHidden",
}
ADD_LIST_LEAVES = {"preferences", "feature", "businessBlock", "helpCodeList"}


def _setup_logging() -> None:
    """Формат лога Tools: дата время - [LEVEL] - сообщение [def: имя]."""
    if logging.getLogger().handlers:
        return
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - [%(levelname)s] - %(message)s [def: %(funcName)s]",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def fill_sheets_from_catalog(catalog: Optional[Dict[str, Any]] = None) -> List[str]:
    """Листы SPOD, которые есть в каталоге fill (порядок первого появления)."""
    data = catalog
    if data is None and CATALOG.is_file():
        data = json.loads(CATALOG.read_text(encoding="utf-8"))
    sheets: List[str] = []
    seen: set[str] = set()
    sections = (data or {}).get("sections") or []
    for sec in sections:
        sid = str(sec.get("id") or "")
        sheet = CATALOG_SECTION_TO_SHEET.get(sid)
        if not sheet or sheet in seen:
            continue
        seen.add(sheet)
        sheets.append(sheet)
    if sheets:
        return sheets
    return list(SHEET_TO_TABLE_KEY.keys())


@dataclass
class SpodTables:
    """Таблицы PROM SPOD, нужные fill (листы каталога)."""

    contest: List[Dict[str, str]]
    group: List[Dict[str, str]]
    indicator: List[Dict[str, str]]
    reward: List[Dict[str, str]]
    reward_link: List[Dict[str, str]]
    schedule: List[Dict[str, str]]
    source_files: Dict[str, str] = field(default_factory=dict)
    block: str = DEFAULT_BLOCK

    def rows_for(self, table_key: str, contest_code: str) -> List[Dict[str, str]]:
        """Строки таблицы с данным CONTEST_CODE."""
        table = getattr(self, table_key)
        return [r for r in table if str(r.get("CONTEST_CODE") or "") == contest_code]


def load_prom_spod_tables(
    *,
    block: str = DEFAULT_BLOCK,
    catalog: Optional[Dict[str, Any]] = None,
) -> SpodTables:
    """
    Прочитать CSV PROM SPOD из input_files CONFIG_RUN_INPUT.json.
    Берутся только листы, которые есть в каталоге fill.
    """
    config_path = default_config_path(str(ROOT))
    cfg = load_config_dict(config_path)
    base_dir = resolve_project_base_dir(config_path)
    sheets = fill_sheets_from_catalog(catalog)
    loaded: Dict[str, List[Dict[str, str]]] = {}
    source_files: Dict[str, str] = {}
    for sheet in sheets:
        table_key = SHEET_TO_TABLE_KEY.get(sheet)
        if not table_key:
            logger.debug("Лист каталога %s не входит в снимок fill", sheet)
            continue
        path_s = resolve_sheet_file(base_dir, cfg, block, sheet)
        if not path_s or not os.path.isfile(path_s):
            raise FileNotFoundError(
                f"Нет CSV листа {sheet} блока {block} из CONFIG_RUN_INPUT.json: {path_s}"
            )
        path = Path(path_s)
        rows = _read_csv(path)
        loaded[table_key] = rows
        try:
            rel = str(path.relative_to(ROOT))
        except ValueError:
            rel = str(path)
        source_files[table_key] = rel
        logger.info("Загружен %s → %s (%s строк)", sheet, rel, len(rows))
    missing = [k for k in SHEET_TO_TABLE_KEY.values() if k not in loaded]
    if missing:
        raise FileNotFoundError(
            "В конфиге нет файлов каталога fill для таблиц: " + ", ".join(missing)
        )
    return SpodTables(
        contest=loaded["contest"],
        group=loaded["group"],
        indicator=loaded["indicator"],
        reward=loaded["reward"],
        reward_link=loaded["reward_link"],
        schedule=loaded["schedule"],
        source_files=source_files,
        block=block,
    )


def _read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as fh:
        return [
            {k: ("" if v is None else str(v)) for k, v in row.items()}
            for row in csv.DictReader(fh, delimiter=";")
        ]


def _cell_list(raw: str) -> str:
    parsed = parse_spod_json(raw) if raw and raw.strip().startswith("[") else None
    if isinstance(parsed, list):
        return form_cell_from_list(parsed)
    return (raw or "").strip()


def _leaf_value(val: Any, *, as_list: bool) -> str:
    if val is None:
        return ""
    if as_list:
        if isinstance(val, list):
            return form_cell_from_list(val)
        return str(val).strip()
    if isinstance(val, bool):
        return "Y" if val else "N"
    if isinstance(val, float) and val == int(val):
        return str(int(val))
    return str(val)


def _parse_array_cell(raw: Any) -> List[Any]:
    parsed = parse_spod_json(raw)
    return list(parsed) if isinstance(parsed, list) else []


def _scalar_str(val: Any, default: str = "") -> str:
    if val is None:
        return default
    if isinstance(val, bool):
        return "Y" if val else "N"
    if isinstance(val, float) and val == int(val):
        return str(int(val))
    return str(val).strip() if str(val).strip() else default


def _norm_contest_period_item(raw: Any) -> Dict[str, str]:
    it = raw if isinstance(raw, dict) else {}
    return {
        "period_code": _scalar_str(it.get("period_code"), "0"),
        "criterion_mark_type": _scalar_str(it.get("criterion_mark_type"), ">"),
        "criterion_mark_value": _scalar_str(it.get("criterion_mark_value"), "0"),
    }


def _norm_filter_period_item(raw: Any) -> Dict[str, str]:
    it = raw if isinstance(raw, dict) else {}
    cmv = it.get("criterion_mark_value")
    return {
        "period_code": _scalar_str(it.get("period_code"), "1"),
        "start_dt": _scalar_str(it.get("start_dt")),
        "end_dt": _scalar_str(it.get("end_dt")),
        "criterion_mark_type": _scalar_str(it.get("criterion_mark_type")),
        "criterion_mark_value": "" if cmv is None or str(cmv).strip() == "" else _scalar_str(cmv),
    }


def _norm_indicator_filter_item(raw: Any) -> Dict[str, str]:
    it = raw if isinstance(raw, dict) else {}
    cond = it.get("filtered_attribute_condition")
    if isinstance(cond, list):
        cond_s = form_cell_from_list(cond)
    else:
        cond_s = _scalar_str(cond)
    fav = it.get("filtered_attribute_value")
    return {
        "filtered_attribute_code": _scalar_str(it.get("filtered_attribute_code")),
        "filtered_attribute_type": _scalar_str(it.get("filtered_attribute_type"), "STRING"),
        "filtered_attribute_match": _scalar_str(it.get("filtered_attribute_match"), "IN"),
        "filtered_attribute_condition": cond_s,
        "filtered_attribute_value": "" if fav is None or str(fav).strip() == "" else _scalar_str(fav),
        "filtered_attribute_dt": _scalar_str(it.get("filtered_attribute_dt")),
    }


def _season_code_from_target(raw: Any) -> str:
    parsed = parse_spod_json(raw)
    if isinstance(parsed, dict):
        return _scalar_str(parsed.get("seasonCode"))
    s = _scalar_str(raw)
    if s.startswith("{") or "seasonCode" in s:
        return ""
    return s


def _row_subset(row: Dict[str, str], cols: Sequence[str]) -> Dict[str, str]:
    return {c: str(row.get(c, "") or "") for c in cols}


def follows_prefixed_principle(full: str, contest_code: str, kind: str) -> bool:
    """Код = r_/t_ + CONTEST_CODE или r_/t_ + CONTEST_CODE + _ + окончание."""
    s = (full or "").strip()
    cc = (contest_code or "").strip()
    if not s or not cc:
        return False
    prefix = ("r_" if kind == "reward" else "t_") + cc
    return s == prefix or s.startswith(prefix + "_")


def code_ending(full: str, contest_code: str, kind: str) -> str:
    """
    Полный код SPOD → окончание для поля fill.
    Снять r_/t_, снять CONTEST_CODE, остаток без ведущих _.
    Если полного хвоста нет (r_CODE / t_CODE) — пустая строка.
    """
    s = (full or "").strip()
    cc = (contest_code or "").strip()
    if kind == "reward" and s.startswith("r_"):
        s = s[2:]
    elif kind == "tournament" and s.startswith("t_"):
        s = s[2:]
    if cc and s.startswith(cc):
        s = s[len(cc) :]
    return s.lstrip("_")


def compose_from_ending(contest_code: str, ending: str, kind: str) -> str:
    """
    Окончание → полный код как в fill/SPOD.
    Пустое окончание → r_CODE / t_CODE (без хвостового _).
    Непустое → r_CODE_ending / t_CODE_ending («_» только перед окончанием).
    """
    cc = (contest_code or "").strip()
    e = (ending or "").lstrip("_")
    prefix = "r_" if kind == "reward" else "t_"
    if not cc:
        return e
    if not e:
        return f"{prefix}{cc}"
    return f"{prefix}{cc}_{e}"


def build_contest_data(
    contest_row: Dict[str, str],
    *,
    groups: List[Dict[str, str]],
    links: List[Dict[str, str]],
    rewards_by_code: Dict[str, Dict[str, str]],
    indicators: List[Dict[str, str]],
    schedules: List[Dict[str, str]],
) -> Dict[str, Any]:
    contest: Dict[str, str] = {}
    for key in CONTEST_FLAT_KEYS:
        raw = contest_row.get(key, "")
        if key in CONTEST_LIST_KEYS:
            contest[key] = _cell_list(raw)
        else:
            contest[key] = str(raw or "")

    cc = contest.get("CONTEST_CODE", "")

    feature_obj = parse_spod_json(contest_row.get("CONTEST_FEATURE", ""))
    if not isinstance(feature_obj, dict):
        feature_obj = {}
    feature: Dict[str, str] = {}
    for leaf, val in feature_obj.items():
        feature[str(leaf)] = _leaf_value(val, as_list=str(leaf) in FEATURE_LIST_LEAVES)

    contest_period = [
        _norm_contest_period_item(x) for x in _parse_array_cell(contest_row.get("CONTEST_PERIOD", ""))
    ]

    badges: List[Dict[str, Any]] = []
    reward_link: List[Dict[str, str]] = []
    for link in links:
        rc_full = str(link.get("REWARD_CODE", "") or "").strip()
        stored_rc = (
            code_ending(rc_full, cc, "reward")
            if follows_prefixed_principle(rc_full, cc, "reward")
            else rc_full
        )
        link_row = _row_subset(link, LINK_COLS)
        link_row["CONTEST_CODE"] = cc
        link_row["REWARD_CODE"] = stored_rc
        reward_link.append(link_row)

        brow = rewards_by_code.get(rc_full) or {}
        flat = {k: str(brow.get(k, "") or "") for k in REWARD_FLAT_KEYS}
        if not flat.get("REWARD_TYPE"):
            flat["REWARD_TYPE"] = "BADGE"
        flat["REWARD_CODE"] = stored_rc
        add_obj = parse_spod_json(brow.get("REWARD_ADD_DATA", ""))
        if not isinstance(add_obj, dict):
            add_obj = {}
        add: Dict[str, str] = {}
        for leaf, val in add_obj.items():
            v = _leaf_value(val, as_list=str(leaf) in ADD_LIST_LEAVES)
            if str(leaf) == "parentRewardCode" and v:
                if follows_prefixed_principle(v, cc, "reward"):
                    v = code_ending(v, cc, "reward")
            add[str(leaf)] = v
        badges.append({"flat": flat, "add": add})

    # Нет строк в CSV — в снимке пустой массив, без заглушек.
    group_rows = [_row_subset(r, GROUP_COLS) for r in groups]
    for g in group_rows:
        g["CONTEST_CODE"] = cc

    ind_rows: List[Dict[str, Any]] = []
    for r in indicators:
        row: Dict[str, Any] = _row_subset(r, IND_COLS)
        row["CONTEST_CODE"] = cc
        row["filter_items"] = [
            _norm_indicator_filter_item(x)
            for x in _parse_array_cell(r.get("INDICATOR_FILTER", ""))
        ]
        ind_rows.append(row)

    sch_rows: List[Dict[str, Any]] = []
    for r in schedules:
        row = _row_subset(r, SCH_COLS)
        row["CONTEST_CODE"] = cc
        tc_full = str(row.get("TOURNAMENT_CODE", "") or "").strip()
        row["TOURNAMENT_CODE"] = (
            code_ending(tc_full, cc, "tournament")
            if follows_prefixed_principle(tc_full, cc, "tournament")
            else tc_full
        )
        row["seasonCode"] = _season_code_from_target(r.get("TARGET_TYPE", ""))
        row["filter_period"] = [
            _norm_filter_period_item(x)
            for x in _parse_array_cell(r.get("FILTER_PERIOD_ARR", ""))
        ]
        sch_rows.append(row)

    return {
        "contest": contest,
        "feature": feature,
        "contestPeriod": contest_period,
        "badges": badges,
        "reward_link": reward_link,
        "group": group_rows,
        "indicator": ind_rows,
        "schedule": sch_rows,
    }


def collect_all_contest_codes(tables: SpodTables) -> List[str]:
    """Все CONTEST_CODE из CONTEST-DATA (порядок CSV, без пустых)."""
    codes: List[str] = []
    seen: set[str] = set()
    for row in tables.contest:
        code = str(row.get("CONTEST_CODE") or "").strip()
        if not code or code in seen:
            continue
        seen.add(code)
        codes.append(code)
    return codes


def collect_badge_contest_codes(tables: SpodTables) -> List[str]:
    """Все CONTEST_CODE, у которых в REWARD-LINK есть награда с REWARD_TYPE=BADGE."""
    badge_codes = {
        str(r.get("REWARD_CODE") or "").strip()
        for r in tables.reward
        if str(r.get("REWARD_TYPE") or "").strip().upper() == "BADGE"
        and str(r.get("REWARD_CODE") or "").strip()
    }
    codes = sorted(
        {
            str(r.get("CONTEST_CODE") or "").strip()
            for r in tables.reward_link
            if str(r.get("CONTEST_CODE") or "").strip()
            and str(r.get("REWARD_CODE") or "").strip() in badge_codes
        }
    )
    return codes


def collect_badge_contest_codes_with_schedule_start(
    tables: SpodTables, needle: str = "2026"
) -> List[str]:
    """
    Конкурсы с BADGE и хотя бы одним периодом SCHEDULE,
    у которого START_DT содержит needle (например «2026»).
    """
    badge = set(collect_badge_contest_codes(tables))
    with_start = {
        str(r.get("CONTEST_CODE") or "").strip()
        for r in tables.schedule
        if str(r.get("CONTEST_CODE") or "").strip()
        and needle in str(r.get("START_DT") or "")
    }
    return sorted(badge & with_start)


def _badge_reward_codes(tables: SpodTables) -> set[str]:
    return {
        str(r.get("REWARD_CODE") or "").strip()
        for r in tables.reward
        if str(r.get("REWARD_TYPE") or "").strip().upper() == "BADGE"
        and str(r.get("REWARD_CODE") or "").strip()
    }


def expected_row_counts(
    tables: SpodTables,
    contest_code: str,
    *,
    badge_only: bool = False,
) -> Dict[str, int]:
    """Ожидаемые длины массивов снимка для CONTEST_CODE."""
    links = tables.rows_for("reward_link", contest_code)
    if badge_only:
        badge_codes = _badge_reward_codes(tables)
        links = [
            r
            for r in links
            if str(r.get("REWARD_CODE") or "").strip() in badge_codes
        ]
    return {
        "group": len(tables.rows_for("group", contest_code)),
        "indicator": len(tables.rows_for("indicator", contest_code)),
        "schedule": len(tables.rows_for("schedule", contest_code)),
        "reward_link": len(links),
        "badges": len(links),
    }


def reconcile_snapshot_with_csv(
    payload: Dict[str, Any],
    tables: SpodTables,
    *,
    badge_only: bool = False,
    expected_codes: Optional[Sequence[str]] = None,
) -> List[str]:
    """
    Сверка JSON-снимка с CSV по каждому CONTEST_CODE.
    Возвращает список расхождений (пустой = совпало).
    """
    errors: List[str] = []
    contests = payload.get("contests") or []
    by_code: Dict[str, Dict[str, Any]] = {}
    for item in contests:
        data = item.get("data") or {}
        contest = data.get("contest") or {}
        code = str(contest.get("CONTEST_CODE") or "").strip()
        if not code:
            errors.append("снимок: конкурс без CONTEST_CODE")
            continue
        if code in by_code:
            errors.append(f"{code}: дубль в снимке")
        by_code[code] = data

    want = list(expected_codes) if expected_codes is not None else collect_all_contest_codes(tables)
    want_set = set(want)
    got_set = set(by_code)
    missing = sorted(want_set - got_set)
    extra = sorted(got_set - want_set)
    if missing:
        errors.append("нет в JSON: " + ", ".join(missing[:20]) + (f" …+{len(missing)-20}" if len(missing) > 20 else ""))
    if extra:
        errors.append("лишние в JSON: " + ", ".join(extra[:20]) + (f" …+{len(extra)-20}" if len(extra) > 20 else ""))

    for code in sorted(want_set & got_set):
        data = by_code[code]
        exp = expected_row_counts(tables, code, badge_only=badge_only)
        for key in ("group", "indicator", "schedule", "reward_link", "badges"):
            arr = data.get(key)
            if not isinstance(arr, list):
                errors.append(f"{code}.{key}: ожидался массив, получено {type(arr).__name__}")
                continue
            got = len(arr)
            if got != exp[key]:
                errors.append(f"{code}.{key}: JSON={got} CSV={exp[key]}")
        csv_name = ""
        for row in tables.contest:
            if str(row.get("CONTEST_CODE") or "") == code:
                csv_name = str(row.get("FULL_NAME") or "")
                break
        json_name = str((data.get("contest") or {}).get("FULL_NAME") or "")
        if csv_name and json_name and csv_name != json_name:
            errors.append(f"{code}.FULL_NAME: JSON={json_name!r} CSV={csv_name!r}")
    return errors


def build_project(
    codes: Sequence[str],
    *,
    title: str,
    tables: SpodTables,
    badge_only: bool = False,
) -> Dict[str, Any]:
    contest_rows = {r["CONTEST_CODE"]: r for r in tables.contest if r.get("CONTEST_CODE")}
    group_all = tables.group
    link_all = tables.reward_link
    reward_all = tables.reward
    ind_all = tables.indicator
    sch_all = tables.schedule
    rewards_by_code = {r["REWARD_CODE"]: r for r in reward_all if r.get("REWARD_CODE")}

    catalog_stamp = ""
    if CATALOG.is_file():
        cat = json.loads(CATALOG.read_text(encoding="utf-8"))
        catalog_stamp = str(cat.get("exported_at") or cat.get("generated_at") or "")

    contests: List[Dict[str, Any]] = []
    missing: List[str] = []
    for code in codes:
        crow = contest_rows.get(code)
        if not crow:
            missing.append(code)
            continue
        links = [r for r in link_all if r.get("CONTEST_CODE") == code]
        if badge_only:
            links = [
                r
                for r in links
                if str(
                    (rewards_by_code.get(str(r.get("REWARD_CODE") or "").strip()) or {}).get(
                        "REWARD_TYPE", ""
                    )
                    or ""
                )
                .strip()
                .upper()
                == "BADGE"
            ]
        data = build_contest_data(
            crow,
            groups=[r for r in group_all if r.get("CONTEST_CODE") == code],
            links=links,
            rewards_by_code=rewards_by_code,
            indicators=[r for r in ind_all if r.get("CONTEST_CODE") == code],
            schedules=[r for r in sch_all if r.get("CONTEST_CODE") == code],
        )
        name = str(crow.get("FULL_NAME") or code)
        contests.append(
            {
                "id": "ex_" + code.replace("-", "_").replace(".", "_"),
                "name": name,
                "data": data,
            }
        )

    if missing:
        raise SystemExit("Нет конкурсов в CONTEST: " + ", ".join(missing))

    files_note = "; ".join(
        f"{key}={name}" for key, name in tables.source_files.items()
    )
    source = (
        f"config/CONFIG_RUN_INPUT.json · {tables.block} · листы каталога fill"
        + (f" · {files_note}" if files_note else "")
    )
    if badge_only:
        source += " · только конкурсы со связью REWARD_TYPE=BADGE"

    return {
        "version": 2,
        "block": tables.block,
        "title": title,
        "source": source,
        "saved_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "catalog_stamp": catalog_stamp,
        "activeContest": 0,
        "contests": contests,
    }


def _write_snapshot(
    path: Path,
    payload: Dict[str, Any],
    tables: SpodTables,
    *,
    badge_only: bool,
    expected_codes: Sequence[str],
) -> None:
    """Записать JSON и сверить длины массивов с CSV."""
    mismatches = reconcile_snapshot_with_csv(
        payload,
        tables,
        badge_only=badge_only,
        expected_codes=expected_codes,
    )
    if mismatches:
        for msg in mismatches:
            logger.error("%s", msg)
        raise SystemExit(
            f"Сверка JSON=CSV не прошла для {path.name}: {len(mismatches)} расхожд."
        )
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    logger.info(
        "OK %s · %s конкурс.",
        path.relative_to(ROOT),
        len(payload["contests"]),
    )
    print(f"OK {path.relative_to(ROOT)} · {len(payload['contests'])} конкурс.")


def main() -> int:
    _setup_logging()
    for folder in (DIR_CURATED, DIR_BADGES, DIR_CONTESTS):
        folder.mkdir(parents=True, exist_ok=True)
    tables = load_prom_spod_tables(block=DEFAULT_BLOCK)
    packs: List[Tuple[Path, str, List[str], bool]] = [
        (
            DIR_CURATED / "spod_fill_example_rewards.json",
            "Примеры наград (индивидуальные накопительные)",
            [
                "09_2026-0_23-1_2",
                "09_2026-0_23-1_3",
                "09_2026-0_23-1_4",
                "09_2026-0_23-1_5",
            ],
            False,
        ),
        (
            DIR_CURATED / "spod_fill_example_tournaments.json",
            "Примеры турниров",
            [
                "01_2026-1_05-3_1",
                "10_2026-0_05-3_1",
                "01_2026-0_05-2_4",
                "01_2026-1_14-1_1",
            ],
            False,
        ),
        (
            DIR_CURATED / "spod_fill_example_mixed.json",
            "Смешанный пример: награды + турниры",
            [
                "09_2026-0_23-1_2",
                "09_2026-0_23-1_3",
                "09_2026-0_23-1_4",
                "09_2026-0_23-1_5",
                "01_2026-1_05-3_1",
                "10_2026-0_05-3_1",
                "01_2026-0_05-2_4",
                "01_2026-1_14-1_1",
            ],
            False,
        ),
        (
            DIR_CURATED / "spod_fill_example_json_arrays.json",
            "Примеры JSON-массивов: CONTEST_PERIOD / FILTER_PERIOD_ARR / INDICATOR_FILTER",
            [
                "01_2026-1_14-1_1",  # CONTEST_PERIOD: 2 периода
                "01_2025-0_13-1_1",  # INDICATOR_FILTER: несколько фильтров
                "01_2025-1_02-2_2",  # INDICATOR_FILTER + FILTER_PERIOD_ARR + CONTEST_PERIOD
                "01_2026-1_09-1_1",  # FILTER_PERIOD_ARR в нескольких турнирах
            ],
            False,
        ),
    ]
    for out_path, title, codes, badge_only in packs:
        payload = build_project(
            codes, title=title, tables=tables, badge_only=badge_only
        )
        _write_snapshot(
            out_path,
            payload,
            tables,
            badge_only=badge_only,
            expected_codes=codes,
        )

    badge_codes = collect_badge_contest_codes(tables)
    all_payload = build_project(
        badge_codes,
        title="Все конкурсы PROM-SPOD с наградами типа BADGE",
        tables=tables,
        badge_only=True,
    )
    _write_snapshot(
        DIR_BADGES / "spod_fill_all_badges.json",
        all_payload,
        tables,
        badge_only=True,
        expected_codes=badge_codes,
    )
    print(f"  (REWARD_TYPE=BADGE)")

    y2026_codes = collect_badge_contest_codes_with_schedule_start(tables, "2026")
    y2026_payload = build_project(
        y2026_codes,
        title="BADGE · SCHEDULE START_DT содержит 2026",
        tables=tables,
        badge_only=True,
    )
    _write_snapshot(
        DIR_BADGES / "spod_fill_badges_schedule_2026.json",
        y2026_payload,
        tables,
        badge_only=True,
        expected_codes=y2026_codes,
    )
    print("  (BADGE + START_DT∋2026)")

    all_codes = collect_all_contest_codes(tables)
    full_payload = build_project(
        all_codes,
        title="Все конкурсы PROM SPOD (листы каталога fill из CONFIG_RUN_INPUT.json)",
        tables=tables,
        badge_only=False,
    )
    _write_snapshot(
        DIR_CONTESTS / "spod_fill_all_contests.json",
        full_payload,
        tables,
        badge_only=False,
        expected_codes=all_codes,
    )
    print(f"  (все CONTEST_CODE, {len(all_codes)} шт.)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
