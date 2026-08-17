# -*- coding: utf-8 -*-
"""Собрать примеры снимков web-fill из CSV IN/PROM/SPOD по кодам конкурсов."""

from __future__ import annotations

import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.contest_badge_form.spod_json import (  # noqa: E402
    form_cell_from_list,
    parse_spod_json,
)

SPOD_DIR = ROOT / "IN" / "PROM" / "SPOD"
OUT_DIR = ROOT / "common" / "web-fill" / "examples"
CATALOG = ROOT / "common" / "web-fill" / "catalog.json"

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


def _latest(pattern: str) -> Path:
    files = sorted(SPOD_DIR.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError(f"Нет файлов {pattern} в {SPOD_DIR}")
    return files[0]


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
        ending = code_ending(rc_full, cc, "reward")
        # в снимке fill храним окончание (поле «Окончание REWARD_CODE»)
        link_row = _row_subset(link, LINK_COLS)
        link_row["CONTEST_CODE"] = cc
        link_row["REWARD_CODE"] = ending
        reward_link.append(link_row)

        brow = rewards_by_code.get(rc_full) or {}
        flat = {k: str(brow.get(k, "") or "") for k in REWARD_FLAT_KEYS}
        if not flat.get("REWARD_TYPE"):
            flat["REWARD_TYPE"] = "BADGE"
        flat["REWARD_CODE"] = ending
        add_obj = parse_spod_json(brow.get("REWARD_ADD_DATA", ""))
        if not isinstance(add_obj, dict):
            add_obj = {}
        add: Dict[str, str] = {}
        for leaf, val in add_obj.items():
            v = _leaf_value(val, as_list=str(leaf) in ADD_LIST_LEAVES)
            if str(leaf) == "parentRewardCode" and v:
                # тоже только окончание, если это код награды этого конкурса
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
        row["TOURNAMENT_CODE"] = code_ending(tc_full, cc, "tournament")
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


def collect_badge_contest_codes() -> List[str]:
    """Все CONTEST_CODE из SPOD, у которых в REWARD-LINK есть награда с REWARD_TYPE=BADGE."""
    link_all = _read_csv(_latest("REWARD-LINK (PROM)*.csv"))
    reward_all = _read_csv(_latest("REWARD (PROM)*.csv"))
    badge_codes = {
        str(r.get("REWARD_CODE") or "").strip()
        for r in reward_all
        if str(r.get("REWARD_TYPE") or "").strip().upper() == "BADGE"
        and str(r.get("REWARD_CODE") or "").strip()
    }
    codes = sorted(
        {
            str(r.get("CONTEST_CODE") or "").strip()
            for r in link_all
            if str(r.get("CONTEST_CODE") or "").strip()
            and str(r.get("REWARD_CODE") or "").strip() in badge_codes
        }
    )
    return codes


def collect_badge_contest_codes_with_schedule_start(needle: str = "2026") -> List[str]:
    """
    Конкурсы с BADGE и хотя бы одним периодом SCHEDULE,
    у которого START_DT содержит needle (например «2026»).
    """
    badge = set(collect_badge_contest_codes())
    sch_all = _read_csv(_latest("SCHEDULE (PROM)*.csv"))
    with_start = {
        str(r.get("CONTEST_CODE") or "").strip()
        for r in sch_all
        if str(r.get("CONTEST_CODE") or "").strip()
        and needle in str(r.get("START_DT") or "")
    }
    return sorted(badge & with_start)

def build_project(
    codes: Sequence[str],
    *,
    title: str,
    badge_only: bool = False,
) -> Dict[str, Any]:
    contest_rows = {r["CONTEST_CODE"]: r for r in _read_csv(_latest("CONTEST (PROM)*.csv"))}
    group_all = _read_csv(_latest("GROUP (PROM)*.csv"))
    link_all = _read_csv(_latest("REWARD-LINK (PROM)*.csv"))
    reward_all = _read_csv(_latest("REWARD (PROM)*.csv"))
    ind_all = _read_csv(_latest("INDICATOR (PROM)*.csv"))
    sch_all = _read_csv(_latest("SCHEDULE (PROM)*.csv"))
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

    source = "IN/PROM/SPOD (latest CONTEST/GROUP/REWARD*/INDICATOR/SCHEDULE)"
    if badge_only:
        source += " · только конкурсы со связью REWARD_TYPE=BADGE"

    return {
        "version": 2,
        "block": "PROM",
        "title": title,
        "source": source,
        "saved_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "catalog_stamp": catalog_stamp,
        "activeContest": 0,
        "contests": contests,
    }


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    packs = [
        (
            "spod_fill_example_rewards.json",
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
            "spod_fill_example_tournaments.json",
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
            "spod_fill_example_mixed.json",
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
            "spod_fill_example_json_arrays.json",
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
    for filename, title, codes, badge_only in packs:
        payload = build_project(codes, title=title, badge_only=badge_only)
        path = OUT_DIR / filename
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        print(f"OK {path.relative_to(ROOT)} · {len(payload['contests'])} конкурс.")

    # Полный снимок: все конкурсы PROM-SPOD со связью на BADGE
    badge_codes = collect_badge_contest_codes()
    all_payload = build_project(
        badge_codes,
        title="Все конкурсы PROM-SPOD с наградами типа BADGE",
        badge_only=True,
    )
    all_path = OUT_DIR / "spod_fill_all_badges.json"
    all_path.write_text(
        json.dumps(all_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(
        f"OK {all_path.relative_to(ROOT)} · {len(all_payload['contests'])} конкурс. "
        f"(REWARD_TYPE=BADGE)"
    )

    # BADGE + в SCHEDULE есть турнир с START_DT, содержащим 2026
    y2026_codes = collect_badge_contest_codes_with_schedule_start("2026")
    y2026_payload = build_project(
        y2026_codes,
        title="BADGE · SCHEDULE START_DT содержит 2026",
        badge_only=True,
    )
    y2026_path = OUT_DIR / "spod_fill_badges_schedule_2026.json"
    y2026_path.write_text(
        json.dumps(y2026_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(
        f"OK {y2026_path.relative_to(ROOT)} · {len(y2026_payload['contests'])} конкурс. "
        f"(BADGE + START_DT∋2026)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
