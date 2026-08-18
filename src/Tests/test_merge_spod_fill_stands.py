# -*- coding: utf-8 -*-
"""Тесты merge PROM+PSI для web-fill."""

from __future__ import annotations

from typing import Any, Dict

from src.Tools.merge_spod_fill_stands import (
    STAND_PROM,
    STAND_PSI,
    annotate_stand_tags,
    merge_contest_data,
    merge_fill_projects,
    verify_prom_preserved,
)


def _contest_payload(code: str, *, groups: int = 0, schedules: int = 0) -> Dict[str, Any]:
    data: Dict[str, Any] = {
        "contest": {"CONTEST_CODE": code, "FULL_NAME": f"Name {code}"},
        "feature": {},
        "contestPeriod": [],
        "group": [{"CONTEST_CODE": code, "GROUP_CODE": f"g{i}"} for i in range(groups)],
        "indicator": [],
        "schedule": [
            {"CONTEST_CODE": code, "TOURNAMENT_CODE": f"t{i}"} for i in range(schedules)
        ],
        "reward_link": [],
        "badges": [],
    }
    return {
        "version": 2,
        "block": "PROM",
        "contests": [{"id": f"ex_{code}", "name": f"Name {code}", "data": data}],
    }


def test_annotate_stand_tags() -> None:
    payload = _contest_payload("C1", groups=1)
    annotate_stand_tags(payload, STAND_PROM)
    item = payload["contests"][0]
    assert item["stands"] == [STAND_PROM]
    assert item["data"]["contest"]["stands"] == [STAND_PROM]
    assert item["data"]["group"][0]["stands"] == [STAND_PROM]
    assert payload["version"] == 5


def test_merge_mixed_schedules() -> None:
    prom = _contest_payload("C1", schedules=2)
    psi = _contest_payload("C1", schedules=3)
    psi["contests"][0]["data"]["schedule"] = [
        {"CONTEST_CODE": "C1", "TOURNAMENT_CODE": "t0"},
        {"CONTEST_CODE": "C1", "TOURNAMENT_CODE": "t1"},
        {"CONTEST_CODE": "C1", "TOURNAMENT_CODE": "t9"},
    ]
    annotate_stand_tags(prom, STAND_PROM)
    annotate_stand_tags(psi, STAND_PSI)
    merged, _ = merge_fill_projects(prom, psi)
    data = merged["contests"][0]["data"]
    codes = [r["TOURNAMENT_CODE"] for r in data["schedule"]]
    assert codes == ["t0", "t1", "t9"]
    stands_by = {r["TOURNAMENT_CODE"]: r["stands"] for r in data["schedule"]}
    assert stands_by["t0"] == [STAND_PROM, STAND_PSI]
    assert stands_by["t1"] == [STAND_PROM, STAND_PSI]
    assert stands_by["t9"] == [STAND_PSI]
    assert verify_prom_preserved(prom, merged) == []


def test_merge_psi_only_contest() -> None:
    prom = _contest_payload("C1")
    psi = _contest_payload("PSI_ONLY")
    annotate_stand_tags(prom, STAND_PROM)
    annotate_stand_tags(psi, STAND_PSI)
    merged, _ = merge_fill_projects(prom, psi)
    codes = [
        c["data"]["contest"]["CONTEST_CODE"] for c in merged["contests"]
    ]
    assert codes == ["C1", "PSI_ONLY"]
    psi_item = merged["contests"][1]
    assert psi_item["stands"] == [STAND_PSI]
    assert verify_prom_preserved(prom, merged) == []


def test_merge_contest_card_prom_priority() -> None:
    prom_data = {
        "contest": {"CONTEST_CODE": "X", "FULL_NAME": "PROM name"},
        "feature": {"vid": "ПРОМ"},
        "contestPeriod": [],
        "group": [],
        "indicator": [],
        "schedule": [],
        "reward_link": [],
        "badges": [],
    }
    psi_data = {
        "contest": {"CONTEST_CODE": "X", "FULL_NAME": "PSI name"},
        "feature": {"vid": "ТЕСТ"},
        "contestPeriod": [],
        "group": [],
        "indicator": [],
        "schedule": [],
        "reward_link": [],
        "badges": [],
    }
    merged, warnings = merge_contest_data(prom_data, psi_data, contest_code="X")
    assert merged["contest"]["FULL_NAME"] == "PROM name"
    assert merged["contest"]["stands"] == [STAND_PROM, STAND_PSI]
    assert any("contest" in w for w in warnings)
