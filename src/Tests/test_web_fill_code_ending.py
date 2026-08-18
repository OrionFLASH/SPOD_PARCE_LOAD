"""Правило окончаний r_/t_ + CONTEST_CODE (+ _ + ending только если ending непустой)."""

from __future__ import annotations

from src.Tools.export_web_fill_examples_from_spod import (
    build_contest_data,
    code_ending,
    compose_from_ending,
)


def test_reward_without_ending() -> None:
    cc = "09_2026-0_23-1_2"
    full = f"r_{cc}"
    assert code_ending(full, cc, "reward") == ""
    assert compose_from_ending(cc, "", "reward") == full
    assert not compose_from_ending(cc, "", "reward").endswith("_")


def test_reward_with_ending() -> None:
    cc = "01_2026-1_05-3_1"
    assert code_ending(f"r_{cc}_1", cc, "reward") == "1"
    assert compose_from_ending(cc, "1", "reward") == f"r_{cc}_1"


def test_tournament_with_ending() -> None:
    cc = "09_2026-0_23-1_2"
    assert code_ending(f"t_{cc}_4001", cc, "tournament") == "4001"
    assert compose_from_ending(cc, "4001", "tournament") == f"t_{cc}_4001"


def test_tournament_without_ending() -> None:
    cc = "10_2026-0_05-3_1"
    full = f"t_{cc}"
    assert code_ending(full, cc, "tournament") == ""
    assert compose_from_ending(cc, "", "tournament") == full


def test_build_contest_data_no_stub_rows() -> None:
    """Нет строк CSV → пустые массивы, без фантомного t_CODE / пустого индикатора."""
    contest_row = {
        "CONTEST_CODE": "CONTEST_00",
        "FULL_NAME": "Приветственный",
        "CONTEST_FEATURE": "",
        "CONTEST_PERIOD": "",
        "BUSINESS_BLOCK": "",
    }
    data = build_contest_data(
        contest_row,
        groups=[],
        links=[],
        rewards_by_code={},
        indicators=[],
        schedules=[],
    )
    assert data["schedule"] == []
    assert data["indicator"] == []
    assert data["group"] == []
    assert data["badges"] == []
    assert data["reward_link"] == []


def test_nonstandard_tournament_code_kept_full() -> None:
    """Старый код без t_+CONTEST_CODE в снимке хранится целиком."""
    contest_row = {
        "CONTEST_CODE": "C1",
        "FULL_NAME": "X",
        "CONTEST_FEATURE": "",
        "CONTEST_PERIOD": "",
        "BUSINESS_BLOCK": "",
    }
    schedules = [
        {
            "TOURNAMENT_CODE": "LEGACY_C1_OLD",
            "PERIOD_TYPE": "произвольный",
            "START_DT": "2026-01-01",
            "END_DT": "2026-01-31",
            "RESULT_DT": "",
            "PLAN_PERIOD_START_DT": "",
            "PLAN_PERIOD_END_DT": "",
            "CRITERION_MARK_TYPE": ">=",
            "CRITERION_MARK_VALUE": "0",
            "TOURNAMENT_STATUS": "АКТИВНЫЙ",
            "CONTEST_CODE": "C1",
            "CALC_TYPE": "1",
            "TRN_INDICATOR_FILTER": "",
            "TARGET_TYPE": "",
            "FILTER_PERIOD_ARR": "",
        }
    ]
    data = build_contest_data(
        contest_row,
        groups=[],
        links=[],
        rewards_by_code={},
        indicators=[],
        schedules=schedules,
    )
    assert data["schedule"][0]["TOURNAMENT_CODE"] == "LEGACY_C1_OLD"
    assert data["schedule"][0]["TOURNAMENT_CODE_ENDING"] == ""


def test_nonstandard_reward_code_kept_full() -> None:
    contest_row = {
        "CONTEST_CODE": "C1",
        "FULL_NAME": "X",
        "CONTEST_FEATURE": "",
        "CONTEST_PERIOD": "",
        "BUSINESS_BLOCK": "",
    }
    links = [{"CONTEST_CODE": "C1", "GROUP_CODE": "G1", "REWARD_CODE": "ITEM_99"}]
    rewards_by_code = {
        "ITEM_99": {
            "REWARD_CODE": "ITEM_99",
            "REWARD_TYPE": "ITEM",
            "FULL_NAME": "Товар",
            "REWARD_DESCRIPTION": "",
            "REWARD_CONDITION": "",
            "REWARD_COST": "5",
            "REWARD_ADD_DATA": "",
        }
    }
    data = build_contest_data(
        contest_row,
        groups=[],
        links=links,
        rewards_by_code=rewards_by_code,
        indicators=[],
        schedules=[],
    )
    assert data["reward_link"][0]["REWARD_CODE"] == "ITEM_99"
    assert data["reward_link"][0]["REWARD_CODE_ENDING"] == "99"
    assert data["badges"][0]["flat"]["REWARD_CODE"] == "ITEM_99"
    assert data["badges"][0]["flat"]["REWARD_CODE_ENDING"] == "99"


def test_build_contest_data_keeps_real_schedule() -> None:
    contest_row = {"CONTEST_CODE": "C1", "FULL_NAME": "X", "CONTEST_FEATURE": "", "CONTEST_PERIOD": "", "BUSINESS_BLOCK": ""}
    schedules = [
        {
            "TOURNAMENT_CODE": "t_C1_4001",
            "PERIOD_TYPE": "произвольный",
            "START_DT": "2026-01-01",
            "END_DT": "2026-01-31",
            "RESULT_DT": "",
            "PLAN_PERIOD_START_DT": "",
            "PLAN_PERIOD_END_DT": "",
            "CRITERION_MARK_TYPE": ">=",
            "CRITERION_MARK_VALUE": "0",
            "TOURNAMENT_STATUS": "АКТИВНЫЙ",
            "CONTEST_CODE": "C1",
            "CALC_TYPE": "1",
            "TRN_INDICATOR_FILTER": "",
            "TARGET_TYPE": "",
            "FILTER_PERIOD_ARR": "",
        }
    ]
    data = build_contest_data(
        contest_row,
        groups=[],
        links=[],
        rewards_by_code={},
        indicators=[],
        schedules=schedules,
    )
    assert len(data["schedule"]) == 1
    assert data["schedule"][0]["TOURNAMENT_CODE"] == "t_C1_4001"
    assert data["schedule"][0]["TOURNAMENT_CODE_ENDING"] == "4001"
    assert data["schedule"][0]["TOURNAMENT_STATUS"] == "АКТИВНЫЙ"


def test_prefixed_reward_full_code_and_ending() -> None:
    contest_row = {
        "CONTEST_CODE": "C1",
        "FULL_NAME": "X",
        "CONTEST_FEATURE": "",
        "CONTEST_PERIOD": "",
        "BUSINESS_BLOCK": "",
    }
    links = [{"CONTEST_CODE": "C1", "GROUP_CODE": "G1", "REWARD_CODE": "r_C1_1"}]
    rewards_by_code = {
        "r_C1_1": {
            "REWARD_CODE": "r_C1_1",
            "REWARD_TYPE": "BADGE",
            "FULL_NAME": "Бейдж",
            "REWARD_DESCRIPTION": "",
            "REWARD_CONDITION": "",
            "REWARD_COST": "1",
            "REWARD_ADD_DATA": "",
        }
    }
    data = build_contest_data(
        contest_row,
        groups=[],
        links=links,
        rewards_by_code=rewards_by_code,
        indicators=[],
        schedules=[],
    )
    assert data["reward_link"][0]["REWARD_CODE"] == "r_C1_1"
    assert data["reward_link"][0]["REWARD_CODE_ENDING"] == "1"
    assert data["badges"][0]["flat"]["REWARD_CODE"] == "r_C1_1"
    assert data["badges"][0]["flat"]["REWARD_CODE_ENDING"] == "1"
