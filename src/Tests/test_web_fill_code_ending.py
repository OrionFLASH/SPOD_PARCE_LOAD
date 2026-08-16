"""Правило окончаний r_/t_ + CONTEST_CODE (+ _ + ending только если ending непустой)."""

from __future__ import annotations

from src.Tools.export_web_fill_examples_from_spod import code_ending, compose_from_ending


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
