# -*- coding: utf-8 -*-
"""Тесты performance.skip_data_alignment_sheets и сопоставления имён листов."""

from __future__ import annotations

from src.config_loader import (
    DEFAULT_SKIP_DATA_ALIGNMENT_SHEETS,
    parse_skip_data_alignment_sheets,
    sheet_skips_data_alignment,
)


def test_default_patterns_cover_heavy_sheets() -> None:
    pats = DEFAULT_SKIP_DATA_ALIGNMENT_SHEETS
    assert sheet_skips_data_alignment("LIST-REWARDS", pats)
    assert sheet_skips_data_alignment("STATISTICS", pats)
    assert sheet_skips_data_alignment("RATING", pats)
    assert sheet_skips_data_alignment("RATING_2026_1 (KMKKSB)", pats)
    assert sheet_skips_data_alignment("RATING_ALLTIME (CSM)", pats)
    assert sheet_skips_data_alignment("ORDER", pats)
    assert sheet_skips_data_alignment("ORDER_2025_2 (MNS)", pats)
    assert sheet_skips_data_alignment("ORDER-SEASON-SUMMARY", pats)
    assert not sheet_skips_data_alignment("REPORT", pats)
    assert not sheet_skips_data_alignment("EMPLOYEE", pats)


def test_parse_missing_key_uses_defaults() -> None:
    assert parse_skip_data_alignment_sheets({}) == list(DEFAULT_SKIP_DATA_ALIGNMENT_SHEETS)
    assert parse_skip_data_alignment_sheets({"performance": {}}) == list(
        DEFAULT_SKIP_DATA_ALIGNMENT_SHEETS
    )


def test_parse_empty_list_disables_skip() -> None:
    pats = parse_skip_data_alignment_sheets(
        {"performance": {"skip_data_alignment_sheets": []}}
    )
    assert pats == []
    assert not sheet_skips_data_alignment("LIST-REWARDS", pats)


def test_parse_custom_list() -> None:
    pats = parse_skip_data_alignment_sheets(
        {"performance": {"skip_data_alignment_sheets": ["REPORT", "YEAR_*"]}}
    )
    assert sheet_skips_data_alignment("REPORT", pats)
    assert sheet_skips_data_alignment("YEAR_STATA", pats)
    assert not sheet_skips_data_alignment("LIST-REWARDS", pats)
