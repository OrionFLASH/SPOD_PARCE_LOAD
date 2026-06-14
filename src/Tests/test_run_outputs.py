# -*- coding: utf-8 -*-
"""Тесты разбора run_outputs (комбинации выходных файлов)."""

from __future__ import annotations

from src.config_loader import parse_run_outputs_config


def test_manager_stats_and_main_both_enabled() -> None:
    ro = parse_run_outputs_config(
        {"run_outputs": ["manager_stats_only", "main_only"]}
    )
    (
        sorted_tokens,
        source_only_exit,
        write_source,
        write_main,
        write_consistency,
        consistency_early,
        write_manager_stats,
        manager_stats_early,
        write_stat_file,
        _compat,
        _rating_matrix,
        _season_summary,
    ) = ro
    assert sorted_tokens == ["main_only", "manager_stats_only"]
    assert write_main is True
    assert write_manager_stats is True
    assert manager_stats_early is False
    assert source_only_exit is False
    assert consistency_early is False
    assert write_stat_file is False


def test_manager_stats_only_early() -> None:
    ro = parse_run_outputs_config({"run_outputs": ["manager_stats_only"]})
    assert ro[3] is False  # write_main
    assert ro[6] is True  # write_manager_stats
    assert ro[7] is True  # manager_stats_early


def test_full_combo_tokens() -> None:
    ro = parse_run_outputs_config(
        {
            "run_outputs": [
                "source_only",
                "main_only",
                "consistency_only",
                "manager_stats_only",
                "stat_file_only",
                "rating_item_matrix",
                "season_order_summary",
            ]
        }
    )
    assert ro[2] is True  # write_source
    assert ro[3] is True  # write_main
    assert ro[4] is True  # write_consistency
    assert ro[6] is True  # write_manager_stats
    assert ro[7] is False  # manager_stats_early
    assert ro[8] is True  # write_stat_file
    assert ro[10] is True  # run_rating_item_matrix
    assert ro[11] is True  # run_season_order_summary


def test_manager_stats_only_skips_rating_and_season_tokens() -> None:
    ro = parse_run_outputs_config({"run_outputs": ["manager_stats_only"]})
    assert ro[10] is False
    assert ro[11] is False


def test_rating_and_season_tokens_alone() -> None:
    ro = parse_run_outputs_config(
        {"run_outputs": ["main_only", "rating_item_matrix", "season_order_summary"]}
    )
    assert ro[3] is True
    assert ro[10] is True
    assert ro[11] is True
