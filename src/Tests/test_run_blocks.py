# -*- coding: utf-8 -*-
"""Тесты разбора run_blocks и фильтрации input_files по блокам PROM/IFT/PSI."""

from __future__ import annotations

import os
from datetime import datetime

import pytest

from src.config_loader import (
    filter_input_files_for_block,
    parse_run_blocks_config,
    resolve_output_filename_template,
)


def test_parse_run_blocks_default() -> None:
    assert parse_run_blocks_config({}) == ["PROM"]
    assert parse_run_blocks_config({"run_blocks": None}) == ["PROM"]


def test_parse_run_blocks_order_and_dedup() -> None:
    assert parse_run_blocks_config({"run_blocks": ["ift", "PROM", "IFT", "psi"]}) == [
        "IFT",
        "PROM",
        "PSI",
    ]


def test_parse_run_blocks_invalid() -> None:
    with pytest.raises(ValueError, match="неизвестный блок"):
        parse_run_blocks_config({"run_blocks": ["DEV"]})
    with pytest.raises(ValueError, match="хотя бы одно"):
        parse_run_blocks_config({"run_blocks": []})
    with pytest.raises(ValueError, match="массив"):
        parse_run_blocks_config({"run_blocks": "PROM"})


def test_filter_input_files_for_block() -> None:
    files = [
        {"file": "a.csv", "block": "PROM", "subdir": "SPOD/PROM"},
        {"file": "b.csv", "block": "IFT", "subdir": "SPOD/IFT"},
        {"file": "c.csv", "subdir": "FILE"},
        {"file": "d.csv", "block": "*", "subdir": "FILE"},
    ]
    prom = filter_input_files_for_block(files, "PROM")
    assert [x["file"] for x in prom] == ["a.csv", "c.csv", "d.csv"]
    ift = filter_input_files_for_block(files, "IFT")
    assert [x["file"] for x in ift] == ["b.csv", "c.csv", "d.csv"]
    psi = filter_input_files_for_block(files, "PSI")
    assert [x["file"] for x in psi] == ["c.csv", "d.csv"]


def test_resolve_output_filename_template() -> None:
    assert resolve_output_filename_template("SPOD_{BLOCK} main", "ift") == "SPOD_IFT main"
    assert (
        resolve_output_filename_template("SPOD_{BLOCK} MANAGER_STATS", "PSI")
        == "SPOD_PSI MANAGER_STATS"
    )
    assert resolve_output_filename_template("SPOD_PROM source", "IFT") == "SPOD_IFT source"


def test_get_output_dir_for_run_includes_block(tmp_path) -> None:
    from src.main_impl import get_output_dir_for_run

    base = str(tmp_path / "OUT")
    path = get_output_dir_for_run(base, block="PROM")
    now = datetime.now()
    expected = os.path.join(base, "PROM", now.strftime("%Y"), now.strftime("%d-%m"))
    assert path == expected
    assert os.path.isdir(path)
