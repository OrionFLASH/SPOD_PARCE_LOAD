# -*- coding: utf-8 -*-
"""Тесты разбора run_blocks и input_files по разделам PROM/IFT/PSI."""

from __future__ import annotations

import os
from datetime import datetime

import pytest

from src.config_loader import (
    filter_input_files_for_block,
    get_input_files_for_block,
    parse_input_files_by_block,
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


def test_parse_input_files_by_block_sections() -> None:
    cfg = {
        "input_files": {
            "PROM": [{"file": "a.csv", "sheet": "A", "subdir": "SPOD/PROM"}],
            "IFT": [
                {"file": "a.csv", "sheet": "A", "subdir": "SPOD/IFT"},
                {"file": "b.csv", "sheet": "B", "subdir": "FILE"},
            ],
            "PSI": [],
        }
    }
    by_block = parse_input_files_by_block(cfg)
    assert len(by_block["PROM"]) == 1
    assert len(by_block["IFT"]) == 2
    assert by_block["PSI"] == []
    assert get_input_files_for_block(by_block, "IFT")[1]["file"] == "b.csv"


def test_parse_input_files_legacy_flat_list() -> None:
    cfg = {
        "input_files": [
            {"file": "a.csv", "block": "PROM", "subdir": "SPOD/PROM"},
            {"file": "b.csv", "subdir": "FILE"},
            {"file": "c.csv", "block": "IFT", "subdir": "SPOD/IFT"},
        ]
    }
    by_block = parse_input_files_by_block(cfg)
    assert [x["file"] for x in by_block["PROM"]] == ["a.csv", "b.csv"]
    assert [x["file"] for x in by_block["IFT"]] == ["b.csv", "c.csv"]
    assert [x["file"] for x in by_block["PSI"]] == ["b.csv"]


def test_filter_input_files_for_block_dict_and_list() -> None:
    sections = {
        "PROM": [{"file": "p.csv"}],
        "IFT": [{"file": "i.csv"}],
        "PSI": [],
    }
    assert filter_input_files_for_block(sections, "PROM")[0]["file"] == "p.csv"
    flat = [
        {"file": "a.csv", "block": "PROM"},
        {"file": "c.csv"},
    ]
    assert [x["file"] for x in filter_input_files_for_block(flat, "PROM")] == [
        "a.csv",
        "c.csv",
    ]


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


def test_real_config_input_files_are_sections() -> None:
    from src.config_loader import Config

    cfg = Config()
    assert set(cfg.input_files_by_block.keys()) == {"PROM", "IFT", "PSI"}
    for block in ("PROM", "IFT", "PSI"):
        files = cfg.input_files_by_block[block]
        assert len(files) == len(cfg.input_files_by_block["PROM"])
        spod = [f for f in files if str(f.get("subdir", "")).startswith("SPOD/")]
        assert spod
        assert all(f["subdir"] == f"SPOD/{block}" for f in spod)
        assert all("block" not in f for f in files)
