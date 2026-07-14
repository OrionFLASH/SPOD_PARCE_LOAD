# -*- coding: utf-8 -*-
"""Тесты блоков PROM/IFT/PSI: run_outputs по блокам, пути, плейсхолдеры."""

from __future__ import annotations

import os
from datetime import datetime

import pytest

from src.block_runtime import (
    block_label,
    resolve_block_placeholders,
    set_current_block,
)
from src.config_loader import (
    filter_input_files_for_block,
    get_input_files_for_block,
    parse_input_files_by_block,
    parse_run_blocks_config,
    parse_run_blocks_parallel,
    parse_run_outputs_for_block,
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


def test_parse_run_blocks_parallel_flag() -> None:
    assert parse_run_blocks_parallel({}) is False
    assert parse_run_blocks_parallel({"run_blocks_parallel": True}) is True
    assert parse_run_blocks_parallel({"run_blocks_parallel": 0}) is False


def test_parse_input_files_by_block_sections() -> None:
    cfg = {
        "input_files": {
            "PROM": [{"file": "a.csv", "sheet": "A", "subdir": "PROM/SPOD"}],
            "IFT": [
                {"file": "a.csv", "sheet": "A", "subdir": "IFT/SPOD"},
                {"file": "b.csv", "sheet": "B", "subdir": "IFT/FILE"},
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
            {"file": "a.csv", "block": "PROM", "subdir": "PROM/SPOD"},
            {"file": "b.csv", "subdir": "FILE"},
            {"file": "c.csv", "block": "IFT", "subdir": "IFT/SPOD"},
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


def test_run_outputs_per_block_object() -> None:
    cfg = {
        "run_outputs": {
            "PROM": ["main_only"],
            "IFT": ["source_only", "main_only"],
            "PSI": ["consistency_only"],
        }
    }
    prom = parse_run_outputs_for_block(cfg, "PROM")
    assert prom[0] == ["main_only"]
    assert prom[3] is True  # write_main
    ift = parse_run_outputs_for_block(cfg, "IFT")
    assert "source_only" in ift[0] and "main_only" in ift[0]
    assert ift[2] is True and ift[3] is True
    psi = parse_run_outputs_for_block(cfg, "PSI")
    assert psi[4] is True and psi[3] is False  # consistency only early-ish
    assert psi[5] is True  # consistency_early


def test_run_outputs_flat_list_same_for_all_blocks() -> None:
    cfg = {"run_outputs": ["manager_stats_only"]}
    for b in ("PROM", "IFT", "PSI"):
        ro = parse_run_outputs_for_block(cfg, b)
        assert ro[6] is True
        assert ro[7] is True  # early


def test_resolve_block_placeholders_paths() -> None:
    path = "OUT/DB/{BLOCK}/spod_input_archive_{BLOCK}_v2.sqlite"
    assert (
        resolve_block_placeholders(path, "prom")
        == "OUT/DB/PROM/spod_input_archive_PROM_v2.sqlite"
    )
    nested = {
        "db_path": path,
        "extra": ["{BLOCK}/JS", 1],
    }
    out = resolve_block_placeholders(nested, "IFT")
    assert out["db_path"].endswith("IFT_v2.sqlite")
    assert out["extra"][0] == "IFT/JS"


def test_block_label() -> None:
    set_current_block(None)
    assert block_label() == ""
    set_current_block("ift")
    assert block_label() == "[IFT]"
    set_current_block(None)


def test_get_output_dir_for_run_includes_block(tmp_path) -> None:
    from src.main_impl import get_output_dir_for_run

    base = str(tmp_path / "OUT")
    path = get_output_dir_for_run(base, block="PROM")
    now = datetime.now()
    expected = os.path.join(base, "PROM", now.strftime("%Y"), now.strftime("%d-%m"))
    assert path == expected
    assert os.path.isdir(path)


def test_real_config_sections_and_new_layout() -> None:
    from src.config_loader import Config

    cfg = Config()
    assert set(cfg.input_files_by_block.keys()) == {"PROM", "IFT", "PSI"}
    assert isinstance(cfg._cfg.get("run_outputs"), dict)
    assert cfg.run_blocks_parallel is False
    assert "{BLOCK}" in cfg.input_archive_sqlite.get("db_path", "")
    for block in ("PROM", "IFT", "PSI"):
        files = cfg.input_files_by_block[block]
        assert files
        assert all(
            str(f.get("subdir", "")).startswith(f"{block}/") for f in files
        ), f"subdir must start with {block}/"
        assert any(f["subdir"] == f"{block}/SPOD" for f in files)
        assert any(f["subdir"] == f"{block}/FILE" for f in files)
        for f in files:
            ap = f.get("archive_db_path") or ""
            assert "{BLOCK}" in ap
