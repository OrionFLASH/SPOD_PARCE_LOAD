# -*- coding: utf-8 -*-
"""Тесты загрузки config/ с $include."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.config_loader import (
    Config,
    default_config_path,
    load_config_dict,
    resolve_project_base_dir,
)


def test_default_path_and_base_dir() -> None:
    path = default_config_path()
    assert path.endswith("config/config.json") or path.endswith("config\\config.json")
    base = resolve_project_base_dir(path)
    assert Path(base, "src").is_dir()
    assert Path(base, "config").is_dir()


def test_real_config_loads_merged() -> None:
    cfg = Config()
    assert cfg.run_blocks == ["PROM"] or isinstance(cfg.run_blocks, list)
    assert "PROM" in cfg.input_files_by_block
    assert cfg.consistency_checks.get("rules")
    assert cfg.merge_fields_advanced
    assert "rating_item_matrix" in cfg._cfg or cfg.rating_item_matrix is not None
    assert str(cfg.dir_input).endswith("IN")


def test_include_merge_and_override(tmp_path: Path) -> None:
    (tmp_path / "A.json").write_text(
        json.dumps({"paths": {"input": "IN", "output": "OUT", "logs": "LOGS"}, "a": 1}),
        encoding="utf-8",
    )
    (tmp_path / "B.json").write_text(
        json.dumps({"logging": {"level": "INFO", "base_name": "LOGS"}, "b": 2}),
        encoding="utf-8",
    )
    entry = tmp_path / "config.json"
    entry.write_text(
        json.dumps(
            {
                "$include": ["A.json", "B.json"],
                "a": 9,
                "logging": {"level": "DEBUG", "base_name": "LOGS"},
            }
        ),
        encoding="utf-8",
    )
    merged = load_config_dict(str(entry))
    assert merged["a"] == 9
    assert merged["b"] == 2
    assert merged["logging"]["level"] == "DEBUG"
    assert merged["paths"]["input"] == "IN"


def test_duplicate_top_level_key_raises(tmp_path: Path) -> None:
    (tmp_path / "A.json").write_text(json.dumps({"x": 1}), encoding="utf-8")
    (tmp_path / "B.json").write_text(json.dumps({"x": 2}), encoding="utf-8")
    entry = tmp_path / "config.json"
    entry.write_text(json.dumps({"$include": ["A.json", "B.json"]}), encoding="utf-8")
    with pytest.raises(ValueError, match="Дублирующий ключ"):
        load_config_dict(str(entry))


def test_nested_include_forbidden(tmp_path: Path) -> None:
    (tmp_path / "A.json").write_text(
        json.dumps({"$include": ["B.json"], "x": 1}), encoding="utf-8"
    )
    entry = tmp_path / "config.json"
    entry.write_text(json.dumps({"$include": ["A.json"]}), encoding="utf-8")
    with pytest.raises(ValueError, match="вложенные include"):
        load_config_dict(str(entry))
