# -*- coding: utf-8 -*-
"""Сверка снимка fill JSON с CSV PROM SPOD (листы каталога из CONFIG_RUN_INPUT)."""

from __future__ import annotations

import os
import unittest
from typing import Any, Dict, List

from src.config_loader import project_root_dir
from src.Tools.export_web_fill_examples_from_spod import (
    SpodTables,
    build_contest_data,
    build_project,
    collect_all_contest_codes,
    expected_row_counts,
    fill_sheets_from_catalog,
    load_prom_spod_tables,
    reconcile_snapshot_with_csv,
)


def _empty_tables() -> SpodTables:
    return SpodTables(
        contest=[],
        group=[],
        indicator=[],
        reward=[],
        reward_link=[],
        schedule=[],
        source_files={},
        block="PROM",
    )


def test_fill_sheets_from_catalog_order() -> None:
    catalog = {
        "sections": [
            {"id": "CONTEST"},
            {"id": "CONTEST_FEATURE"},
            {"id": "TABLE:GROUP"},
            {"id": "TABLE:INDICATOR"},
            {"id": "TABLE:SCHEDULE"},
            {"id": "REWARD"},
            {"id": "TABLE:REWARD-LINK"},
        ]
    }
    assert fill_sheets_from_catalog(catalog) == [
        "CONTEST-DATA",
        "GROUP",
        "INDICATOR",
        "TOURNAMENT-SCHEDULE",
        "REWARD",
        "REWARD-LINK",
    ]


def test_reconcile_counts_match() -> None:
    tables = _empty_tables()
    tables.contest = [{"CONTEST_CODE": "C1", "FULL_NAME": "A"}]
    tables.schedule = [{"CONTEST_CODE": "C1", "TOURNAMENT_CODE": "t_C1_1"}]
    payload: Dict[str, Any] = {
        "contests": [
            {
                "data": {
                    "contest": {"CONTEST_CODE": "C1", "FULL_NAME": "A"},
                    "group": [],
                    "indicator": [],
                    "schedule": [{"TOURNAMENT_CODE": "1"}],
                    "reward_link": [],
                    "badges": [],
                }
            }
        ]
    }
    assert reconcile_snapshot_with_csv(payload, tables, expected_codes=["C1"]) == []


def test_reconcile_detects_phantom_schedule() -> None:
    tables = _empty_tables()
    tables.contest = [{"CONTEST_CODE": "CONTEST_00", "FULL_NAME": "Приветственный"}]
    payload: Dict[str, Any] = {
        "contests": [
            {
                "data": {
                    "contest": {"CONTEST_CODE": "CONTEST_00", "FULL_NAME": "Приветственный"},
                    "group": [],
                    "indicator": [],
                    "schedule": [{"TOURNAMENT_CODE": ""}],
                    "reward_link": [],
                    "badges": [],
                }
            }
        ]
    }
    errors = reconcile_snapshot_with_csv(
        payload, tables, expected_codes=["CONTEST_00"]
    )
    assert any("schedule" in e and "JSON=1" in e and "CSV=0" for e in errors)


@unittest.skipUnless(
    os.path.isdir(os.path.join(project_root_dir(), "IN", "PROM", "SPOD")),
    "Нет IN/PROM/SPOD",
)
class TestPromSpodCsvJson(unittest.TestCase):
    """Интеграция: файлы из CONFIG_RUN_INPUT.json, все конкурсы включая CONTEST_00."""

    tables: SpodTables

    @classmethod
    def setUpClass(cls) -> None:
        cls.tables = load_prom_spod_tables(block="PROM")

    def test_config_files_are_prom_spod_from_input(self) -> None:
        files = self.tables.source_files
        self.assertIn("contest", files)
        for rel in files.values():
            self.assertIn("IN/PROM/SPOD/", rel.replace("\\", "/"))
            self.assertTrue(rel.endswith(".csv"))

    def test_all_contest_codes_include_contest_00(self) -> None:
        codes = collect_all_contest_codes(self.tables)
        self.assertIn("CONTEST_00", codes)
        self.assertGreaterEqual(len(codes), 2)

    def test_contest_00_csv_counts(self) -> None:
        exp = expected_row_counts(self.tables, "CONTEST_00", badge_only=False)
        self.assertEqual(exp["schedule"], 0)
        self.assertEqual(exp["indicator"], 0)
        data = build_contest_data(
            next(
                r
                for r in self.tables.contest
                if str(r.get("CONTEST_CODE") or "") == "CONTEST_00"
            ),
            groups=self.tables.rows_for("group", "CONTEST_00"),
            links=self.tables.rows_for("reward_link", "CONTEST_00"),
            rewards_by_code={
                r["REWARD_CODE"]: r for r in self.tables.reward if r.get("REWARD_CODE")
            },
            indicators=self.tables.rows_for("indicator", "CONTEST_00"),
            schedules=self.tables.rows_for("schedule", "CONTEST_00"),
        )
        self.assertEqual(data["schedule"], [])
        self.assertEqual(data["indicator"], [])
        self.assertEqual(len(data["group"]), exp["group"])
        self.assertEqual(len(data["badges"]), exp["badges"])

    def test_all_contests_json_equals_csv_counts(self) -> None:
        codes: List[str] = collect_all_contest_codes(self.tables)
        payload = build_project(
            codes,
            title="test-all",
            tables=self.tables,
            badge_only=False,
        )
        errors = reconcile_snapshot_with_csv(
            payload,
            self.tables,
            badge_only=False,
            expected_codes=codes,
        )
        self.assertEqual(errors, [], msg="\n".join(errors[:30]))
        self.assertEqual(len(payload["contests"]), len(codes))
