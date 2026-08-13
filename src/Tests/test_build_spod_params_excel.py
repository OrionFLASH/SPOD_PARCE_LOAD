# -*- coding: utf-8 -*-
"""Тесты утилиты каталога параметров SPOD → Excel."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.Tools.build_spod_params_excel import (  # noqa: E402
    apply_excel_format_fields,
    display_full_path,
    excel_format_fields_for_row,
    format_header_candidates,
    is_native_format_column_entry,
    leaf_key_name,
    load_formats_config,
    make_param_id,
    normalize_json_cell,
    resolve_table_name,
    try_parse_json,
    walk_json_leaves,
)


class TestSpodParamsExcel(unittest.TestCase):
    def test_resolve_table_names(self) -> None:
        self.assertEqual(resolve_table_name("CONTEST__PROM__05-08_v0.csv"), "CONTEST-DATA")
        self.assertEqual(resolve_table_name("REWARD-LINK__PROM__x.csv"), "REWARD-LINK")
        self.assertEqual(resolve_table_name("REWARD__PROM__x.csv"), "REWARD")
        self.assertEqual(resolve_table_name("SCHEDULE__PROM__x.csv"), "TOURNAMENT-SCHEDULE")
        self.assertEqual(resolve_table_name("USER_ROLE_SB__PROM__x.csv"), "USER_ROLE SB")
        self.assertEqual(resolve_table_name("USER_ROLE__PROM__x.csv"), "USER_ROLE")
        self.assertEqual(resolve_table_name("ORG_UNIT_V20_04-06_v2.csv"), "ORG_UNIT_V20")

    def test_normalize_filter_period_trailing_quote(self) -> None:
        raw = (
            '[{"period_code""": 1, """criterion_mark_type""": """>""", '
            '"""criterion_mark_value""": 0, """start_dt""": """2023-06-01""", '
            '"""end_dt""": """2023-06-30"""}]"'
        )
        obj, ok = try_parse_json(raw)
        self.assertTrue(ok)
        self.assertIsInstance(obj, list)
        self.assertEqual(obj[0]["period_code"], 1)
        self.assertEqual(obj[0]["end_dt"], "2023-06-30")
        self.assertFalse(normalize_json_cell(raw).endswith('"'))

    def test_leaf_paths_and_names(self) -> None:
        self.assertEqual(leaf_key_name("hidden"), "hidden")
        self.assertEqual(
            leaf_key_name("getCondition.employeeRating.minRatingGOSB"),
            "minRatingGOSB",
        )
        self.assertEqual(leaf_key_name("feature"), "feature")
        self.assertEqual(
            leaf_key_name("getCondition.rewards[].rewardCode"),
            "rewardCode",
        )
        self.assertEqual(leaf_key_name("[].period_code"), "period_code")
        self.assertEqual(
            display_full_path("getCondition.nonRewards[].nonRewardCode"),
            "getCondition.nonRewards[].nonRewardCode",
        )

    def test_walk_leaves_skips_containers(self) -> None:
        obj = {
            "hidden": "N",
            "feature": ["a", ""],
            "getCondition": {
                "employeeRating": {"minRatingGOSB": "3", "seasonCode": "S1"},
                "rewards": [{"rewardCode": "r1", "amount": "1"}],
                "nonRewards": [],
            },
        }
        acc: dict = {}
        walk_json_leaves(obj, "", "REWARD_ADD_DATA", "REWARD", acc, {}, 0)
        paths = set(acc.keys())
        # конечные
        self.assertIn("hidden", paths)
        self.assertIn("feature", paths)
        self.assertIn("getCondition.employeeRating.minRatingGOSB", paths)
        self.assertIn("getCondition.employeeRating.seasonCode", paths)
        self.assertIn("getCondition.rewards[].rewardCode", paths)
        self.assertIn("getCondition.rewards[].amount", paths)
        # контейнеры не регистрируются
        self.assertNotIn("getCondition", paths)
        self.assertNotIn("getCondition.employeeRating", paths)
        self.assertNotIn("getCondition.rewards", paths)
        self.assertNotIn("getCondition.rewards[]", paths)
        # пустой массив — конечное значение (в т.ч. пустое)
        self.assertIn("getCondition.nonRewards", paths)

    def test_param_id_unique_shape(self) -> None:
        a = make_param_id("REWARD", "REWARD_CODE", "-", False)
        b = make_param_id("REWARD", "REWARD_ADD_DATA", "priority", True)
        self.assertEqual(a, "REWARD__COL__REWARD_CODE")
        self.assertIn("JSON", b)
        self.assertIn("priority", b)


class TestExcelFormatEnrichment(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.formats = load_formats_config()

    def test_native_vs_cross_sheet_entries(self) -> None:
        self.assertTrue(is_native_format_column_entry("START_DT", "TOURNAMENT-SCHEDULE"))
        self.assertTrue(is_native_format_column_entry("ADD_DATA => itemMinShow", "REWARD"))
        self.assertFalse(
            is_native_format_column_entry("REPORT=>CONTEST_DATE", "TOURNAMENT-SCHEDULE")
        )
        self.assertFalse(
            is_native_format_column_entry(
                "LIST-TOURNAMENT=>Дата обновления данных источника",
                "TOURNAMENT-SCHEDULE",
            )
        )

    def test_close_dt_is_date(self) -> None:
        row = {
            "table": "CONTEST-DATA",
            "param_kind": "КОЛОНКА",
            "csv_column": "CLOSE_DT",
            "json_path": "-",
        }
        fields = excel_format_fields_for_row(self.formats, row)
        self.assertEqual(fields["excel_type"], "date")
        self.assertIn("YYYY-MM-DD", fields["excel_limits"])
        self.assertIn("гориз=center", fields["excel_align"])
        self.assertEqual(fields["excel_width"], "-")

    def test_reward_cost_is_number(self) -> None:
        row = {
            "table": "REWARD",
            "param_kind": "КОЛОНКА",
            "csv_column": "REWARD_COST",
            "json_path": "-",
        }
        fields = excel_format_fields_for_row(self.formats, row)
        self.assertEqual(fields["excel_type"], "number")
        self.assertIn("знаки=0", fields["excel_limits"])
        self.assertEqual(fields["excel_width"], "-")

    def test_json_item_min_show_number(self) -> None:
        cands = format_header_candidates(
            "REWARD", "REWARD_ADD_DATA", "itemMinShow", True
        )
        self.assertIn("ADD_DATA => itemMinShow", cands)
        row = {
            "table": "REWARD",
            "param_kind": "JSON-КЛЮЧ",
            "csv_column": "REWARD_ADD_DATA",
            "json_path": "itemMinShow",
        }
        fields = excel_format_fields_for_row(self.formats, row)
        self.assertEqual(fields["excel_type"], "number")

    def test_schedule_ignores_report_merge_in_same_rule(self) -> None:
        """Правило SCHEDULE содержит REPORT=>… — на START_DT это не влияет."""
        row_start = {
            "table": "TOURNAMENT-SCHEDULE",
            "param_kind": "КОЛОНКА",
            "csv_column": "START_DT",
            "json_path": "-",
        }
        fields = excel_format_fields_for_row(self.formats, row_start)
        self.assertEqual(fields["excel_type"], "date")

        # чужая merge-колонка не матчится как native CSV SCHEDULE
        row_fake = {
            "table": "TOURNAMENT-SCHEDULE",
            "param_kind": "КОЛОНКА",
            "csv_column": "REPORT=>CONTEST_DATE",
            "json_path": "-",
        }
        fields_fake = excel_format_fields_for_row(self.formats, row_fake)
        self.assertEqual(fields_fake["excel_type"], "-")

    def test_sheet_width_not_applied(self) -> None:
        """Листовая ширина из RUN_INPUT не проставляется; без per-column в FORMATS — '-'."""
        row = {
            "table": "REPORT",
            "param_kind": "КОЛОНКА",
            "csv_column": "CONTEST_DATE",
            "json_path": "-",
        }
        fields = excel_format_fields_for_row(self.formats, row)
        self.assertEqual(fields["excel_type"], "date")
        self.assertEqual(fields["excel_width"], "-")

    def test_per_column_width_from_rule(self) -> None:
        cfg = {
            "color_scheme": [],
            "column_formats": [
                {
                    "sheet": "REWARD",
                    "columns": ["REWARD_COST"],
                    "data_type": "number",
                    "decimal_places": 0,
                    "decimal_separator": ",",
                    "thousands_separator": False,
                    "horizontal": "center",
                    "vertical": "center",
                    "wrap_text": False,
                    "width_mode": 24,
                    "min_width": 10,
                    "max_width": 40,
                }
            ],
        }
        row = {
            "table": "REWARD",
            "param_kind": "КОЛОНКА",
            "csv_column": "REWARD_COST",
            "json_path": "-",
        }
        fields = excel_format_fields_for_row(cfg, row)
        self.assertEqual(fields["excel_width"], "mode=24; min=10; max=40")

    def test_apply_does_not_add_rows(self) -> None:
        rows = [
            {
                "table": "GROUP",
                "param_kind": "КОЛОНКА",
                "csv_column": "GROUP_CODE",
                "json_path": "-",
                "name": "GROUP_CODE",
            }
        ]
        apply_excel_format_fields(rows, self.formats)
        self.assertEqual(len(rows), 1)
        self.assertIn("excel_type", rows[0])
        self.assertEqual(rows[0]["excel_type"], "-")


if __name__ == "__main__":
    unittest.main()
