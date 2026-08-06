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
    display_full_path,
    leaf_key_name,
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


if __name__ == "__main__":
    unittest.main()
