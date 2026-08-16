# -*- coding: utf-8 -*-
"""Тесты Excel-формы BADGE: SPOD-JSON и round-trip export→import."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from typing import Any, Dict

from src.config_loader import load_config_dict, project_root_dir
from src.contest_badge_form.export_form import export_contest_codes
from src.contest_badge_form.form_io import create_blank_form, read_form_workbook
from src.contest_badge_form.import_form import import_form_file
from src.contest_badge_form.schema import max_badge_slots
from src.contest_badge_form.spod_json import (
    dumps_spod_json,
    parse_spod_json,
)


def _strip_empties(obj: Any) -> Any:
    """Убрать пустые строки/списки/словари для сравнения round-trip."""
    if isinstance(obj, dict):
        out: Dict[str, Any] = {}
        for k, v in obj.items():
            nv = _strip_empties(v)
            if nv == "" or nv == [] or nv == {}:
                continue
            out[k] = nv
        return out
    if isinstance(obj, list):
        return [_strip_empties(x) for x in obj]
    return obj


class TestSpodJson(unittest.TestCase):
    def test_roundtrip_simple(self) -> None:
        obj = {
            "feature": [],
            "nftFlg": "N",
            "businessBlock": ["KMKKSB"],
            "fileName": "",
        }
        raw = dumps_spod_json(obj)
        self.assertIn('"""nftFlg"""', raw)
        self.assertIn('"""N"""', raw)
        parsed = parse_spod_json(raw)
        self.assertEqual(parsed["nftFlg"], "N")
        self.assertEqual(parsed["businessBlock"], ["KMKKSB"])

    def test_parse_real_add_data_fragment(self) -> None:
        raw = (
            '{"""feature""": [], """nftFlg""":"""N""", '
            '"""businessBlock""": ["""KMKKSB"""]}'
        )
        parsed = parse_spod_json(raw)
        assert isinstance(parsed, dict)
        self.assertEqual(parsed["nftFlg"], "N")
        self.assertEqual(parsed["businessBlock"], ["KMKKSB"])

    def test_parse_trailing_quote_array(self) -> None:
        """Выгрузки CSV часто оставляют хвост «]"» после массива."""
        raw = (
            '[{"period_code""": 1, """start_dt""": """2026-01-01""", '
            '"""end_dt""": """2026-01-31"""}]"'
        )
        parsed = parse_spod_json(raw)
        assert isinstance(parsed, list)
        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0]["period_code"], 1)
        self.assertEqual(parsed[0]["start_dt"], "2026-01-01")


class TestSchemaLimits(unittest.TestCase):
    def test_limits(self) -> None:
        self.assertEqual(max_badge_slots("ТУРНИРНЫЙ"), 3)
        self.assertEqual(max_badge_slots("ИНДИВИДУАЛЬНЫЙ"), 1)
        self.assertEqual(max_badge_slots("ИНДИВИДУАЛЬНЫЙ НАКОПИТЕЛЬНЫЙ"), 1)


class TestBlankForm(unittest.TestCase):
    def test_create_blank(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "blank.xlsx")
            create_blank_form(path, sheet_count=2, contest_type="ТУРНИРНЫЙ")
            payloads = read_form_workbook(path)
            self.assertEqual(len(payloads), 2)
            self.assertEqual(
                payloads[0]["contest_flat"].get("CONTEST_TYPE"), "ТУРНИРНЫЙ"
            )


@unittest.skipUnless(
    os.path.isdir(
        os.path.join(project_root_dir(), "IN", "PROM", "SPOD")
    ),
    "Нет IN/PROM/SPOD",
)
class TestContestBadgeFormRoundTrip(unittest.TestCase):
    def test_export_import_roundtrip(self) -> None:
        root = project_root_dir()
        cfg_path = os.path.join(root, "config", "config.json")
        cfg = load_config_dict(cfg_path)
        codes = ["01_2025-0_11-1_1", "01_2025-0_00-2_1"]

        with tempfile.TemporaryDirectory() as tmp:
            form_path = os.path.join(tmp, "form.xlsx")
            export_contest_codes(root, cfg, "PROM", codes, output_path=form_path)
            self.assertTrue(os.path.isfile(form_path))

            out_dir = os.path.join(tmp, "import_out")
            meta = import_form_file(form_path, root, cfg, "PROM", output_dir=out_dir)
            self.assertTrue(os.path.isfile(meta["excel"]))
            tables = meta["tables"]
            self.assertEqual(len(tables["contest"]), 2)

            # Сверка с исходным CSV
            import pandas as pd

            from src.contest_badge_form.csv_load import load_spod_frames, rewards_for_contest

            frames = load_spod_frames(root, cfg, "PROM")
            src_contest = frames["contest"]

            for crow in tables["contest"]:
                code = crow["CONTEST_CODE"]
                src = src_contest[src_contest["CONTEST_CODE"] == code].iloc[0]
                for col in (
                    "FULL_NAME",
                    "CONTEST_TYPE",
                    "BUSINESS_STATUS",
                    "TARGET_TYPE",
                    "CALC_TYPE",
                ):
                    self.assertEqual(
                        str(crow.get(col, "")),
                        str(src[col]),
                        msg=f"{code}.{col}",
                    )
                # JSON-поля после нормализации
                for col in ("BUSINESS_BLOCK", "CONTEST_FEATURE"):
                    a = _strip_empties(parse_spod_json(crow[col]))
                    b = _strip_empties(parse_spod_json(src[col]))
                    self.assertEqual(a, b, msg=f"{code}.{col}")

                rewards_df, _links = rewards_for_contest(
                    frames["reward"], frames["reward_link"], code
                )
                badge_src = rewards_df[
                    rewards_df["REWARD_TYPE"].astype(str).str.upper() == "BADGE"
                ]
                imported = [
                    r
                    for r in tables["reward"]
                    if r["REWARD_CODE"]
                    in set(badge_src["REWARD_CODE"].astype(str))
                ]
                self.assertEqual(len(imported), len(badge_src), msg=code)
                by_code = {r["REWARD_CODE"]: r for r in imported}
                for _, srow in badge_src.iterrows():
                    rc = str(srow["REWARD_CODE"])
                    self.assertIn(rc, by_code)
                    self.assertEqual(by_code[rc]["FULL_NAME"], srow["FULL_NAME"])
                    a = _strip_empties(parse_spod_json(by_code[rc]["REWARD_ADD_DATA"]))
                    b = _strip_empties(parse_spod_json(srow["REWARD_ADD_DATA"]))
                    self.assertEqual(a, b, msg=f"ADD_DATA {rc}")


if __name__ == "__main__":
    unittest.main()
