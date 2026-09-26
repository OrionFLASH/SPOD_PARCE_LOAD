# -*- coding: utf-8 -*-
"""
Эталонный тест всего пайплайна main.py → main_impl (TEST-01).

Синтетический набор CSV блока PROM (src/Tests/fixtures/pipeline_prom_fixture.py) прогоняется
через main_impl.main() в отдельном процессе во временном каталоге (рабочие IN/OUT/LOGS
и SQLite-архив не затрагиваются). Итоговая книга «SPOD_PROM main_*.xlsx» сводится к
отпечатку (src/Tools/pipeline_fingerprint.py) и сравнивается с эталоном
src/Tests/fixtures/pipeline_prom_etalon.json.

Конфигурация — действующая config/config.json, поэтому тест ловит и изменения кода,
и изменения правил. Если расхождение ожидаемо (правка конфига, осознанное исправление
вроде BUG-02), эталон пересоздаётся:
  Windows:  set SPOD_UPDATE_ETALON=1 && python -m pytest src/Tests/test_pipeline_etalon.py
  macOS:    SPOD_UPDATE_ETALON=1 python3.12 -m pytest src/Tests/test_pipeline_etalon.py
и изменения эталона проверяются в git diff вместе с кодом.

PYTHONHASHSEED=0: порядок строк SUMMARY сейчас зависит от порядка обхода set() строк,
а он без фиксированного seed меняется от запуска к запуску.
"""

from __future__ import annotations

import glob
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict

import openpyxl
import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.Tests.fixtures.pipeline_prom_fixture import write_fixture_inputs  # noqa: E402
from src.Tools.pipeline_fingerprint import (  # noqa: E402
    compare_fingerprints,
    fingerprint_workbook,
)
from src.config_loader import default_config_path, load_config_dict  # noqa: E402

ETALON_PATH = ROOT / "src" / "Tests" / "fixtures" / "pipeline_prom_etalon.json"
BLOCK = "PROM"

_RUNNER = (
    "import sys\n"
    "from src.config_loader import Config\n"
    "from src.config_holder import set_current_config\n"
    "from src import main_impl\n"
    "set_current_config(Config(sys.argv[1]))\n"
    "main_impl.main()\n"
)


def _build_isolated_project(base: Path) -> Path:
    """Каталог-проект: config/config.json (без $include) + синтетический IN."""
    cfg: Dict[str, Any] = load_config_dict(default_config_path(str(ROOT)))
    input_files = write_fixture_inputs(str(base / cfg["paths"]["input"]), cfg["input_files"][BLOCK])
    cfg["input_files"] = {BLOCK: input_files}
    cfg["run_blocks"] = [BLOCK]
    cfg["run_blocks_parallel"] = False
    cfg["run_outputs"] = {BLOCK: ["main_only"]}
    cfg["input_archive_sqlite"] = dict(cfg.get("input_archive_sqlite") or {}, enabled=False)
    cfg["logging"] = dict(cfg.get("logging") or {}, level="INFO")
    cfg_dir = base / "config"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = cfg_dir / "config.json"
    cfg_path.write_text(json.dumps(cfg, ensure_ascii=False, indent=1), encoding="utf-8")
    return cfg_path


def run_fixture_pipeline(base: Path) -> Path:
    """Прогнать пайплайн на синтетике; вернуть путь к основной книге."""
    cfg_path = _build_isolated_project(base)
    env = dict(os.environ)
    env["PYTHONHASHSEED"] = "0"
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    proc = subprocess.run(
        [sys.executable, "-c", _RUNNER, str(cfg_path)],
        cwd=str(base),
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=600,
    )
    assert proc.returncode == 0, (
        f"пайплайн завершился с кодом {proc.returncode}\n"
        f"--- stdout (хвост) ---\n{proc.stdout[-3000:]}\n--- stderr (хвост) ---\n{proc.stderr[-3000:]}"
    )
    found = glob.glob(str(base / "OUT" / BLOCK / "**" / f"SPOD_{BLOCK} main_*.xlsx"), recursive=True)
    assert len(found) == 1, f"ожидалась одна основная книга, найдено: {found}"
    return Path(found[0])


@pytest.fixture(scope="module")
def fixture_output(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return run_fixture_pipeline(tmp_path_factory.mktemp("spod_etalon"))


def test_pipeline_matches_etalon(fixture_output: Path) -> None:
    actual = fingerprint_workbook(str(fixture_output))
    if os.environ.get("SPOD_UPDATE_ETALON") == "1":
        ETALON_PATH.write_text(json.dumps(actual, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
        pytest.skip(f"эталон пересоздан: {ETALON_PATH}")
    assert ETALON_PATH.is_file(), (
        f"нет эталона {ETALON_PATH}; создать: SPOD_UPDATE_ETALON=1 python -m pytest {Path(__file__).name}"
    )
    expected = json.loads(ETALON_PATH.read_text(encoding="utf-8"))
    diffs, _notes = compare_fingerprints(expected, actual)
    assert not diffs, (
        "результат пайплайна отличается от эталона:\n  - "
        + "\n  - ".join(diffs)
        + "\nЕсли изменение ожидаемо — пересоздайте эталон (SPOD_UPDATE_ETALON=1, см. docstring модуля)."
    )


def test_fixture_output_has_key_sheets(fixture_output: Path) -> None:
    """Страховка от «пустого» эталона: ключевые листы есть и merge дал значения."""
    wb = openpyxl.load_workbook(fixture_output, read_only=True)
    try:
        names = set(wb.sheetnames)
        for sheet in ("SUMMARY", "CONSISTENCY", "CONTEST-DATA", "TOURNAMENT-SCHEDULE", "REPORT", "EMPLOYEE", "STAT_FILE"):
            assert sheet in names, f"нет листа {sheet}"
        rows = list(wb["TOURNAMENT-SCHEDULE"].iter_rows(values_only=True))
        header = list(rows[0])
        status = {r[header.index("TOURNAMENT_CODE")]: r[header.index("CALC_TOURNAMENT_STATUS")] for r in rows[1:]}
        assert status["t_T01_1"] == "ЗАВЕРШЕН"
        assert status["t_T01_2"] == "АКТИВНЫЙ"
        assert status["t_T02_1"] == "ЗАПЛАНИРОВАН"
        assert status["t_T03_2"] == "НЕОПРЕДЕЛЕН"
        report = list(wb["REPORT"].iter_rows(values_only=True))
        rh = list(report[0])
        full_names = [r[rh.index("CONTEST-DATA=>FULL_NAME")] for r in report[1:]]
        assert "Тестовый конкурс продаж" in full_names
    finally:
        wb.close()
