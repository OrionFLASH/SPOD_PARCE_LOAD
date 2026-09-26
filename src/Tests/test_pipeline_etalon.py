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

PYTHONHASHSEED=0 — страховка: до исправления BUG-13 порядок строк SUMMARY зависел от обхода set()
строк и менялся от запуска к запуску; сейчас порядок строк и колонок детерминирован и
сравнивается строго (strict_order).
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
    "sys.exit(main_impl.main())\n"
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


def _run_main(base: Path, cfg_path: Path) -> subprocess.CompletedProcess:
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
    return proc


def run_fixture_pipeline(base: Path) -> Path:
    """Прогнать пайплайн на синтетике; вернуть путь к основной книге."""
    proc = _run_main(base, _build_isolated_project(base))
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
    # Порядок строк и колонок детерминирован (BUG-13) — сравниваем строго
    diffs, _notes = compare_fingerprints(expected, actual, strict_order=True)
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


def test_missing_input_file_exit_code_2_before_reading(tmp_path: Path) -> None:
    """BUG-06 / LOG-02: нет входного файла — код 2, до чтения CSV и записи Excel."""
    cfg_path = _build_isolated_project(tmp_path)
    victim = next((tmp_path / "IN").rglob("GROUP.csv"))
    victim.unlink()
    proc = _run_main(tmp_path, cfg_path)
    assert proc.returncode == 2, proc.stdout[-2000:] + proc.stderr[-2000:]
    assert "GROUP.csv" in proc.stderr
    assert not list((tmp_path / "OUT").rglob("*.xlsx")) if (tmp_path / "OUT").exists() else True
    logs = list((tmp_path / "LOGS").rglob("*.log"))
    assert logs, "лог-файл должен быть создан"
    log_text = logs[0].read_text(encoding="utf-8")
    assert "read_csv_file" not in log_text, "CSV не должны читаться, если файла не хватает"
    assert "код возврата: 2" in log_text


def test_parallel_blocks_exit_code_and_outputs(tmp_path: Path) -> None:
    """run_blocks_parallel: каждый блок в своём процессе; код 0 и основная книга на каждый блок."""
    cfg_path = _build_isolated_project(tmp_path)
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    cfg["run_blocks"] = ["PROM", "IFT"]
    cfg["run_blocks_parallel"] = True
    cfg["input_files"]["IFT"] = cfg["input_files"][BLOCK]
    cfg["run_outputs"]["IFT"] = ["main_only"]
    cfg_path.write_text(json.dumps(cfg, ensure_ascii=False), encoding="utf-8")
    proc = _run_main(tmp_path, cfg_path)
    assert proc.returncode == 0, proc.stdout[-3000:] + proc.stderr[-3000:]
    for block in ("PROM", "IFT"):
        books = list((tmp_path / "OUT" / block).rglob(f"SPOD_{block} main_*.xlsx"))
        assert len(books) == 1, f"{block}: {books}"
    # Лог каждого процесса-блока — отдельный файл не гарантируется (BUG-11), но итог есть в выводе
    assert "код возврата: 0" in proc.stdout


SOURCE_ETALON_PATH = ROOT / "src" / "Tests" / "fixtures" / "pipeline_prom_source_etalon.json"


def test_source_only_matches_etalon(tmp_path: Path) -> None:
    """Режим source_only: книга сырых данных «SPOD_PROM source …» совпадает с эталоном (PERF-07)."""
    cfg_path = _build_isolated_project(tmp_path)
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    cfg["run_outputs"] = {BLOCK: ["source_only"]}
    cfg_path.write_text(json.dumps(cfg, ensure_ascii=False), encoding="utf-8")
    proc = _run_main(tmp_path, cfg_path)
    assert proc.returncode == 0, proc.stdout[-3000:] + proc.stderr[-3000:]
    books = list((tmp_path / "OUT" / BLOCK).rglob(f"SPOD_{BLOCK} source *.xlsx"))
    assert len(books) == 1, books
    actual = fingerprint_workbook(str(books[0]))
    if os.environ.get("SPOD_UPDATE_ETALON") == "1":
        SOURCE_ETALON_PATH.write_text(json.dumps(actual, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
        pytest.skip(f"эталон пересоздан: {SOURCE_ETALON_PATH}")
    expected = json.loads(SOURCE_ETALON_PATH.read_text(encoding="utf-8"))
    diffs, _ = compare_fingerprints(expected, actual, strict_order=True)
    assert not diffs, "source-книга отличается от эталона:\n  - " + "\n  - ".join(diffs)
