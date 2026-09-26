# -*- coding: utf-8 -*-
"""Ошибки записи Excel (BUG-01), итог прогона (LOG-02) и безопасный вывод в консоль (LOG-05)."""

from __future__ import annotations

import io
import os
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import console_ui  # noqa: E402
from src import main_impl  # noqa: E402


def _sheets() -> dict:
    return {"DATA": (pd.DataFrame({"A": ["x", "y"]}), {"sheet": "DATA"})}


def test_write_to_excel_raises_and_leaves_no_file(tmp_path: Path) -> None:
    target = tmp_path / "нет_каталога" / "out.xlsx"
    with pytest.raises(main_impl.OutputWriteError):
        main_impl.write_to_excel(_sheets(), str(target))
    assert not target.exists()


@pytest.mark.skipif(os.name == "nt" or (hasattr(os, "geteuid") and os.geteuid() == 0),
                    reason="права каталога POSIX; под root запись разрешена")
def test_write_to_excel_permission_error_message(tmp_path: Path) -> None:
    locked = tmp_path / "locked"
    locked.mkdir()
    locked.chmod(0o500)
    try:
        with pytest.raises(main_impl.OutputWriteError, match="закройте"):
            main_impl.write_to_excel(_sheets(), str(locked / "out.xlsx"))
    finally:
        locked.chmod(0o700)


def test_write_to_excel_success(tmp_path: Path) -> None:
    target = tmp_path / "ok.xlsx"
    main_impl.write_to_excel(_sheets(), str(target))
    assert target.is_file()


def test_print_run_result(capsys: pytest.CaptureFixture) -> None:
    console_ui.print_run_result(3, 0, "LOGS/x.log", 0)
    out = capsys.readouterr().out
    assert "Предупреждений: 3, ошибок: 0; код возврата: 0 (успех)" in out
    assert "LOGS/x.log" in out
    console_ui.print_run_result(0, 1, "LOGS/x.log", 2)
    assert "(нет входных файлов)" in capsys.readouterr().out


def test_configure_console_output_redirected_cp1251(monkeypatch: pytest.MonkeyPatch) -> None:
    """Перенаправленный вывод в cp1251 (Windows): после настройки символы ✓ █ не роняют print."""
    raw = io.BytesIO()
    stream = io.TextIOWrapper(raw, encoding="cp1251")
    monkeypatch.setattr(sys, "stdout", stream)
    monkeypatch.setattr(sys, "stderr", io.TextIOWrapper(io.BytesIO(), encoding="cp1251"))
    console_ui.configure_console_output()
    print("✓ █ готово")
    stream.flush()
    assert raw.getvalue().decode("utf-8") == "✓ █ готово\n"
