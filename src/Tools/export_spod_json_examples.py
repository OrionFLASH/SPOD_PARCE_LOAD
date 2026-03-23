# -*- coding: utf-8 -*-
"""
Экспорт выгрузок CSV из IN/SPOD в JSON: **один CSV = один .json** с тем же именем.

В каждой строке ``rows`` — **все колонки** исходного файла. Значения по умолчанию —
строки; если ячейка после нормализации начинается с ``{`` или ``[``, выполняется
попытка ``json.loads`` (как для REWARD_ADD_DATA / CONTEST_FEATURE и др.).

Запуск из корня проекта:
    python src/Tools/export_spod_json_examples.py
"""
from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[2]
IN_SPOD = ROOT / "IN" / "SPOD"
OUT_DIR = ROOT / "Docs" / "JSON" / "examples"
DELIM = ";"
ENC = "utf-8"

# Список файлов выгрузки (имена в IN/SPOD), для которых строятся примеры JSON.
# Порядок — как в документации; отсутствующий файл пропускается с предупреждением.
CSV_EXAMPLES_ORDER: List[str] = [
    "CONTEST (PROM) 23-03 v3.csv",
    "employee (PROM) 13-03 v0.csv",
    "GROUP (PROM) 18-03 v0.csv",
    "INDICATOR (PROM) 18-03 v0.csv",
    "ORG_UNIT_V20 20-03 v0.csv",
    "REPORT (PROM) 18-03 v0.csv",
    "REWARD (PROM) 23-03 v3.csv",
    "REWARD-LINK (PROM) 18-03 v0.csv",
    "SCHEDULE (PROM) 18-03 v0.csv",
    "USER_ROLE (PROM) 13-03 v0.csv",
]


def normalize_json_cell(s: str) -> str:
    """Тройные кавычки → обычные (как при разборе JSON в ячейках CSV)."""
    return (s or "").replace('"""', '"').strip()


def try_parse_json_cell(s: str) -> Optional[Any]:
    t = normalize_json_cell(s)
    if not t:
        return None
    try:
        return json.loads(t)
    except json.JSONDecodeError:
        return None


def cell_to_value(raw: Optional[str]) -> Any:
    """
    Значение ячейки для JSON: пусто → пустая строка;
    если похоже на JSON-объект/массив — разбор, иначе строка как в CSV.
    """
    if raw is None:
        return ""
    s = normalize_json_cell(raw)
    if not s:
        return ""
    if s[0] in "{[":
        parsed = try_parse_json_cell(raw)
        if parsed is not None:
            return parsed
    return s


def export_csv_full(csv_path: Path) -> tuple[Path, int]:
    """Полный дамп CSV: все колонки, все строки (без пропусков)."""
    rows_out: List[Dict[str, Any]] = []
    with csv_path.open(encoding=ENC, newline="") as f:
        reader = csv.DictReader(f, delimiter=DELIM)
        fieldnames = list(reader.fieldnames or [])
        if not fieldnames:
            raise ValueError("пустой заголовок CSV")
        for row in reader:
            rec: Dict[str, Any] = {}
            for col in fieldnames:
                rec[col] = cell_to_value(row.get(col))
            rows_out.append(rec)

    payload = {
        "source_csv": f"IN/SPOD/{csv_path.name}",
        "columns": fieldnames,
        "rows": rows_out,
    }
    out_path = OUT_DIR / f"{csv_path.stem}.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return out_path, len(rows_out)


def main() -> None:
    if not IN_SPOD.is_dir():
        print(f"Каталог не найден: {IN_SPOD}")
        return
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    for name in CSV_EXAMPLES_ORDER:
        csv_path = IN_SPOD / name
        if not csv_path.is_file():
            print(f"Пропуск (нет файла): {name}")
            continue
        try:
            out, nrows = export_csv_full(csv_path)
            print(f"Записано: {out.relative_to(ROOT)} ({nrows} строк)")
        except Exception as e:
            print(f"Ошибка {name}: {e}")

    print(f"Готово. Каталог: {OUT_DIR.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
