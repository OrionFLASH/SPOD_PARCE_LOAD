# -*- coding: utf-8 -*-
"""
Сбор строк REPORT по списку TOURNAMENT_CODE из каталога IN/REPORT (рекурсивно).

Независим от main.py / config/config.json. Конфигурация: config_folder_parce.json.

Запуск:
  python folder_parce.py
  python folder_parce.py --config config_folder_parce.json
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import pandas as pd

COL_FILES_FOUND = "PARCE_FILES_FOUND"
COL_FILES_MAX_DATE = "PARCE_FILES_WITH_MAX_DATE"
COL_SOURCE_FILE = "PARCE_SOURCE_FILE"
SOURCE_NOT_FOUND = "НЕ ОБНАРУЖЕН REPORT"
PLACEHOLDER_DASH = "-"


@dataclass(frozen=True)
class FileHit:
    """Результат Pass1: турнир найден в файле."""

    code: str
    path_abs: Path
    path_rel: str
    max_date: str
    mtime: float
    rows: int


@dataclass(frozen=True)
class WinnerInfo:
    """Победитель по одному TOURNAMENT_CODE."""

    code: str
    path_abs: Path
    path_rel: str
    max_date: str
    mtime: float
    files_found: int
    files_same_date: int


def project_root() -> Path:
    """Корень репозитория = каталог, где лежит этот скрипт."""
    return Path(__file__).resolve().parent


def default_config_path() -> Path:
    return project_root() / "config_folder_parce.json"


def load_config(config_path: Path) -> Dict[str, Any]:
    """Загрузка и базовая валидация config_folder_parce.json."""
    if not config_path.is_file():
        raise FileNotFoundError(f"Конфиг не найден: {config_path}")
    with config_path.open("r", encoding="utf-8") as f:
        cfg = json.load(f)
    if not isinstance(cfg, dict):
        raise ValueError("Конфиг должен быть JSON-объектом")

    codes_raw = cfg.get("tournament_codes")
    if not isinstance(codes_raw, list) or not codes_raw:
        raise ValueError("tournament_codes: нужен непустой массив строк")
    codes: List[str] = []
    for item in codes_raw:
        if isinstance(item, str) and item.strip():
            codes.append(item.strip())
    if not codes:
        raise ValueError("tournament_codes: после очистки список пуст")
    cfg["tournament_codes"] = codes

    paths = cfg.get("paths") or {}
    if not isinstance(paths, dict):
        raise ValueError("paths: ожидается объект")
    cfg["paths"] = paths

    csv_cfg = cfg.get("csv") or {}
    if not isinstance(csv_cfg, dict):
        raise ValueError("csv: ожидается объект")
    cfg["csv"] = {
        "sep": str(csv_cfg.get("sep", ";")),
        "encoding": str(csv_cfg.get("encoding", "utf-8-sig")),
        "tournament_column": str(csv_cfg.get("tournament_column", "TOURNAMENT_CODE")),
        "date_column": str(csv_cfg.get("date_column", "CONTEST_DATE")),
        "glob": str(csv_cfg.get("glob", "*.csv")),
    }

    perf = cfg.get("performance") or {}
    workers = int(perf.get("max_workers", 8) or 8)
    cfg["performance"] = {"max_workers": max(1, workers)}

    log_cfg = cfg.get("logging") or {}
    every = int(log_cfg.get("progress_every_files", 1) or 1)
    cfg["logging"] = {"progress_every_files": max(1, every)}

    return cfg


def resolve_path(root: Path, rel: str) -> Path:
    p = Path(rel)
    if p.is_absolute():
        return p
    return (root / p).resolve()


def discover_csv_files(report_root: Path, glob_pat: str) -> List[Path]:
    """Все CSV под report_root с любой вложенностью, стабильный порядок."""
    return sorted(
        {p.resolve() for p in report_root.rglob(glob_pat) if p.is_file()},
        key=lambda x: x.as_posix().lower(),
    )


def _is_empty_date(val: Any) -> bool:
    if val is None:
        return True
    if isinstance(val, float) and pd.isna(val):
        return True
    s = str(val).strip()
    return s == "" or s.lower() in ("nan", "none", "null", "-")


def _max_contest_date(series: pd.Series) -> Optional[str]:
    """Максимум YYYY-MM-DD среди непустых; None если дат нет."""
    best: Optional[str] = None
    for raw in series:
        if _is_empty_date(raw):
            continue
        s = str(raw).strip()[:10]
        if len(s) < 10:
            continue
        if best is None or s > best:
            best = s
    return best


def scan_one_file(
    path: Path,
    report_root: Path,
    codes: Set[str],
    sep: str,
    encoding: str,
    col_code: str,
    col_date: str,
) -> Tuple[str, List[FileHit], Optional[str]]:
    """
    Pass1: читает только CODE+DATE.
    Возвращает (path_rel, hits, error_or_None).
    """
    try:
        rel = path.relative_to(report_root).as_posix()
    except ValueError:
        rel = path.name

    try:
        mtime = path.stat().st_mtime
    except OSError as ex:
        return rel, [], f"stat: {ex}"

    try:
        header = pd.read_csv(path, sep=sep, encoding=encoding, dtype=str, nrows=0)
    except Exception as ex:  # noqa: BLE001
        return rel, [], f"заголовок: {ex}"

    if col_code not in header.columns or col_date not in header.columns:
        return rel, [], f"нет колонок {col_code}/{col_date}"

    try:
        df = pd.read_csv(
            path,
            sep=sep,
            encoding=encoding,
            dtype=str,
            usecols=[col_code, col_date],
            low_memory=False,
        )
    except Exception as ex:  # noqa: BLE001
        return rel, [], f"чтение: {ex}"

    hits: List[FileHit] = []
    code_series = df[col_code].fillna("").astype(str).str.strip()
    for code in codes:
        mask = code_series == code
        n = int(mask.sum())
        if n == 0:
            continue
        max_date = _max_contest_date(df.loc[mask, col_date])
        if max_date is None:
            continue
        hits.append(
            FileHit(
                code=code,
                path_abs=path,
                path_rel=rel,
                max_date=max_date,
                mtime=mtime,
                rows=n,
            )
        )
    return rel, hits, None


def choose_winners(
    codes_order: Sequence[str],
    hits_by_code: Dict[str, List[FileHit]],
) -> Dict[str, WinnerInfo]:
    """По каждому коду: max CONTEST_DATE, затем max mtime, затем max path_rel."""
    winners: Dict[str, WinnerInfo] = {}
    for code in codes_order:
        hits = hits_by_code.get(code) or []
        if not hits:
            continue
        best_date = max(h.max_date for h in hits)
        same = [h for h in hits if h.max_date == best_date]
        winner = max(same, key=lambda h: (h.mtime, h.path_rel))
        winners[code] = WinnerInfo(
            code=code,
            path_abs=winner.path_abs,
            path_rel=winner.path_rel,
            max_date=best_date,
            mtime=winner.mtime,
            files_found=len(hits),
            files_same_date=len(same),
        )
    return winners


def _mtime_str(mtime: float) -> str:
    return datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")


def print_winners_table(
    codes_order: Sequence[str],
    winners: Dict[str, WinnerInfo],
) -> None:
    print("— Сводка по турнирам —", flush=True)
    for code in codes_order:
        w = winners.get(code)
        if w is None:
            print(f"  {code}: не найден", flush=True)
            continue
        print(
            f"  {code}: max_date={w.max_date}  files={w.files_found}  "
            f"same_date={w.files_same_date}  winner={w.path_rel}  "
            f"mtime={_mtime_str(w.mtime)}",
            flush=True,
        )


def _not_found_row(code: str, columns: Sequence[str], col_code: str) -> Dict[str, str]:
    """Одна строка для турнира, которого нет ни в одном CSV."""
    row: Dict[str, str] = {c: PLACEHOLDER_DASH for c in columns}
    row[col_code] = code
    row[COL_SOURCE_FILE] = SOURCE_NOT_FOUND
    row[COL_FILES_FOUND] = PLACEHOLDER_DASH
    row[COL_FILES_MAX_DATE] = PLACEHOLDER_DASH
    return row


def _default_columns(col_code: str, col_date: str) -> List[str]:
    """Колонки, если ни один файл-победитель не прочитан (все коды не найдены)."""
    return [
        "MANAGER_PERSON_NUMBER",
        "CONTEST_CODE",
        col_code,
        col_date,
        "PLAN_VALUE",
        "FACT_VALUE",
        "priority_type",
        COL_FILES_FOUND,
        COL_FILES_MAX_DATE,
        COL_SOURCE_FILE,
    ]


def load_winner_rows(
    winners: Dict[str, WinnerInfo],
    codes_order: Sequence[str],
    sep: str,
    encoding: str,
    col_code: str,
    col_date: str,
    max_workers: int,
) -> pd.DataFrame:
    """
    Pass2: полное чтение файлов-победителей (параллельно по уникальным путям),
    фильтр строк, служебные колонки. Ненайденные коды — одна строка
    с PARCE_SOURCE_FILE=«НЕ ОБНАРУЖЕН REPORT» и «-» в остальных полях.
    Порядок блоков — как в tournament_codes.
    """
    by_file: Dict[Path, List[WinnerInfo]] = {}
    for code in codes_order:
        w = winners.get(code)
        if w is None:
            continue
        by_file.setdefault(w.path_abs, []).append(w)

    loaded: Dict[Path, pd.DataFrame] = {}
    columns: List[str] = []

    if by_file:
        def _read_file(path: Path) -> Tuple[Path, Optional[pd.DataFrame], Optional[str]]:
            try:
                df = pd.read_csv(path, sep=sep, encoding=encoding, dtype=str, low_memory=False)
                return path, df, None
            except Exception as ex:  # noqa: BLE001
                return path, None, str(ex)

        paths = list(by_file.keys())
        workers = min(max_workers, len(paths))
        print(
            f"— Pass2: чтение {len(paths)} файл(ов)-победителей (workers={workers}) —",
            flush=True,
        )

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(_read_file, p): p for p in paths}
            for fut in as_completed(futs):
                path, df, err = fut.result()
                codes_here = [w.code for w in by_file[path]]
                if err or df is None:
                    print(f"  ERROR {path.name}: {err}", file=sys.stderr, flush=True)
                    continue
                if col_code not in df.columns:
                    print(
                        f"  ERROR {path.name}: нет колонки {col_code}",
                        file=sys.stderr,
                        flush=True,
                    )
                    continue
                loaded[path] = df
                if not columns:
                    columns = list(df.columns) + [
                        COL_FILES_FOUND,
                        COL_FILES_MAX_DATE,
                        COL_SOURCE_FILE,
                    ]
                print(
                    f"  прочитан: {by_file[path][0].path_rel} → коды: {', '.join(codes_here)} "
                    f"(строк файла: {len(df)})",
                    flush=True,
                )

    if not columns:
        columns = _default_columns(col_code, col_date)
        print(
            "— Pass2: победителей нет, все коды будут с пометкой «НЕ ОБНАРУЖЕН REPORT» —",
            flush=True,
        )

    parts: List[pd.DataFrame] = []
    not_found_n = 0
    for code in codes_order:
        w = winners.get(code)
        if w is None:
            row = _not_found_row(code, columns, col_code)
            parts.append(pd.DataFrame([row], columns=columns))
            not_found_n += 1
            print(f"  {code}: {SOURCE_NOT_FOUND}", flush=True)
            continue
        df = loaded.get(w.path_abs)
        if df is None:
            row = _not_found_row(code, columns, col_code)
            parts.append(pd.DataFrame([row], columns=columns))
            not_found_n += 1
            print(f"  {code}: файл победителя не загружен → {SOURCE_NOT_FOUND}", flush=True)
            continue
        mask = df[col_code].fillna("").astype(str).str.strip() == code
        chunk = df.loc[mask].copy()
        if chunk.empty:
            row = _not_found_row(code, columns, col_code)
            parts.append(pd.DataFrame([row], columns=columns))
            not_found_n += 1
            print(f"  {code}: 0 строк в победителе → {SOURCE_NOT_FOUND}", flush=True)
            continue
        chunk[COL_FILES_FOUND] = w.files_found
        chunk[COL_FILES_MAX_DATE] = w.files_same_date
        chunk[COL_SOURCE_FILE] = w.path_rel
        # выровнять набор колонок
        for c in columns:
            if c not in chunk.columns:
                chunk[c] = PLACEHOLDER_DASH
        chunk = chunk[columns]
        parts.append(chunk)
        print(f"  {code}: строк {len(chunk)} ← {w.path_rel}", flush=True)

    if not_found_n:
        print(f"Pass2: не найдено в REPORT: {not_found_n}", flush=True)

    if not parts:
        return pd.DataFrame(columns=columns)
    return pd.concat(parts, ignore_index=True)


def run(config_path: Path) -> int:
    t0 = time.perf_counter()
    root = project_root()
    cfg = load_config(config_path)

    paths_cfg = cfg["paths"]
    report_root = resolve_path(root, str(paths_cfg.get("report_root", "IN/REPORT")))
    output_dir = resolve_path(root, str(paths_cfg.get("output_dir", "OUT/REPORT_FOLDER_PARCE")))
    name_tpl = str(
        paths_cfg.get("output_filename_template", "REPORT_folder_parce_{timestamp}.xlsx")
    )

    codes: List[str] = list(cfg["tournament_codes"])
    codes_set: Set[str] = set(codes)
    csv_cfg = cfg["csv"]
    sep = csv_cfg["sep"]
    encoding = csv_cfg["encoding"]
    col_code = csv_cfg["tournament_column"]
    col_date = csv_cfg["date_column"]
    glob_pat = csv_cfg["glob"]
    max_workers = int(cfg["performance"]["max_workers"])
    progress_every = int(cfg["logging"]["progress_every_files"])

    print("=" * 72, flush=True)
    print("folder_parce — сбор REPORT по TOURNAMENT_CODE", flush=True)
    print("=" * 72, flush=True)
    print(f"Конфиг:      {config_path}", flush=True)
    print(f"REPORT root: {report_root}", flush=True)
    print(f"Кодов:       {len(codes)}", flush=True)
    print(f"Workers:     {max_workers}", flush=True)

    if not report_root.is_dir():
        print(f"ERROR: каталог REPORT не найден: {report_root}", file=sys.stderr, flush=True)
        return 2

    files = discover_csv_files(report_root, glob_pat)
    print(f"CSV файлов:  {len(files)}", flush=True)
    if not files:
        print("ERROR: CSV не найдены", file=sys.stderr, flush=True)
        return 2

    print(
        f"— Pass1: индекс {col_code}+{col_date} "
        f"(workers={min(max_workers, len(files))}) —",
        flush=True,
    )
    hits_by_code: Dict[str, List[FileHit]] = {c: [] for c in codes}
    done = 0
    warn_count = 0

    with ThreadPoolExecutor(max_workers=min(max_workers, len(files))) as ex:
        futs = [
            ex.submit(
                scan_one_file,
                path,
                report_root,
                codes_set,
                sep,
                encoding,
                col_code,
                col_date,
            )
            for path in files
        ]
        for fut in as_completed(futs):
            rel, hits, err = fut.result()
            done += 1
            if err:
                warn_count += 1
                print(f"  [{done}/{len(files)}] WARN {rel}: {err}", flush=True)
            elif done == 1 or done % progress_every == 0 or done == len(files):
                n_hit = len(hits)
                extra = f"  (хитов кодов: {n_hit})" if n_hit else ""
                print(f"  [{done}/{len(files)}] {rel}{extra}", flush=True)
            for h in hits:
                hits_by_code[h.code].append(h)

    if warn_count:
        print(f"Pass1: предупреждений по файлам: {warn_count}", flush=True)

    winners = choose_winners(codes, hits_by_code)
    print_winners_table(codes, winners)

    out_df = load_winner_rows(
        winners,
        codes,
        sep=sep,
        encoding=encoding,
        col_code=col_code,
        col_date=col_date,
        max_workers=max_workers,
    )
    if out_df.empty:
        print("ERROR: не удалось сформировать строки для Excel", file=sys.stderr, flush=True)
        return 1

    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_name = name_tpl.replace("{timestamp}", ts)
    out_path = output_dir / out_name
    print(f"— Запись Excel: {out_path} ({len(out_df)} строк) —", flush=True)
    out_df.to_excel(out_path, index=False, engine="openpyxl")

    found_n = len(winners)
    missing_n = len(codes) - found_n
    elapsed = time.perf_counter() - t0
    print("— Итог —", flush=True)
    print(f"  Excel:      {out_path}", flush=True)
    print(f"  Строк:      {len(out_df)}", flush=True)
    print(f"  Турниров:   найдено {found_n}, не найдено {missing_n}, всего {len(codes)}", flush=True)
    print(f"  Wall-clock: {elapsed:.2f}s", flush=True)
    print("=" * 72, flush=True)
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Сбор REPORT по TOURNAMENT_CODE из IN/REPORT (см. config_folder_parce.json)"
    )
    parser.add_argument(
        "--config",
        type=str,
        default=str(default_config_path()),
        help="Путь к config_folder_parce.json",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        return run(Path(args.config).expanduser().resolve())
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as ex:
        print(f"ERROR: {ex}", file=sys.stderr, flush=True)
        return 2


if __name__ == "__main__":
    sys.exit(main())
