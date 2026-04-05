# -*- coding: utf-8 -*-
"""
Краткий вывод в консоль (stdout): этапы работы, сводки без длинных строк.
Подробные сообщения остаются в лог-файле (уровень DEBUG/INFO на файловом обработчике).
Только стандартная библиотека Python 3.10 (shutil, sys, textwrap).
"""

from __future__ import annotations

import shutil
import sys
import textwrap
from typing import Any, Dict, List, Optional, Set

# Счётчик завершённых верхнеуровневых фаз (depth==0) для «[N] ✓» и прогресс-бара
_phase_done_count: List[int] = [0]
# Ожидаемое число таких фаз за прогон (задаётся из main по RUN_MODE); None — полоса не рисуется
_phase_total_expected: List[Optional[int]] = [None]


def expected_phases_for_run_flags(
    source_only_exit: bool,
    write_source: bool,
    write_main: bool,
    write_consistency_file: bool,
    consistency_early: bool,
) -> int:
    """
    Число верхнеуровневых debug_phase в main_impl для полосы прогресса.
    Синхронизировано с ветвлениями run_outputs в main.
    """
    if source_only_exit:
        return 2  # 01 + source write
    n = 1  # 01
    if write_source:
        n += 1
    n += 1  # 02
    n += 1  # 03
    if consistency_early:
        n += 1  # 04
        return n
    if write_main:
        n += 2  # 05, 06
        if write_consistency_file:
            n += 1  # 07
    return n


def expected_phases_for_run_mode(run_mode: int) -> int:
    """Устаревший вариант по числовому коду 1–4 (если вызывается из стороннего кода)."""
    if run_mode == 2:
        return 2
    if run_mode == 4:
        return 4
    if run_mode == 3:
        return 5
    if run_mode == 1:
        return 7
    return 5


def set_phase_progress_total(total: Optional[int]) -> None:
    """
    Задать знаменатель для прогресс-бара (завершённые этапы / total).
    None или total<=0 — после каждого этапа только номер и время, без полосы.
    """
    if total is None or total <= 0:
        _phase_total_expected[0] = None
    else:
        _phase_total_expected[0] = int(total)


def reset_phase_counter() -> None:
    """Сброс счётчика фаз (вызывать в начале main вместе с reset_run_timing)."""
    _phase_done_count[0] = 0
    _phase_total_expected[0] = None


def terminal_width(fallback: int = 80) -> int:
    """Ширина терминала для обрезки строк; при ошибке — fallback."""
    try:
        w = shutil.get_terminal_size(fallback=(fallback, 24)).columns
        return max(40, min(w, 200))
    except OSError:
        return fallback


def print_wrapped(text: str, width: Optional[int] = None, indent: str = "  ", sub_indent: str = "    ") -> None:
    """
    Вывод многострочного текста с переносами по ширине терминала (без усечения «…»).
    """
    w = width if width is not None else terminal_width()
    w = max(40, w)
    s = " ".join(str(text).split())
    if not s:
        return
    lines = textwrap.wrap(s, width=w, break_long_words=True, break_on_hyphens=False)
    if not lines:
        return
    print(f"{indent}{lines[0]}", flush=True)
    for ln in lines[1:]:
        print(f"{sub_indent}{ln}", flush=True)


def _truncate(text: str, max_len: int) -> str:
    """Одна строка без переносов; длиннее max_len — обрезка с «…»."""
    s = " ".join(str(text).split())
    if len(s) <= max_len:
        return s
    if max_len <= 1:
        return "…"
    return s[: max_len - 1] + "…"


def _fmt_duration(sec: float) -> str:
    if sec >= 100:
        return f"{sec:.0f}s"
    if sec >= 10:
        return f"{sec:.1f}s"
    return f"{sec:.2f}s"


def print_banner(title: str) -> None:
    """Заголовок блока в консоли."""
    w = terminal_width()
    line = "=" * min(w, 72)
    print(line, flush=True)
    print(_truncate(title, w - 4).center(min(w, 72)), flush=True)
    print(line, flush=True)


def on_phase_start(label: str, depth: int = 0) -> None:
    """Хук старта этапа (debug_phase): короткая строка «работаем над …» (только верхний уровень)."""
    if depth > 0:
        return
    w = terminal_width()
    short = _truncate(label, w - 8)
    print(f"  … {short}", flush=True)


def on_phase_end(label: str, duration_sec: float, depth: int = 0) -> None:
    """
    Хук завершения этапа: прогресс-бар (если задан total), номер, время, усечённое имя.
    Вложенные фазы (depth>0) в консоль не выводим — чтобы не путать счётчик и полосу.
    """
    if depth > 0:
        return
    _phase_done_count[0] += 1
    done = _phase_done_count[0]
    total = _phase_total_expected[0]
    w = terminal_width()
    short = _truncate(label, w - 36)
    bar_part = ""
    if total is not None and total > 0:
        bar = render_progress_bar(done, total, width=18)
        bar_part = f"{bar} {done}/{total}  "
    line = f"  {bar_part}[{done:>2}] ✓ {_fmt_duration(duration_sec):>7}  {short}"
    print(_truncate(line, w), flush=True)


def print_consistency_summary(results: List[Dict[str, Any]]) -> None:
    """
    Сводка проверок консистентности: обзор (правила, листы, сумма нарушений),
    таблица по типам; при нарушениях — краткая детализация по правилам.
    Длинные sample не выводятся — только в лог.
    """
    w = terminal_width()
    print("— Консистентность (сводка) —", flush=True)
    if not results:
        print("  Проверки не выполнялись (в конфиге нет правил или блок пропущен).", flush=True)
        return

    included = [r for r in results if r.get("include_in_summary", True)]
    if not included:
        print("  Нет результатов для свода (все правила с include_in_summary: false).", flush=True)
        return

    # Уникальные листы, на которых сработали проверки (поле sheet у результата)
    sheets_set: Set[str] = set()
    for r in included:
        sh = str(r.get("sheet") or "").strip()
        if sh:
            sheets_set.add(sh)

    n_rules = len(included)
    n_sheets = len(sheets_set)
    total_violations = sum(int(r.get("violations") or 0) for r in included)
    with_v = [r for r in included if int(r.get("violations") or 0) > 0]
    n_rules_with_violations = len(with_v)

    print_wrapped(
        f"Оценка: правил в отчёте — {n_rules}; листов с колонками проверок — {n_sheets}.",
        width=w,
    )
    if total_violations == 0:
        print_wrapped(
            "Проблем с консистентностью не обнаружено: суммарно нарушений по счётчикам правил — 0.",
            width=w,
        )
    else:
        print_wrapped(
            f"Выявлено нарушений (сумма по правилам): {total_violations}; "
            f"правил с нарушениями — {n_rules_with_violations}.",
            width=w,
        )

    # Таблица по типам проверки: сколько правил, сколько листов, сколько нарушений
    by_type: Dict[str, List[Dict[str, Any]]] = {}
    for r in included:
        t = str(r.get("type", "?"))
        by_type.setdefault(t, []).append(r)

    type_keys = sorted(by_type.keys())
    w_type = max(len("Тип проверки"), max((len(k) for k in type_keys), default=0))
    hdr = f"  {'Тип проверки':<{w_type}}  {'Правил':>7}  {'Листов':>7}  {'Нарушений':>11}  Примечание"
    print(hdr, flush=True)
    print(f"  {'-' * w_type}  -------  -------  -----------  ----------", flush=True)
    for t in type_keys:
        lst = by_type[t]
        st_local: Set[str] = set()
        for x in lst:
            s = str(x.get("sheet") or "").strip()
            if s:
                st_local.add(s)
        nv_t = sum(int(x.get("violations") or 0) for x in lst)
        note = "OK" if nv_t == 0 else f"есть ({nv_t})"
        print(
            f"  {t:<{w_type}}  {len(lst):>7}  {len(st_local):>7}  {nv_t:>11}  {note}",
            flush=True,
        )

    if not with_v:
        print_wrapped("Примеры строк и полный разбор — в лог-файле (DEBUG).", width=w)
        return

    # Детализация: только правила с нарушениями, по типам (длинные строки переносим)
    print("  — По правилам с нарушениями —", flush=True)
    by_type_v: Dict[str, List[Dict[str, Any]]] = {}
    for r in with_v:
        tp = str(r.get("type", "?"))
        by_type_v.setdefault(tp, []).append(r)
    for t in sorted(by_type_v.keys()):
        rows = by_type_v[t]
        total_t = sum(int(x.get("violations") or 0) for x in rows)
        sheets_t = sorted({str(x.get("sheet", "")) for x in rows if x.get("sheet")})
        sh_line = ", ".join(sheets_t)
        print_wrapped(f"· {t}: нарушений {total_t}; листы: {sh_line}", width=w, indent="    ", sub_indent="      ")
        for r in rows[:8]:
            cid = str(r.get("check_id", ""))
            sh_one = str(r.get("sheet", ""))
            col = str(r.get("column_on_sheet", ""))
            nv = int(r.get("violations") or 0)
            line = f"{cid} | {sh_one} | {col} | ×{nv}"
            print_wrapped(line, width=w, indent="      ", sub_indent="        ")
        if len(rows) > 8:
            print(f"      … ещё правил с нарушениями по типу «{t}»: {len(rows) - 8}", flush=True)
    print_wrapped("Детали и примеры значений — в лог-файле (DEBUG).", width=w)


def print_phases_table(phases: List[Dict[str, Any]]) -> None:
    """Таблица этапов: имя (усечённое) и длительность."""
    if not phases:
        return
    w = terminal_width()
    print(_truncate("— Этапы (время) —", w), flush=True)
    total = 0.0
    for p in phases:
        sec = float(p.get("duration_sec", 0))
        total += sec
        lab = _truncate(str(p.get("label", "")), w - 14)
        print(f"  {_fmt_duration(sec):>8}  {lab}", flush=True)
    print(f"  {'Σ':>8}  {_fmt_duration(total)}  (сумма этапов)", flush=True)


def print_top_functions(top: List[tuple]) -> None:
    """
    top: список (короткое_имя, total_sec, count).
    """
    if not top:
        return
    w = terminal_width()
    print(_truncate("— Топ функций по суммарному времени (@debug_timed) —", w), flush=True)
    for name, tot_sec, cnt in top[:8]:
        nm = _truncate(name, w - 22)
        print(f"  {_fmt_duration(tot_sec):>8}  ×{cnt:<5}  {nm}", flush=True)


def _split_sheet_summary_line(line: str) -> tuple[str, str]:
    """
    Разбор строки вида «ИМЯ_ЛИСТА: 200 строк» или «лист: ошибка» из summary в main_impl.
    Если двоеточия нет — вся строка в первой колонке.
    """
    s = str(line).strip()
    if ": " in s:
        name, rest = s.split(": ", 1)
        return name.strip(), rest.strip()
    if ":" in s:
        name, rest = s.split(":", 1)
        return name.strip(), rest.strip()
    return s, ""


def print_data_processing_summary(
    files_processed: int,
    rows_total: int,
    summary_parts: Optional[List[str]] = None,
) -> None:
    """
    Сколько файлов/строк обработано; по листам — таблица (имя | строки/примечание), без обрезки.
    """
    w = terminal_width()
    print(_truncate("— Обработка входных данных —", w), flush=True)
    print(f"  Файлов: {files_processed}; строк (сумма по листам): {rows_total}", flush=True)
    if not summary_parts:
        return
    rows: List[tuple[str, str]] = [_split_sheet_summary_line(p) for p in summary_parts]
    col1_w = max(len("Лист"), max((len(a) for a, _ in rows), default=0))
    max_rest = max((len(b) for _, b in rows), default=0)
    # Линия-разделитель: разумная длина; сами значения во второй колонке печатаются полностью
    sep2_len = max(len("Строки / примечание"), min(max_rest, 72))
    print(f"  {'Лист':<{col1_w}}  |  Строки / примечание", flush=True)
    print(f"  {'-' * col1_w}  |  {'-' * sep2_len}", flush=True)
    for name, rest in rows:
        print(f"  {name:<{col1_w}}  |  {rest}", flush=True)


def print_validation_and_csv_compact(
    validation_report: List[Dict[str, Any]],
    csv_mismatch_report: Optional[List[Dict[str, Any]]] = None,
) -> None:
    """Кратко: длина полей и расхождения CSV; без многострочных примеров."""
    w = terminal_width()
    print(_truncate("— Длина полей и CSV —", w), flush=True)
    if validation_report:
        tot = sum(int(r.get("n_violations", 0) or 0) for r in validation_report)
        print(f"  Отклонения длины полей: правил с нарушениями {len(validation_report)}, строк ~{tot}", flush=True)
        for r in validation_report[:6]:
            sh = _truncate(str(r.get("sheet", "")), 20)
            rc = _truncate(str(r.get("result_column", "")), 30)
            nv = r.get("n_violations", 0)
            print(f"    · {sh} / {rc} … ×{nv}", flush=True)
        if len(validation_report) > 6:
            print(f"    … ещё {len(validation_report) - 6} лист(ов)", flush=True)
    else:
        print("  Отклонения длины полей: нет", flush=True)

    csv_mismatch_report = csv_mismatch_report or []
    if csv_mismatch_report:
        print(f"  Расхождения числа колонок CSV: записей {len(csv_mismatch_report)}", flush=True)
        for r in csv_mismatch_report[:5]:
            sh = _truncate(str(r.get("sheet", "")), 24)
            fn = _truncate(str(r.get("file", "")), 28)
            print(f"    · {fn} → {sh}", flush=True)
        if len(csv_mismatch_report) > 5:
            print(f"    … ещё {len(csv_mismatch_report) - 5}", flush=True)
    else:
        print("  Расхождения числа колонок CSV: нет", flush=True)


def print_paths_and_total_time(
    output_excel: Optional[str],
    log_file: Optional[str],
    wall_clock_seconds: float,
) -> None:
    w = terminal_width()
    print(_truncate("— Итог —", w), flush=True)
    if output_excel:
        print(f"  Excel: {_truncate(output_excel, w - 10)}", flush=True)
    if log_file:
        print(f"  Лог:   {_truncate(log_file, w - 10)}", flush=True)
    print(f"  Wall-clock ~ {_fmt_duration(wall_clock_seconds)}", flush=True)


def render_progress_bar(done: int, total: int, width: int = 24) -> str:
    """Полоса из символов # и -; только stdlib. При total<=0 — пусто."""
    if total <= 0:
        return "[" + "-" * width + "]"
    done = max(0, min(done, total))
    filled = int(round(width * done / total))
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def stderr_message(lines: List[str]) -> None:
    """Критические сообщения пользователю в stderr."""
    for ln in lines:
        print(ln, file=sys.stderr, flush=True)
