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
    write_manager_stats: bool = False,
    manager_stats_early: bool = False,
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
    if manager_stats_early:
        n += 1  # 08 manager_stats
        return n
    if write_main:
        n += 2  # 05, 06
        if write_consistency_file:
            n += 1  # 07
        if write_manager_stats:
            n += 1  # 08
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


def _consistency_rule_map(rules: Optional[List[Dict[str, Any]]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    if not rules:
        return out
    for rule in rules:
        rid = str(rule.get("id") or "").strip()
        if rid:
            out[rid] = rule
    return out


def _consistency_column_widths(width: int) -> tuple[int, int, int]:
    """Ширины колонок таблицы OK: лист, ключ/поля, суть проверки."""
    usable = max(40, width) - 4
    w_sheet = max(10, min(24, usable // 5))
    w_keys = max(12, min(32, usable // 3))
    w_sum = max(16, usable - w_sheet - w_keys - 4)
    return w_sheet, w_keys, w_sum


def _print_consistency_ok_table(
    included: List[Dict[str, Any]],
    rule_by_id: Dict[str, Dict[str, Any]],
    width: int,
) -> None:
    """Построчный список пройденных проверок, сгруппированный по типу проверки."""
    from src.consistency_checks import consistency_check_line_parts

    by_type: Dict[str, List[tuple[Dict[str, Any], Dict[str, str]]]] = {}
    type_labels: Dict[str, str] = {}
    for r in included:
        parts = consistency_check_line_parts(r, rule_by_id.get(str(r.get("check_id") or "")))
        tkey = str(parts.get("type") or "?")
        type_labels[tkey] = str(parts.get("type_label") or tkey)
        by_type.setdefault(tkey, []).append((r, parts))

    w_sheet, w_keys, w_sum = _consistency_column_widths(width - 4)
    indent = "    "
    hdr = (
        f"{indent}{'Лист':<{w_sheet}}  {'Ключ / поля':<{w_keys}}  "
        f"{'Проверка':<{w_sum}}  Итог"
    )
    sep = f"{indent}{'-' * w_sheet}  {'-' * w_keys}  {'-' * w_sum}  ----"

    type_order = sorted(by_type.keys(), key=lambda k: type_labels.get(k, k).casefold())
    for tkey in type_order:
        rows = by_type[tkey]
        label = _truncate(type_labels[tkey], max(20, width - 16))
        print(f"  ▸ {label} ({len(rows)})", flush=True)
        print(hdr, flush=True)
        print(sep, flush=True)
        for r, parts in rows:
            sheet = _truncate(parts["sheet"], w_sheet)
            keys = _truncate(parts["keys"] or "—", w_keys)
            check_label = (
                str(r.get("name") or "").strip()
                or parts["summary"]
                or parts["check_id"]
                or "?"
            )
            summary = _truncate(check_label, w_sum)
            print(
                f"{indent}{sheet:<{w_sheet}}  {keys:<{w_keys}}  {summary:<{w_sum}}  OK",
                flush=True,
            )
        print(flush=True)


def _print_consistency_violations(
    with_v: List[Dict[str, Any]],
    rule_by_id: Dict[str, Dict[str, Any]],
    width: int,
    *,
    max_samples_per_rule: int = 6,
) -> None:
    """Детализация только нарушенных проверок (без строк OK)."""
    from src.consistency_checks import consistency_check_line_parts

    for r in with_v:
        cid = str(r.get("check_id") or "").strip()
        rule = rule_by_id.get(cid)
        parts = consistency_check_line_parts(r, rule)
        nv = int(r.get("violations") or 0)
        err = str(r.get("error") or "").strip()
        display_name = str(r.get("name") or "").strip() or cid or parts["type"] or "?"
        head = f"✗ {display_name}"
        if cid and cid not in display_name:
            head = f"{head} [{cid}]"
        print(head, flush=True)
        essence = str(parts.get("summary") or "").strip()
        if essence and essence != display_name:
            print_wrapped(f"Суть: {essence}", width=width, indent="    ", sub_indent="    ")
        print_wrapped(
            f"Лист: {parts['sheet']}; ключ/поля: {parts['keys'] or '—'}",
            width=width,
            indent="    ",
            sub_indent="    ",
        )
        if parts["column_on_sheet"]:
            print_wrapped(
                f"Колонка на листе: {parts['column_on_sheet']}",
                width=width,
                indent="    ",
                sub_indent="    ",
            )
        if err:
            print_wrapped(f"Ошибка выполнения: {err}", width=width, indent="    ", sub_indent="    ")
        elif nv > 0:
            print(f"    Нарушений: {nv}", flush=True)
            sample = r.get("sample") or []
            if sample:
                print("    Примеры:", flush=True)
                for item in sample[:max_samples_per_rule]:
                    print_wrapped(str(item), width=width, indent="      ", sub_indent="        ")
                rest = len(sample) - max_samples_per_rule
                if rest > 0:
                    more = int(nv) - len(sample[:max_samples_per_rule])
                    if more > rest:
                        print(
                            f"      … ещё примеров в sample: {rest}; всего нарушений: {nv}",
                            flush=True,
                        )
                    else:
                        print(f"      … ещё примеров: {rest}", flush=True)
            else:
                print_wrapped(
                    "Конкретные строки не попали в sample — см. колонку результата на листе или лист CONSISTENCY.",
                    width=width,
                    indent="      ",
                    sub_indent="        ",
                )
        print(flush=True)


def print_consistency_summary(
    results: List[Dict[str, Any]],
    rules: Optional[List[Dict[str, Any]]] = None,
) -> None:
    """
    Сводка проверок консистентности в консоль.
    При OK — построчно: лист, ключ, суть проверки, итог.
    При нарушениях — только проблемные правила с деталями (лист, ключ, колонка, примеры).
    """
    w = terminal_width()
    print("— Консистентность —", flush=True)
    if not results:
        print("  Проверки не выполнялись (в конфиге нет правил или блок пропущен).", flush=True)
        return

    rule_by_id = _consistency_rule_map(rules)
    included = [r for r in results if r.get("include_in_summary", True)]
    if not included:
        print("  Нет результатов для свода (все правила с include_in_summary: false).", flush=True)
        return

    n_rules = len(included)
    total_violations = sum(int(r.get("violations") or 0) for r in included)
    with_v = [
        r for r in included
        if int(r.get("violations") or 0) > 0 or bool(str(r.get("error") or "").strip())
    ]

    if not with_v:
        print_wrapped(
            f"Проблем не обнаружено: {n_rules} проверок, нарушений — 0.",
            width=w,
        )
        _print_consistency_ok_table(included, rule_by_id, w)
        print_wrapped(
            "Подробные примеры строк — в debug-логе (logging.level=DEBUG).",
            width=w,
        )
        return

    print_wrapped(
        f"Найдены ошибки консистентности: {total_violations} нарушений "
        f"в {len(with_v)} из {n_rules} проверок.",
        width=w,
    )
    _print_consistency_violations(with_v, rule_by_id, w)
    print_wrapped(
        "Полный список — лист CONSISTENCY в выходном файле; примеры строк — в debug-логе.",
        width=w,
    )


def print_manager_stats_summary(unique_tabs: int, output_path: str) -> None:
    """Краткая сводка по файлу статистики менеджеров (табельные номера)."""
    w = terminal_width()
    print("— Статистика менеджеров (табельные) —", flush=True)
    print_wrapped(
        f"Уникальных табельных номеров: {unique_tabs}.",
        width=w,
    )
    if output_path:
        print_wrapped(f"Файл: {output_path}", width=w)


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


def print_input_archive_sqlite_report(
    console_mode: str,
    db_display: str,
    stats: Dict[str, Any],
    events: List[Dict[str, Any]],
) -> None:
    """
    Сводка по записи сырых CSV в архивную SQLite (после run_input_archive_sqlite).

    console_mode (config input_archive_sqlite.reporting.console):
      off — ничего; summary — только заголовок и счётчики;
      normal — плюс таблица по каждому обработанному листу;
      verbose — плюс размер файла, префикс SHA, пояснения.
    """
    mode = str(console_mode or "normal").lower().strip()
    if mode in ("0", "off", "none", "no", "false"):
        return

    w = terminal_width()
    print(_truncate("— Архив входных CSV (SQLite) —", w), flush=True)
    # Путь к БД не усечём: полный относительный путь от корня проекта, перенос по ширине терминала
    db_s = str(db_display or "").strip() or "—"
    print("  Файл БД (от корня проекта):", flush=True)
    for ln in textwrap.wrap(db_s, width=max(40, w - 6), break_long_words=True, break_on_hyphens=False):
        print(f"    {ln}", flush=True)
    print(
        f"  Итог: новых снимков {int(stats.get('ingested', 0))}; "
        f"без изменений {int(stats.get('unchanged', 0))}; "
        f"реактивация {int(stats.get('reactivated', 0))}; "
        f"дозапись SHA {int(stats.get('sha_backfill', 0))}; "
        f"пропуск (только первый снимок) {int(stats.get('skipped_first', 0))}; "
        f"ошибок доступа {int(stats.get('errors', 0))}; "
        f"вне архива по конфигу {int(stats.get('not_requested', 0))}; "
        f"нет данных {int(stats.get('no_payload', 0))}.",
        flush=True,
    )
    if mode in ("1", "summary", "brief", "short"):
        return

    verbose = mode in ("3", "verbose", "debug", "detail", "full")
    if not events:
        print("  Нет строк отчёта по листам.", flush=True)
        return

    # Табличный вид: имя листа и число строк в фиксированных колонках, описание — дальше
    print("  — По листам —", flush=True)
    col_sheet = min(42, max(22, w // 3))
    hdr = f"  {'Лист':<{col_sheet}} {'Строки':<9} Примечание"
    print(hdr[:w], flush=True)
    print("  " + "-" * min(w - 2, col_sheet + 9 + 24), flush=True)
    for e in events:
        sh = str(e.get("sheet", "") or "")
        lbl = str(e.get("label", "") or "")
        rows = e.get("rows")
        rows_s = "—" if rows is None else str(int(rows))
        sh_disp = sh if len(sh) <= col_sheet else sh[: max(1, col_sheet - 1)] + "…"
        rest_w = max(20, w - col_sheet - 14)
        lbl_short = lbl if len(lbl) <= rest_w else lbl[: max(1, rest_w - 1)] + "…"
        line1 = f"  {sh_disp:<{col_sheet}} {rows_s:<9} {lbl_short}"
        print(line1[:w], flush=True)
        if verbose:
            sz = e.get("size")
            sz_s = "—" if sz is None else str(int(sz))
            sha16 = e.get("sha16") or ""
            sha_s = (str(sha16) + "…") if sha16 else "—"
            snap = e.get("snapshot_id")
            snap_s = "—" if snap is None else str(int(snap))
            extra = str(e.get("extra") or "").strip()
            line2 = f"      size={sz_s}  snapshot={snap_s}  sha16={sha_s}"
            if extra:
                line2 += f"  |  {extra}"
            print(_truncate(line2, w), flush=True)


def print_input_archive_row_report(
    console_mode: str,
    db_display: str,
    stats: Dict[str, Any],
    events: List[Dict[str, Any]],
) -> None:
    """
    Сводка построчного архива SQLite v2 (run_input_archive_sqlite_v2).
    """
    mode = str(console_mode or "normal").lower().strip()
    if mode in ("0", "off", "none", "no", "false"):
        return

    w = terminal_width()
    print(_truncate("— Архив входных CSV (SQLite v2, построчно) —", w), flush=True)
    db_s = str(db_display or "").strip() or "—"
    print("  Файл БД (от корня проекта):", flush=True)
    for ln in textwrap.wrap(db_s, width=max(40, w - 6), break_long_words=True, break_on_hyphens=False):
        print(f"    {ln}", flush=True)
    print(
        f"  Итог: новых ключей в БД {int(stats.get('new', 0))}; "
        f"изменённых строк {int(stats.get('changed', 0))}; "
        f"без изменений {int(stats.get('unchanged', 0))}; "
        f"неактуальных (inactive) {int(stats.get('inactive', 0))}; "
        f"файлов без изменений (SHA) {int(stats.get('file_unchanged', 0))}; "
        f"ошибок ключа {int(stats.get('key_errors', 0))}; "
        f"ошибок {int(stats.get('errors', 0))}; "
        f"нет ключа в config {int(stats.get('no_row_key', 0))}.",
        flush=True,
    )
    print(
        "  (новых = ключ строки впервые в БД; изменённых = тот же ключ, другое содержимое CSV)",
        flush=True,
    )
    if mode in ("1", "summary", "brief", "short"):
        return

    verbose = mode in ("3", "verbose", "debug", "detail", "full")
    if not events:
        print("  Нет строк отчёта по листам.", flush=True)
        return

    print("  — По листам —", flush=True)
    col_sheet = min(42, max(22, w // 3))
    print(f"  {'Лист':<{col_sheet}} {'Строки':<9} Примечание"[:w], flush=True)
    print("  " + "-" * min(w - 2, col_sheet + 9 + 24), flush=True)
    for e in events:
        sh = str(e.get("sheet", "") or "")
        lbl = str(e.get("label", "") or "")
        rows = e.get("rows")
        rows_s = "—" if rows is None else str(int(rows))
        sh_disp = sh if len(sh) <= col_sheet else sh[: max(1, col_sheet - 1)] + "…"
        rest_w = max(20, w - col_sheet - 14)
        lbl_short = lbl if len(lbl) <= rest_w else lbl[: max(1, rest_w - 1)] + "…"
        print(f"  {sh_disp:<{col_sheet}} {rows_s:<9} {lbl_short}"[:w], flush=True)
        if verbose:
            extra = str(e.get("extra") or "").strip()
            if extra:
                print(_truncate(f"      {extra}", w), flush=True)
