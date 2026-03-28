# -*- coding: utf-8 -*-
"""
Диагностическое логирование производительности: сквозное время запуска, счётчики вызовов,
длительность функций. Пишется в лог уровня DEBUG (файл); краткая сводка — INFO в конце прогона.

Использование:
  - в начале main после setup_logger: reset_run_timing()
  - на функции: @debug_timed() или @debug_timed(hot=True) для «горячих» функций
    (агрегация без лога на каждый вызов; детали — в итоговой сводке)
  - крупные этапы: with debug_phase("имя"): ...
  - отдельный файл Excel со сводкой: write_performance_statistics_excel (каталог OUT по дате).
"""
from __future__ import annotations

import atexit
import functools
import logging
import os
import threading
import time
from datetime import datetime
from contextlib import contextmanager
from typing import Any, Callable, Dict, List, Optional, TypeVar

# --- глобальное состояние одного прогона ---
_lock = threading.RLock()
_run_start_perf: float = 0.0
# ключ: "модуль.qualname" -> статистика
_stats: Dict[str, Dict[str, Any]] = {}
# Завершённые фазы (порядок = хронология окончания этапа) — для выгрузки в отдельный Excel
_phase_records: List[Dict[str, Any]] = []
_atexit_registered: bool = False
# глубина вложенности фаз (для отступа в логе)
_phase_depth = threading.local()

F = TypeVar("F", bound=Callable[..., Any])


def _ensure_depth() -> int:
    d = getattr(_phase_depth, "value", None)
    if d is None:
        _phase_depth.value = 0
        return 0
    return int(d)


def _indent() -> str:
    return "  " * _ensure_depth()


def reset_run_timing() -> None:
    """Сброс статистики и фиксация момента старта прогона (вызывать один раз в начале main)."""
    global _run_start_perf, _stats, _phase_records, _atexit_registered
    with _lock:
        _run_start_perf = time.perf_counter()
        _stats = {}
        _phase_records = []
        if not _atexit_registered:
            atexit.register(log_perf_summary)
            _atexit_registered = True


def run_elapsed_sec() -> float:
    """Секунды с момента reset_run_timing() (monotonic)."""
    with _lock:
        if _run_start_perf <= 0:
            return 0.0
        return time.perf_counter() - _run_start_perf


def _stat_key(fn: Callable[..., Any], override_name: Optional[str]) -> str:
    name = override_name or fn.__qualname__
    return f"{fn.__module__}.{name}"


def _record_call(key: str, duration_sec: float, hot: bool) -> None:
    with _lock:
        if key not in _stats:
            _stats[key] = {
                "count": 0,
                "total_sec": 0.0,
                "max_sec": 0.0,
                "min_sec": float("inf"),
                "hot": hot,
            }
        s = _stats[key]
        s["count"] += 1
        s["total_sec"] += duration_sec
        s["max_sec"] = max(s["max_sec"], duration_sec)
        s["min_sec"] = min(s["min_sec"], duration_sec)


def debug_timed(
    _fn: Optional[F] = None,
    *,
    hot: bool = False,
    name: Optional[str] = None,
    log_args_len: bool = False,
) -> Any:
    """
    Декоратор: DEBUG при входе/выходе, длительность, порядковый номер вызова за прогон.

    hot=True: не логировать каждый вызов (только агрегат в сводке) — для часто вызываемых функций.
    log_args_len: добавить в лог число позиционных и именованных аргументов (без значений).
    """

    def decorator(fn: F) -> F:
        key = _stat_key(fn, name)
        call_seq = {"n": 0}  # локальный счётчик для порядкового номера в логе (потокобезопасно через lock)

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with _lock:
                call_seq["n"] += 1
                seq = call_seq["n"]
            t0 = time.perf_counter()
            run_ms = run_elapsed_sec() * 1000.0
            extra = ""
            if log_args_len:
                extra = f" args={len(args)} kwargs={len(kwargs)}"
            if not hot:
                logging.debug(
                    f"{_indent()}[PERF] → вход «{key}» вызов#{seq} от старта прогона {run_ms:.1f} ms{extra}"
                )
            try:
                return fn(*args, **kwargs)
            finally:
                dt = time.perf_counter() - t0
                _record_call(key, dt, hot)
                run_ms_after = run_elapsed_sec() * 1000.0
                if not hot:
                    with _lock:
                        s = _stats.get(key, {})
                        c = s.get("count", seq)
                        tot = s.get("total_sec", dt)
                    logging.debug(
                        f"{_indent()}[PERF] ← выход «{key}» вызов#{seq} длительность={dt*1000:.2f} ms "
                        f"(сумма по функции {tot*1000:.2f} ms, всего вызовов {c}) от старта {run_ms_after:.1f} ms"
                    )

        return wrapper  # type: ignore[return-value]

    if _fn is not None:
        return decorator(_fn)
    return decorator


@contextmanager
def debug_phase(label: str) -> Any:
    """Контекстный менеджер: DEBUG старт/финиш фазы с длительностью и временем от начала прогона."""
    d0 = _ensure_depth()
    _phase_depth.value = d0 + 1
    t0 = time.perf_counter()
    run_ms = run_elapsed_sec() * 1000.0
    logging.debug(f"{_indent()}[PERF] [[ фаза «{label}» START (run+{run_ms:.1f} ms) ]]")
    try:
        yield
    finally:
        dt = time.perf_counter() - t0
        run_ms_after = run_elapsed_sec() * 1000.0
        logging.debug(
            f"{_indent()}[PERF] ]] фаза «{label}» END за {dt*1000:.2f} ms (run+{run_ms_after:.1f} ms) [["
        )
        with _lock:
            _phase_records.append(
                {
                    "label": label,
                    "duration_sec": dt,
                    "run_ms_start": run_ms,
                    "run_ms_end": run_ms_after,
                }
            )
        _phase_depth.value = max(0, d0)


def log_perf_summary() -> None:
    """
    Итоговая таблица: все учтённые функции, сортировка по убыванию суммарного времени.
    Вызывается через atexit; безопасно при пустой статистике.
    """
    try:
        _log_perf_summary_impl()
    except Exception as e:
        try:
            logging.error("[PERF] Не удалось вывести сводку производительности: %s", e)
        except Exception:
            pass


def _log_perf_summary_impl() -> None:
    with _lock:
        if _run_start_perf <= 0 or not _stats:
            return
        total_run = time.perf_counter() - _run_start_perf
        items: List[tuple[str, Dict[str, Any]]] = list(_stats.items())
    items.sort(key=lambda x: x[1]["total_sec"], reverse=True)

    lines_debug: List[str] = [
        "[PERF] ========== СВОДКА ПРОИЗВОДИТЕЛЬНОСТИ (полный прогон) ==========",
        f"[PERF] Общее wall-time (monotonic) прогона: {total_run*1000:.2f} ms ({total_run:.3f} s)",
        "[PERF] Функция | вызовов | сумма ms | среднее ms | min ms | max ms | hot",
        "[PERF] " + "-" * 88,
    ]
    top_for_info: List[str] = []
    for key, s in items:
        cnt = s["count"]
        tot = s["total_sec"]
        mn = s["min_sec"] if s["min_sec"] != float("inf") else 0.0
        mx = s["max_sec"]
        avg = tot / cnt if cnt else 0.0
        hot = "да" if s.get("hot") else "нет"
        lines_debug.append(
            f"[PERF] {key} | {cnt} | {tot*1000:.2f} | {avg*1000:.2f} | {mn*1000:.2f} | {mx*1000:.2f} | {hot}"
        )
        if len(top_for_info) < 8:
            top_for_info.append(f"{key}: {tot*1000:.0f} ms ×{cnt}")

    lines_debug.append("[PERF] ========== конец сводки ==========")

    for line in lines_debug:
        logging.debug(line)

    if top_for_info:
        logging.info(
            "[PERF] Топ по суммарному времени (подробности в лог-файле DEBUG): "
            + "; ".join(top_for_info)
        )

    # Дубликаты по числу вызовов: подсказка для анализа
    suspicious = [(k, v["count"]) for k, v in items if v["count"] > 50]
    if suspicious:
        suspicious.sort(key=lambda x: -x[1])
        logging.debug(
            "[PERF] Функции с большим числом вызовов (>50), возможны узкие места или избыточные вызовы: "
            + ", ".join(f"{k}×{c}" for k, c in suspicious[:25])
        )


def format_duration_ru(seconds: float) -> str:
    """
    Форматирует длительность для отчёта: «ХХ мин. YY сек ZZZ мс» (минуты без лидирующих нулей,
    секунды — две цифры 00–59, миллисекунды — три цифры).
    """
    if seconds < 0:
        seconds = 0.0
    total_ms = int(round(seconds * 1000.0))
    minutes = total_ms // 60000
    rem = total_ms % 60000
    secs = rem // 1000
    ms = rem % 1000
    return f"{minutes:02d} мин. {secs:02d} сек {ms:03d} мс"


def write_performance_statistics_excel(
    output_dir: str,
    *,
    program_started_at: Optional[str] = None,
    run_mode_label: Optional[str] = None,
) -> Optional[str]:
    """
    Создаёт файл ``STAT_FILE YYYY-MM-DD_HH-MM-SS.xlsx`` в указанном каталоге с листами:
    «Сводка» (общие сведения и время прогона), «Этапы» (фазы ``debug_phase``),
    «Функции» (агрегаты ``@debug_timed``). Время в человекочитаемом формате и дубли в секундах для сортировки.

    Returns:
        Полный путь к файлу или None, если таймер прогона не был запущен.
    """
    with _lock:
        if _run_start_perf <= 0:
            return None
        total_run_sec = time.perf_counter() - _run_start_perf
        phases = list(_phase_records)
        stat_items = list(_stats.items())

    try:
        import pandas as pd
    except ImportError:
        logging.warning("[PERF] pandas не установлен — файл STAT_FILE *.xlsx не создан")
        return None

    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    out_path = os.path.join(output_dir, f"STAT_FILE {ts}.xlsx")

    stat_items.sort(key=lambda x: x[1]["total_sec"], reverse=True)
    finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # --- лист «Сводка»: двухколоночная таблица параметр / значение ---
    summary_rows: List[Dict[str, Any]] = [
        {"Параметр": "Момент формирования файла (wall-clock)", "Значение": finished_at},
        {
            "Параметр": "Общее время прогона (perf_counter)",
            "Значение": format_duration_ru(total_run_sec),
        },
        {
            "Параметр": "Общее время прогона, сек",
            "Значение": round(total_run_sec, 6),
        },
        {"Параметр": "Число зафиксированных этапов", "Значение": len(phases)},
        {"Параметр": "Число учтённых функций (@debug_timed)", "Значение": len(stat_items)},
    ]
    if program_started_at:
        summary_rows.insert(0, {"Параметр": "Старт программы (wall-clock)", "Значение": program_started_at})
    if run_mode_label:
        summary_rows.insert(1 if program_started_at else 0, {"Параметр": "Режим запуска", "Значение": run_mode_label})
    df_summary = pd.DataFrame(summary_rows)

    # --- лист «Этапы» ---
    phase_rows: List[Dict[str, Any]] = []
    for i, ph in enumerate(phases, start=1):
        d_sec = float(ph["duration_sec"])
        phase_rows.append(
            {
                "№": i,
                "Этап": str(ph["label"]),
                "Длительность": format_duration_ru(d_sec),
                "Длительность_сек": round(d_sec, 6),
                "До старта этапа от начала прогона": format_duration_ru(float(ph["run_ms_start"]) / 1000.0),
                "До конца этапа от начала прогона": format_duration_ru(float(ph["run_ms_end"]) / 1000.0),
                "До старта_сек": round(float(ph["run_ms_start"]) / 1000.0, 6),
                "До конца_сек": round(float(ph["run_ms_end"]) / 1000.0, 6),
            }
        )
    df_phases = pd.DataFrame(phase_rows) if phase_rows else pd.DataFrame(
        columns=[
            "№",
            "Этап",
            "Длительность",
            "Длительность_сек",
            "До старта этапа от начала прогона",
            "До конца этапа от начала прогона",
            "До старта_сек",
            "До конца_сек",
        ]
    )

    # --- лист «Функции» ---
    denom = total_run_sec if total_run_sec > 1e-12 else 1e-12
    func_rows: List[Dict[str, Any]] = []
    for j, (key, s) in enumerate(stat_items, start=1):
        cnt = int(s["count"])
        tot = float(s["total_sec"])
        mn = float(s["min_sec"]) if s["min_sec"] != float("inf") else 0.0
        mx = float(s["max_sec"])
        avg = tot / cnt if cnt else 0.0
        share_pct = round((tot / denom) * 100.0, 3)
        func_rows.append(
            {
                "№": j,
                "Функция": key,
                "Вызовов": cnt,
                "Суммарное время": format_duration_ru(tot),
                "Среднее за вызов": format_duration_ru(avg),
                "Минимум за вызов": format_duration_ru(mn),
                "Максимум за вызов": format_duration_ru(mx),
                "Горячая (hot)": "да" if s.get("hot") else "нет",
                "Доля от общего времени прогона, %": share_pct,
                "Суммарно_сек": round(tot, 6),
                "Среднее_сек": round(avg, 6),
                "Min_сек": round(mn, 6),
                "Max_сек": round(mx, 6),
            }
        )
    df_funcs = pd.DataFrame(func_rows) if func_rows else pd.DataFrame(
        columns=[
            "№",
            "Функция",
            "Вызовов",
            "Суммарное время",
            "Среднее за вызов",
            "Минимум за вызов",
            "Максимум за вызов",
            "Горячая (hot)",
            "Доля от общего времени прогона, %",
            "Суммарно_сек",
            "Среднее_сек",
            "Min_сек",
            "Max_сек",
        ]
    )

    try:
        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            df_summary.to_excel(writer, index=False, sheet_name="Сводка")
            df_phases.to_excel(writer, index=False, sheet_name="Этапы")
            df_funcs.to_excel(writer, index=False, sheet_name="Функции")
        logging.info(f"[PERF] Файл статистики времени: {out_path}")
        return out_path
    except Exception as ex:
        logging.warning(f"[PERF] Не удалось записать {out_path}: {ex}")
        return None
