# -*- coding: utf-8 -*-
"""
Генерация Profile_GP_LOAD_AutoRun.js рядом с Excel (OUT/YYYY/DD-MM).
Табельные — из MANAGER_STATS TAB_NUMBERS, у которых не заполнены поля из profile_gp_load.missing_columns
(ФИО, ТБ/ГОСБ, роль, email).
"""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

from src.leaders_for_admin_auto_js import manager_stats_only_in_run_outputs
from src.manager_stats import _cell_str, is_enrich_value_missing, merge_manager_stats_config

DEFAULT_JS_FILENAME = "Profile_GP_LOAD_AutoRun.js"
DEFAULT_TEMPLATE_FILENAME = "Profile_GP_LOAD_file.js"
DEFAULT_PROFILE_MISSING_COLUMNS: List[str] = [
    "Фамилия",
    "Имя",
    "ТБ",
    "ГОСБ",
    "Код роли",
    "Наименование Роли",
    "Email Sigma",
    "Email Alpha",
]
DEFAULT_PROFILE_JS_MISSING_COLUMNS: List[str] = [
    "Фамилия",
    "Имя",
    "ТБ",
    "ГОСБ",
    "Код роли",
]
PANEL_SECTION_MARKER = (
    "// =============================================================================\n"
    "// ЗАГРУЗКА ТН ИЗ ФАЙЛА И ЗАПУСК СБОРА"
)

AUTO_HEADER = """// =============================================================================
// Profile_GP_LOAD_AutoRun.js — автовыгрузка профилей по табельным
// =============================================================================
// Сгенерировано: {{GENERATED_AT}}
// Источник: MANAGER_STATS TAB_NUMBERS — табельные без полей: {{MISSING_COLS}}
// ТН в списке: {{TAB_COUNT}}
//
// Использование: DevTools → Console на странице стенда (omega / salesheroes).
// Вставить весь файл и Enter — выгрузка начнётся сразу, JSON скачается батчами.
// Эталон: IN/JS/Profile_GP_LOAD_file.js
// =============================================================================
"""

AUTO_FOOTER = """
// =============================================================================
// Автозапуск (без панели) — подробный лог в console (как «Журнал работы»)
// =============================================================================
console.log(
  "[Profile Auto] ——— Старт сбора ——— | ТН: " +
    TAB_NUMS.length +
    " | задержка: {{REQUEST_DELAY_MS}} мс | batch: {{BATCH_SIZE}} | retry: {{ENABLE_RETRY_JS}}"
);

void runCollectProfiles(
  TAB_NUMS,
  normalizeRunOptions({
    requestDelayMs: {{REQUEST_DELAY_MS}},
    enableRetry: {{ENABLE_RETRY_JS}},
    maxRetries: {{MAX_RETRIES}},
    retryDelayOnErrorMs: {{RETRY_DELAY_MS}},
    outputBaseName: "{{OUTPUT_BASE_NAME}}",
    batchSize: {{BATCH_SIZE}},
    enablePhotoDownload: {{ENABLE_PHOTO_DOWNLOAD_JS}},
    enablePhotoStrip: {{ENABLE_PHOTO_STRIP_JS}}
  })
);
})();
"""


PROFILE_ECHO_CONSOLE = """function profileGpPanelEcho(level) {
  var parts = [];
  for (var pi = 1; pi < arguments.length; pi++) {
    parts.push(String(arguments[pi]));
  }
  var s = parts.join(" ");
  var tag = "[Profile Auto]";
  if (level === "error") {
    console.error(tag, s);
  } else if (level === "warn") {
    console.warn(tag, s);
  } else {
    console.log(tag, s);
  }
}"""


def _replace_js_function_body(core: str, func_name: str, new_body: str) -> str:
    """Заменяет тело функции func_name(...) { ... } в JS-тексте."""
    marker = f"function {func_name}("
    start = core.find(marker)
    if start < 0:
        raise ValueError(f"В эталоне не найдена функция {func_name}")
    brace_start = core.find("{", start)
    if brace_start < 0:
        raise ValueError(f"У функции {func_name} не найдено тело")
    depth = 0
    end = brace_start
    for i in range(brace_start, len(core)):
        ch = core[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    return core[:start] + new_body + core[end:]


def _patch_profile_script_for_console(core: str) -> str:
    """Переносит подробный журнал профилей в console (как в панели эталона)."""
    core = _replace_js_function_body(core, "profileGpPanelEcho", PROFILE_ECHO_CONSOLE)
    core = core.replace(
        "[Профили героев] Сбор запущен.",
        "[Profile Auto] Сбор запущен.",
    )
    old_final = (
        '  console.log(\n'
        '    "[Профили героев] Сбор завершён. Всего ТН: " +\n'
        "      list.length +\n"
        '      " | успешно: " +\n'
        "      totalOk +\n"
        '      " | ошибок: " +\n'
        "      totalErr +\n"
        '      " | обработано записей: " +\n'
        "      totalCount\n"
        "  );"
    )
    new_final = (
        '  console.log("[Profile Auto] ==== ИТОГ ====");\n'
        "  console.log(\n"
        '    "[Profile Auto] Всего ТН: " +\n'
        "      list.length +\n"
        '      " | успешно: " +\n'
        "      totalOk +\n"
        '      " | ошибок: " +\n'
        "      totalErr +\n"
        '      " | обработано: " +\n'
        "      totalCount +\n"
        '      " | размер ответов ДО обработки: " +\n'
        "      totalSizeBefore +\n"
        '      " bytes | ПОСЛЕ обработки: " +\n'
        "      totalSizeAfter +\n"
        '      " bytes"\n'
        "  );"
    )
    if old_final in core:
        core = core.replace(old_final, new_final)
    return core


def tab_for_profile_js(tab_padded: str) -> str:
    """20-значный табельный → формат 8–20 цифр для API профиля."""
    digits = re.sub(r"\D", "", str(tab_padded).strip())
    if not digits:
        return ""
    core = digits.lstrip("0") or "0"
    if len(core) < 8:
        return core.zfill(8)
    if len(core) > 20:
        return core[-20:]
    return core


def profile_js_check_columns(pg_cfg: Mapping[str, Any]) -> List[str]:
    """
    Колонки для отбора табельных в Profile AutoRun — только после CSV-enrich и JSON.
    По умолчанию: js_missing_columns или ключи json_field_map (поля профиля API).
    """
    raw_js = pg_cfg.get("js_missing_columns")
    if isinstance(raw_js, list) and raw_js:
        return [str(c).strip() for c in raw_js if str(c).strip()]
    field_map = pg_cfg.get("json_field_map")
    if isinstance(field_map, dict) and field_map:
        return [str(k).strip() for k in field_map if str(k).strip()]
    return list(DEFAULT_PROFILE_JS_MISSING_COLUMNS)


def prepare_tabs_for_profile_js(
    df_tabs: Optional[pd.DataFrame],
    mcfg: Mapping[str, Any],
    *,
    paths_cfg: Optional[Mapping[str, Any]] = None,
    full_cfg: Optional[Mapping[str, Any]] = None,
) -> Tuple[Optional[pd.DataFrame], List[str]]:
    """
    Подготовка списка ТН для AutoRun: повторная подстановка JSON (идемпотентно),
    затем отбор табельных с пустыми полями js_missing_columns.
    """
    if df_tabs is None or df_tabs.empty:
        return df_tabs, []

    from src.profile_gp_json import apply_profile_gp_json_enrich

    resolved_paths = paths_cfg
    if resolved_paths is None and isinstance(full_cfg, Mapping):
        resolved_paths = {"input": str((full_cfg.get("paths") or {}).get("input") or "IN")}
    if resolved_paths is None:
        resolved_paths = dict(mcfg.get("_paths") or {})

    df_ready = apply_profile_gp_json_enrich(df_tabs, mcfg, paths_cfg=resolved_paths)
    tabs = collect_tabs_missing_profile_fields(df_ready, mcfg)
    return df_ready, tabs


def collect_tabs_missing_profile_fields(
    df_tabs: Optional[pd.DataFrame],
    mcfg: Mapping[str, Any],
) -> List[str]:
    """
    Табельные, у которых хотя бы одна из js_missing_columns пуста или равна default («-»).
    Вызывать только после CSV-enrich и apply_profile_gp_json_enrich.
    """
    if df_tabs is None or df_tabs.empty:
        return []

    pg_cfg = dict(mcfg.get("profile_gp_load") or {})
    missing_cols = profile_js_check_columns(pg_cfg)
    if not missing_cols:
        return []

    default_val = str(mcfg.get("enrich_default") or "-").strip()
    tab_col = "Табельный номер"
    if tab_col not in df_tabs.columns:
        logging.warning(
            "[manager_stats] profile GP JS: в TAB_NUMBERS нет колонки %r",
            tab_col,
        )
        return []

    def _is_missing(val: Any) -> bool:
        return is_enrich_value_missing(val, default_val)

    ordered: List[str] = []
    seen: set[str] = set()

    for _, row in df_tabs.iterrows():
        tab_raw = _cell_str(row.get(tab_col))
        if not tab_raw:
            continue
        need = any(
            col not in df_tabs.columns or _is_missing(row.get(col))
            for col in missing_cols
        )
        if not need:
            continue
        tab_js = tab_for_profile_js(tab_raw)
        if not tab_js or tab_js in seen:
            continue
        seen.add(tab_js)
        ordered.append(tab_js)

    return ordered


def _resolve_template_path(full_cfg: Mapping[str, Any], pg_cfg: Mapping[str, Any]) -> Path:
    """Путь к эталонному Profile_GP_LOAD_file.js."""
    input_root = Path(str((full_cfg.get("paths") or {}).get("input") or "IN"))
    subdir = str(pg_cfg.get("js_template_subdir") or "JS").strip()
    filename = str(pg_cfg.get("js_template") or DEFAULT_TEMPLATE_FILENAME).strip()
    base = input_root / subdir if subdir else input_root
    path = base / filename
    if not path.is_file():
        raise FileNotFoundError(f"Эталонный JS профилей не найден: {path}")
    return path


def _extract_core_from_template(template_text: str) -> str:
    """Тело IIFE без панели startWithChoice."""
    marker_idx = template_text.find(PANEL_SECTION_MARKER)
    if marker_idx < 0:
        raise ValueError(
            "В эталоне Profile_GP_LOAD_file.js не найден маркер секции панели "
            "(ЗАГРУЗКА ТН ИЗ ФАЙЛА)"
        )
    iife_idx = template_text.find("(function () {")
    if iife_idx < 0 or iife_idx >= marker_idx:
        raise ValueError("В эталоне Profile_GP_LOAD_file.js не найдено начало IIFE")
    return template_text[iife_idx:marker_idx]


def _replace_tab_nums_block(core: str, tab_nums: Sequence[str]) -> str:
    """Подставляет TAB_NUMS в ядро скрипта."""
    tab_json = json.dumps(list(tab_nums), ensure_ascii=False, indent=2)
    new_core, n = re.subn(
        r"const TAB_NUMS = \[[\s\S]*?\];",
        f"const TAB_NUMS = {tab_json};",
        core,
        count=1,
    )
    if n != 1:
        raise ValueError("В эталоне Profile_GP_LOAD_file.js не найден блок const TAB_NUMS")
    return new_core


def _js_bool(value: bool) -> str:
    return "true" if value else "false"


def build_js_content(
    tab_nums: Sequence[str],
    *,
    missing_columns: Sequence[str],
    pg_cfg: Mapping[str, Any],
    template_path: Path,
) -> str:
    """Собирает Profile_GP_LOAD_AutoRun.js из эталона и списка ТН."""
    template_text = template_path.read_text(encoding="utf-8")
    core = _extract_core_from_template(template_text)
    core = _replace_tab_nums_block(core, tab_nums)
    core = _patch_profile_script_for_console(core)

    footer = (
        AUTO_FOOTER.replace("{{REQUEST_DELAY_MS}}", str(int(pg_cfg.get("request_delay_ms") or 2)))
        .replace("{{ENABLE_RETRY_JS}}", _js_bool(bool(pg_cfg.get("enable_retry", True))))
        .replace("{{MAX_RETRIES}}", str(int(pg_cfg.get("max_retries") or 1)))
        .replace(
            "{{RETRY_DELAY_MS}}",
            str(int(pg_cfg.get("retry_delay_on_error_ms") or 1500)),
        )
        .replace(
            "{{OUTPUT_BASE_NAME}}",
            str(pg_cfg.get("output_base_name") or "profiles").replace('"', '\\"'),
        )
        .replace("{{BATCH_SIZE}}", str(int(pg_cfg.get("batch_size") or 12000)))
        .replace(
            "{{ENABLE_PHOTO_DOWNLOAD_JS}}",
            _js_bool(bool(pg_cfg.get("enable_photo_download", False))),
        )
        .replace(
            "{{ENABLE_PHOTO_STRIP_JS}}",
            _js_bool(bool(pg_cfg.get("enable_photo_strip", True))),
        )
    )

    header = (
        AUTO_HEADER.replace("{{GENERATED_AT}}", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        .replace("{{MISSING_COLS}}", ", ".join(missing_columns))
        .replace("{{TAB_COUNT}}", str(len(tab_nums)))
    )

    return header + core + footer


def write_profile_gp_auto_js(
    output_dir: str,
    *,
    df_tabs: Optional[pd.DataFrame] = None,
    manager_stats_cfg: Optional[Mapping[str, Any]] = None,
    full_cfg: Optional[Mapping[str, Any]] = None,
) -> Optional[str]:
    """
    Записывает Profile_GP_LOAD_AutoRun.js в каталог прогона (рядом с Excel).

    Returns:
        Путь к файлу или None, если генерация отключена или нет табельных.
    """
    mcfg = merge_manager_stats_config(manager_stats_cfg)
    pg_cfg = dict(mcfg.get("profile_gp_load") or {})
    if pg_cfg.get("js_enabled") is False:
        return None

    if full_cfg is None:
        full_cfg = {}

    if not manager_stats_only_in_run_outputs(full_cfg):
        logging.debug(
            "[manager_stats] profile GP JS: пропуск — в run_outputs нет manager_stats_only"
        )
        return None

    missing_cols = profile_js_check_columns(pg_cfg)

    paths_cfg = {"input": str((full_cfg.get("paths") or {}).get("input") or "IN")}
    if isinstance(manager_stats_cfg, Mapping) and manager_stats_cfg.get("_paths"):
        paths_cfg = dict(manager_stats_cfg["_paths"])

    df_ready, tab_nums = prepare_tabs_for_profile_js(
        df_tabs,
        mcfg,
        paths_cfg=paths_cfg,
        full_cfg=full_cfg,
    )
    del df_ready

    if not tab_nums:
        logging.info(
            "[manager_stats] profile GP JS: нет табельных после CSV+JSON enrich (пусто: %s)",
            ", ".join(missing_cols),
        )
        return None

    js_name = str(pg_cfg.get("js_file") or DEFAULT_JS_FILENAME).strip() or DEFAULT_JS_FILENAME

    try:
        template_path = _resolve_template_path(full_cfg, pg_cfg)
    except FileNotFoundError as exc:
        logging.warning("[manager_stats] profile GP JS: %s", exc)
        return None

    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, js_name)
    content = build_js_content(
        tab_nums,
        missing_columns=missing_cols,
        pg_cfg=pg_cfg,
        template_path=template_path,
    )
    Path(out_path).write_text(content, encoding="utf-8")
    logging.info(
        "[manager_stats] profile GP JS: %s ТН после CSV+JSON enrich (пусто: %s) → %s",
        len(tab_nums),
        ", ".join(missing_cols),
        out_path,
    )
    return out_path
