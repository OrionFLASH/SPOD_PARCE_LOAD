# -*- coding: utf-8 -*-
"""Точка входа режимов contest_badge_form_* из run_outputs."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

from src.contest_badge_form.export_form import export_contest_codes
from src.contest_badge_form.form_io import create_blank_form
from src.contest_badge_form.import_form import import_form_file
from src.contest_badge_form.schema import FORM_TOKENS


def run_contest_badge_form_modes(
    project_base_dir: str,
    block: str,
    run_outputs: List[str],
    cfg: Dict[str, Any],
) -> bool:
    """
    Выполнить blank/export/import по токенам run_outputs.
    Возвращает True, если был выполнен хотя бы один режим формы.
    """
    tokens = {str(t).strip().lower().replace("-", "_") for t in run_outputs}
    if not (tokens & set(FORM_TOKENS)):
        return False

    form_cfg = dict(cfg.get("contest_badge_form") or {})
    # Блок из конфига формы может перекрывать текущий
    form_block = str(form_cfg.get("block") or block or "PROM").strip().upper()
    dropdowns = {
        str(k): [str(x) for x in (v or [])]
        for k, v in (form_cfg.get("dropdowns") or {}).items()
    }

    if "contest_badge_form_blank" in tokens:
        blank_override = form_cfg.get("blank_path")
        if blank_override:
            blank_path = str(blank_override)
            if not os.path.isabs(blank_path):
                blank_path = os.path.join(project_base_dir, blank_path)
        else:
            paths = cfg.get("paths") or {}
            out_root = str(paths.get("output") or "OUT")
            blank_path = os.path.join(
                project_base_dir,
                out_root,
                form_block,
                "CONTEST_BADGE_FORM",
                "CONTEST_BADGE_FORM_BLANK.xlsx",
            )
        sheet_count = int(form_cfg.get("blank_sheet_count") or 1)
        contest_type = str(
            form_cfg.get("blank_contest_type") or "ТУРНИРНЫЙ"
        ).strip() or "ТУРНИРНЫЙ"
        path = create_blank_form(
            blank_path,
            sheet_count=sheet_count,
            contest_type=contest_type,
            dropdowns=dropdowns,
        )
        logging.info("[contest_badge_form] Пустая форма: %s", path)

        # Пример с заполненными конкурсами (ОСВ + турниры и т.п.)
        example_codes = [
            str(c).strip()
            for c in (form_cfg.get("example_contest_codes") or [])
            if str(c).strip()
        ]
        if example_codes:
            example_override = form_cfg.get("example_path")
            if example_override:
                example_path = str(example_override)
                if not os.path.isabs(example_path):
                    example_path = os.path.join(project_base_dir, example_path)
            else:
                paths = cfg.get("paths") or {}
                out_root = str(paths.get("output") or "OUT")
                example_path = os.path.join(
                    project_base_dir,
                    out_root,
                    form_block,
                    "CONTEST_BADGE_FORM",
                    "CONTEST_BADGE_FORM_EXAMPLE.xlsx",
                )
            ex_path = export_contest_codes(
                project_base_dir,
                cfg,
                form_block,
                example_codes,
                output_path=example_path,
            )
            logging.info("[contest_badge_form] Пример формы: %s", ex_path)

    if "contest_badge_form_export" in tokens:
        codes = [str(c).strip() for c in (form_cfg.get("contest_codes") or []) if str(c).strip()]
        if not codes:
            raise ValueError(
                "contest_badge_form_export: укажите contest_badge_form.contest_codes в конфиге"
            )
        out_override = form_cfg.get("export_path")
        path = export_contest_codes(
            project_base_dir,
            cfg,
            form_block,
            codes,
            output_path=str(out_override) if out_override else None,
        )
        logging.info("[contest_badge_form] Export завершён: %s", path)

    if "contest_badge_form_import" in tokens:
        form_path = str(form_cfg.get("import_form_path") or "").strip()
        if not form_path:
            raise ValueError(
                "contest_badge_form_import: укажите contest_badge_form.import_form_path"
            )
        if not os.path.isabs(form_path):
            form_path = os.path.join(project_base_dir, form_path)
        if not os.path.isfile(form_path):
            raise FileNotFoundError(
                f"contest_badge_form_import: файл формы не найден: {form_path}"
            )
        out_dir = form_cfg.get("import_output_dir")
        meta = import_form_file(
            form_path,
            project_base_dir,
            cfg,
            form_block,
            output_dir=str(out_dir) if out_dir else None,
        )
        logging.info(
            "[contest_badge_form] Import завершён: %s", meta.get("output_dir")
        )

    return True


def only_form_tokens(run_outputs: List[str]) -> bool:
    """True, если в run_outputs только токены формы (или пусто после вычета формы)."""
    tokens = {str(t).strip().lower().replace("-", "_") for t in run_outputs}
    other = tokens - set(FORM_TOKENS)
    return bool(tokens & set(FORM_TOKENS)) and not other
