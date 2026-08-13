# -*- coding: utf-8 -*-
"""Экспорт конкурсов BADGE из IN CSV в Excel-форму."""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from src.contest_badge_form import schema
from src.contest_badge_form.csv_load import (
    filter_by_contest,
    load_spod_frames,
    rewards_for_contest,
)
from src.contest_badge_form.form_io import (
    df_rows_to_dicts,
    payload_from_csv_bundle,
)
from src.contest_badge_form.spod_json import parse_spod_json
from src.contest_badge_form.xlsx_write import write_form_xlsx


def _split_badges(
    rewards_df: Any, contest_code: str
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Разделить награды на BADGE и прочие (для лога)."""
    badges: List[Dict[str, Any]] = []
    others: List[Dict[str, Any]] = []
    if rewards_df is None or getattr(rewards_df, "empty", True):
        return badges, others
    for _, series in rewards_df.iterrows():
        row = {c: series[c] for c in rewards_df.columns}
        rtype = str(row.get("REWARD_TYPE") or "").strip().upper()
        if rtype == "BADGE":
            badges.append(row)
        else:
            others.append(row)
            logging.warning(
                "[contest_badge_form] Конкурс %s: пропуск не-BADGE награды %s (тип=%s)",
                contest_code,
                row.get("REWARD_CODE"),
                row.get("REWARD_TYPE"),
            )
    return badges, others


def export_contest_codes(
    project_base_dir: str,
    cfg: Dict[str, Any],
    block: str,
    contest_codes: List[str],
    output_path: Optional[str] = None,
) -> str:
    """
    Экспорт списка CONTEST_CODE в Excel-форму.
    Возвращает путь к созданному файлу.
    """
    form_cfg = cfg.get("contest_badge_form") or {}
    frames = load_spod_frames(project_base_dir, cfg, block)
    contest_df = frames["contest"]
    dropdowns = {
        str(k): [str(x) for x in (v or [])]
        for k, v in (form_cfg.get("dropdowns") or {}).items()
    }

    payloads: List[Dict[str, Any]] = []
    for code in contest_codes:
        code = str(code).strip()
        if not code:
            continue
        if contest_df.empty or "CONTEST_CODE" not in contest_df.columns:
            logging.error(
                "[contest_badge_form] Лист CONTEST пуст — код %s пропущен", code
            )
            continue
        matched = contest_df[contest_df["CONTEST_CODE"].astype(str) == code]
        if matched.empty:
            logging.error(
                "[contest_badge_form] CONTEST_CODE не найден: %s", code
            )
            continue
        contest_row = {c: matched.iloc[0][c] for c in matched.columns}
        rewards_df, links_df = rewards_for_contest(
            frames["reward"], frames["reward_link"], code
        )
        badge_rows, _others = _split_badges(rewards_df, code)
        contest_type = str(contest_row.get("CONTEST_TYPE") or "")
        limit = schema.max_badge_slots(contest_type)
        if len(badge_rows) > limit:
            logging.warning(
                "[contest_badge_form] %s: BADGE=%s больше лимита %s (%s) — лишние обрезаны",
                code,
                len(badge_rows),
                limit,
                schema.expected_badge_count_note(contest_type),
            )
            badge_rows = badge_rows[:limit]
        elif contest_type.strip().upper() in {
            "ИНДИВИДУАЛЬНЫЙ",
            "ИНДИВИДУАЛЬНЫЙ НАКОПИТЕЛЬНЫЙ",
        } and len(badge_rows) != 1:
            logging.warning(
                "[contest_badge_form] %s: ожидалась 1 BADGE, найдено %s",
                code,
                len(badge_rows),
            )

        add_data_list: List[Dict[str, Any]] = []
        for brow in badge_rows:
            parsed = parse_spod_json(brow.get("REWARD_ADD_DATA", ""))
            add_data_list.append(parsed if isinstance(parsed, dict) else {})

        # Сортировка BADGE по коду для стабильности
        order = sorted(
            range(len(badge_rows)),
            key=lambda i: str(badge_rows[i].get("REWARD_CODE") or ""),
        )
        badge_rows = [badge_rows[i] for i in order]
        add_data_list = [add_data_list[i] for i in order]

        link_dicts = df_rows_to_dicts(links_df, schema.REWARD_LINK_COLUMNS)
        # В форме оставляем только связи на BADGE-коды
        badge_codes = {str(b.get("REWARD_CODE") or "") for b in badge_rows}
        link_dicts = [
            r for r in link_dicts if str(r.get("REWARD_CODE") or "") in badge_codes
        ]

        group_df = filter_by_contest(frames["group"], code)
        ind_df = filter_by_contest(frames["indicator"], code)
        sch_df = filter_by_contest(frames["schedule"], code)

        payload = payload_from_csv_bundle(
            contest_row,
            badge_rows,
            add_data_list,
            link_dicts,
            df_rows_to_dicts(group_df, schema.GROUP_COLUMNS),
            df_rows_to_dicts(ind_df, schema.INDICATOR_COLUMNS),
            df_rows_to_dicts(sch_df, schema.SCHEDULE_COLUMNS),
        )
        payloads.append(payload)
        logging.info(
            "[contest_badge_form] Экспорт %s (%s): badges=%s links=%s group=%s ind=%s sch=%s",
            code,
            contest_type,
            len(badge_rows),
            len(link_dicts),
            len(group_df),
            len(ind_df),
            len(sch_df),
        )

    if not payloads:
        raise ValueError(
            "contest_badge_form_export: нет конкурсов для экспорта "
            "(проверьте contest_codes и CSV)"
        )

    if not output_path:
        paths = cfg.get("paths") or {}
        out_root = str(paths.get("output") or "OUT")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_dir = os.path.join(
            project_base_dir,
            out_root,
            block,
            "CONTEST_BADGE_FORM",
        )
        os.makedirs(out_dir, exist_ok=True)
        output_path = os.path.join(
            out_dir, f"CONTEST_BADGE_FORM_EXPORT_{block}_{ts}.xlsx"
        )
    else:
        os.makedirs(os.path.dirname(os.path.abspath(output_path)) or ".", exist_ok=True)

    write_form_xlsx(output_path, payloads, with_dropdowns=True)
    return output_path
