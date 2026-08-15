# -*- coding: utf-8 -*-
"""Собрать common/param_catalog_review/catalog.json из схемы + field_meta + MD-каталога."""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.contest_badge_form import schema  # noqa: E402
from src.contest_badge_form.field_meta import (  # noqa: E402
    DROPDOWN_VALUES,
    TABLE_COLUMN_HINTS,
    TABLE_DROPDOWNS,
    allow_empty_for,
    default_for,
    description_for,
    input_kind_for_kv,
    input_kind_for_table_col,
    json_pack_target,
    json_pack_target_table,
)

OUT_DIR = ROOT / "common" / "param_catalog_review"
WEB_EDIT_DIR = ROOT / "common" / "web-edit"
MD_PATH = ROOT / "common" / "param_catalog_review" / "CONTEST_BADGE_FORM_PARAM_REVIEW.md"


def _clean_html_text(s: str) -> str:
    t = (
        (s or "")
        .replace("&#124;", "|")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("<br />", "\n")
        .replace("<br/>", "\n")
        .replace("<br>", "\n")
    )
    return re.sub(r"\s+\n", "\n", t).strip()


def parse_md_overrides() -> Dict[str, Dict[str, dict]]:
    """Статусы/подписи/описания из текущего MD (HTML-строки)."""
    if not MD_PATH.exists():
        return {}
    text = MD_PATH.read_text(encoding="utf-8")
    row_re = re.compile(
        r"<tr>\s*"
        r"<td>(\d+)</td>\s*"
        r"<td><code>([^<]*)</code></td>\s*"
        r"<td><code>([^<]*)</code></td>\s*"
        r"<td>(.*?)</td>\s*"
        r"<td>(.*?)</td>\s*"
        r"<td>(.*?)</td>\s*"
        r"<td>(.*?)</td>\s*"
        r"<td>(.*?)</td>\s*"
        r"<td>(.*?)</td>\s*"
        r"<td>(.*?)</td>\s*"
        r"<td>(.*?)</td>\s*"
        r"</tr>",
        re.S,
    )
    # GFM fallback
    gfm_re = re.compile(
        r"^\| (\d+) \| `(\[[^\]]+\])` \| `([^`]+)` \| ([^|]+) \| ([^|]+) \| "
        r"([^|]+) \| ([^|]+) \| ([^|]+) \| ([^|]+) \| ([^|]+) \| ([^|]*) \|$"
    )
    out: Dict[str, Dict[str, dict]] = {}
    for m in row_re.finditer(text):
        before = text[: m.start()]
        sm = list(re.finditer(r"^## (.+)$", before, re.M))
        section = sm[-1].group(1).strip() if sm else "?"
        if section.startswith("Как") or section.startswith("Сводка"):
            continue
        key = m.group(3)
        opts_raw = _clean_html_text(m.group(7))
        variants: List[str] = []
        if opts_raw and opts_raw not in {"—", "-", "через ;"}:
            if "\n" in opts_raw:
                variants = [x.strip() for x in opts_raw.splitlines() if x.strip()]
            else:
                # не режем по запятой агрессивно — оставим как одну строку,
                # если список известен из DROPDOWN, он перезапишется ниже
                variants = [x.strip() for x in opts_raw.split(", ") if x.strip()]
        out.setdefault(section, {})[key] = {
            "status": m.group(2),
            "label": _clean_html_text(m.group(4)),
            "description": _clean_html_text(m.group(5)),
            "kind": _clean_html_text(m.group(6)),
            "variants": variants,
            "default": _clean_html_text(m.group(8)),
            "allow_empty": _clean_html_text(m.group(9)) != "нет",
            "json_target": _clean_html_text(m.group(10)),
            "note": _clean_html_text(m.group(11)),
        }
    if out:
        return out
    sec = ""
    for line in text.splitlines():
        if line.startswith("## "):
            sec = line[3:].strip()
            continue
        m = gfm_re.match(line)
        if not m or not sec:
            continue
        key = m.group(3)
        opts = m.group(7).replace("<br>", "\n").replace("<br/>", "\n")
        variants = [x.strip() for x in opts.splitlines() if x.strip() and x.strip() != "—"]
        out.setdefault(sec, {})[key] = {
            "status": m.group(2),
            "label": m.group(4).strip().replace(" · ", " | "),
            "description": m.group(5).strip().replace(" · ", " | "),
            "kind": m.group(6).strip(),
            "variants": variants,
            "default": m.group(8).strip(),
            "allow_empty": m.group(9).strip() != "нет",
            "json_target": m.group(10).strip(),
            "note": m.group(11).strip(),
        }
    return out


def _variants_for(form_key: str, kind: str, table_key: Optional[str] = None, col: Optional[str] = None) -> List[str]:
    if table_key and col and col in (TABLE_DROPDOWNS.get(table_key) or {}):
        return list(TABLE_DROPDOWNS[table_key][col])
    if form_key in DROPDOWN_VALUES:
        return list(DROPDOWN_VALUES[form_key])
    return []


def _field(
    *,
    n: int,
    key: str,
    label: str,
    kind: str,
    description: str,
    variants: List[str],
    default: str,
    allow_empty: bool,
    json_target: str,
    status: str = "[ ]",
    note: str = "",
) -> Dict[str, Any]:
    return {
        "n": n,
        "key": key,
        "status": status if status in {"[ ]", "[v]", "[w]"} else "[ ]",
        "label": label,
        "description": description,
        "kind": kind,
        "variants": variants,
        "default": "" if default in {"—", "-"} else default,
        "allow_empty": allow_empty,
        "json_target": "" if json_target in {"—", "-"} else json_target,
        "note": note,
    }


def build_catalog() -> Dict[str, Any]:
    ov = parse_md_overrides()
    sections: List[Dict[str, Any]] = []
    n = 0

    def add_section(
        sec_id: str,
        title: str,
        intro: str,
        fields: List[Dict[str, Any]],
        *,
        kind: str,
        parent: Optional[str] = None,
        menu_label: Optional[str] = None,
        sheet: Optional[str] = None,
    ) -> None:
        sections.append(
            {
                "id": sec_id,
                "title": title,
                "menu_label": menu_label or title,
                "intro": intro,
                "kind": kind,  # table | json
                "parent": parent,
                "sheet": sheet or "",
                "fields": fields,
            }
        )

    def take_any(*sec_names: str, key: str) -> dict:
        for name in sec_names:
            hit = ov.get(name, {}).get(key)
            if hit:
                return hit
        return {}

    # TABLE: CONTEST — лист CONTEST-DATA
    fields = []
    for key, schema_label in list(schema.CONTEST_FLAT_FIELDS) + list(
        schema.CONTEST_ARRAY_FIELDS
    ):
        n += 1
        o = take_any("CONTEST", "TABLE:CONTEST", key=key)
        sk = "list" if key in dict(schema.CONTEST_ARRAY_FIELDS) else None
        kind = (o.get("kind") or input_kind_for_kv(
            key, schema_kind=sk, has_dropdown=key in DROPDOWN_VALUES
        )).split()[0]
        variants = o.get("variants") or _variants_for(key, kind)
        if key in DROPDOWN_VALUES:
            variants = list(DROPDOWN_VALUES[key])
        fields.append(
            _field(
                n=n,
                key=key,
                label=o.get("label") or schema_label,
                kind=kind,
                description=o.get("description") or description_for(key),
                variants=variants,
                default=o.get("default") if "default" in o else default_for(key),
                allow_empty=o.get("allow_empty")
                if "allow_empty" in o
                else allow_empty_for(key),
                json_target=o.get("json_target") or json_pack_target(key),
                status=o.get("status", "[ ]"),
                note=o.get("note", ""),
            )
        )
    add_section(
        "CONTEST",
        "CONTEST",
        "Таблица / лист CONTEST-DATA — плоские колонки конкурса",
        fields,
        kind="table",
        menu_label="CONTEST",
        sheet="CONTEST-DATA",
    )

    # JSON: CONTEST_FEATURE внутри CONTEST
    fields = []
    for leaf, schema_label, sk in schema.CONTEST_FEATURE_FIELDS:
        n += 1
        key = f"FEATURE.{leaf}"
        o = take_any("FEATURE", "CONTEST_FEATURE", key=key)
        kind = (o.get("kind") or input_kind_for_kv(
            key, schema_kind=sk, has_dropdown=key in DROPDOWN_VALUES
        )).split()[0]
        variants = list(DROPDOWN_VALUES[key]) if key in DROPDOWN_VALUES else (o.get("variants") or [])
        fields.append(
            _field(
                n=n,
                key=key,
                label=o.get("label") or schema_label,
                kind=kind,
                description=o.get("description") or description_for(key),
                variants=variants,
                default=o.get("default") if "default" in o else default_for(key),
                allow_empty=o.get("allow_empty")
                if "allow_empty" in o
                else allow_empty_for(key),
                json_target="CONTEST_FEATURE",
                status=o.get("status", "[ ]"),
                note=o.get("note", ""),
            )
        )
    add_section(
        "CONTEST_FEATURE",
        "CONTEST_FEATURE",
        "JSON-колонка CONTEST_FEATURE внутри таблицы CONTEST",
        fields,
        kind="json",
        parent="CONTEST",
        menu_label="CONTEST_FEATURE",
        sheet="CONTEST-DATA",
    )

    # TABLE: REWARD (ранее BADGE в форме)
    fields = []
    for key, schema_label in schema.REWARD_FLAT_FIELDS:
        n += 1
        o = take_any("REWARD", "BADGE", key=key)
        kind = (o.get("kind") or input_kind_for_kv(
            key, has_dropdown=key in DROPDOWN_VALUES
        )).split()[0]
        variants = list(DROPDOWN_VALUES[key]) if key in DROPDOWN_VALUES else (o.get("variants") or [])
        fields.append(
            _field(
                n=n,
                key=key,
                label=o.get("label") or schema_label,
                kind=kind,
                description=o.get("description")
                or description_for(key, in_badge_slot=True),
                variants=variants,
                default=o.get("default")
                if "default" in o
                else default_for(key, in_badge_slot=True),
                allow_empty=o.get("allow_empty")
                if "allow_empty" in o
                else allow_empty_for(key, in_badge_slot=True),
                json_target="",
                status=o.get("status", "[ ]"),
                note=o.get("note", ""),
            )
        )
    add_section(
        "REWARD",
        "REWARD",
        "Таблица / лист REWARD — плоские колонки награды (в форме слоты BADGE)",
        fields,
        kind="table",
        menu_label="REWARD",
        sheet="REWARD",
    )

    # JSON: REWARD_ADD_DATA внутри REWARD
    fields = []
    for leaf, schema_label, sk in schema.REWARD_ADD_DATA_FIELDS:
        n += 1
        key = f"ADD.{leaf}"
        o = take_any("REWARD_ADD_DATA", "ADD", key=key)
        kind = (o.get("kind") or input_kind_for_kv(
            key, schema_kind=sk, has_dropdown=key in DROPDOWN_VALUES
        )).split()[0]
        variants = list(DROPDOWN_VALUES[key]) if key in DROPDOWN_VALUES else (o.get("variants") or [])
        fields.append(
            _field(
                n=n,
                key=key,
                label=o.get("label") or schema_label,
                kind=kind,
                description=o.get("description") or description_for(key),
                variants=variants,
                default=o.get("default") if "default" in o else default_for(key),
                allow_empty=o.get("allow_empty")
                if "allow_empty" in o
                else allow_empty_for(key),
                json_target="REWARD_ADD_DATA",
                status=o.get("status", "[ ]"),
                note=o.get("note", ""),
            )
        )
    add_section(
        "REWARD_ADD_DATA",
        "REWARD_ADD_DATA",
        "JSON-колонка REWARD_ADD_DATA внутри таблицы REWARD",
        fields,
        kind="json",
        parent="REWARD",
        menu_label="REWARD_ADD_DATA",
        sheet="REWARD",
    )

    for sec_name, table_key, cols in [
        ("TABLE:REWARD-LINK", "REWARD-LINK", schema.REWARD_LINK_COLUMNS),
        ("TABLE:GROUP", "GROUP", schema.GROUP_COLUMNS),
        ("TABLE:INDICATOR", "INDICATOR", schema.INDICATOR_COLUMNS),
        ("TABLE:SCHEDULE", "SCHEDULE", schema.SCHEDULE_COLUMNS),
    ]:
        fields = []
        hints = TABLE_COLUMN_HINTS.get(table_key, {})
        for col in cols:
            n += 1
            o = take_any(sec_name, table_key, key=col)
            kind = (
                o.get("kind") or input_kind_for_table_col(table_key, col)
            ).split()[0]
            variants = _variants_for(col, kind, table_key=table_key, col=col)
            fields.append(
                _field(
                    n=n,
                    key=col,
                    label=o.get("label") or col,
                    kind=kind,
                    description=o.get("description") or hints.get(col, ""),
                    variants=variants,
                    default=o.get("default") if "default" in o else "",
                    allow_empty=o.get("allow_empty") if "allow_empty" in o else True,
                    json_target=json_pack_target_table(table_key, col),
                    status=o.get("status", "[ ]"),
                    note=o.get("note", ""),
                )
            )
        sheet_name = {
            "REWARD-LINK": "REWARD-LINK",
            "GROUP": "GROUP",
            "INDICATOR": "INDICATOR",
            "SCHEDULE": "TOURNAMENT-SCHEDULE",
        }.get(table_key, table_key)
        add_section(
            sec_name,
            table_key,
            f"Таблица / лист {sheet_name}",
            fields,
            kind="table",
            menu_label=table_key,
            sheet=sheet_name,
        )

    return {
        "version": 2,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "schema + field_meta + CONTEST_BADGE_FORM_PARAM_REVIEW.md",
        "sections": sections,
    }


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    WEB_EDIT_DIR.mkdir(parents=True, exist_ok=True)
    catalog = build_catalog()
    payload = json.dumps(catalog, ensure_ascii=False, indent=2) + "\n"
    js_body = (
        "/* зеркало catalog.json — собирается build_param_review_editor.py */\n"
        f"window.PARAM_REVIEW_CATALOG = {json.dumps(catalog, ensure_ascii=False, indent=2)};\n"
    )
    targets = [OUT_DIR, WEB_EDIT_DIR]
    for dest in targets:
        (dest / "catalog.json").write_text(payload, encoding="utf-8")
        (dest / "catalog.js").write_text(js_body, encoding="utf-8")
    total = sum(len(s["fields"]) for s in catalog["sections"])
    print(
        f"OK: catalog.json + catalog.js → {OUT_DIR.name}/ и {WEB_EDIT_DIR.name}/ "
        f"({total} полей, {len(catalog['sections'])} секций)"
    )


if __name__ == "__main__":
    main()
