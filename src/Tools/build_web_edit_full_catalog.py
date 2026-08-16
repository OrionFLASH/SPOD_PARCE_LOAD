# -*- coding: utf-8 -*-
"""Собрать полный каталог web-edit-full из CSV IN/PROM/SPOD (без исключений)."""

from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

ROOT = Path(__file__).resolve().parents[2]
SPOD_DIR = ROOT / "IN" / "PROM" / "SPOD"
BASE_CATALOG = ROOT / "common" / "web-edit" / "game_edit_catalog_20260816_2304.json"
OUT_DIR = ROOT / "common" / "web-edit-full"
OUT_CATALOG = OUT_DIR / "game_edit_catalog.json"

SHEETS: Dict[str, Tuple[str, str]] = {
    "CONTEST-DATA": ("CONTEST ", "CONTEST"),
    "REWARD": ("REWARD ", "REWARD"),
    "REWARD-LINK": ("REWARD-LINK ", "TABLE:REWARD-LINK"),
    "GROUP": ("GROUP ", "TABLE:GROUP"),
    "INDICATOR": ("INDICATOR ", "TABLE:INDICATOR"),
    "TOURNAMENT-SCHEDULE": ("SCHEDULE ", "TABLE:SCHEDULE"),
}

JSON_MAP: Dict[Tuple[str, str], Tuple[str, str]] = {
    ("CONTEST-DATA", "CONTEST_FEATURE"): ("CONTEST_FEATURE", "object"),
    ("CONTEST-DATA", "CONTEST_PERIOD"): ("CONTEST_PERIOD", "array"),
    ("REWARD", "REWARD_ADD_DATA"): ("REWARD_ADD_DATA", "object"),
    ("INDICATOR", "INDICATOR_FILTER"): ("INDICATOR_FILTER", "array"),
    ("TOURNAMENT-SCHEDULE", "FILTER_PERIOD_ARR"): ("FILTER_PERIOD_ARR", "array"),
    ("TOURNAMENT-SCHEDULE", "TARGET_TYPE"): ("SCHEDULE_TARGET_TYPE", "object"),
}

# Известные новые ключи: описание/тип можно заполнить сразу (status всё равно [ ])
KNOWN_META: Dict[str, Dict[str, Any]] = {
    "itemFeature": {
        "label": "Особенности товара",
        "description": "Тексты особенностей товара/награды для карточки (массив строк).",
        "kind": "list",
    },
    "ignoreConditions": {
        "label": "Игнор условий",
        "description": "Список условий, которые не проверяются при выдаче.",
        "kind": "list",
    },
    "itemAmount": {
        "label": "Количество товара",
        "description": "Лимит/количество экземпляров товара.",
        "kind": "number",
    },
    "itemGroupAmount": {
        "label": "Количество в группе",
        "description": "Лимит количества в группе товаров.",
        "kind": "number",
    },
    "itemLimitCount": {
        "label": "Лимит заказов",
        "description": "Максимальное число заказов/получений.",
        "kind": "number",
    },
    "itemLimitPeriod": {
        "label": "Период лимита",
        "description": "Период действия лимита заказов.",
        "kind": "text",
    },
    "itemMinShow": {
        "label": "Мин. показ остатка",
        "description": "С какого остатка показывать количество.",
        "kind": "number",
    },
    "bookingRequired": {
        "label": "Нужно бронирование",
        "description": "Требуется бронь (Y/N).",
        "kind": "dropdown",
        "variants": ["Y", "N"],
        "variant_labels": ["Да", "Нет"],
    },
    "deliveryRequired": {
        "label": "Нужна доставка",
        "description": "Требуется доставка (Y/N).",
        "kind": "dropdown",
        "variants": ["Y", "N"],
        "variant_labels": ["Да", "Нет"],
    },
    "commingSoon": {
        "label": "Скоро в наличии",
        "description": "Метка «скоро» (Y/N). Опечатка comming в SPOD сохранена.",
        "kind": "dropdown",
        "variants": ["Y", "N"],
        "variant_labels": ["Да", "Нет"],
    },
    "getCondition": {
        "label": "Условия получения (объект)",
        "description": "Вложенный объект условий получения награды/товара.",
        "kind": "text",
    },
    "isGrouping": {
        "label": "Группировка",
        "description": "Признак группировки позиций (Y/N).",
        "kind": "dropdown",
        "variants": ["Y", "N"],
        "variant_labels": ["Да", "Нет"],
    },
    "isGroupingName": {
        "label": "Имя группы",
        "description": "Техническое/отображаемое имя группы.",
        "kind": "text",
    },
    "isGroupingTitle": {
        "label": "Заголовок группы",
        "description": "Заголовок группы в UI.",
        "kind": "text",
    },
    "isGroupingTultip": {
        "label": "Подсказка группы",
        "description": "Tooltip группы (опечатка Tultip в SPOD сохранена).",
        "kind": "text",
    },
    "persomanNumberVisible": {
        "label": "Видимость для сотрудников",
        "description": "Табельные, которым видна награда/товар (массив).",
        "kind": "list",
    },
    "tagColor": {
        "label": "Цвет тега",
        "description": "Цвет метки/тега награды.",
        "kind": "text",
    },
    "tagEndDT": {
        "label": "Дата конца тега",
        "description": "До какой даты показывать тег.",
        "kind": "date",
    },
}


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def normalize_spod_json_text(raw: Any) -> str:
    if raw is None:
        return ""
    s = str(raw).strip()
    if not s or s in {"-", "None", "null"}:
        return ""
    if len(s) >= 2 and s[0] == "'" and s[-1] == "'":
        inner = s[1:-1].strip()
        if inner.startswith("{") or inner.startswith("["):
            s = inner
    s = s.replace('"""', '"')
    while len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        inner = s[1:-1].strip()
        if inner.startswith("{") or inner.startswith("["):
            s = inner
        else:
            break
    while len(s) >= 2 and s[-1] == '"' and (s.startswith("{") or s.startswith("[")):
        s = s[:-1].rstrip()
    return s.strip()


def parse_spod_json(raw: Any) -> Any:
    norm = normalize_spod_json_text(raw)
    if not norm:
        return None
    try:
        return json.loads(norm)
    except json.JSONDecodeError:
        try:
            obj, _ = json.JSONDecoder().raw_decode(norm)
            return obj
        except json.JSONDecodeError:
            return None


def files_for(sheet: str, prefix: str) -> List[Path]:
    out: List[Path] = []
    for p in SPOD_DIR.iterdir():
        if not p.name.endswith(".csv"):
            continue
        if sheet == "REWARD":
            if p.name.startswith("REWARD ") and not p.name.startswith("REWARD-LINK"):
                out.append(p)
        elif p.name.startswith(prefix):
            out.append(p)
    return sorted(out)


def read_rows(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    text = path.read_text(encoding="utf-8-sig", errors="replace")
    delim = ";" if text[:8000].count(";") >= text[:8000].count(",") else ","
    with path.open("r", encoding="utf-8-sig", newline="", errors="replace") as f:
        reader = csv.DictReader(f, delimiter=delim)
        cols = list(reader.fieldnames or [])
        if len(cols) == 1 and cols[0] and ";" in cols[0]:
            f.seek(0)
            reader = csv.DictReader(f, delimiter=";")
            cols = list(reader.fieldnames or [])
            return cols, [dict(r) for r in reader]
        return cols, [dict(r) for r in reader]


def collect_obj(obj: Any, prefix: str, sink: Dict[str, Counter]) -> None:
    if not isinstance(obj, dict):
        return
    for k, v in obj.items():
        full = f"{prefix}.{k}"
        if isinstance(v, list):
            sink[full]["__kind_list"] += 1
            for it in v[:80]:
                if it is None or it == "":
                    sink[full]["__empty"] += 1
                else:
                    sink[full][str(it)] += 1
        elif isinstance(v, dict):
            sink[full]["__kind_object"] += 1
            collect_obj(v, full, sink)
        elif isinstance(v, bool):
            sink[full][str(v).lower()] += 1
            sink[full]["__kind_bool"] += 1
        elif isinstance(v, (int, float)) and not isinstance(v, bool):
            sink[full][str(v)] += 1
            sink[full]["__kind_number"] += 1
        else:
            s = "" if v is None else str(v)
            if s == "":
                sink[full]["__empty"] += 1
            else:
                sink[full][s] += 1


def collect_arr(arr: Any, prefix: str, sink: Dict[str, Counter]) -> None:
    if not isinstance(arr, list):
        return
    for it in arr:
        if isinstance(it, dict):
            collect_obj(it, prefix, sink)


def _is_num(s: str) -> bool:
    try:
        float(s)
        return True
    except ValueError:
        return False


def guess_kind(counter: Counter) -> Tuple[str, List[str], List[str], str, bool]:
    kinds = {k: counter.get(k, 0) for k in ("__kind_list", "__kind_number", "__kind_bool", "__kind_object")}
    empty = counter.get("__empty", 0)
    vals = [(k, n) for k, n in counter.most_common() if not str(k).startswith("__")]
    uniq = [k for k, _ in vals]
    allow = empty > 0
    if kinds["__kind_list"] and kinds["__kind_list"] >= max(kinds.values()):
        return "list", [], [], "", True
    date_re = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    if uniq and all(date_re.match(u) for u in uniq[:40]):
        return "date", [], [], uniq[0], allow
    if kinds["__kind_number"] and kinds["__kind_number"] >= len([u for u in uniq if not _is_num(u)]):
        return "number", [], [], (uniq[0] if uniq else "0"), allow
    yn = set(uniq)
    if yn and yn <= {"Y", "N", ""}:
        return "dropdown", ["Y", "N"], ["Да", "Нет"], ("N" if "N" in yn else "Y"), allow
    if 1 <= len(uniq) <= 16 and all(len(u) <= 64 for u in uniq):
        return "dropdown", uniq, [], uniq[0], allow
    if 17 <= len(uniq) <= 40 and all(len(u) <= 64 for u in uniq):
        return "dropdown_custom", uniq[:30], [], uniq[0], allow
    if uniq and all(_is_num(u) for u in uniq[:30]):
        return "number", [], [], uniq[0], allow
    return "text", [], [], "", allow


def max_n(sections: List[Dict[str, Any]]) -> int:
    m = 0
    for s in sections:
        for f in s.get("fields") or []:
            try:
                m = max(m, int(f.get("n") or 0))
            except (TypeError, ValueError):
                pass
    return m


def index_fields(sec: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {str(f.get("key") or ""): f for f in sec.get("fields") or []}


def has_leaf(existing: Dict[str, Dict[str, Any]], leaf: str) -> bool:
    for k in existing:
        if k == leaf or k.endswith("." + leaf) or k.split(".")[-1] == leaf and k.count(".") >= 1:
            # exact leaf path match preferred via full key check by caller
            pass
    for k in existing:
        if k.endswith("." + leaf) or k == leaf:
            return True
    return False


def human_label(leaf: str) -> str:
    s = re.sub(r"([a-z])([A-Z])", r"\1 \2", leaf).replace("_", " ")
    return (s[:1].upper() + s[1:]) if s else leaf


def main() -> int:
    if not BASE_CATALOG.exists():
        raise SystemExit(f"Нет baseline: {BASE_CATALOG}")
    if not SPOD_DIR.is_dir():
        raise SystemExit(f"Нет каталога: {SPOD_DIR}")

    base = json.loads(BASE_CATALOG.read_text(encoding="utf-8"))
    sections: List[Dict[str, Any]] = list(base.get("sections") or [])
    by_id = {s["id"]: s for s in sections}

    flat_cols: Dict[str, Set[str]] = defaultdict(set)
    json_stats: Dict[str, Dict[str, Counter]] = defaultdict(lambda: defaultdict(Counter))
    scanned: List[str] = []

    for sheet, (prefix, table_id) in SHEETS.items():
        for path in files_for(sheet, prefix):
            scanned.append(path.name)
            cols, rows = read_rows(path)
            for c in cols:
                if c:
                    flat_cols[table_id].add(c)
            for (sh, col), (sec_id, mode) in JSON_MAP.items():
                if sh != sheet or col not in cols:
                    continue
                for row in rows:
                    parsed = parse_spod_json(row.get(col))
                    if parsed is None:
                        continue
                    if mode == "object" and isinstance(parsed, dict):
                        collect_obj(parsed, sec_id, json_stats[sec_id])
                    elif mode == "array" and isinstance(parsed, list):
                        collect_arr(parsed, sec_id, json_stats[sec_id])

    next_n = max_n(sections) + 1
    added: List[str] = []

    for table_id, cols in flat_cols.items():
        sec = by_id.get(table_id)
        if not sec:
            continue
        existing = index_fields(sec)
        for col in sorted(cols):
            if col in existing:
                continue
            is_json_shell = any(col == c for (_, c) in JSON_MAP)
            kind = "json" if is_json_shell else "text"
            if col.endswith("_DT") or col.endswith("_DATE"):
                kind = "date"
            sec.setdefault("fields", []).append(
                {
                    "n": next_n,
                    "key": col,
                    "status": "[ ]",
                    "label": col,
                    "description": f"Колонка из PROM SPOD («{col}»). Добавлено автосканом — описание уточнить.",
                    "kind": kind,
                    "variants": [],
                    "default": "",
                    "allow_empty": True,
                    "json_target": col if is_json_shell else "",
                    "note": "auto: PROM SPOD scan",
                }
            )
            next_n += 1
            added.append(f"{table_id}.{col}")

    for sec_id, leaf_map in json_stats.items():
        sec = by_id.get(sec_id)
        if not sec:
            continue
        existing = index_fields(sec)
        for full, counter in sorted(leaf_map.items()):
            # полный путь относительно section: CONTEST_FEATURE.vid или REWARD_ADD_DATA.getCondition.rewards
            if not full.startswith(sec_id + "."):
                continue
            rel = full[len(sec_id) + 1 :]
            if not rel or rel.startswith("__"):
                continue
            key = f"{sec_id}.{rel}"
            if key in existing:
                continue
            # одноуровневый leaf: не дублировать уже известный ключ section.leaf
            if "." not in rel and any(
                k == f"{sec_id}.{rel}" or k.endswith("." + rel) for k in existing
            ):
                continue

            leaf = rel.split(".")[-1]
            known = KNOWN_META.get(leaf, {}) if "." not in rel else KNOWN_META.get(rel, {})
            # для вложенных — KNOWN по полному rel или последнему leaf
            if not known:
                known = KNOWN_META.get(rel, {}) or KNOWN_META.get(leaf, {})

            g_kind, variants, vlabels, default, allow = guess_kind(counter)
            kind = str(known.get("kind") or g_kind)
            if known.get("variants"):
                variants = list(known["variants"])
            if known.get("variant_labels"):
                vlabels = list(known["variant_labels"])
            label = str(known.get("label") or human_label(leaf))
            description = str(
                known.get("description")
                or f"Ключ JSON «{rel}» из PROM SPOD. Встречается в выгрузках; описание уточнить."
            )
            json_target = (
                f"{sec_id}[].{rel}" if sec.get("kind") == "json_array" else f"{sec_id}.{rel}"
            )
            field: Dict[str, Any] = {
                "n": next_n,
                "key": key,
                "status": "[ ]",
                "label": label,
                "description": description,
                "kind": kind,
                "variants": variants,
                "default": str(default or ""),
                "allow_empty": allow,
                "json_target": json_target,
                "note": "auto: PROM SPOD scan",
                "json_required": False,
            }
            if vlabels:
                field["variant_labels"] = vlabels
            sec.setdefault("fields", []).append(field)
            existing[key] = field
            next_n += 1
            added.append(key)

    stamp = _stamp()
    out = {
        "version": int(base.get("version") or 2) + 1,
        "generated_at": stamp,
        "exported_at": stamp,
        "source": (
            f"web-edit-full: baseline {BASE_CATALOG.name} + полный скан IN/PROM/SPOD "
            f"({len(scanned)} csv)"
        ),
        "scan_files": scanned,
        "added_fields": added,
        "sections": sections,
    }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    text = json.dumps(out, ensure_ascii=False, indent=2) + "\n"
    OUT_CATALOG.write_text(text, encoding="utf-8")
    snap = OUT_DIR / f"game_edit_catalog_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
    snap.write_text(text, encoding="utf-8")
    print(f"CSV: {len(scanned)}; добавлено полей: {len(added)}")
    for a in added:
        print(" +", a)
    print("→", OUT_CATALOG)
    print("→", snap)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
