# -*- coding: utf-8 -*-
"""Загрузка catalog.json (web-edit) как оверлея метаданных полей формы."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# key формы → overlay
_fields_by_key: Dict[str, Dict[str, Any]] = {}
# (table_key, col_name) → overlay
_table_fields: Dict[Tuple[str, str], Dict[str, Any]] = {}
_catalog_path: str = ""


def clear_param_catalog() -> None:
    """Сбросить загруженный каталог."""
    global _catalog_path
    _fields_by_key.clear()
    _table_fields.clear()
    _catalog_path = ""


def is_catalog_loaded() -> bool:
    return bool(_fields_by_key)


def catalog_source_path() -> str:
    return _catalog_path


def _normalize_field(raw: Dict[str, Any]) -> Dict[str, Any]:
    variants = raw.get("variants") or []
    if isinstance(variants, str):
        variants = [v.strip() for v in variants.split(",") if v.strip()]
    else:
        variants = [str(v).strip() for v in variants if str(v).strip()]
    labels_raw = raw.get("variant_labels") or []
    if isinstance(labels_raw, str):
        labels_raw = [x.strip() for x in labels_raw.split("\n")]
    else:
        labels_raw = [str(x).strip() if x is not None else "" for x in labels_raw]
    # Подписи по индексу; лишние отбрасываем, недостающие — пустые
    variant_labels = [
        (labels_raw[i] if i < len(labels_raw) else "") for i in range(len(variants))
    ]
    kind = str(raw.get("kind") or "").strip().lower() or None
    out: Dict[str, Any] = {
        "key": str(raw.get("key") or "").strip(),
        "label": str(raw.get("label") or "").strip(),
        "description": str(raw.get("description") or "").strip(),
        "kind": kind,
        "variants": variants,
        "default": "" if raw.get("default") is None else str(raw.get("default")),
        "allow_empty": bool(raw.get("allow_empty", True)),
        "note": str(raw.get("note") or "").strip(),
    }
    if any(variant_labels):
        out["variant_labels"] = variant_labels
    return out


def load_param_catalog(path: str | Path) -> int:
    """
    Загрузить catalog.json веб-редактора.
    Возвращает число проиндексированных полей.
    """
    clear_param_catalog()
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(f"catalog.json не найден: {p}")
    data = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or not isinstance(data.get("sections"), list):
        raise ValueError(f"В {p} нет sections[]")

    global _catalog_path
    _catalog_path = str(p.resolve())
    n = 0
    for sec in data["sections"]:
        if not isinstance(sec, dict):
            continue
        sid = str(sec.get("id") or "")
        fields = sec.get("fields") or []
        if not isinstance(fields, list):
            continue
        table_key = ""
        if sid.startswith("TABLE:"):
            table_key = sid[6:].strip().upper()
            if table_key == "REWARD_LINK":
                table_key = "REWARD-LINK"
        for raw in fields:
            if not isinstance(raw, dict):
                continue
            entry = _normalize_field(raw)
            key = entry["key"]
            if not key:
                continue
            if table_key:
                _table_fields[(table_key, key)] = entry
            else:
                if sid == "REWARD" and key == "FULL_NAME":
                    _fields_by_key["REWARD.FULL_NAME"] = entry
                elif key not in _fields_by_key:
                    _fields_by_key[key] = entry
                else:
                    # не затирать CONTEST и др.; доп. ключ по секции
                    _fields_by_key[f"{sid}.{key}"] = entry
            n += 1

    logging.info(
        "[catalog_loader] Загружено полей: %s из %s",
        n,
        _catalog_path,
    )
    return n


def field_overlay(key: str) -> Optional[Dict[str, Any]]:
    """Оверлей для KV-ключа формы (CONTEST / FEATURE.* / ADD.* / REWARD)."""
    return _fields_by_key.get(key)


def table_overlay(table_key: str, col_name: str) -> Optional[Dict[str, Any]]:
    """Оверлей для колонки таблицы (#TABLE:…)."""
    tk = str(table_key or "").strip().upper()
    if tk == "REWARD_LINK":
        tk = "REWARD-LINK"
    return _table_fields.get((tk, str(col_name)))


def iter_kv_overlays() -> Iterable[Tuple[str, Dict[str, Any]]]:
    return _fields_by_key.items()


def iter_table_overlays() -> Iterable[Tuple[str, str, Dict[str, Any]]]:
    for (tk, col), ov in _table_fields.items():
        yield tk, col, ov


def dropdown_overrides_from_catalog() -> Dict[str, List[str]]:
    """Ключ формы → variants (только непустые списки KV-полей)."""
    out: Dict[str, List[str]] = {}
    for key, ov in _fields_by_key.items():
        variants = ov.get("variants") or []
        if variants:
            out[key] = list(variants)
    return out


def table_dropdown_overrides_from_catalog() -> Dict[str, Dict[str, List[str]]]:
    """table_key → {col → variants}."""
    out: Dict[str, Dict[str, List[str]]] = {}
    for tk, col, ov in iter_table_overlays():
        variants = ov.get("variants") or []
        if not variants:
            continue
        out.setdefault(tk, {})[col] = list(variants)
    return out
