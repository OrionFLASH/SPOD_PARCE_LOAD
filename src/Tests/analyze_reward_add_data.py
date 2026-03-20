# -*- coding: utf-8 -*-
"""
Разовый анализ CSV REWARD: колонка REWARD_ADD_DATA после замены \"\"\" -> \".
Результат: Markdown-отчёт со структурой JSON по REWARD_TYPE.
"""
from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# Пороги для вывода
SHORT_STR_MAX_LEN: int = 100  # длиннее — помечаем как «длинный текст»
MAX_VALUES_TO_LIST: int = 50  # не больше стольких вариантов перечисляем
MAX_ARRAY_SAMPLE_ITEMS: int = 5  # примеры элементов массива


def normalize_json_cell(raw: str) -> str:
    """Тройные кавычки в данных заменяются на обычные — как в пайплайне."""
    return raw.replace('"""', '"').strip()


def type_label(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "boolean"
    if isinstance(v, int) and not isinstance(v, bool):
        return "integer"
    if isinstance(v, float):
        return "number"
    if isinstance(v, str):
        return "string"
    if isinstance(v, list):
        return "array"
    if isinstance(v, dict):
        return "object"
    return type(v).__name__


def merge_path(base: str, key: str) -> str:
    if not base:
        return key
    return f"{base}.{key}"


class PathStats:
    """Накопление статистики по JSON-пути."""

    def __init__(self) -> None:
        self.types: Dict[str, int] = defaultdict(int)
        self.str_values: Set[str] = set()
        self.str_max_len: int = 0
        self.num_samples: Set[str] = set()
        self.bool_samples: Set[str] = set()
        self.null_count: int = 0
        self.row_count: int = 0
        self.array_len_samples: List[int] = []

    def add_scalar(self, v: Any) -> None:
        self.row_count += 1
        self.types[type_label(v)] += 1
        if v is None:
            self.null_count += 1
        elif isinstance(v, bool):
            self.bool_samples.add(str(v).lower())
        elif isinstance(v, (int, float)) and not isinstance(v, bool):
            s = str(v)
            if len(self.num_samples) < MAX_VALUES_TO_LIST:
                self.num_samples.add(s)
        elif isinstance(v, str):
            self.str_max_len = max(self.str_max_len, len(v))
            if len(v) <= SHORT_STR_MAX_LEN and len(self.str_values) < MAX_VALUES_TO_LIST * 2:
                self.str_values.add(v)
            elif len(self.str_values) < MAX_VALUES_TO_LIST:
                self.str_values.add(v[:SHORT_STR_MAX_LEN] + "…")

    def add_array_meta(self, length: int) -> None:
        self.array_len_samples.append(length)
        if len(self.array_len_samples) > 2000:
            self.array_len_samples = self.array_len_samples[-1000:]


def walk(
    obj: Any,
    path: str,
    by_path: Dict[str, PathStats],
    array_of_object_paths: Set[str],
) -> None:
    """Обход значения; пути вида a.b, a[].field для массивов объектов."""
    if path not in by_path:
        by_path[path] = PathStats()

    if isinstance(obj, dict):
        by_path[path].types["object"] += 1
        by_path[path].row_count += 1
        for k, v in obj.items():
            child = merge_path(path, k)
            if isinstance(v, dict):
                walk(v, child, by_path, array_of_object_paths)
            elif isinstance(v, list):
                walk_array(v, child, by_path, array_of_object_paths)
            else:
                if child not in by_path:
                    by_path[child] = PathStats()
                by_path[child].add_scalar(v)
    elif isinstance(obj, list):
        walk_array(obj, path, by_path, array_of_object_paths)
    else:
        by_path[path].add_scalar(obj)


def walk_array(
    arr: List[Any],
    path: str,
    by_path: Dict[str, PathStats],
    array_of_object_paths: Set[str],
) -> None:
    p_stats = by_path.setdefault(path, PathStats())
    p_stats.types["array"] += 1
    p_stats.row_count += 1
    p_stats.add_array_meta(len(arr))

    if not arr:
        return

    first = arr[0]
    if all(isinstance(x, type(first)) for x in arr):
        if isinstance(first, dict):
            array_of_object_paths.add(path)
            elem_path = f"{path}[]"
            for item in arr:
                if isinstance(item, dict):
                    walk(item, elem_path, by_path, array_of_object_paths)
        elif isinstance(first, (str, int, float, bool)) or first is None:
            elem_path = f"{path}[]"
            st = by_path.setdefault(elem_path, PathStats())
            for item in arr:
                st.add_scalar(item)
        else:
            elem_path = f"{path}[]"
            for item in arr:
                walk(item, elem_path, by_path, array_of_object_paths)


def format_path_stats(st: PathStats) -> str:
    parts: List[str] = []
    parts.append(f"типы: {dict(st.types)}")
    if st.str_max_len > SHORT_STR_MAX_LEN:
        parts.append(f"строки: длинный текст (макс. длина {st.str_max_len})")
    elif st.str_values:
        vals = sorted(st.str_values, key=lambda x: (len(x), x))[:MAX_VALUES_TO_LIST]
        parts.append(f"строки (до {SHORT_STR_MAX_LEN} симв.): {vals}")
    elif "string" in st.types:
        parts.append("строки: (пусто или не попали в выборку)")
    if st.bool_samples:
        parts.append(f"boolean: {sorted(st.bool_samples)}")
    if st.num_samples:
        parts.append(f"числа (примеры): {sorted(st.num_samples, key=lambda x: (len(x), x))[:30]}")
    if st.null_count:
        parts.append(f"null: {st.null_count} раз(а) в листьях")
    if st.array_len_samples:
        mn, mx = min(st.array_len_samples), max(st.array_len_samples)
        parts.append(f"длина массива: min={mn}, max={mx}")
    return "; ".join(parts)


def build_tree_lines(
    paths: Set[str],
    array_obj: Set[str],
    indent: str = "  ",
) -> List[str]:
    """Упорядоченное дерево по префиксам путей (без повторного вывода листьев)."""
    paths_sorted = sorted(paths)
    lines: List[str] = []
    seen_prefix: Set[str] = set()

    def is_under(prefix: str, p: str) -> bool:
        return p == prefix or p.startswith(prefix + ".")

    for p in paths_sorted:
        skip = False
        for pref in seen_prefix:
            if is_under(pref, p) and p != pref:
                # если pref — объект и p его потомок, не дублируем короткими строками
                pass
        parts = p.split(".")
        for i in range(len(parts)):
            pref = ".".join(parts[: i + 1])
            if pref in seen_prefix:
                continue
            # показываем этот узел, если это первое вхождение префикса
            if not any(pref.startswith(s + ".") for s in seen_prefix if s != pref):
                depth = i
                name = parts[i]
                if name.endswith("[]"):
                    name = name
                lines.append(indent * depth + "- " + pref)
                seen_prefix.add(pref)
        # упрощённо: просто выводим каждый путь один раз как линию с отступом по глубине
    lines.clear()
    for p in paths_sorted:
        depth = p.count(".")
        lines.append(indent * depth + "└─ " + p)
    return lines


def analyze_file(csv_path: Path, out_md: Path) -> Tuple[int, int, List[str]]:
    by_type: Dict[str, Dict[str, PathStats]] = defaultdict(lambda: defaultdict(PathStats))
    type_parse_ok: Dict[str, int] = defaultdict(int)
    type_parse_fail: Dict[str, int] = defaultdict(int)
    errors: List[str] = []
    all_keys_by_type: Dict[str, Set[str]] = defaultdict(set)
    key_presence_by_type: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    row_by_type: Dict[str, int] = defaultdict(int)

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        if not reader.fieldnames or "REWARD_ADD_DATA" not in reader.fieldnames:
            raise SystemExit("Нет колонки REWARD_ADD_DATA")
        rtype_col = "REWARD_TYPE" if "REWARD_TYPE" in reader.fieldnames else None

        for row in reader:
            rtype = (row.get(rtype_col) or "").strip() or "(пусто)"
            row_by_type[rtype] += 1
            raw = row.get("REWARD_ADD_DATA") or ""
            raw = raw.strip()
            if not raw:
                type_parse_fail[rtype] += 1
                errors.append(f"{row.get('REWARD_CODE','?')}: пустой REWARD_ADD_DATA")
                continue
            normalized = normalize_json_cell(raw)
            try:
                data = json.loads(normalized)
            except json.JSONDecodeError as e:
                type_parse_fail[rtype] += 1
                errors.append(f"{row.get('REWARD_CODE','?')}: JSON {e}")
                continue

            type_parse_ok[rtype] += 1
            array_obj: Set[str] = set()
            local_paths: Dict[str, PathStats] = {}
            walk(data, "REWARD_ADD_DATA", local_paths, array_obj)

            for path, st in local_paths.items():
                all_keys_by_type[rtype].add(path)
                tgt = by_type[rtype][path]
                for t, c in st.types.items():
                    tgt.types[t] = tgt.types.get(t, 0) + c
                tgt.str_values.update(st.str_values)
                tgt.str_max_len = max(tgt.str_max_len, st.str_max_len)
                tgt.num_samples.update(st.num_samples)
                tgt.bool_samples.update(st.bool_samples)
                tgt.null_count += st.null_count
                tgt.row_count += st.row_count
                tgt.array_len_samples.extend(st.array_len_samples)
                key_presence_by_type[rtype][path] += 1

    # Markdown
    lines: List[str] = []
    lines.append("# Анализ REWARD_ADD_DATA (JSON) по файлу REWARD CSV")
    lines.append("")
    lines.append(f"**Файл:** `{csv_path.name}`")
    lines.append("")
    lines.append("## Предобработка")
    lines.append("")
    lines.append("- В ячейке последовательность `\\\"\\\"\\\"` заменяется на `\"` (как при разборе в программе).")
    lines.append("- Затем `json.loads`.")
    lines.append("")
    lines.append("## Сводка по строкам и разбору JSON")
    lines.append("")
    lines.append("| REWARD_TYPE | строк в CSV | JSON OK | JSON ошибка / пусто |")
    lines.append("|-------------|------------|---------|---------------------|")
    for rtype in sorted(row_by_type.keys(), key=lambda x: (-row_by_type[x], x)):
        ok = type_parse_ok.get(rtype, 0)
        bad = type_parse_fail.get(rtype, 0)
        lines.append(f"| {rtype} | {row_by_type[rtype]} | {ok} | {bad} |")
    lines.append("")

    if errors[:20]:
        lines.append("### Примеры ошибок разбора (до 20)")
        lines.append("")
        for e in errors[:20]:
            lines.append(f"- {e}")
        lines.append("")

    # Сравнение наборов ключей между типами
    lines.append("## Зависимость от REWARD_TYPE: наборы полей (путей)")
    lines.append("")
    all_types = sorted(row_by_type.keys())
    union_keys: Set[str] = set()
    for t in all_types:
        union_keys.update(all_keys_by_type[t])

    lines.append("### Ключи, встречающиеся не во всех типах")
    lines.append("")
    for key in sorted(union_keys):
        types_with = [t for t in all_types if key in all_keys_by_type[t]]
        if len(types_with) < len(all_types):
            missing = [t for t in all_types if t not in types_with]
            lines.append(f"- `{key}`: есть в {types_with}; **нет** в {missing}")
    lines.append("")

    lines.append("### Количество уникальных путей по типу")
    lines.append("")
    lines.append("| REWARD_TYPE | число путей |")
    lines.append("|-------------|------------|")
    for t in sorted(all_types, key=lambda x: -len(all_keys_by_type[x])):
        lines.append(f"| {t} | {len(all_keys_by_type[t])} |")
    lines.append("")

    # Детально по каждому REWARD_TYPE
    for rtype in sorted(all_types, key=lambda x: (-row_by_type[x], x)):
        lines.append(f"## Тип награды: `{rtype}`")
        lines.append("")
        lines.append(f"Строк с этим типом: **{row_by_type[rtype]}**; успешно распарсено JSON: **{type_parse_ok.get(rtype, 0)}**.")
        lines.append("")

        paths = sorted(by_type[rtype].keys())
        lines.append("### Дерево путей (иерархия ключей)")
        lines.append("")
        lines.append("```")
        # дерево: группируем по первому сегменту
        tree_root: Dict[str, Any] = {}

        def add_to_tree(p: str) -> None:
            segs = p.split(".")
            cur = tree_root
            for s in segs:
                if s not in cur:
                    cur[s] = {}
                cur = cur[s]

        for p in paths:
            add_to_tree(p)

        def print_tree(node: Dict[str, Any], prefix: str = "") -> None:
            for k in sorted(node.keys()):
                lines.append(prefix + k)
                if node[k]:
                    print_tree(node[k], prefix + "  ")

        print_tree(tree_root)
        lines.append("```")
        lines.append("")

        lines.append("### Листья и вложенные узлы: типы и варианты значений")
        lines.append("")
        for path in paths:
            st = by_type[rtype][path]
            occ = key_presence_by_type[rtype].get(path, 0)
            lines.append(f"#### `{path}`")
            lines.append("")
            lines.append(f"- встречается в **{occ}** разобранных JSON этого типа")
            lines.append(f"- {format_path_stats(st)}")
            lines.append("")

    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines), encoding="utf-8")
    return len(errors), sum(row_by_type.values()), errors


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    csv_path = root / "IN" / "SPOD" / "REWARD (PROM) 20-03 v0.csv"
    # Сырой отчёт; единый каталог по всем листам: src/Tools/build_spod_input_catalog.py
    out_md = root / "Docs" / "SPOD_MACHINE_REWARD_ADD_DATA.md"
    if len(sys.argv) >= 2:
        csv_path = Path(sys.argv[1])
    if len(sys.argv) >= 3:
        out_md = Path(sys.argv[2])
    if not csv_path.is_file():
        print(f"Файл не найден: {csv_path}", file=sys.stderr)
        sys.exit(1)
    n_err, n_rows, _ = analyze_file(csv_path, out_md)
    print(f"Строк: {n_rows}, проблем разбора: {n_err}")
    print(f"Отчёт: {out_md}")


if __name__ == "__main__":
    main()
