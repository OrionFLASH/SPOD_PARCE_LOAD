# -*- coding: utf-8 -*-
"""
Сборка единого Markdown-каталога по CSV в IN/SPOD: колонки, варианты значений,
вложенные разделы по JSON (REWARD_ADD_DATA, CONTEST_FEATURE, GROUP_VALUE, SCHEDULE, USER_ROLE).
Запуск из корня проекта: python src/Tools/build_spod_input_catalog.py
"""
from __future__ import annotations

import csv
import json
import re
from datetime import date
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

# --- пути относительно корня проекта ---
ROOT = Path(__file__).resolve().parents[2]
IN_SPOD = ROOT / "IN" / "SPOD"
OUT_MD = ROOT / "Docs" / "JSON" / "SPOD_INPUT_DATA_CATALOG.md"
GLOSSARY_DIR = ROOT / "src" / "Tools" / "catalog_glossary"

DELIM = ";"
ENC = "utf-8"

MAX_UNIQ_SHOW: int = 55
MAX_LONG_COL_SAMPLES: int = 8
LONG_TEXT_THRESHOLD: int = 160
JSON_SHORT_STR: int = 100
JSON_MAX_VALUES: int = 45


def slug_anchor(name: str) -> str:
    """Якорь оглавления: латиница и дефисы."""
    s = name.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s, flags=re.I)
    return s.strip("-") or "section"


def normalize_json_cell(raw: str) -> str:
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
            if len(self.num_samples) < JSON_MAX_VALUES:
                self.num_samples.add(s)
        elif isinstance(v, str):
            self.str_max_len = max(self.str_max_len, len(v))
            if len(v) <= JSON_SHORT_STR and len(self.str_values) < JSON_MAX_VALUES * 2:
                self.str_values.add(v)
            elif len(self.str_values) < JSON_MAX_VALUES:
                self.str_values.add(v[:JSON_SHORT_STR] + "…")

    def add_array_meta(self, length: int) -> None:
        self.array_len_samples.append(length)
        if len(self.array_len_samples) > 2000:
            self.array_len_samples = self.array_len_samples[-1000:]


def walk(obj: Any, path: str, by_path: Dict[str, PathStats]) -> None:
    if path not in by_path:
        by_path[path] = PathStats()
    if isinstance(obj, dict):
        by_path[path].types["object"] += 1
        by_path[path].row_count += 1
        for k, v in obj.items():
            child = merge_path(path, k)
            if isinstance(v, dict):
                walk(v, child, by_path)
            elif isinstance(v, list):
                walk_array(v, child, by_path)
            else:
                if child not in by_path:
                    by_path[child] = PathStats()
                by_path[child].add_scalar(v)
    elif isinstance(obj, list):
        walk_array(obj, path, by_path)
    else:
        by_path[path].add_scalar(obj)


def walk_array(arr: List[Any], path: str, by_path: Dict[str, PathStats]) -> None:
    p_stats = by_path.setdefault(path, PathStats())
    p_stats.types["array"] += 1
    p_stats.row_count += 1
    p_stats.add_array_meta(len(arr))
    if not arr:
        return
    first = arr[0]
    if all(isinstance(x, type(first)) for x in arr):
        if isinstance(first, dict):
            elem_path = f"{path}[]"
            for item in arr:
                if isinstance(item, dict):
                    walk(item, elem_path, by_path)
        elif isinstance(first, (str, int, float, bool)) or first is None:
            elem_path = f"{path}[]"
            st = by_path.setdefault(elem_path, PathStats())
            for item in arr:
                st.add_scalar(item)
        else:
            elem_path = f"{path}[]"
            for item in arr:
                walk(item, elem_path, by_path)


def format_json_path_stats(st: PathStats) -> str:
    parts: List[str] = [f"типы: `{dict(st.types)}`"]
    if st.str_max_len > JSON_SHORT_STR:
        parts.append(f"строки: **длинный текст** (макс. {st.str_max_len} симв.)")
    elif st.str_values:
        vals = sorted(st.str_values, key=lambda x: (len(x), x))[:JSON_MAX_VALUES]
        parts.append(f"примеры строк: `{vals}`")
    elif "string" in st.types:
        parts.append("строки: (редкие/длинные)")
    if st.bool_samples:
        parts.append(f"boolean: {sorted(st.bool_samples)}")
    if st.num_samples:
        parts.append(f"числа (примеры): {sorted(st.num_samples, key=lambda x: (len(x), x))[:25]}")
    if st.null_count:
        parts.append(f"null: {st.null_count}")
    if st.array_len_samples:
        parts.append(f"длина массива: min={min(st.array_len_samples)}, max={max(st.array_len_samples)}")
    return "; ".join(parts)


def _append_glossary(lines: List[str], filename: str) -> None:
    """Добавляет встроенный пояснительный фрагмент (после машинного разбора JSON)."""
    path = GLOSSARY_DIR / filename
    if path.is_file():
        lines.append("")
        lines.extend(path.read_text(encoding="utf-8").splitlines())
        lines.append("")


# Краткие назначения колонок по имени файла (для человекочитаемого каталога).
COLUMN_HINTS: Dict[str, Dict[str, str]] = {
    "REWARD (PROM) 20-03 v0.csv": {
        "REWARD_CODE": "Уникальный код награды (связь с REWARD-LINK и отчётами).",
        "REWARD_TYPE": "Тип: ITEM / BADGE / LABEL / CRYSTAL — задаёт схему JSON в ADD_DATA.",
        "FULL_NAME": "Краткое отображаемое название награды.",
        "REWARD_DESCRIPTION": "Полное текстовое описание / условия для пользователя.",
        "REWARD_CONDITION": "Код или класс условия начисления (в выборке 1 или 2).",
        "REWARD_COST": "Стоимость / «цена» в условных единицах (целое).",
        "REWARD_ADD_DATA": "JSON с признаками UI, рассылок, сезонов, связей; структура зависит от REWARD_TYPE.",
    },
    "CONTEST (PROM) 18-03 v0.csv": {
        "CONTEST_CODE": "Код конкурса (ключ связей GROUP, INDICATOR, REPORT, SCHEDULE).",
        "FULL_NAME": "Наименование конкурса.",
        "CREATE_DT": "Дата начала действия записи.",
        "CLOSE_DT": "Дата окончания (4000-01-01 — «без срока»).",
        "BUSINESS_STATUS": "Статус в бизнес-контуре (например АКТИВНЫЙ).",
        "CONTEST_TYPE": "Тип конкурса: влияет на набор полей в CONTEST_FEATURE (JSON).",
        "CONTEST_DESCRIPTION": "Текстовое описание.",
        "CONTEST_FEATURE": "JSON: вид промо, рассылки, фильтры ТБ/ГОСБ, feature-тексты и т.д.",
        "SHOW_INDICATOR": "Единица отображения индикатора (например шт., Факт).",
        "PRODUCT_GROUP": "Группа продукта / линейка в классификаторе.",
        "PRODUCT": "Продукт / тематика конкурса.",
        "CONTEST_SUBJECT": "Предмет конкурса (роль/объект).",
        "FACTOR_MARK_TYPE": "Тип отметки фактора (CRITERION и др.).",
        "CONTEST_INDICATOR_METHOD": "Метод расчёта по индикатору (INTEGRAL и др.).",
        "CONTEST_FACTOR_METHOD": "Метод фактора (FACT и др.).",
        "PLAN_METHOD_CODE": "Код метода планирования.",
        "PLAN_MOD_METOD": "Модификатор метода плана.",
        "PLAN_MOD_VALUE": "Значение модификатора плана.",
        "FACTOR_MATCH": "Правило сопоставления фактора.",
        "CONTEST_PERIOD": "Код или метка периода конкурса.",
        "TARGET_TYPE": "Тип целевой аудитории.",
        "SOURCE_UPD_FREQUENCY": "Периодичность обновления источника.",
        "CALC_TYPE": "Тип расчёта (код).",
        "BUSINESS_BLOCK": "Бизнес-блок(и), привязанные к конкурсу (часто JSON-массив в соседних полях конфига).",
        "FACT_POST_PROCESSING": "Постобработка факта (коды блоков и т.п.).",
    },
    "employee (PROM) 13-03 v0.csv": {
        "PERSON_NUMBER": "Табельный номер (20 знаков, ведущие нули).",
        "PERSON_NUMBER_ADD": "Дублирующий/нормализованный табельный номер.",
        "SURNAME": "Фамилия.",
        "FIRST_NAME": "Имя.",
        "MIDDLE_NAME": "Отчество.",
        "MANAGER_FULL_NAME": "ФИО руководителя (строкой).",
        "POSITION_NAME": "Наименование должности.",
        "TB_CODE": "Код территориального банка.",
        "GOSB_CODE": "Код ГОСБ (0 — аппарат ТБ).",
        "BUSINESS_BLOCK": "Код бизнес-блока сотрудника.",
        "PRIORITY_TYPE": "Тип приоритета (код, напр. 1).",
        "KPK_CODE": "Код КПК (если есть).",
        "KPK_NAME": "Наименование КПК.",
        "ROLE_CODE": "Код роли в системе промо.",
        "UCH_CODE": "Код участка/учёта (1/2 и т.д.).",
        "GENDER": "Пол (код).",
        "ORG_UNIT_CODE": "Код оргподразделения (связь с ORG_UNIT).",
    },
    "GROUP (PROM) 18-03 v0.csv": {
        "CONTEST_CODE": "Код конкурса.",
        "GROUP_CODE": "Код группы расчёта (BANK, TB, …).",
        "GROUP_VALUE": "Значение группы: `*`, код или JSON-массив (напр. `[38]`); см. разбор JSON ниже.",
        "GET_CALC_METHOD": "Метод получения расчёта (код).",
        "GET_CALC_CRITERION": "Критерий расчёта GET (код).",
        "ADD_CALC_CRITERION": "Доп. критерий расчёта.",
        "ADD_CALC_CRITERION_2": "Второй доп. критерий.",
        "BASE_CALC_CODE": "Базовый код метода расчёта (BANK, TB, …).",
    },
    "INDICATOR (PROM) 18-03 v0.csv": {
        "CONTEST_CODE": "Код конкурса.",
        "INDICATOR_CALC_TYPE": "Тип расчёта индикатора.",
        "INDICATOR_ADD_CALC_TYPE": "Доп. тип расчёта.",
        "FULL_NAME": "Полное имя / метка индикатора.",
        "INDICATOR_CODE": "Код индикатора (WAIT, RATING, …).",
        "INDICATOR_AGG_FUNCTION": "Агрегирующая функция.",
        "INDICATOR_WEIGHT": "Вес индикатора.",
        "INDICATOR_OBJECT": "Объект применения индикатора.",
        "INDICATOR_MARK_TYPE": "Тип отметки (RATING, …).",
        "INDICATOR_MATCH": "Условие совпадения (MIN, …).",
        "INDICATOR_VALUE": "Значение порога / константы.",
        "CONTEST_CRITERION": "Критерий конкурса.",
        "INDICATOR_FILTER": "Фильтр отбора по индикатору.",
        "CONTESTANT_SELECTION": "Правило выбора участников.",
        "CALC_TYPE": "Тип расчёта (числовой код).",
        "N": "Параметр N (порядковый или множитель в формуле).",
    },
    "ORG_UNIT_V20 20-03 v0.csv": {
        "TB_CODE": "Код территориального банка.",
        "TB_FULL_NAME": "Полное название ТБ.",
        "TB_SHORT_NAME": "Краткое название ТБ.",
        "GOSB_CODE": "Код ГОСБ.",
        "GOSB_NAME": "Полное название ГОСБ.",
        "GOSB_SHORT_NAME": "Краткое название ГОСБ.",
        "CLUSTER_CODE": "Код кластера.",
        "GROUPING_CODE": "Код группировки в иерархии.",
        "GOSB_CNT": "Счётчик ГОСБ (число).",
        "GROUPING_CNT": "Счётчик группировки.",
        "ORG_UNIT_CODE": "Уникальный код оргподразделения (ключ).",
    },
    "REPORT (PROM) 18-03 v0.csv": {
        "MANAGER_PERSON_NUMBER": "Табельный номер сотрудника (менеджер/участник отчёта).",
        "CONTEST_CODE": "Код конкурса.",
        "TOURNAMENT_CODE": "Код турнира/периода расчёта.",
        "CONTEST_DATE": "Дата среза показателя.",
        "PLAN_VALUE": "Плановое значение (число).",
        "FACT_VALUE": "Фактическое значение (число).",
        "priority_type": "Тип приоритета строки отчёта.",
    },
    "REWARD-LINK (PROM) 18-03 v0.csv": {
        "CONTEST_CODE": "Код конкурса.",
        "GROUP_CODE": "Код группы на конкурсе.",
        "REWARD_CODE": "Код награды, доступной в этой связке.",
    },
    "SCHEDULE (PROM) 18-03 v0.csv": {
        "TOURNAMENT_CODE": "Уникальный код турнира/слота расписания.",
        "PERIOD_TYPE": "Тип периода (текстовая метка).",
        "START_DT": "Дата начала периода.",
        "END_DT": "Дата окончания периода.",
        "RESULT_DT": "Дата публикации/фиксации результата.",
        "PLAN_PERIOD_START_DT": "Плановое начало периода.",
        "PLAN_PERIOD_END_DT": "Плановое окончание периода.",
        "CRITERION_MARK_TYPE": "Тип отметки критерия.",
        "CRITERION_MARK_VALUE": "Значение отметки критерия.",
        "FILTER_PERIOD_ARR": "JSON/массив фильтра периодов (если заполнено).",
        "TOURNAMENT_STATUS": "Статус турнира (АКТИВНЫЙ, УДАЛЕН, …).",
        "CONTEST_CODE": "Код родительского конкурса.",
        "TARGET_TYPE": "Тип цели: часто JSON-объект (напр. `seasonCode`); см. разбор JSON ниже.",
        "CALC_TYPE": "Тип расчёта.",
        "TRN_INDICATOR_FILTER": "Фильтр индикаторов турнира.",
    },
    "USER_ROLE (PROM) 13-03 v0.csv": {
        "RULE_NUM": "Номер правила роли.",
        "ROLE_CODE": "Код роли.",
        "ROLE_NAME": "Наименование роли.",
        "PERSON_NUMBER_ARR": "Список табельных номеров (JSON-массив или строка).",
        "STAGE_ETALONE_CODE_ARR": "Коды этапов (массив).",
        "POST_ETALONE_CODE_ARR": "Коды должностей/постов.",
        "DIV_CODE_ARR": "Коды подразделений.",
        "EXCLUDE_DIV_CODE_ARR": "Исключаемые коды подразделений.",
        "BUSINESS_BLOCK": "Бизнес-блок действия правила.",
        "UCH_CODE": "Код участка.",
        "ORG_UNIT_CODE": "Код оргподразделения.",
        "TB_CODE": "Код ТБ.",
        "GOSB_CODE": "Код ГОСБ.",
    },
}
# Актуальные имена выгрузок в IN/SPOD — те же подсказки, что и у предыдущих версий файлов.
COLUMN_HINTS["REWARD (PROM) 23-03 v3.csv"] = dict(COLUMN_HINTS["REWARD (PROM) 20-03 v0.csv"])
COLUMN_HINTS["CONTEST (PROM) 23-03 v3.csv"] = dict(COLUMN_HINTS["CONTEST (PROM) 18-03 v0.csv"])


def format_column_hints(fname: str, fieldnames: List[str]) -> List[str]:
    hints = COLUMN_HINTS.get(fname)
    if not hints:
        return []
    lines: List[str] = [
        "### Краткое назначение колонок",
        "",
        "| Колонка | Назначение |",
        "|---------|------------|",
    ]
    for col in fieldnames:
        desc = hints.get(col, "—")
        lines.append(f"| `{col}` | {desc} |")
    lines.append("")
    return lines


def infer_scalar_type(samples: List[str]) -> str:
    """Грубая оценка типа колонки по непустым значениям."""
    if not samples:
        return "пусто"
    ints_ok = floats_ok = 0
    for s in samples[:500]:
        try:
            int(s)
            ints_ok += 1
        except ValueError:
            pass
        try:
            float(s.replace(",", "."))
            floats_ok += 1
        except ValueError:
            pass
    n = min(len(samples), 500)
    if n and ints_ok == n:
        return "целое (строкой в CSV)"
    if n and floats_ok == n:
        return "число (строкой в CSV)"
    return "строка"


def analyze_flat_columns(
    rows: List[Dict[str, str]],
    fieldnames: List[str],
) -> List[str]:
    """Таблица Markdown по колонкам."""
    lines: List[str] = []
    lines.append("| Колонка | Тип (оценка) | Непустых | Уникальных* | Варианты / комментарий |")
    lines.append("|---------|--------------|----------|-------------|-------------------------|")
    n = len(rows)
    for col in fieldnames:
        vals = [(row.get(col) or "").strip() for row in rows]
        non_empty = [v for v in vals if v]
        cnt = Counter(non_empty)
        uniq = len(cnt)
        inferred = infer_scalar_type(non_empty)
        max_len = max((len(v) for v in non_empty), default=0)

        if uniq == 0:
            note = "все пусто"
        elif max_len > LONG_TEXT_THRESHOLD and uniq > MAX_UNIQ_SHOW:
            top = [p[0] for p in cnt.most_common(MAX_LONG_COL_SAMPLES)]
            short = [t[:LONG_TEXT_THRESHOLD] + "…" if len(t) > LONG_TEXT_THRESHOLD else t for t in top]
            note = f"**длинный текст**, до {max_len} симв.; примеры (обрезка): `{short}`; всего **{uniq}** уникальных"
        elif uniq > MAX_UNIQ_SHOW:
            examples = sorted(cnt.keys(), key=lambda x: (len(x), x))[:15]
            note = f"**высокая кардинальность** ({uniq} уник.); примеры: `{examples}`"
        else:
            listed = sorted(cnt.keys(), key=lambda x: (len(x), x))
            if len(listed) > MAX_UNIQ_SHOW:
                listed = listed[:MAX_UNIQ_SHOW]
            note = f"`{listed}`"
        lines.append(
            f"| `{col}` | {inferred} | {len(non_empty)}/{n} | {uniq} | {note} |"
        )
    lines.append("")
    lines.append(
        "\\* Уникальных по непустым значениям; для длинных текстов перечисление ограничено."
    )
    lines.append("")
    return lines


def json_sections_reward(rows: List[Dict[str, str]]) -> List[str]:
    lines: List[str] = []
    by_type: Dict[str, Dict[str, PathStats]] = defaultdict(lambda: defaultdict(PathStats))
    all_keys: Dict[str, Set[str]] = defaultdict(set)
    key_occ: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    row_by: Dict[str, int] = defaultdict(int)
    parse_ok: Dict[str, int] = defaultdict(int)

    for row in rows:
        rtype = (row.get("REWARD_TYPE") or "").strip() or "(пусто)"
        row_by[rtype] += 1
        raw = (row.get("REWARD_ADD_DATA") or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(normalize_json_cell(raw))
        except json.JSONDecodeError:
            continue
        parse_ok[rtype] += 1
        local: Dict[str, PathStats] = {}
        walk(data, "REWARD_ADD_DATA", local)
        for path, st in local.items():
            all_keys[rtype].add(path)
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
            key_occ[rtype][path] += 1

    lines.append("### JSON: колонка `REWARD_ADD_DATA`")
    lines.append("")
    lines.append(
        "Предобработка: в ячейке последовательность тройных кавычек заменяется на обычную `\"`, "
        "затем `json.loads`. Корень — один объект; пути ниже с префиксом `REWARD_ADD_DATA`."
    )
    lines.append("")
    lines.append("#### Сводка по `REWARD_TYPE`")
    lines.append("")
    lines.append("| REWARD_TYPE | строк | JSON распарсено |")
    lines.append("|-------------|-------|-----------------|")
    for t in sorted(row_by.keys(), key=lambda x: (-row_by[x], x)):
        lines.append(f"| {t} | {row_by[t]} | {parse_ok.get(t, 0)} |")
    lines.append("")

    all_t = sorted(row_by.keys())
    union: Set[str] = set()
    for t in all_t:
        union.update(all_keys[t])
    lines.append("#### Ключи JSON, встречающиеся не во всех типах награды")
    lines.append("")
    for key in sorted(union):
        have = [x for x in all_t if key in all_keys[x]]
        if len(have) < len(all_t):
            miss = [x for x in all_t if x not in have]
            lines.append(f"- `{key}`: есть в {have}; **нет** в {miss}")
    lines.append("")

    for rtype in sorted(all_t, key=lambda x: (-row_by[x], x)):
        lines.append(f"#### Тип награды `{rtype}` — дерево путей")
        lines.append("")
        paths = sorted(by_type[rtype].keys())
        tree: Dict[str, Any] = {}

        def add_to_tree(p: str) -> None:
            cur = tree
            for s in p.split("."):
                if s not in cur:
                    cur[s] = {}
                cur = cur[s]

        for p in paths:
            add_to_tree(p)

        def print_tree(node: Dict[str, Any], pref: str = "") -> None:
            for k in sorted(node.keys()):
                lines.append(f"{pref}- `{k}`")
                if node[k]:
                    print_tree(node[k], pref + "  ")

        print_tree(tree)
        lines.append("")
        lines.append(f"##### Листья и узлы (типы и варианты) — `{rtype}`")
        lines.append("")
        for path in paths:
            st = by_type[rtype][path]
            occ = key_occ[rtype].get(path, 0)
            lines.append(f"- **`{path}`** — в {occ} JSON; {format_json_path_stats(st)}")
        lines.append("")

    _append_glossary(lines, "REWARD_ADD_DATA_glossary.md")
    return lines


def json_sections_contest(rows: List[Dict[str, str]]) -> List[str]:
    lines: List[str] = []
    by_type: Dict[str, Dict[str, PathStats]] = defaultdict(lambda: defaultdict(PathStats))
    all_keys: Dict[str, Set[str]] = defaultdict(set)
    key_occ: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    row_by: Dict[str, int] = defaultdict(int)
    parse_ok: Dict[str, int] = defaultdict(int)

    for row in rows:
        ctype = (row.get("CONTEST_TYPE") or "").strip() or "(пусто)"
        row_by[ctype] += 1
        raw = (row.get("CONTEST_FEATURE") or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(normalize_json_cell(raw))
        except json.JSONDecodeError:
            continue
        parse_ok[ctype] += 1
        local: Dict[str, PathStats] = {}
        walk(data, "CONTEST_FEATURE", local)
        for path, st in local.items():
            all_keys[ctype].add(path)
            tgt = by_type[ctype][path]
            for t, c in st.types.items():
                tgt.types[t] = tgt.types.get(t, 0) + c
            tgt.str_values.update(st.str_values)
            tgt.str_max_len = max(tgt.str_max_len, st.str_max_len)
            tgt.num_samples.update(st.num_samples)
            tgt.bool_samples.update(st.bool_samples)
            tgt.null_count += st.null_count
            tgt.row_count += st.row_count
            tgt.array_len_samples.extend(st.array_len_samples)
            key_occ[ctype][path] += 1

    lines.append("### JSON: колонка `CONTEST_FEATURE`")
    lines.append("")
    lines.append(
        "Предобработка: тройные кавычки в CSV → обычные `\"`, затем `json.loads`. "
        "Пути с префиксом `CONTEST_FEATURE`."
    )
    lines.append("")
    lines.append("#### Сводка по `CONTEST_TYPE`")
    lines.append("")
    lines.append("| CONTEST_TYPE | строк | JSON распарсено |")
    lines.append("|--------------|-------|-----------------|")
    for t in sorted(row_by.keys(), key=lambda x: (-row_by[x], x)):
        lines.append(f"| {t} | {row_by[t]} | {parse_ok.get(t, 0)} |")
    lines.append("")

    all_t = sorted(row_by.keys())
    union: Set[str] = set()
    for t in all_t:
        union.update(all_keys[t])
    lines.append("#### Ключи JSON, встречающиеся не во всех типах конкурса")
    lines.append("")
    for key in sorted(union):
        have = [x for x in all_t if key in all_keys[x]]
        if len(have) < len(all_t):
            miss = [x for x in all_t if x not in have]
            lines.append(f"- `{key}`: есть в {have}; **нет** в {miss}")
    lines.append("")

    for ctype in sorted(all_t, key=lambda x: (-row_by[x], x)):
        lines.append(f"#### Тип конкурса `{ctype}` — дерево путей")
        lines.append("")
        paths = sorted(by_type[ctype].keys())
        tree: Dict[str, Any] = {}

        def add_to_tree(p: str) -> None:
            cur = tree
            for s in p.split("."):
                if s not in cur:
                    cur[s] = {}
                cur = cur[s]

        for p in paths:
            add_to_tree(p)

        def print_tree(node: Dict[str, Any], pref: str = "") -> None:
            for k in sorted(node.keys()):
                lines.append(f"{pref}- `{k}`")
                if node[k]:
                    print_tree(node[k], pref + "  ")

        print_tree(tree)
        lines.append("")
        lines.append(f"##### Листья и узлы — `{ctype}`")
        lines.append("")
        for path in paths:
            st = by_type[ctype][path]
            occ = key_occ[ctype].get(path, 0)
            lines.append(f"- **`{path}`** — в {occ} JSON; {format_json_path_stats(st)}")
        lines.append("")

    _append_glossary(lines, "CONTEST_FEATURE_glossary.md")
    return lines


def merge_path_stats_into(tgt: PathStats, st: PathStats) -> None:
    """Слияние статистики путей при обходе нескольких JSON из одной колонки."""
    for t, c in st.types.items():
        tgt.types[t] = tgt.types.get(t, 0) + c
    tgt.str_values.update(st.str_values)
    tgt.str_max_len = max(tgt.str_max_len, st.str_max_len)
    tgt.num_samples.update(st.num_samples)
    tgt.bool_samples.update(st.bool_samples)
    tgt.null_count += st.null_count
    tgt.row_count += st.row_count
    tgt.array_len_samples.extend(st.array_len_samples)


def json_sections_generic_columns(rows: List[Dict[str, str]], columns: List[str]) -> List[str]:
    """
    Разбор JSON в отдельных колонках (объект или массив в корне), как в примерах
    `Docs/JSON/examples` после `export_spod_json_examples.py`.
    Скаляры (число в ячейке) не включаются — только dict/list.
    """
    lines: List[str] = []
    for col in columns:
        by_path: Dict[str, PathStats] = {}
        key_occ: Dict[str, int] = defaultdict(int)
        parse_n = 0
        for row in rows:
            raw = (row.get(col) or "").strip()
            if not raw:
                continue
            try:
                data = json.loads(normalize_json_cell(raw))
            except json.JSONDecodeError:
                continue
            if not isinstance(data, (dict, list)):
                continue
            parse_n += 1
            local: Dict[str, PathStats] = {}
            walk(data, col, local)
            for path, st in local.items():
                tgt = by_path.setdefault(path, PathStats())
                merge_path_stats_into(tgt, st)
                key_occ[path] += 1
        if parse_n == 0:
            continue
        lines.append(f"### JSON: колонка `{col}`")
        lines.append("")
        lines.append(
            "Предобработка: тройные кавычки в CSV → обычные `\"`, затем `json.loads`. "
            f"Учитываются только значения с корнем **object** или **array**; "
            f"распарсено **{parse_n}** ячеек из {len(rows)} строк."
        )
        lines.append("")
        paths = sorted(by_path.keys())
        tree: Dict[str, Any] = {}

        def add_to_tree(p: str) -> None:
            cur = tree
            for s in p.split("."):
                if s not in cur:
                    cur[s] = {}
                cur = cur[s]

        for p in paths:
            add_to_tree(p)

        def print_tree(node: Dict[str, Any], pref: str = "") -> None:
            for k in sorted(node.keys()):
                lines.append(f"{pref}- `{k}`")
                if node[k]:
                    print_tree(node[k], pref + "  ")

        print_tree(tree)
        lines.append("")
        lines.append(f"##### Листья и узлы — `{col}`")
        lines.append("")
        for path in paths:
            st = by_path[path]
            occ = key_occ[path]
            lines.append(f"- **`{path}`** — в {occ} JSON; {format_json_path_stats(st)}")
        lines.append("")
    return lines


def read_csv_all(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    with path.open("r", encoding=ENC, newline="") as f:
        reader = csv.DictReader(f, delimiter=DELIM)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)
    return fieldnames, rows


def section_file(
    rel_name: str,
    path: Path,
    extra: Optional[Callable[[List[Dict[str, str]]], List[str]]] = None,
) -> Tuple[str, List[str]]:
    """Заголовок раздела и тело (строки)."""
    anchor = slug_anchor(rel_name.replace(".csv", "").replace(" ", "-"))
    if not path.is_file():
        body = [f"_Файл не найден: `{path}`_", ""]
        return anchor, body

    fieldnames, rows = read_csv_all(path)
    lines: List[str] = []
    lines.append(f'<a id="{anchor}"></a>')
    lines.append("")
    lines.append(f"## Файл: `{rel_name}`")
    lines.append("")
    lines.append(f"- **Путь:** `IN/SPOD/{rel_name}`")
    lines.append(f"- **Строк данных:** {len(rows)} (без учёта заголовка)")
    lines.append(f"- **Разделитель:** `{DELIM}`, кодировка UTF-8")
    lines.append(f"- **Колонок:** {len(fieldnames)}")
    lines.append("")
    lines.extend(format_column_hints(rel_name, fieldnames))
    lines.append("### Плоские колонки (статистика значений)")
    lines.append("")
    lines.extend(analyze_flat_columns(rows, fieldnames))
    if extra:
        lines.extend(extra(rows))
    return anchor, lines


def build_catalog() -> str:
    toc_entries: List[Tuple[str, str]] = []
    sections: List[str] = []

    files_config: List[Tuple[str, str, Optional[Callable[[List[Dict[str, str]]], List[str]]]]] = [
        ("REWARD (PROM) 23-03 v3.csv", "REWARD (PROM) 23-03 v3.csv", json_sections_reward),
        ("CONTEST (PROM) 23-03 v3.csv", "CONTEST (PROM) 23-03 v3.csv", json_sections_contest),
        ("employee (PROM) 13-03 v0.csv", "employee (PROM) 13-03 v0.csv", None),
        (
            "GROUP (PROM) 18-03 v0.csv",
            "GROUP (PROM) 18-03 v0.csv",
            lambda r: json_sections_generic_columns(r, ["GROUP_VALUE"]),
        ),
        ("INDICATOR (PROM) 18-03 v0.csv", "INDICATOR (PROM) 18-03 v0.csv", None),
        ("ORG_UNIT_V20 20-03 v0.csv", "ORG_UNIT_V20 20-03 v0.csv", None),
        ("REPORT (PROM) 18-03 v0.csv", "REPORT (PROM) 18-03 v0.csv", None),
        ("REWARD-LINK (PROM) 18-03 v0.csv", "REWARD-LINK (PROM) 18-03 v0.csv", None),
        (
            "SCHEDULE (PROM) 18-03 v0.csv",
            "SCHEDULE (PROM) 18-03 v0.csv",
            lambda r: json_sections_generic_columns(r, ["TARGET_TYPE", "FILTER_PERIOD_ARR"]),
        ),
        (
            "USER_ROLE (PROM) 13-03 v0.csv",
            "USER_ROLE (PROM) 13-03 v0.csv",
            lambda r: json_sections_generic_columns(
                r,
                [
                    "PERSON_NUMBER_ARR",
                    "STAGE_ETALONE_CODE_ARR",
                    "POST_ETALONE_CODE_ARR",
                    "DIV_CODE_ARR",
                    "EXCLUDE_DIV_CODE_ARR",
                ],
            ),
        ),
    ]

    for title, fname, extra in files_config:
        path = IN_SPOD / fname
        anchor, body = section_file(fname, path, extra)
        toc_entries.append((title, anchor))
        sections.append("\n".join(body))

    header: List[str] = [
        "# Каталог входных данных SPOD (CSV, `IN/SPOD`)",
        "",
        "Единый справочник по листам выгрузки: **все колонки**, краткое **назначение**, оценка типа данных, "
        "**варианты значений** (с ограничениями для длинных текстов и высокой кардинальности). "
        "Для **REWARD** (`REWARD_ADD_DATA`) и **CONTEST** (`CONTEST_FEATURE`) — машинный разбор JSON "
        "(деревья, типы, варианты по `REWARD_TYPE` / `CONTEST_TYPE`) и **пояснительный справочник полей**. "
        "Для **GROUP** (`GROUP_VALUE`), **SCHEDULE** (`TARGET_TYPE`, `FILTER_PERIOD_ARR`), **USER_ROLE** "
        "(массивы кодов в колонках `*_ARR`) — дополнительно деревья путей для ячеек с JSON-объектом/массивом "
        "(как в `Docs/JSON/examples`).",
        "",
        "**Пересборка документа:** `python src/Tools/build_spod_input_catalog.py`",
        "",
        "**Примеры JSON:** каталог **`Docs/JSON/examples/`** — один CSV выгрузки → один `.json` с тем же именем; см. **`Docs/JSON/README.md`**. "
        "Команда: `python src/Tools/export_spod_json_examples.py`.",
        "",
        "## Оглавление",
        "",
    ]
    for title, anchor in toc_entries:
        header.append(f"- [{title}](#{anchor})")
    header.append("")
    header.append("---")
    header.append("")

    footer = "\n---\n\n## Мета\n\n"
    footer += f"- **Дата сборки каталога:** {date.today().isoformat()}\n"
    footer += "- **Источник данных:** файлы в `IN/SPOD/` на момент запуска `build_spod_input_catalog.py`.\n"
    footer += "- **Глоссарии JSON:** `src/Tools/catalog_glossary/` (правки вручную при необходимости).\n"
    footer += (
        "- **Примеры JSON:** `Docs/JSON/examples/` — по одному файлу на каждую выгрузку из "
        "`export_spod_json_examples.py` (структура `columns` + `rows`; вложенный JSON в ячейках как в каталоге).\n"
    )
    return "\n".join(header) + "\n".join(sections) + footer


def main() -> None:
    text = build_catalog()
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text(text, encoding="utf-8")
    print(f"Записано: {OUT_MD}")


if __name__ == "__main__":
    main()
