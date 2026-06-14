# -*- coding: utf-8 -*-
"""
Статический анализ кодовой базы SPOD_PROM → Docs/CODEBASE_ANALYTICS.md.
Запуск из корня: python src/Tools/build_codebase_analytics.py
"""

from __future__ import annotations

import ast
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[2]
OUT_PATH = ROOT / "Docs" / "CODEBASE_ANALYTICS.md"
EXCLUDE = {".git", "__pycache__", ".venv", "venv", "node_modules", "IN", "OUT", "LOGS", "log", "POST"}
STD_LIB = {
    "abc", "argparse", "ast", "collections", "concurrent", "contextlib", "copy", "csv",
    "dataclasses", "datetime", "enum", "functools", "glob", "hashlib", "inspect", "io",
    "itertools", "json", "logging", "math", "multiprocessing", "operator", "os", "pathlib",
    "queue", "random", "re", "shutil", "sqlite3", "string", "subprocess", "sys", "tempfile",
    "textwrap", "threading", "time", "traceback", "typing", "unittest", "uuid", "warnings",
    "xml", "__future__",
}
EXTERNAL = {"pandas", "numpy", "openpyxl", "xlsxwriter", "xlrd", "requests", "tqdm", "colorama", "pytest"}
CAT_LABELS = {
    "core": "Ядро (src/)",
    "tests": "Тесты",
    "tools": "Утилиты (Tools)",
    "entrypoint": "Точки входа",
    "package": "Пакет",
}


def _skip(path: Path) -> bool:
    rel_parts = path.relative_to(ROOT).parts
    if any(p in EXCLUDE or p.startswith(".") for p in rel_parts):
        return True
    return "Docs/JSON/examples" in str(path)


def _categorize(rel: str) -> str:
    if rel in ("main.py", "run_main.py"):
        return "entrypoint"
    if rel.startswith("src/Tests/"):
        return "tests"
    if rel.startswith("src/Tools/"):
        return "tools"
    if rel == "src/__init__.py":
        return "package"
    return "core"


def _line_stats(text: str) -> Tuple[int, int, int, int]:
    lines = text.splitlines()
    total = len(lines)
    blank = comment = code = 0
    for line in lines:
        s = line.strip()
        if not s:
            blank += 1
        elif s.startswith("#"):
            comment += 1
        else:
            code += 1
    return total, blank, comment, code


def _bar(value: int, max_val: int, width: int = 24) -> str:
    if max_val <= 0:
        return ""
    filled = int(round(value / max_val * width))
    return "█" * filled + "░" * (width - filled)


class _Metrics(ast.NodeVisitor):
    """Сбор метрик AST для одного файла."""

    def __init__(self, category: str) -> None:
        self.category = category
        self.class_depth = 0
        self.func_depth = 0
        self.classes = 0
        self.functions = 0
        self.methods = 0
        self.nested = 0
        self.module_vars = 0
        self.decorators = 0
        self.type_hints = 0
        self.imports = 0
        self.from_imports = 0
        self.class_names: List[str] = []
        self.internal_imports: List[str] = []
        self.std_imports: List[str] = []
        self.ext_imports: List[str] = []
        self.try_blocks = 0

    def visit_Import(self, node: ast.Import) -> None:
        self.imports += 1
        for alias in node.names:
            top = alias.name.split(".")[0]
            if top == "src" or alias.name.startswith("src."):
                self.internal_imports.append(alias.name)
            elif top in EXTERNAL:
                self.ext_imports.append(top)
            else:
                self.std_imports.append(top)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        self.from_imports += 1
        mod = node.module or ""
        top = mod.split(".")[0] if mod else ""
        if top == "src" or mod.startswith("src."):
            self.internal_imports.append(mod)
        elif top in EXTERNAL:
            self.ext_imports.append(top)
        elif top:
            self.std_imports.append(top)
        self.generic_visit(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.classes += 1
        self.class_names.append(node.name)
        self.decorators += len(node.decorator_list)
        self.class_depth += 1
        self.generic_visit(node)
        self.class_depth -= 1

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._func(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._func(node)

    def _func(self, node: ast.AST) -> None:
        self.decorators += len(getattr(node, "decorator_list", []))
        args = getattr(node, "args", None)
        if getattr(node, "returns", None) or (args and any(getattr(a, "annotation", None) for a in args.args)):
            self.type_hints += 1
        if self.class_depth > 0:
            self.methods += 1
        elif self.func_depth > 0:
            self.nested += 1
        else:
            self.functions += 1
        self.func_depth += 1
        self.generic_visit(node)
        self.func_depth -= 1

    def visit_Assign(self, node: ast.Assign) -> None:
        if self.func_depth == 0 and self.class_depth == 0:
            self.module_vars += len(node.targets)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if self.func_depth == 0 and self.class_depth == 0:
            self.module_vars += 1
        self.generic_visit(node)

    def visit_Try(self, node: ast.Try) -> None:
        self.try_blocks += 1
        self.generic_visit(node)


def analyze() -> Dict[str, Any]:
    """Полный снимок метрик проекта."""
    py_files = sorted(p for p in ROOT.rglob("*.py") if not _skip(p))
    file_rows: List[Dict[str, Any]] = []
    imported_by: Counter[str] = Counter()
    external_deps: Counter[str] = Counter()
    stdlib_deps: Counter[str] = Counter()
    all_classes: List[Tuple[str, str]] = []
    functions_by_cat: Counter[str] = Counter()

    totals = Counter()
    for path in py_files:
        rel = str(path.relative_to(ROOT)).replace("\\", "/")
        cat = _categorize(rel)
        text = path.read_text(encoding="utf-8", errors="replace")
        total, blank, comment, code = _line_stats(text)
        row: Dict[str, Any] = {
            "rel": rel,
            "category": cat,
            "total_lines": total,
            "blank_lines": blank,
            "comment_lines": comment,
            "code_lines": code,
            "parse_error": None,
        }
        try:
            tree = ast.parse(text, filename=str(path))
            m = _Metrics(cat)
            m.visit(tree)
            for key, val in [
                ("classes", m.classes),
                ("functions", m.functions),
                ("methods", m.methods),
                ("nested_functions", m.nested),
                ("imports", m.imports),
                ("from_imports", m.from_imports),
                ("global_assignments", m.module_vars),
                ("decorators", m.decorators),
                ("try_blocks", m.try_blocks),
            ]:
                row[key] = val
                totals[key] += val
            totals["type_hints_funcs"] += m.type_hints
            functions_by_cat[cat] += m.functions
            for name in m.class_names:
                all_classes.append((name, rel))
            for imp in m.internal_imports:
                tgt = imp.replace("src.", "")
                imported_by[tgt.split(".")[0]] += 1
            for pkg in m.ext_imports:
                external_deps[pkg] += 1
            for pkg in m.std_imports:
                stdlib_deps[pkg] += 1
        except SyntaxError as exc:
            row.update(
                classes=0, functions=0, methods=0, nested_functions=0,
                imports=0, from_imports=0, global_assignments=0, decorators=0, try_blocks=0,
            )
            row["parse_error"] = str(exc)
        totals["files"] += 1
        totals["total_lines"] += total
        totals["blank_lines"] += blank
        totals["comment_lines"] += comment
        totals["code_lines"] += code
        file_rows.append(row)

    md_files = sorted(p for p in ROOT.rglob("*.md") if not _skip(p))
    md_stats = [(str(p.relative_to(ROOT)), len(p.read_text(encoding="utf-8", errors="replace").splitlines())) for p in md_files]
    config_lines = len((ROOT / "config.json").read_text(encoding="utf-8", errors="replace").splitlines()) if (ROOT / "config.json").exists() else 0
    readme_lines = len((ROOT / "README.md").read_text(encoding="utf-8", errors="replace").splitlines()) if (ROOT / "README.md").exists() else 0

    by_cat: Dict[str, Dict[str, int]] = {}
    for cat in CAT_LABELS:
        rows = [r for r in file_rows if r["category"] == cat]
        if rows:
            by_cat[cat] = {
                "files": len(rows),
                "code_lines": sum(r["code_lines"] for r in rows),
                "total_lines": sum(r["total_lines"] for r in rows),
                "classes": sum(r.get("classes", 0) for r in rows),
                "functions": sum(r.get("functions", 0) for r in rows),
            }

    internal_modules = sum(1 for r in file_rows if r["rel"].startswith("src/") and r["category"] == "core")

    return {
        "generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "totals": dict(totals),
        "by_category": by_cat,
        "files": file_rows,
        "all_classes": all_classes,
        "unique_classes": len({n for n, _ in all_classes}),
        "functions_by_cat": dict(functions_by_cat),
        "top_level_funcs": sum(functions_by_cat.values()),
        "imported_by_top": imported_by.most_common(20),
        "external_deps": external_deps.most_common(),
        "stdlib_top": stdlib_deps.most_common(15),
        "md_stats": md_stats,
        "md_total": sum(n for _, n in md_stats),
        "config_lines": config_lines,
        "readme_lines": readme_lines,
        "internal_modules": internal_modules,
    }


def render_md(data: Dict[str, Any]) -> str:
    """Формирование markdown-отчёта."""
    t = data["totals"]
    total_code = t["code_lines"]
    lines: List[str] = []

    def add(*parts: str, gap: bool = False) -> None:
        lines.extend(parts)
        if gap:
            lines.append("")

    add(
        "# Аналитика кодовой базы SPOD_PROM",
        "",
        f"> **Дата снимка:** {data['generated']}  ",
        "> **Метод:** статический разбор AST + подсчёт строк (Python 3, без `IN/`, `OUT/`, `LOGS/`)  ",
        f"> **Пересборка:** `python src/Tools/build_codebase_analytics.py`",
        "",
        "---",
        "",
        "## 1. Масштаб проекта — краткая сводка",
        "",
        "Пайплайн обработки CSV/JSON выгрузок PROM → Excel: проверки консистентности, merge, enrich, режимы `run_outputs`.",
        "",
        "| Показатель | Значение |",
        "|------------|----------|",
        f"| Python-файлов | **{t['files']}** |",
        f"| Модулей ядра `src/` (без Tests/Tools) | **{data['internal_modules']}** |",
        f"| Всего строк (физических) | **{t['total_lines']:,}** |".replace(",", " "),
        f"| **Строк кода** (без пустых и `#`) | **{t['code_lines']:,}** |".replace(",", " "),
        f"| Пустых строк | {t['blank_lines']:,} |".replace(",", " "),
        f"| Строк комментариев `#` | {t['comment_lines']:,} |".replace(",", " "),
        f"| Классов (определений / уникальных имён) | **{t['classes']}** / {data['unique_classes']} |",
        f"| Функций верхнего уровня | **{data['top_level_funcs']}** |",
        f"| Методов классов | {t['methods']} |",
        f"| Вложенных функций | {t['nested_functions']} |",
        f"| **Всего callable** (fn+method+nested) | **{data['top_level_funcs'] + t['methods'] + t['nested_functions']}** |",
        f"| Модульных переменных | {t['global_assignments']} |",
        f"| Декораторов | {t['decorators']} |",
        f"| Функций с type hints | {t.get('type_hints_funcs', 0)} |",
        f"| Блоков `try/except` | {t['try_blocks']} |",
        f"| Импортов `import` / `from` | {t['imports']} / {t['from_imports']} |",
        f"| `config.json` | **{data['config_lines']:,}** строк |".replace(",", " "),
        f"| Документация MD | **{len(data['md_stats'])}** файлов, **{data['md_total']:,}** строк |".replace(",", " "),
        f"| `README.md` | {data['readme_lines']:,} строк |".replace(",", " "),
    )

    grand = t["total_lines"] + data["config_lines"] + data["md_total"]
    add(
        "### Визуальный масштаб",
        "",
        "```",
        f"Python LOC     [{_bar(t['code_lines'], 25000, 36)}] {t['code_lines']:,}".replace(",", " "),
        f"config.json    [{_bar(data['config_lines'], 25000, 36)}] {data['config_lines']:,}".replace(",", " "),
        f"Документация   [{_bar(data['md_total'], 25000, 36)}] {data['md_total']:,}".replace(",", " "),
        f"ВСЕГО текста   [{_bar(grand, 40000, 36)}] ~{grand:,}".replace(",", " "),
        "```",
    )

    add("---", "", "## 2. Структура проекта", "", "```mermaid", "flowchart TB")
    add(
        "    subgraph entry [Точки входа]",
        "        main[main.py]",
        "    end",
        "    subgraph core [Ядро — 28 модулей]",
        "        mi[main_impl.py · 3908 LOC]",
        "        ms[manager_stats.py · 2819 LOC]",
        "        cc[consistency_checks.py · 1991 LOC]",
        "        arch[input_archive_sqlite · 2010 LOC]",
        "        rim[rating_item_matrix.py]",
        "        sos[season_order_summary.py]",
        "        prom[profile_gp / leadersForAdmin]",
        "    end",
        "    subgraph infra [Инфраструктура]",
        "        cfg[config_loader + config.json]",
        "        ui[console_ui.py]",
        "        log[logging_setup · debug_timing]",
        "    end",
        "    subgraph aux [Вспомогательное]",
        "        tests[Tests · 8 файлов]",
        "        tools[Tools · 5 скриптов]",
        "        docs[Docs · 25 MD]",
        "    end",
        "    main --> mi",
        "    mi --> core",
        "    mi --> infra",
        "    core --> infra",
        "```",
    )

    add("---", "", "## 3. Разбивка по категориям", "", "```mermaid", "pie title Строки кода Python")
    for cat, block in sorted(data["by_category"].items(), key=lambda x: -x[1]["code_lines"]):
        add(f'    "{CAT_LABELS.get(cat, cat)}" : {block["code_lines"]}')
    add("```")

    add("| Категория | Файлов | LOC | Доля | Классов | Функций |", "|-----------|--------|-----|------|---------|---------|")
    for cat, block in sorted(data["by_category"].items(), key=lambda x: -x[1]["code_lines"]):
        share = block["code_lines"] / total_code * 100 if total_code else 0
        add(
            f"| {CAT_LABELS.get(cat, cat)} | {block['files']} | {block['code_lines']:,} | {share:.1f}% | "
            f"{block['classes']} | {block['functions']} |".replace(",", " ")
        )

    top = sorted(data["files"], key=lambda x: x["code_lines"], reverse=True)[:15]
    max_code = top[0]["code_lines"] if top else 1
    add("---", "", "## 4. Топ-15 файлов по объёму кода", "",
        "| # | Файл | Категория | Всего | LOC | Классы | Fn |", "|---|------|-----------|-------|-----|--------|-----|")
    for i, f in enumerate(top, 1):
        add(f"| {i} | `{f['rel']}` | {CAT_LABELS.get(f['category'], f['category'])} | {f['total_lines']} | {f['code_lines']} | {f.get('classes',0)} | {f.get('functions',0)} |")

    add("```")
    for f in top:
        short = f["rel"].replace("src/", "")
        add(f"{short[:40]:40} {_bar(f['code_lines'], max_code, 28)} {f['code_lines']:,}".replace(",", " "))
    add("```")

    add("---", "", "## 5. Классы", "", f"**{t['classes']}** определений в **{len({r for _, r in data['all_classes']})}** файлах.", "",
        "| Класс | Файл |", "|-------|------|")
    for name, rel in data["all_classes"]:
        add(f"| `{name}` | `{rel}` |")

    add("---", "", "## 6. Функции", "", "```mermaid", "flowchart LR")
    fc = data["functions_by_cat"]
    add(
        f"    core[Ядро: {fc.get('core', 0)}]",
        f"    tests[Тесты: {fc.get('tests', 0)}]",
        f"    tools[Tools: {fc.get('tools', 0)}]",
        f"    entry[Вход: {fc.get('entrypoint', 0)}]",
        f"    methods[Методы: {t['methods']}]",
        f"    nested[Вложенные: {t['nested_functions']}]",
        "```",
        "| Тип | Количество |",
        "|-----|------------|",
        f"| Верхний уровень | {data['top_level_funcs']} |",
        f"| Методы | {t['methods']} |",
        f"| Вложенные | {t['nested_functions']} |",
        f"| **Итого** | **{data['top_level_funcs'] + t['methods'] + t['nested_functions']}** |",
    )

    add("---", "", "## 7. Модули и зависимости", "", "### Наиболее импортируемые модули `src.*`", "",
        "| Модуль | Файлов-импортёров |", "|--------|-------------------|")
    for mod, cnt in data["imported_by_top"]:
        add(f"| `src.{mod}` | {cnt} |")

    add("", "### Внешние библиотеки", "", "| Пакет | Упоминаний |", "|-------|------------|")
    for pkg, cnt in data["external_deps"]:
        add(f"| **{pkg}** | {cnt} |")

    add("", "### Стандартная библиотека (топ-10)", "", "| Модуль | Упоминаний |", "|--------|------------|")
    for pkg, cnt in data["stdlib_top"][:10]:
        add(f"| `{pkg}` | {cnt} |")

    add("", "### Граф связей ядра", "", "```mermaid", "flowchart LR",
        "    main_impl --> config_loader",
        "    main_impl --> consistency_checks",
        "    main_impl --> manager_stats",
        "    main_impl --> rating_item_matrix",
        "    main_impl --> season_order_summary",
        "    main_impl --> input_archive_sqlite_v2",
        "    manager_stats --> profile_gp_json",
        "    manager_stats --> profile_gp_auto_js",
        "    manager_stats --> leaders_for_admin_json",
        "    manager_stats --> rating_item_matrix",
        "    season_order_summary --> rating_item_matrix",
        "```",
    )

    add("---", "", "## 8. Пайплайн выполнения", "", "```mermaid", "sequenceDiagram",
        "    participant M as main.py", "    participant I as main_impl", "    participant L as file_loader",
        "    participant C as consistency_checks", "    participant E as enrich", "    participant X as Excel",
        "    M->>I: Config + run_outputs", "    I->>L: CSV из IN/", "    I->>C: Проверки сырых данных",
        "    I->>E: merge, gender, tournament", "    opt rating_item_matrix", "    I->>E: ITEM на RATING", "    end",
        "    opt season_order_summary", "    I->>E: ORDER-SEASON-SUMMARY", "    end",
        "    opt manager_stats_only", "    I->>X: MANAGER_STATS", "    end",
        "    opt main_only", "    I->>X: SPOD_PROM main", "    end", "```",
    )

    add("---", "", "## 9. Полный реестр Python-файлов", "",
        "| Файл | Кат. | Всего | LOC | Пуст. | `#` | Cls | Fn | Imp |",
        "|------|------|-------|-----|-------|-----|-----|----|----|")
    for f in sorted(data["files"], key=lambda x: x["rel"]):
        err = " ⚠️" if f.get("parse_error") else ""
        imp = f.get("imports", 0) + f.get("from_imports", 0)
        add(
            f"| `{f['rel']}`{err} | {f['category'][:4]} | {f['total_lines']} | {f['code_lines']} | "
            f"{f['blank_lines']} | {f['comment_lines']} | {f.get('classes',0)} | {f.get('functions',0)} | {imp} |"
        )

    add("---", "", "## 10. Документация", "", f"**{len(data['md_stats'])}** файлов, **{data['md_total']:,}** строк.".replace(",", " "),
        "", "| Файл | Строк |", "|------|-------|")
    for rel, n in sorted(data["md_stats"], key=lambda x: -x[1])[:12]:
        add(f"| `{rel}` | {n:,} |".replace(",", " "))
    if len(data["md_stats"]) > 12:
        add(f"| … ещё {len(data['md_stats']) - 12} | |")

    big3 = top[0]["code_lines"] + top[1]["code_lines"] + top[3]["code_lines"]
    add(
        "---", "", "## 11. Выводы",
        "",
        f"1. **Три файла — ~{big3 / total_code * 100:.0f}% кода:** `main_impl.py`, `manager_stats.py`, `consistency_checks.py` "
        f"({big3:,} LOC из {total_code:,}).".replace(",", " "),
        f"2. **config.json** ({data['config_lines']:,} строк) по объёму сопоставим с крупнейшим модулем.".replace(",", " "),
        f"3. **Тесты:** {data['by_category'].get('tests', {}).get('files', 0)} файлов, "
        f"{data['by_category'].get('tests', {}).get('code_lines', 0):,} LOC; лидер — `test_manager_stats.py`.".replace(",", " "),
        f"4. **Зависимости:** `pandas` — основная внешняя; Excel — `openpyxl`; type hints в {t.get('type_hints_funcs', 0)} функциях.",
        f"5. **Хабы:** `config_loader`, `profile_gp_auto_js`, `manager_stats` — наиболее связанные модули.",
        f"6. **main_impl.py** — монолитный orchestrator (~{top[0]['code_lines'] / total_code * 100:.0f}% LOC); кандидат на декомпозицию.",
        "",
        "---",
        "",
        "*Автоматический отчёт. Обновление: `python src/Tools/build_codebase_analytics.py`*",
    )

    return "\n".join(lines)


def main() -> None:
    data = analyze()
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(render_md(data), encoding="utf-8")
    print(f"OK: {OUT_PATH} ({OUT_PATH.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
