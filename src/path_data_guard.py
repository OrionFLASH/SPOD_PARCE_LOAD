# -*- coding: utf-8 -*-
"""
Защита каталогов IN/ и OUT/ от случайного удаления содержимого.

Правило проекта: без явного распоряжения пользователя не очищать IN/ и OUT/
(включая подкаталоги). Тесты и вспомогательные сценарии — только .work/.
"""

from __future__ import annotations

from pathlib import Path

PROTECTED_DIR_NAMES: tuple[str, ...] = ("IN", "OUT")
WORK_DIR_NAME: str = ".work"


class ProtectedDataPathError(PermissionError):
    """Попытка изменить защищённый каталог данных."""


def project_work_root(project_root: Path) -> Path:
    """Корень для временных тестовых копий (не IN/OUT)."""
    return project_root.resolve() / WORK_DIR_NAME


def is_under_protected_data(path: Path, project_root: Path) -> bool:
    """True, если путь лежит внутри IN/ или OUT/ относительно корня проекта."""
    try:
        rel = path.resolve().relative_to(project_root.resolve())
    except ValueError:
        return False
    if not rel.parts:
        return False
    return rel.parts[0] in PROTECTED_DIR_NAMES


def assert_safe_mutable_tree(path: Path, project_root: Path, *, action: str = "изменение") -> None:
    """
    Запретить очистку/удаление дерева внутри IN/ или OUT/.
    Разрешены только пути вне этих каталогов (например .work/).
    """
    if is_under_protected_data(path, project_root):
        rel = path.resolve().relative_to(project_root.resolve())
        raise ProtectedDataPathError(
            f"Запрещено {action} в защищённом каталоге данных: {rel}. "
            f"Используйте {WORK_DIR_NAME}/ для тестов или явное распоряжение пользователя."
        )


def post_decrypt_test_dirs(project_root: Path) -> tuple[Path, Path]:
    """Каталоги для безопасного теста POST decrypt (вне IN/OUT)."""
    base = project_work_root(project_root) / "post_decrypt_test"
    return base / "IN_POST", base / "OUT_POST"
