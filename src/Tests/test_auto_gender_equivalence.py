# -*- coding: utf-8 -*-
"""
AUTO_GENDER: векторизованная версия (используется в пайплайне) совпадает с построчной (PERF-03).
Раньше сравнение выполнялось в каждом рабочем прогоне; теперь — здесь, на наборе особых случаев.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.main_impl import add_auto_gender_column, add_auto_gender_column_vectorized  # noqa: E402

# (фамилия, имя, отчество) — условные ФИО и граничные случаи формата
_NAMES = [
    ("Иванов", "Иван", "Иванович"),
    ("Петрова", "Анна", "Сергеевна"),
    ("Сидоров", "Олег", ""),
    ("Кузнецова", "Мария", ""),
    ("Мамедов", "Эльдар", "Рашид оглы"),
    ("Алиева", "Лейла", "Рашид кызы"),
    ("Ким", "Алекс", ""),
    ("…", "Тест сб", ""),
    ("…", "Иван мб", ""),
    ("ИВАНОВА", "ЕЛЕНА", "ПЕТРОВНА"),
    ("  Смирнов ", " Пётр ", " Андреевич "),
    ("Семёнова", "Алёна", "Фёдоровна"),
    ("Римский-Корсаков", "Николай", "Андреевич"),
    ("Smith", "John", ""),
    ("", "", ""),
    (None, None, None),
    ("Шевченко", "Саша", ""),
    ("Белых", "Женя", "Ильинична"),
    ("Грин", "Никита", "Ильич"),
    ("Кравец", "Валентина", ""),
    ("nan", "nan", "nan"),
]


def _frame() -> pd.DataFrame:
    return pd.DataFrame(_NAMES, columns=["SURNAME", "FIRST_NAME", "MIDDLE_NAME"]).assign(
        PERSON_NUMBER=[f"{i:020d}" for i in range(len(_NAMES))]
    )


def test_vectorized_matches_row_by_row() -> None:
    ref = add_auto_gender_column(_frame(), "EMPLOYEE")
    vec = add_auto_gender_column_vectorized(_frame(), "EMPLOYEE")
    mismatch = [
        (_NAMES[i], ref["AUTO_GENDER"].iloc[i], vec["AUTO_GENDER"].iloc[i])
        for i in range(len(_NAMES))
        if ref["AUTO_GENDER"].iloc[i] != vec["AUTO_GENDER"].iloc[i]
    ]
    assert mismatch == []


def test_vectorized_detects_basic_cases() -> None:
    vec = add_auto_gender_column_vectorized(_frame(), "EMPLOYEE")["AUTO_GENDER"].tolist()
    assert vec[0] == "М" and vec[1] == "Ж"
    assert set(vec) <= {"М", "Ж", "-"}
