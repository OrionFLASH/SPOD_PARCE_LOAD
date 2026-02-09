# -*- coding: utf-8 -*-
"""
Определение пола по отчеству, имени и фамилии по конфигурации паттернов.
"""

import logging
import time
from typing import Any, Dict, Optional

import pandas as pd

from src.config_loader import Config


def _detect_gender_by_patterns(
    value: Any,
    patterns_male: list,
    patterns_female: list,
) -> Optional[str]:
    """Определение пола по окончаниям в тексте."""
    if pd.isna(value) or not isinstance(value, str):
        return None
    value_lower = value.lower().strip()
    if not value_lower:
        return None
    for pattern in patterns_male:
        if value_lower.endswith(pattern.lower()):
            return "М"
    for pattern in patterns_female:
        if value_lower.endswith(pattern.lower()):
            return "Ж"
    return None


def _detect_gender_for_person(
    config: Config,
    patronymic: str,
    first_name: str,
    surname: str,
    row_idx: int,
) -> str:
    """Определение пола для одного человека: отчество -> имя -> фамилия."""
    patterns = config.gender_patterns
    gender = _detect_gender_by_patterns(
        patronymic,
        patterns.get("patronymic_male", []),
        patterns.get("patronymic_female", []),
    )
    if gender:
        logging.debug(f"[DEBUG] Строка {row_idx}: пол по отчеству '{patronymic}' -> {gender}")
        return gender
    gender = _detect_gender_by_patterns(
        first_name,
        patterns.get("name_male", []),
        patterns.get("name_female", []),
    )
    if gender:
        logging.debug(f"[DEBUG] Строка {row_idx}: пол по имени '{first_name}' -> {gender}")
        return gender
    gender = _detect_gender_by_patterns(
        surname,
        patterns.get("surname_male", []),
        patterns.get("surname_female", []),
    )
    if gender:
        logging.debug(f"[DEBUG] Строка {row_idx}: пол по фамилии '{surname}' -> {gender}")
        return gender
    logging.debug(
        f"[DEBUG] Строка {row_idx}: пол не определен (отч:'{patronymic}', имя:'{first_name}', фам:'{surname}')"
    )
    return "-"


def add_auto_gender_column(config: Config, df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    """Добавление колонки AUTO_GENDER (построчная версия)."""
    func_start = time.time()
    required_columns = ["MIDDLE_NAME", "FIRST_NAME", "SURNAME"]
    missing_columns = [c for c in required_columns if c not in df.columns]
    if missing_columns:
        logging.warning(
            f"[GENDER DETECTION] Пропущены колонки {missing_columns} в листе {sheet_name}"
        )
        df["AUTO_GENDER"] = "-"
        return df

    total_rows = len(df)
    logging.info(
        f"[GENDER DETECTION] Начинаем определение пола для листа {sheet_name}, строк: {total_rows}"
    )
    auto_gender = []
    male_count = female_count = unknown_count = 0
    step = config.gender_progress_step

    for idx, row in df.iterrows():
        patronymic = row.get("MIDDLE_NAME", "")
        first_name = row.get("FIRST_NAME", "")
        surname = row.get("SURNAME", "")
        gender = _detect_gender_for_person(
            config, patronymic, first_name, surname, idx
        )
        auto_gender.append(gender)
        if gender == "М":
            male_count += 1
        elif gender == "Ж":
            female_count += 1
        else:
            unknown_count += 1
        if (idx + 1) % step == 0:
            percent = ((idx + 1) / total_rows) * 100
            logging.info(
                f"[GENDER DETECTION] Обработано {idx + 1} из {total_rows} строк ({percent:.1f}%)"
            )

    df["AUTO_GENDER"] = auto_gender
    func_time = time.time() - func_start
    logging.info(
        f"[GENDER DETECTION] Статистика: М={male_count}, Ж={female_count}, неопределено={unknown_count} (всего: {total_rows})"
    )
    logging.info(f"[GENDER DETECTION] Завершено за {func_time:.3f}s для листа {sheet_name}")
    return df


def add_auto_gender_column_vectorized(
    config: Config, df: pd.DataFrame, sheet_name: str
) -> pd.DataFrame:
    """Векторизованное определение пола по паттернам."""
    func_start = time.time()
    required_columns = ["MIDDLE_NAME", "FIRST_NAME", "SURNAME"]
    missing_columns = [c for c in required_columns if c not in df.columns]
    if missing_columns:
        logging.warning(
            f"[GENDER DETECTION VECTORIZED] Пропущены колонки {missing_columns} в листе {sheet_name}"
        )
        df["AUTO_GENDER"] = "-"
        return df

    total_rows = len(df)
    logging.info(
        f"[GENDER DETECTION VECTORIZED] Начинаем определение пола для листа {sheet_name}, строк: {total_rows}"
    )
    patterns = config.gender_patterns
    gender = pd.Series("-", index=df.index)

    patronymic_lower = (
        df["MIDDLE_NAME"].fillna("").astype(str).str.lower().str.strip()
    )
    first_name_lower = (
        df["FIRST_NAME"].fillna("").astype(str).str.lower().str.strip()
    )
    surname_lower = df["SURNAME"].fillna("").astype(str).str.lower().str.strip()

    for pattern in patterns.get("patronymic_male", []):
        mask = patronymic_lower.str.endswith(pattern.lower()) & (gender == "-")
        gender[mask] = "М"
    for pattern in patterns.get("patronymic_female", []):
        mask = patronymic_lower.str.endswith(pattern.lower()) & (gender == "-")
        gender[mask] = "Ж"
    for pattern in patterns.get("name_male", []):
        mask = first_name_lower.str.endswith(pattern.lower()) & (gender == "-")
        gender[mask] = "М"
    for pattern in patterns.get("name_female", []):
        mask = first_name_lower.str.endswith(pattern.lower()) & (gender == "-")
        gender[mask] = "Ж"
    for pattern in patterns.get("surname_male", []):
        mask = surname_lower.str.endswith(pattern.lower()) & (gender == "-")
        gender[mask] = "М"
    for pattern in patterns.get("surname_female", []):
        mask = surname_lower.str.endswith(pattern.lower()) & (gender == "-")
        gender[mask] = "Ж"

    df["AUTO_GENDER"] = gender
    male_count = (gender == "М").sum()
    female_count = (gender == "Ж").sum()
    unknown_count = (gender == "-").sum()
    func_time = time.time() - func_start
    logging.info(
        f"[GENDER DETECTION VECTORIZED] Статистика: М={male_count}, Ж={female_count}, неопределено={unknown_count} (всего: {total_rows})"
    )
    logging.info(
        f"[GENDER DETECTION VECTORIZED] Завершено за {func_time:.3f}s для листа {sheet_name}"
    )
    return df


def compare_gender_results(
    df_old: pd.DataFrame, df_new: pd.DataFrame
) -> Dict[str, Any]:
    """Сравнивает результаты двух версий определения пола."""
    if "AUTO_GENDER" not in df_old.columns or "AUTO_GENDER" not in df_new.columns:
        return {"error": "Колонка AUTO_GENDER не найдена"}
    old_results = df_old["AUTO_GENDER"].fillna("-")
    new_results = df_new["AUTO_GENDER"].fillna("-")
    differences = (old_results != new_results).sum()
    total = len(df_old)
    matches = total - differences
    diff_examples = []
    if differences > 0:
        diff_mask = old_results != new_results
        diff_indices = df_old.index[diff_mask][:5]
        for idx in diff_indices:
            diff_examples.append(
                {
                    "index": idx,
                    "old": old_results.loc[idx],
                    "new": new_results.loc[idx],
                }
            )
    return {
        "total": total,
        "matches": matches,
        "differences": differences,
        "match_percent": (matches / total * 100) if total > 0 else 0,
        "diff_examples": diff_examples,
        "identical": differences == 0,
    }
