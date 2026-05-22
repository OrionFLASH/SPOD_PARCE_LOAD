# -*- coding: utf-8 -*-
"""Сопоставление заголовков CSV с BOM и config."""

import pandas as pd

from src.csv_headers import normalize_csv_column_header, resolve_columns_in_dataframe


def test_normalize_strips_bom() -> None:
    assert normalize_csv_column_header("\ufeffТабельный номер") == "Табельный номер"


def test_resolve_columns_with_bom_header() -> None:
    df = pd.DataFrame(columns=["\ufeffТабельный номер", "Период"])
    resolved, missing = resolve_columns_in_dataframe(df, ["Табельный номер", "Период"])
    assert missing == []
    assert resolved == ["\ufeffТабельный номер", "Период"]
