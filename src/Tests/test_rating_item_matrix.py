# -*- coding: utf-8 -*-
"""Проверки логики матрицы RATING (группы заказов, ключи заливки)."""

from src.rating_item_matrix import (
    FILL_AVAILABLE_NOT_ORDERED,
    FILL_ORDERED_AVAILABLE,
    FILL_ORDERED_UNAVAILABLE,
    FILL_UNAVAILABLE_NOT_ORDERED,
    _blocked_codes_for_row,
    _filter_order_dataframe,
    _matrix_fill_key,
    _order_counts_by_employee,
    _parse_item_order_groups,
)


def test_matrix_fill_key_four_states() -> None:
    assert _matrix_fill_key(True, True) == FILL_ORDERED_AVAILABLE
    assert _matrix_fill_key(False, True) == FILL_ORDERED_UNAVAILABLE
    assert _matrix_fill_key(True, False) == FILL_AVAILABLE_NOT_ORDERED
    assert _matrix_fill_key(False, False) == FILL_UNAVAILABLE_NOT_ORDERED


def test_blocked_codes_when_group_sum_reaches_limit() -> None:
    groups = [
        {
            "id": "G1",
            "max_orders": 2,
            "codes": ["A", "B", "C"],
        }
    ]
    counts = {"A": 1, "B": 1, "C": 0}
    blocked = _blocked_codes_for_row(counts, groups)
    assert blocked == {"A", "B", "C"}


def test_blocked_codes_below_limit() -> None:
    groups = [{"id": "G1", "max_orders": 2, "codes": ["A", "B"]}]
    assert _blocked_codes_for_row({"A": 1}, groups) == set()


def test_order_filter_excludes_status() -> None:
    import pandas as pd

    df = pd.DataFrame(
        {
            "Табельный номер": ["1", "2", "3"],
            "Код товара": ["X", "Y", "Z"],
            "Статус заказа": ["Новый", "Отменён", "Отклонён"],
        }
    )
    cfg = {
        "order_status_col": "Статус заказа",
        "order_status_exclude": ["Отклонён", "Отменён"],
    }
    out = _filter_order_dataframe(df, cfg)
    assert len(out) == 1
    assert out.iloc[0]["Табельный номер"] == "1"


def test_order_counts_by_employee() -> None:
    import pandas as pd

    df = pd.DataFrame(
        {
            "Табельный номер": ["10", "10", "20"],
            "Код товара": ["ITEM_A", "ITEM_A", "ITEM_B"],
        }
    )
    counts = _order_counts_by_employee(df, "Табельный номер", "Код товара")
    assert counts["10"]["ITEM_A"] == 2
    assert counts["20"]["ITEM_B"] == 1


def test_parse_item_order_groups() -> None:
    cfg = {
        "item_order_groups": [
            {"id": "g", "max_orders": 2, "codes": ["X", "Y"]},
        ]
    }
    groups = _parse_item_order_groups(cfg)
    assert len(groups) == 1
    assert groups[0]["max_orders"] == 2
