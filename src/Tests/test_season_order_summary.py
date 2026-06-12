# -*- coding: utf-8 -*-
"""Проверки сводного листа ORDER-SEASON-SUMMARY."""

import pandas as pd

from src.season_order_summary import build_season_order_summary_sheet, merge_season_summary_config


def _minimal_cfg() -> dict:
    return {
        "enabled": True,
        "sheet_name": "ORDER-SEASON-SUMMARY",
        "sheet_rating": "RATING",
        "sheet_order": "ORDER",
        "sheet_reward": "REWARD",
        "order_employee_col": "Табельный номер",
        "order_product_col": "Код товара",
        "order_status_col": "Статус заказа",
        "order_status_exclude": ["Отклонён", "Отменён"],
        "rating_employee_col": "Табельный номер",
        "country_rank_col": "Место в рейтинге по стране",
        "item_order_groups": [
            {
                "id": "SEASON_TEST",
                "max_orders": 2,
                "codes": ["ITEM_X"],
            }
        ],
    }


def test_build_summary_ordered_and_remainder() -> None:
    sheets = {
        "RATING": (
            pd.DataFrame(
                {
                    "Табельный номер": ["100", "200"],
                    "Место в рейтинге по стране": [5, 50],
                }
            ),
            {},
        ),
        "ORDER": (
            pd.DataFrame(
                {
                    "Табельный номер": ["100", "100"],
                    "Код товара": ["ITEM_X", "ITEM_X"],
                    "Статус заказа": ["Новый", "Отменён"],
                }
            ),
            {},
        ),
        "REWARD": (
            pd.DataFrame(
                {
                    "REWARD_TYPE": ["ITEM"],
                    "REWARD_CODE": ["ITEM_X"],
                    "FULL_NAME": ["Товар X"],
                    "REWARD_ADD_DATA": [
                        '{"itemAmount":3,"employeeRating":{"minRatingBANK":10}}'
                    ],
                }
            ),
            {},
        ),
    }
    built = build_season_order_summary_sheet(sheets, _minimal_cfg())
    assert built is not None
    df, _ = built
    row = df.iloc[0]
    assert row["Код награды"] == "ITEM_X"
    assert row["Заказано"] == 1
    assert row["Всего товаров"] == 3
    assert row["Остаток"] == 2
    assert row["Статус наличия"] == ""
    assert row["КМ: условия выполнены"] == 1
    assert row["КМ: без 2 заказов в группе"] == 1
    assert row["Мин. рейтинг BANK"] == 10


def test_other_items_section_without_group() -> None:
    sheets = {
        "RATING": (pd.DataFrame({"Табельный номер": ["1"]}), {}),
        "ORDER": (
            pd.DataFrame(
                {
                    "Табельный номер": ["1"],
                    "Код товара": ["ITEM_Y"],
                    "Статус заказа": ["Новый"],
                }
            ),
            {},
        ),
        "REWARD": (
            pd.DataFrame(
                {
                    "REWARD_TYPE": ["ITEM", "ITEM"],
                    "REWARD_CODE": ["ITEM_X", "ITEM_Y"],
                    "FULL_NAME": ["A", "B"],
                    "REWARD_ADD_DATA": ["{}", "{}"],
                }
            ),
            {},
        ),
    }
    built = build_season_order_summary_sheet(sheets, _minimal_cfg())
    assert built is not None
    df, _ = built
    assert df.iloc[0]["Код награды"] == "ITEM_X"
    assert df.iloc[0]["Группа сезона"] == "SEASON_TEST"
    other = df[df["Код награды"] == "ITEM_Y"]
    assert len(other) == 1
    assert other.iloc[0]["Группа сезона"] == ""


def test_stock_ended_label() -> None:
    sheets = {
        "RATING": (pd.DataFrame({"Табельный номер": ["1"]}), {}),
        "ORDER": (
            pd.DataFrame(
                {
                    "Табельный номер": ["1", "2", "3"],
                    "Код товара": ["ITEM_X", "ITEM_X", "ITEM_X"],
                    "Статус заказа": ["Новый"] * 3,
                }
            ),
            {},
        ),
        "REWARD": (
            pd.DataFrame(
                {
                    "REWARD_TYPE": ["ITEM"],
                    "REWARD_CODE": ["ITEM_X"],
                    "FULL_NAME": ["Т"],
                    "REWARD_ADD_DATA": ['{"itemAmount":2}'],
                }
            ),
            {},
        ),
    }
    built = build_season_order_summary_sheet(sheets, _minimal_cfg())
    assert built is not None
    df, _ = built
    assert df.iloc[0]["Заказано"] == 3
    assert df.iloc[0]["Статус наличия"] == "ЗАКОНЧИЛСЯ"


def test_merge_config_from_rating_item_matrix() -> None:
    cfg = merge_season_summary_config(
        {
            "season_order_summary": {"sheet_name": "CUSTOM"},
            "rating_item_matrix": {"sheet_order": "ORD"},
        }
    )
    assert cfg["sheet_name"] == "CUSTOM"
    assert cfg["sheet_order"] == "ORD"
