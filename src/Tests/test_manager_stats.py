# -*- coding: utf-8 -*-
"""Тесты сбора табельных по manager_stats.sources с фильтрами."""

from __future__ import annotations

import pandas as pd

from src.manager_stats import (
    _build_filter_mask,
    build_manager_stats_workbook_data,
    collect_tab_numbers_from_sheets,
    enrich_tab_dataframe,
    normalize_tab_number,
)


def test_normalize_tab_number_pads_digits() -> None:
    assert normalize_tab_number("12345", 20) == "00000000000000012345"
    assert normalize_tab_number("", 20) == ""
    assert normalize_tab_number(None, 20) == ""


def test_where_in_and_where_not_in() -> None:
    df = pd.DataFrame(
        {
            "Табельный номер": ["1", "2", "3", "4"],
            "Д": ["X", "X", "Y", "X"],
            "М": ["K", "A", "K", "K"],
        }
    )
    mask = _build_filter_mask(
        df,
        where_in={"Д": ["X"]},
        where_not_in={"М": ["K"]},
    )
    assert mask.tolist() == [False, True, False, False]


def test_collect_from_config_sources() -> None:
    sheets = {
        "EMPLOYEE": (
            pd.DataFrame({"PERSON_NUMBER": ["00000000000000000001", "00000000000000000002"]}),
            {},
        ),
        "RATING_2025_2 (KMKKSB)": (
            pd.DataFrame({"Табельный номер": ["00000000000000000001", "00000000000000000003"]}),
            {},
        ),
    }
    cfg = {
        "normalize_pad_width": 20,
        "sources": [
            {"id": "emp", "sheet": "EMPLOYEE", "tab_column": "PERSON_NUMBER"},
            {
                "id": "rat",
                "sheet_pattern": "RATING_*",
                "tab_column": "Табельный номер",
            },
        ],
    }
    df_tabs, df_summary = collect_tab_numbers_from_sheets(sheets, cfg=cfg)
    assert len(df_tabs) == 3
    assert len(df_summary) == 2
    assert "Фильтры" in df_summary.columns


def test_order_status_exclude() -> None:
    sheets = {
        "ORDER": (
            pd.DataFrame(
                {
                    "Табельный номер": ["00000000000000000001", "00000000000000000002"],
                    "Статус заказа": ["Выполнен", "Отклонён"],
                }
            ),
            {},
        ),
    }
    cfg = {
        "sources": [
            {
                "id": "ord",
                "sheet": "ORDER",
                "tab_column": "Табельный номер",
                "where_not_in": {"Статус заказа": ["Отклонён", "Отменён"]},
            }
        ],
    }
    df_tabs, _ = collect_tab_numbers_from_sheets(sheets, cfg=cfg)
    assert len(df_tabs) == 1
    assert df_tabs.iloc[0]["Табельный номер"] == "00000000000000000001"


def test_enrich_priority_and_default() -> None:
    sheets = {
        "STATISTICS": (
            pd.DataFrame(
                {
                    "Табельный номер": ["00000000000000000001", "00000000000000000002"],
                    "Фамилия": ["Иванов", ""],
                }
            ),
            {},
        ),
        "EMPLOYEE": (
            pd.DataFrame(
                {
                    "PERSON_NUMBER": ["00000000000000000002", "00000000000000000003"],
                    "SURNAME": ["Петров", "Сидоров"],
                }
            ),
            {},
        ),
    }
    df_tabs = pd.DataFrame(
        {
            "№": [1, 2, 3],
            "Табельный номер": [
                "00000000000000000001",
                "00000000000000000002",
                "00000000000000000099",
            ],
            "Источники": ["a", "b", "c"],
            "Число источников": [1, 1, 1],
        }
    )
    cfg = {
        "enrich_default": "-",
        "enrich_columns": [
            {
                "id": "last_name",
                "output_column": "Фамилия",
                "mode": "value",
                "multi_row": "first",
                "sources": [
                    {
                        "priority": 1,
                        "sheet": "STATISTICS",
                        "tab_column": "Табельный номер",
                        "value_column": "Фамилия",
                    },
                    {
                        "priority": 2,
                        "sheet": "EMPLOYEE",
                        "tab_column": "PERSON_NUMBER",
                        "value_column": "SURNAME",
                    },
                ],
            }
        ],
    }
    out = enrich_tab_dataframe(df_tabs, sheets, cfg)
    assert out.iloc[0]["Фамилия"] == "Иванов"
    assert out.iloc[1]["Фамилия"] == "Петров"
    assert out.iloc[2]["Фамилия"] == "-"
    assert list(out.columns[:4]) == ["№", "Табельный номер", "Фамилия", "Источники"]


def test_enrich_multi_row_join_and_modes() -> None:
    sheets = {
        "RATING": (
            pd.DataFrame(
                {
                    "Табельный номер": [
                        "00000000000000000001",
                        "00000000000000000001",
                        "00000000000000000001",
                    ],
                    "Балл": ["10", "20", "10"],
                    "Роль": ["A", "B", "A"],
                }
            ),
            {},
        ),
    }
    df_tabs = pd.DataFrame(
        {
            "№": [1],
            "Табельный номер": ["00000000000000000001"],
            "Источники": ["x"],
            "Число источников": [1],
        }
    )
    join_cfg = {
        "enrich_columns": [
            {
                "output_column": "Роли",
                "mode": "value",
                "multi_row": "join",
                "join_separator": ";",
                "sources": [
                    {
                        "priority": 1,
                        "sheet": "RATING",
                        "tab_column": "Табельный номер",
                        "value_column": "Роль",
                    }
                ],
            }
        ],
    }
    out_join = enrich_tab_dataframe(df_tabs, sheets, join_cfg)
    assert out_join.iloc[0]["Роли"] == "A;B"

    sum_cfg = {
        "enrich_columns": [
            {
                "output_column": "Сумма",
                "mode": "sum",
                "sources": [
                    {
                        "priority": 1,
                        "sheet": "RATING",
                        "tab_column": "Табельный номер",
                        "value_column": "Балл",
                    }
                ],
            }
        ],
    }
    out_sum = enrich_tab_dataframe(df_tabs, sheets, sum_cfg)
    assert out_sum.iloc[0]["Сумма"] == "40"

    count_cfg = {
        "enrich_columns": [
            {
                "output_column": "Строк",
                "mode": "count",
                "sources": [
                    {
                        "priority": 1,
                        "sheet": "RATING",
                        "tab_column": "Табельный номер",
                        "value_column": "Балл",
                    }
                ],
            }
        ],
    }
    out_count = enrich_tab_dataframe(df_tabs, sheets, count_cfg)
    assert out_count.iloc[0]["Строк"] == "3"


def test_enrich_first_stops_after_first_source() -> None:
    """mode=value+first: нашли на первом источнике — нижние не смотрим."""
    sheets = {
        "STATISTICS": (
            pd.DataFrame(
                {
                    "Табельный номер": ["00000000000000000001"],
                    "Фамилия": ["ИзСтатистики"],
                }
            ),
            {},
        ),
        "EMPLOYEE": (
            pd.DataFrame(
                {
                    "PERSON_NUMBER": ["00000000000000000001"],
                    "SURNAME": ["ИзСотрудника"],
                }
            ),
            {},
        ),
    }
    df_tabs = pd.DataFrame(
        {
            "№": [1],
            "Табельный номер": ["00000000000000000001"],
            "Источники": ["x"],
            "Число источников": [1],
        }
    )
    cfg = {
        "enrich_columns": [
            {
                "output_column": "Фамилия",
                "mode": "value",
                "multi_row": "first",
                "sources": [
                    {
                        "priority": 1,
                        "sheet": "STATISTICS",
                        "tab_column": "Табельный номер",
                        "value_column": "Фамилия",
                    },
                    {
                        "priority": 2,
                        "sheet": "EMPLOYEE",
                        "tab_column": "PERSON_NUMBER",
                        "value_column": "SURNAME",
                    },
                ],
            }
        ],
    }
    out = enrich_tab_dataframe(df_tabs, sheets, cfg)
    assert out.iloc[0]["Фамилия"] == "ИзСтатистики"


def test_enrich_join_merges_unique_across_sources() -> None:
    """mode=value+join: уникальные значения со всех источников."""
    sheets = {
        "STATISTICS": (
            pd.DataFrame(
                {
                    "Табельный номер": ["00000000000000000001"],
                    "Фамилия": ["Иванов"],
                }
            ),
            {},
        ),
        "EMPLOYEE": (
            pd.DataFrame(
                {
                    "PERSON_NUMBER": [
                        "00000000000000000001",
                        "00000000000000000001",
                    ],
                    "SURNAME": ["Иванов", "Петров"],
                }
            ),
            {},
        ),
    }
    df_tabs = pd.DataFrame(
        {
            "№": [1],
            "Табельный номер": ["00000000000000000001"],
            "Источники": ["x"],
            "Число источников": [1],
        }
    )
    cfg = {
        "enrich_columns": [
            {
                "output_column": "Фамилии",
                "mode": "value",
                "multi_row": "join",
                "join_separator": ";",
                "sources": [
                    {
                        "priority": 1,
                        "sheet": "STATISTICS",
                        "tab_column": "Табельный номер",
                        "value_column": "Фамилия",
                    },
                    {
                        "priority": 2,
                        "sheet": "EMPLOYEE",
                        "tab_column": "PERSON_NUMBER",
                        "value_column": "SURNAME",
                    },
                ],
            }
        ],
    }
    out = enrich_tab_dataframe(df_tabs, sheets, cfg)
    assert out.iloc[0]["Фамилии"] == "Иванов;Петров"


def test_enrich_sum_uses_first_source_with_data() -> None:
    """sum/count — только первый источник в цепочке, у которого есть строки."""
    sheets = {
        "STATISTICS": (
            pd.DataFrame(
                {
                    "Табельный номер": ["00000000000000009999"],
                    "Балл": ["100"],
                }
            ),
            {},
        ),
        "RATING": (
            pd.DataFrame(
                {
                    "Табельный номер": [
                        "00000000000000000001",
                        "00000000000000000001",
                    ],
                    "Балл": ["5", "7"],
                }
            ),
            {},
        ),
    }
    df_tabs = pd.DataFrame(
        {
            "№": [1],
            "Табельный номер": ["00000000000000000001"],
            "Источники": ["x"],
            "Число источников": [1],
        }
    )
    cfg = {
        "enrich_columns": [
            {
                "output_column": "Сумма",
                "mode": "sum",
                "sources": [
                    {
                        "priority": 1,
                        "sheet": "STATISTICS",
                        "tab_column": "Табельный номер",
                        "value_column": "Балл",
                    },
                    {
                        "priority": 2,
                        "sheet": "RATING",
                        "tab_column": "Табельный номер",
                        "value_column": "Балл",
                    },
                ],
            }
        ],
    }
    out = enrich_tab_dataframe(df_tabs, sheets, cfg)
    assert out.iloc[0]["Сумма"] == "12"


def test_enrich_with_where_in_filter() -> None:
    sheets = {
        "STATISTICS": (
            pd.DataFrame(
                {
                    "Табельный номер": ["00000000000000000001", "00000000000000000001"],
                    "Фамилия": ["Иванов", "Петров"],
                    "Код роли": ["MGR", "ADM"],
                }
            ),
            {},
        ),
    }
    df_tabs = pd.DataFrame(
        {
            "№": [1],
            "Табельный номер": ["00000000000000000001"],
            "Источники": ["x"],
            "Число источников": [1],
        }
    )
    cfg = {
        "enrich_columns": [
            {
                "output_column": "Фамилия",
                "mode": "value",
                "multi_row": "first",
                "sources": [
                    {
                        "priority": 1,
                        "sheet": "STATISTICS",
                        "tab_column": "Табельный номер",
                        "value_column": "Фамилия",
                        "where_in": {"Код роли": ["MGR"]},
                    }
                ],
            }
        ],
    }
    out = enrich_tab_dataframe(df_tabs, sheets, cfg)
    assert out.iloc[0]["Фамилия"] == "Иванов"


def test_enrich_last_name_priority_chain() -> None:
    """Цепочка: RATING (период) → RATING (без фильтра) → STATISTICS (текущая роль) → EMPLOYEE."""
    sheets = {
        "RATING": (
            pd.DataFrame(
                {
                    "Табельный номер": [
                        "00000000000000000001",
                        "00000000000000000002",
                        "00000000000000000003",
                    ],
                    "Фамилия": ["РейтингСезон", "РейтингБезФильтра", ""],
                    "Период": ["Сезон 2026", "Сезон 2025", "Сезон 2026"],
                }
            ),
            {},
        ),
        "STATISTICS": (
            pd.DataFrame(
                {
                    "Табельный номер": ["00000000000000000003"],
                    "Фамилия": ["Статистика"],
                    "Текущая роль": ["true"],
                }
            ),
            {},
        ),
        "EMPLOYEE": (
            pd.DataFrame(
                {
                    "PERSON_NUMBER": [
                        "00000000000000000004",
                        "00000000000000000099",
                    ],
                    "PERSON_NUMBER_ADD": [
                        "00000000000000000099",
                        "00000000000000000005",
                    ],
                    "SURNAME": ["ПоОсновному", "ПоДополнительному"],
                }
            ),
            {},
        ),
    }
    df_tabs = pd.DataFrame(
        {
            "№": [1, 2, 3, 4, 5],
            "Табельный номер": [
                "00000000000000000001",
                "00000000000000000002",
                "00000000000000000003",
                "00000000000000000004",
                "00000000000000000005",
            ],
            "Источники": ["a"] * 5,
            "Число источников": [1] * 5,
        }
    )
    cfg = {
        "normalize_pad_width": 20,
        "enrich_columns": [
            {
                "output_column": "Фамилия",
                "mode": "value",
                "multi_row": "first",
                "sources": [
                    {
                        "priority": 1,
                        "sheet": "RATING",
                        "tab_column": "Табельный номер",
                        "value_column": "Фамилия",
                        "where_in": {"Период": ["Сезон 2026"]},
                    },
                    {
                        "priority": 2,
                        "sheet": "RATING",
                        "tab_column": "Табельный номер",
                        "value_column": "Фамилия",
                    },
                    {
                        "priority": 3,
                        "sheet": "STATISTICS",
                        "tab_column": "Табельный номер",
                        "value_column": "Фамилия",
                        "where_in": {"Текущая роль": [True]},
                    },
                    {
                        "priority": 4,
                        "sheet": "EMPLOYEE",
                        "tab_column": "PERSON_NUMBER",
                        "value_column": "SURNAME",
                    },
                    {
                        "priority": 5,
                        "sheet": "EMPLOYEE",
                        "tab_column": "PERSON_NUMBER_ADD",
                        "value_column": "SURNAME",
                    },
                ],
            }
        ],
    }
    out = enrich_tab_dataframe(df_tabs, sheets, cfg)
    assert out.iloc[0]["Фамилия"] == "РейтингСезон"
    assert out.iloc[1]["Фамилия"] == "РейтингБезФильтра"
    assert out.iloc[2]["Фамилия"] == "Статистика"
    assert out.iloc[3]["Фамилия"] == "ПоОсновному"
    assert out.iloc[4]["Фамилия"] == "ПоДополнительному"


def test_tab_match_with_unpadded_source() -> None:
    """Табельные в источнике без ведущих нулей сопоставляются с 20-значным форматом."""
    sheets = {
        "EMPLOYEE": (
            pd.DataFrame(
                {
                    "PERSON_NUMBER": ["12345"],
                    "SURNAME": ["Нормализован"],
                }
            ),
            {},
        ),
    }
    df_tabs = pd.DataFrame(
        {
            "№": [1],
            "Табельный номер": ["00000000000000012345"],
            "Источники": ["x"],
            "Число источников": [1],
        }
    )
    cfg = {
        "normalize_pad_width": 20,
        "enrich_columns": [
            {
                "output_column": "Фамилия",
                "sources": [
                    {
                        "priority": 1,
                        "sheet": "EMPLOYEE",
                        "tab_column": "PERSON_NUMBER",
                        "value_column": "SURNAME",
                    }
                ],
            }
        ],
    }
    out = enrich_tab_dataframe(df_tabs, sheets, cfg)
    assert out.iloc[0]["Фамилия"] == "Нормализован"


def test_build_workbook_includes_enrich() -> None:
    sheets = {
        "EMPLOYEE": (
            pd.DataFrame(
                {
                    "PERSON_NUMBER": ["00000000000000000001"],
                    "SURNAME": ["Иванов"],
                }
            ),
            {},
        ),
    }
    cfg = {
        "sources": [{"sheet": "EMPLOYEE", "tab_column": "PERSON_NUMBER"}],
        "enrich_columns": [
            {
                "output_column": "Фамилия",
                "sources": [
                    {
                        "priority": 1,
                        "sheet": "EMPLOYEE",
                        "tab_column": "PERSON_NUMBER",
                        "value_column": "SURNAME",
                    }
                ],
            }
        ],
    }
    data = build_manager_stats_workbook_data(sheets, cfg=cfg)
    df_tabs = data["TAB_NUMBERS"][0]
    assert "Фамилия" in df_tabs.columns
    assert df_tabs.iloc[0]["Фамилия"] == "Иванов"


def test_enrich_composite_key_org_unit() -> None:
    """TB_FULL_NAME / GOSB_NAME по составному ключу ТБ+ГОСБ из ORG_UNIT_V20."""
    sheets = {
        "EMPLOYEE": (
            pd.DataFrame(
                {
                    "PERSON_NUMBER": ["00000000000000000001"],
                    "TB_CODE": ["18"],
                    "GOSB_CODE": ["0"],
                }
            ),
            {},
        ),
        "ORG_UNIT_V20": (
            pd.DataFrame(
                {
                    "TB_CODE": ["18", "40"],
                    "GOSB_CODE": ["0", "1"],
                    "TB_FULL_NAME": ["Байкальский банк", "Московский банк"],
                    "GOSB_NAME": ["Аппарат территориального банка", "Другое ГОСБ"],
                }
            ),
            {},
        ),
    }
    df_tabs = pd.DataFrame(
        {
            "№": [1, 2],
            "Табельный номер": [
                "00000000000000000001",
                "00000000000000000002",
            ],
            "ТБ": ["18", "99"],
            "ГОСБ": ["0", "1"],
            "Источники": ["x", "y"],
            "Число источников": [1, 1],
        }
    )
    cfg = {
        "sources": [{"sheet": "EMPLOYEE", "tab_column": "PERSON_NUMBER"}],
        "enrich_columns": [
            {
                "output_column": "TB_FULL_NAME",
                "lookup_row_key": ["ТБ", "ГОСБ"],
                "sources": [
                    {
                        "priority": 1,
                        "sheet": "ORG_UNIT_V20",
                        "key_columns": ["TB_CODE", "GOSB_CODE"],
                        "value_column": "TB_FULL_NAME",
                    }
                ],
            },
            {
                "output_column": "GOSB_NAME",
                "lookup_row_key": ["ТБ", "ГОСБ"],
                "sources": [
                    {
                        "priority": 1,
                        "sheet": "ORG_UNIT_V20",
                        "key_columns": ["TB_CODE", "GOSB_CODE"],
                        "value_column": "GOSB_NAME",
                    }
                ],
            },
        ],
    }
    out = enrich_tab_dataframe(df_tabs, sheets, cfg)
    assert out.iloc[0]["TB_FULL_NAME"] == "Байкальский банк"
    assert out.iloc[0]["GOSB_NAME"] == "Аппарат территориального банка"
    assert out.iloc[1]["TB_FULL_NAME"] == "-"
    assert out.iloc[1]["GOSB_NAME"] == "-"


def test_enrich_exists_in_rating() -> None:
    sheets = {
        "RATING": (
            pd.DataFrame(
                {
                    "Табельный номер": [
                        "00000000000000000001",
                        "00000000000000000002",
                        "00000000000000000001",
                    ],
                    "Наименование Роли": [
                        "Клиентский менеджер крупнейшего, крупного и среднего бизнеса",
                        "Клиентский менеджер крупнейшего, крупного и среднего бизнеса",
                        "Другая роль",
                    ],
                    "Период": ["Сезон 2026", "Сезон 2025", "Сезон 2026"],
                }
            ),
            {},
        ),
    }
    df_tabs = pd.DataFrame(
        {
            "№": [1, 2, 3],
            "Табельный номер": [
                "00000000000000000001",
                "00000000000000000002",
                "00000000000000000003",
            ],
            "Источники": ["a", "b", "c"],
            "Число источников": [1, 1, 1],
        }
    )
    cfg = {
        "enrich_columns": [
            {
                "output_column": "есть в текущем рейтинге",
                "mode": "exists",
                "present_value": "ДА",
                "default": "-",
                "sources": [
                    {
                        "priority": 1,
                        "sheet": "RATING",
                        "tab_column": "Табельный номер",
                        "where_in": {
                            "Наименование Роли": [
                                "Клиентский менеджер крупнейшего, крупного и среднего бизнеса"
                            ],
                            "Период": ["Сезон 2026"],
                        },
                    }
                ],
            }
        ],
    }
    out = enrich_tab_dataframe(df_tabs, sheets, cfg)
    assert out.iloc[0]["есть в текущем рейтинге"] == "ДА"
    assert out.iloc[1]["есть в текущем рейтинге"] == "-"
    assert out.iloc[2]["есть в текущем рейтинге"] == "-"


def test_workbook_tab_column_widths() -> None:
    data = build_manager_stats_workbook_data({}, cfg={})
    tab_params = data["TAB_NUMBERS"][1]
    widths = tab_params.get("added_columns_width") or {}
    assert widths["Табельный номер"]["width_mode"] == 24
    assert widths["Источники"]["min_width"] == 50
    assert widths["Источники"]["max_width"] == 80
