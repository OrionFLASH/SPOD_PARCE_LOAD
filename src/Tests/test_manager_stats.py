# -*- coding: utf-8 -*-
"""Тесты сбора табельных по manager_stats.sources с фильтрами."""

from __future__ import annotations

import pandas as pd

from src.manager_stats import (
    _build_enrich_field_context,
    _build_filter_mask,
    _normalized_enrich_fields_from_config,
    build_manager_stats_summary_dataframe,
    build_manager_stats_workbook_data,
    collect_tab_numbers_from_sheets,
    enrich_tab_dataframe,
    merge_manager_stats_config,
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


def test_employee_surname_ellipsis_filtered_from_sources() -> None:
    """Строки EMPLOYEE с SURNAME=«…» не попадают в исходный список табельных (sources)."""
    sheets = {
        "EMPLOYEE": (
            pd.DataFrame(
                {
                    "PERSON_NUMBER": [
                        "00000000000000000001",
                        "00000000000000000002",
                    ],
                    "SURNAME": ["Иванов", "…"],
                }
            ),
            {},
        ),
    }
    cfg = {
        "sources": [
            {
                "sheet": "EMPLOYEE",
                "tab_column": "PERSON_NUMBER",
                "where_not_in": {"SURNAME": ["…", "..."]},
            }
        ],
    }
    df_tabs, _ = collect_tab_numbers_from_sheets(sheets, cfg=cfg)
    tabs = set(df_tabs["Табельный номер"].tolist())
    assert "00000000000000000001" in tabs
    assert "00000000000000000002" not in tabs


def test_employee_placeholder_excluded_even_from_other_sources() -> None:
    """Заглушка EMPLOYEE убирает табельный из итога, даже если он пришёл с REPORT."""
    sheets = {
        "EMPLOYEE": (
            pd.DataFrame(
                {
                    "PERSON_NUMBER": ["00000000000000000099"],
                    "PERSON_NUMBER_ADD": ["00000000000000000099"],
                    "SURNAME": ["…"],
                }
            ),
            {},
        ),
        "REPORT": (
            pd.DataFrame(
                {
                    "MANAGER_PERSON_NUMBER": ["00000000000000000099"],
                }
            ),
            {},
        ),
    }
    cfg = {
        "sources": [
            {
                "id": "report_manager",
                "sheet": "REPORT",
                "tab_column": "MANAGER_PERSON_NUMBER",
            },
            {
                "id": "employee_person",
                "sheet": "EMPLOYEE",
                "tab_column": "PERSON_NUMBER",
                "where_not_in": {"SURNAME": ["…", "..."]},
            },
        ],
    }
    df_tabs, _ = collect_tab_numbers_from_sheets(sheets, cfg=cfg)
    assert df_tabs.empty or "00000000000000000099" not in set(df_tabs["Табельный номер"].tolist())


def test_employee_position_name_filtered_from_sources() -> None:
    """Строки EMPLOYEE с POSITION_NAME из списка исключений не попадают в список табельных."""
    sheets = {
        "EMPLOYEE": (
            pd.DataFrame(
                {
                    "PERSON_NUMBER": [
                        "00000000000000000001",
                        "00000000000000000002",
                    ],
                    "SURNAME": ["Иванов", "Петров"],
                    "POSITION_NAME": ["Менеджер", "КПК"],
                }
            ),
            {},
        ),
    }
    cfg = {
        "sources": [
            {
                "sheet": "EMPLOYEE",
                "tab_column": "PERSON_NUMBER",
                "where_not_in": {
                    "SURNAME": ["…", "..."],
                    "POSITION_NAME": ["КПК", "ГОСБ", "ТБ"],
                },
            }
        ],
    }
    df_tabs, _ = collect_tab_numbers_from_sheets(sheets, cfg=cfg)
    tabs = set(df_tabs["Табельный номер"].tolist())
    assert "00000000000000000001" in tabs
    assert "00000000000000000002" not in tabs


def test_enrich_email_sigma_and_alpha() -> None:
    """Email Sigma / Email Alpha: STATISTICS, затем ORDER."""
    sheets = {
        "STATISTICS": (
            pd.DataFrame(
                {
                    "Табельный номер": [
                        "00000000000000000001",
                        "00000000000000000002",
                        "00000000000000000003",
                    ],
                    "Почта Сигма": ["sigma@stats.ru", "", ""],
                    "Почта Альфа": ["alpha@stats.ru", "alpha2@stats.ru", ""],
                }
            ),
            {},
        ),
        "ORDER": (
            pd.DataFrame(
                {
                    "Табельный номер": [
                        "00000000000000000002",
                        "00000000000000000003",
                    ],
                    "Email в домене Sigma": ["sigma@order.ru", "sigma3@order.ru"],
                    "Email в домене Alpha": ["alpha@order.ru", "alpha3@order.ru"],
                    "Статус заказа": ["Выполнен", "Выполнен"],
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
                "id": "email_sigma",
                "output_column": "Email Sigma",
                "sources": [
                    {
                        "priority": 1,
                        "sheet": "STATISTICS",
                        "tab_column": "Табельный номер",
                        "value_column": "Почта Сигма",
                    },
                    {
                        "priority": 2,
                        "sheet": "ORDER",
                        "tab_column": "Табельный номер",
                        "value_column": "Email в домене Sigma",
                    },
                ],
            },
            {
                "id": "email_alpha",
                "output_column": "Email Alpha",
                "sources": [
                    {
                        "priority": 1,
                        "sheet": "STATISTICS",
                        "tab_column": "Табельный номер",
                        "value_column": "Почта Альфа",
                    },
                    {
                        "priority": 2,
                        "sheet": "ORDER",
                        "tab_column": "Табельный номер",
                        "value_column": "Email в домене Alpha",
                    },
                ],
            },
        ],
    }
    out = enrich_tab_dataframe(df_tabs, sheets, cfg)
    assert out.iloc[0]["Email Sigma"] == "sigma@stats.ru"
    assert out.iloc[0]["Email Alpha"] == "alpha@stats.ru"
    assert out.iloc[1]["Email Sigma"] == "sigma@order.ru"
    assert out.iloc[1]["Email Alpha"] == "alpha2@stats.ru"
    assert out.iloc[2]["Email Sigma"] == "sigma3@order.ru"
    assert out.iloc[2]["Email Alpha"] == "alpha3@order.ru"


def test_enrich_rating_groups_by_role_and_period() -> None:
    """Метрики RATING по группам Наименование Роли + Период."""
    role = "Клиентский менеджер крупнейшего, крупного и среднего бизнеса"
    sheets = {
        "RATING": (
            pd.DataFrame(
                {
                    "Табельный номер": [
                        "00000000000000000001",
                        "00000000000000000001",
                        "00000000000000000002",
                    ],
                    "Наименование Роли": [role, role, role],
                    "Период": ["Сезон 2026", "Сезон 2024", "Сезон 2026"],
                    "Количество кристаллов": ["100", "50", "200"],
                    "Место в рейтинге по стране": ["1", "10", "5"],
                    "Место в рейтинге ТБ": ["2", "20", "6"],
                    "Место в рейтинге ГОСБ": ["3", "30", "7"],
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
        "enrich_columns": [
            {
                "id": "rating_crystals_season_2026",
                "output_column": "Количество кристаллов | Сезон 2026",
                "sources": [
                    {
                        "priority": 1,
                        "sheet": "RATING",
                        "tab_column": "Табельный номер",
                        "value_column": "Количество кристаллов",
                        "where_in": {
                            "Наименование Роли": [role],
                            "Период": ["Сезон 2026"],
                        },
                    }
                ],
            },
            {
                "id": "rating_crystals_season_2024",
                "output_column": "Количество кристаллов | Сезон 2024",
                "sources": [
                    {
                        "priority": 1,
                        "sheet": "RATING",
                        "tab_column": "Табельный номер",
                        "value_column": "Количество кристаллов",
                        "where_in": {
                            "Наименование Роли": [role],
                            "Период": ["Сезон 2024"],
                        },
                    }
                ],
            },
            {
                "id": "rating_rank_country_season_2026",
                "output_column": "Место в рейтинге по стране | Сезон 2026",
                "sources": [
                    {
                        "priority": 1,
                        "sheet": "RATING",
                        "tab_column": "Табельный номер",
                        "value_column": "Место в рейтинге по стране",
                        "where_in": {
                            "Наименование Роли": [role],
                            "Период": ["Сезон 2026"],
                        },
                    }
                ],
            },
        ],
    }
    out = enrich_tab_dataframe(df_tabs, sheets, cfg)
    assert out.iloc[0]["Количество кристаллов | Сезон 2026"] == "100"
    assert out.iloc[0]["Количество кристаллов | Сезон 2024"] == "50"
    assert out.iloc[0]["Место в рейтинге по стране | Сезон 2026"] == "1"
    assert out.iloc[1]["Количество кристаллов | Сезон 2026"] == "200"
    assert out.iloc[2]["Количество кристаллов | Сезон 2026"] == "-"


def test_manager_stats_summary_includes_enrich_and_sources() -> None:
    """MANAGER_STATS_SUMMARY: sources, enrich и форматы колонок TAB_NUMBERS."""
    sheets = {
        "EMPLOYEE": (
            pd.DataFrame({"PERSON_NUMBER": ["00000000000000000001"]}),
            {},
        ),
    }
    cfg = {
        "sources": [{"id": "emp", "sheet": "EMPLOYEE", "tab_column": "PERSON_NUMBER"}],
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
                        "where_in": {"Текущая роль": [True]},
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
        "column_formats": [
            {
                "column_prefixes": ["Количество кристаллов |"],
                "data_type": "number",
                "decimal_places": 0,
            }
        ],
    }
    _, df_sources = collect_tab_numbers_from_sheets(sheets, cfg=cfg)
    df_summary = build_manager_stats_summary_dataframe(df_sources, cfg)
    assert "Раздел" in df_summary.columns
    assert "Обогащение" in set(df_summary["Раздел"].dropna())
    assert "Сбор табельных" in set(df_summary["Раздел"].dropna())
    enrich = df_summary[df_summary["Раздел"] == "Обогащение"]
    assert (enrich["Колонка TAB_NUMBERS"] == "Фамилия").any()
    assert (enrich["Приоритет"] == "1").any()
    assert "Текущая роль" in enrich.iloc[0]["Фильтры"]
    data = build_manager_stats_workbook_data(sheets, cfg=cfg)
    summary_df = data["MANAGER_STATS_SUMMARY"][0]
    assert "Логика" in summary_df.columns
    assert len(summary_df) >= 3


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
    role_km = "Клиентский менеджер крупнейшего, крупного и среднего бизнеса"
    sheets = {
        "RATING": (
            pd.DataFrame(
                {
                    "Табельный номер": [
                        "00000000000000000001",
                        "00000000000000000002",
                        "00000000000000000003",
                        "00000000000000000004",
                    ],
                    "Наименование Роли": [
                        role_km,
                        role_km,
                        "Другая роль",
                        "Руководитель проектов по технологическому развитию клиентов",
                    ],
                    "Период": ["Сезон 2026", "Сезон 2025", "Сезон 2026", "Сезон 2026"],
                }
            ),
            {},
        ),
    }
    df_tabs = pd.DataFrame(
        {
            "№": [1, 2, 3, 4],
            "Табельный номер": [
                "00000000000000000001",
                "00000000000000000002",
                "00000000000000000003",
                "00000000000000000004",
            ],
            "Источники": ["a", "b", "c", "d"],
            "Число источников": [1, 1, 1, 1],
        }
    )
    cfg = {
        "enrich_columns": [
            {
                "output_column": "есть в текущем рейтинге",
                "mode": "exists",
                "multi_row": "first",
                "default": "-",
                "sources": [
                    {
                        "priority": 1,
                        "sheet": "RATING",
                        "tab_column": "Табельный номер",
                        "present_value": "КМ",
                        "where_in": {
                            "Наименование Роли": [role_km],
                            "Период": ["Сезон 2026"],
                        },
                    },
                    {
                        "priority": 4,
                        "sheet": "RATING",
                        "tab_column": "Табельный номер",
                        "present_value": "CSM",
                        "where_in": {
                            "Наименование Роли": [
                                "Руководитель проектов по технологическому развитию клиентов"
                            ],
                            "Период": ["Сезон 2026"],
                        },
                    },
                ],
            }
        ],
    }
    out = enrich_tab_dataframe(df_tabs, sheets, cfg)
    assert out.iloc[0]["есть в текущем рейтинге"] == "КМ"
    assert out.iloc[1]["есть в текущем рейтинге"] == "-"
    assert out.iloc[2]["есть в текущем рейтинге"] == "-"
    assert out.iloc[3]["есть в текущем рейтинге"] == "CSM"


def test_enrich_exists_in_rating_join_multiple_roles() -> None:
    """Несколько ролей в RATING → коды через join_separator."""
    role_km = "Клиентский менеджер крупнейшего, крупного и среднего бизнеса"
    role_csm = "Руководитель проектов по технологическому развитию клиентов"
    sheets = {
        "RATING": (
            pd.DataFrame(
                {
                    "Табельный номер": [
                        "00000000000000000001",
                        "00000000000000000001",
                    ],
                    "Наименование Роли": [role_km, role_csm],
                    "Период": ["Сезон 2026", "Сезон 2026"],
                }
            ),
            {},
        ),
    }
    df_tabs = pd.DataFrame(
        {
            "№": [1],
            "Табельный номер": ["00000000000000000001"],
            "Источники": ["a"],
            "Число источников": [1],
        }
    )
    cfg = {
        "enrich_columns": [
            {
                "output_column": "есть в текущем рейтинге",
                "mode": "exists",
                "multi_row": "join",
                "join_separator": "; ",
                "default": "-",
                "sources": [
                    {
                        "priority": 1,
                        "sheet": "RATING",
                        "tab_column": "Табельный номер",
                        "present_value": "КМ",
                        "where_in": {
                            "Наименование Роли": [role_km],
                            "Период": ["Сезон 2026"],
                        },
                    },
                    {
                        "priority": 4,
                        "sheet": "RATING",
                        "tab_column": "Табельный номер",
                        "present_value": "CSM",
                        "where_in": {
                            "Наименование Роли": [role_csm],
                            "Период": ["Сезон 2026"],
                        },
                    },
                ],
            }
        ],
    }
    out = enrich_tab_dataframe(df_tabs, sheets, cfg)
    assert out.iloc[0]["есть в текущем рейтинге"] == "КМ; CSM"


def test_enrich_exists_in_rating_join_uses_single_combined_index() -> None:
    """exists+join строит один комбинированный индекс на лист RATING."""
    role_km = "Клиентский менеджер крупнейшего, крупного и среднего бизнеса"
    role_csm = "Руководитель проектов по технологическому развитию клиентов"
    sheets = {
        "RATING": (
            pd.DataFrame(
                {
                    "Табельный номер": ["1", "1"],
                    "Наименование Роли": [role_km, role_csm],
                    "Период": ["Сезон 2026", "Сезон 2026"],
                }
            ),
            {},
        ),
    }
    mcfg = merge_manager_stats_config(
        {
            "enrich_columns": [
                {
                    "id": "in_current_rating",
                    "output_column": "есть в текущем рейтинге",
                    "mode": "exists",
                    "multi_row": "join",
                    "join_separator": "; ",
                    "sources": [
                        {
                            "priority": 1,
                            "sheet": "RATING",
                            "tab_column": "Табельный номер",
                            "present_value": "КМ",
                            "where_in": {
                                "Наименование Роли": [role_km],
                                "Период": ["Сезон 2026"],
                            },
                        },
                        {
                            "priority": 4,
                            "sheet": "RATING",
                            "tab_column": "Табельный номер",
                            "present_value": "CSM",
                            "where_in": {
                                "Наименование Роли": [role_csm],
                                "Период": ["Сезон 2026"],
                            },
                        },
                    ],
                }
            ],
        }
    )
    field = _normalized_enrich_fields_from_config(mcfg)[0]
    ctx = _build_enrich_field_context(field, sheets, ["RATING"], pad_width=20)
    assert len(ctx.sources) == 1
    tab = "00000000000000000001"
    assert ctx.sources[0].join_map[tab] == ["КМ", "CSM"]


def test_workbook_tab_column_widths() -> None:
    data = build_manager_stats_workbook_data({}, cfg={})
    tab_params = data["TAB_NUMBERS"][1]
    widths = tab_params.get("added_columns_width") or {}
    assert widths["Табельный номер"]["width_mode"] == 24
    assert widths["Источники"]["min_width"] == 50
    assert widths["Источники"]["max_width"] == 80
    fmt_rules = tab_params.get("column_format_rules") or []
    assert fmt_rules
    assert fmt_rules[0].get("data_type") == "number"
    assert "Количество кристаллов |" in (fmt_rules[0].get("column_prefixes") or [])


def test_enrich_rating_numeric_conversion_for_excel() -> None:
    """Колонки RATING-групп преобразуются в числа перед записью Excel."""
    from src.main_impl import apply_column_format_conversion

    df = pd.DataFrame(
        {
            "Количество кристаллов | Сезон 2026": ["1 234", "-"],
            "Место в рейтинге ТБ | Сезон 2026": ["5", ""],
            "Фамилия": ["Иванов", "Петров"],
        }
    )
    apply_column_format_conversion(
        df,
        "TAB_NUMBERS",
        extra_rules=[
            {
                "column_prefixes": [
                    "Количество кристаллов |",
                    "Место в рейтинге по стране |",
                    "Место в рейтинге ТБ |",
                    "Место в рейтинге ГОСБ |",
                ],
                "data_type": "number",
                "decimal_places": 0,
            }
        ],
    )
    assert df.iloc[0]["Количество кристаллов | Сезон 2026"] == 1234
    assert pd.isna(df.iloc[1]["Количество кристаллов | Сезон 2026"])
    assert df.iloc[0]["Место в рейтинге ТБ | Сезон 2026"] == 5
    assert df.iloc[0]["Фамилия"] == "Иванов"


def test_tb_gosb_numeric_conversion_for_excel() -> None:
    """Колонки ТБ и ГОСБ — числовой формат при записи Excel."""
    from src.main_impl import apply_column_format_conversion

    df = pd.DataFrame(
        {
            "ТБ": ["18", "-"],
            "ГОСБ": ["0", "5"],
            "Фамилия": ["Иванов", "Петров"],
        }
    )
    apply_column_format_conversion(
        df,
        "TAB_NUMBERS",
        extra_rules=[
            {
                "columns": ["ТБ", "ГОСБ"],
                "data_type": "number",
                "decimal_places": 0,
            }
        ],
    )
    assert df.iloc[0]["ТБ"] == 18
    assert df.iloc[0]["ГОСБ"] == 0
    assert pd.isna(df.iloc[1]["ТБ"])
    assert df.iloc[1]["ГОСБ"] == 5
