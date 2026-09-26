# -*- coding: utf-8 -*-
"""
Эталонная (прежняя, до PERF-02) реализация collect_summary_keys — дословная копия из
src/main_impl.py на коммите c2486fa. Используется только в тестах: новая реализация должна
давать тот же НАБОР строк (порядок в новой — детерминированная сортировка, BUG-13).
"""

import logging

import pandas as pd

from src.main_impl import SUMMARY_KEY_COLUMNS


def legacy_collect_summary_keys(dfs):
    """
    Собирает все реально существующие сочетания ключей,
    включая осиротевшие коды и сочетания с GROUP_VALUE и INDICATOR_ADD_CALC_TYPE.
    Теперь учитывает ВСЕ коды из всех таблиц, включая CONTEST-DATA и INDICATOR.
    ИСПРАВЛЕНИЕ: GROUP_VALUE правильно связан с конкретным GROUP_CODE.
    """
    all_rows = []

    # ОПТИМИЗАЦИЯ v5.0: Проверка на None перед использованием
    rewards = dfs.get("REWARD-LINK", pd.DataFrame())
    tournaments = dfs.get("TOURNAMENT-SCHEDULE", pd.DataFrame())
    groups = dfs.get("GROUP", pd.DataFrame())
    reward_data = dfs.get("REWARD", pd.DataFrame())
    contest_data = dfs.get("CONTEST-DATA", pd.DataFrame())
    indicators = dfs.get("INDICATOR", pd.DataFrame())
    
    # Заменяем None на пустые DataFrame
    if rewards is None:
        rewards = pd.DataFrame()
    if tournaments is None:
        tournaments = pd.DataFrame()
    if groups is None:
        groups = pd.DataFrame()
    if reward_data is None:
        reward_data = pd.DataFrame()
    if contest_data is None:
        contest_data = pd.DataFrame()
    if indicators is None:
        indicators = pd.DataFrame()

    # Коды для детального логирования
    DEBUG_CODES = []  # Отключено подробное логирование
    
    all_contest_codes = set()
    all_tournament_codes = set()
    all_reward_codes = set()
    all_group_codes = set()
    all_group_values = set()
    all_indicator_add_calc_types = set()

    # Собираем ВСЕ коды из всех таблиц
    if not rewards.empty:
        all_contest_codes.update(rewards["CONTEST_CODE"].dropna())
        all_reward_codes.update(rewards["REWARD_CODE"].dropna())
    if not tournaments.empty:
        all_contest_codes.update(tournaments["CONTEST_CODE"].dropna())
        all_tournament_codes.update(tournaments["TOURNAMENT_CODE"].dropna())
    if not groups.empty:
        all_contest_codes.update(groups["CONTEST_CODE"].dropna())
        all_group_codes.update(groups["GROUP_CODE"].dropna())
        all_group_values.update(groups["GROUP_VALUE"].dropna())
    if not contest_data.empty:
        all_contest_codes.update(contest_data["CONTEST_CODE"].dropna())
    if not reward_data.empty:
        all_reward_codes.update(reward_data["REWARD_CODE"].dropna())
    if not indicators.empty:
        all_contest_codes.update(indicators["CONTEST_CODE"].dropna())
        indicator_types = indicators["INDICATOR_ADD_CALC_TYPE"].fillna("").unique()
        all_indicator_add_calc_types.update(indicator_types)

    def _indicator_code_for_contest_type(ind_df: pd.DataFrame, contest_code: str, ind_type: str) -> str:
        """Для пары (CONTEST_CODE, INDICATOR_ADD_CALC_TYPE) возвращает INDICATOR_CODE при наличии совпадений (первый при нескольких)."""
        if ind_df is None or ind_df.empty or contest_code == "-":
            return ""
        cc = str(contest_code).strip()
        it = str(ind_type).strip()
        m = ind_df[
            (ind_df["CONTEST_CODE"].astype(str).str.strip() == cc)
            & (ind_df["INDICATOR_ADD_CALC_TYPE"].fillna("").astype(str).str.strip() == it)
        ]
        if m.empty:
            return ""
        codes = m["INDICATOR_CODE"].dropna().astype(str).str.strip().unique()
        return codes[0] if len(codes) >= 1 else ""

    # 1. Для каждого CONTEST_CODE
    for code in all_contest_codes:
        is_debug = str(code) in DEBUG_CODES
        if is_debug:
            logging.debug(f"[GROUP] === Обработка CONTEST_CODE: {code} ===")
        
        tourns = tournaments[tournaments["CONTEST_CODE"] == code][
            "TOURNAMENT_CODE"].dropna().unique() if not tournaments.empty else []
        rewards_ = rewards[rewards["CONTEST_CODE"] == code][
            "REWARD_CODE"].dropna().unique() if not rewards.empty else []
        groups_df = groups[groups["CONTEST_CODE"] == code] if not groups.empty else pd.DataFrame()
        
        if is_debug:
            logging.debug(f"[GROUP] Найдено строк в GROUP для CONTEST_CODE {code}: {len(groups_df)}")
            if not groups_df.empty:
                logging.debug(f"[GROUP] Строки GROUP:\n{groups_df[['GROUP_CODE', 'GROUP_VALUE', 'CONTEST_CODE']].to_string()}")
        
        # ИСПРАВЛЕНИЕ: GROUP_VALUE должен быть связан с конкретным GROUP_CODE
        # Вместо декартова произведения создаем пары (GROUP_CODE, GROUP_VALUE)
        group_code_value_pairs = []
        if not groups_df.empty:
            # Создаем список уникальных пар (GROUP_CODE, GROUP_VALUE)
            for _, row in groups_df.iterrows():
                g_code = row.get("GROUP_CODE", "")
                g_value = row.get("GROUP_VALUE", "")
                if pd.notna(g_code) and pd.notna(g_value):
                    pair = (str(g_code), str(g_value))
                    if pair not in group_code_value_pairs:
                        group_code_value_pairs.append(pair)
        
        if is_debug:
            logging.debug(f"[GROUP] Уникальные пары (GROUP_CODE, GROUP_VALUE) для CONTEST_CODE {code}: {group_code_value_pairs}")
            if not groups_df.empty:
                unique_groups = groups_df["GROUP_CODE"].dropna().unique()
                unique_values = groups_df["GROUP_VALUE"].dropna().unique()
                logging.debug(f"[GROUP] Уникальные GROUP_CODE: {list(unique_groups)}")
                logging.debug(f"[GROUP] Уникальные GROUP_VALUE: {list(unique_values)}")
        
        # Если нет пар, создаем одну с "-"
        if not group_code_value_pairs:
            group_code_value_pairs = [("-", "-")]
        
        # Добавляем INDICATOR_ADD_CALC_TYPE для данного CONTEST_CODE
        indicator_types_ = []
        if not indicators.empty:
            indicator_df = indicators[indicators["CONTEST_CODE"] == code]
            if not indicator_df.empty:
                indicator_types_ = indicator_df["INDICATOR_ADD_CALC_TYPE"].fillna("").unique().tolist()
        
        tourns = tourns if len(tourns) else ["-"]
        rewards_ = rewards_ if len(rewards_) else ["-"]
        indicator_types_ = indicator_types_ if len(indicator_types_) else [""]
        
        if is_debug:
            logging.debug(f"[GROUP] TOURNAMENT_CODE: {list(tourns)}")
            logging.debug(f"[GROUP] REWARD_CODE: {list(rewards_)}")
            logging.debug(f"[GROUP] INDICATOR_ADD_CALC_TYPE: {indicator_types_}")
            logging.debug(f"[GROUP] Будет создано комбинаций: {len(tourns)} x {len(rewards_)} x {len(group_code_value_pairs)} x {len(indicator_types_)} = {len(tourns) * len(rewards_) * len(group_code_value_pairs) * len(indicator_types_)}")

        for t in tourns:
            for r in rewards_:
                for g_code, g_value in group_code_value_pairs:
                    for ind_type in indicator_types_:
                        ind_code = _indicator_code_for_contest_type(indicators, str(code), ind_type)
                        all_rows.append((str(code), str(t), str(r), str(g_code), str(g_value), ind_code, str(ind_type)))
                        if is_debug:
                            logging.debug(f"[GROUP] Создана строка: CONTEST={code}, TOURNAMENT={t}, REWARD={r}, GROUP_CODE={g_code}, GROUP_VALUE={g_value}, INDICATOR={ind_type}")

    # 2. Для каждого TOURNAMENT_CODE (даже если нет CONTEST_CODE)
    if not tournaments.empty:
        for t_code in tournaments["TOURNAMENT_CODE"].dropna().unique():
            code = tournaments[tournaments["TOURNAMENT_CODE"] == t_code]["CONTEST_CODE"].dropna().unique()
            code = code[0] if len(code) else "-"
            is_debug = str(code) in DEBUG_CODES or str(t_code) in DEBUG_CODES
            
            rewards_ = rewards[rewards["CONTEST_CODE"] == code][
                "REWARD_CODE"].dropna().unique() if not rewards.empty else []
            groups_df = groups[groups["CONTEST_CODE"] == code] if not groups.empty else pd.DataFrame()
            
            # ИСПРАВЛЕНИЕ: Используем пары (GROUP_CODE, GROUP_VALUE)
            group_code_value_pairs = []
            if not groups_df.empty:
                for _, row in groups_df.iterrows():
                    g_code = row.get("GROUP_CODE", "")
                    g_value = row.get("GROUP_VALUE", "")
                    if pd.notna(g_code) and pd.notna(g_value):
                        pair = (str(g_code), str(g_value))
                        if pair not in group_code_value_pairs:
                            group_code_value_pairs.append(pair)
            
            if not group_code_value_pairs:
                group_code_value_pairs = [("-", "-")]
            
            indicator_types_ = []
            if code != "-" and not indicators.empty:
                indicator_df = indicators[indicators["CONTEST_CODE"] == code]
                if not indicator_df.empty:
                    indicator_types_ = indicator_df["INDICATOR_ADD_CALC_TYPE"].fillna("").unique().tolist()
            
            rewards_ = rewards_ if len(rewards_) else ["-"]
            indicator_types_ = indicator_types_ if len(indicator_types_) else [""]
            
            for r in rewards_:
                for g_code, g_value in group_code_value_pairs:
                    for ind_type in indicator_types_:
                        ind_code = _indicator_code_for_contest_type(indicators, str(code), ind_type)
                        all_rows.append((str(code), str(t_code), str(r), str(g_code), str(g_value), ind_code, str(ind_type)))

    # 3. Для каждого REWARD_CODE (даже если нет CONTEST_CODE)
    for r_code in all_reward_codes:
        if not rewards.empty:
            code = rewards[rewards["REWARD_CODE"] == r_code]["CONTEST_CODE"].dropna().unique()
            code = code[0] if len(code) else "-"
        else:
            code = "-"
        
        is_debug = str(code) in DEBUG_CODES or str(r_code) in DEBUG_CODES

        if code != "-" and not tournaments.empty:
            tourns = tournaments[tournaments["CONTEST_CODE"] == code]["TOURNAMENT_CODE"].dropna().unique()
        else:
            tourns = []

        if code != "-" and not groups.empty:
            groups_df = groups[groups["CONTEST_CODE"] == code]
            # ИСПРАВЛЕНИЕ: Используем пары (GROUP_CODE, GROUP_VALUE)
            group_code_value_pairs = []
            for _, row in groups_df.iterrows():
                g_code = row.get("GROUP_CODE", "")
                g_value = row.get("GROUP_VALUE", "")
                if pd.notna(g_code) and pd.notna(g_value):
                    pair = (str(g_code), str(g_value))
                    if pair not in group_code_value_pairs:
                        group_code_value_pairs.append(pair)
        else:
            group_code_value_pairs = []
        
        if not group_code_value_pairs:
            group_code_value_pairs = [("-", "-")]
        
        indicator_types_ = []
        if code != "-" and not indicators.empty:
            indicator_df = indicators[indicators["CONTEST_CODE"] == code]
            if not indicator_df.empty:
                indicator_types_ = indicator_df["INDICATOR_ADD_CALC_TYPE"].fillna("").unique().tolist()

        tourns = tourns if len(tourns) else ["-"]
        indicator_types_ = indicator_types_ if len(indicator_types_) else [""]

        for t in tourns:
            for g_code, g_value in group_code_value_pairs:
                for ind_type in indicator_types_:
                    ind_code = _indicator_code_for_contest_type(indicators, str(code), ind_type)
                    all_rows.append((str(code), str(t), str(r_code), str(g_code), str(g_value), ind_code, str(ind_type)))

        # 4. Для каждого GROUP_CODE (даже если нет CONTEST_CODE)
    if not groups.empty:
        for g_code in groups["GROUP_CODE"].dropna().unique():
            is_debug = str(g_code) in DEBUG_CODES
            
            if is_debug:
                logging.debug(f"[GROUP] === Обработка GROUP_CODE: {g_code} ===")
            
            # ИСПРАВЛЕНИЕ: Находим все CONTEST_CODE для данного GROUP_CODE и обрабатываем каждый отдельно
            group_contest_codes = groups[groups["GROUP_CODE"] == g_code]["CONTEST_CODE"].dropna().unique()
            
            if is_debug:
                logging.debug(f"[GROUP] Найдено CONTEST_CODE для GROUP_CODE {g_code}: {list(group_contest_codes)}")
            
            # Обрабатываем каждый CONTEST_CODE отдельно
            for group_contest_code in group_contest_codes:
                actual_code = str(group_contest_code)
                
                if is_debug:
                    logging.debug(f"[GROUP] Обработка GROUP_CODE {g_code} для CONTEST_CODE: {actual_code}")
                
                # Берем GROUP_VALUE только для конкретного CONTEST_CODE и GROUP_CODE
                group_values_df = groups[(groups["GROUP_CODE"] == g_code) & (groups["CONTEST_CODE"] == actual_code)]
                group_values_ = group_values_df["GROUP_VALUE"].dropna().unique() if not group_values_df.empty else []
                
                if is_debug:
                    logging.debug(f"[GROUP] Найдено строк в GROUP для GROUP_CODE {g_code} и CONTEST_CODE {actual_code}: {len(group_values_df)}")
                    if not group_values_df.empty:
                        logging.debug(f"[GROUP] Строки GROUP:\n{group_values_df[['GROUP_CODE', 'GROUP_VALUE', 'CONTEST_CODE']].to_string()}")
                    logging.debug(f"[GROUP] Уникальные GROUP_VALUE: {list(group_values_)}")
                
                # Ищем связанные TOURNAMENT_CODE и REWARD_CODE для этого CONTEST_CODE
                tourns = tournaments[tournaments["CONTEST_CODE"] == actual_code][
                    "TOURNAMENT_CODE"].dropna().unique() if not tournaments.empty else []
                rewards_ = rewards[rewards["CONTEST_CODE"] == actual_code][
                    "REWARD_CODE"].dropna().unique() if not rewards.empty else []
                
                # Добавляем INDICATOR_ADD_CALC_TYPE
                indicator_types_ = []
                if not indicators.empty:
                    indicator_df = indicators[indicators["CONTEST_CODE"] == actual_code]
                    if not indicator_df.empty:
                        indicator_types_ = indicator_df["INDICATOR_ADD_CALC_TYPE"].fillna("").unique().tolist()
                
                tourns = tourns if len(tourns) else ["-"]
                rewards_ = rewards_ if len(rewards_) else ["-"]
                group_values_ = group_values_ if len(group_values_) else ["-"]
                indicator_types_ = indicator_types_ if len(indicator_types_) else [""]
                
                if is_debug:
                    logging.debug(f"[GROUP] Будет создано комбинаций: {len(tourns)} x {len(rewards_)} x {len(group_values_)} x {len(indicator_types_)} = {len(tourns) * len(rewards_) * len(group_values_) * len(indicator_types_)}")
                
                for t in tourns:
                    for r in rewards_:
                        for gv in group_values_:
                            for ind_type in indicator_types_:
                                ind_code = _indicator_code_for_contest_type(indicators, actual_code, ind_type)
                                all_rows.append((actual_code, str(t), str(r), str(g_code), str(gv), ind_code, str(ind_type)))
                                if is_debug:
                                    logging.debug(f"[GROUP] Создана строка: CONTEST={actual_code}, TOURNAMENT={t}, REWARD={r}, GROUP_CODE={g_code}, GROUP_VALUE={gv}, INDICATOR={ind_type}")

# 5. Для каждого INDICATOR_ADD_CALC_TYPE (даже если нет CONTEST_CODE)
    if not indicators.empty:
        for _, ind_row in indicators.iterrows():
            code = ind_row.get("CONTEST_CODE", "")
            ind_type = ind_row.get("INDICATOR_ADD_CALC_TYPE", "")
            ind_code = ind_row.get("INDICATOR_CODE", "")
            if pd.isna(code):
                code = "-"
            if pd.isna(ind_type):
                ind_type = ""
            if pd.isna(ind_code):
                ind_code = ""
            
            code = str(code)
            ind_type = str(ind_type)
            ind_code = str(ind_code)

            if code != "-" and not tournaments.empty:
                tourns = tournaments[tournaments["CONTEST_CODE"] == code]["TOURNAMENT_CODE"].dropna().unique()
            else:
                tourns = []
            
            if code != "-" and not rewards.empty:
                rewards_ = rewards[rewards["CONTEST_CODE"] == code]["REWARD_CODE"].dropna().unique()
            else:
                rewards_ = []
            
            if code != "-" and not groups.empty:
                groups_df = groups[groups["CONTEST_CODE"] == code]
                # ИСПРАВЛЕНИЕ: Используем пары (GROUP_CODE, GROUP_VALUE)
                group_code_value_pairs = []
                for _, row in groups_df.iterrows():
                    g_code = row.get("GROUP_CODE", "")
                    g_value = row.get("GROUP_VALUE", "")
                    if pd.notna(g_code) and pd.notna(g_value):
                        pair = (str(g_code), str(g_value))
                        if pair not in group_code_value_pairs:
                            group_code_value_pairs.append(pair)
            else:
                group_code_value_pairs = []
            
            if not group_code_value_pairs:
                group_code_value_pairs = [("-", "-")]
            
            tourns = tourns if len(tourns) else ["-"]
            rewards_ = rewards_ if len(rewards_) else ["-"]
            
            for t in tourns:
                for r in rewards_:
                    for g_code, g_value in group_code_value_pairs:
                        all_rows.append((code, str(t), str(r), str(g_code), str(g_value), ind_code, ind_type))

    # Удалить дубли и отбросить строку-заглушку (все ключи "-" и пустые индикаторы)
    _placeholder_row = ("-", "-", "-", "-", "-", "", "")
    all_rows_filtered = [r for r in all_rows if r != _placeholder_row]

    # ОПТИМИЗАЦИЯ v5.0: Гарантируем, что всегда возвращаем DataFrame
    if len(all_rows_filtered) == 0:
        # Если нет данных, создаем пустой DataFrame с правильными колонками
        summary_keys = pd.DataFrame(columns=SUMMARY_KEY_COLUMNS)
    else:
        summary_keys = pd.DataFrame(all_rows_filtered, columns=SUMMARY_KEY_COLUMNS).drop_duplicates().reset_index(drop=True)
    
    # Детальное логирование для отладки
    for debug_code in DEBUG_CODES:
        debug_rows = summary_keys[summary_keys["CONTEST_CODE"] == debug_code]
        if not debug_rows.empty:
            logging.debug(f"[GROUP] === ИТОГОВЫЕ СТРОКИ В SUMMARY для CONTEST_CODE: {debug_code} ===")
            logging.debug(f"[GROUP] Всего строк: {len(debug_rows)}")
            logging.debug(f"[GROUP] Уникальные GROUP_CODE: {debug_rows['GROUP_CODE'].unique().tolist()}")
            logging.debug(f"[GROUP] Уникальные GROUP_VALUE: {debug_rows['GROUP_VALUE'].unique().tolist()}")
            logging.debug("[GROUP] Комбинации (GROUP_CODE, GROUP_VALUE):")
            for _, row in debug_rows.iterrows():
                logging.debug(f"[GROUP]   GROUP_CODE={row['GROUP_CODE']}, GROUP_VALUE={row['GROUP_VALUE']}")
    
    
    # ОПТИМИЗАЦИЯ v5.0: Финальная проверка - гарантируем возврат DataFrame
    if summary_keys is None or not isinstance(summary_keys, pd.DataFrame):
        logging.warning("[collect_summary_keys] summary_keys равен None или не DataFrame, создаем пустой DataFrame")
        summary_keys = pd.DataFrame(columns=SUMMARY_KEY_COLUMNS)
    
    return summary_keys
