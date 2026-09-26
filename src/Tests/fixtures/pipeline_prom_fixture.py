# -*- coding: utf-8 -*-
"""
Синтетический входной набор блока PROM для эталонного теста всего пайплайна (TEST-01).

Все значения выдуманы (коды CONTEST_T01, t_T01_1, r_T01_1…; ФИО — условные), совпадают
только имена полей — они взяты из реальных выгрузок. Данные связаны по ключам так, чтобы
работали merge, SUMMARY, AUTO_GENDER и проверки консистентности, и содержат намеренные
особые случаи:
  * дубли ключа с РАЗНЫМИ значениями (BUG-02): у CONTEST_T01 и CONTEST_T02 по два турнира
    с разными TOURNAMENT_STATUS/TARGET_TYPE; у CONTEST_T02 два INDICATOR с одним ключом;
    сотрудник 00900001 встречается в EMPLOYEE дважды; r_T02_1 привязана к двум конкурсам;
  * строка GROUP с лишним полем (расхождение числа колонок CSV);
  * сотрудники с пустым отчеством и замаскированной фамилией (AUTO_GENDER «-», INFO-01);
  * «сироты»: турнир неизвестного конкурса, награда без связи, отчёт по неизвестному турниру.
Даты — либо до 2021 года, либо после 2098-го: расчётный статус турнира не зависит от дня запуска.

Файлы пишутся под именами «<лист>.csv» в подкаталоги из config (PROM/SPOD, PROM/FILE).
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Sequence, Tuple

# Заголовки входных CSV (только имена полей, без данных). Сгенерировано из реальных выгрузок.
HEADERS = {
    'LIST_REWARDS': ['Уникальный идентификатор записи', 'Код награды', 'Табельный номер сотрудника', 'Код турнира', 'Комментарий', 'Дата создания', 'Дата обновления', 'Дата начала действия награды', 'Дата окончания действия награды', 'Версия записи'],
    'STATISTICS': ['Табельный номер', 'Фамилия', 'Имя', 'Код роли', 'Наименование Роли', 'Текущая роль', 'Дата вступления в роль', 'Дата смены роли', 'ТБ', 'ГОСБ', 'Почта Альфа', 'Почта Сигма', 'Статус текущий', 'Статус предыдущий', 'Динамика статуса', 'Август 2025 входов', 'Сентябрь 2025 входов', 'Октябрь 2025 входов', 'Ноябрь 2025 входов', 'Декабрь 2025 входов', 'Январь 2026 входов', 'Февраль 2026 входов', 'Март 2026 входов', 'Апрель 2026 входов', 'Май 2026 входов', 'Июнь 2026 входов', 'Июль 2026 входов', 'Август 2026 входов', 'Август 2025 дней', 'Сентябрь 2025 дней', 'Октябрь 2025 дней', 'Ноябрь 2025 дней', 'Декабрь 2025 дней', 'Январь 2026 дней', 'Февраль 2026 дней', 'Март 2026 дней', 'Апрель 2026 дней', 'Май 2026 дней', 'Июнь 2026 дней', 'Июль 2026 дней', 'Август 2026 дней', '29.06.2026 - 05.07.2026', '06.07.2026 - 12.07.2026', '13.07.2026 - 19.07.2026', '20.07.2026 - 26.07.2026', '27.07.2026 - 02.08.2026', '25.05.2026 - 07.06.2026', '08.06.2026 - 21.06.2026', '22.06.2026 - 05.07.2026', '06.07.2026 - 19.07.2026', '20.07.2026 - 02.08.2026'],
    'LIST_TOURNAMENT': ['Код конкурса', 'Код турнира', 'Тип периодичности турнира', 'Бизнес-статус турнира', 'Дата обновления турнира', 'Дата создания турнира', 'Дата подведения итога по турниру', 'Дата начала действия турнира', 'Дата окончания действия турнира', 'Дата выполнения расчета результатов по турниру', 'Дата, после которой можно закрывать турнир и подводить итоги', 'Дата обновления данных источника', 'Версия записи данных по турниру', 'Дополнительные сведения о турнире'],
    'YEAR_STATA': ['Уникальный идентификатор записи', 'Табельный номер', 'ТБ', 'ГОСБ', 'Общее количество разыгранных наград (badgeAmount)', 'Количество разыгранных золотых наград (amountGold)', 'Количество разыгранных серебряных наград (amountSilver)', 'Количество разыгранных бронзовых наград (amountBronze)', 'Количество турниров, в которых участвовал сотрудник (amountTournament)', 'Количество полученных золотых наград (amountEarnedGold)', 'Количество полученных серебряных наград (amountEarnedSilver)', 'Количество полученных бронзовых наград (amountEarnedBronze)', 'Общее количество полученных турнирных наград', 'Процентиль полученных турнирных наград (amountEarnedBadgePercentile)', 'Максимальное количество золотых наград (maxAmountEarnedGold)', 'Максимальное количество серебряных наград (maxAmountEarnedSilver)', 'Максимальное количество бронзовых наград (maxAmountEarnedBronze)', 'Максимальное количество золотых наград ТБ (maxAmountEarnedGoldTB)', 'Максимальное количество серебряных наград ТБ (maxAmountEarnedSilverTB)', 'Максимальное количество бронзовых наград ТБ (maxAmountEarnedBronzeTB)', 'Количество полученных бейджей (earnedIndividualBadges)', 'Бейдж по Индии (badgeIndiaEarned)', 'Количество менеджеров с бейджем по Индии (managersEarnedIndia)', 'Число клиентов валютных ПФИ (valPfiCount)', 'Число клиентов ПФИ CAPMAN (procPfiCount)', 'Наличие бейджа по цифровой трансформации (earnedDTASS)', 'Количество клиентов, привлеченных на пакет услуг (packageClients)', 'Наличие бейджа "Одним сердцем" (earnedHeart)', 'Рост портфеля пассивов (deltaPassive)', 'Процентиль роста портфеля пассивов (deltaPassivePercent)', 'Процентиль роста портфеля пассивов ТБ (deltaPassivePercentTB)', 'Количество клиентов с депозитами (depositsClients)', 'Количество депозитов размещено (depositsAmount)', 'Максимальная сумма депозита (maxSumDeposit)', 'Количество сотрудников с большей или равной максимальной суммой депозита (managersMaxSumDeposit)', 'Рост портфеля активов (deltaActive)', 'Процентиль роста портфеля активов (deltaActivePercent)', 'Процентиль роста портфеля активов ТБ (deltaActivePercentTB)', 'Количество клиентов, по которым были сделки (creditClients)', 'Количество кредитных сделок (creditAmount)', 'Максимальная сумма кредита, млрд (maxSumCreditMLRD)', 'Максимальная сумма кредита, млн (maxSumCreditMLN)', 'Количество сотрудников с большей или равной максимальной суммой кредита (managersMaxSumCredit)', 'Количество новых заемщиков (newCreditClients)', 'Количество реализованных сделок ККП (kkpDoneAmount)', 'Количество реализованных сделок ККП с кросс-продажами (kkpDoneCrossAmount)', 'Отношение сделок ККП к сделкам с кросс-продажами (shareKkpDoneCrossAmount)', 'Прирост ОД (ODsum)', 'Процентиль ОД (ODpercentileBANK)', 'Процентиль ОД ТБ (ODpercentileTB)', 'Темп прироста ОД (ODspeed)', 'Процентиль темпа прироста ОД (ODspeedPercentileBANK)', 'Процентиль темпа прироста ОД ТБ (ODspeedPercentileTB)', 'Динамика уровня доверия (trustDynamic)', 'Процентиль по динамике уровня доверия (trustPercentile)', 'Количество клиентов, которые выросли до максимального уровня доверия (trustClients)', 'Количество кристаллов, заработанных за год (crystals)', 'Рейтинг в начале года (placeInRatingToBe)', 'Текущий рейтинг (placeInRatingNow)', 'Лучший месяц (bestMonth)', 'Эксперт по развитию бизнеса (expertBusiness)', 'Отраслевой эксперт (expertIndustry)', 'Количество коллег, от которых были получены реакции', 'Количество коллег топ-менеджеров, от которых были получены реакции', 'Количество коллег, которым были поставлены реакции', 'Признак просмотра', 'Дата создания записи', 'Дата обновления записи', 'Кем создана запись', 'Версия записи'],
    'ORDER': ['Уникальный идентификатор транзакции', 'Табельный номер', 'Имя', 'Фамилия', 'Наименование роли', 'Email в домене Sigma', 'Email в домене Alpha', 'ТБ', 'Короткое наименование ТБ', 'ГОСБ', 'Наименование ГОСБ', 'Сезон', 'Код товара', 'Наименование товара', 'Особенности транзакции', 'Дата создания заказа', 'Статус заказа', 'Награды, признак выполнения условий', 'Место в рейтинге за сезон, признак выполнения условий', 'Количество кристаллов за сезон, признак выполнения условий', 'Оставшееся количество товара, доступного участнику за сезон'],
    'RATING': ['Табельный номер', 'Имя', 'Фамилия', 'Наименование Роли', 'ТБ', 'Короткое наименование ТБ', 'ГОСБ', 'Наименование ГОСБ', 'Период', 'Количество кристаллов', 'Место в рейтинге по стране', 'Место в рейтинге ТБ', 'Место в рейтинге ГОСБ'],
    'CONTEST': ['CONTEST_CODE', 'FULL_NAME', 'CREATE_DT', 'CLOSE_DT', 'BUSINESS_STATUS', 'CONTEST_TYPE', 'CONTEST_DESCRIPTION', 'CONTEST_FEATURE', 'SHOW_INDICATOR', 'PRODUCT_GROUP', 'PRODUCT', 'CONTEST_SUBJECT', 'FACTOR_MARK_TYPE', 'CONTEST_INDICATOR_METHOD', 'CONTEST_FACTOR_METHOD', 'PLAN_METHOD_CODE', 'PLAN_MOD_METOD', 'PLAN_MOD_VALUE', 'FACTOR_MATCH', 'CONTEST_PERIOD', 'TARGET_TYPE', 'SOURCE_UPD_FREQUENCY', 'CALC_TYPE', 'BUSINESS_BLOCK', 'FACT_POST_PROCESSING'],
    'GROUP': ['CONTEST_CODE', 'GROUP_CODE', 'GROUP_VALUE', 'GET_CALC_METHOD', 'GET_CALC_CRITERION', 'ADD_CALC_CRITERION', 'ADD_CALC_CRITERION_2', 'BASE_CALC_CODE'],
    'INDICATOR': ['CONTEST_CODE', 'INDICATOR_CALC_TYPE', 'INDICATOR_ADD_CALC_TYPE', 'FULL_NAME', 'INDICATOR_CODE', 'INDICATOR_AGG_FUNCTION', 'INDICATOR_WEIGHT', 'INDICATOR_OBJECT', 'INDICATOR_MARK_TYPE', 'INDICATOR_MATCH', 'INDICATOR_VALUE', 'CONTEST_CRITERION', 'INDICATOR_FILTER', 'CONTESTANT_SELECTION', 'CALC_TYPE', 'N'],
    'REPORT': ['MANAGER_PERSON_NUMBER', 'CONTEST_CODE', 'TOURNAMENT_CODE', 'CONTEST_DATE', 'PLAN_VALUE', 'FACT_VALUE', 'priority_type'],
    'REWARD': ['REWARD_CODE', 'REWARD_TYPE', 'FULL_NAME', 'REWARD_DESCRIPTION', 'REWARD_CONDITION', 'REWARD_COST', 'REWARD_ADD_DATA'],
    'REWARD_LINK': ['CONTEST_CODE', 'GROUP_CODE', 'REWARD_CODE'],
    'SCHEDULE': ['TOURNAMENT_CODE', 'PERIOD_TYPE', 'START_DT', 'END_DT', 'RESULT_DT', 'PLAN_PERIOD_START_DT', 'PLAN_PERIOD_END_DT', 'CRITERION_MARK_TYPE', 'CRITERION_MARK_VALUE', 'FILTER_PERIOD_ARR', 'TOURNAMENT_STATUS', 'CONTEST_CODE', 'TARGET_TYPE', 'CALC_TYPE', 'TRN_INDICATOR_FILTER'],
    'ORG_UNIT': ['TB_CODE', 'TB_FULL_NAME', 'TB_SHORT_NAME', 'GOSB_CODE', 'GOSB_NAME', 'GOSB_SHORT_NAME', 'CLUSTER_CODE', 'GROUPING_CODE', 'GOSB_CNT', 'GROUPING_CNT', 'ORG_UNIT_CODE'],
    'USER_ROLE': ['RULE_NUM', 'ROLE_CODE', 'ROLE_NAME', 'PERSON_NUMBER_ARR', 'STAGE_ETALONE_CODE_ARR', 'POST_ETALONE_CODE_ARR', 'DIV_CODE_ARR', 'EXCLUDE_DIV_CODE_ARR', 'BUSINESS_BLOCK', 'UCH_CODE', 'ORG_UNIT_CODE', 'TB_CODE', 'GOSB_CODE'],
    'EMPLOYEE': ['PERSON_NUMBER', 'PERSON_NUMBER_ADD', 'SURNAME', 'FIRST_NAME', 'MIDDLE_NAME', 'MANAGER_FULL_NAME', 'POSITION_NAME', 'TB_CODE', 'GOSB_CODE', 'BUSINESS_BLOCK', 'PRIORITY_TYPE', 'KPK_CODE', 'KPK_NAME', 'ROLE_CODE', 'UCH_CODE', 'GENDER', 'ORG_UNIT_CODE'],
}


def _spod_json(obj: Any) -> str:
    """JSON в поле SPOD-выгрузки: каждая кавычка утроена (так приходят реальные файлы)."""
    return json.dumps(obj, ensure_ascii=False).replace('"', '"""')


def _tab8(n: int) -> str:
    """Табельный номер в файлах FILE (8 знаков)."""
    return f"{90000 + n:08d}"


def _tab20(n: int) -> str:
    """Табельный номер в SPOD-выгрузках (20 знаков)."""
    return f"{90000 + n:020d}"


# Сотрудники: (номер, фамилия, имя, отчество, ТБ, ГОСБ, ORG_UNIT_CODE, роль)
_PEOPLE: List[Tuple[int, str, str, str, str, str, str, str]] = [
    (1, "Иванов", "Иван", "Иванович", "90", "0", "10090000", "KM_KKSB"),
    (2, "Петрова", "Анна", "Сергеевна", "90", "9001", "10090001", "KM_KKSB"),
    (3, "Сидоров", "Олег", "", "90", "9001", "10090001", "MNS"),
    (4, "Кузнецова", "Мария", "Павловна", "91", "0", "10091000", "KM_KKSB"),
    (5, "Ким", "Алекс", "", "91", "9101", "10091001", "MNS"),
    (6, "…", "Тест сб", "", "91", "9101", "10091001", "NOT_USED"),
    (7, "Смирнов", "Пётр", "Андреевич", "90", "0", "10090000", "CSM"),
    (8, "Орлова", "Елена", "", "91", "0", "10091000", "CSM"),
]

_ORG_UNITS = [
    # TB_CODE, TB_FULL_NAME, TB_SHORT_NAME, GOSB_CODE, GOSB_NAME, GOSB_SHORT_NAME, CLUSTER, GROUPING, GOSB_CNT, GROUPING_CNT, ORG_UNIT_CODE
    ("90", "Тестовый банк Север", "ТБС", "0", "Аппарат территориального банка", "Аппарат ТБС", "0", "1", "0", "0", "10090000"),
    ("90", "Тестовый банк Север", "ТБС", "9001", "Отделение Север-1", "ОС-1", "1", "1", "3", "3", "10090001"),
    ("91", "Тестовый банк Юг", "ТБЮ", "0", "Аппарат территориального банка", "Аппарат ТБЮ", "0", "1", "0", "0", "10091000"),
    ("91", "Тестовый банк Юг", "ТБЮ", "9101", "Отделение Юг-1", "ОЮ-1", "2", "1", "2", "2", "10091001"),
]
_TB_SHORT = {"90": "ТБС", "91": "ТБЮ"}
_GOSB_NAME = {"0": "Аппарат территориального банка", "9001": "Отделение Север-1", "9101": "Отделение Юг-1"}


def _contest_feature(vid: str, blocks: Sequence[str], **extra: Any) -> Dict[str, Any]:
    feature: Dict[str, Any] = {
        "feature": [], "vid": vid, "momentRewarding": "AFTER", "minNumber": 1, "capacity": "",
        "accuracy": 0, "masking": "N", "tournamentStartMailing": "N", "tournamentEndMailing": "N",
        "tournamentLikeMailing": "Y", "tournamentRewardingMailing": "N", "tournamentListMailing": [],
        "persomanNumberVisible": [_tab8(1)], "persomanNumberHidden": [], "gosbVisible": [],
        "gosbHidden": [], "tbVisible": [], "tbHidden": [], "businessBlock": list(blocks),
        "typeRewarding": "all",
    }
    feature.update(extra)
    return feature


def _contests() -> List[List[str]]:
    rows = []
    specs = [
        ("CONTEST_T01", "Тестовый конкурс продаж", "АКТИВНЫЙ", "ИНДИВИДУАЛЬНЫЙ",
         _contest_feature("ТЕСТ", ["KMKKSB"], feature=["Продажи", "Качество"])),
        ("CONTEST_T02", "Тестовый командный конкурс", "АКТИВНЫЙ", "КОМАНДНЫЙ",
         _contest_feature("ПРОДАЖИ", ["MNS"], momentRewarding="BEFORE", tournamentTeam="Y")),
        ("CONTEST_T03", "Архивный конкурс", "АРХИВНЫЙ", "ИНДИВИДУАЛЬНЫЙ",
         _contest_feature("ТЕСТ", ["KMKKSB", "MNS"], preferences=["one", "two"])),
        ("CONTEST_T04", "Конкурс с апострофом O'Neil", "АКТИВНЫЙ", "ИНДИВИДУАЛЬНЫЙ",
         _contest_feature("ТЕСТ", ["CSM"], helpCodeList=["HELP_1"], feature=["Клиент O'Neil"])),
        ("CONTEST_T05", "Конкурс без турниров", "ЗАПЛАНИРОВАН", "ИНДИВИДУАЛЬНЫЙ", None),
    ]
    for i, (code, name, status, ctype, feature) in enumerate(specs, start=1):
        rows.append([
            code, name, "2020-01-01", "4000-01-01", status, ctype, f"Описание {name}",
            _spod_json(feature) if feature is not None else "",
            "шт.", "Системные", f"Продукт {i}", "EMPLOYEE", "CRITERION", "INTEGRAL", "FACT",
            "PRESET_VALUE", "", str(i), "=", "[]", "ТЕСТ" if i % 2 else "PROM", "1", str(i % 3),
            _spod_json(feature["businessBlock"] if feature else []), "",
        ])
    return rows


def _groups() -> List[List[str]]:
    rows = []
    for code in ("CONTEST_T01", "CONTEST_T02", "CONTEST_T03", "CONTEST_T04", "CONTEST_T05"):
        rows.append([code, "BANK", "*", "2", "1", "0", "0", "BANK"])
    rows.append(["CONTEST_T01", "TB", "90", "2", "1", "0", "0", "TB"])
    rows.append(["CONTEST_T01", "TB", "91", "2", "1", "0", "0", "TB"])
    # Лишнее поле: расхождение числа колонок (хвост склеивается в последнюю колонку)
    rows.append(["CONTEST_T02", "GOSB", "9001", "2", "1", "0", "0", "GOSB", "ЛИШНЕЕ"])
    return rows


def _indicators() -> List[List[str]]:
    base = ["1", "", "Показатель", "IND_1", "SUM", "1", "", "RATING", "MIN", "0", "", "", "", "1", "4"]
    rows = []
    for code, calc_add, ind_code, weight, value, n in (
        ("CONTEST_T01", "", "IND_T01_A", "1", "0", "4"),
        ("CONTEST_T01", "ADD1", "IND_T01_B", "0.5", "10", "5"),
        # Один ключ (CONTEST_CODE, INDICATOR_ADD_CALC_TYPE) — разные значения (BUG-02)
        ("CONTEST_T02", "", "IND_T02_A", "1", "0", "4"),
        ("CONTEST_T02", "", "IND_T02_B", "2", "100", "7"),
        ("CONTEST_T04", "", "IND_T04_A", "1", "0", "3"),
    ):
        row = list(base)
        # индексы — без CONTEST_CODE: 1 ADD_CALC_TYPE, 2 FULL_NAME, 3 CODE, 5 WEIGHT, 9 VALUE, 14 N
        row[1] = calc_add
        row[2] = f"Показатель {ind_code}"
        row[3] = ind_code
        row[5] = weight
        row[9] = value
        row[14] = n
        rows.append([code] + row)
    return rows


# Турниры: код, конкурс, тип, START, END, RESULT, TOURNAMENT_STATUS, сезон (TARGET_TYPE)
_TOURNAMENTS = [
    ("t_T01_1", "CONTEST_T01", "турнир года", "2020-01-01", "2020-12-31", "2021-01-15", "ЗАВЕРШЕН", "SEASON_T_2020"),
    ("t_T01_2", "CONTEST_T01", "месяц", "2020-01-01", "2099-12-31", "2100-01-31", "АКТИВНЫЙ", "SEASON_T_2020"),
    ("t_T02_1", "CONTEST_T02", "квартал", "2098-01-01", "2098-12-31", "2099-01-31", "ЗАПЛАНИРОВАН", "SEASON_T_2098"),
    ("t_T02_2", "CONTEST_T02", "квартал", "2019-01-01", "2019-12-31", "2099-01-01", "ПОДВЕДЕНИЕ ИТОГОВ", "SEASON_T_2019"),
    ("t_T03_1", "CONTEST_T03", "турнир года", "2019-01-01", "2019-06-30", "2019-12-31", "ПОДВЕДЕНИЕ ИТОГОВ", "SEASON_T_2019"),
    ("t_T03_2", "CONTEST_T03", "месяц", "", "2019-06-30", "", "НЕОПРЕДЕЛЕН", "SEASON_T_2019"),
    ("t_T04_1", "CONTEST_T04", "месяц", "2020-01-01", "2020-12-31", "2021-03-01", "ПОДВЕДЕНИЕ ИТОГОВ", "SEASON_T_2020"),
    ("t_T09_1", "CONTEST_T09", "месяц", "2020-01-01", "2020-12-31", "2021-01-31", "ЗАВЕРШЕН", "SEASON_T_2020"),
]


def _schedule() -> List[List[str]]:
    rows = []
    for code, contest, period, start, end, result, status, season in _TOURNAMENTS:
        rows.append([
            code, period, start, end, result, "", "", "", "", "", status, contest,
            _spod_json({"seasonCode": season}), "1", "",
        ])
    return rows


def _reward_add_data(code: str, rtype: str, blocks: Sequence[str], **extra: Any) -> str:
    data: Dict[str, Any] = {
        "feature": [f"Условие {code}"], "itemFeature": [], "nftFlg": "N", "outstanding": "N",
        "rewardRule": "Правило вручения", "rewardAgainGlobal": "N", "rewardAgainTournament": "N",
        "hidden": "N", "hiddenRewardList": "N", "persomanNumberVisible": [_tab8(1)], "fileName": "",
        "teamNews": "", "singleNews": "", "refreshOldNews": "N", "businessBlock": list(blocks),
        "newsType": "GENERAL", "winCriterion": "1", "priority": "1",
    }
    if rtype == "ITEM":
        data["getCondition"] = {
            "rewards": [{"rewardCode": "r_T01_1", "amount": "1"}],
            "nonRewards": [{"nonRewardCode": "ITEM_T_02"}],
            "employeeRating": {"minRatingBANK": "", "minRatingTB": "", "minRatingGOSB": "3",
                               "minCrystalEarnedTotal": "", "seasonCode": "SEASON_T_2020"},
        }
        data["seasonItem"] = "Y"
    data.update(extra)
    return _spod_json(data)


def _rewards() -> List[List[str]]:
    return [
        ["r_T01_1", "BADGE", "Золотой знак", "Первое место в турнире", "1", "0",
         _reward_add_data("r_T01_1", "BADGE", ["KMKKSB"], masterBadge="Y")],
        ["r_T01_2", "BADGE", "Серебряный знак", "Второе место", "1", "0",
         _reward_add_data("r_T01_2", "BADGE", ["KMKKSB"], parentRewardCode="r_T01_1")],
        ["r_T02_1", "CRYSTAL", "Кристаллы", "Кристаллы за участие", "1", "50",
         _reward_add_data("r_T02_1", "CRYSTAL", ["MNS"])],
        ["ITEM_T_01", "ITEM", "Сертификат", "Сертификат по итогам сезона. \\\\nПодробности у организаторов.", "1", "300",
         _reward_add_data("ITEM_T_01", "ITEM", ["MNS"], recommendationLevel="2")],
        ["ITEM_T_02", "ITEM", "Поездка", "Поездка для победителей", "1", "1000",
         _reward_add_data("ITEM_T_02", "ITEM", ["CSM"], helpCodeList=["HELP_1"])],
        ["r_T09_orphan", "BADGE", "Знак без связи", "Нет в REWARD-LINK", "1", "0",
         _reward_add_data("r_T09_orphan", "BADGE", ["KMKKSB"])],
    ]


def _reward_links() -> List[List[str]]:
    return [
        ["CONTEST_T01", "BANK", "r_T01_1"],
        ["CONTEST_T01", "BANK", "r_T01_2"],
        ["CONTEST_T01", "TB", "r_T01_1"],
        ["CONTEST_T02", "BANK", "r_T02_1"],
        ["CONTEST_T03", "BANK", "r_T02_1"],
        ["CONTEST_T02", "BANK", "ITEM_T_01"],
        ["CONTEST_T04", "BANK", "ITEM_T_02"],
    ]


def _reports() -> List[List[str]]:
    dates = {"t_T01_1": "2021-02-01", "t_T01_2": "2020-06-30", "t_T02_2": "2019-12-31", "t_T04_1": "2020-06-30"}
    contest_of = {t[0]: t[1] for t in _TOURNAMENTS}
    rows = []
    for n in range(1, 7):
        for t_code, dt in dates.items():
            if (n + len(t_code)) % 4 == 0:
                continue
            rows.append([_tab20(n), contest_of[t_code], t_code, dt, "10000000.00000",
                         f"{n * 1234567.891:.5f}", str(1 + n % 2)])
    rows.append([_tab20(99), "CONTEST_T01", "t_T99_unknown", "2020-06-30", "0.00000", "0.00000", "1"])
    return rows


def _employees() -> List[List[str]]:
    rows = []
    for n, sur, first, mid, tb, gosb, ou, role in _PEOPLE:
        fio = " ".join(x for x in (sur, first, mid) if x)
        rows.append([_tab20(n), _tab20(n), sur, first, mid, fio, "Менеджер", tb, gosb, "KMKKSB",
                     "1", "", "", role, "1", "1" if n % 2 else "2", ou])
    # Дубль табельного номера с другой должностью (BUG-02 для EMPLOYEE → REPORT)
    rows.append([_tab20(1), _tab20(1), "Иванов", "Иван", "Иванович", "Иванов Иван Иванович",
                 "Старший менеджер", "90", "0", "KMKKSB", "1", "", "", "KM_KKSB", "1", "1", "10090000"])
    return rows


def _user_roles(prefix: str) -> List[List[str]]:
    return [
        [f"{prefix}01", "KM_KKSB", "Клиентский менеджер (ТБС)", "", "", "[20000001, 20000002]", "[10090000]", "",
         "KMKKSB", "1", "10090000", "", ""],
        [f"{prefix}02", "MNS", "Менеджер сервисов (ТБЮ)", f"[{_tab8(3)}]", "", "[]", "[10091001]", "",
         "MNS", "1", "10091001", "", ""],
        [f"{prefix}03", "CSM", "Менеджер (неизвестное подразделение)", "", "", "[]", "[10099999]", "",
         "CSM", "0", "10099999", "", ""],
    ]


def _list_rewards() -> List[List[str]]:
    rows = []
    items = [("r_T01_1", 1, "t_T01_1"), ("r_T01_2", 2, "t_T01_1"), ("r_T01_1", 3, "t_T01_2"),
             ("r_T02_1", 3, "t_T02_2"), ("r_T02_1", 5, "t_T02_2"), ("ITEM_T_01", 5, "t_T02_2"),
             ("ITEM_T_02", 7, "t_T04_1"), ("r_T01_1", 1, "t_T01_2"), ("r_T09_orphan", 8, "t_T09_1")]
    for i, (reward, n, t_code) in enumerate(items, start=1):
        rows.append([str(700000000000000000 + i), reward, _tab8(n), t_code, "null",
                     "2021-01-20T03:00:56.122Z", "2021-01-20T03:00:56.122Z", "2021-01-20T00:00Z", "null", "0"])
    return rows


def _statistics() -> List[List[str]]:
    rows = []
    statuses = ["Активный", "Слабоактивный", "Неактивный"]
    for n, sur, first, _mid, tb, gosb, _ou, role in _PEOPLE:
        head = [_tab8(n), sur, first, role, f"Роль {role}", "true", "2020-04-01", "null", tb, gosb,
                f"user{n}@alpha.example", f"user{n}@sigma.example", statuses[n % 3], statuses[(n + 1) % 3],
                "Без изменений" if n % 2 else "Рост"]
        rest = [str((n * (k + 3)) % 17) for k in range(len(HEADERS["STATISTICS"]) - len(head))]
        rows.append(head + rest)
    return rows


def _list_tournament() -> List[List[str]]:
    biz = {"ЗАВЕРШЕН": "Завершен", "АКТИВНЫЙ": "Активный", "ЗАПЛАНИРОВАН": "Запланирован",
           "ПОДВЕДЕНИЕ ИТОГОВ": "Подведение итогов", "НЕОПРЕДЕЛЕН": "Удален"}
    rows = []
    for i, (code, contest, period, start, end, result, status, season) in enumerate(_TOURNAMENTS[:-1], start=1):
        rows.append([contest, code, period, biz[status], "2020-08-03", "2019-10-21", result or "null",
                     start or "null", end, "2020-10-29", "null" if i % 2 else "2021-02-01", "null",
                     str(5000 + i), json.dumps({"seasonCode": season})])
    return rows


def _rating(sheet: str, seed: int) -> List[List[str]]:
    rows = []
    for place, (n, sur, first, _mid, tb, gosb, _ou, role) in enumerate(_PEOPLE[seed % 3:seed % 3 + 4], start=1):
        rows.append([_tab8(n), first, sur, f"Роль {role}", tb, _TB_SHORT[tb], gosb, _GOSB_NAME[gosb],
                     sheet, str(100 - place * 7 - seed), str(place), str(place), str(1 + place % 2)])
    return rows


def _order(sheet: str, seed: int) -> List[List[str]]:
    rows = []
    goods = [("ITEM_T_01", "Сертификат"), ("ITEM_T_02", "Поездка")]
    for k, (n, sur, first, _mid, tb, gosb, _ou, role) in enumerate(_PEOPLE[seed % 4:seed % 4 + 3]):
        code, name = goods[(k + seed) % 2]
        rows.append([str(800000000000000000 + seed * 10 + k), _tab8(n), first, sur, f"Роль {role}",
                     f"user{n}@sigma.example", f"user{n}@alpha.example", tb, _TB_SHORT[tb], gosb,
                     _GOSB_NAME[gosb], f"Сезон {sheet}", code, name, "null", f"{10 + k}.04.2020",
                     "Новый" if k % 2 else "Выдан", "-", "Да", "-", str(k)])
    return rows


def _year_stata() -> List[List[str]]:
    header = HEADERS["YEAR_STATA"]
    rows = []
    for n, _sur, _first, _mid, tb, gosb, _ou, _role in _PEOPLE[:5]:
        row = []
        for idx, col in enumerate(header):
            if idx == 0:
                row.append(str(789000000000000000 + n))
            elif idx == 1:
                row.append(_tab8(n))
            elif idx == 2:
                row.append(tb)
            elif idx == 3:
                row.append(gosb)
            elif col.startswith(("Наличие", "Эксперт", "Отраслевой", "Признак")):
                row.append("true" if n % 2 else "false")
            elif col.startswith("Бейдж"):
                row.append(f"r_T01_{1 + n % 2}")
            elif col.startswith("Лучший месяц"):
                row.append("Август")
            elif col.startswith("Дата"):
                row.append("2020-12-18 15:00:16.522378+00")
            elif col.startswith("Кем"):
                row.append("TEST_TECH")
            elif "(delta" in col or "Отношение" in col:
                row.append(f"{(n - 3) * 0.37:.2f}")
            else:
                row.append(str((n * (idx + 1)) % 97))
        rows.append(row)
    return rows


def rows_for_sheet(sheet: str) -> Tuple[List[str], List[List[str]], bool]:
    """(заголовок, строки, BOM) для листа из input_files блока PROM."""
    fixed = {
        "CONTEST-DATA": ("CONTEST", _contests, False),
        "GROUP": ("GROUP", _groups, False),
        "INDICATOR": ("INDICATOR", _indicators, False),
        "REPORT": ("REPORT", _reports, False),
        "REWARD": ("REWARD", _rewards, False),
        "REWARD-LINK": ("REWARD_LINK", _reward_links, False),
        "TOURNAMENT-SCHEDULE": ("SCHEDULE", _schedule, False),
        "ORG_UNIT_V20": ("ORG_UNIT", lambda: [list(r) for r in _ORG_UNITS], False),
        "USER_ROLE": ("USER_ROLE", lambda: _user_roles("1"), False),
        "USER_ROLE SB": ("USER_ROLE", lambda: _user_roles("2"), False),
        "EMPLOYEE": ("EMPLOYEE", _employees, False),
        "LIST-REWARDS": ("LIST_REWARDS", _list_rewards, True),
        "STATISTICS": ("STATISTICS", _statistics, True),
        "LIST-TOURNAMENT": ("LIST_TOURNAMENT", _list_tournament, True),
        "YEAR_STATA": ("YEAR_STATA", _year_stata, True),
    }
    if sheet in fixed:
        key, make, bom = fixed[sheet]
        return HEADERS[key], make(), bom
    seed = sum(ord(c) for c in sheet)
    if sheet.startswith("RATING"):
        return HEADERS["RATING"], _rating(sheet, seed), True
    if sheet.startswith("ORDER"):
        return HEADERS["ORDER"], _order(sheet, seed), True
    raise KeyError(
        f"Нет синтетических данных для листа «{sheet}»: добавьте его в "
        f"src/Tests/fixtures/pipeline_prom_fixture.py (rows_for_sheet)"
    )


def fixture_file_name(sheet: str) -> str:
    return f"{sheet}.csv"


def write_fixture_inputs(input_dir: str, input_files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Записать синтетические CSV для всех записей input_files (блок PROM) в input_dir/<subdir>.
    Возвращает копию input_files с подменёнными именами файлов (остальные параметры — как в config).
    """
    out = []
    for entry in input_files:
        sheet = entry["sheet"]
        header, rows, bom = rows_for_sheet(sheet)
        name = fixture_file_name(sheet)
        target_dir = os.path.join(input_dir, entry.get("subdir") or "")
        os.makedirs(target_dir, exist_ok=True)
        with open(os.path.join(target_dir, name), "w", encoding="utf-8-sig" if bom else "utf-8", newline="") as f:
            f.write(";".join(header) + "\n")
            for row in rows:
                f.write(";".join(row) + "\n")
        new_entry = dict(entry)
        new_entry["file"] = name
        out.append(new_entry)
    return out
