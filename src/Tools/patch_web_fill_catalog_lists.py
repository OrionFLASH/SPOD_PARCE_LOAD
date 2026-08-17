# -*- coding: utf-8 -*-
"""Обновить списки/подписи каталога fill (web-edit → дальше sync)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[2]

INDICATOR_CODES: List[str] = [
    "WAIT",
    "PPO_IN",
    "PPO_ALL",
    "PULMIS_BALANCE_OUT_RUB",
    "PULMIS_BALANCE_OUT",
    "PULMIS_SDO_IN_RUB",
    "PULMIS_SDO_IN",
    "PULMIS_INCOME",
    "PULMIS_INCOME_RUB",
    "PULMIS_AGRMNT_AMT_RUB",
    "PULMIS_CUSTOMER_ID",
    "LEAGUE",
    "SUPERCUP",
    "INCOME",
    "PFIMIS_INCOME",
    "PFIMIS_VOLUME",
    "PFIMIS_INCOME_SOFT",
    "PFIMIS_CUSTOMER_ID",
    "PFIMIS_DEAL_CNT",
    "PFIMIS_DEAL_ID",
    "CC360_CLIENT_VOLUM_CHPDP_M",
    "CC360_CLIENT_VOLUM_FOT_M",
    "INSURANCEMIS_AGENT_COMMISION",
    "INSURANCEMIS_BANK_COMMISION",
    "INSURANCEMIS_COMMISION",
    "INSURANCE_AMMOUNT",
    "EFFICIENCYARSKKSB_EFF",
    "EFFICIENCYARSKKSB_OD_YEAR",
    "EFFICIENCYARSKKSB_OD_YEAR_APPG",
    "EFFICIENCYARSKKSB_OD_QUARTER_APPG",
    "EFFICIENCYARSKKSB_OD_YEAR_GROWTH",
    "EFFICIENCYARSKKSB_OD_YEAR_TEMP",
    "EFFICIENCYARSKKSB_OD_QUARTER_GROWTH",
    "EFFICIENCYARSKKSB_OD_QUARTER_TEMP",
    "EFFICIENCYARS_OVERBONUS",
    "EFFICIENCYARS_OVERBONUS_YEAR",
    "EFFICIENCYARS_OVERBONUS_YEAR_APPG",
    "EFFICIENCYARS_OVERBONUS_QUARTER_APPG",
    "EFFICIENCYARS_OVERBONUS_YEAR_GROWTH",
    "EFFICIENCYARS_OVERBONUS_YEAR_TEMP",
    "EFFICIENCYARS_OVERBONUS_QUARTER_GROWTH",
    "EFFICIENCYARS_OVERBONUS_QUARTER_TEMP",
    "TRUSTLEVELCC360_STAR_COUNT",
    "TRUSTLEVELCC360_STAR_START_COUNT",
    "TRUSTLEVELCC360_LEVEL0_COUNT",
    "TRUSTLEVELCC360_LEVEL3_COUNT",
    "TRUSTLEVELCC360_LEVEL4_COUNT",
    "TRUSTLEVELCC360_LEVEL5_COUNT",
    "FUNNELARS_ACTIVE_DEAL_ID",
    "FUNNELARS_ACTIVE_DEAL_MARGIN",
    "FUNNELARS_ACTIVE_DEAL_CHOD",
    "FUNNELARS_ACTIVE_CUSTOMER_ID",
    "COMPASARS_KKP_ID",
    "CC360_NKD_DETAIL_CHKD",
    "CC360_NKD_DETAIL_CHKD_PLAN",
    "KANBANARS_OFFER_VALUE",
    "KANBANARS_STAGE_VALUE",
    "KANBANARS_STAGE_INC",
    "KANBANARS_OFFER_INC",
    "KANBANARS_STAGE_AMOUNT",
    "KANBANARS_DEAL_AMOUNT",
    "KANBANARS_DEAL_NUM",
    "KANBANARS_OFFER_VALUE_VKS",
    "KANBANARS_STAGE_VALUE_VKS",
    "KANBANARS_STAGE_INC_VKS",
    "KANBANARS_OFFER_INC_VKS",
    "KANBANARS_STAGE_AMOUNT_VKS",
    "KANBANARS_DEAL_AMOUNT_VKS",
    "KANBANARS_DEAL_NUM_VKS",
    "KANBANARS_OFFER_VALUE_VKO",
    "KANBANARS_STAGE_VALUE_VKO",
    "KANBANARS_STAGE_INC_VKO",
    "KANBANARS_OFFER_INC_VKO",
    "KANBANARS_STAGE_AMOUNT_VKO",
    "KANBANARS_DEAL_AMOUNT_VKO",
    "KANBANARS_DEAL_NUM_VKO",
    "WD",
]

FIELD_PATCHES: Dict[str, Dict[str, Any]] = {
    "CONTEST_INDICATOR_METHOD": {
        "variant_labels": ["Интегральный", "Отношение агрегатов"],
        "description": "Метод расчета показателя конкурса: интегральный (по умолчанию) / отношение агрегированных значений.",
    },
    "CONTEST_FACTOR_METHOD": {
        "variants": [
            "FACT",
            "FACT0-FACT1",
            "RUN_RATE",
            "RUN_RATE-FACT1",
            "FACT0-RUN_RATE1_DOWN",
            "RUN_RATE/FACT1",
            "FACT0/RUN_RATE1_DOWN",
        ],
        "variant_labels": [
            "Факт",
            "Прирост",
            "Run rate",
            "Run rate прирост",
            "Run rate отклонение",
            "Run rate % прироста",
            "Run rate % отклонения",
        ],
        "description": "Способ расчета показателя. FACT — ручные данные; остальные — автоматические турниры (прирост / run rate).",
    },
    "PLAN_METHOD_CODE": {
        "variants": ["NOT_USED", "PRESET_VALUE", "DEPENDS_PREVIOUS_PERIOD"],
        "variant_labels": ["План не задан", "Предустановленное", "От прошлого периода"],
        "default": "PRESET_VALUE",
        "description": "Как задаётся план: не задан / предустановленное значение (по умолчанию) / зависит от прошлого периода.",
    },
    "PLAN_MOD_METOD": {
        "variants": ["MULTIPLIER", "APPEND"],
        "variant_labels": ["× коэффициент", "+ число к прошлому"],
        "default": "MULTIPLIER",
        "description": "Модификатор плана от прошлого периода: умножить на коэффициент (по умолчанию) или добавить число.",
    },
    "FACT_POST_PROCESSING": {
        "variants": [
            "PERCENTILE",
            "PERCENTILE_DOWN",
            "PERCENTILE_UPEST",
            "PERCENTILE_UP",
            "SPECIAL_INDICATOR_1",
            "COUNT_BIGGER",
        ],
        "variant_labels": [
            "% «лучше чем»",
            "% «попал в»",
            "% «лучше меня»",
            "% «не хуже»",
            "Уровень группы",
            "Счётчик лучших",
        ],
        "default": "",
        "allow_empty": True,
        "description": "Постобработка факта: процентили, уровень группы или число участников с лучшим результатом. Можно не указывать.",
    },
    "INDICATOR_CODE": {
        "kind": "dropdown",
        "variants": INDICATOR_CODES,
        "variant_labels": ["Ручной" if c == "WAIT" else "" for c in INDICATOR_CODES],
        "default": "WAIT",
        "allow_empty": False,
        "description": "Код показателя для расчётов. Только список (свой вариант нельзя). По умолчанию WAIT.",
    },
    "INDICATOR_ADD_CALC_TYPE": {
        "variants": ["NUMERATOR", "DIVIDER"],
        "variant_labels": ["Числитель", "Знаменатель"],
        "allow_empty": True,
    },
    "INDICATOR_AGG_FUNCTION": {
        "variants": [
            "SUM",
            "MAX",
            "MIN",
            "AVG",
            "COUNT",
            "COUNT_DISTINCT",
            "COUNT_DISTINCT_CUSTOMER",
            "COUNT_DISTINCT_DEAL",
            "LAST_VALUE",
        ],
        "variant_labels": [
            "Сумма",
            "Максимум",
            "Минимум",
            "Среднее",
            "Количество",
            "Уник. индикаторы",
            "Уник. клиенты",
            "Уник. договоры",
            "Последнее по дате",
        ],
        "default": "SUM",
        "description": "Функция агрегации показателя.",
    },
}

CATALOGS = [
    ROOT / "common" / "web-edit" / "game_edit_catalog.json",
    ROOT / "common" / "web-edit-full" / "game_edit_catalog.json",
]


def _patch_field(field: Dict[str, Any], patch: Dict[str, Any]) -> None:
    for key, val in patch.items():
        field[key] = val


def patch_catalog(path: Path) -> int:
    data = json.loads(path.read_text(encoding="utf-8"))
    n = 0
    for section in data.get("sections") or []:
        for field in section.get("fields") or []:
            key = str(field.get("key") or "")
            leaf = key.split(".")[-1]
            patch = FIELD_PATCHES.get(key) or FIELD_PATCHES.get(leaf)
            if not patch:
                continue
            _patch_field(field, patch)
            n += 1
    data["exported_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return n


def main() -> int:
    total = 0
    for path in CATALOGS:
        if not path.is_file():
            print(f"Пропуск (нет файла): {path}")
            continue
        n = patch_catalog(path)
        total += n
        print(f"OK {path.relative_to(ROOT)} · полей: {n}")
    print(f"Готово · {total} полей")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
