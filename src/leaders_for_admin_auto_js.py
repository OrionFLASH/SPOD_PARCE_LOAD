# -*- coding: utf-8 -*-
"""
Генерация Tournament_LeadersForAdmin_AutoRun.js рядом с Excel (OUT/YYYY/DD-MM).
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

from src.manager_stats import (
    _get_sheet_df,
    collect_leaders_for_admin_tournament_codes,
    merge_manager_stats_config,
)
from src.config_loader import parse_run_outputs_config

REQUEST_GAP_MS = 5
DEFAULT_JS_FILENAME = "Tournament_LeadersForAdmin_AutoRun.js"


def manager_stats_only_in_run_outputs(full_cfg: Mapping[str, Any]) -> bool:
    """True, если в run_outputs указан токен manager_stats_only."""
    _ro = parse_run_outputs_config(dict(full_cfg))
    return bool(_ro[6])


def get_run_output_dir(base_dir: str) -> str:
    """Подкаталог вывода по дате: base_dir/YYYY/DD-MM (как для Excel в main_impl)."""
    now = datetime.now()
    path = os.path.join(base_dir, now.strftime("%Y"), now.strftime("%d-%m"))
    os.makedirs(path, exist_ok=True)
    return path


def _input_csv_path(full_cfg: Mapping[str, Any], sheet_name: str) -> Path:
    """Путь к CSV листа из input_files."""
    input_root = Path(str((full_cfg.get("paths") or {}).get("input") or "IN"))
    for item in full_cfg.get("input_files") or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("sheet") or "").strip() != sheet_name:
            continue
        subdir = str(item.get("subdir") or "").strip()
        filename = str(item.get("file") or "").strip()
        if not filename:
            continue
        base = input_root / subdir if subdir else input_root
        return base / filename
    raise FileNotFoundError(f"В config.json не найден input_files с sheet={sheet_name!r}")


def _read_csv_dataframe(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, sep=";", encoding="utf-8-sig", dtype=str).fillna("")


def _resolve_schedule_contest_frames(
    *,
    sheets_data: Optional[Mapping[str, Any]],
    full_cfg: Mapping[str, Any],
    catalog_cfg: Mapping[str, Any],
) -> Tuple[pd.DataFrame, pd.DataFrame, str, str]:
    """DataFrame расписания и конкурсов; подписи источников для заголовка JS."""
    schedule_sheet = str(catalog_cfg.get("schedule_sheet") or "TOURNAMENT-SCHEDULE").strip()
    contest_sheet = str(catalog_cfg.get("contest_sheet") or "CONTEST-DATA").strip()

    if sheets_data:
        df_schedule = _get_sheet_df(sheets_data, schedule_sheet)
        df_contest = _get_sheet_df(sheets_data, contest_sheet)
        if df_schedule is not None and df_contest is not None:
            return (
                df_schedule,
                df_contest,
                f"лист {schedule_sheet} (прогон)",
                f"лист {contest_sheet} (прогон)",
            )

    schedule_path = _input_csv_path(full_cfg, schedule_sheet)
    contest_path = _input_csv_path(full_cfg, contest_sheet)
    if not schedule_path.is_file():
        raise FileNotFoundError(f"CSV не найден: {schedule_path}")
    if not contest_path.is_file():
        raise FileNotFoundError(f"CSV не найден: {contest_path}")
    return (
        _read_csv_dataframe(schedule_path),
        _read_csv_dataframe(contest_path),
        schedule_path.name,
        contest_path.name,
    )


JS_TEMPLATE = r"""// =============================================================================
// Tournament_LeadersForAdmin_AutoRun.js — автовыгрузка leadersForAdmin
// =============================================================================
// Сгенерировано: {{GENERATED_AT}}
// Источник: {{SOURCE_SCHEDULE}} (TOURNAMENT-SCHEDULE) + {{SOURCE_CONTEST}} (CONTEST-DATA)
// Фильтр TOURNAMENT_STATUS: {{ACTIVE_STATUSES}}
// Фильтр CONTEST_TYPE (через CONTEST_CODE): {{CONTEST_TYPE_RAW}}
// Кодов в списке: {{CODE_COUNT}}
//
// Использование: DevTools → Console на странице стенда (omega / salesheroes).
// Вставить весь файл и Enter — выгрузка начнётся сразу, JSON скачается в конце.
// Стенд и контур определяются по window.location.origin.
// =============================================================================
(function () {
  "use strict";

  const TOURNAMENT_CODES = {{CODES_JSON}};

  const TOURNAMENT_BASE = {
    PROM: {
      ALPHA: "https://efs-our-business-prom.omega.sbrf.ru/bo/rmkib.gamification/api/v1/tournaments/",
      SIGMA: "https://salesheroes.sberbank.ru/bo/rmkib.gamification/api/v1/tournaments/"
    },
    PSI: {
      ALPHA: "https://iam-enigma-psi.omega.sbrf.ru/bo/rmkib.gamification/api/v1/tournaments/",
      SIGMA: "https://salesheroes-psi.sigma.sbrf.ru/bo/rmkib.gamification/api/v1/tournaments/"
    },
    "IFT-SB": {
      ALPHA: "https://iam-enigma-psi.omega.sbrf.ru/bo/rmkib.gamification/api/v1/tournaments/",
      SIGMA: "https://salesheroes-psi.sigma.sbrf.ru/bo/rmkib.gamification/api/v1/tournaments/"
    },
    "IFT-GF": {
      ALPHA: "https://iam-enigma-psi.omega.sbrf.ru/bo/rmkib.gamification/api/v1/tournaments/",
      SIGMA: "https://salesheroes-psi.sigma.sbrf.ru/bo/rmkib.gamification/api/v1/tournaments/"
    }
  };
  const TOURNAMENT_STAND_KEYS = ["PROM", "PSI", "IFT-SB", "IFT-GF"];
  const TOURNAMENT_CONTOUR_KEYS = ["ALPHA", "SIGMA"];
  const LEADERS_SERVICE = "leadersForAdmin";
  const REQUEST_GAP_MS = {{REQUEST_GAP_MS}};

  function detectTournamentEnvFromLocation() {
    var origin = "";
    try {
      origin = String(window.location.origin || "").toLowerCase();
    } catch (e) {}
    for (var si = 0; si < TOURNAMENT_STAND_KEYS.length; si++) {
      var stand = TOURNAMENT_STAND_KEYS[si];
      var byStand = TOURNAMENT_BASE[stand];
      if (!byStand) continue;
      for (var ci = 0; ci < TOURNAMENT_CONTOUR_KEYS.length; ci++) {
        var contour = TOURNAMENT_CONTOUR_KEYS[ci];
        var baseUrl = String((byStand && byStand[contour]) || "");
        var host = "";
        try {
          host = new URL(baseUrl).origin.toLowerCase();
        } catch (eHost) {}
        if (host && host === origin) {
          return { stand: stand, contour: contour };
        }
      }
    }
    return null;
  }

  const AUTO_ENV = detectTournamentEnvFromLocation();
  const DEFAULT_STAND = (AUTO_ENV && AUTO_ENV.stand) || "PROM";
  const DEFAULT_CONTOUR = (AUTO_ENV && AUTO_ENV.contour) || "ALPHA";

  function getTournamentEnv() {
    var stand =
      TOURNAMENT_STAND_KEYS.indexOf(DEFAULT_STAND) >= 0 ? DEFAULT_STAND : "PROM";
    var contour =
      TOURNAMENT_CONTOUR_KEYS.indexOf(DEFAULT_CONTOUR) >= 0
        ? DEFAULT_CONTOUR
        : "ALPHA";
    var byStand = TOURNAMENT_BASE[stand] || TOURNAMENT_BASE.PROM;
    var baseUrl =
      (byStand && byStand[contour]) || TOURNAMENT_BASE.PROM.ALPHA;
    return { stand: stand, contour: contour, baseUrl: baseUrl };
  }

  function delay(ms) {
    return new Promise(function (resolve) {
      setTimeout(resolve, ms);
    });
  }

  function getTimestamp() {
    const d = new Date();
    const p = function (n) {
      return n.toString().padStart(2, "0");
    };
    return (
      d.getFullYear().toString() +
      p(d.getMonth() + 1) +
      p(d.getDate()) +
      "-" +
      p(d.getHours()) +
      p(d.getMinutes()) +
      p(d.getSeconds())
    );
  }

  function countLeadersInResponseData(data) {
    if (data == null || typeof data !== "object") return null;
    const leadersArr =
      (data.body && data.body.tournament && data.body.tournament.leaders) ||
      (data.body && data.body.badge && data.body.badge.leaders);
    if (!Array.isArray(leadersArr)) return 0;
    return leadersArr.length;
  }

  function countEmployeeNumberFieldsInTree(obj) {
    let n = 0;
    function walk(o) {
      if (o == null) return;
      if (Array.isArray(o)) {
        for (let i = 0; i < o.length; i++) walk(o[i]);
        return;
      }
      if (typeof o !== "object") return;
      const keys = Object.keys(o);
      for (let i = 0; i < keys.length; i++) {
        const k = keys[i];
        const v = o[k];
        if (k === "employeeNumber") {
          if (v != null && v !== "") n++;
        } else {
          walk(v);
        }
      }
    }
    walk(obj);
    return n;
  }

  function buildTournamentWrappedErrorRecord(tid, errObj) {
    const id = tid != null ? String(tid) : "";
    return {
      success: true,
      body: {
        tournament: {
          tournamentId: id,
          tournamentIndicator: "",
          status: "ERROR",
          contestants: "",
          leaders: [],
          error: errObj
        }
      }
    };
  }

  function getExportErrorPayload(root) {
    if (root == null || typeof root !== "object") return null;
    if (root.success === false && root.error && typeof root.error === "object")
      return root.error;
    const t = root.body && root.body.tournament;
    if (
      root.success === true &&
      t &&
      typeof t === "object" &&
      t.status === "ERROR" &&
      t.error &&
      typeof t.error === "object"
    ) {
      return t.error;
    }
    return null;
  }

  function buildLeadersExportRecordArray(tid, fr) {
    if (!fr.ok) {
      const d = fr.data;
      if (
        d &&
        typeof d === "object" &&
        d.success === false &&
        d.error &&
        typeof d.error === "object"
      ) {
        return [buildTournamentWrappedErrorRecord(tid, d.error)];
      }
      return [
        buildTournamentWrappedErrorRecord(tid, {
          code: "HTTP-" + fr.status,
          title: "Ошибка HTTP",
          text:
            "Запрос GET leadersForAdmin для «" +
            tid +
            "» завершился со статусом " +
            fr.status +
            ".",
          type: "error",
          tournamentId: tid
        })
      ];
    }

    const data = fr.data;
    if (data == null || typeof data !== "object") {
      return null;
    }

    if (data.success === false && data.error && typeof data.error === "object") {
      return [buildTournamentWrappedErrorRecord(tid, data.error)];
    }

    const cnt = countLeadersInResponseData(data);
    if (cnt === null) {
      return null;
    }

    if (cnt === 0) {
      const src =
        (data.body &&
          data.body.tournament &&
          typeof data.body.tournament === "object" &&
          data.body.tournament) ||
        (data.body &&
          data.body.badge &&
          typeof data.body.badge === "object" &&
          data.body.badge) ||
        {};
      const leadersArr = Array.isArray(src.leaders) ? src.leaders.slice() : [];
      const tObj = {
        tournamentId:
          src.tournamentId != null && String(src.tournamentId) !== ""
            ? src.tournamentId
            : src.id != null && String(src.id) !== ""
              ? src.id
              : tid,
        tournamentIndicator:
          src.tournamentIndicator != null ? String(src.tournamentIndicator) : "",
        status: src.status != null ? String(src.status) : "",
        contestants: "0 участников",
        leaders: leadersArr
      };
      return [
        {
          success: false,
          body: {
            tournament: tObj
          }
        }
      ];
    }

    return [data];
  }

  async function fetchLeadersForAdmin(baseUrl, tournamentId) {
    const url =
      baseUrl +
      encodeURIComponent(tournamentId) +
      "/" +
      LEADERS_SERVICE +
      "?pageNum=1";
    const res = await fetch(url, {
      method: "GET",
      credentials: "include",
      headers: { Accept: "application/json" }
    });
    const data = await res.json().catch(function () {
      return null;
    });
    return {
      ok: res.ok,
      status: res.status,
      tournamentId: tournamentId,
      data: data
    };
  }

  function getJsonSizeBytes(obj) {
    if (obj == null) return 0;
    try {
      return new TextEncoder().encode(JSON.stringify(obj)).length;
    } catch (eSize) {
      return 0;
    }
  }

  function downloadJson(name, obj) {
    const jsonText = JSON.stringify(obj, null, 2);
    const fileSize = new TextEncoder().encode(jsonText).length;
    const blob = new Blob([jsonText], {
      type: "application/json"
    });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = name;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    setTimeout(function () {
      URL.revokeObjectURL(a.href);
    }, 0);
    return fileSize;
  }

  async function runAutoExport() {
    if (!TOURNAMENT_CODES || TOURNAMENT_CODES.length === 0) {
      console.warn("[leadersForAdmin Auto] Список кодов пуст — выгрузка отменена.");
      return;
    }

    const env = getTournamentEnv();
    const baseUrl = env.baseUrl;
    const prefix =
      LEADERS_SERVICE + "_" + env.stand + "_" + env.contour + "_";

    if (!AUTO_ENV) {
      console.warn(
        "[leadersForAdmin Auto] Стенд/контур не определены по URL страницы — используется " +
          env.stand +
          "/" +
          env.contour +
          ". Откройте скрипт на странице omega или salesheroes."
      );
    }

    console.log("[leadersForAdmin Auto] ——— Старт выгрузки ———");
    console.log(
      "[leadersForAdmin Auto] Кодов в очереди: " +
        TOURNAMENT_CODES.length +
        " | стенд/контур: " +
        env.stand +
        "/" +
        env.contour +
        " | URL: " +
        baseUrl +
        " | пауза: " +
        REQUEST_GAP_MS +
        " мс | префикс файла: " +
        prefix +
        "<дата>.json"
    );

    const results = {};
    let savedCount = 0;
    let skipped = 0;
    let errors = 0;
    let totalRespBytes = 0;
    const skippedNotSaved = [];

    for (let i = 0; i < TOURNAMENT_CODES.length; i++) {
      const tid = TOURNAMENT_CODES[i];
      console.log(
        "[leadersForAdmin Auto] Запрос " +
          (i + 1) +
          "/" +
          TOURNAMENT_CODES.length +
          " — GET leadersForAdmin «" +
          tid +
          "»"
      );
      try {
        const fr = await fetchLeadersForAdmin(baseUrl, tid);
        const respSize = getJsonSizeBytes(fr.data);
        totalRespBytes += respSize;
        if (!fr.ok) {
          console.warn(
            "[leadersForAdmin Auto]   → HTTP " +
              fr.status +
              " «" +
              tid +
              "» | размер ответа: " +
              respSize +
              " bytes — в файл пойдёт запись об ошибке."
          );
        }
        const pack = buildLeadersExportRecordArray(tid, fr);
        if (pack == null) {
          skipped++;
          skippedNotSaved.push(tid);
          console.warn(
            "[leadersForAdmin Auto]   → Пропуск «" +
              tid +
              "»: нет JSON-тела при HTTP OK | размер ответа: " +
              respSize +
              " bytes."
          );
          continue;
        }
        results[tid] = pack;
        savedCount++;
        const root = pack[0];
        const packSize = getJsonSizeBytes(pack);
        const empInTree = countEmployeeNumberFieldsInTree(root);
        const errPay = getExportErrorPayload(root);
        if (errPay) {
          console.log(
            "[leadersForAdmin Auto]   → в файл «" +
              tid +
              "»: ERROR" +
              (errPay.code ? " (код " + errPay.code + ")" : "") +
              " | размер ответа: " +
              respSize +
              " bytes | размер записи в файле: " +
              packSize +
              " bytes | employeeNumber в дереве: " +
              empInTree
          );
        } else if (
          root &&
          root.success === false &&
          root.body &&
          root.body.tournament
        ) {
          const lc = Array.isArray(root.body.tournament.leaders)
            ? root.body.tournament.leaders.length
            : 0;
          console.log(
            "[leadersForAdmin Auto]   → в файл «" +
              tid +
              "»: «0 участников» | leaders=" +
              lc +
              " | размер ответа: " +
              respSize +
              " bytes | размер записи: " +
              packSize +
              " bytes | employeeNumber: " +
              empInTree
          );
        } else {
          const cnt = countLeadersInResponseData(root);
          console.log(
            "[leadersForAdmin Auto]   → в файл «" +
              tid +
              "»: leaders=" +
              (cnt == null ? "?" : cnt) +
              " | размер ответа: " +
              respSize +
              " bytes | размер записи: " +
              packSize +
              " bytes | employeeNumber: " +
              empInTree
          );
        }
      } catch (e) {
        errors++;
        console.error(
          "[leadersForAdmin Auto] Исключение «" +
            tid +
            "»:" +
            (e && e.message ? " " + e.message : "")
        );
      }
      if (i < TOURNAMENT_CODES.length - 1) {
        await delay(REQUEST_GAP_MS);
      }
    }

    if (skippedNotSaved.length > 0) {
      console.warn(
        "[leadersForAdmin Auto] Пропуски без записи в JSON (нет тела): " +
          skippedNotSaved.join(", ")
      );
    }

    if (savedCount === 0 || Object.keys(results).length === 0) {
      console.warn(
        "[leadersForAdmin Auto] Файл не создан. Пропусков: " +
          skipped +
          " | исключений: " +
          errors +
          " | суммарный размер ответов: " +
          totalRespBytes +
          " bytes"
      );
      return;
    }

    const fname = prefix + getTimestamp() + ".json";
    let totalEmp = 0;
    const perTournament = [];
    Object.keys(results).forEach(function (k) {
      const pack = results[k];
      const rootData = pack && pack[0];
      const em = countEmployeeNumberFieldsInTree(rootData);
      totalEmp += em;
      var line = "«" + k + "»:";
      const errP = getExportErrorPayload(rootData);
      if (errP) {
        line +=
          " ERROR, код " +
          (errP.code != null ? String(errP.code) : "?") +
          ", employeeNumber=" +
          em;
      } else if (
        rootData &&
        rootData.success === false &&
        rootData.body &&
        rootData.body.tournament
      ) {
        var t = rootData.body.tournament;
        line +=
          " " +
          (t.contestants != null ? String(t.contestants) : "0 участников") +
          ", leaders=" +
          (Array.isArray(t.leaders) ? t.leaders.length : 0) +
          ", employeeNumber=" +
          em;
      } else {
        const lc = countLeadersInResponseData(rootData);
        line += " leaders=" + (lc == null ? "?" : lc) + ", employeeNumber=" + em;
      }
      perTournament.push(line);
    });

    const fileSize = downloadJson(fname, results);
    console.log("[leadersForAdmin Auto] ==== ИТОГ ====");
    console.log(
      "[leadersForAdmin Auto] Готово. Файл: " +
        fname +
        " | записей в файле: " +
        savedCount +
        " | размер файла: " +
        fileSize +
        " bytes | суммарный размер ответов: " +
        totalRespBytes +
        " bytes"
    );
    console.log(
      "[leadersForAdmin Auto] Σ employeeNumber по дереву: " +
        totalEmp +
        " | пропусков (без тела): " +
        skipped +
        " | исключений: " +
        errors
    );
    console.log(
      "[leadersForAdmin Auto] Детально по турнирам: " + perTournament.join(" | ")
    );
  }

  void runAutoExport();
})();
"""


def build_js_content(
    codes: List[str],
    *,
    schedule_name: str,
    contest_name: str,
    active_statuses: Sequence[str],
    contest_type_raw: str,
) -> str:
    """Подставляет список кодов и метаданные в шаблон JS."""
    return (
        JS_TEMPLATE.replace("{{GENERATED_AT}}", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        .replace("{{SOURCE_SCHEDULE}}", schedule_name)
        .replace("{{SOURCE_CONTEST}}", contest_name)
        .replace("{{ACTIVE_STATUSES}}", ", ".join(active_statuses))
        .replace("{{CONTEST_TYPE_RAW}}", contest_type_raw)
        .replace("{{CODE_COUNT}}", str(len(codes)))
        .replace("{{CODES_JSON}}", json.dumps(codes, ensure_ascii=False, indent=2))
        .replace("{{REQUEST_GAP_MS}}", str(REQUEST_GAP_MS))
    )


def write_tournament_leaders_auto_js(
    output_dir: str,
    *,
    sheets_data: Optional[Mapping[str, Any]] = None,
    manager_stats_cfg: Optional[Mapping[str, Any]] = None,
    full_cfg: Optional[Mapping[str, Any]] = None,
) -> Optional[str]:
    """
    Записывает Tournament_LeadersForAdmin_AutoRun.js в каталог прогона (рядом с Excel).

    Returns:
        Путь к файлу или None, если генерация отключена или нет кодов.
    """
    mcfg = merge_manager_stats_config(manager_stats_cfg)
    catalog_cfg = dict(mcfg.get("prom_tournament_catalog") or {})
    if catalog_cfg.get("leaders_for_admin_js_enabled") is False:
        return None

    if full_cfg is None:
        full_cfg = {}

    if not manager_stats_only_in_run_outputs(full_cfg):
        logging.debug(
            "[manager_stats] leadersForAdmin JS: пропуск — в run_outputs нет manager_stats_only"
        )
        return None

    active_statuses = list(
        catalog_cfg.get("active_statuses") or ["АКТИВНЫЙ", "ПОДВЕДЕНИЕ ИТОГОВ"]
    )
    contest_vid = str(catalog_cfg.get("contest_vid") or "ПРОМ").strip()
    contest_type_raw = str(
        catalog_cfg.get("leaders_for_admin_contest_type") or "ТУРНИРНЫЙ"
    ).strip()
    js_name = str(
        catalog_cfg.get("leaders_for_admin_js_file") or DEFAULT_JS_FILENAME
    ).strip() or DEFAULT_JS_FILENAME

    try:
        df_schedule, df_contest, schedule_label, contest_label = _resolve_schedule_contest_frames(
            sheets_data=sheets_data,
            full_cfg=full_cfg,
            catalog_cfg=catalog_cfg,
        )
    except FileNotFoundError as exc:
        logging.warning("[manager_stats] leadersForAdmin JS: %s", exc)
        return None

    codes = collect_leaders_for_admin_tournament_codes(
        df_schedule,
        df_contest,
        active_statuses=active_statuses,
        contest_vid=contest_vid,
        contest_type_raw=contest_type_raw,
    )
    if not codes:
        logging.warning(
            "[manager_stats] leadersForAdmin JS: нет кодов (статусы %s, CONTEST_TYPE=%s)",
            active_statuses,
            contest_type_raw,
        )
        return None

    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, js_name)
    content = build_js_content(
        codes,
        schedule_name=schedule_label,
        contest_name=contest_label,
        active_statuses=active_statuses,
        contest_type_raw=contest_type_raw,
    )
    Path(out_path).write_text(content, encoding="utf-8")
    logging.info(
        "[manager_stats] leadersForAdmin JS: %s кодов → %s",
        len(codes),
        out_path,
    )
    return out_path
