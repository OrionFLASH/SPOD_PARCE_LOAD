/**
 * Ядро подготовки REPORT (логика Power Query FIO + TN в одном контуре).
 * Работает в браузере и в Node (тесты).
 */
(function (root) {
  "use strict";

  var DEFAULTS = {
    personNumberLength: 20,
    numberDecimals: 5,
    priorityType: "1",
    missingPersonPlaceholder: "00000000",
    noDuplicateMark: "-",
    missingFioFlag: "ДА",
  };

  function mergeDefaults(options) {
    var out = {};
    var key;
    for (key in DEFAULTS) {
      if (Object.prototype.hasOwnProperty.call(DEFAULTS, key)) {
        out[key] = DEFAULTS[key];
      }
    }
    if (options && typeof options === "object") {
      for (key in options) {
        if (Object.prototype.hasOwnProperty.call(options, key) && options[key] != null) {
          out[key] = options[key];
        }
      }
    }
    return out;
  }

  /** Преобразование значения с запятой/точкой в число (как ИзЗапятойВЧисло). */
  function parseNumberFromComma(value) {
    if (value == null || value === "") {
      return 0;
    }
    if (typeof value === "number" && isFinite(value)) {
      return value;
    }
    var text = String(value).trim().replace(/\s+/g, "").replace(",", ".");
    if (!text) {
      return 0;
    }
    var parts = text.split(".");
    var whole = Number(parts[0]);
    if (!isFinite(whole)) {
      whole = 0;
    }
    if (parts.length === 1) {
      return whole;
    }
    var fracText = parts.slice(1).join("");
    var fracNum = Number(fracText);
    if (!isFinite(fracNum)) {
      fracNum = 0;
    }
    var divisor = Math.pow(10, fracText.length);
    if (divisor === 0) {
      return whole;
    }
    return whole + fracNum / divisor;
  }

  function formatNumberDot(num, decimals) {
    var d = decimals == null ? DEFAULTS.numberDecimals : decimals;
    var n = typeof num === "number" && isFinite(num) ? num : 0;
    return n.toFixed(d);
  }

  function formatNumberComma(num, decimals) {
    return formatNumberDot(num, decimals).replace(".", ",");
  }

  function padPersonNumber(raw, length) {
    var len = length == null ? DEFAULTS.personNumberLength : length;
    var text = String(raw == null ? "" : raw).trim();
    if (!text) {
      text = DEFAULTS.missingPersonPlaceholder;
    }
    if (text.length >= len) {
      return text;
    }
    return new Array(len - text.length + 1).join("0") + text;
  }

  function normalizeFioKey(fio) {
    return String(fio == null ? "" : fio).trim().toUpperCase();
  }

  /**
   * Справочник ФИО → табельный.
   * @param {Array<{fio:string, person_number:string}>} entries
   * @returns {Map<string, string>}
   */
  function buildFioMap(entries) {
    var map = new Map();
    if (!Array.isArray(entries)) {
      return map;
    }
    entries.forEach(function (entry) {
      if (!entry) {
        return;
      }
      var key = normalizeFioKey(entry.fio);
      var code = String(entry.person_number == null ? "" : entry.person_number).trim();
      if (!key || !code) {
        return;
      }
      if (!map.has(key)) {
        map.set(key, code);
      }
    });
    return map;
  }

  function emptyCell(value) {
    return value == null || String(value).trim() === "";
  }

  /**
   * Нормализация строк одного турнира.
   * @returns {{ rows: object[], missingFio: string[] }}
   */
  function normalizeTournamentRows(rawRows, tournament, fioMap, options) {
    var cfg = mergeDefaults(options);
    var typeInd = String(tournament.type_ind || "TN").trim().toUpperCase();
    var modeFio = typeInd === "FIO";
    var colId = tournament.column_id || (modeFio ? "ФИО" : "ТАБЕЛЬНЫЙ НОМЕР");
    var colFact = tournament.column_fact || "ПОКАЗАТЕЛЬ";
    var planNum = parseNumberFromComma(tournament.plan_value);
    var planText = formatNumberDot(planNum, cfg.numberDecimals);
    var missingFio = [];
    var missingSeen = Object.create(null);
    var rows = [];

    (rawRows || []).forEach(function (raw, index) {
      if (!raw || typeof raw !== "object") {
        return;
      }
      var idRaw = raw[colId];
      if (emptyCell(idRaw)) {
        return;
      }
      if (String(idRaw).trim() === colId) {
        return;
      }

      var fio = modeFio ? String(idRaw).trim() : cfg.noDuplicateMark;
      var personRaw;
      var notFound = cfg.noDuplicateMark;

      if (modeFio) {
        var key = normalizeFioKey(fio);
        var found = fioMap && fioMap.get(key);
        if (found == null || String(found).trim() === "") {
          personRaw = cfg.missingPersonPlaceholder;
          notFound = cfg.missingFioFlag;
          if (!missingSeen[key]) {
            missingSeen[key] = true;
            missingFio.push(fio);
          }
        } else {
          personRaw = String(found).trim();
        }
      } else {
        personRaw = String(idRaw).trim();
      }

      var factNum = parseNumberFromComma(raw[colFact]);
      rows.push({
        source_index: index,
        tournament_id: tournament.id || "",
        MANAGER_PERSON_NUMBER: padPersonNumber(personRaw, cfg.personNumberLength),
        CONTEST_CODE: String(tournament.contest_code || "").trim(),
        TOURNAMENT_CODE: String(tournament.tournament_code || "").trim(),
        CONTEST_DATE: String(tournament.contest_date || "").trim(),
        PLAN_VALUE: planText,
        FACT_VALUE: formatNumberDot(factNum, cfg.numberDecimals),
        priority_type: String(cfg.priorityType),
        "CONTEST-DATA=>FULL_NAME": String(tournament.full_name || "").trim(),
        FIO: fio,
        "ТАБЕЛЬНЫЙ НЕ НАЙДЕН": notFound,
        PLAN_VALUE_число: planNum,
        FACT_VALUE_число: factNum,
        ДУБЛЬ: cfg.noDuplicateMark,
        "СУММА ПО ТАБЕЛЬНОМУ": cfg.noDuplicateMark,
        "В ФОРМАТЕ": cfg.noDuplicateMark,
        duplicate_comment: "",
        include_in_csv: true,
      });
    });

    return { rows: rows, missingFio: missingFio };
  }

  function duplicateKey(row) {
    return [row.CONTEST_CODE, row.TOURNAMENT_CODE, row.MANAGER_PERSON_NUMBER].join("|");
  }

  /**
   * Группы дублей: ключ → массив индексов в rows.
   */
  function findDuplicateGroups(rows) {
    var groups = Object.create(null);
    (rows || []).forEach(function (row, idx) {
      var key = duplicateKey(row);
      if (!groups[key]) {
        groups[key] = [];
      }
      groups[key].push(idx);
    });
    var result = [];
    Object.keys(groups).forEach(function (key) {
      if (groups[key].length > 1) {
        result.push({
          key: key,
          indices: groups[key],
          rows: groups[key].map(function (i) {
            return rows[i];
          }),
          sum: groups[key].reduce(function (acc, i) {
            return acc + (rows[i].FACT_VALUE_число || 0);
          }, 0),
        });
      }
    });
    return result;
  }

  /**
   * Аннотация колонок ДУБЛЬ / СУММА как в PQ (до разрешения).
   */
  function annotateDuplicatesLikePq(rows, options) {
    var cfg = mergeDefaults(options);
    var groups = findDuplicateGroups(rows);
    var byKey = Object.create(null);
    groups.forEach(function (g) {
      byKey[g.key] = g;
    });
    rows.forEach(function (row) {
      var g = byKey[duplicateKey(row)];
      if (!g) {
        row.ДУБЛЬ = cfg.noDuplicateMark;
        row["СУММА ПО ТАБЕЛЬНОМУ"] = cfg.noDuplicateMark;
        row["В ФОРМАТЕ"] = cfg.noDuplicateMark;
        return;
      }
      row.ДУБЛЬ = String(g.indices.length);
      row["СУММА ПО ТАБЕЛЬНОМУ"] = formatNumberComma(g.sum, cfg.numberDecimals);
      row["В ФОРМАТЕ"] = formatNumberDot(g.sum, cfg.numberDecimals);
    });
    return rows;
  }

  /**
   * Применение решения по дублям.
   * resolution: { mode: 'abort'|'drop_all'|'keep_one'|'sum', keepIndex?: number }
   * resolutionsByKey: { [key]: resolution }
   */
  function applyDuplicateResolutions(rows, resolutionsByKey, options) {
    var cfg = mergeDefaults(options);
    var list = (rows || []).map(function (r) {
      return Object.assign({}, r);
    });
    annotateDuplicatesLikePq(list, cfg);

    var groups = findDuplicateGroups(list);
    for (var gi = 0; gi < groups.length; gi++) {
      var g = groups[gi];
      var res = resolutionsByKey && resolutionsByKey[g.key];
      if (!res || !res.mode) {
        return { ok: false, error: "Нет решения для дубля: " + g.key, rows: list };
      }
      if (res.mode === "abort") {
        return { ok: false, error: "Формирование остановлено пользователем (дубли).", aborted: true, rows: list };
      }

      var indices = g.indices.slice().sort(function (a, b) {
        return a - b;
      });
      var commentBase = "Дубль " + g.indices.length + " строк; сумма=" + formatNumberDot(g.sum, cfg.numberDecimals);

      if (res.mode === "drop_all") {
        indices.forEach(function (idx) {
          list[idx].include_in_csv = false;
          list[idx].duplicate_comment = commentBase + "; решение: убрать все дубли из CSV";
        });
        continue;
      }

      if (res.mode === "keep_one") {
        var keepIdx = res.keepIndex;
        if (keepIdx == null || indices.indexOf(keepIdx) < 0) {
          return { ok: false, error: "Не выбран ряд для keep_one: " + g.key, rows: list };
        }
        indices.forEach(function (idx) {
          if (idx === keepIdx) {
            list[idx].include_in_csv = true;
            list[idx].duplicate_comment = commentBase + "; решение: оставлена эта строка";
          } else {
            list[idx].include_in_csv = false;
            list[idx].duplicate_comment = commentBase + "; решение: исключена (выбрана другая строка)";
          }
        });
        continue;
      }

      if (res.mode === "sum") {
        var first = indices[0];
        var sumVal = g.sum;
        list[first].FACT_VALUE_число = sumVal;
        list[first].FACT_VALUE = formatNumberDot(sumVal, cfg.numberDecimals);
        list[first].include_in_csv = true;
        list[first].duplicate_comment = commentBase + "; решение: сумма показателей в одну строку";
        indices.slice(1).forEach(function (idx) {
          list[idx].include_in_csv = false;
          list[idx].duplicate_comment = commentBase + "; решение: поглощена суммой в первую строку группы";
        });
        continue;
      }

      return { ok: false, error: "Неизвестный режим дубля: " + res.mode, rows: list };
    }

    return { ok: true, rows: list };
  }

  var CSV_COLUMNS = [
    "MANAGER_PERSON_NUMBER",
    "CONTEST_CODE",
    "TOURNAMENT_CODE",
    "CONTEST_DATE",
    "PLAN_VALUE",
    "FACT_VALUE",
    "priority_type",
  ];

  var XLSX_COLUMNS = [
    "MANAGER_PERSON_NUMBER",
    "CONTEST_CODE",
    "TOURNAMENT_CODE",
    "CONTEST_DATE",
    "PLAN_VALUE",
    "FACT_VALUE",
    "priority_type",
    "CONTEST-DATA=>FULL_NAME",
    "ДУБЛЬ",
    "СУММА ПО ТАБЕЛЬНОМУ",
    "В ФОРМАТЕ",
    "PLAN_VALUE_число",
    "FACT_VALUE_число",
    "FIO",
    "ТАБЕЛЬНЫЙ НЕ НАЙДЕН",
    "КОММЕНТАРИЙ_ДУБЛЬ",
  ];

  function rowsForCsv(rows) {
    return (rows || [])
      .filter(function (r) {
        return r.include_in_csv !== false;
      })
      .map(function (r) {
        var out = {};
        CSV_COLUMNS.forEach(function (c) {
          out[c] = r[c] == null ? "" : r[c];
        });
        return out;
      });
  }

  function rowsForXlsx(rows) {
    return (rows || []).map(function (r) {
      var out = {};
      XLSX_COLUMNS.forEach(function (c) {
        if (c === "КОММЕНТАРИЙ_ДУБЛЬ") {
          out[c] = r.duplicate_comment || "";
        } else {
          out[c] = r[c] == null ? "" : r[c];
        }
      });
      return out;
    });
  }

  /**
   * Полный прогон без интерактива (если нет дублей / решений переданы).
   */
  function processAll(tournamentsPayload, fioEntries, resolutionsByKey, options) {
    var cfg = mergeDefaults(options);
    var fioMap = buildFioMap(fioEntries);
    var allRows = [];
    var allMissing = [];
    var missingSeen = Object.create(null);

    (tournamentsPayload || []).forEach(function (item) {
      var result = normalizeTournamentRows(item.rows, item.tournament, fioMap, cfg);
      allRows = allRows.concat(result.rows);
      result.missingFio.forEach(function (fio) {
        var k = normalizeFioKey(fio);
        if (!missingSeen[k]) {
          missingSeen[k] = true;
          allMissing.push(fio);
        }
      });
    });

    annotateDuplicatesLikePq(allRows, cfg);
    var groups = findDuplicateGroups(allRows);

    if (groups.length > 0) {
      var applied = applyDuplicateResolutions(allRows, resolutionsByKey || {}, cfg);
      if (!applied.ok) {
        return {
          ok: false,
          error: applied.error,
          aborted: !!applied.aborted,
          rows: applied.rows,
          missingFio: allMissing,
          duplicateGroups: groups,
        };
      }
      allRows = applied.rows;
    }

    return {
      ok: true,
      rows: allRows,
      missingFio: allMissing,
      duplicateGroups: groups,
      csvRows: rowsForCsv(allRows),
      xlsxRows: rowsForXlsx(allRows),
    };
  }

  function createEmptyTournament(partial) {
    var t = {
      id: "t_" + Date.now() + "_" + Math.floor(Math.random() * 10000),
      contest_code: "",
      tournament_code: "",
      plan_value: "100.00000",
      contest_date: "",
      full_name: "",
      type_ind: "TN",
      column_id: "",
      column_fact: "",
      source_file_name: "",
      sheet_name: "",
      table_start_row: 1,
      table_start_col: 1,
      needs_identity_fix: false,
      copy_lock_code: "",
      copy_lock_name: "",
    };
    if (partial && typeof partial === "object") {
      Object.keys(partial).forEach(function (k) {
        t[k] = partial[k];
      });
    }
    return t;
  }

  function cloneTournament(source) {
    var src = source || {};
    var copy = createEmptyTournament({
      contest_code: src.contest_code,
      tournament_code: src.tournament_code,
      plan_value: src.plan_value,
      contest_date: src.contest_date,
      full_name: src.full_name,
      type_ind: src.type_ind,
      column_id: src.column_id,
      column_fact: src.column_fact,
      source_file_name: src.source_file_name,
      sheet_name: src.sheet_name,
      table_start_row: src.table_start_row || 1,
      table_start_col: src.table_start_col || 1,
      needs_identity_fix: true,
      copy_lock_code: String(src.tournament_code || "").trim(),
      copy_lock_name: String(src.full_name || "").trim(),
    });
    return copy;
  }

  function tournamentFieldsOk(t) {
    if (!t) {
      return false;
    }
    return !!(
      String(t.contest_code || "").trim() &&
      String(t.tournament_code || "").trim() &&
      String(t.contest_date || "").trim() &&
      String(t.plan_value || "").trim() &&
      String(t.full_name || "").trim() &&
      String(t.type_ind || "").trim()
    );
  }

  function tournamentIdentityUnlocked(t) {
    if (!t || !t.needs_identity_fix) {
      return true;
    }
    var code = String(t.tournament_code || "").trim();
    var name = String(t.full_name || "").trim();
    var lockCode = String(t.copy_lock_code || "").trim();
    var lockName = String(t.copy_lock_name || "").trim();
    return code !== lockCode && name !== lockName && !!code && !!name;
  }

  function tournamentSourceOk(t, dataPack) {
    if (!t || !dataPack || !dataPack.rows || !dataPack.rows.length) {
      return false;
    }
    return !!(String(t.column_id || "").trim() && String(t.column_fact || "").trim());
  }

  /**
   * Стадии готовности для шапки.
   * dataById: { [tournamentId]: { rows, columns } }
   * fioEntries: массив справочника
   * checkState: { duplicatesCleared: bool, missingFioCleared: bool } | null
   */
  function computeStages(tournaments, dataById, fioEntries, checkState) {
    var list = tournaments || [];
    var data = dataById || {};
    var fioMap = buildFioMap(fioEntries || []);
    var hasTournaments = list.length > 0;
    var fieldsFilled =
      hasTournaments &&
      list.every(function (t) {
        return tournamentFieldsOk(t) && tournamentIdentityUnlocked(t);
      });
    var sourcesOk =
      hasTournaments &&
      list.every(function (t) {
        return tournamentSourceOk(t, data[t.id]);
      });

    var fioOk = true;
    var missingFio = [];
    if (sourcesOk) {
      list.forEach(function (t) {
        if (String(t.type_ind || "").toUpperCase() !== "FIO") {
          return;
        }
        var pack = data[t.id];
        var norm = normalizeTournamentRows(pack.rows, t, fioMap, {});
        norm.missingFio.forEach(function (f) {
          if (missingFio.indexOf(f) < 0) {
            missingFio.push(f);
          }
        });
      });
      fioOk = missingFio.length === 0 || !!(checkState && checkState.missingFioCleared);
    } else {
      fioOk = false;
    }

    var dupOk = false;
    var dupGroups = [];
    if (sourcesOk) {
      var allRows = [];
      list.forEach(function (t) {
        var pack = data[t.id];
        var norm = normalizeTournamentRows(pack.rows, t, fioMap, {});
        allRows = allRows.concat(norm.rows);
      });
      annotateDuplicatesLikePq(allRows, {});
      dupGroups = findDuplicateGroups(allRows);
      dupOk = dupGroups.length === 0 || !!(checkState && checkState.duplicatesCleared);
    }

    var blockedCopies = list.filter(function (t) {
      return !tournamentIdentityUnlocked(t);
    });

    return {
      hasTournaments: hasTournaments,
      fieldsFilled: fieldsFilled,
      sourcesOk: sourcesOk,
      fioOk: fioOk,
      duplicatesOk: dupOk,
      missingFio: missingFio,
      duplicateGroups: dupGroups,
      blockedCopies: blockedCopies,
      canProcess: hasTournaments && fieldsFilled && sourcesOk && fioOk && dupOk && blockedCopies.length === 0,
      canCheck: hasTournaments && fieldsFilled && sourcesOk && blockedCopies.length === 0,
    };
  }

  function serializeSettings(tournaments) {
    return {
      version: 1,
      kind: "spod_web_report_settings",
      tournaments: (tournaments || []).map(function (t) {
        return {
          id: t.id,
          contest_code: t.contest_code,
          tournament_code: t.tournament_code,
          plan_value: t.plan_value,
          contest_date: t.contest_date,
          full_name: t.full_name,
          type_ind: t.type_ind,
          column_id: t.column_id || "",
          column_fact: t.column_fact || "",
          source_file_name: t.source_file_name || "",
          sheet_name: t.sheet_name || "",
          table_start_row: t.table_start_row || 1,
          table_start_col: t.table_start_col || 1,
          needs_identity_fix: !!t.needs_identity_fix,
          copy_lock_code: t.copy_lock_code || "",
          copy_lock_name: t.copy_lock_name || "",
        };
      }),
    };
  }

  function parseSettings(json) {
    var data = typeof json === "string" ? JSON.parse(json) : json;
    if (!data || !Array.isArray(data.tournaments)) {
      throw new Error("Неверный JSON настроек: нужен массив tournaments");
    }
    return data.tournaments.map(function (t) {
      return createEmptyTournament(t);
    });
  }

  function serializeFioDictionary(entries) {
    return {
      version: 1,
      kind: "spod_web_report_fio_dictionary",
      entries: (entries || [])
        .map(function (e) {
          return {
            fio: String(e.fio || "").trim(),
            person_number: String(e.person_number || "").trim(),
          };
        })
        .filter(function (e) {
          return e.fio && e.person_number;
        }),
    };
  }

  function parseFioDictionary(json) {
    var data = typeof json === "string" ? JSON.parse(json) : json;
    if (!data || !Array.isArray(data.entries)) {
      throw new Error("Неверный JSON справочника ФИО: нужен массив entries");
    }
    return data.entries.map(function (e) {
      return {
        fio: String(e.fio || "").trim(),
        person_number: String(e.person_number || "").trim(),
      };
    });
  }

  var ReportCore = {
    DEFAULTS: DEFAULTS,
    mergeDefaults: mergeDefaults,
    parseNumberFromComma: parseNumberFromComma,
    formatNumberDot: formatNumberDot,
    formatNumberComma: formatNumberComma,
    padPersonNumber: padPersonNumber,
    normalizeFioKey: normalizeFioKey,
    buildFioMap: buildFioMap,
    normalizeTournamentRows: normalizeTournamentRows,
    duplicateKey: duplicateKey,
    findDuplicateGroups: findDuplicateGroups,
    annotateDuplicatesLikePq: annotateDuplicatesLikePq,
    applyDuplicateResolutions: applyDuplicateResolutions,
    rowsForCsv: rowsForCsv,
    rowsForXlsx: rowsForXlsx,
    processAll: processAll,
    createEmptyTournament: createEmptyTournament,
    cloneTournament: cloneTournament,
    tournamentFieldsOk: tournamentFieldsOk,
    tournamentIdentityUnlocked: tournamentIdentityUnlocked,
    tournamentSourceOk: tournamentSourceOk,
    computeStages: computeStages,
    serializeSettings: serializeSettings,
    parseSettings: parseSettings,
    serializeFioDictionary: serializeFioDictionary,
    parseFioDictionary: parseFioDictionary,
    CSV_COLUMNS: CSV_COLUMNS,
    XLSX_COLUMNS: XLSX_COLUMNS,
  };

  if (typeof module !== "undefined" && module.exports) {
    module.exports = ReportCore;
  }
  root.ReportCore = ReportCore;
})(typeof globalThis !== "undefined" ? globalThis : this);
