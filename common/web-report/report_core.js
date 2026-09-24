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

  /** Периоды турнира: Y / Q1–Q4 / M1–M12 / F */
  var PERIOD_OPTIONS = [
    { code: "Y", label: "Турнир года" },
    { code: "Q1", label: "1 квартал" },
    { code: "Q2", label: "2 квартал" },
    { code: "Q3", label: "3 квартал" },
    { code: "Q4", label: "4 квартал" },
    { code: "M1", label: "Январь" },
    { code: "M2", label: "Февраль" },
    { code: "M3", label: "Март" },
    { code: "M4", label: "Апрель" },
    { code: "M5", label: "Май" },
    { code: "M6", label: "Июнь" },
    { code: "M7", label: "Июль" },
    { code: "M8", label: "Август" },
    { code: "M9", label: "Сентябрь" },
    { code: "M10", label: "Октябрь" },
    { code: "M11", label: "Ноябрь" },
    { code: "M12", label: "Декабрь" },
    { code: "F", label: "Произвольный" },
  ];

  function normalizePeriodCode(code) {
    var c = String(code == null ? "Y" : code).trim().toUpperCase();
    for (var i = 0; i < PERIOD_OPTIONS.length; i++) {
      if (PERIOD_OPTIONS[i].code === c) {
        return c;
      }
    }
    return "F";
  }

  function periodLabel(code) {
    var c = normalizePeriodCode(code);
    for (var i = 0; i < PERIOD_OPTIONS.length; i++) {
      if (PERIOD_OPTIONS[i].code === c) {
        return PERIOD_OPTIONS[i].label;
      }
    }
    return "Произвольный";
  }

  function periodDisplay(code) {
    var c = normalizePeriodCode(code);
    return periodLabel(c) + " " + c;
  }

  /**
   * Краткие метки периода в списке: Y, Q1, M1…
   * Если в одном CONTEST_CODE несколько турниров с одним периодом — суффикс (1)(2).
   */
  function periodBadgesForTournaments(tournaments) {
    var list = tournaments || [];
    var counts = Object.create(null);
    var seen = Object.create(null);
    list.forEach(function (t) {
      var key = String(t.contest_code || "").trim() + "|" + normalizePeriodCode(t.period_code);
      counts[key] = (counts[key] || 0) + 1;
    });
    var out = Object.create(null);
    list.forEach(function (t) {
      var code = normalizePeriodCode(t.period_code);
      var key = String(t.contest_code || "").trim() + "|" + code;
      if (counts[key] <= 1) {
        out[t.id] = code;
        return;
      }
      seen[key] = (seen[key] || 0) + 1;
      out[t.id] = code + "(" + seen[key] + ")";
    });
    return out;
  }

  /** Операции над показателем перед записью в FACT_VALUE. */
  function applyFactOperation(rawNum, op, opValue) {
    var n = typeof rawNum === "number" && isFinite(rawNum) ? rawNum : 0;
    var mode = String(op || "none").toLowerCase();
    var v = parseNumberFromComma(opValue == null || opValue === "" ? (mode === "mul" || mode === "div" ? 1 : 0) : opValue);
    if (mode === "mul" || mode === "multiply" || mode === "*") {
      return n * v;
    }
    if (mode === "div" || mode === "divide" || mode === "/") {
      if (v === 0) {
        return n;
      }
      return n / v;
    }
    if (mode === "add" || mode === "+") {
      return n + v;
    }
    if (mode === "sub" || mode === "subtract" || mode === "-") {
      return n - v;
    }
    return n;
  }

  function isValidPersonNumber20(value, length) {
    var len = length == null ? DEFAULTS.personNumberLength : length;
    var text = String(value == null ? "" : value).trim();
    if (text.length !== len) {
      return false;
    }
    return /^\d+$/.test(text);
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

  /**
   * Табельный из таблицы ФИО: непусто и только цифры (ведущие нули допустимы).
   * @param {*} value
   * @returns {boolean}
   */
  function isNumericPersonToken(value) {
    var text = String(value == null ? "" : value).trim();
    return text !== "" && /^\d+$/.test(text);
  }

  /**
   * Собрать записи справочника из строк таблицы ФИО.
   * Дубли по ФИО: берём первую строку с корректным табельным; если ФИО одно — берём как есть.
   * @returns {{
   *   entries: Array<{fio:string, person_number:string}>,
   *   stats: {
   *     sourceRows: number,
   *     uniqueFio: number,
   *     duplicateFioNames: number,
   *     duplicateExtraRows: number,
   *     invalidTnRows: number,
   *     message: string
   *   }
   * }}
   */
  function resolveFioTableEntries(rows, colFio, colTn) {
    var groups = Object.create(null);
    var order = [];
    var sourceRows = 0;
    var invalidTnRows = 0;

    (rows || []).forEach(function (row) {
      if (!row) return;
      var fio = String(row[colFio] == null ? "" : row[colFio]).trim();
      var tnRaw = row[colTn];
      var tn = String(tnRaw == null ? "" : tnRaw).trim();
      if (!fio) return;
      // пропуск строки заголовка, попавшей в данные
      if (fio === colFio || tn === colTn) return;
      sourceRows += 1;
      var tnOk = isNumericPersonToken(tn);
      if (!tnOk) {
        invalidTnRows += 1;
      }
      var key = normalizeFioKey(fio);
      if (!groups[key]) {
        groups[key] = [];
        order.push(key);
      }
      groups[key].push({ fio: fio, person_number: tn, tnOk: tnOk });
    });

    var entries = [];
    var duplicateFioNames = 0;
    var duplicateExtraRows = 0;
    var issues = [];

    order.forEach(function (key) {
      var list = groups[key];
      var isDup = list.length > 1;
      if (isDup) {
        duplicateFioNames += 1;
        duplicateExtraRows += list.length - 1;
      }
      var chosen = null;
      var chosenIdx = 0;
      if (list.length === 1) {
        chosen = list[0];
        chosenIdx = 0;
      } else {
        for (var i = 0; i < list.length; i += 1) {
          if (list[i].tnOk) {
            chosen = list[i];
            chosenIdx = i;
            break;
          }
        }
        if (!chosen) {
          chosen = list[0];
          chosenIdx = 0;
        }
      }
      entries.push({ fio: chosen.fio, person_number: chosen.person_number });

      // детализация проблем: дубли и/или невалидный ТН
      var groupHasProblem = isDup || list.some(function (r) {
        return !r.tnOk;
      });
      if (groupHasProblem) {
        list.forEach(function (row, idx) {
          var reasons = [];
          if (isDup) reasons.push("дубль ФИО");
          if (!row.person_number) reasons.push("табельный пуст");
          else if (!row.tnOk) reasons.push("табельный не из цифр");
          issues.push({
            fio: row.fio,
            person_number: row.person_number,
            tnOk: row.tnOk,
            isDuplicate: isDup,
            chosen: idx === chosenIdx,
            reasons: reasons,
          });
        });
      }
    });

    var parts = [];
    if (duplicateFioNames > 0) {
      parts.push(
        "повторяющихся ФИО — " +
          duplicateFioNames +
          " (лишних строк: " +
          duplicateExtraRows +
          "; для каждого взята первая с корректным табельным)"
      );
    }
    if (invalidTnRows > 0) {
      parts.push("строк с пустым или нечисловым табельным — " + invalidTnRows);
    }
    var message = parts.length
      ? "В файле ФИО: " + parts.join("; ") + "."
      : "";

    return {
      entries: entries,
      issues: issues,
      stats: {
        sourceRows: sourceRows,
        uniqueFio: entries.length,
        duplicateFioNames: duplicateFioNames,
        duplicateExtraRows: duplicateExtraRows,
        invalidTnRows: invalidTnRows,
        message: message,
        hasProblems: issues.length > 0,
      },
    };
  }

  /**
   * Краткая статистика строк источника турнира для карточки/панелей.
   * @returns {{ loaded: boolean, total: number, errors: number, forCsv: number }}
   */
  function tournamentRowStats(tournament, pack, fioEntries, options) {
    var empty = { loaded: false, total: 0, errors: 0, forCsv: 0 };
    if (!tournament || !pack || !Array.isArray(pack.rows)) {
      return empty;
    }
    var fioMap = buildFioMap(fioEntries || []);
    var norm = normalizeTournamentRows(pack.rows, tournament, fioMap, options || {});
    var rows = norm.rows || [];
    var total = rows.length;
    if (!total) {
      return { loaded: true, total: 0, errors: 0, forCsv: 0 };
    }
    var cfg = mergeDefaults(options || {});
    var errors = 0;
    rows.forEach(function (row) {
      var bad = false;
      if (!isValidPersonNumber20(row.MANAGER_PERSON_NUMBER, cfg.personNumberLength)) {
        bad = true;
      }
      if (String(row["ТАБЕЛЬНЫЙ НЕ НАЙДЕН"] || "") === cfg.missingFioFlag) {
        bad = true;
      }
      if (emptyCell(row.FACT_VALUE) || emptyCell(row.CONTEST_CODE) || emptyCell(row.TOURNAMENT_CODE)) {
        bad = true;
      }
      if (bad) errors += 1;
    });
    return {
      loaded: true,
      total: total,
      errors: errors,
      forCsv: Math.max(0, total - errors),
    };
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

      var factRaw = parseNumberFromComma(raw[colFact]);
      var factNum = applyFactOperation(factRaw, tournament.fact_op, tournament.fact_op_value);
      var periodCode = normalizePeriodCode(tournament.period_code);
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
        PERIOD_CODE: periodCode,
        PERIOD: periodDisplay(periodCode),
        FIO: fio,
        "ТАБЕЛЬНЫЙ НЕ НАЙДЕН": notFound,
        PLAN_VALUE_число: planNum,
        FACT_VALUE_число: factNum,
        ДУБЛЬ: cfg.noDuplicateMark,
        "СУММА ПО ТАБЕЛЬНОМУ": cfg.noDuplicateMark,
        "В ФОРМАТЕ": cfg.noDuplicateMark,
        duplicate_comment: "",
        CSV_ERROR: cfg.noDuplicateMark,
        include_in_csv: true,
      });
    });

    return { rows: rows, missingFio: missingFio };
  }

  function duplicateKey(row) {
    return [row.CONTEST_CODE, row.TOURNAMENT_CODE, row.MANAGER_PERSON_NUMBER].join("|");
  }

  function duplicateKeyFio(row) {
    return [row.CONTEST_CODE, row.TOURNAMENT_CODE, normalizeFioKey(row.FIO)].join("|");
  }

  function isFioDataRow(row) {
    if (!row) return false;
    var fio = String(row.FIO == null ? "" : row.FIO).trim();
    return fio !== "" && fio !== "-";
  }

  /** Отпечаток строки для сохранения/восстановления решения по дублю. */
  function rowFingerprint(row) {
    if (!row) return "";
    return [
      row.tournament_id || "",
      row.source_index != null ? String(row.source_index) : "",
      row.MANAGER_PERSON_NUMBER || "",
      normalizeFioKey(row.FIO),
      row.FACT_VALUE || "",
      row.CONTEST_CODE || "",
      row.TOURNAMENT_CODE || "",
    ].join("|");
  }

  /**
   * Восстановить индексы keep_* из сохранённого решения по отпечаткам/индексам.
   * @returns {number[]}
   */
  function resolveKeepIndices(group, resolution) {
    if (!group || !resolution) return [];
    var indices = group.indices || [];
    var rows = group.rows || [];
    if (resolution.mode === "keep_one" && resolution.keepFingerprint) {
      for (var i = 0; i < rows.length; i++) {
        if (rowFingerprint(rows[i]) === resolution.keepFingerprint) {
          return [indices[i]];
        }
      }
    }
    if (resolution.mode === "keep_one" && resolution.keepIndex != null) {
      if (indices.indexOf(resolution.keepIndex) >= 0) {
        return [resolution.keepIndex];
      }
    }
    if (
      (resolution.mode === "keep_selected" || resolution.mode === "keep_one") &&
      resolution.keepFingerprints &&
      resolution.keepFingerprints.length
    ) {
      var out = [];
      var wanted = Object.create(null);
      resolution.keepFingerprints.forEach(function (fp) {
        wanted[fp] = true;
      });
      rows.forEach(function (r, ri) {
        if (wanted[rowFingerprint(r)]) {
          out.push(indices[ri]);
        }
      });
      return out;
    }
    if (resolution.mode === "keep_selected" && resolution.keepIndices && resolution.keepIndices.length) {
      return resolution.keepIndices.filter(function (idx) {
        return indices.indexOf(idx) >= 0;
      });
    }
    return [];
  }

  /** Краткое описание сохранённого решения для UI. */
  function describeResolution(resolution) {
    if (!resolution || !resolution.mode) return "";
    if (resolution.mode === "sum") return "ранее: сумма в одну строку";
    if (resolution.mode === "drop_all") return "ранее: убрать все из CSV";
    if (resolution.mode === "keep_one") return "ранее: оставлена одна строка";
    if (resolution.mode === "keep_selected") {
      var n = (resolution.keepFingerprints || resolution.keepIndices || []).length;
      return "ранее: оставлены отмеченные (" + (n || "?") + ")";
    }
    return "ранее: " + resolution.mode;
  }

  /**
   * Группы дублей. keyFn — функция ключа; optional filterFn — какие строки учитывать.
   */
  function findDuplicateGroups(rows, keyFn, filterFn) {
    var keyOf = keyFn || duplicateKey;
    var groups = Object.create(null);
    (rows || []).forEach(function (row, idx) {
      if (filterFn && !filterFn(row)) {
        return;
      }
      var key = keyOf(row);
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

  function findFioDuplicateGroups(rows) {
    return findDuplicateGroups(rows, duplicateKeyFio, isFioDataRow);
  }

  function findTnDuplicateGroups(rows) {
    return findDuplicateGroups(
      rows,
      duplicateKey,
      function (row) {
        return row.include_in_csv !== false;
      }
    );
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
   * resolution: { mode: 'abort'|'drop_all'|'keep_one'|'keep_selected'|'sum', keepIndex?, keepIndices? }
   * keyFn — какой ключ дубля использовать при поиске групп.
   * filterFn — фильтр строк для группировки.
   */
  function applyDuplicateResolutions(rows, resolutionsByKey, options, keyFn, filterFn) {
    var cfg = mergeDefaults(options);
    var keyOf = keyFn || duplicateKey;
    var list = (rows || []).map(function (r) {
      return Object.assign({}, r);
    });
    if (keyOf === duplicateKey || !keyFn) {
      annotateDuplicatesLikePq(list, cfg);
    }

    var groups = findDuplicateGroups(list, keyOf, filterFn);
    for (var gi = 0; gi < groups.length; gi++) {
      var g = groups[gi];
      var res = resolutionsByKey && resolutionsByKey[g.key];
      if (!res || !res.mode) {
        return { ok: false, error: "Нет решения для дубля: " + g.key, rows: list, pendingKey: g.key };
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
          list[idx].duplicate_comment =
            (list[idx].duplicate_comment ? list[idx].duplicate_comment + " | " : "") +
            commentBase +
            "; решение: убрать все дубли из CSV";
        });
        continue;
      }

      if (res.mode === "keep_one") {
        var keepIdx = res.keepIndex;
        if ((keepIdx == null || indices.indexOf(keepIdx) < 0) && res.keepFingerprint) {
          var resolvedOne = resolveKeepIndices(
            { indices: indices, rows: indices.map(function (i) { return list[i]; }) },
            res
          );
          keepIdx = resolvedOne[0];
        }
        if (keepIdx == null || indices.indexOf(keepIdx) < 0) {
          return { ok: false, error: "Не выбран ряд для keep_one: " + g.key, rows: list };
        }
        // дополняем отпечаток для повторного показа
        res.keepFingerprint = res.keepFingerprint || rowFingerprint(list[keepIdx]);
        res.keepFingerprints = [res.keepFingerprint];
        indices.forEach(function (idx) {
          if (idx === keepIdx) {
            list[idx].include_in_csv = true;
            list[idx].duplicate_comment =
              (list[idx].duplicate_comment ? list[idx].duplicate_comment + " | " : "") +
              commentBase +
              "; решение: оставлена эта строка";
          } else {
            list[idx].include_in_csv = false;
            list[idx].duplicate_comment =
              (list[idx].duplicate_comment ? list[idx].duplicate_comment + " | " : "") +
              commentBase +
              "; решение: исключена (выбрана другая строка)";
          }
        });
        continue;
      }

      if (res.mode === "keep_selected") {
        var keepSet = Object.create(null);
        var resolvedKeep = resolveKeepIndices(
          { indices: indices, rows: indices.map(function (i) { return list[i]; }) },
          res
        );
        if (!resolvedKeep.length && res.keepIndices) {
          resolvedKeep = res.keepIndices;
        }
        resolvedKeep.forEach(function (idx) {
          keepSet[idx] = true;
        });
        var any = indices.some(function (idx) {
          return keepSet[idx];
        });
        if (!any) {
          return { ok: false, error: "Не отмечены строки для keep_selected: " + g.key, rows: list };
        }
        res.keepIndices = resolvedKeep;
        res.keepFingerprints = resolvedKeep.map(function (idx) {
          return rowFingerprint(list[idx]);
        });
        indices.forEach(function (idx) {
          if (keepSet[idx]) {
            list[idx].include_in_csv = true;
            list[idx].duplicate_comment =
              (list[idx].duplicate_comment ? list[idx].duplicate_comment + " | " : "") +
              commentBase +
              "; решение: оставлена (множественный выбор)";
          } else {
            list[idx].include_in_csv = false;
            list[idx].duplicate_comment =
              (list[idx].duplicate_comment ? list[idx].duplicate_comment + " | " : "") +
              commentBase +
              "; решение: исключена (не отмечена)";
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
        list[first].duplicate_comment =
          (list[first].duplicate_comment ? list[first].duplicate_comment + " | " : "") +
          commentBase +
          "; решение: сумма показателей в одну строку";
        indices.slice(1).forEach(function (idx) {
          list[idx].include_in_csv = false;
          list[idx].duplicate_comment =
            (list[idx].duplicate_comment ? list[idx].duplicate_comment + " | " : "") +
            commentBase +
            "; решение: поглощена суммой в первую строку группы";
        });
        continue;
      }

      return { ok: false, error: "Неизвестный режим дубля: " + res.mode, rows: list };
    }

    return { ok: true, rows: list };
  }

  /**
   * Сводка после проверок: сколько строк останется в CSV и т.п.
   */
  function summarizeCheckedRows(rows) {
    var list = rows || [];
    var included = list.filter(function (r) {
      return r.include_in_csv !== false;
    });
    var excluded = list.length - included.length;
    var missingFlag = list.filter(function (r) {
      return r["ТАБЕЛЬНЫЙ НЕ НАЙДЕН"] === "ДА" && r.include_in_csv !== false;
    }).length;
    return {
      total: list.length,
      included: included.length,
      excluded: excluded,
      missingFioFlag: missingFlag,
      csvRows: rowsForCsv(list),
      xlsxRows: rowsForXlsx(list),
    };
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
    "PERIOD",
    "PERIOD_CODE",
    "ДУБЛЬ",
    "СУММА ПО ТАБЕЛЬНОМУ",
    "В ФОРМАТЕ",
    "PLAN_VALUE_число",
    "FACT_VALUE_число",
    "FIO",
    "ТАБЕЛЬНЫЙ НЕ НАЙДЕН",
    "КОММЕНТАРИЙ_ДУБЛЬ",
    "CSV_ERROR",
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
   * Проверки перед выгрузкой CSV.
   * Блокирует: дубли ключа, ТН не из 20 цифр, пустые ячейки CSV-колонок.
   */
  function validateCsvExportRows(rows, options) {
    var cfg = mergeDefaults(options);
    var list = (rows || []).filter(function (r) {
      return r.include_in_csv !== false;
    });
    var keyCounts = Object.create(null);
    list.forEach(function (r) {
      var key = duplicateKey(r);
      keyCounts[key] = (keyCounts[key] || 0) + 1;
    });

    var byTournament = Object.create(null);
    function bump(tCode, field) {
      var k = tCode || "(без кода)";
      if (!byTournament[k]) {
        byTournament[k] = { tournament_code: k, duplicates: 0, bad_person: 0, empty_cells: 0 };
      }
      byTournament[k][field] += 1;
    }

    var duplicateKeys = [];
    var badPerson = [];
    var emptyCells = [];
    var marked = (rows || []).map(function (r) {
      var copy = Object.assign({}, r);
      var errs = [];
      if (copy.include_in_csv === false) {
        copy.CSV_ERROR = cfg.noDuplicateMark;
        return copy;
      }
      var key = duplicateKey(copy);
      if (keyCounts[key] > 1) {
        errs.push("дубль " + key);
        bump(copy.TOURNAMENT_CODE, "duplicates");
        if (duplicateKeys.indexOf(key) < 0) {
          duplicateKeys.push(key);
        }
      }
      if (!isValidPersonNumber20(copy.MANAGER_PERSON_NUMBER, cfg.personNumberLength)) {
        errs.push("ТН не " + cfg.personNumberLength + " цифр");
        bump(copy.TOURNAMENT_CODE, "bad_person");
        badPerson.push({
          tournament_code: copy.TOURNAMENT_CODE,
          contest_code: copy.CONTEST_CODE,
          value: copy.MANAGER_PERSON_NUMBER,
        });
      }
      CSV_COLUMNS.forEach(function (c) {
        var v = copy[c];
        if (v == null || String(v).trim() === "" || String(v).toLowerCase() === "null") {
          errs.push("пустое " + c);
          bump(copy.TOURNAMENT_CODE, "empty_cells");
          emptyCells.push({
            tournament_code: copy.TOURNAMENT_CODE,
            column: c,
          });
        }
      });
      copy.CSV_ERROR = errs.length ? errs.join("; ") : cfg.noDuplicateMark;
      return copy;
    });

    var tournamentSummaries = Object.keys(byTournament).map(function (k) {
      return byTournament[k];
    });
    var ok = duplicateKeys.length === 0 && badPerson.length === 0 && emptyCells.length === 0;
    var parts = [];
    if (duplicateKeys.length) {
      parts.push("дубли ключа: " + duplicateKeys.length);
    }
    if (badPerson.length) {
      parts.push("ТН не " + cfg.personNumberLength + " цифр: " + badPerson.length);
    }
    if (emptyCells.length) {
      parts.push("пустые ячейки: " + emptyCells.length);
    }

    return {
      ok: ok,
      rowsMarked: marked,
      duplicateKeys: duplicateKeys,
      badPerson: badPerson,
      emptyCells: emptyCells,
      byTournament: tournamentSummaries,
      message: ok
        ? "CSV готов"
        : "CSV заблокирован (" +
          parts.join("; ") +
          "). Для дублей оставьте одну строку, сумму или исключите лишние.",
    };
  }

  function tournamentIncluded(t) {
    return !t || t.include_in_report !== false;
  }

  function includedTournaments(tournaments) {
    return (tournaments || []).filter(tournamentIncluded);
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

  function todayIsoDate() {
    var d = new Date();
    var y = d.getFullYear();
    var m = d.getMonth() + 1;
    var day = d.getDate();
    return y + "-" + (m < 10 ? "0" : "") + m + "-" + (day < 10 ? "0" : "") + day;
  }

  function createEmptyTournament(partial) {
    var t = {
      id: "t_" + Date.now() + "_" + Math.floor(Math.random() * 10000),
      contest_code: "",
      tournament_code: "",
      plan_value: "0",
      contest_date: todayIsoDate(),
      full_name: "",
      type_ind: "TN",
      period_code: "Y",
      column_id: "",
      column_fact: "",
      fact_op: "none",
      fact_op_value: "1",
      source_file_name: "",
      sheet_name: "",
      table_start_row: 1,
      table_start_col: 1,
      include_in_report: true,
      source_error: "",
      needs_identity_fix: false,
      copy_lock_code: "",
      copy_lock_name: "",
    };
    if (partial && typeof partial === "object") {
      Object.keys(partial).forEach(function (k) {
        t[k] = partial[k];
      });
    }
    t.period_code = normalizePeriodCode(t.period_code);
    if (t.include_in_report === undefined) {
      t.include_in_report = true;
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
      period_code: src.period_code,
      column_id: src.column_id,
      column_fact: src.column_fact,
      fact_op: src.fact_op || "none",
      fact_op_value: src.fact_op_value != null ? src.fact_op_value : "1",
      source_file_name: src.source_file_name,
      sheet_name: src.sheet_name,
      table_start_row: src.table_start_row || 1,
      table_start_col: src.table_start_col || 1,
      include_in_report: src.include_in_report !== false,
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
      String(t.type_ind || "").trim() &&
      String(t.period_code || "").trim()
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
    if (!code || !name) {
      return false;
    }
    // разблокировка: оба поля изменены относительно значений при копировании
    return code !== lockCode && name !== lockName;
  }

  function tournamentSourceOk(t, dataPack) {
    if (!t || !dataPack || !dataPack.rows || !dataPack.rows.length) {
      return false;
    }
    if (t.source_error) {
      return false;
    }
    var cols = dataPack.columns || [];
    var idOk = String(t.column_id || "").trim() && cols.indexOf(t.column_id) >= 0;
    var factOk = String(t.column_fact || "").trim() && cols.indexOf(t.column_fact) >= 0;
    return !!(idOk && factOk);
  }

  /**
   * Стадии готовности для шапки (только турниры с include_in_report).
   */
  function computeStages(tournaments, dataById, fioEntries, checkState) {
    var list = includedTournaments(tournaments);
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
    var fioDupGroups = [];
    if (sourcesOk) {
      var allRows = [];
      list.forEach(function (t) {
        var pack = data[t.id];
        var norm = normalizeTournamentRows(pack.rows, t, fioMap, {});
        allRows = allRows.concat(norm.rows);
      });
      annotateDuplicatesLikePq(allRows, {});
      fioDupGroups = findFioDuplicateGroups(allRows);
      dupGroups = findTnDuplicateGroups(allRows);
      var fioDupOk = fioDupGroups.length === 0 || !!(checkState && checkState.fioDupCleared);
      var tnDupOk = dupGroups.length === 0 || !!(checkState && checkState.duplicatesCleared);
      dupOk = fioDupOk && tnDupOk;
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
      fioDuplicateGroups: fioDupGroups,
      blockedCopies: blockedCopies,
      canProcess: hasTournaments && fieldsFilled && sourcesOk && fioOk && dupOk && blockedCopies.length === 0,
      canCheck: hasTournaments && fieldsFilled && sourcesOk && blockedCopies.length === 0,
    };
  }

  function serializeSettings(tournaments, fioMeta) {
    var fio = fioMeta || {};
    return {
      version: 3,
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
          period_code: normalizePeriodCode(t.period_code),
          column_id: t.column_id || "",
          column_fact: t.column_fact || "",
          fact_op: t.fact_op || "none",
          fact_op_value: t.fact_op_value != null ? String(t.fact_op_value) : "1",
          source_file_name: t.source_file_name || "",
          source_file_kind: t.source_file_kind || "",
          sheet_name: t.sheet_name || "",
          table_start_row: t.table_start_row || 1,
          table_start_col: t.table_start_col || 1,
          include_in_report: t.include_in_report !== false,
          needs_identity_fix: !!t.needs_identity_fix,
          copy_lock_code: t.copy_lock_code || "",
          copy_lock_name: t.copy_lock_name || "",
        };
      }),
      fio: {
        entries: Array.isArray(fio.entries)
          ? fio.entries
              .map(function (e) {
                return {
                  fio: String((e && e.fio) || "").trim(),
                  person_number: String((e && e.person_number) || "").trim(),
                };
              })
              .filter(function (e) {
                return e.fio && e.person_number;
              })
          : [],
        file_name: fio.file_name || "",
        sheet_name: fio.sheet_name || "",
        start_row: fio.start_row || 1,
        start_col: fio.start_col || 1,
        col_fio: fio.col_fio || "",
        col_tn: fio.col_tn || "",
      },
    };
  }

  function parseSettings(json) {
    var data = typeof json === "string" ? JSON.parse(json) : json;
    if (!data || !Array.isArray(data.tournaments)) {
      throw new Error("Неверный JSON настроек: нужен массив tournaments");
    }
    return {
      tournaments: data.tournaments.map(function (t) {
        var copy = Object.assign({}, t || {});
        // содержимое файлов в JSON не храним (игнорируем устаревшие base64)
        delete copy.source_file_b64;
        return createEmptyTournament(copy);
      }),
      fio: data.fio || null,
      version: data.version || 1,
    };
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
    PERIOD_OPTIONS: PERIOD_OPTIONS,
    mergeDefaults: mergeDefaults,
    parseNumberFromComma: parseNumberFromComma,
    formatNumberDot: formatNumberDot,
    formatNumberComma: formatNumberComma,
    padPersonNumber: padPersonNumber,
    normalizeFioKey: normalizeFioKey,
    buildFioMap: buildFioMap,
    isNumericPersonToken: isNumericPersonToken,
    resolveFioTableEntries: resolveFioTableEntries,
    tournamentRowStats: tournamentRowStats,
    normalizePeriodCode: normalizePeriodCode,
    periodLabel: periodLabel,
    periodDisplay: periodDisplay,
    periodBadgesForTournaments: periodBadgesForTournaments,
    applyFactOperation: applyFactOperation,
    isValidPersonNumber20: isValidPersonNumber20,
    normalizeTournamentRows: normalizeTournamentRows,
    duplicateKey: duplicateKey,
    duplicateKeyFio: duplicateKeyFio,
    isFioDataRow: isFioDataRow,
    rowFingerprint: rowFingerprint,
    resolveKeepIndices: resolveKeepIndices,
    describeResolution: describeResolution,
    findDuplicateGroups: findDuplicateGroups,
    findFioDuplicateGroups: findFioDuplicateGroups,
    findTnDuplicateGroups: findTnDuplicateGroups,
    annotateDuplicatesLikePq: annotateDuplicatesLikePq,
    applyDuplicateResolutions: applyDuplicateResolutions,
    rowsForCsv: rowsForCsv,
    rowsForXlsx: rowsForXlsx,
    validateCsvExportRows: validateCsvExportRows,
    summarizeCheckedRows: summarizeCheckedRows,
    processAll: processAll,
    createEmptyTournament: createEmptyTournament,
    todayIsoDate: todayIsoDate,
    cloneTournament: cloneTournament,
    tournamentFieldsOk: tournamentFieldsOk,
    tournamentIdentityUnlocked: tournamentIdentityUnlocked,
    tournamentSourceOk: tournamentSourceOk,
    tournamentIncluded: tournamentIncluded,
    includedTournaments: includedTournaments,
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
