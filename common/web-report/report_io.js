/**
 * Загрузка CSV/Excel и выгрузка результатов (браузер).
 * Зависит от ReportCore и глобального XLSX (SheetJS).
 */
(function (root) {
  "use strict";

  function getConfig() {
    return root.ReportConfig || {
      csv_delimiter: ";",
      csv_encodings: ["utf-8", "windows-1251", "ibm866"],
      person_number_length: 20,
      number_decimals: 5,
      priority_type: "1",
      missing_person_placeholder: "00000000",
      no_duplicate_mark: "-",
      missing_fio_flag: "ДА",
    };
  }

  function coreOptions() {
    var c = getConfig();
    return {
      personNumberLength: c.person_number_length,
      numberDecimals: c.number_decimals,
      priorityType: c.priority_type,
      missingPersonPlaceholder: c.missing_person_placeholder,
      noDuplicateMark: c.no_duplicate_mark,
      missingFioFlag: c.missing_fio_flag,
    };
  }

  function scoreDecodedText(text) {
    if (!text) {
      return -1e9;
    }
    var score = 0;
    var replacement = (text.match(/\uFFFD/g) || []).length;
    score -= replacement * 50;
    // Кириллица
    var cyr = (text.match(/[А-Яа-яЁё]/g) || []).length;
    score += cyr * 2;
    // Типичные заголовки
    if (/ТАБЕЛЬНЫЙ|ФИО|ПОКАЗАТЕЛЬ|MANAGER|CONTEST/i.test(text)) {
      score += 80;
    }
    if (text.indexOf(";") >= 0) {
      score += 20;
    }
    return score;
  }

  function decodeBuffer(buffer, encoding) {
    try {
      var decoder = new TextDecoder(encoding, { fatal: false });
      return decoder.decode(buffer);
    } catch (err) {
      return null;
    }
  }

  /**
   * Определение кодировки CSV и разбор.
   * @returns {{ rows: object[], columns: string[], encoding: string, text: string }}
   */
  function parseCsvBuffer(arrayBuffer, delimiter) {
    var delim = delimiter || getConfig().csv_delimiter || ";";
    var encodings = getConfig().csv_encodings || ["utf-8", "windows-1251", "ibm866"];
    var best = null;
    encodings.forEach(function (enc) {
      var text = decodeBuffer(arrayBuffer, enc);
      if (text == null) {
        return;
      }
      // UTF-8 BOM
      if (text.charCodeAt(0) === 0xfeff) {
        text = text.slice(1);
      }
      var sc = scoreDecodedText(text);
      if (!best || sc > best.score) {
        best = { encoding: enc, text: text, score: sc };
      }
    });
    if (!best) {
      throw new Error("Не удалось декодировать CSV");
    }
    var parsed = parseCsvText(best.text, delim);
    parsed.encoding = best.encoding;
    parsed.text = best.text;
    return parsed;
  }

  function parseCsvText(text, delimiter) {
    var delim = delimiter || ";";
    var lines = String(text).replace(/\r\n/g, "\n").replace(/\r/g, "\n").split("\n");
    while (lines.length && String(lines[lines.length - 1]).trim() === "") {
      lines.pop();
    }
    if (!lines.length) {
      return { rows: [], columns: [] };
    }
    var columns = splitCsvLine(lines[0], delim).map(function (c) {
      return String(c).trim();
    });
    var rows = [];
    for (var i = 1; i < lines.length; i++) {
      if (String(lines[i]).trim() === "") {
        continue;
      }
      var cells = splitCsvLine(lines[i], delim);
      var row = {};
      columns.forEach(function (col, idx) {
        row[col] = cells[idx] != null ? cells[idx] : "";
      });
      rows.push(row);
    }
    return { rows: rows, columns: columns };
  }

  function splitCsvLine(line, delimiter) {
    var result = [];
    var cur = "";
    var inQuotes = false;
    for (var i = 0; i < line.length; i++) {
      var ch = line[i];
      if (ch === '"') {
        if (inQuotes && line[i + 1] === '"') {
          cur += '"';
          i++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (ch === delimiter && !inQuotes) {
        result.push(cur);
        cur = "";
      } else {
        cur += ch;
      }
    }
    result.push(cur);
    return result;
  }

  function aoaToTable(aoa) {
    if (!aoa || !aoa.length) {
      return { rows: [], columns: [] };
    }
    var columns = (aoa[0] || []).map(function (c) {
      return String(c == null ? "" : c).trim();
    });
    var rows = [];
    for (var r = 1; r < aoa.length; r++) {
      var line = aoa[r] || [];
      var empty = true;
      var row = {};
      columns.forEach(function (col, idx) {
        var v = line[idx];
        if (v != null && String(v).trim() !== "") {
          empty = false;
        }
        row[col] = v == null ? "" : v;
      });
      if (!empty) {
        rows.push(row);
      }
    }
    return { rows: rows, columns: columns };
  }

  function sheetToTable(sheet) {
    var aoa = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: "", raw: false });
    return aoaToTable(aoa);
  }

  /** Разбор Excel: все листы. */
  function parseExcelWorkbook(arrayBuffer) {
    if (typeof XLSX === "undefined") {
      throw new Error("Библиотека XLSX не загружена");
    }
    var wb = XLSX.read(arrayBuffer, { type: "array", cellDates: false, raw: false });
    var sheetNames = wb.SheetNames || [];
    var sheets = {};
    sheetNames.forEach(function (name) {
      sheets[name] = sheetToTable(wb.Sheets[name]);
    });
    var first = sheetNames[0] || "";
    var table = first ? sheets[first] : { rows: [], columns: [] };
    return {
      sheetNames: sheetNames,
      sheets: sheets,
      sheetName: first,
      rows: table.rows,
      columns: table.columns,
    };
  }

  function parseExcelArrayBuffer(arrayBuffer, sheetName) {
    var book = parseExcelWorkbook(arrayBuffer);
    if (sheetName && book.sheets[sheetName]) {
      var t = book.sheets[sheetName];
      return {
        rows: t.rows,
        columns: t.columns,
        sheetName: sheetName,
        sheetNames: book.sheetNames,
        sheets: book.sheets,
      };
    }
    return book;
  }

  async function readTableFile(file) {
    var name = (file && file.name) || "";
    var lower = name.toLowerCase();
    var buffer = await file.arrayBuffer();
    if (lower.endsWith(".csv") || lower.endsWith(".txt")) {
      var csv = parseCsvBuffer(buffer);
      return {
        kind: "csv",
        rows: csv.rows,
        columns: csv.columns,
        encoding: csv.encoding,
        fileName: name,
        sheetNames: [],
        sheets: null,
        sheetName: "",
      };
    }
    if (lower.endsWith(".xlsx") || lower.endsWith(".xls") || lower.endsWith(".xlsm")) {
      var xls = parseExcelWorkbook(buffer);
      return {
        kind: "excel",
        rows: xls.rows,
        columns: xls.columns,
        sheetName: xls.sheetName,
        sheetNames: xls.sheetNames,
        sheets: xls.sheets,
        encoding: "binary",
        fileName: name,
        buffer: buffer,
      };
    }
    throw new Error("Поддерживаются CSV и Excel (.xlsx/.xls)");
  }

  function pickSheetFromPack(pack, sheetName) {
    if (!pack || pack.kind !== "excel" || !pack.sheets) {
      return pack;
    }
    var name = sheetName || pack.sheetName || (pack.sheetNames && pack.sheetNames[0]) || "";
    var table = pack.sheets[name] || { rows: [], columns: [] };
    return Object.assign({}, pack, {
      sheetName: name,
      rows: table.rows,
      columns: table.columns,
    });
  }

  function downloadBlob(filename, blob) {
    var url = URL.createObjectURL(blob);
    var a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(function () {
      URL.revokeObjectURL(url);
    }, 1500);
  }

  function downloadText(filename, text, mime) {
    downloadBlob(filename, new Blob([text], { type: mime || "text/plain;charset=utf-8" }));
  }

  function downloadJson(filename, obj) {
    downloadText(filename, JSON.stringify(obj, null, 2), "application/json;charset=utf-8");
  }

  function escapeCsvCell(value, delimiter) {
    var s = value == null ? "" : String(value);
    if (s.indexOf('"') >= 0 || s.indexOf(delimiter) >= 0 || s.indexOf("\n") >= 0 || s.indexOf("\r") >= 0) {
      return '"' + s.replace(/"/g, '""') + '"';
    }
    return s;
  }

  function buildCsvContent(rows, columns, delimiter) {
    var delim = delimiter || getConfig().csv_delimiter || ";";
    var cols = columns || ReportCore.CSV_COLUMNS;
    var lines = [cols.join(delim)];
    (rows || []).forEach(function (row) {
      lines.push(
        cols
          .map(function (c) {
            return escapeCsvCell(row[c], delim);
          })
          .join(delim)
      );
    });
    // BOM для Excel
    return "\uFEFF" + lines.join("\r\n");
  }

  function downloadReportCsv(rows, filename) {
    var content = buildCsvContent(rows, ReportCore.CSV_COLUMNS);
    downloadText(filename || timestampName("report", "csv"), content, "text/csv;charset=utf-8");
  }

  function autoColWidths(aoa) {
    if (!aoa || !aoa.length) {
      return [];
    }
    var colCount = aoa[0].length;
    var widths = [];
    for (var c = 0; c < colCount; c++) {
      var maxLen = 8;
      for (var r = 0; r < aoa.length; r++) {
        var cell = aoa[r][c];
        var len = String(cell == null ? "" : cell).length;
        if (len > maxLen) {
          maxLen = len;
        }
      }
      widths.push({ wch: Math.min(42, Math.max(10, maxLen + 2)) });
    }
    return widths;
  }

  function downloadReportXlsx(rows, filename) {
    if (typeof XLSX === "undefined") {
      throw new Error("Библиотека XLSX не загружена");
    }
    var cols = ReportCore.XLSX_COLUMNS;
    var aoa = [cols];
    (rows || []).forEach(function (row) {
      aoa.push(
        cols.map(function (c) {
          return row[c] == null ? "" : row[c];
        })
      );
    });
    var ws = XLSX.utils.aoa_to_sheet(aoa);
    var lastRow = Math.max(aoa.length, 1);
    var lastCol = Math.max(cols.length, 1);
    var range = XLSX.utils.encode_range({
      s: { r: 0, c: 0 },
      e: { r: lastRow - 1, c: lastCol - 1 },
    });
    ws["!ref"] = range;
    ws["!autofilter"] = { ref: range };
    ws["!freeze"] = {
      xSplit: 0,
      ySplit: 1,
      topLeftCell: "A2",
      activePane: "bottomLeft",
      state: "frozen",
    };
    ws["!cols"] = autoColWidths(aoa);
    var wb = XLSX.utils.book_new();
    if (!wb.Workbook) {
      wb.Workbook = {};
    }
    if (!wb.Workbook.Views) {
      wb.Workbook.Views = [{}];
    }
    wb.Workbook.Views[0].ySplit = 1;
    XLSX.utils.book_append_sheet(wb, ws, "REPORT");
    var out = XLSX.write(wb, { bookType: "xlsx", type: "array" });
    downloadBlob(
      filename || timestampName("report", "xlsx"),
      new Blob([out], {
        type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      })
    );
  }

  function timestampName(prefix, ext) {
    var d = new Date();
    function p(n) {
      return n < 10 ? "0" + n : String(n);
    }
    var stamp =
      d.getFullYear() +
      p(d.getMonth() + 1) +
      p(d.getDate()) +
      "_" +
      p(d.getHours()) +
      p(d.getMinutes());
    return prefix + "_" + stamp + "." + ext;
  }

  function guessIdColumn(columns, typeInd) {
    var mode = String(typeInd || "TN").toUpperCase();
    var list = columns || [];
    var patterns =
      mode === "FIO"
        ? [/фио/i, /клиентск/i, /менеджер/i, /fio/i, /name/i]
        : [/табель/i, /person/i, /таб\.?\s*н/i, /код\s*км/i, /number/i];
    for (var p = 0; p < patterns.length; p++) {
      for (var i = 0; i < list.length; i++) {
        if (patterns[p].test(list[i])) {
          return list[i];
        }
      }
    }
    return list[0] || "";
  }

  function guessFactColumn(columns) {
    var list = columns || [];
    var patterns = [/показател/i, /fact/i, /факт/i, /value/i, /сумм/i];
    for (var p = 0; p < patterns.length; p++) {
      for (var i = 0; i < list.length; i++) {
        if (patterns[p].test(list[i])) {
          return list[i];
        }
      }
    }
    return list.length > 1 ? list[1] : list[0] || "";
  }

  root.ReportIO = {
    getConfig: getConfig,
    coreOptions: coreOptions,
    parseCsvBuffer: parseCsvBuffer,
    parseCsvText: parseCsvText,
    parseExcelArrayBuffer: parseExcelArrayBuffer,
    parseExcelWorkbook: parseExcelWorkbook,
    pickSheetFromPack: pickSheetFromPack,
    readTableFile: readTableFile,
    downloadBlob: downloadBlob,
    downloadText: downloadText,
    downloadJson: downloadJson,
    buildCsvContent: buildCsvContent,
    downloadReportCsv: downloadReportCsv,
    downloadReportXlsx: downloadReportXlsx,
    autoColWidths: autoColWidths,
    timestampName: timestampName,
    guessIdColumn: guessIdColumn,
    guessFactColumn: guessFactColumn,
  };
})(typeof globalThis !== "undefined" ? globalThis : this);
