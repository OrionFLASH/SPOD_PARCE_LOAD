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

  function parseCsvText(text, delimiter) {
    var delim = delimiter || ";";
    var lines = String(text).replace(/\r\n/g, "\n").replace(/\r/g, "\n").split("\n");
    while (lines.length && String(lines[lines.length - 1]).trim() === "") {
      lines.pop();
    }
    var aoa = lines.map(function (line) {
      return splitCsvLine(line, delim);
    });
    return { aoa: aoa, text: text };
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

  /**
   * Вырезает таблицу из AOA: startRow/startCol — 1-based (левый верхний угол заголовка).
   */
  function sliceAoaOrigin(aoa, startRow, startCol) {
    var r0 = Math.max(0, (Number(startRow) || 1) - 1);
    var c0 = Math.max(0, (Number(startCol) || 1) - 1);
    var out = [];
    for (var r = r0; r < (aoa || []).length; r++) {
      var line = aoa[r] || [];
      out.push(line.slice(c0));
    }
    return out;
  }

  function aoaToTable(aoa) {
    if (!aoa || !aoa.length) {
      return { rows: [], columns: [] };
    }
    var columns = (aoa[0] || []).map(function (c) {
      return String(c == null ? "" : c).trim();
    });
    // пустые имена колонок — ColN
    columns = columns.map(function (c, idx) {
      return c || "Col" + (idx + 1);
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

  function tableFromAoaOrigin(aoa, startRow, startCol) {
    return aoaToTable(sliceAoaOrigin(aoa, startRow, startCol));
  }

  function sheetToAoa(sheet) {
    return XLSX.utils.sheet_to_json(sheet, { header: 1, defval: "", raw: false });
  }

  /** Разбор Excel: все листы как AOA + таблица с угла 1,1. */
  function parseExcelWorkbook(arrayBuffer, startRow, startCol) {
    if (typeof XLSX === "undefined") {
      throw new Error("Библиотека XLSX не загружена");
    }
    var sr = startRow == null ? 1 : startRow;
    var sc = startCol == null ? 1 : startCol;
    var wb = XLSX.read(arrayBuffer, { type: "array", cellDates: false, raw: false });
    var sheetNames = wb.SheetNames || [];
    var sheetsAoa = {};
    var sheets = {};
    sheetNames.forEach(function (name) {
      var aoa = sheetToAoa(wb.Sheets[name]);
      sheetsAoa[name] = aoa;
      sheets[name] = tableFromAoaOrigin(aoa, sr, sc);
    });
    var first = sheetNames[0] || "";
    var table = first ? sheets[first] : { rows: [], columns: [] };
    return {
      sheetNames: sheetNames,
      sheetsAoa: sheetsAoa,
      sheets: sheets,
      sheetName: first,
      rawAoa: first ? sheetsAoa[first] : [],
      rows: table.rows,
      columns: table.columns,
      start_row: sr,
      start_col: sc,
    };
  }

  function parseExcelArrayBuffer(arrayBuffer, sheetName, startRow, startCol) {
    var book = parseExcelWorkbook(arrayBuffer, startRow, startCol);
    if (sheetName && book.sheetsAoa[sheetName]) {
      var table = tableFromAoaOrigin(book.sheetsAoa[sheetName], startRow, startCol);
      return Object.assign({}, book, {
        sheetName: sheetName,
        rawAoa: book.sheetsAoa[sheetName],
        rows: table.rows,
        columns: table.columns,
        start_row: startRow == null ? 1 : startRow,
        start_col: startCol == null ? 1 : startCol,
      });
    }
    return book;
  }

  function parseCsvBuffer(arrayBuffer, delimiter, startRow, startCol) {
    var delim = delimiter || getConfig().csv_delimiter || ";";
    var encodings = getConfig().csv_encodings || ["utf-8", "windows-1251", "ibm866"];
    var best = null;
    encodings.forEach(function (enc) {
      var text = decodeBuffer(arrayBuffer, enc);
      if (text == null) {
        return;
      }
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
    var sr = startRow == null ? 1 : startRow;
    var sc = startCol == null ? 1 : startCol;
    var table = tableFromAoaOrigin(parsed.aoa, sr, sc);
    return {
      rows: table.rows,
      columns: table.columns,
      encoding: best.encoding,
      text: best.text,
      rawAoa: parsed.aoa,
      start_row: sr,
      start_col: sc,
    };
  }

  async function readTableFile(file, startRow, startCol) {
    var name = (file && file.name) || "";
    var lower = name.toLowerCase();
    var buffer = await file.arrayBuffer();
    var sr = startRow == null ? 1 : Number(startRow) || 1;
    var sc = startCol == null ? 1 : Number(startCol) || 1;
    if (lower.endsWith(".csv") || lower.endsWith(".txt")) {
      var csv = parseCsvBuffer(buffer, null, sr, sc);
      return {
        kind: "csv",
        rows: csv.rows,
        columns: csv.columns,
        encoding: csv.encoding,
        fileName: name,
        sheetNames: [],
        sheets: null,
        sheetsAoa: null,
        sheetName: "",
        rawAoa: csv.rawAoa,
        start_row: sr,
        start_col: sc,
      };
    }
    if (lower.endsWith(".xlsx") || lower.endsWith(".xls") || lower.endsWith(".xlsm")) {
      var xls = parseExcelWorkbook(buffer, sr, sc);
      return {
        kind: "excel",
        rows: xls.rows,
        columns: xls.columns,
        sheetName: xls.sheetName,
        sheetNames: xls.sheetNames,
        sheets: xls.sheets,
        sheetsAoa: xls.sheetsAoa,
        rawAoa: xls.rawAoa,
        encoding: "binary",
        fileName: name,
        start_row: sr,
        start_col: sc,
      };
    }
    throw new Error("Поддерживаются CSV и Excel (.xlsx/.xls)");
  }

  /** Пересчитать rows/columns пакета при смене листа или угла. */
  function applyPackOrigin(pack, opts) {
    if (!pack) {
      return pack;
    }
    var o = opts || {};
    var sr = o.start_row != null ? Number(o.start_row) || 1 : pack.start_row || 1;
    var sc = o.start_col != null ? Number(o.start_col) || 1 : pack.start_col || 1;
    var sheetName = o.sheetName != null ? o.sheetName : pack.sheetName;
    var aoa = pack.rawAoa || [];
    if (pack.kind === "excel" && pack.sheetsAoa) {
      var name = sheetName || pack.sheetName || (pack.sheetNames && pack.sheetNames[0]) || "";
      aoa = pack.sheetsAoa[name] || [];
      sheetName = name;
    }
    var table = tableFromAoaOrigin(aoa, sr, sc);
    var sheets = pack.sheets;
    if (pack.kind === "excel" && pack.sheetsAoa) {
      sheets = {};
      Object.keys(pack.sheetsAoa).forEach(function (n) {
        sheets[n] = tableFromAoaOrigin(pack.sheetsAoa[n], sr, sc);
      });
    }
    return Object.assign({}, pack, {
      sheetName: sheetName || "",
      rawAoa: aoa,
      rows: table.rows,
      columns: table.columns,
      start_row: sr,
      start_col: sc,
      sheets: sheets,
    });
  }

  function pickSheetFromPack(pack, sheetName) {
    return applyPackOrigin(pack, { sheetName: sheetName });
  }

  function entriesFromFioTable(rows, colFio, colTn) {
    var out = [];
    var seen = Object.create(null);
    (rows || []).forEach(function (row) {
      if (!row) return;
      var fio = String(row[colFio] == null ? "" : row[colFio]).trim();
      var tn = String(row[colTn] == null ? "" : row[colTn]).trim();
      if (!fio || !tn) return;
      if (fio === colFio || tn === colTn) return;
      var key = fio.toUpperCase();
      if (seen[key]) return;
      seen[key] = true;
      out.push({ fio: fio, person_number: tn });
    });
    return out;
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
    applyPackOrigin: applyPackOrigin,
    tableFromAoaOrigin: tableFromAoaOrigin,
    entriesFromFioTable: entriesFromFioTable,
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
