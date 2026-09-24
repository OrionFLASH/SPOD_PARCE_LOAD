#!/usr/bin/env node
/**
 * Тесты ядра web-report (логика PQ FIO/TN).
 * Запуск: node src/Tests/test_web_report_core.mjs
 */
import { createRequire } from "module";
import assert from "assert";
import path from "path";
import { fileURLToPath } from "url";

const require = createRequire(import.meta.url);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ReportCore = require(path.join(__dirname, "../../common/web-report/report_core.js"));

function testParseNumber() {
  assert.strictEqual(ReportCore.parseNumberFromComma("100,00000"), 100);
  assert.strictEqual(ReportCore.parseNumberFromComma("10,5"), 10.5);
  assert.strictEqual(ReportCore.parseNumberFromComma("1 000,25"), 1000.25);
  assert.strictEqual(ReportCore.parseNumberFromComma(12), 12);
  assert.strictEqual(ReportCore.parseNumberFromComma(null), 0);
}

function testPad() {
  assert.strictEqual(ReportCore.padPersonNumber("12345", 20), "00000000000000012345");
  assert.strictEqual(ReportCore.padPersonNumber("", 20), "00000000000000000000");
}

function testTnMode() {
  var t = ReportCore.createEmptyTournament({
    contest_code: "C1",
    tournament_code: "T1",
    contest_date: "2026-09-17",
    plan_value: "100.00000",
    full_name: "Demo",
    type_ind: "TN",
    column_id: "ТАБЕЛЬНЫЙ НОМЕР",
    column_fact: "ПОКАЗАТЕЛЬ",
  });
  var raw = [
    { "ТАБЕЛЬНЫЙ НОМЕР": "12345", ПОКАЗАТЕЛЬ: "10,5" },
    { "ТАБЕЛЬНЫЙ НОМЕР": "67890", ПОКАЗАТЕЛЬ: "20" },
  ];
  var out = ReportCore.normalizeTournamentRows(raw, t, null, {});
  assert.strictEqual(out.rows.length, 2);
  assert.strictEqual(out.rows[0].MANAGER_PERSON_NUMBER, "00000000000000012345");
  assert.strictEqual(out.rows[0].FACT_VALUE, "10.50000");
  assert.strictEqual(out.rows[0].PLAN_VALUE, "100.00000");
  assert.strictEqual(out.rows[0].priority_type, "1");
  assert.strictEqual(out.rows[0].FIO, "-");
}

function testFioModeAndMissing() {
  var t = ReportCore.createEmptyTournament({
    contest_code: "C1",
    tournament_code: "T2",
    contest_date: "2026-09-17",
    plan_value: "50",
    full_name: "FIO demo",
    type_ind: "FIO",
    column_id: "ФИО",
    column_fact: "ПОКАЗАТЕЛЬ",
  });
  var fioMap = ReportCore.buildFioMap([
    { fio: "Иванов Иван Иванович", person_number: "12345" },
  ]);
  var raw = [
    { ФИО: "Иванов Иван Иванович", ПОКАЗАТЕЛЬ: "100" },
    { ФИО: "Неизвестный", ПОКАЗАТЕЛЬ: "1" },
  ];
  var out = ReportCore.normalizeTournamentRows(raw, t, fioMap, {});
  assert.strictEqual(out.rows.length, 2);
  assert.strictEqual(out.rows[0]["ТАБЕЛЬНЫЙ НЕ НАЙДЕН"], "-");
  assert.strictEqual(out.rows[1]["ТАБЕЛЬНЫЙ НЕ НАЙДЕН"], "ДА");
  assert.strictEqual(out.rows[1].MANAGER_PERSON_NUMBER, "00000000000000000000");
  assert.deepStrictEqual(out.missingFio, ["Неизвестный"]);
}

function testDuplicatesSum() {
  var rows = [
    {
      MANAGER_PERSON_NUMBER: "00000000000000012345",
      CONTEST_CODE: "C",
      TOURNAMENT_CODE: "T",
      CONTEST_DATE: "2026-09-17",
      PLAN_VALUE: "100.00000",
      FACT_VALUE: "10.00000",
      priority_type: "1",
      "CONTEST-DATA=>FULL_NAME": "X",
      FIO: "-",
      "ТАБЕЛЬНЫЙ НЕ НАЙДЕН": "-",
      PLAN_VALUE_число: 100,
      FACT_VALUE_число: 10,
      include_in_csv: true,
      duplicate_comment: "",
    },
    {
      MANAGER_PERSON_NUMBER: "00000000000000012345",
      CONTEST_CODE: "C",
      TOURNAMENT_CODE: "T",
      CONTEST_DATE: "2026-09-17",
      PLAN_VALUE: "100.00000",
      FACT_VALUE: "5.50000",
      priority_type: "1",
      "CONTEST-DATA=>FULL_NAME": "X",
      FIO: "-",
      "ТАБЕЛЬНЫЙ НЕ НАЙДЕН": "-",
      PLAN_VALUE_число: 100,
      FACT_VALUE_число: 5.5,
      include_in_csv: true,
      duplicate_comment: "",
    },
  ];
  var groups = ReportCore.findDuplicateGroups(rows);
  assert.strictEqual(groups.length, 1);
  assert.strictEqual(groups[0].sum, 15.5);
  var applied = ReportCore.applyDuplicateResolutions(
    rows,
    { [groups[0].key]: { mode: "sum" } },
    {}
  );
  assert.ok(applied.ok);
  var csv = ReportCore.rowsForCsv(applied.rows);
  assert.strictEqual(csv.length, 1);
  assert.strictEqual(csv[0].FACT_VALUE, "15.50000");
  var xlsx = ReportCore.rowsForXlsx(applied.rows);
  assert.strictEqual(xlsx.length, 2);
  assert.ok(xlsx[0].КОММЕНТАРИЙ_ДУБЛЬ.indexOf("сумма") >= 0);
}

function testKeepOneAndDrop() {
  var base = function (fact) {
    return {
      MANAGER_PERSON_NUMBER: "00000000000000000001",
      CONTEST_CODE: "C",
      TOURNAMENT_CODE: "T",
      CONTEST_DATE: "2026-09-17",
      PLAN_VALUE: "1.00000",
      FACT_VALUE: ReportCore.formatNumberDot(fact),
      priority_type: "1",
      "CONTEST-DATA=>FULL_NAME": "X",
      FIO: "-",
      "ТАБЕЛЬНЫЙ НЕ НАЙДЕН": "-",
      PLAN_VALUE_число: 1,
      FACT_VALUE_число: fact,
      include_in_csv: true,
      duplicate_comment: "",
    };
  };
  var rows = [base(1), base(2), base(3)];
  var key = ReportCore.duplicateKey(rows[0]);
  var keep = ReportCore.applyDuplicateResolutions(rows, { [key]: { mode: "keep_one", keepIndex: 1 } }, {});
  assert.ok(keep.ok);
  assert.strictEqual(ReportCore.rowsForCsv(keep.rows).length, 1);
  assert.strictEqual(ReportCore.rowsForCsv(keep.rows)[0].FACT_VALUE, "2.00000");

  var drop = ReportCore.applyDuplicateResolutions(rows, { [key]: { mode: "drop_all" } }, {});
  assert.ok(drop.ok);
  assert.strictEqual(ReportCore.rowsForCsv(drop.rows).length, 0);
  assert.strictEqual(ReportCore.rowsForXlsx(drop.rows).length, 3);
}

function testProcessAll() {
  var payload = [
    {
      tournament: ReportCore.createEmptyTournament({
        contest_code: "01_2026-1_16-2_1",
        tournament_code: "t_01_2026-1_16-2_1_2012",
        plan_value: "100.00000",
        contest_date: "2026-09-17",
        full_name: "500 млн руб. сделка лизинга",
        type_ind: "TN",
        column_id: "ТАБЕЛЬНЫЙ НОМЕР",
        column_fact: "ПОКАЗАТЕЛЬ",
      }),
      rows: [
        { "ТАБЕЛЬНЫЙ НОМЕР": "12345", ПОКАЗАТЕЛЬ: "10" },
        { "ТАБЕЛЬНЫЙ НОМЕР": "999", ПОКАЗАТЕЛЬ: "1" },
      ],
    },
  ];
  var result = ReportCore.processAll(payload, [], {}, {});
  assert.ok(result.ok);
  assert.strictEqual(result.csvRows.length, 2);
  assert.strictEqual(result.csvRows[0].CONTEST_CODE, "01_2026-1_16-2_1");
  assert.deepStrictEqual(Object.keys(result.csvRows[0]), ReportCore.CSV_COLUMNS);
}

function testSettingsRoundtrip() {
  var list = [
    ReportCore.createEmptyTournament({
      contest_code: "A",
      tournament_code: "B",
      type_ind: "FIO",
      period_code: "Q1",
      fact_op: "mul",
      fact_op_value: "100",
      include_in_report: false,
      source_file_name: "data.csv",
      column_id: "ФИО",
      column_fact: "ПОКАЗАТЕЛЬ",
    }),
  ];
  var json = ReportCore.serializeSettings(list, {
    entries: [{ fio: "Иванов", person_number: "1" }],
    file_name: "fio.csv",
    file_path: "examples/fio.csv",
    col_fio: "ФИО",
    col_tn: "ТН",
  });
  assert.strictEqual(json.version, 3);
  assert.ok(!Object.prototype.hasOwnProperty.call(json.tournaments[0], "source_file_b64"));
  assert.ok(!json.fio.source_file_b64);
  assert.strictEqual(json.fio.entries.length, 1);
  assert.strictEqual(json.fio.file_path, "examples/fio.csv");
  assert.strictEqual(json.tournaments[0].source_file_name, "data.csv");
  assert.strictEqual(json.tournaments[0].source_file_path || "", "");
  var withPath = ReportCore.serializeSettings(
    [
      ReportCore.createEmptyTournament({
        contest_code: "A",
        source_file_name: "data.csv",
        source_file_path: "examples/data.csv",
      }),
    ],
    { entries: [] }
  );
  assert.strictEqual(withPath.tournaments[0].source_file_path, "examples/data.csv");
  var back = ReportCore.parseSettings(json);
  assert.strictEqual(back.tournaments.length, 1);
  assert.strictEqual(back.tournaments[0].contest_code, "A");
  assert.strictEqual(back.tournaments[0].type_ind, "FIO");
  assert.strictEqual(back.tournaments[0].period_code, "Q1");
  assert.strictEqual(back.tournaments[0].fact_op, "mul");
  assert.strictEqual(back.tournaments[0].include_in_report, false);
  assert.ok(back.fio);
  // устаревший base64 в JSON игнорируется
  var legacy = ReportCore.parseSettings({
    version: 2,
    tournaments: [{ contest_code: "X", source_file_b64: "AAAA" }],
    fio: { entries: [] },
  });
  assert.ok(!legacy.tournaments[0].source_file_b64);
}

function testFactOpAndPeriod() {
  var t = ReportCore.createEmptyTournament({
    contest_code: "C1",
    tournament_code: "T1",
    contest_date: "2026-09-17",
    plan_value: "100",
    full_name: "Demo",
    type_ind: "TN",
    period_code: "M3",
    column_id: "ТАБЕЛЬНЫЙ НОМЕР",
    column_fact: "ПОКАЗАТЕЛЬ",
    fact_op: "mul",
    fact_op_value: "100",
  });
  var out = ReportCore.normalizeTournamentRows(
    [{ "ТАБЕЛЬНЫЙ НОМЕР": "1", ПОКАЗАТЕЛЬ: "0,5" }],
    t,
    null,
    {}
  );
  assert.strictEqual(out.rows[0].FACT_VALUE, "50.00000");
  assert.strictEqual(out.rows[0].PERIOD_CODE, "M3");
  assert.ok(String(out.rows[0].PERIOD).indexOf("M3") >= 0);

  assert.strictEqual(ReportCore.applyFactOperation(10, "div", 2), 5);
  assert.strictEqual(ReportCore.applyFactOperation(10, "add", 2), 12);
  assert.strictEqual(ReportCore.applyFactOperation(10, "sub", 3), 7);

  var badges = ReportCore.periodBadgesForTournaments([
    ReportCore.createEmptyTournament({ id: "a", contest_code: "X", period_code: "Y" }),
    ReportCore.createEmptyTournament({ id: "b", contest_code: "X", period_code: "Y" }),
    ReportCore.createEmptyTournament({ id: "c", contest_code: "X", period_code: "Q1" }),
  ]);
  assert.strictEqual(badges.a, "Y(1)");
  assert.strictEqual(badges.b, "Y(2)");
  assert.strictEqual(badges.c, "Q1");
}

function testCsvAfterKeepOne() {
  var rows = [
    {
      tournament_id: "t1",
      source_index: 0,
      MANAGER_PERSON_NUMBER: "00000000000000000001",
      CONTEST_CODE: "C",
      TOURNAMENT_CODE: "T",
      CONTEST_DATE: "2026-01-01",
      PLAN_VALUE: "1.00000",
      FACT_VALUE: "1.00000",
      FACT_VALUE_число: 1,
      priority_type: "1",
      FIO: "-",
      include_in_csv: true,
    },
    {
      tournament_id: "t1",
      source_index: 1,
      MANAGER_PERSON_NUMBER: "00000000000000000001",
      CONTEST_CODE: "C",
      TOURNAMENT_CODE: "T",
      CONTEST_DATE: "2026-01-01",
      PLAN_VALUE: "1.00000",
      FACT_VALUE: "2.00000",
      FACT_VALUE_число: 2,
      priority_type: "1",
      FIO: "-",
      include_in_csv: true,
    },
  ];
  var key = ReportCore.duplicateKey(rows[0]);
  var applied = ReportCore.applyDuplicateResolutions(rows, {
    [key]: { mode: "keep_one", keepIndex: 0 },
  });
  assert.ok(applied.ok);
  var v = ReportCore.validateCsvExportRows(applied.rows, {});
  assert.strictEqual(v.ok, true, "после keep_one CSV должен быть разрешён");
  assert.strictEqual(ReportCore.rowsForCsv(applied.rows).length, 1);
}

function testResolutionFingerprints() {
  var g = {
    key: "C|T|0001",
    indices: [0, 1],
    rows: [
      {
        tournament_id: "t1",
        source_index: 0,
        MANAGER_PERSON_NUMBER: "00000000000000000001",
        CONTEST_CODE: "C",
        TOURNAMENT_CODE: "T",
        FACT_VALUE: "1.00000",
        FIO: "-",
      },
      {
        tournament_id: "t1",
        source_index: 1,
        MANAGER_PERSON_NUMBER: "00000000000000000001",
        CONTEST_CODE: "C",
        TOURNAMENT_CODE: "T",
        FACT_VALUE: "2.00000",
        FIO: "-",
      },
    ],
  };
  var fp = ReportCore.rowFingerprint(g.rows[1]);
  var res = { mode: "keep_one", keepFingerprint: fp };
  var idx = ReportCore.resolveKeepIndices(g, res);
  assert.deepStrictEqual(idx, [1]);
  assert.ok(ReportCore.describeResolution({ mode: "sum" }).indexOf("сумма") >= 0);
}

function testCsvValidation() {
  var rows = [
    {
      MANAGER_PERSON_NUMBER: "00000000000000000001",
      CONTEST_CODE: "C",
      TOURNAMENT_CODE: "T",
      CONTEST_DATE: "2026-01-01",
      PLAN_VALUE: "1.00000",
      FACT_VALUE: "1.00000",
      priority_type: "1",
      include_in_csv: true,
    },
    {
      MANAGER_PERSON_NUMBER: "00000000000000000001",
      CONTEST_CODE: "C",
      TOURNAMENT_CODE: "T",
      CONTEST_DATE: "2026-01-01",
      PLAN_VALUE: "1.00000",
      FACT_VALUE: "2.00000",
      priority_type: "1",
      include_in_csv: true,
    },
  ];
  var v = ReportCore.validateCsvExportRows(rows, {});
  assert.strictEqual(v.ok, false);
  assert.ok(v.duplicateKeys.length >= 1);

  var badTn = [
    {
      MANAGER_PERSON_NUMBER: "000000нет",
      CONTEST_CODE: "C",
      TOURNAMENT_CODE: "T2",
      CONTEST_DATE: "2026-01-01",
      PLAN_VALUE: "1.00000",
      FACT_VALUE: "1.00000",
      priority_type: "1",
      include_in_csv: true,
    },
  ];
  var v2 = ReportCore.validateCsvExportRows(badTn, {});
  assert.strictEqual(v2.ok, false);
  assert.ok(v2.badPerson.length >= 1);
  assert.ok(String(v2.rowsMarked[0].CSV_ERROR).indexOf("ТН") >= 0);
}

function testIncludeStages() {
  var t1 = ReportCore.createEmptyTournament({
    contest_code: "C",
    tournament_code: "T1",
    plan_value: "1",
    contest_date: "2026-09-17",
    full_name: "Name",
    type_ind: "TN",
    column_id: "ТАБЕЛЬНЫЙ НОМЕР",
    column_fact: "ПОКАЗАТЕЛЬ",
    include_in_report: false,
  });
  var st = ReportCore.computeStages([t1], {}, [], null);
  assert.strictEqual(st.hasTournaments, false);
  assert.strictEqual(st.canCheck, false);
}

function testCloneAndStages() {
  var t = ReportCore.createEmptyTournament({
    contest_code: "C",
    tournament_code: "T1",
    plan_value: "1",
    contest_date: "2026-09-17",
    full_name: "Name",
    type_ind: "TN",
    column_id: "ТАБЕЛЬНЫЙ НОМЕР",
    column_fact: "ПОКАЗАТЕЛЬ",
  });
  var copy = ReportCore.cloneTournament(t);
  assert.strictEqual(copy.needs_identity_fix, true);
  assert.strictEqual(ReportCore.tournamentIdentityUnlocked(copy), false);
  copy.tournament_code = "T2";
  copy.full_name = "Other";
  assert.strictEqual(ReportCore.tournamentIdentityUnlocked(copy), true);

  var data = {};
  data[t.id] = {
    rows: [{ "ТАБЕЛЬНЫЙ НОМЕР": "1", ПОКАЗАТЕЛЬ: "10" }],
    columns: ["ТАБЕЛЬНЫЙ НОМЕР", "ПОКАЗАТЕЛЬ"],
  };
  var st = ReportCore.computeStages([t], data, [], {
    duplicatesCleared: true,
    missingFioCleared: true,
    fioDupCleared: true,
  });
  assert.ok(st.hasTournaments);
  assert.ok(st.fieldsFilled);
  assert.ok(st.sourcesOk);
  assert.ok(st.canProcess);
}

function testResolveFioTableEntries() {
  assert.strictEqual(ReportCore.isNumericPersonToken("000123"), true);
  assert.strictEqual(ReportCore.isNumericPersonToken("12345"), true);
  assert.strictEqual(ReportCore.isNumericPersonToken(""), false);
  assert.strictEqual(ReportCore.isNumericPersonToken("12a"), false);
  assert.strictEqual(ReportCore.isNumericPersonToken("12.5"), false);

  var rows = [
    { ФИО: "Иванов Иван", ТН: "abc" },
    { ФИО: "Иванов Иван", ТН: "000111" },
    { ФИО: "Иванов Иван", ТН: "222" },
    { ФИО: "Петров Пётр", ТН: "" },
    { ФИО: "Сидоров", ТН: "333" },
    { ФИО: "Сидоров", ТН: "xxx" },
  ];
  var resolved = ReportCore.resolveFioTableEntries(rows, "ФИО", "ТН");
  assert.strictEqual(resolved.entries.length, 3);
  var byFio = Object.create(null);
  resolved.entries.forEach(function (e) {
    byFio[e.fio] = e.person_number;
  });
  // дубль Иванова: первая с цифрами — 000111 (не abc)
  assert.strictEqual(byFio["Иванов Иван"], "000111");
  // одно вхождение Петрова — берём пустой ТН как есть
  assert.strictEqual(byFio["Петров Пётр"], "");
  // дубль Сидорова: первая с цифрами — 333
  assert.strictEqual(byFio["Сидоров"], "333");
  assert.strictEqual(resolved.stats.duplicateFioNames, 2);
  assert.strictEqual(resolved.stats.duplicateExtraRows, 3);
  assert.strictEqual(resolved.stats.invalidTnRows, 3);
  assert.ok(resolved.stats.message.indexOf("повторяющихся ФИО") >= 0);
  assert.ok(resolved.stats.message.indexOf("нечисловым") >= 0);
  assert.ok(Array.isArray(resolved.issues));
  assert.ok(resolved.issues.length >= 3);
  var ivanIssues = resolved.issues.filter(function (it) {
    return it.fio === "Иванов Иван";
  });
  assert.strictEqual(ivanIssues.length, 3);
  var chosenIvan = ivanIssues.filter(function (it) {
    return it.chosen;
  });
  assert.strictEqual(chosenIvan.length, 1);
  assert.strictEqual(chosenIvan[0].person_number, "000111");
  assert.ok(chosenIvan[0].tnOk);
}

function testNumberParsingStrict() {
  const p = (v) => ReportCore.parseNumberStrict(v);
  // отрицательные с дробью (раньше -10,5 → -9.5)
  assert.strictEqual(p("-10,5").value, -10.5);
  assert.strictEqual(p("-0,25").value, -0.25);
  assert.strictEqual(p("−3,5").value, -3.5);
  assert.strictEqual(p("+7").value, 7);
  // разделители тысяч
  assert.strictEqual(p("1.000,25").value, 1000.25);
  assert.strictEqual(p("1,000.25").value, 1000.25);
  assert.strictEqual(p("1 234 567,5").value, 1234567.5);
  assert.strictEqual(p("1 234,5").value, 1234.5);
  assert.strictEqual(p("1.000.000").value, 1000000);
  // одиночный разделитель — дробный (как раньше)
  assert.strictEqual(p("10,5").value, 10.5);
  assert.strictEqual(p("10.5").value, 10.5);
  assert.strictEqual(p(",5").value, 0.5);
  // проценты
  assert.strictEqual(p("50%").value, 0.5);
  // пусто → 0 без ошибки
  assert.deepStrictEqual(p(""), { ok: true, value: 0, empty: true });
  assert.deepStrictEqual(p(null), { ok: true, value: 0, empty: true });
  // не число → ошибка
  ["н/д", "abc", "1,2,3", "12.34.5", "1.000,2,5", "-", "1e5", "10 руб"].forEach((v) => {
    assert.strictEqual(p(v).ok, false, "ожидалась ошибка для " + v);
  });
  // совместимость: parseNumberFromComma → 0 для не числа
  assert.strictEqual(ReportCore.parseNumberFromComma("н/д"), 0);
}

function testNumberFormatDot() {
  const f = (n) => ReportCore.formatNumberDot(n, 5);
  assert.strictEqual(f(100), "100.00000");
  assert.strictEqual(f(10.5), "10.50000");
  assert.strictEqual(f(-10.5), "-10.50000");
  assert.strictEqual(f(0.123456), "0.12346");
  assert.strictEqual(f(1.000005), "1.00001");
  assert.strictEqual(f(-1.000005), "-1.00001");
  assert.strictEqual(f(-0.000001), "0.00000");
  assert.strictEqual(f(0), "0.00000");
}

function testFactNotNumberBlocksCsv() {
  const t = ReportCore.createEmptyTournament({
    id: "t1",
    contest_code: "C",
    tournament_code: "T",
    plan_value: "-5,5",
    contest_date: "2026-09-25",
    full_name: "N",
    type_ind: "TN",
    column_id: "ТН",
    column_fact: "ФАКТ",
  });
  const norm = ReportCore.normalizeTournamentRows(
    [
      { ТН: "1", ФАКТ: "-10,5" },
      { ТН: "2", ФАКТ: "н/д" },
    ],
    t,
    new Map(),
    {}
  );
  assert.strictEqual(norm.rows[0].PLAN_VALUE, "-5.50000");
  assert.strictEqual(norm.rows[0].FACT_VALUE, "-10.50000");
  assert.strictEqual(norm.rows[1].FACT_VALUE, "");
  assert.ok(norm.rows[1].fact_error.indexOf("н/д") >= 0);

  const v = ReportCore.validateCsvExportRows(norm.rows, {});
  assert.strictEqual(v.ok, false);
  assert.strictEqual(v.badFact.length, 1);
  assert.strictEqual(v.emptyCells.length, 0);
  assert.strictEqual(v.byTournament[0].bad_fact, 1);
  assert.ok(v.rowsMarked[1].CSV_ERROR.indexOf("показатель не число") >= 0);
  assert.strictEqual(v.rowsMarked[0].CSV_ERROR, "-");

  const stats = ReportCore.tournamentRowStats(t, { rows: [{ ТН: "1", ФАКТ: "1" }, { ТН: "2", ФАКТ: "x" }] }, [], {});
  assert.strictEqual(stats.errors, 1);
  assert.strictEqual(stats.forCsv, 1);
}

function testPlanAndOpValueValidation() {
  const base = {
    contest_code: "C",
    tournament_code: "T",
    contest_date: "2026-09-25",
    full_name: "N",
    type_ind: "TN",
    period_code: "Y",
  };
  assert.ok(ReportCore.tournamentFieldsOk(Object.assign({}, base, { plan_value: "-3,25" })));
  assert.ok(!ReportCore.tournamentFieldsOk(Object.assign({}, base, { plan_value: "сто" })));
  assert.ok(
    !ReportCore.tournamentFieldsOk(Object.assign({}, base, { plan_value: "1", fact_op: "mul", fact_op_value: "x" }))
  );
  assert.ok(
    ReportCore.tournamentFieldsOk(Object.assign({}, base, { plan_value: "1", fact_op: "none", fact_op_value: "x" }))
  );
}

function testPersonNumberProblem() {
  const pr = (v) => ReportCore.personNumberProblem(v, 20);
  assert.strictEqual(pr("00000000000000012345"), "");
  assert.ok(pr("00000000001.23457E+19").indexOf("экспонент") >= 0);
  assert.ok(pr("0000000000000000нет1").indexOf("не только цифры") >= 0);
  assert.ok(pr("123456789012345678901").indexOf("длиннее") >= 0);
  assert.ok(pr("").indexOf("пуст") >= 0);
}

const tests = [
  ["numberParsingStrict", testNumberParsingStrict],
  ["numberFormatDot", testNumberFormatDot],
  ["factNotNumber", testFactNotNumberBlocksCsv],
  ["planOpValidation", testPlanAndOpValueValidation],
  ["personNumberProblem", testPersonNumberProblem],
  ["parseNumber", testParseNumber],
  ["pad", testPad],
  ["tnMode", testTnMode],
  ["fioMode", testFioModeAndMissing],
  ["duplicatesSum", testDuplicatesSum],
  ["keepOneDrop", testKeepOneAndDrop],
  ["processAll", testProcessAll],
  ["settings", testSettingsRoundtrip],
  ["factOpPeriod", testFactOpAndPeriod],
  ["csvValidation", testCsvValidation],
  ["csvAfterKeepOne", testCsvAfterKeepOne],
  ["resolutionFingerprints", testResolutionFingerprints],
  ["includeStages", testIncludeStages],
  ["cloneStages", testCloneAndStages],
  ["resolveFioTable", testResolveFioTableEntries],
];

let failed = 0;
for (const [name, fn] of tests) {
  try {
    fn();
    console.log("OK  ", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err);
  }
}

if (failed) {
  console.error("Failed:", failed);
  process.exit(1);
}
console.log("All tests passed:", tests.length);
