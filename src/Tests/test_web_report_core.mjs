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
    }),
  ];
  var json = ReportCore.serializeSettings(list);
  var back = ReportCore.parseSettings(json);
  assert.strictEqual(back.length, 1);
  assert.strictEqual(back[0].contest_code, "A");
  assert.strictEqual(back[0].type_ind, "FIO");
}

const tests = [
  ["parseNumber", testParseNumber],
  ["pad", testPad],
  ["tnMode", testTnMode],
  ["fioMode", testFioModeAndMissing],
  ["duplicatesSum", testDuplicatesSum],
  ["keepOneDrop", testKeepOneAndDrop],
  ["processAll", testProcessAll],
  ["settings", testSettingsRoundtrip],
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
