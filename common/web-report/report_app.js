/**
 * UI web-report: турниры, загрузка данных, диалоги ФИО/дублей, выгрузка.
 */
(function () {
  "use strict";

  var state = {
    config: null,
    tournaments: [],
    activeId: null,
    /** @type {Record<string, {rows:object[], columns:string[], encoding?:string}>} */
    dataByTournament: {},
    fioEntries: [],
    lastResult: null,
  };

  var els = {};

  function $(id) {
    return document.getElementById(id);
  }

  function setStatus(text) {
    if (els.footerStatus) {
      els.footerStatus.textContent = "Статус: " + text;
    }
  }

  function showToast(text) {
    var toast = $("save-toast");
    var label = $("save-toast-text");
    if (!toast || !label) {
      return;
    }
    label.textContent = text;
    toast.hidden = false;
    clearTimeout(showToast._t);
    showToast._t = setTimeout(function () {
      toast.hidden = true;
    }, 2200);
  }

  function coreOpts() {
    return ReportIO.coreOptions();
  }

  function storageKeys() {
    var c = state.config || {};
    return {
      settings: c.local_storage_settings_key || "spod_web_report_settings_v1",
      fio: c.local_storage_fio_key || "spod_web_report_fio_v1",
    };
  }

  function persistDraft() {
    try {
      var keys = storageKeys();
      localStorage.setItem(keys.settings, JSON.stringify(ReportCore.serializeSettings(state.tournaments)));
      localStorage.setItem(keys.fio, JSON.stringify(ReportCore.serializeFioDictionary(state.fioEntries)));
    } catch (err) {
      /* ignore quota */
    }
  }

  function restoreDraft() {
    try {
      var keys = storageKeys();
      var s = localStorage.getItem(keys.settings);
      var f = localStorage.getItem(keys.fio);
      if (s) {
        state.tournaments = ReportCore.parseSettings(JSON.parse(s));
      }
      if (f) {
        state.fioEntries = ReportCore.parseFioDictionary(JSON.parse(f));
      }
    } catch (err) {
      console.warn(err);
    }
  }

  function activeTournament() {
    return state.tournaments.find(function (t) {
      return t.id === state.activeId;
    });
  }

  function ensureActive() {
    if (!state.tournaments.length) {
      state.activeId = null;
      return;
    }
    if (!state.tournaments.some(function (t) {
      return t.id === state.activeId;
    })) {
      state.activeId = state.tournaments[0].id;
    }
  }

  function renderNav() {
    var nav = els.tournamentNav;
    nav.innerHTML = "";
    state.tournaments.forEach(function (t) {
      var btn = document.createElement("button");
      btn.type = "button";
      btn.className = "tournament-nav-item" + (t.id === state.activeId ? " active" : "");
      btn.innerHTML =
        '<div class="tournament-nav-item__code">' +
        escapeHtml(t.tournament_code || t.contest_code || t.id) +
        "</div>" +
        '<div class="tournament-nav-item__name">' +
        escapeHtml(t.full_name || t.type_ind || "") +
        "</div>";
      btn.addEventListener("click", function () {
        flushEditorToState();
        state.activeId = t.id;
        renderAll();
      });
      nav.appendChild(btn);
    });
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function fillSelect(select, options, selected) {
    select.innerHTML = "";
    var empty = document.createElement("option");
    empty.value = "";
    empty.textContent = "— выберите —";
    select.appendChild(empty);
    (options || []).forEach(function (name) {
      var opt = document.createElement("option");
      opt.value = name;
      opt.textContent = name;
      if (name === selected) {
        opt.selected = true;
      }
      select.appendChild(opt);
    });
  }

  function renderEditor() {
    var t = activeTournament();
    var empty = $("empty-state");
    var editor = $("tournament-editor");
    if (!t) {
      empty.hidden = false;
      editor.hidden = true;
      return;
    }
    empty.hidden = true;
    editor.hidden = false;
    $("editor-title").textContent = t.full_name || t.tournament_code || "Турнир";
    $("f-contest-code").value = t.contest_code || "";
    $("f-tournament-code").value = t.tournament_code || "";
    $("f-plan").value = t.plan_value || "";
    $("f-date").value = t.contest_date || "";
    $("f-full-name").value = t.full_name || "";
    $("f-type").value = (t.type_ind || "TN").toUpperCase() === "FIO" ? "FIO" : "TN";
    $("f-source-name").value = t.source_file_name || "";

    var pack = state.dataByTournament[t.id];
    var columns = pack ? pack.columns : [];
    fillSelect($("f-col-id"), columns, t.column_id || "");
    fillSelect($("f-col-fact"), columns, t.column_fact || "");

    var meta = $("data-meta");
    var warn = $("editor-warn");
    var preview = $("preview-wrap");
    if (pack) {
      meta.hidden = false;
      meta.textContent =
        pack.rows.length +
        " строк" +
        (pack.encoding ? ", " + pack.encoding : "") +
        (columns.length ? ", " + columns.length + " кол." : "");
      renderPreview(pack);
      preview.hidden = false;
      warn.hidden = true;
    } else {
      meta.hidden = true;
      preview.hidden = true;
      warn.hidden = false;
      warn.textContent = "Загрузите CSV (разделитель «;») или Excel и укажите колонки.";
    }
  }

  function renderPreview(pack) {
    var table = $("preview-table");
    var thead = table.querySelector("thead");
    var tbody = table.querySelector("tbody");
    thead.innerHTML = "";
    tbody.innerHTML = "";
    var cols = (pack.columns || []).slice(0, 8);
    var trh = document.createElement("tr");
    cols.forEach(function (c) {
      var th = document.createElement("th");
      th.textContent = c;
      trh.appendChild(th);
    });
    thead.appendChild(trh);
    (pack.rows || []).slice(0, 12).forEach(function (row) {
      var tr = document.createElement("tr");
      cols.forEach(function (c) {
        var td = document.createElement("td");
        td.textContent = row[c] == null ? "" : String(row[c]);
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
  }

  function renderFioStats() {
    $("fio-stats").textContent = "Записей: " + state.fioEntries.length;
  }

  function renderResultPanel() {
    var panel = $("result-panel");
    var r = state.lastResult;
    if (!r || !r.ok) {
      panel.hidden = true;
      els.btnExportCsv.disabled = true;
      els.btnExportXlsx.disabled = true;
      return;
    }
    panel.hidden = false;
    els.btnExportCsv.disabled = false;
    els.btnExportXlsx.disabled = false;
    $("result-summary").textContent =
      "Строк в XLSX: " +
      r.xlsxRows.length +
      "; в CSV (согласовано): " +
      r.csvRows.length +
      (r.missingFio && r.missingFio.length ? "; ФИО без табельного (осталось): " + r.missingFio.length : "");
    var w = $("result-warnings");
    if (r.missingFio && r.missingFio.length) {
      w.hidden = false;
      w.textContent = "Не найдены в справочнике (подставлен 00000000): " + r.missingFio.join("; ");
    } else {
      w.hidden = true;
    }
  }

  function renderAll() {
    ensureActive();
    renderNav();
    renderEditor();
    renderFioStats();
    renderResultPanel();
    persistDraft();
  }

  function flushEditorToState() {
    var t = activeTournament();
    if (!t) {
      return;
    }
    t.contest_code = $("f-contest-code").value.trim();
    t.tournament_code = $("f-tournament-code").value.trim();
    t.plan_value = $("f-plan").value.trim();
    t.contest_date = $("f-date").value.trim();
    t.full_name = $("f-full-name").value.trim();
    t.type_ind = $("f-type").value;
    t.column_id = $("f-col-id").value;
    t.column_fact = $("f-col-fact").value;
  }

  function addTournament() {
    flushEditorToState();
    var t = ReportCore.createEmptyTournament({
      contest_date: new Date().toISOString().slice(0, 10),
      type_ind: "TN",
    });
    state.tournaments.push(t);
    state.activeId = t.id;
    state.lastResult = null;
    renderAll();
    setStatus("добавлен турнир");
  }

  function removeTournament() {
    flushEditorToState();
    if (!state.activeId) {
      return;
    }
    var id = state.activeId;
    state.tournaments = state.tournaments.filter(function (t) {
      return t.id !== id;
    });
    delete state.dataByTournament[id];
    state.activeId = state.tournaments[0] ? state.tournaments[0].id : null;
    state.lastResult = null;
    renderAll();
    setStatus("турнир удалён");
  }

  function bindFieldSync() {
    ["f-contest-code", "f-tournament-code", "f-plan", "f-date", "f-full-name", "f-type", "f-col-id", "f-col-fact"].forEach(
      function (id) {
        var el = $(id);
        el.addEventListener("change", function () {
          flushEditorToState();
          if (id === "f-type") {
            var t = activeTournament();
            var pack = t && state.dataByTournament[t.id];
            if (t && pack) {
              if (!t.column_id) {
                t.column_id = ReportIO.guessIdColumn(pack.columns, t.type_ind);
              }
              if (!t.column_fact) {
                t.column_fact = ReportIO.guessFactColumn(pack.columns);
              }
            }
          }
          renderNav();
          persistDraft();
        });
        el.addEventListener("input", function () {
          flushEditorToState();
          persistDraft();
        });
      }
    );
  }

  async function onImportData(file) {
    flushEditorToState();
    var t = activeTournament();
    if (!t) {
      return;
    }
    try {
      setStatus("чтение файла…");
      var pack = await ReportIO.readTableFile(file);
      state.dataByTournament[t.id] = {
        rows: pack.rows,
        columns: pack.columns,
        encoding: pack.encoding,
      };
      t.source_file_name = pack.fileName;
      t.column_id = ReportIO.guessIdColumn(pack.columns, t.type_ind);
      t.column_fact = ReportIO.guessFactColumn(pack.columns);
      state.lastResult = null;
      renderAll();
      showToast("Данные загружены");
      setStatus("данные: " + pack.rows.length + " строк (" + (pack.encoding || pack.kind) + ")");
    } catch (err) {
      console.error(err);
      setStatus("ошибка загрузки");
      alert(err.message || String(err));
    }
  }

  function buildPayload() {
    flushEditorToState();
    return state.tournaments.map(function (t) {
      var pack = state.dataByTournament[t.id] || { rows: [] };
      return { tournament: t, rows: pack.rows };
    });
  }

  function validateBeforeProcess() {
    if (!state.tournaments.length) {
      return "Добавьте хотя бы один турнир";
    }
    for (var i = 0; i < state.tournaments.length; i++) {
      var t = state.tournaments[i];
      if (!t.contest_code || !t.tournament_code || !t.contest_date) {
        return "Заполните CONTEST_CODE, TOURNAMENT_CODE и дату у всех турниров";
      }
      var pack = state.dataByTournament[t.id];
      if (!pack || !pack.rows.length) {
        return "Нет данных у турнира: " + (t.tournament_code || t.id);
      }
      if (!t.column_id || !t.column_fact) {
        return "Укажите колонки идентификатора и показателя: " + (t.tournament_code || t.id);
      }
    }
    return null;
  }

  function openModal(id) {
    $(id).hidden = false;
  }

  function closeModal(id) {
    $(id).hidden = true;
  }

  function askMissingFio(missingList) {
    return new Promise(function (resolve) {
      var list = $("modal-fio-list");
      list.innerHTML = "";
      missingList.forEach(function (fio, idx) {
        var row = document.createElement("div");
        row.className = "fio-row";
        row.innerHTML =
          '<input class="field-input" data-fio-name readonly />' +
          '<input class="field-input" data-fio-num placeholder="табельный" autocomplete="off" />';
        row.querySelector("[data-fio-name]").value = fio;
        row.querySelector("[data-fio-num]").dataset.idx = String(idx);
        list.appendChild(row);
      });
      openModal("modal-fio");

      function cleanup() {
        $("modal-fio-apply").onclick = null;
        $("modal-fio-skip").onclick = null;
        $("modal-fio-cancel").onclick = null;
        closeModal("modal-fio");
      }

      $("modal-fio-apply").onclick = function () {
        var inputs = list.querySelectorAll("[data-fio-num]");
        var added = 0;
        inputs.forEach(function (inp, idx) {
          var num = inp.value.trim();
          if (!num) {
            return;
          }
          var fio = missingList[idx];
          var key = ReportCore.normalizeFioKey(fio);
          state.fioEntries = state.fioEntries.filter(function (e) {
            return ReportCore.normalizeFioKey(e.fio) !== key;
          });
          state.fioEntries.push({ fio: fio, person_number: num });
          added++;
        });
        cleanup();
        persistDraft();
        renderFioStats();
        resolve({ action: "apply", added: added });
      };

      $("modal-fio-skip").onclick = function () {
        cleanup();
        resolve({ action: "skip" });
      };

      $("modal-fio-cancel").onclick = function () {
        cleanup();
        resolve({ action: "cancel" });
      };
    });
  }

  function askDuplicates(groups, rows) {
    return new Promise(function (resolve) {
      var list = $("modal-dup-list");
      list.innerHTML = "";
      groups.forEach(function (g, gi) {
        var box = document.createElement("div");
        box.className = "dup-group";
        box.dataset.key = g.key;
        var opts =
          '<label class="field-label">Действие</label>' +
          '<select class="field-select" data-dup-mode>' +
          '<option value="sum">Оставить с суммой показателя</option>' +
          '<option value="keep_one">Выбрать одну строку</option>' +
          '<option value="drop_all">Убрать все дубли из CSV</option>' +
          "</select>";
        var rowsHtml = g.rows
          .map(function (r, ri) {
            var idx = g.indices[ri];
            return (
              '<label style="display:flex;gap:8px;align-items:center;margin:4px 0;font-size:12px">' +
              '<input type="radio" name="dup_' +
              gi +
              '" value="' +
              idx +
              '"' +
              (ri === 0 ? " checked" : "") +
              " />" +
              escapeHtml(r.MANAGER_PERSON_NUMBER) +
              " | FIO=" +
              escapeHtml(r.FIO) +
              " | FACT=" +
              escapeHtml(r.FACT_VALUE) +
              "</label>"
            );
          })
          .join("");
        box.innerHTML =
          '<div class="dup-group__meta">' +
          escapeHtml(g.key) +
          " · строк: " +
          g.indices.length +
          " · сумма: " +
          ReportCore.formatNumberDot(g.sum, coreOpts().numberDecimals) +
          "</div>" +
          opts +
          '<div data-keep-wrap style="margin-top:8px">' +
          rowsHtml +
          "</div>";
        list.appendChild(box);
      });
      openModal("modal-dup");

      function cleanup() {
        $("modal-dup-apply").onclick = null;
        $("modal-dup-abort").onclick = null;
        $("modal-dup-cancel").onclick = null;
        closeModal("modal-dup");
      }

      $("modal-dup-apply").onclick = function () {
        var resolutions = {};
        Array.prototype.forEach.call(list.querySelectorAll(".dup-group"), function (box) {
          var key = box.dataset.key;
          var mode = box.querySelector("[data-dup-mode]").value;
          var res = { mode: mode };
          if (mode === "keep_one") {
            var checked = box.querySelector('input[type="radio"]:checked');
            res.keepIndex = checked ? Number(checked.value) : null;
          }
          resolutions[key] = res;
        });
        cleanup();
        resolve({ action: "apply", resolutions: resolutions });
      };

      $("modal-dup-abort").onclick = function () {
        cleanup();
        resolve({ action: "abort" });
      };

      $("modal-dup-cancel").onclick = function () {
        cleanup();
        resolve({ action: "cancel" });
      };
    });
  }

  async function runProcess() {
    var err = validateBeforeProcess();
    if (err) {
      alert(err);
      return;
    }
    flushEditorToState();
    setStatus("обработка…");

    var payload = buildPayload();
    var first = ReportCore.processAll(payload, state.fioEntries, {}, coreOpts());

    // Если есть ненайденные ФИО — предложить дополнить справочник
    if (first.missingFio && first.missingFio.length) {
      var fioAns = await askMissingFio(first.missingFio.slice());
      if (fioAns.action === "cancel") {
        setStatus("отменено");
        return;
      }
      if (fioAns.action === "apply") {
        // Пересобрать entries аккуратнее: merge старых с введёнными (в askMissingFio уже добавили)
        first = ReportCore.processAll(payload, state.fioEntries, {}, coreOpts());
      }
    }

    // Пересчитать rows до резолюции дублей
    var fioMap = ReportCore.buildFioMap(state.fioEntries);
    var allRows = [];
    var allMissing = [];
    var missingSeen = Object.create(null);
    payload.forEach(function (item) {
      var part = ReportCore.normalizeTournamentRows(item.rows, item.tournament, fioMap, coreOpts());
      allRows = allRows.concat(part.rows);
      part.missingFio.forEach(function (fio) {
        var k = ReportCore.normalizeFioKey(fio);
        if (!missingSeen[k]) {
          missingSeen[k] = true;
          allMissing.push(fio);
        }
      });
    });
    ReportCore.annotateDuplicatesLikePq(allRows, coreOpts());
    var groups = ReportCore.findDuplicateGroups(allRows);

    var resolutions = {};
    if (groups.length) {
      var dupAns = await askDuplicates(groups, allRows);
      if (dupAns.action === "cancel") {
        setStatus("отменено");
        return;
      }
      if (dupAns.action === "abort") {
        groups.forEach(function (g) {
          resolutions[g.key] = { mode: "abort" };
        });
        var aborted = ReportCore.applyDuplicateResolutions(allRows, resolutions, coreOpts());
        state.lastResult = null;
        setStatus(aborted.error || "остановлено");
        alert(aborted.error || "Формирование остановлено");
        renderResultPanel();
        return;
      }
      resolutions = dupAns.resolutions || {};
    }

    var applied = ReportCore.applyDuplicateResolutions(allRows, resolutions, coreOpts());
    if (!applied.ok) {
      state.lastResult = null;
      setStatus(applied.error || "ошибка");
      alert(applied.error || "Ошибка разрешения дублей");
      return;
    }

    state.lastResult = {
      ok: true,
      rows: applied.rows,
      missingFio: allMissing,
      duplicateGroups: groups,
      csvRows: ReportCore.rowsForCsv(applied.rows),
      xlsxRows: ReportCore.rowsForXlsx(applied.rows),
    };
    renderResultPanel();
    showToast("Готово");
    setStatus("сформировано: CSV " + state.lastResult.csvRows.length + " / XLSX " + state.lastResult.xlsxRows.length);
  }

  function exportCsv() {
    if (!state.lastResult || !state.lastResult.ok) {
      return;
    }
    ReportIO.downloadReportCsv(state.lastResult.csvRows, ReportIO.timestampName("REPORT", "csv"));
    showToast("CSV сохранён");
  }

  function exportXlsx() {
    if (!state.lastResult || !state.lastResult.ok) {
      return;
    }
    ReportIO.downloadReportXlsx(state.lastResult.xlsxRows, ReportIO.timestampName("REPORT", "xlsx"));
    showToast("XLSX сохранён");
  }

  async function loadConfig() {
    try {
      var res = await fetch("config.json", { cache: "no-store" });
      if (!res.ok) {
        throw new Error("HTTP " + res.status);
      }
      state.config = await res.json();
      rootConfig(state.config);
    } catch (err) {
      console.warn("config.json не прочитан, defaults", err);
      state.config = {
        csv_delimiter: ";",
        csv_encodings: ["utf-8", "windows-1251", "ibm866"],
        person_number_length: 20,
        number_decimals: 5,
        priority_type: "1",
        missing_person_placeholder: "00000000",
        no_duplicate_mark: "-",
        missing_fio_flag: "ДА",
      };
      rootConfig(state.config);
    }
  }

  function rootConfig(cfg) {
    window.ReportConfig = cfg;
  }

  function readJsonFile(file) {
    return file.text().then(function (text) {
      return JSON.parse(text);
    });
  }

  async function init() {
    els.tournamentNav = $("tournament-nav");
    els.footerStatus = $("footer-status");
    els.btnExportCsv = $("btn-export-csv");
    els.btnExportXlsx = $("btn-export-xlsx");

    await loadConfig();
    restoreDraft();

    $("btn-add-tournament").addEventListener("click", addTournament);
    $("btn-add-tournament-empty").addEventListener("click", addTournament);
    $("btn-remove-tournament").addEventListener("click", removeTournament);
    $("btn-save-settings").addEventListener("click", function () {
      flushEditorToState();
      ReportIO.downloadJson(
        ReportIO.timestampName("web_report_settings", "json"),
        ReportCore.serializeSettings(state.tournaments)
      );
      showToast("JSON настроек");
    });
    $("import-settings").addEventListener("change", async function (ev) {
      var file = ev.target.files && ev.target.files[0];
      ev.target.value = "";
      if (!file) {
        return;
      }
      try {
        var data = await readJsonFile(file);
        state.tournaments = ReportCore.parseSettings(data);
        state.dataByTournament = {};
        state.lastResult = null;
        state.activeId = state.tournaments[0] ? state.tournaments[0].id : null;
        renderAll();
        showToast("Настройки загружены");
        setStatus("загружен JSON настроек");
      } catch (err) {
        alert(err.message || String(err));
      }
    });

    $("btn-save-fio").addEventListener("click", function () {
      ReportIO.downloadJson(
        ReportIO.timestampName("fio_dictionary", "json"),
        ReportCore.serializeFioDictionary(state.fioEntries)
      );
      showToast("Справочник ФИО");
    });
    $("import-fio").addEventListener("change", async function (ev) {
      var file = ev.target.files && ev.target.files[0];
      ev.target.value = "";
      if (!file) {
        return;
      }
      try {
        var data = await readJsonFile(file);
        state.fioEntries = ReportCore.parseFioDictionary(data);
        persistDraft();
        renderFioStats();
        showToast("Справочник загружен");
      } catch (err) {
        alert(err.message || String(err));
      }
    });

    $("import-data").addEventListener("change", function (ev) {
      var file = ev.target.files && ev.target.files[0];
      ev.target.value = "";
      if (file) {
        onImportData(file);
      }
    });

    $("btn-process").addEventListener("click", function () {
      runProcess().catch(function (err) {
        console.error(err);
        alert(err.message || String(err));
      });
    });
    $("btn-export-csv").addEventListener("click", exportCsv);
    $("btn-export-xlsx").addEventListener("click", exportXlsx);
    $("btn-export-csv-2").addEventListener("click", exportCsv);
    $("btn-export-xlsx-2").addEventListener("click", exportXlsx);

    bindFieldSync();
    initTips();
    renderAll();
    setStatus("готово к работе");
  }

  function initTips() {
    var tip = $("glassTip");
    if (!tip) {
      return;
    }
    document.addEventListener("mouseover", function (ev) {
      var el = ev.target.closest("[data-tip]");
      if (!el) {
        tip.hidden = true;
        tip.setAttribute("aria-hidden", "true");
        return;
      }
      tip.textContent = el.getAttribute("data-tip") || "";
      tip.hidden = false;
      tip.setAttribute("aria-hidden", "false");
      var r = el.getBoundingClientRect();
      tip.style.left = Math.min(window.innerWidth - 20, r.left + r.width / 2) + "px";
      tip.style.top = Math.max(8, r.top - 8) + "px";
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
