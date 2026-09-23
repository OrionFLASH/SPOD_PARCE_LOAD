/**
 * UI web-report: панели, этапы, копия турнира, проверка, маппинг данных.
 */
(function () {
  "use strict";

  var state = {
    config: null,
    tournaments: [],
    activeId: null,
    dataByTournament: {},
    fioEntries: [],
    lastResult: null,
    checkState: { duplicatesCleared: false, missingFioCleared: false },
    lastResolutions: {},
    filters: { search: "", types: { TN: true, FIO: true }, ready: { ready: true, draft: true, copy: true } },
    sidebarOpen: true,
    filtersOpen: true,
    chromeOpen: true,
  };

  function $(id) {
    return document.getElementById(id);
  }

  function setStatus(text) {
    var el = $("footer-status");
    if (el) el.textContent = "Статус: " + text;
  }

  function showToast(text) {
    var toast = $("save-toast");
    var label = $("save-toast-text");
    if (!toast || !label) return;
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
    } catch (e) {}
  }

  function restoreDraft() {
    try {
      var keys = storageKeys();
      var s = localStorage.getItem(keys.settings);
      var f = localStorage.getItem(keys.fio);
      if (s) state.tournaments = ReportCore.parseSettings(JSON.parse(s));
      if (f) state.fioEntries = ReportCore.parseFioDictionary(JSON.parse(f));
    } catch (e) {
      console.warn(e);
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

  function invalidateChecks() {
    state.checkState = { duplicatesCleared: false, missingFioCleared: false };
    state.lastResolutions = {};
    state.lastResult = null;
  }

  function stages() {
    return ReportCore.computeStages(state.tournaments, state.dataByTournament, state.fioEntries, state.checkState);
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function tournamentReadyKind(t) {
    if (!ReportCore.tournamentIdentityUnlocked(t)) return "copy";
    var pack = state.dataByTournament[t.id];
    if (ReportCore.tournamentFieldsOk(t) && ReportCore.tournamentSourceOk(t, pack)) return "ready";
    return "draft";
  }

  function matchesFilters(t) {
    var type = String(t.type_ind || "TN").toUpperCase() === "FIO" ? "FIO" : "TN";
    if (!state.filters.types[type]) return false;
    var kind = tournamentReadyKind(t);
    if (!state.filters.ready[kind]) return false;
    var q = String(state.filters.search || "").trim().toLowerCase();
    if (!q) return true;
    var hay = [t.contest_code, t.tournament_code, t.full_name, t.type_ind].join(" ").toLowerCase();
    return hay.indexOf(q) >= 0;
  }

  function setSidebarOpen(open) {
    state.sidebarOpen = !!open;
    var app = $("app-root");
    if (app) app.classList.toggle("is-sidebar-collapsed", !state.sidebarOpen);
    var hide = $("btn-sidebar-hide");
    var show = $("btn-sidebar-show");
    if (hide) hide.setAttribute("aria-expanded", state.sidebarOpen ? "true" : "false");
    if (show) show.setAttribute("aria-expanded", state.sidebarOpen ? "true" : "false");
  }

  function setFiltersOpen(open) {
    state.filtersOpen = !!open;
    var app = $("app-root");
    if (app) app.classList.toggle("is-filters-collapsed", !state.filtersOpen);
  }

  function setChromeOpen(open) {
    state.chromeOpen = !!open;
    var app = $("app-root");
    if (app) app.classList.toggle("is-chrome-collapsed", !state.chromeOpen);
    var btn = $("btn-chrome-toggle");
    if (btn) btn.setAttribute("aria-expanded", state.chromeOpen ? "true" : "false");
  }

  function renderStages() {
    var st = stages();
    var box = $("top-stages");
    if (!box) return;
    var items = [
      { ok: st.hasTournaments, label: "Турниры" },
      { ok: st.fieldsFilled, label: "Поля" },
      { ok: st.sourcesOk, label: "Источники" },
      { ok: st.fioOk, label: "ФИО / данные" },
      { ok: st.duplicatesOk, label: "Без дублей" },
    ];
    box.innerHTML = items
      .map(function (it) {
        var cls = "stage-chip " + (it.ok ? "is-done" : "is-bad");
        return (
          '<span class="' +
          cls +
          '"><span class="stage-chip__dot"></span>' +
          escapeHtml(it.label) +
          "</span>"
        );
      })
      .join("");

    $("btn-process").disabled =
      !st.hasTournaments || !st.fieldsFilled || !st.sourcesOk || st.blockedCopies.length > 0;
    $("btn-check").disabled = !st.canCheck;
    $("btn-export-csv").disabled = !(state.lastResult && state.lastResult.ok);
    $("btn-export-xlsx").disabled = !(state.lastResult && state.lastResult.ok);
  }

  function renderNav() {
    var nav = $("tournament-nav");
    nav.innerHTML = "";
    state.tournaments.filter(matchesFilters).forEach(function (t) {
      var btn = document.createElement("button");
      btn.type = "button";
      var cls = "contest-tab";
      if (t.id === state.activeId) cls += " active";
      if (!ReportCore.tournamentIdentityUnlocked(t)) cls += " is-copy-lock";
      btn.className = cls;
      var kind = tournamentReadyKind(t);
      var type = String(t.type_ind || "TN").toUpperCase();
      btn.innerHTML =
        '<div class="ct-text">' +
        '<div class="ct-code">' +
        escapeHtml(t.tournament_code || t.contest_code || t.id) +
        "</div>" +
        '<div class="ct-name">' +
        escapeHtml(t.full_name || "Без названия") +
        "</div>" +
        '<div class="ct-badges">' +
        '<span class="mini-badge' +
        (type === "FIO" ? " mini-badge--fio" : "") +
        '">' +
        type +
        "</span>" +
        '<span class="mini-badge' +
        (kind === "ready" ? " mini-badge--ok" : " mini-badge--warn") +
        '">' +
        (kind === "copy" ? "КОПИЯ" : kind === "ready" ? "OK" : "DRAFT") +
        "</span>" +
        "</div></div>";
      btn.addEventListener("click", function () {
        flushEditorToState();
        state.activeId = t.id;
        renderAll();
      });
      nav.appendChild(btn);
    });
    $("fio-stats").textContent = "ФИО в справочнике: " + state.fioEntries.length;
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
      if (name === selected) opt.selected = true;
      select.appendChild(opt);
    });
  }

  function renderPreviewTable(pack, limit) {
    var cols = (pack.columns || []).slice(0, 8);
    var rows = (pack.rows || []).slice(0, limit || 12);
    var head = cols.map(function (c) {
      return "<th>" + escapeHtml(c) + "</th>";
    }).join("");
    var body = rows
      .map(function (row) {
        return (
          "<tr>" +
          cols
            .map(function (c) {
              return "<td>" + escapeHtml(row[c] == null ? "" : row[c]) + "</td>";
            })
            .join("") +
          "</tr>"
        );
      })
      .join("");
    return (
      '<div class="preview-table-wrap"><table class="preview-table"><thead><tr>' +
      head +
      "</tr></thead><tbody>" +
      body +
      "</tbody></table></div>"
    );
  }

  function renderWorkspace() {
    var ws = $("workspace");
    var t = activeTournament();
    if (!t) {
      ws.innerHTML =
        '<div class="panel"><h2>Нет турниров</h2><p class="panel__intro">Добавьте турнир слева или откройте JSON настроек.</p>' +
        '<div class="toolbar-row"><button type="button" class="btn btn-primary" id="btn-add-tournament-empty">' +
        '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 5v14"/><path d="M5 12h14"/></svg> Добавить турнир</button></div></div>';
      var addEmpty = $("btn-add-tournament-empty");
      if (addEmpty) addEmpty.addEventListener("click", addTournament);
      return;
    }

    var pack = state.dataByTournament[t.id];
    var lock = !ReportCore.tournamentIdentityUnlocked(t);
    var codeHighlight = lock ? " is-highlight" : "";
    var nameHighlight = lock ? " is-highlight" : "";

    var sheetBlock = "";
    if (pack && pack.kind === "excel" && pack.sheetNames && pack.sheetNames.length) {
      sheetBlock =
        '<div class="field field--third"><label class="field-label" for="f-sheet">Лист Excel</label>' +
        '<select class="field-select" id="f-sheet"></select></div>';
    }

    ws.innerHTML =
      '<div class="panel" id="panel-params">' +
      "<h2>Параметры турнира</h2>" +
      '<p class="panel__intro">Заполните поля. Название — длинное поле на всю ширину.</p>' +
      (lock
        ? '<div class="warn-box">Копия турнира: обязательно смените <b>код турнира</b> и <b>наименование</b> (оба поля подсвечены). Пока они совпадают с оригиналом — формирование заблокировано.</div>'
        : "") +
      '<div class="fields-grid">' +
      '<div class="field"><label class="field-label" for="f-contest-code">Код конкурса</label><input class="field-input" id="f-contest-code" /></div>' +
      '<div class="field"><label class="field-label" for="f-tournament-code">Код турнира</label><input class="field-input' +
      codeHighlight +
      '" id="f-tournament-code" />' +
      (lock ? '<div class="field-hint">Нужно изменить относительно копии</div>' : "") +
      "</div>" +
      '<div class="field field--full"><label class="field-label" for="f-full-name">Наименование турнира</label><input class="field-input' +
      nameHighlight +
      '" id="f-full-name" />' +
      (lock ? '<div class="field-hint">Нужно изменить относительно копии</div>' : "") +
      "</div>" +
      '<div class="field field--third"><label class="field-label" for="f-plan">План (PLAN_VALUE)</label><input class="field-input" id="f-plan" /></div>' +
      '<div class="field field--third"><label class="field-label" for="f-date">Дата данных</label><input class="field-input" id="f-date" type="date" /></div>' +
      '<div class="field field--third"><label class="field-label" for="f-type">Тип расчёта</label><select class="field-select" id="f-type"><option value="TN">TN — табельный</option><option value="FIO">FIO — ФИО</option></select></div>' +
      "</div></div>" +
      '<div class="panel" id="panel-data">' +
      "<h2>Источник данных</h2>" +
      '<p class="panel__intro">Загрузите CSV (; ) или Excel. Выберите лист, колонки и посмотрите пример строк.</p>' +
      '<div class="toolbar-row">' +
      '<label class="btn btn-primary file-pick"><svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 21V9"/><path d="M7 14l5-5 5 5"/><path d="M5 3h14"/></svg> Загрузить CSV / Excel' +
      '<input type="file" id="import-data" class="file-pick__input" accept=".csv,.txt,.xlsx,.xls,.xlsm" /></label>' +
      '<span class="mini-badge" id="data-meta"></span></div>' +
      '<div class="fields-grid" style="margin-top:12px">' +
      sheetBlock +
      '<div class="field"><label class="field-label" for="f-col-id">Колонка ФИО / табельного</label><select class="field-select" id="f-col-id"></select></div>' +
      '<div class="field"><label class="field-label" for="f-col-fact">Колонка показателя</label><select class="field-select" id="f-col-fact"></select></div>' +
      '<div class="field field--full"><label class="field-label" for="f-source-name">Имя файла</label><input class="field-input" id="f-source-name" readonly /></div>' +
      "</div>" +
      '<div id="data-preview-host"></div>' +
      "</div>" +
      '<div class="panel" id="panel-result" hidden>' +
      "<h2>Результат</h2>" +
      '<div id="result-summary" class="info-box"></div>' +
      '<div id="result-warnings" class="warn-box" hidden></div>' +
      '<div class="toolbar-row">' +
      '<button type="button" class="btn btn-primary" id="btn-export-csv-2">Скачать CSV</button>' +
      '<button type="button" class="btn btn-primary" id="btn-export-xlsx-2">Скачать XLSX</button>' +
      "</div></div>";

    $("f-contest-code").value = t.contest_code || "";
    $("f-tournament-code").value = t.tournament_code || "";
    $("f-full-name").value = t.full_name || "";
    $("f-plan").value = t.plan_value || "";
    $("f-date").value = t.contest_date || "";
    $("f-type").value = String(t.type_ind || "TN").toUpperCase() === "FIO" ? "FIO" : "TN";
    $("f-source-name").value = t.source_file_name || "";

    var colId = $("f-col-id");
    var colFact = $("f-col-fact");
    if (pack) {
      fillSelect(colId, pack.columns, t.column_id || "");
      fillSelect(colFact, pack.columns, t.column_fact || "");
      $("data-meta").textContent =
        pack.rows.length +
        " строк" +
        (pack.encoding ? ", " + pack.encoding : "") +
        (pack.sheetName ? ", лист: " + pack.sheetName : "");
      $("data-preview-host").innerHTML = renderPreviewTable(pack);
      if (pack.kind === "excel" && $("f-sheet")) {
        fillSelect($("f-sheet"), pack.sheetNames, t.sheet_name || pack.sheetName || "");
        $("f-sheet").addEventListener("change", function () {
          var name = $("f-sheet").value;
          var next = ReportIO.pickSheetFromPack(pack, name);
          state.dataByTournament[t.id] = next;
          t.sheet_name = name;
          t.column_id = ReportIO.guessIdColumn(next.columns, t.type_ind);
          t.column_fact = ReportIO.guessFactColumn(next.columns);
          invalidateChecks();
          renderAll();
        });
      }
    } else {
      fillSelect(colId, [], "");
      fillSelect(colFact, [], "");
      $("data-meta").textContent = "файл не загружен";
      $("data-preview-host").innerHTML = '<div class="warn-box">Загрузите файл — здесь появятся пример строк и выбор колонок.</div>';
    }

    bindEditorEvents();
    if (state.lastResult && state.lastResult.ok) {
      var panel = $("panel-result");
      panel.hidden = false;
      $("result-summary").textContent =
        "Строк XLSX: " + state.lastResult.xlsxRows.length + "; CSV: " + state.lastResult.csvRows.length;
      var w = $("result-warnings");
      if (state.lastResult.missingFio && state.lastResult.missingFio.length) {
        w.hidden = false;
        w.textContent = "ФИО без табельного: " + state.lastResult.missingFio.join("; ");
      } else {
        w.hidden = true;
      }
      $("btn-export-csv-2").onclick = exportCsv;
      $("btn-export-xlsx-2").onclick = exportXlsx;
    }
  }

  function bindEditorEvents() {
    ["f-contest-code", "f-tournament-code", "f-full-name", "f-plan", "f-date", "f-type", "f-col-id", "f-col-fact"].forEach(
      function (id) {
        var el = $(id);
        if (!el) return;
        el.addEventListener("change", onEditorChange);
        el.addEventListener("input", onEditorChange);
      }
    );
    var importData = $("import-data");
    if (importData) {
      importData.addEventListener("change", function (ev) {
        var file = ev.target.files && ev.target.files[0];
        ev.target.value = "";
        if (file) onImportData(file);
      });
    }
  }

  function onEditorChange() {
    flushEditorToState();
    var t = activeTournament();
    if (t && t.needs_identity_fix && ReportCore.tournamentIdentityUnlocked(t)) {
      t.needs_identity_fix = false;
      t.copy_lock_code = "";
      t.copy_lock_name = "";
      showToast("Копия разблокирована");
    }
    invalidateChecks();
    persistDraft();
    renderStages();
    renderNav();
    // обновить подсветку полей без полного рендера при вводе кода/имени
    var lock = t && !ReportCore.tournamentIdentityUnlocked(t);
    ["f-tournament-code", "f-full-name"].forEach(function (id) {
      var el = $(id);
      if (!el) return;
      el.classList.toggle("is-highlight", !!lock);
    });
  }

  function flushEditorToState() {
    var t = activeTournament();
    if (!t) return;
    if (!$("f-contest-code")) return;
    t.contest_code = $("f-contest-code").value.trim();
    t.tournament_code = $("f-tournament-code").value.trim();
    t.full_name = $("f-full-name").value.trim();
    t.plan_value = $("f-plan").value.trim();
    t.contest_date = $("f-date").value.trim();
    t.type_ind = $("f-type").value;
    t.column_id = $("f-col-id").value;
    t.column_fact = $("f-col-fact").value;
    if ($("f-sheet")) t.sheet_name = $("f-sheet").value;
  }

  function renderAll() {
    ensureActive();
    renderStages();
    renderNav();
    renderWorkspace();
    persistDraft();
  }

  function addTournament() {
    flushEditorToState();
    var t = ReportCore.createEmptyTournament({
      contest_date: new Date().toISOString().slice(0, 10),
      type_ind: "TN",
    });
    state.tournaments.push(t);
    state.activeId = t.id;
    invalidateChecks();
    renderAll();
    setStatus("добавлен турнир");
  }

  function copyTournament() {
    flushEditorToState();
    var src = activeTournament();
    if (!src) {
      alert("Сначала выберите турнир");
      return;
    }
    var copy = ReportCore.cloneTournament(src);
    state.tournaments.push(copy);
    var pack = state.dataByTournament[src.id];
    if (pack) {
      state.dataByTournament[copy.id] = JSON.parse(JSON.stringify({
        kind: pack.kind,
        rows: pack.rows,
        columns: pack.columns,
        encoding: pack.encoding,
        fileName: pack.fileName,
        sheetName: pack.sheetName,
        sheetNames: pack.sheetNames,
        sheets: pack.sheets,
      }));
    }
    state.activeId = copy.id;
    invalidateChecks();
    renderAll();
    setStatus("создана копия — смените код и название");
    showToast("Смените код турнира и название");
  }

  function removeTournament() {
    flushEditorToState();
    if (!state.activeId) return;
    var id = state.activeId;
    state.tournaments = state.tournaments.filter(function (t) {
      return t.id !== id;
    });
    delete state.dataByTournament[id];
    state.activeId = state.tournaments[0] ? state.tournaments[0].id : null;
    invalidateChecks();
    renderAll();
    setStatus("турнир удалён");
  }

  async function onImportData(file) {
    flushEditorToState();
    var t = activeTournament();
    if (!t) return;
    try {
      setStatus("чтение файла…");
      var pack = await ReportIO.readTableFile(file);
      // не храним ArrayBuffer в state
      if (pack.buffer) delete pack.buffer;
      state.dataByTournament[t.id] = pack;
      t.source_file_name = pack.fileName;
      t.sheet_name = pack.sheetName || "";
      t.column_id = ReportIO.guessIdColumn(pack.columns, t.type_ind);
      t.column_fact = ReportIO.guessFactColumn(pack.columns);
      invalidateChecks();
      renderAll();
      showToast("Данные загружены");
      setStatus("данные: " + pack.rows.length + " строк");
    } catch (err) {
      console.error(err);
      alert(err.message || String(err));
      setStatus("ошибка загрузки");
    }
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
      missingList.forEach(function (fio) {
        var row = document.createElement("div");
        row.className = "fio-row";
        row.innerHTML =
          '<input class="field-input" data-fio-name readonly />' +
          '<input class="field-input" data-fio-num placeholder="табельный" autocomplete="off" />';
        row.querySelector("[data-fio-name]").value = fio;
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
        inputs.forEach(function (inp, idx) {
          var num = inp.value.trim();
          if (!num) return;
          var fio = missingList[idx];
          var key = ReportCore.normalizeFioKey(fio);
          state.fioEntries = state.fioEntries.filter(function (e) {
            return ReportCore.normalizeFioKey(e.fio) !== key;
          });
          state.fioEntries.push({ fio: fio, person_number: num });
        });
        cleanup();
        persistDraft();
        resolve({ action: "apply" });
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

  function askDuplicates(groups) {
    return new Promise(function (resolve) {
      var list = $("modal-dup-list");
      list.innerHTML = "";
      groups.forEach(function (g, gi) {
        var box = document.createElement("div");
        box.className = "dup-group";
        box.dataset.key = g.key;
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
          " · " +
          g.indices.length +
          " · сумма " +
          ReportCore.formatNumberDot(g.sum, coreOpts().numberDecimals) +
          "</div>" +
          '<label class="field-label">Действие</label>' +
          '<select class="field-select" data-dup-mode>' +
          '<option value="sum">Сумма показателя</option>' +
          '<option value="keep_one">Выбрать одну строку</option>' +
          '<option value="drop_all">Убрать все из CSV</option>' +
          "</select>" +
          '<div style="margin-top:8px">' +
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

  function buildAllRows() {
    var fioMap = ReportCore.buildFioMap(state.fioEntries);
    var allRows = [];
    var allMissing = [];
    var seen = Object.create(null);
    state.tournaments.forEach(function (t) {
      var pack = state.dataByTournament[t.id] || { rows: [] };
      var part = ReportCore.normalizeTournamentRows(pack.rows, t, fioMap, coreOpts());
      allRows = allRows.concat(part.rows);
      part.missingFio.forEach(function (fio) {
        var k = ReportCore.normalizeFioKey(fio);
        if (!seen[k]) {
          seen[k] = true;
          allMissing.push(fio);
        }
      });
    });
    ReportCore.annotateDuplicatesLikePq(allRows, coreOpts());
    return { rows: allRows, missingFio: allMissing, groups: ReportCore.findDuplicateGroups(allRows) };
  }

  function validateBaseOrAlert() {
    flushEditorToState();
    var st = stages();
    if (!st.hasTournaments) {
      alert("Добавьте хотя бы один турнир");
      return false;
    }
    if (st.blockedCopies.length) {
      alert("После копирования смените код турнира и наименование у копии");
      return false;
    }
    if (!st.fieldsFilled) {
      alert("Заполните все параметры турниров (код конкурса, код турнира, план, дата, название, тип)");
      return false;
    }
    if (!st.sourcesOk) {
      alert("У каждого турнира загрузите данные и укажите колонки идентификатора и показателя");
      return false;
    }
    return true;
  }

  async function runCheck() {
    if (!validateBaseOrAlert()) return;
    setStatus("проверка…");
    var built = buildAllRows();
    var okFio = built.missingFio.length === 0;
    var okDup = built.groups.length === 0;
    var body = $("modal-check-body");
    var title = $("modal-check-title");

    if (okFio && okDup) {
      state.checkState = { duplicatesCleared: true, missingFioCleared: true };
      title.textContent = "Проверка: всё ОК";
      body.innerHTML = '<div class="ok-box">Ненайденных ФИО нет. Дублей нет. Можно формировать отчёт.</div>';
      openModal("modal-check");
      $("modal-check-close").onclick = function () {
        closeModal("modal-check");
      };
      renderStages();
      setStatus("проверка OK");
      showToast("Проверка OK");
      return;
    }

    title.textContent = "Проверка: есть замечания";
    var html = "";
    if (!okFio) {
      html +=
        '<div class="warn-box"><b>Не найдены ФИО (' +
        built.missingFio.length +
        "):</b> " +
        escapeHtml(built.missingFio.join("; ")) +
        "</div>";
    }
    if (!okDup) {
      html +=
        '<div class="warn-box"><b>Дубли:</b> групп ' +
        built.groups.length +
        ". Можно разрешить в диалоге.</div>";
    }
    html +=
      '<div class="toolbar-row" style="margin-top:12px">' +
      (!okFio ? '<button type="button" class="btn btn-primary" id="check-fix-fio">Дополнить ФИО</button>' : "") +
      (!okDup ? '<button type="button" class="btn btn-primary" id="check-fix-dup">Разрешить дубли</button>' : "") +
      "</div>";
    body.innerHTML = html;
    openModal("modal-check");

    $("modal-check-close").onclick = function () {
      closeModal("modal-check");
    };
    var btnFio = $("check-fix-fio");
    if (btnFio) {
      btnFio.onclick = async function () {
        closeModal("modal-check");
        var ans = await askMissingFio(built.missingFio.slice());
        if (ans.action === "cancel") return;
        if (ans.action === "skip") state.checkState.missingFioCleared = true;
        if (ans.action === "apply") {
          var again = buildAllRows();
          state.checkState.missingFioCleared = again.missingFio.length === 0;
        }
        renderAll();
        runCheck();
      };
    }
    var btnDup = $("check-fix-dup");
    if (btnDup) {
      btnDup.onclick = async function () {
        closeModal("modal-check");
        var built2 = buildAllRows();
        var ans = await askDuplicates(built2.groups);
        if (ans.action === "cancel") return;
        if (ans.action === "abort") {
          state.checkState.duplicatesCleared = false;
          setStatus("проверка: дубли не разрешены");
          return;
        }
        state.lastResolutions = ans.resolutions || {};
        state.checkState.duplicatesCleared = true;
        renderStages();
        setStatus("дубли разрешены");
        showToast("Дубли разрешены");
        runCheck();
      };
    }
    setStatus("проверка: есть замечания");
  }

  async function runProcess() {
    if (!validateBaseOrAlert()) return;
    setStatus("обработка…");

    var built = buildAllRows();
    if (built.missingFio.length && !state.checkState.missingFioCleared) {
      var fioAns = await askMissingFio(built.missingFio.slice());
      if (fioAns.action === "cancel") {
        setStatus("отменено");
        return;
      }
      if (fioAns.action === "skip") state.checkState.missingFioCleared = true;
      built = buildAllRows();
      if (fioAns.action === "apply" && built.missingFio.length === 0) {
        state.checkState.missingFioCleared = true;
      }
    }

    built = buildAllRows();
    var resolutions = state.lastResolutions || {};
    if (built.groups.length && !state.checkState.duplicatesCleared) {
      var dupAns = await askDuplicates(built.groups);
      if (dupAns.action === "cancel") {
        setStatus("отменено");
        return;
      }
      if (dupAns.action === "abort") {
        setStatus("остановлено из‑за дублей");
        alert("Формирование остановлено");
        return;
      }
      resolutions = dupAns.resolutions || {};
      state.lastResolutions = resolutions;
      state.checkState.duplicatesCleared = true;
    }

    var applied = ReportCore.applyDuplicateResolutions(built.rows, resolutions, coreOpts());
    if (!applied.ok) {
      alert(applied.error || "Ошибка");
      setStatus(applied.error || "ошибка");
      return;
    }

    state.checkState.missingFioCleared = true;
    state.checkState.duplicatesCleared = true;
    state.lastResult = {
      ok: true,
      rows: applied.rows,
      missingFio: built.missingFio,
      csvRows: ReportCore.rowsForCsv(applied.rows),
      xlsxRows: ReportCore.rowsForXlsx(applied.rows),
    };
    renderAll();
    showToast("Готово");
    setStatus("сформировано CSV " + state.lastResult.csvRows.length + " / XLSX " + state.lastResult.xlsxRows.length);
  }

  function exportCsv() {
    if (!state.lastResult || !state.lastResult.ok) return;
    ReportIO.downloadReportCsv(state.lastResult.csvRows, ReportIO.timestampName("REPORT", "csv"));
    showToast("CSV сохранён");
  }

  function exportXlsx() {
    if (!state.lastResult || !state.lastResult.ok) return;
    ReportIO.downloadReportXlsx(state.lastResult.xlsxRows, ReportIO.timestampName("REPORT", "xlsx"));
    showToast("XLSX сохранён");
  }

  function readJsonFile(file) {
    return file.text().then(function (text) {
      return JSON.parse(text);
    });
  }

  async function loadConfig() {
    try {
      var res = await fetch("config.json", { cache: "no-store" });
      state.config = await res.json();
      window.ReportConfig = state.config;
    } catch (e) {
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
      window.ReportConfig = state.config;
    }
  }

  function initTips() {
    var tip = $("glassTip");
    if (!tip) return;
    document.addEventListener("mouseover", function (ev) {
      var el = ev.target.closest("[data-tip]");
      if (!el) {
        tip.hidden = true;
        return;
      }
      tip.textContent = el.getAttribute("data-tip") || "";
      tip.hidden = false;
      var r = el.getBoundingClientRect();
      tip.style.left = Math.min(window.innerWidth - 20, r.left + r.width / 2) + "px";
      tip.style.top = Math.max(8, r.top - 8) + "px";
    });
  }

  async function init() {
    await loadConfig();
    restoreDraft();

    $("btn-add-tournament").addEventListener("click", addTournament);
    $("btn-copy-tournament").addEventListener("click", copyTournament);
    $("btn-remove-tournament").addEventListener("click", removeTournament);
    $("btn-save-settings").addEventListener("click", function () {
      flushEditorToState();
      ReportIO.downloadJson(ReportIO.timestampName("web_report_settings", "json"), ReportCore.serializeSettings(state.tournaments));
      showToast("JSON настроек");
    });
    $("import-settings").addEventListener("change", async function (ev) {
      var file = ev.target.files && ev.target.files[0];
      ev.target.value = "";
      if (!file) return;
      try {
        var data = await readJsonFile(file);
        state.tournaments = ReportCore.parseSettings(data);
        state.dataByTournament = {};
        invalidateChecks();
        state.activeId = state.tournaments[0] ? state.tournaments[0].id : null;
        renderAll();
        showToast("Настройки загружены");
      } catch (err) {
        alert(err.message || String(err));
      }
    });
    $("btn-save-fio").addEventListener("click", function () {
      ReportIO.downloadJson(ReportIO.timestampName("fio_dictionary", "json"), ReportCore.serializeFioDictionary(state.fioEntries));
      showToast("Справочник ФИО");
    });
    $("import-fio").addEventListener("change", async function (ev) {
      var file = ev.target.files && ev.target.files[0];
      ev.target.value = "";
      if (!file) return;
      try {
        state.fioEntries = ReportCore.parseFioDictionary(await readJsonFile(file));
        invalidateChecks();
        renderAll();
        showToast("Справочник загружен");
      } catch (err) {
        alert(err.message || String(err));
      }
    });

    $("btn-process").addEventListener("click", function () {
      runProcess().catch(function (err) {
        console.error(err);
        alert(err.message || String(err));
      });
    });
    $("btn-check").addEventListener("click", function () {
      runCheck().catch(function (err) {
        console.error(err);
        alert(err.message || String(err));
      });
    });
    $("btn-export-csv").addEventListener("click", exportCsv);
    $("btn-export-xlsx").addEventListener("click", exportXlsx);

    $("btn-sidebar-hide").addEventListener("click", function () {
      setSidebarOpen(false);
    });
    $("btn-sidebar-show").addEventListener("click", function () {
      setSidebarOpen(true);
    });
    $("btn-filters-hide").addEventListener("click", function () {
      setFiltersOpen(false);
    });
    $("btn-filters-show").addEventListener("click", function () {
      setFiltersOpen(true);
    });
    $("btn-chrome-toggle").addEventListener("click", function () {
      setChromeOpen(!state.chromeOpen);
    });

    $("filter-search").addEventListener("input", function (ev) {
      state.filters.search = ev.target.value;
      renderNav();
    });
    document.querySelectorAll("[data-filter-type]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var key = btn.getAttribute("data-filter-type");
        state.filters.types[key] = !state.filters.types[key];
        btn.classList.toggle("is-on", state.filters.types[key]);
        btn.setAttribute("aria-pressed", state.filters.types[key] ? "true" : "false");
        renderNav();
      });
    });
    document.querySelectorAll("[data-filter-ready]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var key = btn.getAttribute("data-filter-ready");
        state.filters.ready[key] = !state.filters.ready[key];
        btn.classList.toggle("is-on", state.filters.ready[key]);
        btn.setAttribute("aria-pressed", state.filters.ready[key] ? "true" : "false");
        renderNav();
      });
    });

    initTips();
    setSidebarOpen(true);
    setFiltersOpen(true);
    setChromeOpen(true);
    renderAll();
    setStatus("готово к работе");
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
