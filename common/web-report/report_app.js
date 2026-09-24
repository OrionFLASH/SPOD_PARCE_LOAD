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
    fioPack: null,
    fioUi: {
      sheet_name: "",
      start_row: 1,
      start_col: 1,
      col_fio: "",
      col_tn: "",
      file_name: "",
      source_file_b64: "",
      source_file_kind: "",
      source_error: "",
    },
    lastResult: null,
    checkState: {
      duplicatesCleared: false,
      fioDupCleared: false,
      missingFioCleared: false,
    },
    lastResolutions: {},
    lastFioResolutions: {},
    checkedPipeline: null,
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
    // черновик в localStorage не используем: старт всегда пустой (п.7)
  }

  function restoreDraft() {
    // намеренно пусто — турниры только через «Добавить» / JSON
  }

  function clearLegacyStorage() {
    try {
      var keys = storageKeys();
      localStorage.removeItem(keys.settings);
      localStorage.removeItem(keys.fio);
    } catch (e) {}
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

  function invalidateChecks(opts) {
    var o = opts || {};
    state.checkState = {
      duplicatesCleared: false,
      fioDupCleared: false,
      missingFioCleared: false,
    };
    if (!o.keepResolutions) {
      state.lastResolutions = {};
      state.lastFioResolutions = {};
    }
    state.checkedPipeline = null;
    state.lastResult = null;
  }

  function clearResolutions() {
    state.lastResolutions = {};
    state.lastFioResolutions = {};
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
    if (!ReportCore.tournamentIncluded(t)) return "off";
    if (!ReportCore.tournamentIdentityUnlocked(t)) return "copy";
    var pack = state.dataByTournament[t.id];
    if (ReportCore.tournamentFieldsOk(t) && ReportCore.tournamentSourceOk(t, pack)) return "ready";
    return "draft";
  }

  function matchesFilters(t) {
    var type = String(t.type_ind || "TN").toUpperCase() === "FIO" ? "FIO" : "TN";
    if (!state.filters.types[type]) return false;
    var kind = tournamentReadyKind(t);
    var readyMap = state.filters.ready;
    if (kind === "off") {
      // выключенные показываем всегда, если включён draft или ready
      if (!readyMap.draft && !readyMap.ready && !readyMap.copy) return false;
    } else if (!readyMap[kind]) {
      return false;
    }
    var q = String(state.filters.search || "").trim().toLowerCase();
    if (!q) return true;
    var hay = [t.contest_code, t.tournament_code, t.full_name, t.type_ind, t.period_code].join(" ").toLowerCase();
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

  function hasActiveFioMode() {
    var t = activeTournament();
    return !!(t && String(t.type_ind || "").toUpperCase() === "FIO");
  }

  function hasAnyFioMode() {
    return state.tournaments.some(function (t) {
      return String(t.type_ind || "").toUpperCase() === "FIO";
    });
  }

  function renderNav() {
    var nav = $("tournament-nav");
    nav.innerHTML = "";
    var periodBadges = ReportCore.periodBadgesForTournaments(state.tournaments);
    state.tournaments.filter(matchesFilters).forEach(function (t) {
      var btn = document.createElement("button");
      btn.type = "button";
      var cls = "contest-tab";
      if (t.id === state.activeId) cls += " active";
      if (!ReportCore.tournamentIdentityUnlocked(t)) cls += " is-copy-lock";
      if (!ReportCore.tournamentIncluded(t)) cls += " is-excluded";
      if (t.source_error) cls += " is-error";
      btn.className = cls;
      var kind = tournamentReadyKind(t);
      var type = String(t.type_ind || "TN").toUpperCase();
      var periodBadge = periodBadges[t.id] || ReportCore.normalizePeriodCode(t.period_code);
      var actions =
        t.id === state.activeId
          ? '<div class="ct-actions">' +
            '<button type="button" class="tab-icon-btn" data-act="copy" data-tip="Копировать турнир (данные и параметры)">' +
            '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="11" height="11" rx="2"/><path d="M5 15V5a2 2 0 0 1 2-2h10"/></svg></button>' +
            '<button type="button" class="tab-icon-btn tab-icon-btn--danger" data-act="remove" data-tip="Удалить турнир">' +
            '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 7h16"/><path d="M10 11v6"/><path d="M14 11v6"/><path d="M6 7l1 12a2 2 0 0 0 2 2h6a2 2 0 0 0 2-2l1-12"/><path d="M9 7V5a2 2 0 0 1 2-2h2a2 2 0 0 1 2 2v2"/></svg></button>' +
            "</div>"
          : "";
      btn.innerHTML =
        '<div class="ct-text">' +
        '<div class="ct-code">' +
        escapeHtml(t.tournament_code || t.contest_code || t.id) +
        "</div>" +
        '<div class="ct-name">' +
        escapeHtml(t.full_name || "Без названия") +
        "</div>" +
        '<div class="ct-badges">' +
        '<span class="mini-badge mini-badge--period" data-tip="' +
        escapeHtml(ReportCore.periodDisplay(t.period_code)) +
        '">' +
        escapeHtml(periodBadge) +
        "</span>" +
        '<span class="mini-badge' +
        (type === "FIO" ? " mini-badge--fio" : "") +
        '">' +
        type +
        "</span>" +
        '<span class="mini-badge' +
        (kind === "ready" ? " mini-badge--ok" : kind === "off" ? " mini-badge--off" : " mini-badge--warn") +
        '">' +
        (kind === "copy" ? "КОПИЯ" : kind === "ready" ? "OK" : kind === "off" ? "ВЫКЛ" : "DRAFT") +
        "</span>" +
        "</div></div>" +
        actions;
      btn.addEventListener("click", function (ev) {
        var actBtn = ev.target.closest("[data-act]");
        if (actBtn) {
          ev.preventDefault();
          ev.stopPropagation();
          var act = actBtn.getAttribute("data-act");
          if (act === "copy") copyTournament();
          if (act === "remove") removeTournament();
          return;
        }
        flushEditorToState();
        state.activeId = t.id;
        renderAll();
      });
      nav.appendChild(btn);
    });
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

  function previewRowLimit() {
    var c = state.config || {};
    var n = Number(c.preview_row_limit);
    return n > 0 ? n : 100;
  }

  function renderPreviewTable(pack, limit, highlightCols) {
    var cols = pack.columns || [];
    var total = (pack.rows || []).length;
    var lim = limit == null ? previewRowLimit() : limit;
    var rows = (pack.rows || []).slice(0, lim);
    var hl = {};
    (highlightCols || []).forEach(function (c) {
      if (c) hl[c] = true;
    });
    var head = cols
      .map(function (c) {
        return '<th class="' + (hl[c] ? "is-selected" : "") + '">' + escapeHtml(c) + "</th>";
      })
      .join("");
    var body = rows
      .map(function (row) {
        return (
          "<tr>" +
          cols
            .map(function (c) {
              return (
                '<td class="' +
                (hl[c] ? "is-selected" : "") +
                '">' +
                escapeHtml(row[c] == null ? "" : row[c]) +
                "</td>"
              );
            })
            .join("") +
          "</tr>"
        );
      })
      .join("");
    var meta =
      total > lim
        ? '<div class="preview-meta">Показаны первые <b>' +
          lim +
          "</b> из <b>" +
          total +
          "</b> строк (прокрутка вправо/вниз).</div>"
        : '<div class="preview-meta">Строк: <b>' +
          total +
          "</b>" +
          (cols.length ? " · колонок: <b>" + cols.length + "</b>" : "") +
          " (прокрутка вправо/вниз при необходимости).</div>";
    return (
      meta +
      '<div class="preview-table-wrap"><table class="preview-table"><thead><tr>' +
      head +
      "</tr></thead><tbody>" +
      body +
      "</tbody></table></div>"
    );
  }

  function renderFioPanelHtml() {
    // блок виден, если активный турнир в режиме FIO (при смене типа — полный re-render)
    if (!hasActiveFioMode()) return "";
    var pack = state.fioPack;
    var ui = state.fioUi;
    var sheetBlock = "";
    if (pack && pack.kind === "excel" && pack.sheetNames && pack.sheetNames.length) {
      sheetBlock =
        '<div class="field field--third"><label class="field-label" for="fio-sheet">Лист Excel</label>' +
        '<select class="field-select" id="fio-sheet"></select></div>';
    }
    var fioErr = state.fioUi.source_error
      ? '<div class="error-box" style="margin-bottom:10px">' + escapeHtml(state.fioUi.source_error) + "</div>"
      : "";
    return (
      '<div class="panel" id="panel-fio">' +
      "<h2>Справочник ФИО</h2>" +
      '<p class="panel__intro">Доступен для активного турнира в режиме FIO. Загрузите JSON или таблицу и укажите угол, колонки ФИО и табельного.</p>' +
      fioErr +
      '<div class="info-box">Записей в справочнике: <b id="fio-stats">' +
      state.fioEntries.length +
      "</b></div>" +
      '<div class="toolbar-row">' +
      '<label class="btn file-pick" data-tip="Загрузить ранее сохранённый JSON справочника">' +
      "<span>Открыть JSON</span>" +
      '<input type="file" id="import-fio-json" class="file-pick__input" accept=".json,application/json" /></label>' +
      '<button type="button" class="btn" id="btn-save-fio" data-tip="Сохранить справочник ФИО в JSON">Сохранить JSON</button>' +
      '<label class="btn btn-primary file-pick" data-tip="Загрузить CSV/Excel со столбцами ФИО и табельный">' +
      "<span>Загрузить таблицу ФИО</span>" +
      '<input type="file" id="import-fio-table" class="file-pick__input" accept=".csv,.txt,.xlsx,.xls,.xlsm" /></label>' +
      '<button type="button" class="btn btn-primary" id="btn-apply-fio-table" data-tip="Взять строки из таблицы в справочник">Применить из таблицы</button>' +
      "</div>" +
      '<div class="fields-grid" style="margin-top:12px">' +
      sheetBlock +
      '<div class="field field--third"><label class="field-label" for="fio-start-row">Строка угла</label>' +
      '<input class="field-input" id="fio-start-row" type="number" min="1" step="1" /></div>' +
      '<div class="field field--third"><label class="field-label" for="fio-start-col">Колонка угла</label>' +
      '<input class="field-input" id="fio-start-col" type="number" min="1" step="1" /></div>' +
      '<div class="field"><label class="field-label" for="fio-col-fio">Колонка ФИО</label><select class="field-select" id="fio-col-fio"></select></div>' +
      '<div class="field"><label class="field-label" for="fio-col-tn">Колонка табельного</label><select class="field-select" id="fio-col-tn"></select></div>' +
      '<div class="field field--full"><label class="field-label" for="fio-file-name">Файл таблицы</label>' +
      '<input class="field-input" id="fio-file-name" readonly /></div>' +
      "</div>" +
      '<div id="fio-preview-host"></div></div>'
    );
  }

  function bindFioPanel() {
    if (!hasActiveFioMode()) return;
    var pack = state.fioPack;
    var ui = state.fioUi;
    $("fio-start-row").value = ui.start_row || 1;
    $("fio-start-col").value = ui.start_col || 1;
    $("fio-file-name").value = ui.file_name || "";
    if (pack) {
      fillSelect($("fio-col-fio"), pack.columns, ui.col_fio || "");
      fillSelect($("fio-col-tn"), pack.columns, ui.col_tn || "");
      $("fio-preview-host").innerHTML = renderPreviewTable(pack, null, [ui.col_fio, ui.col_tn]);
      if (pack.kind === "excel" && $("fio-sheet")) {
        fillSelect($("fio-sheet"), pack.sheetNames, ui.sheet_name || pack.sheetName || "");
        $("fio-sheet").addEventListener("change", function () {
          reapplyFioOrigin({ sheetName: $("fio-sheet").value });
        });
      }
    } else {
      fillSelect($("fio-col-fio"), [], "");
      fillSelect($("fio-col-tn"), [], "");
      $("fio-preview-host").innerHTML =
        '<div class="warn-box">Загрузите таблицу ФИО или JSON — здесь появится превью.</div>';
    }

    ["fio-start-row", "fio-start-col"].forEach(function (id) {
      $(id).addEventListener("change", function () {
        reapplyFioOrigin({
          start_row: Number($("fio-start-row").value) || 1,
          start_col: Number($("fio-start-col").value) || 1,
        });
      });
    });
    ["fio-col-fio", "fio-col-tn"].forEach(function (id) {
      $(id).addEventListener("change", function () {
        state.fioUi.col_fio = $("fio-col-fio").value;
        state.fioUi.col_tn = $("fio-col-tn").value;
        state.fioUi.source_error = "";
        if (state.fioPack) {
          $("fio-preview-host").innerHTML = renderPreviewTable(state.fioPack, null, [
            state.fioUi.col_fio,
            state.fioUi.col_tn,
          ]);
        }
      });
    });

    $("import-fio-json").addEventListener("change", async function (ev) {
      var file = ev.target.files && ev.target.files[0];
      ev.target.value = "";
      if (!file) return;
      try {
        state.fioEntries = ReportCore.parseFioDictionary(await readJsonFile(file));
        invalidateChecks();
        renderAll();
        showToast("Справочник JSON загружен");
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
    $("import-fio-table").addEventListener("change", async function (ev) {
      var file = ev.target.files && ev.target.files[0];
      ev.target.value = "";
      if (!file) return;
      try {
        var pack = await ReportIO.readTableFile(file, state.fioUi.start_row || 1, state.fioUi.start_col || 1);
        state.fioPack = pack;
        state.fioUi.file_name = pack.fileName;
        state.fioUi.sheet_name = pack.sheetName || "";
        state.fioUi.source_file_b64 = pack.source_b64 || "";
        state.fioUi.source_file_kind = pack.kind || "";
        state.fioUi.source_error = "";
        state.fioUi.col_fio = ReportIO.guessIdColumn(pack.columns, "FIO");
        state.fioUi.col_tn = ReportIO.guessIdColumn(pack.columns, "TN");
        renderAll();
        showToast("Таблица ФИО загружена");
      } catch (err) {
        alert(err.message || String(err));
      }
    });
    $("btn-apply-fio-table").addEventListener("click", function () {
      if (!state.fioPack) {
        alert("Сначала загрузите таблицу ФИО");
        return;
      }
      var colFio = $("fio-col-fio").value;
      var colTn = $("fio-col-tn").value;
      if (!colFio || !colTn) {
        alert("Укажите колонки ФИО и табельного");
        return;
      }
      state.fioUi.col_fio = colFio;
      state.fioUi.col_tn = colTn;
      var entries = ReportIO.entriesFromFioTable(state.fioPack.rows, colFio, colTn);
      if (!entries.length) {
        alert("Не удалось прочитать ни одной пары ФИО / табельный");
        return;
      }
      var byKey = Object.create(null);
      state.fioEntries.forEach(function (e) {
        byKey[ReportCore.normalizeFioKey(e.fio)] = e;
      });
      entries.forEach(function (e) {
        byKey[ReportCore.normalizeFioKey(e.fio)] = e;
      });
      state.fioEntries = Object.keys(byKey).map(function (k) {
        return byKey[k];
      });
      invalidateChecks();
      renderAll();
      showToast("В справочник: " + entries.length);
    });
  }

  function reapplyFioOrigin(partial) {
    if (!state.fioPack) return;
    if (partial) {
      if (partial.sheetName != null) state.fioUi.sheet_name = partial.sheetName;
      if (partial.start_row != null) state.fioUi.start_row = partial.start_row;
      if (partial.start_col != null) state.fioUi.start_col = partial.start_col;
    }
    state.fioPack = ReportIO.applyPackOrigin(state.fioPack, {
      sheetName: state.fioUi.sheet_name || state.fioPack.sheetName,
      start_row: state.fioUi.start_row,
      start_col: state.fioUi.start_col,
    });
    state.fioUi.sheet_name = state.fioPack.sheetName || "";
    state.fioUi.start_row = state.fioPack.start_row || 1;
    state.fioUi.start_col = state.fioPack.start_col || 1;
    if (!state.fioUi.col_fio || state.fioPack.columns.indexOf(state.fioUi.col_fio) < 0) {
      state.fioUi.col_fio = ReportIO.guessIdColumn(state.fioPack.columns, "FIO");
    }
    if (!state.fioUi.col_tn || state.fioPack.columns.indexOf(state.fioUi.col_tn) < 0) {
      state.fioUi.col_tn = ReportIO.guessIdColumn(state.fioPack.columns, "TN");
    }
    renderAll();
  }

  function renderWorkspace() {
    var ws = $("workspace");
    var t = activeTournament();
    var fioHtml = renderFioPanelHtml();
    if (!t) {
      ws.innerHTML =
        '<div class="panel"><h2>Нет турниров</h2><p class="panel__intro">Добавьте турнир слева или откройте JSON настроек.</p>' +
        '<div class="toolbar-row"><button type="button" class="btn btn-primary" id="btn-add-tournament-empty" data-tip="Добавить турнир">' +
        '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 5v14"/><path d="M5 12h14"/></svg> Добавить турнир</button></div></div>' +
        fioHtml;
      var addEmpty = $("btn-add-tournament-empty");
      if (addEmpty) addEmpty.addEventListener("click", addTournament);
      bindFioPanel();
      return;
    }

    var pack = state.dataByTournament[t.id];
    var lock = !ReportCore.tournamentIdentityUnlocked(t);
    var codeHighlight = lock ? " is-highlight" : "";
    var nameHighlight = lock ? " is-highlight" : "";
    var periodOpts = ReportCore.PERIOD_OPTIONS.map(function (p) {
      return (
        '<option value="' +
        p.code +
        '"' +
        (ReportCore.normalizePeriodCode(t.period_code) === p.code ? " selected" : "") +
        ">" +
        escapeHtml(p.label) +
        " (" +
        p.code +
        ")</option>"
      );
    }).join("");
    var factOp = t.fact_op || "none";
    var sourceErrHtml = t.source_error
      ? '<div class="error-box" style="margin-bottom:10px">' + escapeHtml(t.source_error) + "</div>"
      : "";

    var sheetBlock = "";
    if (pack && pack.kind === "excel" && pack.sheetNames && pack.sheetNames.length) {
      sheetBlock =
        '<div class="field field--third"><label class="field-label" for="f-sheet">Лист Excel</label>' +
        '<select class="field-select" id="f-sheet" data-tip="Лист, с которого брать таблицу"></select></div>';
    }

    ws.innerHTML =
      '<div class="panel" id="panel-params">' +
      "<h2>Параметры турнира</h2>" +
      '<p class="panel__intro">План — целое или с запятой (например 100 или 100,5); в выгрузке будет формат 0.00000.</p>' +
      (lock
        ? '<div class="warn-box">Копия турнира: обязательно смените <b>код турнира</b> и <b>наименование</b>. Пока они совпадают с оригиналом — формирование заблокировано.</div>'
        : "") +
      '<div class="fields-grid">' +
      '<div class="field field--full"><label class="check-row"><input type="checkbox" id="f-include" ' +
      (t.include_in_report !== false ? "checked" : "") +
      ' /> <span>Включать в проверку и выгрузку</span></label></div>' +
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
      '<div class="field field--third"><label class="field-label" for="f-plan">План (PLAN_VALUE)</label>' +
      '<input class="field-input" id="f-plan" placeholder="100 или 100,5" data-tip="Целое или дробь с запятой; в CSV/XLSX — точка и 5 знаков" /></div>' +
      '<div class="field field--third"><label class="field-label" for="f-date">Дата данных</label><input class="field-input" id="f-date" type="date" /></div>' +
      '<div class="field field--third"><label class="field-label" for="f-type">Тип расчёта</label><select class="field-select" id="f-type"><option value="TN">TN — табельный</option><option value="FIO">FIO — ФИО</option></select></div>' +
      '<div class="field"><label class="field-label" for="f-period">Период турнира</label><select class="field-select" id="f-period">' +
      periodOpts +
      "</select></div>" +
      "</div></div>" +
      '<div class="panel" id="panel-data">' +
      "<h2>Источник данных</h2>" +
      '<p class="panel__intro">CSV (;) или Excel. Укажите лист и левый верхний угол таблицы (строка и колонка заголовка, по умолчанию 1 и 1). Для показателя можно задать множитель/делитель/±.</p>' +
      sourceErrHtml +
      '<div class="toolbar-row">' +
      '<label class="btn btn-primary file-pick" data-tip="Загрузить CSV или Excel с показателями">' +
      '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 21V9"/><path d="M7 14l5-5 5 5"/><path d="M5 3h14"/></svg> Загрузить CSV / Excel' +
      '<input type="file" id="import-data" class="file-pick__input" accept=".csv,.txt,.xlsx,.xls,.xlsm" /></label>' +
      '<span class="mini-badge" id="data-meta"></span></div>' +
      '<div class="fields-grid" style="margin-top:12px">' +
      sheetBlock +
      '<div class="field field--third"><label class="field-label" for="f-start-row">Строка угла</label>' +
      '<input class="field-input" id="f-start-row" type="number" min="1" step="1" data-tip="Номер строки заголовка таблицы (с 1)" /></div>' +
      '<div class="field field--third"><label class="field-label" for="f-start-col">Колонка угла</label>' +
      '<input class="field-input" id="f-start-col" type="number" min="1" step="1" data-tip="Номер колонки левого верхнего угла (с 1)" /></div>' +
      '<div class="field"><label class="field-label" for="f-col-id">Колонка ФИО / табельного</label><select class="field-select" id="f-col-id"></select></div>' +
      '<div class="field"><label class="field-label" for="f-col-fact">Колонка показателя</label><select class="field-select" id="f-col-fact"></select></div>' +
      '<div class="field"><label class="field-label" for="f-fact-op">Показатель: действие</label>' +
      '<select class="field-select" id="f-fact-op" data-tip="Как преобразовать значение колонки показателя перед расчётом">' +
      '<option value="none"' +
      (factOp === "none" ? " selected" : "") +
      ">Брать неизменно</option>" +
      '<option value="mul"' +
      (factOp === "mul" ? " selected" : "") +
      ">Умножить (×)</option>" +
      '<option value="div"' +
      (factOp === "div" ? " selected" : "") +
      ">Разделить (÷)</option>" +
      '<option value="add"' +
      (factOp === "add" ? " selected" : "") +
      ">Сложить (+)</option>" +
      '<option value="sub"' +
      (factOp === "sub" ? " selected" : "") +
      ">Вычесть (−)</option>" +
      "</select></div>" +
      '<div class="field"><label class="field-label" for="f-fact-op-value">Число операции</label>' +
      '<input class="field-input" id="f-fact-op-value" placeholder="напр. 100 или 2" data-tip="Для ×100 из доли 0,5 получится 50" /></div>' +
      '<div class="field field--full"><label class="field-label" for="f-source-name">Имя файла</label><input class="field-input" id="f-source-name" readonly /></div>' +
      "</div>" +
      '<div id="data-preview-host"></div>' +
      "</div>" +
      fioHtml +
      '<div class="panel" id="panel-result" hidden>' +
      "<h2>Результат</h2>" +
      '<div id="result-summary" class="info-box"></div>' +
      '<div id="result-warnings" class="warn-box" hidden></div>' +
      '<div class="toolbar-row">' +
      '<button type="button" class="btn btn-primary" id="btn-export-csv-2" data-tip="Скачать согласованный CSV">Скачать CSV</button>' +
      '<button type="button" class="btn btn-primary" id="btn-export-xlsx-2" data-tip="Скачать XLSX с закреплением и автофильтром">Скачать XLSX</button>' +
      "</div></div>";

    $("f-contest-code").value = t.contest_code || "";
    $("f-tournament-code").value = t.tournament_code || "";
    $("f-full-name").value = t.full_name || "";
    $("f-plan").value = t.plan_value || "";
    $("f-date").value = t.contest_date || "";
    $("f-type").value = String(t.type_ind || "TN").toUpperCase() === "FIO" ? "FIO" : "TN";
    $("f-source-name").value = t.source_file_name || "";
    $("f-start-row").value = t.table_start_row || (pack && pack.start_row) || 1;
    $("f-start-col").value = t.table_start_col || (pack && pack.start_col) || 1;
    $("f-fact-op-value").value = t.fact_op_value != null ? t.fact_op_value : "1";

    var colId = $("f-col-id");
    var colFact = $("f-col-fact");
    if (pack) {
      fillSelect(colId, pack.columns, t.column_id || "");
      fillSelect(colFact, pack.columns, t.column_fact || "");
      if (t.column_id && pack.columns.indexOf(t.column_id) < 0) {
        colId.classList.add("is-error");
      }
      if (t.column_fact && pack.columns.indexOf(t.column_fact) < 0) {
        colFact.classList.add("is-error");
      }
      $("data-meta").textContent =
        pack.rows.length +
        " строк · угол " +
        (pack.start_row || 1) +
        "," +
        (pack.start_col || 1) +
        (pack.encoding ? " · " + pack.encoding : "") +
        (pack.sheetName ? " · лист: " + pack.sheetName : "");
      $("data-preview-host").innerHTML = renderPreviewTable(pack, null, [t.column_id, t.column_fact]);
      if (pack.kind === "excel" && $("f-sheet")) {
        fillSelect($("f-sheet"), pack.sheetNames, t.sheet_name || pack.sheetName || "");
        if (t.sheet_name && pack.sheetNames && pack.sheetNames.indexOf(t.sheet_name) < 0) {
          $("f-sheet").classList.add("is-error");
        }
        $("f-sheet").addEventListener("change", function () {
          reapplyDataOrigin({ sheetName: $("f-sheet").value });
        });
      }
    } else {
      fillSelect(colId, [], "");
      fillSelect(colFact, [], "");
      $("data-meta").textContent = t.source_file_name ? "файл не загружен: " + t.source_file_name : "файл не загружен";
      $("data-preview-host").innerHTML =
        '<div class="warn-box">Загрузите файл — здесь появятся пример строк, угол таблицы и выбор колонок.</div>';
    }

    bindEditorEvents();
    bindFioPanel();
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

  function reapplyDataOrigin(partial) {
    var t = activeTournament();
    if (!t) return;
    var pack = state.dataByTournament[t.id];
    if (!pack) return;
    if (partial) {
      if (partial.sheetName != null) t.sheet_name = partial.sheetName;
      if (partial.start_row != null) t.table_start_row = partial.start_row;
      if (partial.start_col != null) t.table_start_col = partial.start_col;
    }
    var next = ReportIO.applyPackOrigin(pack, {
      sheetName: t.sheet_name || pack.sheetName,
      start_row: t.table_start_row || 1,
      start_col: t.table_start_col || 1,
    });
    state.dataByTournament[t.id] = next;
    if (!t.column_id || next.columns.indexOf(t.column_id) < 0) {
      t.column_id = ReportIO.guessIdColumn(next.columns, t.type_ind);
    }
    if (!t.column_fact || next.columns.indexOf(t.column_fact) < 0) {
      t.column_fact = ReportIO.guessFactColumn(next.columns);
    }
    invalidateChecks();
    renderAll();
  }

  function bindEditorEvents() {
    [
      "f-contest-code",
      "f-tournament-code",
      "f-full-name",
      "f-plan",
      "f-date",
      "f-type",
      "f-period",
      "f-col-id",
      "f-col-fact",
      "f-fact-op",
      "f-fact-op-value",
      "f-include",
    ].forEach(function (id) {
      var el = $(id);
      if (!el) return;
      el.addEventListener("change", onEditorChange);
      if (el.type !== "checkbox" && el.tagName !== "SELECT") {
        el.addEventListener("input", onEditorChange);
      }
    });
    ["f-start-row", "f-start-col"].forEach(function (id) {
      var el = $(id);
      if (!el) return;
      el.addEventListener("change", function () {
        flushEditorToState();
        reapplyDataOrigin({
          start_row: Number($("f-start-row").value) || 1,
          start_col: Number($("f-start-col").value) || 1,
        });
      });
    });
    var importData = $("import-data");
    if (importData) {
      importData.addEventListener("change", function (ev) {
        var file = ev.target.files && ev.target.files[0];
        ev.target.value = "";
        if (file) onImportData(file);
      });
    }
  }

  function onEditorChange(ev) {
    var prevType = activeTournament() && activeTournament().type_ind;
    flushEditorToState();
    var t = activeTournament();
    var unlockedNow = false;
    if (t && t.needs_identity_fix && ReportCore.tournamentIdentityUnlocked(t)) {
      t.needs_identity_fix = false;
      t.copy_lock_code = "";
      t.copy_lock_name = "";
      unlockedNow = true;
      showToast("Копия разблокирована");
    }
    invalidateChecks();
    var typeChanged = t && String(prevType || "") !== String(t.type_ind || "");
    var needFull =
      unlockedNow ||
      typeChanged ||
      (ev && ev.target && (ev.target.id === "f-include" || ev.target.id === "f-period" || ev.target.id === "f-fact-op"));
    if (needFull) {
      renderAll();
      return;
    }
    // подсветка выбранных колонок в превью
    if (t && state.dataByTournament[t.id] && $("data-preview-host")) {
      $("data-preview-host").innerHTML = renderPreviewTable(state.dataByTournament[t.id], null, [
        t.column_id,
        t.column_fact,
      ]);
    }
    renderStages();
    renderNav();
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
    if ($("f-period")) t.period_code = ReportCore.normalizePeriodCode($("f-period").value);
    if ($("f-include")) t.include_in_report = !!$("f-include").checked;
    t.column_id = $("f-col-id").value;
    t.column_fact = $("f-col-fact").value;
    if ($("f-fact-op")) t.fact_op = $("f-fact-op").value || "none";
    if ($("f-fact-op-value")) t.fact_op_value = $("f-fact-op-value").value.trim() || "1";
    if ($("f-sheet")) t.sheet_name = $("f-sheet").value;
    if ($("f-start-row")) t.table_start_row = Number($("f-start-row").value) || 1;
    if ($("f-start-col")) t.table_start_col = Number($("f-start-col").value) || 1;
    // сброс ошибки источника, если колонки снова валидны
    var pack = state.dataByTournament[t.id];
    if (pack) {
      var errs = [];
      if (t.column_id && pack.columns.indexOf(t.column_id) < 0) errs.push("колонка ID не найдена: " + t.column_id);
      if (t.column_fact && pack.columns.indexOf(t.column_fact) < 0) errs.push("колонка показателя не найдена: " + t.column_fact);
      if (t.sheet_name && pack.sheetNames && pack.sheetNames.length && pack.sheetNames.indexOf(t.sheet_name) < 0) {
        errs.push("лист не найден: " + t.sheet_name);
      }
      t.source_error = errs.join("; ");
    }
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
      state.dataByTournament[copy.id] = JSON.parse(
        JSON.stringify({
          kind: pack.kind,
          rows: pack.rows,
          columns: pack.columns,
          encoding: pack.encoding,
          fileName: pack.fileName,
          sheetName: pack.sheetName,
          sheetNames: pack.sheetNames,
          sheets: pack.sheets,
          sheetsAoa: pack.sheetsAoa,
          rawAoa: pack.rawAoa,
          start_row: pack.start_row,
          start_col: pack.start_col,
          source_b64: pack.source_b64,
        })
      );
      copy.source_file_b64 = pack.source_b64 || src.source_file_b64 || "";
      copy.source_file_kind = pack.kind || src.source_file_kind || "";
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
      var sr = t.table_start_row || 1;
      var sc = t.table_start_col || 1;
      var pack = await ReportIO.readTableFile(file, sr, sc);
      state.dataByTournament[t.id] = pack;
      t.source_file_name = pack.fileName;
      t.source_file_kind = pack.kind;
      t.source_file_b64 = pack.source_b64 || "";
      t.source_error = "";
      t.sheet_name = pack.sheetName || "";
      t.table_start_row = pack.start_row || sr;
      t.table_start_col = pack.start_col || sc;
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

  function askDuplicates(groups, options) {
    var opts = options || {};
    var kindLabel = opts.kindLabel || "табельный + турнир";
    var keyHint = opts.keyHint || "CONTEST_CODE + TOURNAMENT_CODE + MANAGER_PERSON_NUMBER";
    var previous = opts.previousResolutions || {};
    var requireUnique = opts.requireUnique !== false;
    return new Promise(function (resolve) {
      if (!groups || !groups.length) {
        resolve({ action: "apply", resolutions: {} });
        return;
      }
      var list = $("modal-dup-list");
      var title = $("modal-dup-title");
      var intro = $("modal-dup-intro");
      var progress = $("modal-dup-progress");
      title.textContent = "Дубли: " + kindLabel;
      intro.textContent =
        "Ключ: " +
        keyHint +
        ". Разбираем по одной группе. По умолчанию отмечена первая строка. Для CSV в группе должна остаться одна строка (или сумма / исключить все).";
      var idx = 0;
      var resolutions = Object.assign({}, previous);

      function renderCurrent() {
        var g = groups[idx];
        var prev = previous[g.key] || resolutions[g.key] || null;
        var prevLabel = ReportCore.describeResolution(prev);
        progress.textContent =
          "Группа " +
          (idx + 1) +
          " из " +
          groups.length +
          " · сумма " +
          ReportCore.formatNumberDot(g.sum, coreOpts().numberDecimals);
        var keepSet = Object.create(null);
        var modeDefault = "keep_selected";
        if (prev) {
          modeDefault = prev.mode === "keep_one" ? "keep_selected" : prev.mode;
          if (prev.mode === "sum" || prev.mode === "drop_all") {
            modeDefault = prev.mode;
          }
          var prevKeep = ReportCore.resolveKeepIndices(g, prev);
          if (prevKeep.length) {
            prevKeep.forEach(function (i) {
              keepSet[i] = true;
            });
          }
        }
        // по умолчанию — только первая строка (чтобы CSV не оставался с дублями)
        if (!Object.keys(keepSet).length && g.indices.length) {
          keepSet[g.indices[0]] = true;
        }
        var rowsHtml = g.rows
          .map(function (r, ri) {
            var rowIdx = g.indices[ri];
            var checked = keepSet[rowIdx] ? " checked" : "";
            return (
              '<label class="dup-row">' +
              '<input type="checkbox" data-row-idx="' +
              rowIdx +
              '"' +
              checked +
              " />" +
              "<span><code>" +
              escapeHtml(r.MANAGER_PERSON_NUMBER || "") +
              "</code> · FIO=" +
              escapeHtml(r.FIO || "") +
              " · FACT=" +
              escapeHtml(r.FACT_VALUE || "") +
              "</span></label>"
            );
          })
          .join("");
        list.innerHTML =
          '<div class="dup-group" data-key="' +
          escapeHtml(g.key) +
          '">' +
          '<div class="dup-group__meta">' +
          escapeHtml(g.key) +
          " · строк: " +
          g.indices.length +
          "</div>" +
          (prevLabel
            ? '<div class="info-box" style="margin:8px 0">' + escapeHtml(prevLabel) + "</div>"
            : "") +
          '<div class="toolbar-row" style="margin:8px 0">' +
          '<button type="button" class="btn btn-sm" id="dup-sel-all" data-tip="Отметить все строки группы">Все</button>' +
          '<button type="button" class="btn btn-sm" id="dup-sel-none" data-tip="Снять все отметки">Снять</button>' +
          '<button type="button" class="btn btn-sm" id="dup-sel-first" data-tip="Только первая строка">Первая</button>' +
          "</div>" +
          '<div class="dup-rows">' +
          rowsHtml +
          "</div>" +
          '<label class="field-label" style="margin-top:10px">Действие для этой группы</label>' +
          '<select class="field-select" id="dup-mode">' +
          '<option value="keep_selected"' +
          (modeDefault === "keep_selected" || modeDefault === "keep_one" ? " selected" : "") +
          ">Оставить отмеченные</option>" +
          '<option value="sum"' +
          (modeDefault === "sum" ? " selected" : "") +
          ">Сумма показателя в одну строку</option>" +
          '<option value="drop_all"' +
          (modeDefault === "drop_all" ? " selected" : "") +
          ">Убрать все из CSV</option>" +
          "</select>" +
          (requireUnique
            ? '<div class="field-hint">Для выгрузки CSV в группе нужна одна строка, «Сумма» или «Убрать все».</div>'
            : "") +
          "</div>";
        $("dup-sel-all").onclick = function () {
          list.querySelectorAll('input[type="checkbox"]').forEach(function (c) {
            c.checked = true;
          });
        };
        $("dup-sel-none").onclick = function () {
          list.querySelectorAll('input[type="checkbox"]').forEach(function (c) {
            c.checked = false;
          });
        };
        $("dup-sel-first").onclick = function () {
          list.querySelectorAll('input[type="checkbox"]').forEach(function (c, i) {
            c.checked = i === 0;
          });
        };
      }

      function cleanup() {
        $("modal-dup-apply").onclick = null;
        $("modal-dup-abort").onclick = null;
        $("modal-dup-cancel").onclick = null;
        closeModal("modal-dup");
      }

      openModal("modal-dup");
      renderCurrent();

      $("modal-dup-apply").onclick = function () {
        var g = groups[idx];
        var mode = $("dup-mode").value;
        var res = { mode: mode };
        if (mode === "keep_selected") {
          var keepIndices = [];
          list.querySelectorAll('input[type="checkbox"]:checked').forEach(function (c) {
            keepIndices.push(Number(c.getAttribute("data-row-idx")));
          });
          if (!keepIndices.length) {
            alert("Отметьте хотя бы одну строку или выберите «Убрать все» / «Сумма»");
            return;
          }
          if (requireUnique && keepIndices.length > 1) {
            alert(
              "Для CSV нельзя оставить несколько строк с одним ключом. Отметьте одну строку, либо выберите «Сумма» / «Убрать все»."
            );
            return;
          }
          res.keepIndices = keepIndices;
          res.keepFingerprints = keepIndices.map(function (rowIdx) {
            var pos = g.indices.indexOf(rowIdx);
            return ReportCore.rowFingerprint(pos >= 0 ? g.rows[pos] : null);
          });
          if (keepIndices.length === 1) {
            res.mode = "keep_one";
            res.keepIndex = keepIndices[0];
            res.keepFingerprint = res.keepFingerprints[0];
          }
        }
        resolutions[g.key] = res;
        idx += 1;
        if (idx >= groups.length) {
          cleanup();
          resolve({ action: "apply", resolutions: resolutions });
          return;
        }
        renderCurrent();
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
    ReportCore.includedTournaments(state.tournaments).forEach(function (t) {
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
    return {
      rows: allRows,
      missingFio: allMissing,
      fioGroups: ReportCore.findFioDuplicateGroups(allRows),
      tnGroups: ReportCore.findTnDuplicateGroups(allRows),
      groups: ReportCore.findTnDuplicateGroups(allRows),
    };
  }

  /**
   * Пайплайн проверок: ФИО отсутствующие → дубли ФИО+турнир → дубли табельный+турнир.
   * Возвращает итоговые строки с учётом решений.
   */
  async function runValidationPipeline(opts) {
    var options = opts || {};
    var interactive = options.interactive !== false;
    var rows = buildAllRows().rows;
    var missing = buildAllRows().missingFio;

    // 1) отсутствующие ФИО
    if (missing.length && !state.checkState.missingFioCleared) {
      if (!interactive) {
        return { ok: false, need: "missing_fio", missingFio: missing, rows: rows };
      }
      var fioAns = await askMissingFio(missing.slice());
      if (fioAns.action === "cancel") return { ok: false, cancelled: true };
      state.checkState.missingFioCleared = true;
      rows = buildAllRows().rows;
      missing = buildAllRows().missingFio;
    } else if (!missing.length) {
      state.checkState.missingFioCleared = true;
    }

    rows = buildAllRows().rows;

    // 2) дубли по ФИО + турнир (только строки с реальным ФИО)
    var fioGroups = ReportCore.findFioDuplicateGroups(rows);
    if (fioGroups.length && !state.checkState.fioDupCleared) {
      if (!interactive) {
        return { ok: false, need: "fio_dup", fioGroups: fioGroups, rows: rows };
      }
      var fioDupAns = await askDuplicates(fioGroups, {
        kindLabel: "ФИО + турнир",
        keyHint: "CONTEST_CODE + TOURNAMENT_CODE + ФИО",
        previousResolutions: state.lastFioResolutions,
        requireUnique: true,
      });
      if (fioDupAns.action === "cancel") return { ok: false, cancelled: true };
      if (fioDupAns.action === "abort") return { ok: false, aborted: true };
      state.lastFioResolutions = Object.assign({}, state.lastFioResolutions, fioDupAns.resolutions || {});
      var fioApplied = ReportCore.applyDuplicateResolutions(
        rows,
        state.lastFioResolutions,
        coreOpts(),
        ReportCore.duplicateKeyFio,
        ReportCore.isFioDataRow
      );
      if (!fioApplied.ok) {
        return { ok: false, error: fioApplied.error, rows: fioApplied.rows };
      }
      rows = fioApplied.rows;
      // если после решения всё ещё есть группы — не помечаем cleared (не должно случиться при requireUnique)
      fioGroups = ReportCore.findFioDuplicateGroups(rows);
      state.checkState.fioDupCleared = fioGroups.length === 0;
      if (fioGroups.length) {
        return {
          ok: false,
          error: "После разрешения остались дубли ФИО. Оставьте по одной строке в группе.",
          rows: rows,
          fioGroups: fioGroups,
        };
      }
    } else if (!fioGroups.length) {
      state.checkState.fioDupCleared = true;
    }

    // 3) дубли по табельному + турнир (после сцепки, с учётом include_in_csv)
    ReportCore.annotateDuplicatesLikePq(rows, coreOpts());
    var tnGroups = ReportCore.findTnDuplicateGroups(rows);
    if (tnGroups.length && !state.checkState.duplicatesCleared) {
      if (!interactive) {
        return { ok: false, need: "tn_dup", tnGroups: tnGroups, rows: rows };
      }
      var tnAns = await askDuplicates(tnGroups, {
        kindLabel: "табельный + турнир",
        keyHint: "CONTEST_CODE + TOURNAMENT_CODE + MANAGER_PERSON_NUMBER",
        previousResolutions: state.lastResolutions,
        requireUnique: true,
      });
      if (tnAns.action === "cancel") return { ok: false, cancelled: true };
      if (tnAns.action === "abort") return { ok: false, aborted: true };
      state.lastResolutions = Object.assign({}, state.lastResolutions, tnAns.resolutions || {});
      var tnApplied = ReportCore.applyDuplicateResolutions(
        rows,
        state.lastResolutions,
        coreOpts(),
        ReportCore.duplicateKey,
        function (row) {
          return row.include_in_csv !== false;
        }
      );
      if (!tnApplied.ok) {
        return { ok: false, error: tnApplied.error, rows: tnApplied.rows };
      }
      rows = tnApplied.rows;
      tnGroups = ReportCore.findTnDuplicateGroups(rows);
      state.checkState.duplicatesCleared = tnGroups.length === 0;
      if (tnGroups.length) {
        return {
          ok: false,
          error: "После разрешения остались дубли табельного. Оставьте одну строку, сумму или исключите.",
          rows: rows,
          tnGroups: tnGroups,
        };
      }
    } else if (!tnGroups.length) {
      state.checkState.duplicatesCleared = true;
    }

    ReportCore.annotateDuplicatesLikePq(rows, coreOpts());
    var summary = ReportCore.summarizeCheckedRows(rows);
    var csvGate = ReportCore.validateCsvExportRows(rows, coreOpts());
    summary.csvGate = csvGate;
    if (!csvGate.ok) {
      // помечаем строки и отдаём предупреждение — Excel можно, CSV нет
      summary.xlsxRows = ReportCore.rowsForXlsx(csvGate.rowsMarked);
      summary.csvRows = [];
    }
    state.checkedPipeline = { rows: rows, summary: summary, csvGate: csvGate };
    return {
      ok: true,
      rows: rows,
      summary: summary,
      csvGate: csvGate,
      missingFio: buildAllRows().missingFio,
    };
  }

  function validateBaseOrAlert() {
    flushEditorToState();
    var st = stages();
    if (!st.hasTournaments) {
      alert("Нет турниров, включённых в выгрузку. Добавьте турнир или включите галочку «Включать в проверку и выгрузку».");
      return false;
    }
    if (st.blockedCopies.length) {
      alert("После копирования смените код турнира и наименование у копии");
      return false;
    }
    if (!st.fieldsFilled) {
      alert("Заполните все параметры включённых турниров (код конкурса, код турнира, план, дата, название, тип, период)");
      return false;
    }
    var broken = ReportCore.includedTournaments(state.tournaments).filter(function (t) {
      return !!t.source_error || !ReportCore.tournamentSourceOk(t, state.dataByTournament[t.id]);
    });
    if (broken.length || !st.sourcesOk) {
      var details = broken
        .map(function (t) {
          return (t.tournament_code || t.id) + (t.source_error ? ": " + t.source_error : "");
        })
        .join("\n");
      alert(
        "У включённых турниров загрузите данные и укажите существующие колонки/листы.\n" +
          (details || "Проверьте источники.")
      );
      return false;
    }
    return true;
  }

  function renderCheckSummary(summary, missingFio, csvGate) {
    var html =
      '<div class="ok-box">Проверка завершена с учётом применённых решений.</div>' +
      '<div class="info-box">Всего строк после обработки: <b>' +
      summary.total +
      "</b><br>В CSV попадёт: <b>" +
      summary.included +
      "</b><br>Исключено (дубли/решения): <b>" +
      summary.excluded +
      "</b>";
    if (summary.missingFioFlag) {
      html += "<br>С флагом «табельный не найден»: <b>" + summary.missingFioFlag + "</b>";
    }
    html += "</div>";
    if (csvGate && !csvGate.ok) {
      html += '<div class="error-box">' + escapeHtml(csvGate.message) + "</div>";
    } else if (csvGate && csvGate.ok) {
      html += '<div class="ok-box">Критерии CSV выполнены — выгрузка CSV разрешена.</div>';
    }
    if (missingFio && missingFio.length) {
      html +=
        '<div class="warn-box">Остались ФИО без табельного в справочнике (подставлен 00000000): ' +
        escapeHtml(missingFio.join("; ")) +
        "</div>";
    }
    if (summary.csvRows && summary.csvRows.length) {
      var previewRows = summary.csvRows.slice(0, 8);
      var cols = ReportCore.CSV_COLUMNS;
      html +=
        '<div class="preview-table-wrap"><table class="preview-table"><thead><tr>' +
        cols
          .map(function (c) {
            return "<th>" + escapeHtml(c) + "</th>";
          })
          .join("") +
        "</tr></thead><tbody>" +
        previewRows
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
          .join("") +
        "</tbody></table></div>";
    }
    return html;
  }

  async function runCheck() {
    if (!validateBaseOrAlert()) return;
    setStatus("проверка…");
    // сохраняем прошлые решения для показа в диалоге
    invalidateChecks({ keepResolutions: true });
    var result = await runValidationPipeline({ interactive: true });
    var body = $("modal-check-body");
    var title = $("modal-check-title");
    if (result.cancelled) {
      setStatus("проверка отменена");
      return;
    }
    if (result.aborted) {
      setStatus("проверка остановлена");
      alert("Проверка остановлена");
      return;
    }
    if (!result.ok) {
      setStatus(result.error || "ошибка проверки");
      alert(result.error || "Ошибка проверки");
      return;
    }
    title.textContent = "Проверка: готово";
    body.innerHTML = renderCheckSummary(result.summary, result.missingFio, result.csvGate);
    openModal("modal-check");
    $("modal-check-close").onclick = function () {
      closeModal("modal-check");
    };
    renderStages();
    var csvOk = !result.csvGate || result.csvGate.ok;
    setStatus(
      csvOk
        ? "проверка OK · в CSV " + result.summary.included
        : "проверка OK · CSV пока заблокирован"
    );
    showToast(csvOk ? "Проверка OK" : "Проверка OK, CSV требует доработки");
  }

  async function runProcess() {
    if (!validateBaseOrAlert()) return;
    setStatus("обработка…");
    // если уже есть свежий пайплайн после проверки — используем его, иначе прогоняем заново
    var result;
    if (
      state.checkedPipeline &&
      state.checkState.missingFioCleared &&
      state.checkState.fioDupCleared &&
      state.checkState.duplicatesCleared
    ) {
      result = {
        ok: true,
        rows: state.checkedPipeline.rows,
        summary: state.checkedPipeline.summary,
        csvGate: state.checkedPipeline.csvGate,
        missingFio: buildAllRows().missingFio,
      };
    } else {
      invalidateChecks({ keepResolutions: true });
      result = await runValidationPipeline({ interactive: true });
    }
    if (result.cancelled) {
      setStatus("отменено");
      return;
    }
    if (result.aborted) {
      setStatus("остановлено");
      alert("Формирование остановлено");
      return;
    }
    if (!result.ok) {
      alert(result.error || "Ошибка");
      setStatus(result.error || "ошибка");
      return;
    }

    state.lastResult = {
      ok: true,
      rows: result.rows,
      missingFio: result.missingFio || [],
      csvRows: result.summary.csvRows,
      xlsxRows: result.summary.xlsxRows,
    };
    // сразу пометить строки для Excel / гейта CSV
    var gate = ReportCore.validateCsvExportRows(state.lastResult.rows, coreOpts());
    state.lastResult.xlsxRows = ReportCore.rowsForXlsx(gate.rowsMarked);
    state.lastResult.csvRows = gate.ok ? ReportCore.rowsForCsv(gate.rowsMarked) : [];
    state.lastResult.csvGate = gate;
    renderAll();
    showToast(gate.ok ? "Готово" : "Готово · CSV потребует исправлений");
    setStatus(
      "сформировано XLSX " +
        state.lastResult.xlsxRows.length +
        (gate.ok ? " / CSV " + state.lastResult.csvRows.length : " · CSV заблокирован")
    );
  }

  function showCsvBlockModal(validation) {
    var lines = (validation.byTournament || []).map(function (s) {
      return (
        "<tr><td>" +
        escapeHtml(s.tournament_code) +
        "</td><td>" +
        s.duplicates +
        "</td><td>" +
        s.bad_person +
        "</td><td>" +
        s.empty_cells +
        "</td></tr>"
      );
    });
    var html =
      '<div class="error-box">' +
      escapeHtml(validation.message) +
      "</div>" +
      '<table class="preview-table" style="margin-top:12px;width:100%"><thead><tr>' +
      "<th>Турнир</th><th>Дубли строк</th><th>ТН≠20 цифр</th><th>Пустые ячейки</th>" +
      "</tr></thead><tbody>" +
      (lines.join("") || "<tr><td colspan='4'>нет детализации</td></tr>") +
      "</tbody></table>" +
      '<p class="panel__intro" style="margin-top:10px">XLSX можно скачать с пометками в колонке CSV_ERROR. CSV — только после исправления.</p>';
    var title = $("modal-check-title");
    var body = $("modal-check-body");
    if (title) title.textContent = "CSV заблокирован";
    if (body) body.innerHTML = html;
    openModal("modal-check");
    $("modal-check-close").onclick = function () {
      closeModal("modal-check");
    };
  }

  function prepareExportRows() {
    if (!state.lastResult || !state.lastResult.ok) return null;
    var validation = ReportCore.validateCsvExportRows(state.lastResult.rows, coreOpts());
    var xlsxRows = ReportCore.rowsForXlsx(validation.rowsMarked);
    var csvRows = validation.ok ? ReportCore.rowsForCsv(validation.rowsMarked) : [];
    return { validation: validation, csvRows: csvRows, xlsxRows: xlsxRows };
  }

  function exportCsv() {
    if (!state.lastResult || !state.lastResult.ok) return;
    var prepared = prepareExportRows();
    if (!prepared.validation.ok) {
      showCsvBlockModal(prepared.validation);
      setStatus("CSV заблокирован");
      return;
    }
    ReportIO.downloadReportCsv(prepared.csvRows, ReportIO.timestampName("REPORT", "csv"));
    showToast("CSV сохранён");
  }

  function exportXlsx() {
    if (!state.lastResult || !state.lastResult.ok) return;
    var prepared = prepareExportRows();
    if (!prepared.validation.ok) {
      // Excel разрешён с пометками; кратко предупредим
      showToast("XLSX с пометками CSV_ERROR");
    }
    ReportIO.downloadReportXlsx(prepared.xlsxRows, ReportIO.timestampName("REPORT", "xlsx"));
    showToast("XLSX сохранён");
  }

  function buildSettingsPayload() {
    var payloads = {};
    state.tournaments.forEach(function (t) {
      var pack = state.dataByTournament[t.id];
      if (pack && pack.source_b64) {
        payloads[t.id] = { b64: pack.source_b64, kind: pack.kind };
      } else if (t.source_file_b64) {
        payloads[t.id] = { b64: t.source_file_b64, kind: t.source_file_kind || "" };
      }
    });
    var fioMeta = {
      entries: state.fioEntries,
      file_name: state.fioUi.file_name || "",
      sheet_name: state.fioUi.sheet_name || "",
      start_row: state.fioUi.start_row || 1,
      start_col: state.fioUi.start_col || 1,
      col_fio: state.fioUi.col_fio || "",
      col_tn: state.fioUi.col_tn || "",
      source_file_b64: (state.fioPack && state.fioPack.source_b64) || state.fioUi.source_file_b64 || "",
      source_file_kind: (state.fioPack && state.fioPack.kind) || state.fioUi.source_file_kind || "",
    };
    return ReportCore.serializeSettings(state.tournaments, fioMeta, payloads);
  }

  function applySourceErrors(t, pack) {
    var errs = [];
    if (!pack) {
      if (t.source_file_name) errs.push("файл источника не найден: " + t.source_file_name);
      t.source_error = errs.join("; ");
      return;
    }
    if (t.sheet_name && pack.sheetNames && pack.sheetNames.length && pack.sheetNames.indexOf(t.sheet_name) < 0) {
      errs.push("лист не найден: " + t.sheet_name);
    }
    if (t.column_id && pack.columns.indexOf(t.column_id) < 0) errs.push("колонка ID не найдена: " + t.column_id);
    if (t.column_fact && pack.columns.indexOf(t.column_fact) < 0) {
      errs.push("колонка показателя не найдена: " + t.column_fact);
    }
    t.source_error = errs.join("; ");
  }

  async function loadSettingsFromJson(data) {
    var parsed = ReportCore.parseSettings(data);
    state.tournaments = parsed.tournaments;
    state.dataByTournament = {};
    state.fioPack = null;
    state.fioUi.source_error = "";

    // восстановить источники турниров из base64
    state.tournaments.forEach(function (t) {
      var pack = null;
      try {
        pack = ReportIO.packFromStoredSource({
          source_file_b64: t.source_file_b64,
          source_file_name: t.source_file_name,
          source_file_kind: t.source_file_kind,
          sheet_name: t.sheet_name,
          table_start_row: t.table_start_row,
          table_start_col: t.table_start_col,
        });
      } catch (err) {
        t.source_error = "ошибка чтения вложенного файла: " + (err.message || err);
        pack = null;
      }
      if (pack) {
        state.dataByTournament[t.id] = pack;
      }
      applySourceErrors(t, pack);
    });

    // ФИО блок из JSON
    if (parsed.fio) {
      if (Array.isArray(parsed.fio.entries)) {
        state.fioEntries = parsed.fio.entries.map(function (e) {
          return {
            fio: String(e.fio || "").trim(),
            person_number: String(e.person_number || "").trim(),
          };
        });
      }
      state.fioUi.file_name = parsed.fio.file_name || "";
      state.fioUi.sheet_name = parsed.fio.sheet_name || "";
      state.fioUi.start_row = parsed.fio.start_row || 1;
      state.fioUi.start_col = parsed.fio.start_col || 1;
      state.fioUi.col_fio = parsed.fio.col_fio || "";
      state.fioUi.col_tn = parsed.fio.col_tn || "";
      state.fioUi.source_file_b64 = parsed.fio.source_file_b64 || "";
      state.fioUi.source_file_kind = parsed.fio.source_file_kind || "";
      if (parsed.fio.source_file_b64) {
        try {
          state.fioPack = ReportIO.packFromStoredSource({
            source_file_b64: parsed.fio.source_file_b64,
            source_file_name: parsed.fio.file_name,
            source_file_kind: parsed.fio.source_file_kind,
            sheet_name: parsed.fio.sheet_name,
            table_start_row: parsed.fio.start_row,
            table_start_col: parsed.fio.start_col,
          });
          if (state.fioPack) {
            if (state.fioUi.col_fio && state.fioPack.columns.indexOf(state.fioUi.col_fio) < 0) {
              state.fioUi.source_error = "колонка ФИО не найдена: " + state.fioUi.col_fio;
            } else if (state.fioUi.col_tn && state.fioPack.columns.indexOf(state.fioUi.col_tn) < 0) {
              state.fioUi.source_error = "колонка табельного не найдена: " + state.fioUi.col_tn;
            }
          }
        } catch (err) {
          state.fioUi.source_error = "ошибка файла ФИО: " + (err.message || err);
        }
      } else if (parsed.fio.file_name) {
        state.fioUi.source_error = "файл ФИО не вложен в JSON: " + parsed.fio.file_name;
      }
    }

    invalidateChecks();
    state.activeId = state.tournaments[0] ? state.tournaments[0].id : null;
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
        preview_row_limit: 100,
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
    clearLegacyStorage();
    restoreDraft();

    $("btn-add-tournament").addEventListener("click", addTournament);
    $("btn-save-settings").addEventListener("click", function () {
      flushEditorToState();
      ReportIO.downloadJson(ReportIO.timestampName("web_report_settings", "json"), buildSettingsPayload());
      showToast("JSON настроек (+файлы)");
    });
    $("import-settings").addEventListener("change", async function (ev) {
      var file = ev.target.files && ev.target.files[0];
      ev.target.value = "";
      if (!file) return;
      try {
        var data = await readJsonFile(file);
        await loadSettingsFromJson(data);
        renderAll();
        showToast("Настройки и файлы загружены");
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
