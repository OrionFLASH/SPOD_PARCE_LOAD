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
      file_path: "",
      source_file_kind: "",
      source_error: "",
      apply_warning: "",
      apply_issues: [],
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
    filtersOpen: false,
    chromeOpen: true,
    previewOpen: { source: false, fio: false },
  };

  /** Транзитное состояние модалки «Загрузить списки» (сбрасывается при открытии). */
  var importTour = {
    step: "files",
    schedulePack: null,
    contestPack: null,
    reportPack: null,
    selectedStatuses: {},
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

    var included = ReportCore.includedTournaments(state.tournaments);
    var total = state.tournaments.length;
    var includedN = included.length;
    var noActive = includedN === 0;
    var anyFio = state.tournaments.some(function (t) {
      return String(t.type_ind || "").toUpperCase() === "FIO";
    });
    var includedFio = included.some(function (t) {
      return String(t.type_ind || "").toUpperCase() === "FIO";
    });
    var activeIsFio = hasActiveFioMode();
    // ФИО-этап не участвует: режим TN у текущего или в списке нет FIO
    var fioIdle = !anyFio || !activeIsFio;

    function stageStatus(ok, idle) {
      if (idle) return "idle";
      return ok ? "done" : "bad";
    }

    var icons = {
      tournaments:
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M8 6h13"/><path d="M8 12h13"/><path d="M8 18h13"/><path d="M3 6h.01"/><path d="M3 12h.01"/><path d="M3 18h.01"/></svg>',
      fields:
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 20h9"/><path d="M16.5 3.5a2.1 2.1 0 0 1 3 3L7 19l-4 1 1-4Z"/></svg>',
      sources:
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 3H6a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"/><path d="M14 3v6h6"/><path d="M12 18v-6"/><path d="M9 15l3 3 3-3"/></svg>',
      fio:
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="8" r="4"/><path d="M4 20c1.5-3.5 4.5-5 8-5s6.5 1.5 8 5"/></svg>',
      dups:
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="11" height="11" rx="2"/><path d="M5 15V5a2 2 0 0 1 2-2h10"/></svg>',
    };

    var tournamentsDetail;
    var tournamentsOk = st.hasTournaments && !noActive;
    if (total === 0) {
      tournamentsDetail = "добавьте турнир";
    } else if (noActive) {
      tournamentsDetail = "нет к выгрузке (все выкл.)";
    } else {
      tournamentsDetail = includedN + " из " + total + " в отчёте";
    }

    var fieldsDetail = noActive
      ? "ожидает активный турнир"
      : st.fieldsFilled
        ? "параметры заполнены"
        : st.blockedCopies.length
          ? "смените код и название копии"
          : "заполните коды, название, план";

    var sourcesDetail = noActive
      ? "нечего загружать"
      : st.sourcesOk
        ? "файлы и колонки ок"
        : "нужен файл и колонки";

    var fioDetail;
    if (!anyFio) {
      fioDetail = "нет турниров FIO";
    } else if (!activeIsFio) {
      fioDetail = "не нужен для TN";
    } else if (!includedFio) {
      fioDetail = "FIO выключены в отчёте";
    } else if (!st.sourcesOk) {
      fioDetail = "сначала загрузите источник";
    } else if (st.fioOk) {
      fioDetail = "справочник покрывает";
    } else {
      fioDetail = "есть ФИО без табельного";
    }

    var dupsDetail = noActive
      ? "ожидает данные"
      : st.duplicatesOk
        ? "конфликтов нет"
        : "нужна проверка / решение";

    var items = [
      {
        key: "tournaments",
        title: "Турниры",
        detail: tournamentsDetail,
        status: stageStatus(tournamentsOk, false),
        tip: noActive ? "Нет активных турниров для выгрузки" : "Турниры, включённые в отчёт",
        icon: icons.tournaments,
      },
      {
        key: "fields",
        title: "Поля",
        detail: fieldsDetail,
        status: stageStatus(st.fieldsFilled, noActive),
        tip: "Обязательные параметры турниров",
        icon: icons.fields,
      },
      {
        key: "sources",
        title: "Источники",
        detail: sourcesDetail,
        status: stageStatus(st.sourcesOk, noActive),
        tip: "CSV/Excel и выбранные колонки",
        icon: icons.sources,
      },
      {
        key: "fio",
        title: "ФИО",
        detail: fioDetail,
        status: stageStatus(st.fioOk && includedFio, fioIdle || noActive || !includedFio),
        tip: fioIdle
          ? "Этап ФИО не участвует (TN или нет FIO-турниров)"
          : "Справочник ФИО ↔ табельный",
        icon: icons.fio,
      },
      {
        key: "dups",
        title: "Дубли",
        detail: dupsDetail,
        status: stageStatus(st.duplicatesOk, noActive),
        tip: "Конфликты ключей после проверки",
        icon: icons.dups,
      },
    ];

    box.innerHTML = items
      .map(function (it) {
        var cls = "stage-chip is-" + it.status;
        return (
          '<span class="' +
          cls +
          '" data-stage="' +
          it.key +
          '" data-tip="' +
          escapeHtml(it.tip) +
          '">' +
          '<span class="stage-chip__icon" aria-hidden="true">' +
          it.icon +
          "</span>" +
          '<span class="stage-chip__body">' +
          '<span class="stage-chip__title">' +
          escapeHtml(it.title) +
          "</span>" +
          '<span class="stage-chip__detail">' +
          escapeHtml(it.detail) +
          "</span>" +
          "</span></span>"
        );
      })
      .join("");

    $("btn-process").disabled =
      !st.hasTournaments || !st.fieldsFilled || !st.sourcesOk || st.blockedCopies.length > 0 || noActive;
    $("btn-check").disabled = !st.canCheck || noActive;
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
      if (t.import_warning) cls += " is-import-warn";
      btn.className = cls;
      var kind = tournamentReadyKind(t);
      var type = String(t.type_ind || "TN").toUpperCase();
      var periodBadge = periodBadges[t.id] || ReportCore.normalizePeriodCode(t.period_code);
      var pack = state.dataByTournament[t.id];
      var rowStats = ReportCore.tournamentRowStats(t, pack, state.fioEntries, coreOpts());
      var metricsHtml = "";
      if (rowStats.loaded) {
        metricsHtml =
          '<div class="ct-metrics">' +
          '<span class="ct-metric ct-metric--total" data-tip="Загружено табельных / строк">' +
          metricIcon("total") +
          "<b>" +
          rowStats.total +
          "</b></span>" +
          '<span class="ct-metric ct-metric--err" data-tip="Строк с ошибкой (не попадут в CSV)">' +
          metricIcon("error") +
          "<b>" +
          rowStats.errors +
          "</b></span>" +
          '<span class="ct-metric ct-metric--csv" data-tip="Строк, которые попадут в CSV">' +
          metricIcon("csv") +
          "<b>" +
          rowStats.forCsv +
          "</b></span>" +
          "</div>";
      } else if (t.source_file_name) {
        metricsHtml =
          '<div class="ct-metrics ct-metrics--empty">' +
          '<span class="ct-metric ct-metric--miss" data-tip="Файл указан, но не загружен">' +
          metricIcon("missing") +
          " нет данных</span></div>";
      }
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
        (t.import_warning
          ? '<span class="mini-badge mini-badge--import-warn" data-tip="' +
            escapeHtml("Загружено из списков, требует проверки: " + t.import_warning) +
            '">! СПИСКИ</span>'
          : "") +
        "</div>" +
        metricsHtml +
        "</div>" +
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
    var list = options ? options.slice() : [];
    var sel = selected || "";
    if (sel && list.indexOf(sel) < 0) {
      list.push(sel);
    }
    list.forEach(function (name) {
      var opt = document.createElement("option");
      opt.value = name;
      opt.textContent = name;
      if (name === sel) opt.selected = true;
      select.appendChild(opt);
    });
  }

  function isExcelKind(kind, fileName) {
    var k = String(kind || "").toLowerCase();
    if (k === "excel" || /xlsx?|xlsm/.test(k)) return true;
    var n = String(fileName || "").toLowerCase();
    return /\.xlsx?$|\.xlsm$/.test(n);
  }

  function renderSheetFieldHtml(id, pack, kind, fileName, sheetName) {
    var show =
      (pack && pack.kind === "excel") ||
      isExcelKind(kind, fileName) ||
      !!String(sheetName || "").trim();
    if (!show) return "";
    return (
      '<div class="field field--sheet"><label class="field-label" for="' +
      id +
      '">Лист</label>' +
      '<select class="field-select" id="' +
      id +
      '" data-tip="Лист Excel"></select></div>'
    );
  }

  function fillSheetSelect(select, pack, preferred) {
    if (!select) return;
    var names =
      pack && pack.sheetNames && pack.sheetNames.length
        ? pack.sheetNames.slice()
        : [];
    var sel = preferred || (pack && pack.sheetName) || "";
    fillSelect(select, names, sel);
  }

  function renderPreviewFold(kind, pack, highlightCols) {
    var open = !!(state.previewOpen && state.previewOpen[kind]);
    var hostId = kind === "fio" ? "fio-preview-host" : "data-preview-host";
    var foldId = kind === "fio" ? "fio-preview-fold" : "data-preview-fold";
    var title =
      kind === "fio" ? "Превью таблицы ФИО" : "Превью данных источника";
    var body;
    if (pack) {
      body = renderPreviewTable(pack, null, highlightCols);
    } else {
      body =
        '<div class="warn-box">Файл ещё не загружен — превью появится после загрузки.</div>';
    }
    return (
      '<details class="preview-fold" id="' +
      foldId +
      '"' +
      (open ? " open" : "") +
      ">" +
      "<summary>" +
      title +
      (pack && pack.rows ? " · " + pack.rows.length + " строк" : "") +
      "</summary>" +
      '<div id="' +
      hostId +
      '">' +
      body +
      "</div></details>"
    );
  }

  function bindPreviewFold(kind) {
    var foldId = kind === "fio" ? "fio-preview-fold" : "data-preview-fold";
    var el = $(foldId);
    if (!el) return;
    el.addEventListener("toggle", function () {
      if (!state.previewOpen) state.previewOpen = { source: false, fio: false };
      state.previewOpen[kind] = !!el.open;
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
    var sheetBlock = renderSheetFieldHtml(
      "fio-sheet",
      pack,
      ui.source_file_kind,
      ui.file_name,
      ui.sheet_name
    );
    var fioErr = state.fioUi.source_error
      ? '<div class="error-box">' + escapeHtml(state.fioUi.source_error) + "</div>"
      : "";
    var hasIssues = state.fioUi.apply_issues && state.fioUi.apply_issues.length > 0;
    var fioApplyWarn = state.fioUi.apply_warning
      ? '<div class="warn-box warn-box--row" id="fio-apply-warning">' +
        "<span>" +
        escapeHtml(state.fioUi.apply_warning) +
        "</span>" +
        (hasIssues
          ? '<button type="button" class="btn btn-sm" id="btn-fio-issues" data-tip="Показать строки с дублями и нечисловым табельным">Подробнее</button>'
          : "") +
        "</div>"
      : "";
    return (
      '<div class="panel" id="panel-fio">' +
      "<h2>Справочник ФИО</h2>" +
      '<p class="panel__intro panel__intro--tight">Режим FIO: JSON или таблица · угол · колонки ФИО и табельного.</p>' +
      fioErr +
      fioApplyWarn +
      '<div class="toolbar-row toolbar-row--top">' +
      '<div class="info-box info-box--inline">Записей: <b id="fio-stats">' +
      state.fioEntries.length +
      "</b></div>" +
      '<label class="btn file-pick" data-tip="Загрузить ранее сохранённый JSON справочника">' +
      "<span>Открыть JSON</span>" +
      '<input type="file" id="import-fio-json" class="file-pick__input" accept=".json,application/json" /></label>' +
      '<button type="button" class="btn" id="btn-save-fio" data-tip="Сохранить справочник ФИО в JSON">Сохранить JSON</button>' +
      '<label class="btn btn-primary file-pick" data-tip="Загрузить CSV/Excel со столбцами ФИО и табельный">' +
      '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 21V9"/><path d="M7 14l5-5 5 5"/><path d="M5 3h14"/></svg> ' +
      "<span>Загрузить таблицу ФИО</span>" +
      '<input type="file" id="import-fio-table" class="file-pick__input" accept=".csv,.txt,.xlsx,.xls,.xlsm" /></label>' +
      '<button type="button" class="btn btn-primary" id="btn-apply-fio-table" disabled data-tip="Взять строки из таблицы в справочник (нужны файл и колонки)">Применить</button>' +
      "</div>" +
      '<div class="fields-grid">' +
      '<div class="fields-row fields-row--table-map' +
      (sheetBlock ? "" : " fields-row--table-map--nosheet") +
      '">' +
      (sheetBlock || "") +
      '<div class="field field--corner"><label class="field-label" for="fio-start-row">Стр.</label>' +
      '<input class="field-input field-input--corner" id="fio-start-row" type="number" min="1" step="1" data-tip="Строка угла (заголовок)" /></div>' +
      '<div class="field field--corner"><label class="field-label" for="fio-start-col">Кол.</label>' +
      '<input class="field-input field-input--corner" id="fio-start-col" type="number" min="1" step="1" data-tip="Колонка угла" /></div>' +
      '<div class="field field--colpick"><label class="field-label" for="fio-col-fio">Колонка ФИО</label>' +
      '<select class="field-select" id="fio-col-fio"></select></div>' +
      '<div class="field field--colpick"><label class="field-label" for="fio-col-tn">Колонка табельного</label>' +
      '<select class="field-select" id="fio-col-tn"></select></div>' +
      "</div>" +
      '<div class="fields-row fields-row--file-meta">' +
      '<div class="field"><label class="field-label" for="fio-file-name">Имя файла</label>' +
      '<input class="field-input" id="fio-file-name" readonly /></div>' +
      '<div class="field"><label class="field-label" for="fio-file-path">Путь к файлу</label>' +
      '<input class="field-input" id="fio-file-path" placeholder="examples/fio.xlsx или URL" data-tip="Относительный путь или URL для автозагрузки при открытии JSON" /></div>' +
      "</div>" +
      "</div>" +
      renderPreviewFold("fio", pack, [ui.col_fio, ui.col_tn]) +
      "</div>"
    );
  }

  function bindFioPanel() {
    if (!hasActiveFioMode()) return;
    var pack = state.fioPack;
    var ui = state.fioUi;
    $("fio-start-row").value = ui.start_row || 1;
    $("fio-start-col").value = ui.start_col || 1;
    $("fio-file-name").value = ui.file_name || "";
    if ($("fio-file-path")) $("fio-file-path").value = ui.file_path || "";
    var colOpts = pack ? pack.columns : [];
    fillSelect($("fio-col-fio"), colOpts, ui.col_fio || "");
    fillSelect($("fio-col-tn"), colOpts, ui.col_tn || "");
    if ($("fio-sheet")) {
      fillSheetSelect($("fio-sheet"), pack, ui.sheet_name || "");
      $("fio-sheet").addEventListener("change", function () {
        state.fioUi.sheet_name = $("fio-sheet").value;
        if (state.fioPack) reapplyFioOrigin({ sheetName: $("fio-sheet").value });
      });
    }
    bindPreviewFold("fio");
    refreshFioFieldHighlights();

    ["fio-start-row", "fio-start-col"].forEach(function (id) {
      $(id).addEventListener("change", function () {
        if (!state.fioPack) {
          state.fioUi.start_row = Number($("fio-start-row").value) || 1;
          state.fioUi.start_col = Number($("fio-start-col").value) || 1;
          return;
        }
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
        if (state.fioPack && $("fio-preview-host")) {
          $("fio-preview-host").innerHTML = renderPreviewTable(state.fioPack, null, [
            state.fioUi.col_fio,
            state.fioUi.col_tn,
          ]);
        }
        updateFioApplyEnabled();
        refreshFioFieldHighlights();
      });
    });
    if ($("fio-file-path")) {
      $("fio-file-path").addEventListener("change", function () {
        state.fioUi.file_path = $("fio-file-path").value.trim();
      });
    }
    updateFioApplyEnabled();
    if ($("btn-fio-issues")) {
      $("btn-fio-issues").addEventListener("click", openFioIssuesModal);
    }

    $("import-fio-json").addEventListener("change", async function (ev) {
      var file = ev.target.files && ev.target.files[0];
      ev.target.value = "";
      if (!file) return;
      try {
        state.fioEntries = ReportCore.parseFioDictionary(await readJsonFile(file));
        state.fioUi.apply_warning = "";
        state.fioUi.apply_issues = [];
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
        // новая загрузка: угол 1,1; колонки сбрасываем — нужно выбрать вручную
        var pack = await ReportIO.readTableFile(file, 1, 1);
        state.fioPack = pack;
        state.fioUi.file_name = pack.fileName;
        state.fioUi.file_path = state.fioUi.file_path || "";
        state.fioUi.sheet_name = pack.sheetName || "";
        state.fioUi.source_file_kind = pack.kind || "";
        state.fioUi.start_row = 1;
        state.fioUi.start_col = 1;
        state.fioUi.col_fio = "";
        state.fioUi.col_tn = "";
        state.fioUi.source_error = "";
        state.fioUi.apply_warning = "";
        state.fioUi.apply_issues = [];
        renderAll();
        showToast("Таблица ФИО загружена — выберите колонки");
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
      var resolved = ReportCore.resolveFioTableEntries(state.fioPack.rows, colFio, colTn);
      var entries = resolved.entries;
      if (!entries.length) {
        state.fioUi.apply_warning = "";
        state.fioUi.apply_issues = [];
        alert("Не удалось прочитать ни одной строки с ФИО");
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
      state.fioUi.apply_warning = resolved.stats.message || "";
      state.fioUi.apply_issues = resolved.issues || [];
      invalidateChecks();
      renderAll();
      var toastMsg = "В справочник: " + entries.length;
      if (resolved.stats.message) {
        toastMsg += ". " + resolved.stats.message;
      }
      showToast(toastMsg);
    });
  }

  function refreshFioFieldHighlights() {
    if (!$("fio-col-fio")) return;
    function blank(v) {
      return !String(v == null ? "" : v).trim();
    }
    function mark(id, need) {
      var el = $(id);
      if (!el) return;
      if (el.classList.contains("is-error")) return;
      el.classList.toggle("is-highlight", !!need);
    }
    var pack = state.fioPack;
    mark("fio-col-fio", !pack || blank(state.fioUi.col_fio));
    mark("fio-col-tn", !pack || blank(state.fioUi.col_tn));
    mark("fio-file-name", !pack || blank(state.fioUi.file_name));
    if ($("fio-sheet")) {
      mark(
        "fio-sheet",
        !pack || blank(state.fioUi.sheet_name || (pack && pack.sheetName))
      );
    }
  }

  function updateFioApplyEnabled() {
    var btn = $("btn-apply-fio-table");
    if (!btn) return;
    var ok =
      !!state.fioPack &&
      !!($("fio-col-fio") && $("fio-col-fio").value) &&
      !!($("fio-col-tn") && $("fio-col-tn").value);
    btn.disabled = !ok;
  }

  function openFioIssuesModal() {
    var issues = state.fioUi.apply_issues || [];
    var body = $("modal-fio-issues-body");
    var modal = $("modal-fio-issues");
    if (!body || !modal) return;
    if (!issues.length) {
      body.innerHTML = '<div class="info-box">Проблемных строк нет.</div>';
    } else {
      var rowsHtml = issues
        .map(function (it) {
          var tnShow = it.person_number === "" ? "—" : escapeHtml(String(it.person_number));
          var reason = (it.reasons || []).join(", ") || "—";
          return (
            "<tr class=\"" +
            (it.chosen ? "is-chosen" : "") +
            (it.tnOk ? "" : " is-bad-tn") +
            '">' +
            "<td>" +
            escapeHtml(it.fio) +
            "</td>" +
            "<td class=\"mono\">" +
            tnShow +
            "</td>" +
            "<td>" +
            escapeHtml(reason) +
            "</td>" +
            "<td>" +
            (it.chosen
              ? '<span class="mini-badge mini-badge--ok">выбран</span>'
              : '<span class="mini-badge mini-badge--off">пропуск</span>') +
            "</td></tr>"
          );
        })
        .join("");
      body.innerHTML =
        '<div class="fio-issues-table-wrap"><table class="fio-issues-table">' +
        "<thead><tr><th>ФИО</th><th>Табельный / значение</th><th>Что не так</th><th>В справочник</th></tr></thead>" +
        "<tbody>" +
        rowsHtml +
        "</tbody></table></div>";
    }
    modal.hidden = false;
  }

  function closeFioIssuesModal() {
    var modal = $("modal-fio-issues");
    if (modal) modal.hidden = true;
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
    if (state.fioUi.col_fio && state.fioPack.columns.indexOf(state.fioUi.col_fio) < 0) {
      state.fioUi.col_fio = "";
    }
    if (state.fioUi.col_tn && state.fioPack.columns.indexOf(state.fioUi.col_tn) < 0) {
      state.fioUi.col_tn = "";
    }
    renderAll();
  }

  /** SVG-иконка для мини-метрик (16×16). */
  function metricIcon(kind) {
    if (kind === "total") {
      return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 6h16"/><path d="M4 12h16"/><path d="M4 18h10"/></svg>';
    }
    if (kind === "error") {
      return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="9"/><path d="M12 8v5"/><path d="M12 16h.01"/></svg>';
    }
    if (kind === "csv") {
      return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 3v12"/><path d="M7 10l5 5 5-5"/><path d="M5 21h14"/></svg>';
    }
    if (kind === "tournaments") {
      return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M8 21h8"/><path d="M12 17v4"/><path d="M7 4h10v4a5 5 0 0 1-10 0V4z"/><path d="M5 6H3a4 4 0 0 0 4 4"/><path d="M19 6h2a4 4 0 0 1-4 4"/></svg>';
    }
    if (kind === "active") {
      return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M5 12l5 5L20 7"/></svg>';
    }
    if (kind === "filled") {
      return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="4" y="4" width="16" height="16" rx="2"/><path d="M8 12h8"/><path d="M8 8h8"/><path d="M8 16h5"/></svg>';
    }
    if (kind === "empty") {
      return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="4" y="4" width="16" height="16" rx="2"/><path d="M9 12h6"/></svg>';
    }
    if (kind === "dup") {
      return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="8" y="8" width="12" height="12" rx="2"/><path d="M4 16V6a2 2 0 0 1 2-2h10"/></svg>';
    }
    if (kind === "missing") {
      return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="9"/><path d="M8 12h8"/></svg>';
    }
    if (kind === "fio") {
      return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="8" r="3"/><path d="M5 19a7 7 0 0 1 14 0"/></svg>';
    }
    return "";
  }

  /** Сводная статистика по всем турнирам для правой/верхней панели. */
  function computePanelStats() {
    var total = state.tournaments.length;
    var active = 0;
    var filled = 0;
    var draft = 0;
    var copies = 0;
    var withSource = 0;
    var withoutSource = 0;
    var rowsTotal = 0;
    var rowsErrors = 0;
    var rowsCsv = 0;
    var fioIssues = (state.fioUi.apply_issues || []).length;
    var fioDupNames = 0;
    var fioBadTn = 0;
    (state.fioUi.apply_issues || []).forEach(function (it) {
      if (it.isDuplicate) fioDupNames += 1;
      if (!it.tnOk) fioBadTn += 1;
    });
    state.tournaments.forEach(function (t) {
      if (ReportCore.tournamentIncluded(t)) active += 1;
      var kind = tournamentReadyKind(t);
      if (kind === "ready") filled += 1;
      else if (kind === "copy") copies += 1;
      else if (kind !== "off") draft += 1;
      var pack = state.dataByTournament[t.id];
      if (pack && pack.rows) {
        withSource += 1;
        var rs = ReportCore.tournamentRowStats(t, pack, state.fioEntries, coreOpts());
        rowsTotal += rs.total;
        rowsErrors += rs.errors;
        rowsCsv += rs.forCsv;
      } else {
        withoutSource += 1;
      }
    });
    return {
      total: total,
      active: active,
      filled: filled,
      draft: draft,
      copies: copies,
      withSource: withSource,
      withoutSource: withoutSource,
      rowsTotal: rowsTotal,
      rowsErrors: rowsErrors,
      rowsCsv: rowsCsv,
      fioEntries: state.fioEntries.length,
      fioIssues: fioIssues,
      fioDupNames: fioDupNames,
      fioBadTn: fioBadTn,
      resultOk: !!(state.lastResult && state.lastResult.ok),
      resultCsv: state.lastResult && state.lastResult.csvRows ? state.lastResult.csvRows.length : 0,
      resultXlsx: state.lastResult && state.lastResult.xlsxRows ? state.lastResult.xlsxRows.length : 0,
    };
  }

  function renderSideStats() {
    var host = $("side-stats");
    if (!host) return;
    var s = computePanelStats();
    if (!s.total) {
      host.innerHTML = '<div class="side-stats__empty">Добавьте турнир — здесь появится сводка.</div>';
      return;
    }
    function card(kind, label, value, tone, tip) {
      return (
        '<div class="stat-card' +
        (tone ? " stat-card--" + tone : "") +
        '" data-tip="' +
        escapeHtml(tip || label) +
        '">' +
        '<span class="stat-card__icon" aria-hidden="true">' +
        metricIcon(kind) +
        "</span>" +
        '<span class="stat-card__body">' +
        '<span class="stat-card__label">' +
        escapeHtml(label) +
        "</span>" +
        '<span class="stat-card__value">' +
        value +
        "</span></span></div>"
      );
    }
    var html =
      '<div class="stat-grid">' +
      card(
        "tournaments",
        "Турниров",
        s.total,
        "",
        "Всего турниров в списке (включая выключенные и копии)"
      ) +
      card(
        "active",
        "В выгрузке",
        s.active,
        s.active ? "ok" : "",
        "Турниры с галочкой «включать в проверку/выгрузку»"
      ) +
      card(
        "filled",
        "Заполнено",
        s.filled,
        "ok",
        "Готовые турниры: поля заполнены, источник загружен, код и название разблокированы"
      ) +
      card(
        "empty",
        "Неполных",
        s.draft + s.copies,
        s.draft + s.copies ? "warn" : "",
        "Черновики и копии без смены кода/названия (не готовы к выгрузке)"
      ) +
      card(
        "total",
        "Строк загружено",
        s.rowsTotal,
        "",
        "Сумма строк из загруженных файлов источников по всем турнирам"
      ) +
      card(
        "error",
        "С ошибкой",
        s.rowsErrors,
        s.rowsErrors ? "bad" : "",
        "Строки с битым табельным, пустыми полями или флагом «табельный не найден» — в CSV не попадут"
      ) +
      card(
        "csv",
        "Попадут в CSV",
        s.rowsCsv,
        "ok",
        "Строки без критических ошибок, которые можно выгрузить в CSV"
      ) +
      card(
        "missing",
        "Без источника",
        s.withoutSource,
        s.withoutSource ? "warn" : "",
        "Турниры, у которых ещё не загружен файл данных"
      ) +
      "</div>";
    if (hasAnyFioMode() || s.fioEntries || s.fioIssues) {
      html +=
        '<div class="stat-grid stat-grid--fio">' +
        card(
          "fio",
          "Записей ФИО",
          s.fioEntries,
          "",
          "Число записей в справочнике ФИО ↔ табельный"
        ) +
        card(
          "dup",
          "Проблем ФИО",
          s.fioIssues,
          s.fioIssues ? "warn" : "",
          "Строки таблицы ФИО с дублями или некорректным табельным (см. «Подробнее»)"
        ) +
        card(
          "error",
          "Битый ТН",
          s.fioBadTn,
          s.fioBadTn ? "bad" : "",
          "Строки справочника ФИО, где табельный пуст или не из цифр"
        ) +
        "</div>";
    }
    host.innerHTML = html;
  }

  function renderTopInfo() {
    var el = $("top-info");
    if (!el) return;
    var parts = [];
    var s = computePanelStats();
    if (s.total) {
      parts.push(
        '<span class="top-info__pill">' +
          metricIcon("tournaments") +
          " <b>" +
          s.total +
          "</b> турн. · <b>" +
          s.active +
          "</b> в выгрузке · <b>" +
          s.filled +
          "</b> готовы</span>"
      );
    }
    if (s.rowsTotal) {
      parts.push(
        '<span class="top-info__pill">' +
          metricIcon("total") +
          " строк <b>" +
          s.rowsTotal +
          "</b> · ошибок <b>" +
          s.rowsErrors +
          "</b> · в CSV <b>" +
          s.rowsCsv +
          "</b></span>"
      );
    }
    if (state.fioUi.apply_warning) {
      parts.push(
        '<span class="top-info__pill top-info__pill--warn">' +
          metricIcon("dup") +
          " " +
          escapeHtml(state.fioUi.apply_warning) +
          "</span>"
      );
    }
    if (s.resultOk) {
      parts.push(
        '<span class="top-info__pill top-info__pill--ok">' +
          metricIcon("csv") +
          " результат: XLSX <b>" +
          s.resultXlsx +
          "</b> · CSV <b>" +
          s.resultCsv +
          "</b></span>"
      );
    }
    if (!parts.length) {
      el.hidden = true;
      el.innerHTML = "";
      return;
    }
    el.hidden = false;
    el.innerHTML = parts.join("");
  }

  function renderSideResult() {
    var host = $("side-result");
    var summary = $("result-summary");
    var warn = $("result-warnings");
    if (!host || !summary) return;
    if (!(state.lastResult && state.lastResult.ok)) {
      host.hidden = true;
      summary.textContent = "";
      if (warn) {
        warn.hidden = true;
        warn.textContent = "";
      }
      return;
    }
    host.hidden = false;
    summary.textContent =
      "Строк XLSX: " +
      state.lastResult.xlsxRows.length +
      "; CSV: " +
      state.lastResult.csvRows.length;
    if (warn) {
      if (state.lastResult.missingFio && state.lastResult.missingFio.length) {
        warn.hidden = false;
        warn.textContent = "ФИО без табельного: " + state.lastResult.missingFio.join("; ");
      } else {
        warn.hidden = true;
        warn.textContent = "";
      }
    }
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

    // одна строка: лист · стр · кол · колонки ID/показателя
    var sheetBlock = renderSheetFieldHtml(
      "f-sheet",
      pack,
      t.source_file_kind,
      t.source_file_name,
      t.sheet_name
    );

    ws.innerHTML =
      '<div class="panel" id="panel-params">' +
      "<h2>Параметры турнира</h2>" +
      '<p class="panel__intro panel__intro--tight">План — целое или с запятой (100 или 100,5); в выгрузке — 0.00000.</p>' +
      (lock
        ? '<div class="warn-box">Копия: смените <b>код турнира</b> и <b>наименование</b> — иначе формирование заблокировано.</div>'
        : "") +
      '<div class="fields-grid">' +
      '<div class="field field--check"><label class="check-row"><input type="checkbox" id="f-include" ' +
      (t.include_in_report !== false ? "checked" : "") +
      ' /> <span>Включать в проверку и выгрузку</span></label></div>' +
      '<div class="field field--third"><label class="field-label" for="f-contest-code">Код конкурса</label><input class="field-input" id="f-contest-code" /></div>' +
      '<div class="field field--third"><label class="field-label" for="f-tournament-code">Код турнира</label><input class="field-input' +
      codeHighlight +
      '" id="f-tournament-code" />' +
      (lock ? '<div class="field-hint">Изменить относительно копии</div>' : "") +
      "</div>" +
      '<div class="field field--third"><label class="field-label" for="f-period">Период</label><select class="field-select" id="f-period">' +
      periodOpts +
      "</select></div>" +
      '<div class="field field--full"><label class="field-label" for="f-full-name">Наименование турнира</label><input class="field-input' +
      nameHighlight +
      '" id="f-full-name" />' +
      (lock ? '<div class="field-hint">Изменить относительно копии</div>' : "") +
      "</div>" +
      '<div class="field field--third"><label class="field-label" for="f-plan">План</label>' +
      '<input class="field-input" id="f-plan" placeholder="100 или 100,5" data-tip="Целое или дробь с запятой; в CSV/XLSX — точка и 5 знаков" /></div>' +
      '<div class="field field--third"><label class="field-label" for="f-date">Дата данных</label><input class="field-input" id="f-date" type="date" /></div>' +
      '<div class="field field--third"><label class="field-label" for="f-type">Тип расчёта</label><select class="field-select" id="f-type"><option value="TN">TN — табельный</option><option value="FIO">FIO — ФИО</option></select></div>' +
      "</div></div>" +
      '<div class="panel" id="panel-data">' +
      "<h2>Источник данных</h2>" +
      '<p class="panel__intro panel__intro--tight">CSV (;) или Excel · угол таблицы · колонки · действие над показателем.</p>' +
      sourceErrHtml +
      '<div class="toolbar-row toolbar-row--top">' +
      '<label class="btn btn-primary file-pick" data-tip="Загрузить CSV или Excel с показателями">' +
      '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 21V9"/><path d="M7 14l5-5 5 5"/><path d="M5 3h14"/></svg> Загрузить CSV / Excel' +
      '<input type="file" id="import-data" class="file-pick__input" accept=".csv,.txt,.xlsx,.xls,.xlsm" /></label>' +
      '<span class="mini-badge" id="data-meta"></span></div>' +
      '<div class="fields-grid">' +
      '<div class="fields-row fields-row--table-map' +
      (sheetBlock ? "" : " fields-row--table-map--nosheet") +
      '">' +
      (sheetBlock || "") +
      '<div class="field field--corner"><label class="field-label" for="f-start-row">Стр.</label>' +
      '<input class="field-input field-input--corner" id="f-start-row" type="number" min="1" step="1" data-tip="Строка угла (заголовок)" /></div>' +
      '<div class="field field--corner"><label class="field-label" for="f-start-col">Кол.</label>' +
      '<input class="field-input field-input--corner" id="f-start-col" type="number" min="1" step="1" data-tip="Колонка угла" /></div>' +
      '<div class="field field--colpick"><label class="field-label" for="f-col-id">Колонка ФИО / табельного</label>' +
      '<select class="field-select" id="f-col-id"></select></div>' +
      '<div class="field field--colpick"><label class="field-label" for="f-col-fact">Колонка показателя</label>' +
      '<select class="field-select" id="f-col-fact"></select></div>' +
      "</div>" +
      '<div class="fields-row fields-row--ops">' +
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
      '<div class="field field--op-val"><label class="field-label" for="f-fact-op-value">Число</label>' +
      '<input class="field-input" id="f-fact-op-value" placeholder="100" data-tip="Для ×100 из доли 0,5 получится 50" /></div>' +
      '<div class="field"><label class="field-label" for="f-source-name">Имя файла</label>' +
      '<input class="field-input" id="f-source-name" readonly /></div>' +
      '<div class="field"><label class="field-label" for="f-source-path">Путь к файлу</label>' +
      '<input class="field-input" id="f-source-path" placeholder="examples/data.xlsx или URL" data-tip="Относительный путь или URL для автозагрузки при открытии JSON" /></div>' +
      "</div>" +
      "</div>" +
      renderPreviewFold("source", pack, [t.column_id, t.column_fact]) +
      "</div>" +
      fioHtml;

    $("f-contest-code").value = t.contest_code || "";
    $("f-tournament-code").value = t.tournament_code || "";
    $("f-full-name").value = t.full_name || "";
    $("f-plan").value = t.plan_value || "";
    $("f-date").value = t.contest_date || "";
    $("f-type").value = String(t.type_ind || "TN").toUpperCase() === "FIO" ? "FIO" : "TN";
    $("f-source-name").value = t.source_file_name || "";
    if ($("f-source-path")) $("f-source-path").value = t.source_file_path || "";
    $("f-start-row").value = t.table_start_row || (pack && pack.start_row) || 1;
    $("f-start-col").value = t.table_start_col || (pack && pack.start_col) || 1;
    $("f-fact-op-value").value = t.fact_op_value != null ? t.fact_op_value : "1";

    var colId = $("f-col-id");
    var colFact = $("f-col-fact");
    var colOpts = pack ? pack.columns : [];
    fillSelect(colId, colOpts, t.column_id || "");
    fillSelect(colFact, colOpts, t.column_fact || "");
    if (pack) {
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
    } else {
      $("data-meta").textContent = t.source_file_name
        ? "файл не загружен: " + t.source_file_name
        : "файл не загружен";
    }
    if ($("f-sheet")) {
      fillSheetSelect($("f-sheet"), pack, t.sheet_name || "");
      if (
        pack &&
        t.sheet_name &&
        pack.sheetNames &&
        pack.sheetNames.indexOf(t.sheet_name) < 0
      ) {
        $("f-sheet").classList.add("is-error");
      }
      $("f-sheet").addEventListener("change", function () {
        t.sheet_name = $("f-sheet").value;
        if (pack) reapplyDataOrigin({ sheetName: $("f-sheet").value });
      });
    }
    bindPreviewFold("source");

    bindEditorEvents();
    bindFioPanel();
    refreshRequiredFieldHighlights();
  }

  /** Подсветка незаполненных обязательных полей (и полей копии). */
  function refreshRequiredFieldHighlights() {
    var t = activeTournament();
    if (!t || !$("f-contest-code")) return;
    var pack = state.dataByTournament[t.id];
    var lock = !ReportCore.tournamentIdentityUnlocked(t);

    function blank(v) {
      return !String(v == null ? "" : v).trim();
    }

    function mark(id, needHighlight) {
      var el = $(id);
      if (!el) return;
      if (el.classList.contains("is-error")) return;
      el.classList.toggle("is-highlight", !!needHighlight);
    }

    mark("f-contest-code", blank(t.contest_code));
    mark("f-tournament-code", blank(t.tournament_code) || lock);
    mark("f-full-name", blank(t.full_name) || lock);
    mark("f-plan", !ReportCore.planValueOk(t));
    mark("f-fact-op-value", !ReportCore.factOpValueOk(t));
    mark("f-date", blank(t.contest_date));
    mark("f-col-id", !pack || blank(t.column_id));
    mark("f-col-fact", !pack || blank(t.column_fact));
    mark("f-source-name", !pack || blank(t.source_file_name));
    if ($("f-sheet")) {
      mark("f-sheet", !pack || blank(t.sheet_name || (pack && pack.sheetName)));
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
    if (t.column_id && next.columns.indexOf(t.column_id) < 0) {
      t.column_id = "";
    }
    if (t.column_fact && next.columns.indexOf(t.column_fact) < 0) {
      t.column_fact = "";
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
    renderSideStats();
    renderTopInfo();
    refreshRequiredFieldHighlights();
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
    if ($("f-source-path")) t.source_file_path = $("f-source-path").value.trim();
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
    renderSideStats();
    renderTopInfo();
    renderSideResult();
    persistDraft();
  }

  function addTournament() {
    flushEditorToState();
    var t = ReportCore.createEmptyTournament({
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
      // новая загрузка: угол 1,1; колонки сбрасываем — нужно выбрать вручную
      var pack = await ReportIO.readTableFile(file, 1, 1);
      state.dataByTournament[t.id] = pack;
      t.source_file_name = pack.fileName;
      t.source_file_kind = pack.kind;
      t.source_error = "";
      t.sheet_name = pack.sheetName || "";
      t.table_start_row = 1;
      t.table_start_col = 1;
      t.column_id = "";
      t.column_fact = "";
      applySourceErrors(t, pack);
      invalidateChecks();
      renderAll();
      showToast("Данные загружены — выберите колонки");
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

  var IMPORT_TOUR_FILES = [
    {
      key: "schedule",
      title: "SCHEDULE",
      required: true,
      hint: "Список турниров: коды, период (PERIOD_TYPE), статус (TOURNAMENT_STATUS)",
      requiredCols: ["TOURNAMENT_CODE", "CONTEST_CODE", "PERIOD_TYPE", "TOURNAMENT_STATUS"],
    },
    {
      key: "contest",
      title: "CONTEST",
      required: true,
      hint: "Названия и план турниров: FULL_NAME, PLAN_MOD_VALUE по CONTEST_CODE",
      requiredCols: ["CONTEST_CODE", "FULL_NAME", "PLAN_MOD_VALUE"],
    },
    {
      key: "report",
      title: "REPORT",
      required: false,
      hint: "Дата турнира — самая новая CONTEST_DATE по TOURNAMENT_CODE (необязателен)",
      requiredCols: ["TOURNAMENT_CODE", "CONTEST_DATE"],
    },
  ];

  function importTourPackKey(key) {
    return key + "Pack";
  }

  /** Все обязательные колонки файла присутствуют в разобранной таблице. */
  function packHasColumns(pack, cols) {
    if (!pack) return false;
    var have = pack.columns || [];
    return (cols || []).every(function (c) {
      return have.indexOf(c) >= 0;
    });
  }

  function resetImportTour() {
    importTour = {
      step: "files",
      schedulePack: null,
      contestPack: null,
      reportPack: null,
      selectedStatuses: {},
    };
  }

  function renderImportFileRow(spec) {
    var pack = importTour[importTourPackKey(spec.key)];
    var metaText = "не выбран";
    var metaCls = "";
    if (pack) {
      var missing = (spec.requiredCols || []).filter(function (c) {
        return (pack.columns || []).indexOf(c) < 0;
      });
      if (missing.length) {
        metaText = "не хватает колонок: " + missing.join(", ");
        metaCls = " is-error";
      } else {
        metaText = pack.fileName + " · строк: " + pack.rows.length;
        metaCls = " is-ok";
      }
    } else if (!spec.required) {
      metaText = "не выбран — дата турниров будет сегодняшней";
    }
    return (
      '<div class="import-file-row">' +
      '<label class="btn btn-primary file-pick" data-tip="' +
      escapeHtml(spec.hint) +
      '"><svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 21V9"/><path d="M7 14l5-5 5 5"/><path d="M5 3h14"/></svg>' +
      "<span>" +
      spec.title +
      (spec.required ? "*" : "") +
      "</span>" +
      '<input type="file" id="import-tour-file-' +
      spec.key +
      '" class="file-pick__input" accept=".csv,.txt" /></label>' +
      '<div class="import-file-row__meta' +
      metaCls +
      '" id="import-tour-meta-' +
      spec.key +
      '">' +
      escapeHtml(metaText) +
      "</div></div>"
    );
  }

  function renderImportTourStepFiles() {
    var host = $("import-tour-body");
    if (!host) return;
    host.innerHTML = IMPORT_TOUR_FILES.map(renderImportFileRow).join("");
    IMPORT_TOUR_FILES.forEach(function (spec) {
      var input = $("import-tour-file-" + spec.key);
      if (!input) return;
      input.addEventListener("change", async function (ev) {
        var file = ev.target.files && ev.target.files[0];
        ev.target.value = "";
        if (!file) return;
        try {
          var pack = await ReportIO.readTableFile(file, 1, 1);
          importTour[importTourPackKey(spec.key)] = pack;
        } catch (err) {
          alert(err.message || String(err));
          return;
        }
        renderImportTourStepFiles();
        updateImportTourPrimaryState();
      });
    });
    updateImportTourPrimaryState();
  }

  function renderImportTourStepStatus() {
    var host = $("import-tour-body");
    if (!host) return;
    var counts = ReportCore.scheduleStatusCounts(importTour.schedulePack.rows);
    var chips = counts
      .map(function (s) {
        var on = !!importTour.selectedStatuses[s.status];
        return (
          '<button type="button" class="chip' +
          (on ? " is-on" : "") +
          '" data-status="' +
          escapeHtml(s.status) +
          '" aria-pressed="' +
          (on ? "true" : "false") +
          '">' +
          escapeHtml(s.status) +
          '<span class="mini-badge">' +
          s.count +
          "</span></button>"
        );
      })
      .join("");
    var reportNote = importTour.reportPack
      ? "REPORT: " + importTour.reportPack.rows.length + " строк"
      : "REPORT не загружен — дата турниров будет сегодняшней";
    host.innerHTML =
      '<div class="info-box">SCHEDULE: ' +
      importTour.schedulePack.rows.length +
      " строк · CONTEST: " +
      importTour.contestPack.rows.length +
      " строк · " +
      reportNote +
      ".</div>" +
      '<div class="filter-block__label" style="margin-top:12px">Статусы турниров (TOURNAMENT_STATUS)</div>' +
      '<div class="toolbar-row toolbar-row--top">' +
      '<button type="button" class="btn btn-sm" id="import-tour-status-all">Отметить все</button>' +
      '<button type="button" class="btn btn-sm" id="import-tour-status-none">Снять все</button>' +
      "</div>" +
      '<div class="import-tour-status-scroll"><div class="chip-row" id="import-tour-status-list" role="group">' +
      (chips || '<span class="filter-hint">В SCHEDULE нет строк со статусом.</span>') +
      "</div></div>" +
      '<div class="info-box" id="import-tour-status-count"></div>';

    host.querySelectorAll("[data-status]").forEach(function (chip) {
      chip.addEventListener("click", function () {
        var status = chip.getAttribute("data-status");
        importTour.selectedStatuses[status] = !importTour.selectedStatuses[status];
        chip.classList.toggle("is-on", !!importTour.selectedStatuses[status]);
        chip.setAttribute("aria-pressed", importTour.selectedStatuses[status] ? "true" : "false");
        updateImportTourStatusCount();
        updateImportTourPrimaryState();
      });
    });
    var allBtn = $("import-tour-status-all");
    var noneBtn = $("import-tour-status-none");
    if (allBtn) {
      allBtn.addEventListener("click", function () {
        counts.forEach(function (s) {
          importTour.selectedStatuses[s.status] = true;
        });
        renderImportTourStepStatus();
        updateImportTourPrimaryState();
      });
    }
    if (noneBtn) {
      noneBtn.addEventListener("click", function () {
        importTour.selectedStatuses = {};
        renderImportTourStepStatus();
        updateImportTourPrimaryState();
      });
    }
    updateImportTourStatusCount();
  }

  function updateImportTourStatusCount() {
    var el = $("import-tour-status-count");
    if (!el) return;
    var statuses = Object.keys(importTour.selectedStatuses).filter(function (s) {
      return importTour.selectedStatuses[s];
    });
    var rows = importTour.schedulePack ? importTour.schedulePack.rows : [];
    var n = rows.filter(function (r) {
      return statuses.indexOf(String((r && r.TOURNAMENT_STATUS) || "").trim()) >= 0;
    }).length;
    el.textContent = statuses.length
      ? "Отмечено статусов: " + statuses.length + " · строк в SCHEDULE: " + n
      : "Отметьте хотя бы один статус.";
  }

  function updateImportTourPrimaryState() {
    var primary = $("import-tour-primary");
    var back = $("import-tour-back");
    if (!primary) return;
    if (importTour.step === "files") {
      if (back) back.hidden = true;
      var scheduleOk = packHasColumns(importTour.schedulePack, IMPORT_TOUR_FILES[0].requiredCols);
      var contestOk = packHasColumns(importTour.contestPack, IMPORT_TOUR_FILES[1].requiredCols);
      primary.textContent = "Далее";
      primary.disabled = !(scheduleOk && contestOk);
    } else {
      if (back) back.hidden = false;
      var anyStatus = Object.keys(importTour.selectedStatuses).some(function (s) {
        return importTour.selectedStatuses[s];
      });
      primary.textContent = "Загрузить турниры";
      primary.disabled = !anyStatus;
    }
  }

  function openImportTournamentsModal() {
    resetImportTour();
    importTour.step = "files";
    renderImportTourStepFiles();
    openModal("modal-import-tour");

    var primary = $("import-tour-primary");
    var back = $("import-tour-back");
    var cancel = $("import-tour-cancel");
    primary.onclick = function () {
      if (importTour.step === "files") {
        importTour.step = "status";
        renderImportTourStepStatus();
        updateImportTourPrimaryState();
      } else {
        runImportTournaments();
      }
    };
    back.onclick = function () {
      importTour.step = "files";
      renderImportTourStepFiles();
      updateImportTourPrimaryState();
    };
    cancel.onclick = function () {
      closeModal("modal-import-tour");
      resetImportTour();
    };
  }

  function runImportTournaments() {
    if (!importTour.schedulePack || !importTour.contestPack) return;
    var statuses = Object.keys(importTour.selectedStatuses).filter(function (s) {
      return importTour.selectedStatuses[s];
    });
    if (!statuses.length) return;
    flushEditorToState();
    var existingCodes = state.tournaments.map(function (t) {
      return t.tournament_code;
    });
    var reportRows = importTour.reportPack ? importTour.reportPack.rows : [];
    var result = ReportCore.buildTournamentsFromSourceFiles(
      importTour.schedulePack.rows,
      importTour.contestPack.rows,
      reportRows,
      statuses,
      existingCodes
    );
    if (result.tournaments.length) {
      state.tournaments = state.tournaments.concat(result.tournaments);
      state.activeId = result.tournaments[0].id;
      invalidateChecks();
    }
    closeModal("modal-import-tour");
    resetImportTour();
    renderAll();

    var parts = ["загружено: " + result.stats.imported];
    if (result.stats.withWarnings) parts.push("требуют проверки: " + result.stats.withWarnings);
    if (result.stats.skippedDuplicate) parts.push("пропущено, уже есть: " + result.stats.skippedDuplicate);
    if (result.stats.skippedNoCode) parts.push("без кода турнира: " + result.stats.skippedNoCode);
    var msg = "Турниры из списков: " + parts.join(" · ");
    showToast(msg);
    setStatus(msg);
    if (!result.tournaments.length) {
      alert("Не загружено ни одного турнира с выбранными статусами (возможно, все уже есть в списке).");
    } else if (result.stats.withWarnings) {
      alert(
        "Загружено турниров: " +
          result.stats.imported +
          ", из них требуют проверки параметров: " +
          result.stats.withWarnings +
          ".\nОни отмечены в списке слева меткой «! СПИСКИ» (фиолетовая рамка) — наведите на метку, чтобы увидеть причину.\n" +
          "«Включать в проверку и выгрузку» у всех загруженных турниров выключено — включите после проверки параметров и загрузки источника данных."
      );
    }
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
        var maxLen = (state.config && state.config.person_number_length) || 20;
        var badInput = null;
        inputs.forEach(function (inp) {
          var num = inp.value.trim();
          var bad = !!num && !(/^\d+$/.test(num) && num.length <= maxLen);
          inp.classList.toggle("is-error", bad);
          if (bad && !badInput) badInput = inp;
        });
        if (badInput) {
          alert("Табельный — только цифры, не длиннее " + maxLen + " (ведущие нули допустимы).");
          badInput.focus();
          return;
        }
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
      alert(
        "Заполните все параметры включённых турниров (код конкурса, код турнира, план, дата, название, тип, период).\n" +
          "План и число операции над показателем — числа: 100, 100,5, -3,25."
      );
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

  /** До 10 уникальных причин по ТН и показателю — чтобы было видно, что именно не так. */
  function renderCsvReasonSamples(validation) {
    var seen = Object.create(null);
    var items = [];
    (validation.badPerson || []).concat(validation.badFact || []).forEach(function (it) {
      var text = (it.tournament_code || "") + ": " + (it.reason || it.value || "");
      if (seen[text] || items.length >= 10) return;
      seen[text] = true;
      items.push("<li>" + escapeHtml(text) + "</li>");
    });
    if (!items.length) return "";
    return '<div class="warn-box" style="margin-top:10px">Примеры:<ul style="margin:6px 0 0 18px;padding:0">' + items.join("") + "</ul></div>";
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
        (s.bad_fact || 0) +
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
      "<th>Турнир</th><th>Дубли строк</th><th>ТН≠20 цифр</th><th>Показатель не число</th><th>Пустые ячейки</th>" +
      "</tr></thead><tbody>" +
      (lines.join("") || "<tr><td colspan='5'>нет детализации</td></tr>") +
      "</tbody></table>" +
      renderCsvReasonSamples(validation) +
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
    if ($("fio-file-path")) state.fioUi.file_path = $("fio-file-path").value.trim();
    var fioMeta = {
      entries: state.fioEntries,
      file_name: state.fioUi.file_name || "",
      file_path: state.fioUi.file_path || "",
      sheet_name: state.fioUi.sheet_name || "",
      start_row: state.fioUi.start_row || 1,
      start_col: state.fioUi.start_col || 1,
      col_fio: state.fioUi.col_fio || "",
      col_tn: state.fioUi.col_tn || "",
    };
    return ReportCore.serializeSettings(state.tournaments, fioMeta);
  }

  function applySourceErrors(t, pack) {
    var errs = [];
    if (!pack) {
      if (t.source_file_name) {
        errs.push("загрузите файл источника: " + t.source_file_name);
      } else {
        errs.push("файл источника не загружен");
      }
      t.source_error = errs.join("; ");
      return;
    }
    if (t.source_file_name && pack.fileName && pack.fileName !== t.source_file_name) {
      // имя может отличаться — не ошибка, но подсказка в meta
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
    state.fioUi = {
      sheet_name: "",
      start_row: 1,
      start_col: 1,
      col_fio: "",
      col_tn: "",
      file_name: "",
      file_path: "",
      source_file_kind: "",
      source_error: "",
      apply_warning: "",
      apply_issues: [],
    };

    // ФИО: entries + метаданные колонок/пути
    if (parsed.fio) {
      if (Array.isArray(parsed.fio.entries)) {
        state.fioEntries = parsed.fio.entries
          .map(function (e) {
            return {
              fio: String(e.fio || "").trim(),
              person_number: String(e.person_number || "").trim(),
            };
          })
          .filter(function (e) {
            return e.fio && e.person_number;
          });
      } else {
        state.fioEntries = [];
      }
      state.fioUi.file_name = parsed.fio.file_name || "";
      state.fioUi.file_path = parsed.fio.file_path || "";
      state.fioUi.sheet_name = parsed.fio.sheet_name || "";
      state.fioUi.start_row = parsed.fio.start_row || 1;
      state.fioUi.start_col = parsed.fio.start_col || 1;
      state.fioUi.col_fio = parsed.fio.col_fio || "";
      state.fioUi.col_tn = parsed.fio.col_tn || "";
      state.fioUi.source_error = "";
    } else {
      state.fioEntries = [];
    }

    var loadedSources = 0;
    var loadedFio = false;
    var failed = [];

    // автозагрузка источников турниров, если путь/имя доступны по HTTP
    for (var i = 0; i < state.tournaments.length; i++) {
      var t = state.tournaments[i];
      t.source_file_b64 = "";
      if (!t.source_file_name && !t.source_file_path) {
        applySourceErrors(t, null);
        continue;
      }
      var sr = t.table_start_row || 1;
      var sc = t.table_start_col || 1;
      var got = await ReportIO.tryReadTableFromPaths(
        t.source_file_path,
        t.source_file_name,
        sr,
        sc
      );
      if (!got) {
        applySourceErrors(t, null);
        failed.push(t.source_file_name || t.source_file_path || t.id);
        continue;
      }
      var pack = got.pack;
      if (t.sheet_name && pack.kind === "excel") {
        pack = ReportIO.applyPackOrigin(pack, {
          sheetName: t.sheet_name,
          start_row: sr,
          start_col: sc,
        });
      }
      state.dataByTournament[t.id] = pack;
      t.source_file_kind = pack.kind || t.source_file_kind || "";
      if (got.usedPath) t.source_file_path = got.usedPath;
      if (!t.source_file_name) t.source_file_name = pack.fileName;
      // сохранить выбранные из JSON колонки; если пусто — не угадываем при restore
      applySourceErrors(t, pack);
      loadedSources += 1;
    }

    // автозагрузка таблицы ФИО
    if (state.fioUi.file_name || state.fioUi.file_path) {
      var fioGot = await ReportIO.tryReadTableFromPaths(
        state.fioUi.file_path,
        state.fioUi.file_name,
        state.fioUi.start_row || 1,
        state.fioUi.start_col || 1
      );
      if (fioGot) {
        var fioPack = fioGot.pack;
        if (state.fioUi.sheet_name && fioPack.kind === "excel") {
          fioPack = ReportIO.applyPackOrigin(fioPack, {
            sheetName: state.fioUi.sheet_name,
            start_row: state.fioUi.start_row || 1,
            start_col: state.fioUi.start_col || 1,
          });
        }
        state.fioPack = fioPack;
        state.fioUi.source_file_kind = fioPack.kind || "";
        if (fioGot.usedPath) state.fioUi.file_path = fioGot.usedPath;
        if (!state.fioUi.file_name) state.fioUi.file_name = fioPack.fileName;
        // если колонки из JSON не заданы — оставить пустыми (подсветка)
        loadedFio = true;
        state.fioUi.source_error = "";
      } else if (!state.fioEntries.length) {
        state.fioUi.source_error =
          "не удалось загрузить таблицу ФИО «" +
          (state.fioUi.file_path || state.fioUi.file_name) +
          "» — укажите доступный путь или загрузите файл";
        failed.push(state.fioUi.file_name || state.fioUi.file_path);
      } else {
        state.fioUi.source_error =
          "записи ФИО из JSON есть (" +
          state.fioEntries.length +
          "), таблица не подтянулась — проверьте путь «" +
          (state.fioUi.file_path || state.fioUi.file_name) +
          "»";
      }
    }

    invalidateChecks();
    state.activeId = state.tournaments[0] ? state.tournaments[0].id : null;

    var parts = [];
    if (loadedSources) parts.push("источников: " + loadedSources);
    if (loadedFio) parts.push("таблица ФИО");
    if (state.fioEntries.length) parts.push("записей ФИО: " + state.fioEntries.length);
    if (failed.length) {
      parts.push("не загружено: " + failed.slice(0, 3).join(", ") + (failed.length > 3 ? "…" : ""));
    }
    if (parts.length) showToast("JSON: " + parts.join(" · "));
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
      showToast("JSON настроек");
    });
    $("btn-import-tournaments").addEventListener("click", openImportTournamentsModal);
    $("import-settings").addEventListener("change", async function (ev) {
      var file = ev.target.files && ev.target.files[0];
      ev.target.value = "";
      if (!file) return;
      try {
        var data = await readJsonFile(file);
        await loadSettingsFromJson(data);
        renderAll();
        showToast("Настройки загружены — укажите файлы источников");
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

    var fioIssuesClose = $("modal-fio-issues-close");
    if (fioIssuesClose) {
      fioIssuesClose.addEventListener("click", closeFioIssuesModal);
    }
    var fioIssuesModal = $("modal-fio-issues");
    if (fioIssuesModal) {
      fioIssuesModal.addEventListener("click", function (ev) {
        if (ev.target === fioIssuesModal) closeFioIssuesModal();
      });
    }

    initTips();
    setSidebarOpen(true);
    setFiltersOpen(false);
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
