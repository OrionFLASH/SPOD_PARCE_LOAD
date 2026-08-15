/* Редактор каталога параметров BADGE-формы */
(function () {
  "use strict";

  const LS_KEY = "spod_param_review_catalog_v5";
  const LS_BASELINE_KEY = "spod_param_review_baseline_v5";
  const LS_SOURCE_KEY = "spod_param_review_source_v5";
  /** Только catalog.json рядом со страницей (без catalog.js и без соседних папок). */
  const CATALOG_URL = "./catalog.json";
  const KINDS = ["dropdown", "dropdown_custom", "text", "number", "list", "json", "date"];
  const KIND_LABELS = {
    dropdown: "Выбор из списка",
    dropdown_custom: "Список + свой вариант",
    text: "Свободный текст",
    number: "Число",
    list: "Массив значений",
    json: "JSON формат {[ ]}",
    date: "Дата (формат YYYY-MM-DD)",
  };
  const KIND_SHORT = {
    dropdown: "Список",
    dropdown_custom: "Список+",
    text: "Текст",
    number: "Число",
    list: "Массив",
    json: "JSON",
    date: "Дата",
  };
  /** Иконки типов ввода (inline SVG) */
  const KIND_ICONS = {
    dropdown:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M4 6h16"/><path d="M4 12h10"/><path d="M4 18h7"/><path d="M15 14l3 3 3-3"/></svg>',
    dropdown_custom:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M4 6h16"/><path d="M4 12h10"/><path d="M4 18h7"/><path d="M15 14l3 3 3-3"/><path d="M19 8v4"/><path d="M17 10h4"/></svg>',
    text:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M4 7V5h16v2"/><path d="M12 5v14"/><path d="M8 19h8"/></svg>',
    number:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M5 8h4"/><path d="M7 8v8"/><path d="M12 16V8l4 8V8"/></svg>',
    list:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M9 6h11"/><path d="M9 12h11"/><path d="M9 18h11"/><path d="M4 6h.01"/><path d="M4 12h.01"/><path d="M4 18h.01"/></svg>',
    json:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M8 4c-2 0-3 1.5-3 4s1 4 0 6-3 4-3 4"/><path d="M16 4c2 0 3 1.5 3 4s-1 4 0 6 3 4 3 4"/></svg>',
    date:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="3" y="5" width="18" height="16" rx="2"/><path d="M3 10h18"/><path d="M8 3v4"/><path d="M16 3v4"/></svg>',
  };
  const STATUSES = ["[ ]", "[w]", "[v]"];
  const STATUS_LABELS = {
    "[ ]": "Не готово",
    "[w]": "В работе",
    "[v]": "Готово",
  };
  const STATUS_TIPS = {
    "[ ]": "Не готово — поле ещё не проверено, описание может быть черновым",
    "[w]": "В работе — правите или уточняете; ещё не финальный вариант",
    "[v]": "Готово — подпись, тип и описание согласованы",
  };
  const STATUS_ICONS = {
    "[ ]":
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="8"/><path d="M9 12h6"/></svg>',
    "[w]":
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M12 20h9"/><path d="M16.5 3.5a2.1 2.1 0 0 1 3 3L7 19l-4 1 1-4L16.5 3.5z"/></svg>',
    "[v]":
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="9"/><path d="M8.5 12.5l2.2 2.2 4.8-5"/></svg>',
  };
  const KIND_TIPS = {
    dropdown: "Выбор из списка — одно значение из вариантов (выпадающий список в Excel)",
    dropdown_custom:
      "Список + свой вариант — варианты как у списка; в fill можно выбрать чип или ввести своё значение",
    text: "Свободный текст — произвольная строка без кавычек-ограничений списка",
    number: "Число — значение без кавычек (в JSON/SPOD как number)",
    list: "Массив значений — несколько элементов через «;» в Excel; варианты по строкам",
    json: "JSON формат {[ ]} — структура как в SPOD",
    date: "Дата в формате YYYY-MM-DD (например 4000-01-01)",
  };

  function kindHasVariants(kind) {
    return kind === "dropdown" || kind === "dropdown_custom" || kind === "list";
  }

  /** @type {any} */
  let catalog = null;
  /** @type {Record<string, string>} снимок после загрузки / импорта / сохранения JSON */
  let baseline = {};
  let activeSectionId = null;
  let saveTimer = null;
  /** Выбранные группы правок: edited | clean; обе по умолчанию */
  let editFilter = new Set(["edited", "clean"]);
  /** Выбранные статусы готовности для фильтра */
  let statusFilter = new Set(["[ ]", "[w]", "[v]"]);
  /** @type {string} */
  let sourceStamp = "";

  const $ = (id) => document.getElementById(id);

  function cloneCatalog(src) {
    return JSON.parse(JSON.stringify(src));
  }

  function catalogStamp(data) {
    return String(
      data.exported_at || data.generated_at || data.version || ""
    );
  }

  function validateCatalog(data) {
    if (!data || !Array.isArray(data.sections)) {
      throw new Error("В JSON нет sections[]");
    }
    return data;
  }

  function showToast(text) {
    const toast = $("save-toast");
    const label = $("save-toast-text");
    if (label) label.textContent = text || "Сохранено локально";
    toast.hidden = false;
    clearTimeout(saveTimer);
    saveTimer = setTimeout(() => {
      toast.hidden = true;
    }, 1400);
  }

  function showApp() {
    const root = $("app-root");
    if (root) root.hidden = false;
  }

  function hasCatalog() {
    return !!(catalog && Array.isArray(catalog.sections) && catalog.sections.length);
  }

  function syncWorkspaceMode() {
    const empty = $("sidebar-empty");
    const loaded = $("sidebar-loaded");
    const searchWrap = $("search-wrap");
    const on = hasCatalog();
    if (empty) empty.hidden = on;
    if (loaded) loaded.hidden = !on;
    if (searchWrap) searchWrap.hidden = !on;
    if (!on) {
      const title = $("section-title");
      const intro = $("section-intro");
      const fields = $("fields");
      if (title) title.textContent = "Настройка описания параметров";
      if (intro) {
        intro.textContent =
          "Выберите catalog.json в панели слева, чтобы начать.";
      }
      if (fields) fields.innerHTML = "";
    }
  }

  function applyCatalog(data, { resetBaseline = true, persistDraft = true } = {}) {
    catalog = cloneCatalog(validateCatalog(data));
    normalizeCatalogFields(catalog);
    sourceStamp = catalogStamp(catalog);
    activeSectionId = catalog.sections[0]?.id || null;
    if (resetBaseline) captureBaseline();
    else if (!loadBaseline()) captureBaseline();
    if (persistDraft) persist();
    showApp();
    syncWorkspaceMode();
    renderAll();
  }

  /** Канонический снимок поля для сравнения с baseline (без ложных «правок»). */
  function fieldSnapshot(field) {
    const variants = Array.isArray(field.variants)
      ? field.variants.map((x) => String(x).trim()).filter(Boolean)
      : [];
    const rawLabs = Array.isArray(field.variant_labels)
      ? field.variant_labels.map((x) => String(x ?? "").trim())
      : [];
    const labels = variants.map((_, i) => (i < rawLabs.length ? rawLabs[i] : ""));
    return {
      status: field.status || "[ ]",
      label: field.label || "",
      kind: field.kind || "text",
      variants,
      variant_labels: labels.some(Boolean) ? labels : [],
      default: field.default || "",
      allow_empty: !!field.allow_empty,
      description: field.description || "",
      note: field.note || "",
      json_target: field.json_target || "",
    };
  }

  function fieldFingerprint(field) {
    return JSON.stringify(fieldSnapshot(field));
  }

  function fieldBaselineKey(sectionId, fieldKey) {
    return `${sectionId}::${fieldKey}`;
  }

  function applySnapshotToField(field, snap) {
    field.status = snap.status || "[ ]";
    field.label = snap.label || "";
    field.kind = snap.kind || "text";
    field.variants = Array.isArray(snap.variants) ? snap.variants.slice() : [];
    field.variant_labels = Array.isArray(snap.variant_labels)
      ? snap.variant_labels.slice()
      : [];
    field.default = snap.default || "";
    field.allow_empty = !!snap.allow_empty;
    field.description = snap.description || "";
    field.note = snap.note || "";
    if (snap.json_target != null) field.json_target = snap.json_target;
    alignVariantLabels(field);
  }

  function getBaselineSnapshot(sectionId, fieldKey) {
    const raw = baseline[fieldBaselineKey(sectionId, fieldKey)];
    if (raw == null) return null;
    try {
      return typeof raw === "string" ? JSON.parse(raw) : raw;
    } catch (_) {
      return null;
    }
  }

  function restoreFieldFromBaseline(sectionId, field) {
    const snap = getBaselineSnapshot(sectionId, field.key);
    if (!snap) return false;
    applySnapshotToField(field, snap);
    return true;
  }

  function restoreAllFromBaseline() {
    let n = 0;
    for (const sec of catalog.sections) {
      for (const f of sec.fields) {
        if (!isEdited(sec.id, f)) continue;
        if (restoreFieldFromBaseline(sec.id, f)) n += 1;
      }
    }
    return n;
  }

  function captureBaseline() {
    baseline = {};
    for (const sec of catalog.sections) {
      for (const f of sec.fields) {
        baseline[fieldBaselineKey(sec.id, f.key)] = fieldFingerprint(f);
      }
    }
    try {
      localStorage.setItem(LS_BASELINE_KEY, JSON.stringify(baseline));
      localStorage.setItem(LS_SOURCE_KEY, sourceStamp || catalogStamp(catalog));
    } catch (_) {
      /* ignore */
    }
  }

  function loadBaseline() {
    try {
      const raw = localStorage.getItem(LS_BASELINE_KEY);
      if (!raw) return false;
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== "object") return false;
      baseline = parsed;
      return true;
    } catch (_) {
      return false;
    }
  }

  function isEdited(sectionId, field) {
    const key = fieldBaselineKey(sectionId, field.key);
    if (!(key in baseline)) return true;
    return fieldFingerprint(field) !== baseline[key];
  }

  function countEdited() {
    let n = 0;
    if (!catalog) return 0;
    for (const sec of catalog.sections) {
      for (const f of sec.fields) {
        if (isEdited(sec.id, f)) n += 1;
      }
    }
    return n;
  }

  function syncRevertAllBtn() {
    const btn = $("btn-revert-all");
    if (!btn) return;
    const n = countEdited();
    const on = n > 0;
    btn.disabled = !on;
    btn.classList.toggle("is-disabled", !on);
    btn.setAttribute(
      "data-tip",
      on
        ? `Вернуть все отредактированные поля (${n}) к состоянию на момент загрузки или импорта. Черновик в браузере обновится.`
        : "Нет правок — возвращать нечего. Появится после изменения хотя бы одного поля."
    );
  }

  async function fetchCatalogJson() {
    const url = new URL(CATALOG_URL, window.location.href).href;
    try {
      const res = await fetch(url, { cache: "no-store" });
      if (!res.ok) {
        throw new Error(`${url} → HTTP ${res.status}`);
      }
      return validateCatalog(await res.json());
    } catch (err) {
      const msg = err && err.message ? err.message : String(err);
      throw new Error(msg);
    }
  }

  async function loadSourceCatalog() {
    return fetchCatalogJson();
  }

  function tryRestoreDraft(fileStamp) {
    try {
      const raw = localStorage.getItem(LS_KEY);
      const savedStamp = localStorage.getItem(LS_SOURCE_KEY) || "";
      if (!raw) return null;
      if (fileStamp && savedStamp && savedStamp !== fileStamp) return null;
      const saved = JSON.parse(raw);
      if (!saved || !Array.isArray(saved.sections)) return null;
      return saved;
    } catch (_) {
      return null;
    }
  }

  function enterEmptyWorkspace() {
    catalog = null;
    baseline = {};
    activeSectionId = null;
    sourceStamp = "";
    showApp();
    syncWorkspaceMode();
  }

  async function bootFromFile({ forceFile = false } = {}) {
    showApp();
    try {
      const fileData = await loadSourceCatalog();
      const fileStamp = catalogStamp(fileData);
      if (!forceFile) {
        const draft = tryRestoreDraft(fileStamp);
        if (draft) {
          sourceStamp = fileStamp || catalogStamp(draft);
          catalog = draft;
          normalizeCatalogFields(catalog);
          if (!loadBaseline()) captureBaseline();
          syncWorkspaceMode();
          renderAll();
          showToast("Черновик из браузера");
          return;
        }
      }
      applyCatalog(fileData, { resetBaseline: true, persistDraft: true });
      showToast("Загружен catalog.json");
    } catch (_) {
      if (!forceFile) {
        // без файла на диске — взять черновик, если уже открывали
        const draft = tryRestoreDraft("");
        if (draft) {
          sourceStamp = catalogStamp(draft);
          catalog = draft;
          normalizeCatalogFields(catalog);
          if (!loadBaseline()) captureBaseline();
          syncWorkspaceMode();
          renderAll();
          showToast("Черновик из браузера");
          return;
        }
      }
      enterEmptyWorkspace();
    }
  }

  function persist() {
    if (!catalog) return;
    localStorage.setItem(LS_KEY, JSON.stringify(catalog));
    localStorage.setItem(LS_SOURCE_KEY, sourceStamp || catalogStamp(catalog));
    showToast("Черновик в браузере");
  }

  function schedulePersist() {
    clearTimeout(saveTimer);
    saveTimer = setTimeout(persist, 250);
  }

  function counts() {
    let v = 0;
    let w = 0;
    let open = 0;
    let total = 0;
    for (const sec of catalog.sections) {
      for (const f of sec.fields) {
        total += 1;
        if (f.status === "[v]") v += 1;
        else if (f.status === "[w]") w += 1;
        else open += 1;
      }
    }
    return { v, w, open, total };
  }

  function sectionProgress(sec) {
    const total = sec.fields.length;
    const done = sec.fields.filter((f) => f.status === "[v]").length;
    return { total, done };
  }

  function renderNav() {
    const nav = $("section-nav");
    nav.innerHTML = "";
    for (const sec of catalog.sections) {
      const btn = document.createElement("button");
      btn.type = "button";
      const isChild = !!sec.parent;
      const kind = sec.kind || "table";
      btn.className =
        "nav-btn" +
        (sec.id === activeSectionId ? " active" : "") +
        (isChild ? " nav-btn--child" : "") +
        (kind === "json" ? " nav-btn--json" : " nav-btn--table");
      const p = sectionProgress(sec);
      const label = escapeHtml(sec.menu_label || sec.title);
      const kindBadge =
        kind === "json"
          ? `<span class="nav-kind nav-kind--json">JSON</span>`
          : `<span class="nav-kind nav-kind--table">TABLE</span>`;
      const nest = isChild
        ? `<span class="nav-nest" aria-hidden="true">↳</span>`
        : "";
      btn.innerHTML =
        `<span class="nav-main">${nest}${kindBadge}<span class="nav-label">${label}</span></span>` +
        `<span class="count">${p.done}/${p.total}</span>`;
      btn.title = "";
      const tipParts = [];
      if (sec.intro) tipParts.push(sec.intro);
      if (sec.sheet) tipParts.push(`Лист Excel: ${sec.sheet}`);
      tipParts.push(kind === "json" ? "Тип раздела: JSON" : "Тип раздела: TABLE");
      tipParts.push(`Готово: ${p.done} из ${p.total}`);
      btn.setAttribute("data-tip", tipParts.join("\n"));
      btn.addEventListener("click", () => {
        activeSectionId = sec.id;
        const searchEl = $("search");
        if (searchEl && searchEl.value) searchEl.value = "";
        renderAll();
        window.scrollTo({ top: 0, behavior: "smooth" });
      });
      const q = searchQuery();
      if (q) {
        const hitCount = sec.fields.filter((f) => matchesFilter(f, sec.id)).length;
        if (hitCount) {
          btn.classList.add("nav-btn--hit");
          btn.querySelector(".count").textContent = String(hitCount);
        } else {
          btn.classList.add("nav-btn--miss");
        }
      }
      nav.appendChild(btn);
    }
    const c = counts();
    const edited = countEdited();
    $("sidebar-stats").innerHTML =
      `<strong style="color:var(--glass-green)">готово</strong> ${c.v}` +
      ` · <strong style="color:var(--glass-orange)">в работе</strong> ${c.w}` +
      ` · <strong style="color:var(--glass-gray)">не готово</strong> ${c.open}` +
      ` · всего ${c.total}` +
      (edited
        ? ` · <span class="stats-edited">отред. ${edited}</span>`
        : "");
  }

  function activeSection() {
    return catalog.sections.find((s) => s.id === activeSectionId) || catalog.sections[0];
  }

  function matchesFilter(field, sectionId) {
    if (statusFilter.size > 0 && !statusFilter.has(field.status || "[ ]")) {
      return false;
    }
    const dirty = isEdited(sectionId, field);
    if (dirty && !editFilter.has("edited")) return false;
    if (!dirty && !editFilter.has("clean")) return false;
    const q = ($("search").value || "").trim().toLowerCase();
    if (!q) return true;
    const variants = Array.isArray(field.variants)
      ? field.variants.join(" ")
      : String(field.variants || "");
    const labels = Array.isArray(field.variant_labels)
      ? field.variant_labels.join(" ")
      : "";
    const blob = [
      field.key,
      field.label,
      field.description,
      field.note,
      field.default,
      field.json_target,
      field.kind,
      variants,
      labels,
    ]
      .map((x) => String(x ?? ""))
      .join(" ")
      .toLowerCase();
    return blob.includes(q);
  }

  function searchQuery() {
    return ($("search").value || "").trim();
  }

  function collectSearchHits() {
    /** @type {{ sec: any, fields: any[] }[]} */
    const groups = [];
    for (const sec of catalog.sections) {
      const fields = sec.fields.filter((f) => matchesFilter(f, sec.id));
      if (fields.length) groups.push({ sec, fields });
    }
    return groups;
  }

  function applyEditedUi(card, dirty) {
    card.classList.toggle("is-edited", dirty);
    const badge = card.querySelector("[data-role='edited-badge']");
    if (badge) badge.hidden = !dirty;
    const revert = card.querySelector("[data-role='revert-field']");
    if (revert) {
      revert.disabled = !dirty;
      revert.classList.toggle("is-disabled", !dirty);
      revert.setAttribute(
        "data-tip",
        dirty
          ? "Вернуть только это поле к состоянию на момент загрузки / импорта"
          : "Нет правок — возвращать нечего. Появится после изменения поля."
      );
    }
  }

  function closeAllDatePops() {
    document.querySelectorAll(".date-pop").forEach((p) => {
      p.hidden = true;
      p.classList.remove("is-open");
      if (p.parentNode) p.parentNode.removeChild(p);
    });
    document.querySelectorAll(".default-date.is-picker-open").forEach((w) => {
      w.classList.remove("is-picker-open");
    });
    window.__spodActiveDateWrap = null;
    window.__spodActiveDatePop = null;
  }

  function afterFieldEdit(card, field, sectionId) {
    const dirty = isEdited(sectionId, field);
    applyEditedUi(card, dirty);
    if (dirty && !editFilter.has("edited")) card.hidden = true;
    else if (!dirty && !editFilter.has("clean")) card.hidden = true;
    else card.hidden = false;
    // попап даты в body — при скрытии карточки его нужно закрыть явно
    if (card.hidden) {
      const dateWrap = card.querySelector(".default-date");
      if (dateWrap && window.__spodActiveDateWrap === dateWrap) {
        closeActiveDatePop();
      }
    }
    schedulePersist();
    const edited = countEdited();
    const stats = $("sidebar-stats");
    if (stats) {
      const c = counts();
      stats.innerHTML =
        `<strong style="color:var(--glass-green)">готово</strong> ${c.v}` +
        ` · <strong style="color:var(--glass-orange)">в работе</strong> ${c.w}` +
        ` · <strong style="color:var(--glass-gray)">не готово</strong> ${c.open}` +
        ` · всего ${c.total}` +
        (edited
          ? ` · <span class="stats-edited">отред. ${edited}</span>`
          : "");
    }
    syncRevertAllBtn();
  }

  function escapeHtml(s) {
    return String(s ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function variantsPlaceholder(kind) {
    if (kind === "dropdown" || kind === "dropdown_custom") {
      return "Каждое значение с новой строки:\nY\nN";
    }
    if (kind === "list") {
      return "Каждый элемент массива с новой строки:\nKMMMB\nKMKKSB\nCSM";
    }
    return "";
  }

  function variantLabelsPlaceholder() {
    return "Подписи в том же порядке (необязательно):\nДа\nНет";
  }

  /** Краткие подписи по умолчанию для известных кодов. */
  function defaultVariantLabel(value) {
    if (value === "Y") return "Да";
    if (value === "N") return "Нет";
    return "";
  }

  /**
   * Выровнять variant_labels по длине variants.
   * Лишние подписи отбрасываются; недостающие слоты — пустые
   * (для новых слотов при fillKnownEmpty — автоподстановка Y/N).
   */
  function alignVariantLabels(field, { fillKnownEmpty = true } = {}) {
    const variants = Array.isArray(field.variants)
      ? field.variants.map((x) => String(x).trim()).filter(Boolean)
      : [];
    field.variants = variants;
    const prev = Array.isArray(field.variant_labels)
      ? field.variant_labels.map((x) => String(x ?? "").trim())
      : [];
    const next = variants.map((v, i) => {
      if (i < prev.length) return prev[i];
      return fillKnownEmpty ? defaultVariantLabel(v) : "";
    });
    field.variant_labels = next;
    return {
      truncated: prev.length > variants.length,
      padded: prev.length < variants.length,
      labels: next,
    };
  }

  function labelForVariant(field, value) {
    const variants = Array.isArray(field.variants) ? field.variants : [];
    const i = variants.indexOf(value);
    if (i < 0) return "";
    const labels = Array.isArray(field.variant_labels) ? field.variant_labels : [];
    return String(labels[i] || "").trim();
  }

  function chipFaceNodes(value, label) {
    const lab = String(label || "").trim();
    const wrap = document.createElement("span");
    wrap.className = "default-chip__text";
    const main = document.createElement("span");
    main.className = "default-chip__label";
    main.textContent = lab || value;
    wrap.appendChild(main);
    if (lab) {
      const code = document.createElement("span");
      code.className = "default-chip__code";
      code.textContent = value;
      wrap.appendChild(code);
    }
    return wrap;
  }

  function syncVariantsUi(card, field, { rewriteLabels = true } = {}) {
    const ta = card.querySelector("[data-role='variants']");
    const taLab = card.querySelector("[data-role='variant-labels']");
    const hint = card.querySelector("[data-role='variants-hint']");
    const wrap = card.querySelector("[data-role='variants-wrap']");
    const wrapLab = card.querySelector("[data-role='variant-labels-wrap']");
    if (!ta) return;
    const active = kindHasVariants(field.kind);
    const aligned = alignVariantLabels(field, { fillKnownEmpty: true });
    ta.disabled = !active;
    ta.placeholder = active ? variantsPlaceholder(field.kind) : "";
    if (wrap) wrap.classList.toggle("is-disabled", !active);
    if (taLab) {
      taLab.disabled = !active;
      taLab.placeholder = active ? variantLabelsPlaceholder() : "";
      if (rewriteLabels) {
        const labs = aligned.labels.slice();
        while (labs.length < (field.variants || []).length) labs.push("");
        taLab.value = labs.join("\n");
      }
    }
    if (wrapLab) wrapLab.classList.toggle("is-disabled", !active);
    if (hint) {
      hint.hidden = !active;
      let base =
        field.kind === "list"
          ? "Массив: значения слева, подписи справа — в том же порядке. В CSV уходит значение."
          : field.kind === "dropdown_custom"
            ? "Список + свой вариант: слева коды, справа подписи (необязательно). В CSV — код."
            : "Выбор из списка: слева коды SPOD, справа понятные подписи (необязательно).";
      const nV = (field.variants || []).length;
      const nFilled = aligned.labels.filter(Boolean).length;
      if (active && nV) {
        base += ` Сейчас ${nV} знач. / ${nFilled} подп.`;
        if (aligned.truncated) {
          base += " Лишние строки подписей отброшены.";
        }
      }
      hint.textContent = base;
    }
    if (!active) {
      ta.value = (field.variants || []).join("\n");
      if (taLab && rewriteLabels) taLab.value = "";
    }
  }

  function parseDefaultList(raw) {
    return String(raw || "")
      .split(/[;\n]/)
      .map((x) => x.trim())
      .filter(Boolean);
  }

  function formatDefaultList(values) {
    return values.join(";");
  }

  function defaultHint(kind) {
    if (kind === "dropdown" || kind === "dropdown_custom") {
      return "Чип → дефолт в BLANK. «Можно пусто» = да: повторный клик снимает всё (пусто). = нет: снять нельзя.";
    }
    if (kind === "list") {
      return "Чипы → дефолт через ;. «Можно пусто» = да: можно снять все. = нет: хотя бы один чип.";
    }
    if (kind === "number") {
      return "Число без кавычек. Пусто = не предзаполнять.";
    }
    if (kind === "date") {
      return "Дата только YYYY-MM-DD: поле слева или чипы справа (начало / конец года / бесконечный = 4000-01-01).";
    }
    return "Предзаполнение ячейки в шаблоне Excel. Пусто = не предзаполнять.";
  }

  const DATE_INFINITE = "4000-01-01";

  function dateYearStart() {
    return `${new Date().getFullYear()}-01-01`;
  }

  function dateYearEnd() {
    return `${new Date().getFullYear()}-12-31`;
  }

  function dateToday() {
    const d = new Date();
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
  }

  function dateMonthStart(y, m) {
    return `${y}-${String(m + 1).padStart(2, "0")}-01`;
  }

  function dateMonthEnd(y, m) {
    const last = new Date(y, m + 1, 0).getDate();
    return `${y}-${String(m + 1).padStart(2, "0")}-${String(last).padStart(2, "0")}`;
  }

  function isIsoDate(s) {
    return /^\d{4}-\d{2}-\d{2}$/.test(String(s || "").trim());
  }

  function hideDatePopEl(pop) {
    if (!pop) return;
    pop.hidden = true;
    pop.classList.remove("is-open");
  }

  function closeActiveDatePop() {
    // попапы висят на document.body, не внутри .default-date
    document.querySelectorAll(".date-pop.is-open, .date-pop:not([hidden])").forEach((p) => {
      hideDatePopEl(p);
    });
    document.querySelectorAll(".default-date.is-picker-open").forEach((w) => {
      w.classList.remove("is-picker-open");
    });
    window.__spodActiveDateWrap = null;
    window.__spodActiveDatePop = null;
  }

  function isDatePickerUiTarget(t) {
    if (!t || !t.closest) return false;
    if (t.closest(".date-pop")) return true;
    if (t.closest(".default-date")) return true;
    return false;
  }

  function renderDefaultDateUi(host, field, sectionId, card) {
    const wrap = document.createElement("div");
    wrap.className = "default-date";
    wrap.dataset.role = "default";

    const row = document.createElement("div");
    row.className = "default-date__row";

    const fieldWrap = document.createElement("div");
    fieldWrap.className = "default-date__field";

    const text = document.createElement("input");
    text.type = "text";
    text.className = "default-date__iso";
    text.placeholder = "YYYY-MM-DD";
    text.spellcheck = false;
    text.setAttribute("inputmode", "numeric");
    text.setAttribute("autocomplete", "off");
    text.setAttribute(
      "data-tip",
      "Дата в формате YYYY-MM-DD (как в Excel / SPOD)"
    );
    text.setAttribute("aria-label", "Дата YYYY-MM-DD");

    const calBtn = document.createElement("button");
    calBtn.type = "button";
    calBtn.className = "default-date__cal";
    calBtn.setAttribute("data-tip", "Открыть календарь");
    calBtn.setAttribute("aria-label", "Календарь");
    calBtn.innerHTML =
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="3" y="5" width="18" height="16" rx="2"/><path d="M3 10h18"/><path d="M8 3v4"/><path d="M16 3v4"/></svg>';

    const pop = document.createElement("div");
    pop.className = "date-pop";
    pop.hidden = true;
    pop.setAttribute("role", "dialog");
    pop.setAttribute("aria-label", "Выбор даты");

    const presets = document.createElement("div");
    presets.className = "default-date__presets";
    presets.setAttribute("role", "group");
    presets.setAttribute("data-tip", "Быстрый выбор даты");

    const presetDefs = [
      {
        id: "year-start",
        label: "Начало года",
        tip: "1 января текущего года (YYYY-01-01)",
        value: () => dateYearStart(),
        icon:
          '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="5" width="18" height="16" rx="2"/><path d="M3 10h18"/><path d="M8 14h3"/><path d="M8 3v4"/><path d="M16 3v4"/></svg>',
      },
      {
        id: "year-end",
        label: "Конец года",
        tip: "31 декабря текущего года (YYYY-12-31)",
        value: () => dateYearEnd(),
        icon:
          '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="5" width="18" height="16" rx="2"/><path d="M3 10h18"/><path d="M13 14h3"/><path d="M8 3v4"/><path d="M16 3v4"/></svg>',
      },
      {
        id: "infinite",
        label: "Бесконечный",
        tip: "Без срока: 4000-01-01",
        value: () => DATE_INFINITE,
        icon:
          '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18.2 8.2a4.2 4.2 0 0 0-6 0L12 8.4l-.2-.2a4.2 4.2 0 1 0 0 6l.2.2.2-.2a4.2 4.2 0 0 0 6-6z"/><path d="M5.8 8.2a4.2 4.2 0 0 1 6 0L12 8.4l.2-.2a4.2 4.2 0 1 1 0 6l-.2.2-.2-.2a4.2 4.2 0 0 1-6-6z"/></svg>',
      },
    ];

    const view = { y: new Date().getFullYear(), m: new Date().getMonth() };
    const MONTHS = [
      "январь",
      "февраль",
      "март",
      "апрель",
      "май",
      "июнь",
      "июль",
      "август",
      "сентябрь",
      "октябрь",
      "ноябрь",
      "декабрь",
    ];
    const DOW = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"];

    function syncPresetActive() {
      const v = String(field.default || "").trim();
      presets.querySelectorAll(".default-date__chip").forEach((btn) => {
        const id = btn.getAttribute("data-preset");
        let match = false;
        if (id === "year-start") match = v === dateYearStart();
        else if (id === "year-end") match = v === dateYearEnd();
        else if (id === "infinite") match = v === DATE_INFINITE;
        btn.classList.toggle("is-on", match);
        btn.setAttribute("aria-pressed", match ? "true" : "false");
      });
    }

    function applyDate(next, { close = true } = {}) {
      const v = String(next || "").trim();
      field.default = isIsoDate(v) ? v : "";
      text.value = field.default;
      if (isIsoDate(field.default)) {
        const [yy, mm] = field.default.split("-").map(Number);
        view.y = yy;
        view.m = mm - 1;
      }
      syncPresetActive();
      if (close) {
        closePop();
      } else if (!pop.hidden) {
        paintPop();
      }
      afterFieldEdit(card, field, sectionId);
    }

    function closePop() {
      hideDatePopEl(pop);
      wrap.classList.remove("is-picker-open");
      if (window.__spodActiveDateWrap === wrap) {
        window.__spodActiveDateWrap = null;
      }
      if (window.__spodActiveDatePop === pop) {
        window.__spodActiveDatePop = null;
      }
    }

    function placePop() {
      const rect = fieldWrap.getBoundingClientRect();
      const pad = 8;
      const width = Math.min(320, Math.max(280, window.innerWidth - pad * 2));
      let left = rect.left;
      if (left + width > window.innerWidth - pad) {
        left = Math.max(pad, window.innerWidth - pad - width);
      }
      let top = rect.bottom + 6;
      pop.style.width = `${width}px`;
      pop.style.left = `${Math.round(left)}px`;
      pop.style.top = `${Math.round(top)}px`;
      // если снизу не влезает — над полем
      requestAnimationFrame(() => {
        const h = pop.offsetHeight || 320;
        if (top + h > window.innerHeight - pad && rect.top > h + pad) {
          pop.style.top = `${Math.round(rect.top - h - 6)}px`;
        }
      });
    }

    function openPop() {
      if (isIsoDate(field.default)) {
        const [yy, mm] = field.default.split("-").map(Number);
        view.y = yy;
        view.m = mm - 1;
      } else {
        const now = new Date();
        view.y = now.getFullYear();
        view.m = now.getMonth();
      }
      // закрыть все другие календари (попапы на body)
      document.querySelectorAll(".date-pop").forEach((p) => {
        if (p !== pop) hideDatePopEl(p);
      });
      document.querySelectorAll(".default-date.is-picker-open").forEach((w) => {
        if (w !== wrap) w.classList.remove("is-picker-open");
      });
      if (!pop.isConnected) document.body.appendChild(pop);
      paintPop();
      pop.hidden = false;
      pop.classList.add("is-open");
      wrap.classList.add("is-picker-open");
      window.__spodActiveDateWrap = wrap;
      window.__spodActiveDatePop = pop;
      placePop();
    }

    function paintPop() {
      const selected = isIsoDate(field.default) ? field.default : "";
      const first = new Date(view.y, view.m, 1);
      let startDow = first.getDay(); // 0=вс
      startDow = startDow === 0 ? 6 : startDow - 1; // пн=0
      const daysInMonth = new Date(view.y, view.m + 1, 0).getDate();

      let html =
        `<div class="date-pop__head">` +
        `<button type="button" class="date-pop__nav" data-nav="-1" aria-label="Предыдущий месяц">‹</button>` +
        `<div class="date-pop__title">${MONTHS[view.m]} ${view.y}</div>` +
        `<button type="button" class="date-pop__nav" data-nav="1" aria-label="Следующий месяц">›</button>` +
        `</div>` +
        `<div class="date-pop__dow">` +
        DOW.map((d) => `<span>${d}</span>`).join("") +
        `</div>` +
        `<div class="date-pop__grid">`;

      for (let i = 0; i < startDow; i += 1) {
        html += `<span class="date-pop__empty"></span>`;
      }
      for (let day = 1; day <= daysInMonth; day += 1) {
        const iso = `${view.y}-${String(view.m + 1).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
        const on = iso === selected ? " is-on" : "";
        const today =
          iso ===
          `${new Date().getFullYear()}-${String(new Date().getMonth() + 1).padStart(2, "0")}-${String(new Date().getDate()).padStart(2, "0")}`
            ? " is-today"
            : "";
        html += `<button type="button" class="date-pop__day${on}${today}" data-day="${iso}">${day}</button>`;
      }
      const mStart = dateMonthStart(view.y, view.m);
      const mEnd = dateMonthEnd(view.y, view.m);
      const todayIso = dateToday();
      html += `</div>`;
      html +=
        `<div class="date-pop__quick" role="group" aria-label="Быстрый выбор">` +
        `<button type="button" class="date-pop__qchip${selected === mStart ? " is-on" : ""}" data-q="month-start" data-tip="Первый день открытого месяца (${mStart})">` +
        `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M4 12h16"/><path d="M8 8l-4 4 4 4"/></svg>` +
        `Нач. мес.</button>` +
        `<button type="button" class="date-pop__qchip${selected === mEnd ? " is-on" : ""}" data-q="month-end" data-tip="Последний день открытого месяца (${mEnd})">` +
        `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M4 12h16"/><path d="M16 8l4 4-4 4"/></svg>` +
        `Кон. мес.</button>` +
        `<button type="button" class="date-pop__qchip${selected === todayIso ? " is-on" : ""}" data-q="today" data-tip="Сегодня (${todayIso})">` +
        `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="8"/><path d="M12 8v4l2 2"/></svg>` +
        `Сегодня</button>` +
        `</div>`;
      pop.innerHTML = html;

      pop.querySelectorAll("[data-nav]").forEach((btn) => {
        btn.addEventListener("click", (e) => {
          e.preventDefault();
          e.stopPropagation();
          const delta = Number(btn.getAttribute("data-nav") || 0);
          view.m += delta;
          if (view.m < 0) {
            view.m = 11;
            view.y -= 1;
          } else if (view.m > 11) {
            view.m = 0;
            view.y += 1;
          }
          paintPop();
        });
      });
      pop.querySelectorAll("[data-day]").forEach((btn) => {
        btn.addEventListener("click", (e) => {
          e.preventDefault();
          e.stopPropagation();
          applyDate(btn.getAttribute("data-day") || "");
        });
      });
      pop.querySelectorAll("[data-q]").forEach((btn) => {
        btn.addEventListener("click", (e) => {
          e.preventDefault();
          e.stopPropagation();
          const q = btn.getAttribute("data-q");
          let next = "";
          if (q === "month-start") next = dateMonthStart(view.y, view.m);
          else if (q === "month-end") next = dateMonthEnd(view.y, view.m);
          else if (q === "today") {
            next = dateToday();
            const d = new Date();
            view.y = d.getFullYear();
            view.m = d.getMonth();
          }
          applyDate(next, { close: true });
        });
      });
    }

    const cur = String(field.default || "").trim();
    if (isIsoDate(cur)) {
      text.value = cur;
    } else if (cur) {
      field.default = "";
    }

    for (const p of presetDefs) {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "default-date__chip";
      btn.setAttribute("data-preset", p.id);
      btn.setAttribute("data-tip", p.tip);
      btn.setAttribute("aria-pressed", "false");
      btn.innerHTML =
        `<span class="default-date__chip-icon" aria-hidden="true">${p.icon}</span>` +
        `<span class="default-date__chip-label">${escapeHtml(p.label)}</span>`;
      btn.addEventListener("click", () => {
        const next = p.value();
        applyDate(field.default === next ? "" : next, { close: true });
      });
      presets.appendChild(btn);
    }

    text.addEventListener("change", () => {
      const v = text.value.trim();
      if (!v) {
        applyDate("", { close: false });
        return;
      }
      if (!isIsoDate(v)) {
        showToast("Дата должна быть в формате YYYY-MM-DD");
        text.value = field.default || "";
        return;
      }
      applyDate(v, { close: true });
    });
    text.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        text.dispatchEvent(new Event("change"));
      }
      if (e.key === "Escape") closePop();
    });

    calBtn.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      if (!pop.hidden && window.__spodActiveDateWrap === wrap) {
        closePop();
      } else {
        openPop();
      }
    });

    text.addEventListener("focus", () => {
      // фокус в текстовом поле — календарь не обязателен; закрываем попап
      if (!pop.hidden) closePop();
    });

    fieldWrap.appendChild(text);
    fieldWrap.appendChild(calBtn);
    // попап в body — не обрезается карточкой и корректно ловит клик снаружи
    document.body.appendChild(pop);
    row.appendChild(fieldWrap);
    row.appendChild(presets);
    wrap.appendChild(row);
    host.appendChild(wrap);
    syncPresetActive();

  }

  function syncDefaultUi(card, field, sectionId) {
    const host = card.querySelector("[data-role='default-host']");
    const hint = card.querySelector("[data-role='default-hint']");
    if (!host) return;

    const kind = field.kind || "text";
    host.innerHTML = "";

    if (kind === "dropdown" || kind === "dropdown_custom") {
      // Для dropdown_custom дефолт — как у списка (чипы); поле «свой вариант» только в fill
      renderDefaultChips(host, field, sectionId, card, { multi: false });
    } else if (kind === "list") {
      renderDefaultChips(host, field, sectionId, card, { multi: true });
    } else if (kind === "date") {
      renderDefaultDateUi(host, field, sectionId, card);
    } else {
      const input = document.createElement("input");
      input.type = "text";
      input.dataset.role = "default";
      input.value = field.default || "";
      input.setAttribute(
        "data-tip",
        kind === "number"
          ? "Число без кавычек для предзаполнения BLANK"
          : "Свободный текст для предзаполнения ячейки в BLANK"
      );
      input.placeholder =
        kind === "number"
          ? "Например: 0 или 1.5"
          : "Свободный текст — попадёт в BLANK";
      input.addEventListener("input", () => {
        field.default = input.value;
        afterFieldEdit(card, field, sectionId);
      });
      host.appendChild(input);
    }

    if (hint) hint.textContent = defaultHint(kind);
  }

  function defaultChipMarkHtml() {
    return (
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.6" stroke-linecap="round" stroke-linejoin="round"><path d="M5 12.5l4.2 4.2L19 7"/></svg>'
    );
  }

  /**
   * Чипы выбора дефолта из вариантов.
   * multi=false — одно значение (Список); multi=true — несколько через ; (Массив).
   * Если allow_empty=false — нельзя снять всё: всегда остаётся выбор.
   */
  function selectionRequired(field) {
    const kind = field.kind || "text";
    return (
      !field.allow_empty &&
      (kind === "dropdown" ||
        kind === "dropdown_custom" ||
        kind === "list")
    );
  }

  function ensureDefaultSelection(field, variants, multi) {
    if (!selectionRequired(field) || !variants.length) return;
    if (multi) {
      const cur = parseDefaultList(field.default).filter((v) =>
        variants.includes(v)
      );
      if (!cur.length) {
        field.default = variants[0];
      }
    } else {
      const cur = String(field.default || "").trim();
      if (!cur || !variants.includes(cur)) {
        field.default = variants[0];
      }
    }
  }

  /**
   * Привести поля к тому же виду, что после первой отрисовки UI,
   * чтобы baseline не помечал нормализацию как «отредактировано».
   */
  function normalizeCatalogFields(data) {
    if (!data || !Array.isArray(data.sections)) return;
    for (const sec of data.sections) {
      for (const f of sec.fields || []) {
        if (!kindHasVariants(f.kind)) {
          if (!Array.isArray(f.variants)) f.variants = [];
          if (!(f.variants || []).length) delete f.variant_labels;
          continue;
        }
        alignVariantLabels(f, { fillKnownEmpty: true });
        const variants = Array.isArray(f.variants) ? f.variants.filter(Boolean) : [];
        f.variants = variants;
        if (!variants.length) {
          f.default = "";
          continue;
        }
        const multi = f.kind === "list";
        ensureDefaultSelection(f, variants, multi);
        if (multi) {
          const selected = parseDefaultList(f.default).filter((v) =>
            variants.includes(v)
          );
          f.default = formatDefaultList(selected);
        } else {
          const cur = String(f.default || "").trim();
          f.default = cur && variants.includes(cur) ? cur : "";
          if (selectionRequired(f) && !f.default) f.default = variants[0] || "";
        }
      }
    }
  }

  function renderDefaultChips(host, field, sectionId, card, { multi }) {
    const variants = Array.isArray(field.variants)
      ? field.variants.filter(Boolean)
      : [];
    if (!variants.length) {
      host.innerHTML = multi
        ? `<div class="default-empty" data-tip="Заполните блок «Варианты» — здесь появятся чипы выбора">Сначала введите варианты — появятся чипы для выбора.</div>`
        : `<div class="default-empty" data-tip="Заполните блок «Варианты» — здесь появятся чипы выбора">Сначала введите варианты — появятся чипы для выбора одного значения.</div>`;
      if (field.default) field.default = "";
      return;
    }

    ensureDefaultSelection(field, variants, multi);

    let selected;
    if (multi) {
      selected = parseDefaultList(field.default).filter((v) =>
        variants.includes(v)
      );
      if (formatDefaultList(selected) !== (field.default || "")) {
        field.default = formatDefaultList(selected);
      }
    } else {
      const cur = String(field.default || "").trim();
      selected = cur && variants.includes(cur) ? [cur] : [];
      if ((field.default || "") !== (selected[0] || "")) {
        field.default = selected[0] || "";
      }
    }

    const required = selectionRequired(field);
    const box = document.createElement("div");
    box.className =
      "default-checks default-checks--chips" +
      (multi ? "" : " default-checks--single");
    box.dataset.role = "default";
    box.setAttribute(
      "data-tip",
      required
        ? multi
          ? "«Можно пусто» = нет: должен остаться хотя бы один чип"
          : "«Можно пусто» = нет: всегда выбран один вариант (снять всё нельзя)"
        : multi
          ? "«Можно пусто» = да: можно снять все чипы — тогда дефолт пустой"
          : "«Можно пусто» = да: повторный клик снимает выбор — дефолт будет пустым"
    );
    box.setAttribute("role", multi ? "group" : "radiogroup");

    for (const v of variants) {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "default-chip";
      btn.setAttribute(
        "data-tip",
        (() => {
          const lab = labelForVariant(field, v);
          const shown = lab ? `${lab} (${v})` : v;
          return multi
            ? `Включить в дефолт: ${shown}`
            : `Выбрать дефолт: ${shown}`;
        })()
      );
      const on = selected.includes(v);
      btn.setAttribute("aria-pressed", on ? "true" : "false");
      if (!multi) btn.setAttribute("role", "radio");
      if (!multi) btn.setAttribute("aria-checked", on ? "true" : "false");
      btn.classList.toggle("is-on", on);
      btn.value = v;
      const mark = document.createElement("span");
      mark.className = "default-chip__mark";
      mark.setAttribute("aria-hidden", "true");
      mark.innerHTML = defaultChipMarkHtml();
      btn.appendChild(mark);
      btn.appendChild(chipFaceNodes(v, labelForVariant(field, v)));
      btn.addEventListener("click", () => {
        if (multi) {
          const wasOn = btn.classList.contains("is-on");
          if (wasOn) {
            const onCount = box.querySelectorAll(".default-chip.is-on").length;
            if (required && onCount <= 1) {
              showToast("«Можно пусто» = нет — оставьте хотя бы один вариант");
              return;
            }
            btn.classList.remove("is-on");
            btn.setAttribute("aria-pressed", "false");
          } else {
            btn.classList.add("is-on");
            btn.setAttribute("aria-pressed", "true");
          }
          const vals = Array.from(
            box.querySelectorAll(".default-chip.is-on")
          ).map((el) => el.value);
          field.default = formatDefaultList(vals);
        } else {
          const wasOn = btn.classList.contains("is-on");
          if (wasOn && required) {
            showToast("«Можно пусто» = нет — значение должно быть выбрано");
            return;
          }
          box.querySelectorAll(".default-chip").forEach((el) => {
            el.classList.remove("is-on");
            el.setAttribute("aria-pressed", "false");
            el.setAttribute("aria-checked", "false");
          });
          if (!wasOn) {
            btn.classList.add("is-on");
            btn.setAttribute("aria-pressed", "true");
            btn.setAttribute("aria-checked", "true");
            field.default = v;
          } else {
            field.default = "";
          }
        }
        afterFieldEdit(card, field, sectionId);
      });
      box.appendChild(btn);
    }
    host.appendChild(box);
  }

  function kindToggleHtml(activeKind) {
    return (
      `<div class="kind-toggle" data-role="kind" role="radiogroup" aria-label="Тип ввода" data-tip="Тип ячейки в Excel-форме BADGE">` +
      KINDS.map((k) => {
        const active = (activeKind || "text") === k;
        return (
          `<button type="button" class="kind-chip kind-chip--${k}${
            active ? " active" : ""
          }" data-kind="${k}" role="radio" aria-checked="${active}" data-tip="${escapeHtml(
            KIND_TIPS[k] || KIND_LABELS[k] || k
          )}">` +
          `<span class="kind-chip__icon">${KIND_ICONS[k] || ""}</span>` +
          `<span class="kind-chip__label">${escapeHtml(KIND_SHORT[k] || k)}</span>` +
          `</button>`
        );
      }).join("") +
      `</div>`
    );
  }

  function applyKindChange(card, field, sectionId, nextKind) {
    field.kind = nextKind;
    if (!kindHasVariants(field.kind)) {
      field.variants = [];
      delete field.variant_labels;
      const ta = card.querySelector("[data-role='variants']");
      const taLab = card.querySelector("[data-role='variant-labels']");
      if (ta) ta.value = "";
      if (taLab) taLab.value = "";
    }
    card.querySelectorAll(".kind-chip").forEach((b) => {
      const on = b.getAttribute("data-kind") === field.kind;
      b.classList.toggle("active", on);
      b.setAttribute("aria-checked", on ? "true" : "false");
    });
    syncVariantsUi(card, field);
    syncDefaultUi(card, field, sectionId);
    afterFieldEdit(card, field, sectionId);
  }

  function fieldCard(field, sectionId) {
    const card = document.createElement("article");
    card.className = "card";
    card.dataset.status = field.status;
    card.dataset.key = field.key;
    const dirty = isEdited(sectionId, field);
    if (dirty) card.classList.add("is-edited");

    const statusBtns = STATUSES.map((st) => {
      const active = field.status === st;
      const cls =
        st === "[v]" ? "ready" : st === "[w]" ? "wip" : "open";
      return (
        `<button type="button" class="status-chip status-chip--${cls}${
          active ? " active" : ""
        }" data-st="${st}" role="radio" aria-checked="${active}" data-tip="${escapeHtml(
          STATUS_TIPS[st] || st
        )}">` +
        `<span class="status-chip__icon">${STATUS_ICONS[st] || ""}</span>` +
        `<span class="status-chip__label">${escapeHtml(
          STATUS_LABELS[st] || st
        )}</span>` +
        `</button>`
      );
    }).join("");

    const variantsText = (field.variants || []).join("\n");
    alignVariantLabels(field, { fillKnownEmpty: true });
    const labelsText = (field.variant_labels || []).join("\n");
    const variantsActive = kindHasVariants(field.kind);
    const jsonFact = (field.json_target || "").trim();

    card.innerHTML = `
      <div class="card-head">
        <div class="card-key-wrap">
          <span class="card-n" data-tip="Порядковый номер поля в разделе">#${field.n}</span>
          <span class="card-key" data-tip="Технический ключ параметра в форме / SPOD">${escapeHtml(field.key)}</span>
          ${
            jsonFact
              ? `<span class="json-fact" data-tip="Полный путь ключа в JSON SPOD (колонка.лист; не редактируется): ${escapeHtml(
                  jsonFact
                )}">${escapeHtml(jsonFact)}</span>`
              : ""
          }
          <span class="edited-badge" data-role="edited-badge" data-tip="Поле изменено после загрузки или импорта. Можно вернуть исходное кнопкой слева от готовности."${dirty ? "" : " hidden"}>отредактировано</span>
        </div>
        <div class="card-head-actions">
          <button type="button" class="revert-chip${
            dirty ? "" : " is-disabled"
          }" data-role="revert-field"${dirty ? "" : " disabled"} data-tip="${
            dirty
              ? "Вернуть только это поле к состоянию на момент загрузки / импорта"
              : "Нет правок — возвращать нечего. Появится активной после изменения поля."
          }">
            <span class="revert-chip__icon" aria-hidden="true">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 12a9 9 0 1 0 3-6.7"/><path d="M3 4v5h5"/></svg>
            </span>
            <span class="revert-chip__label">Вернуть</span>
          </button>
          <div class="status-toggle" data-role="status" role="radiogroup" aria-label="Готовность" data-tip="Готовность описания поля">
            ${statusBtns}
          </div>
          <label class="allow-empty-chip glass-switch${field.allow_empty ? " is-on" : ""}" data-role="allow_empty_wrap" data-tip="Да — можно оставить пустым (в edit/fill снять все чипы). Нет — для списка/dropdown всегда должен быть выбран вариант.">
            <input type="checkbox" data-role="allow_empty" role="switch" ${field.allow_empty ? "checked" : ""} />
            <span class="glass-switch__track" aria-hidden="true"><span class="glass-switch__thumb"></span></span>
            <span class="allow-empty-text">Можно пусто</span>
            <span class="glass-switch__label">${field.allow_empty ? "да" : "нет"}</span>
          </label>
        </div>
      </div>
      <div class="field full">
        <label data-tip="Подпись колонки / строки в Excel-форме">Подпись</label>
        <input type="text" data-role="label" value="${escapeHtml(field.label)}" data-tip="Человекочитаемое имя поля в форме" />
      </div>
      <div class="field full field-kind">
        <label data-tip="Как пользователь вводит значение в Excel">Тип ввода</label>
        ${kindToggleHtml(field.kind)}
      </div>
      <div class="field full field-default">
        <label data-tip="Что попадёт в пустой шаблон BLANK по умолчанию">Значение по умолчанию</label>
        <div class="default-host" data-role="default-host"></div>
        <div class="field-hint" data-role="default-hint"></div>
      </div>
      <div class="field full">
        <label data-tip="Пояснение для заполняющего форму">Описание</label>
        <textarea data-role="description" data-tip="Текст подсказки / описания поля">${escapeHtml(field.description || "")}</textarea>
      </div>
      <div class="grid-2">
        <div class="field${variantsActive ? "" : " is-disabled"}" data-role="variants-wrap">
          <label data-tip="Исходные значения для CSV/SPOD: каждое с новой строки">Варианты (значения)</label>
          <textarea class="variants" data-role="variants" data-tip="Коды SPOD, по одному в строке" ${variantsActive ? "" : "disabled"}>${escapeHtml(variantsText)}</textarea>
        </div>
        <div class="field${variantsActive ? "" : " is-disabled"}" data-role="variant-labels-wrap">
          <label data-tip="Понятные подписи в том же порядке, что и значения слева. Необязательно.">Подписи (текст)</label>
          <textarea class="variants variants--labels" data-role="variant-labels" data-tip="Строка N — подпись к значению N. Пустая строка = на кнопке только код." ${variantsActive ? "" : "disabled"}>${escapeHtml(labelsText)}</textarea>
        </div>
      </div>
      <div class="field-hint" data-role="variants-hint" ${variantsActive ? "" : "hidden"}></div>
      <div class="field full">
        <label data-tip="Внутренний комментарий к полю (в Excel не попадает)">Заметка</label>
        <textarea data-role="note" class="note-tall" data-tip="Заметка для себя / для применения каталога">${escapeHtml(field.note || "")}</textarea>
      </div>
    `;

    syncVariantsUi(card, field);
    syncDefaultUi(card, field, sectionId);

    card.querySelectorAll("[data-role='status'] .status-chip").forEach((btn) => {
      btn.addEventListener("click", () => {
        field.status = btn.getAttribute("data-st");
        card.dataset.status = field.status;
        card.querySelectorAll(".status-chip").forEach((b) => {
          const on = b.getAttribute("data-st") === field.status;
          b.classList.toggle("active", on);
          b.setAttribute("aria-checked", on ? "true" : "false");
        });
        afterFieldEdit(card, field, sectionId);
        renderNav();
      });
    });

    const revertBtn = card.querySelector("[data-role='revert-field']");
    if (revertBtn) {
      revertBtn.addEventListener("click", (e) => {
        e.preventDefault();
        e.stopPropagation();
        if (revertBtn.disabled || !isEdited(sectionId, field)) {
          showToast("Нет правок у этого поля");
          return;
        }
        if (!restoreFieldFromBaseline(sectionId, field)) {
          alert("Нет исходного снимка для этого поля.");
          return;
        }
        persist();
        showToast(`«${field.key}» возвращён к исходному`);
        renderNav();
        renderFields();
      });
    }

    card.querySelectorAll(".kind-chip").forEach((btn) => {
      btn.addEventListener("click", () => {
        const next = btn.getAttribute("data-kind");
        if (!next || next === field.kind) return;
        applyKindChange(card, field, sectionId, next);
      });
    });

    const bind = (role, handler) => {
      const el = card.querySelector(`[data-role='${role}']`);
      if (!el) return;
      el.addEventListener("input", handler);
      el.addEventListener("change", handler);
    };

    bind("label", (e) => {
      field.label = e.target.value;
      afterFieldEdit(card, field, sectionId);
    });
    bind("allow_empty", (e) => {
      field.allow_empty = !!e.target.checked;
      const wrap = card.querySelector("[data-role='allow_empty_wrap']");
      const lab = wrap && wrap.querySelector(".glass-switch__label");
      if (wrap) wrap.classList.toggle("is-on", field.allow_empty);
      if (lab) lab.textContent = field.allow_empty ? "да" : "нет";
      // Нет «можно пусто» + список/dropdown → сразу зафиксировать выбор
      if (!field.allow_empty && kindHasVariants(field.kind)) {
        syncDefaultUi(card, field, sectionId);
      }
      afterFieldEdit(card, field, sectionId);
    });
    bind("description", (e) => {
      field.description = e.target.value;
      afterFieldEdit(card, field, sectionId);
    });
    bind("variants", (e) => {
      if (e.target.disabled) return;
      const prevLabels = Array.isArray(field.variant_labels)
        ? field.variant_labels.slice()
        : [];
      field.variants = e.target.value
        .split(/\r?\n/)
        .map((x) => x.trim())
        .filter(Boolean);
      field.variant_labels = prevLabels;
      alignVariantLabels(field, { fillKnownEmpty: true });
      const taLab = card.querySelector("[data-role='variant-labels']");
      if (taLab) taLab.value = (field.variant_labels || []).join("\n");
      syncVariantsUi(card, field, { rewriteLabels: false });
      syncDefaultUi(card, field, sectionId);
      afterFieldEdit(card, field, sectionId);
    });
    bind("variant-labels", (e) => {
      if (e.target.disabled) return;
      const lines = e.target.value.split(/\r?\n/);
      const n = (field.variants || []).length;
      const next = [];
      for (let i = 0; i < n; i += 1) {
        next.push(i < lines.length ? String(lines[i] || "").trim() : "");
      }
      const extra = lines.length > n;
      field.variant_labels = next;
      const hint = card.querySelector("[data-role='variants-hint']");
      if (hint) {
        const nFilled = next.filter(Boolean).length;
        let base =
          "Подписи необязательны. В CSV/SPOD уходит значение слева; на кнопках — текст, если заполнен.";
        base += ` Сейчас ${n} знач. / ${nFilled} подп.`;
        if (extra) base += " Лишние строки подписей игнорируются.";
        if (lines.length < n) {
          base += " Недостающих подписей нет — на кнопке останется код.";
        }
        hint.hidden = false;
        hint.textContent = base;
      }
      syncDefaultUi(card, field, sectionId);
      afterFieldEdit(card, field, sectionId);
    });
    bind("note", (e) => {
      field.note = e.target.value;
      afterFieldEdit(card, field, sectionId);
    });

    return card;
  }

  function renderFields() {
    closeAllDatePops();
    const wrap = $("fields");
    wrap.innerHTML = "";
    const q = searchQuery();

    if (q) {
      const groups = collectSearchHits();
      const total = groups.reduce((n, g) => n + g.fields.length, 0);
      $("section-title").textContent = `Поиск по всем разделам`;
      $("section-intro").textContent =
        total === 0
          ? `Ничего не найдено по «${q}»`
          : `«${q}» — ${total} поле(й) в ${groups.length} раздел(ах). Ищем по ключу, подписи, описанию, вариантам, дефолту, заметке.`;

      if (!total) {
        const empty = document.createElement("p");
        empty.className = "empty-state";
        empty.textContent = "Нет полей по текущему фильтру.";
        wrap.appendChild(empty);
        return;
      }

      for (const { sec, fields } of groups) {
        const head = document.createElement("div");
        head.className = "search-group-head";
        const kind = sec.kind || "table";
        const kindLabel = kind === "json" ? "JSON" : "TABLE";
        head.innerHTML =
          `<button type="button" class="search-group-jump" data-sec="${escapeHtml(
            sec.id
          )}" data-tip="Открыть раздел «${escapeHtml(
            sec.menu_label || sec.title
          )}» и сбросить поиск">` +
          `<span class="nav-kind nav-kind--${kind === "json" ? "json" : "table"}">${kindLabel}</span>` +
          `<span class="search-group-title">${escapeHtml(sec.menu_label || sec.title)}</span>` +
          `<span class="search-group-count">${fields.length}</span>` +
          `</button>`;
        head.querySelector("button").addEventListener("click", () => {
          activeSectionId = sec.id;
          $("search").value = "";
          renderAll();
          window.scrollTo({ top: 0, behavior: "smooth" });
        });
        wrap.appendChild(head);
        for (const field of fields) {
          wrap.appendChild(fieldCard(field, sec.id));
        }
      }
      return;
    }

    const sec = activeSection();
    const kind = sec.kind || "table";
    const kindLabel = kind === "json" ? "JSON" : "TABLE";
    const sheetBit = sec.sheet ? ` · лист ${sec.sheet}` : "";
    $("section-title").textContent = `${kindLabel}: ${sec.menu_label || sec.title}`;
    $("section-intro").textContent =
      (sec.intro || "") +
      (sec.parent ? ` (внутри ${sec.parent})` : "") +
      sheetBit;

    let shown = 0;
    for (const field of sec.fields) {
      if (!matchesFilter(field, sec.id)) continue;
      wrap.appendChild(fieldCard(field, sec.id));
      shown += 1;
    }
    if (!shown) {
      const empty = document.createElement("p");
      empty.className = "empty-state";
      empty.textContent = "Нет полей по текущему фильтру.";
      wrap.appendChild(empty);
    }
  }

  function renderAll() {
    if (!hasCatalog()) {
      syncWorkspaceMode();
      return;
    }
    if (!activeSectionId && catalog.sections.length) {
      activeSectionId = catalog.sections[0].id;
    }
    renderNav();
    renderFields();
    syncRevertAllBtn();
  }

  function download(filename, text, mime) {
    const blob = new Blob([text], { type: mime || "text/plain;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  }

  function stamp() {
    const d = new Date();
    const p = (n) => String(n).padStart(2, "0");
    return `${d.getFullYear()}${p(d.getMonth() + 1)}${p(d.getDate())}_${p(d.getHours())}${p(d.getMinutes())}`;
  }

  function exportJson() {
    if (!catalog) return;
    // Перед выгрузкой выровнять подписи; пустые массивы не пишем
    for (const sec of catalog.sections || []) {
      for (const f of sec.fields || []) {
        if (!kindHasVariants(f.kind)) {
          f.variants = [];
          delete f.variant_labels;
          continue;
        }
        alignVariantLabels(f, { fillKnownEmpty: true });
        if (!(f.variant_labels || []).some(Boolean)) delete f.variant_labels;
      }
    }
    const payload = {
      ...catalog,
      exported_at: new Date().toISOString(),
    };
    catalog.exported_at = payload.exported_at;
    const name = `catalog_${stamp()}.json`;
    download(name, JSON.stringify(payload, null, 2) + "\n", "application/json");
    persist();
    showToast(`Скачан ${name}`);
  }

  function csvEscape(v) {
    const s = String(v ?? "");
    if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
  }

  function exportCsv() {
    if (!catalog) return;
    persist();
    const cols = [
      "section",
      "n",
      "key",
      "status",
      "label",
      "kind",
      "variants",
      "variant_labels",
      "default",
      "allow_empty",
      "json_target",
      "description",
      "note",
    ];
    const lines = [cols.join(",")];
    for (const sec of catalog.sections) {
      for (const f of sec.fields) {
        lines.push(
          [
            sec.id,
            f.n,
            f.key,
            f.status,
            f.label,
            f.kind,
            (f.variants || []).join(" | "),
            (f.variant_labels || []).join(" | "),
            f.default || "",
            f.allow_empty ? "да" : "нет",
            f.json_target || "",
            f.description || "",
            f.note || "",
          ]
            .map(csvEscape)
            .join(",")
          );
      }
    }
    download(`param_review_${stamp()}.csv`, lines.join("\n") + "\n", "text/csv");
  }

  function mdCell(s) {
    return String(s ?? "")
      .replace(/\|/g, " · ")
      .replace(/\r?\n/g, "<br>");
  }

  function exportMd() {
    if (!catalog) return;
    persist();
    const lines = [];
    lines.push("# Каталог параметров формы BADGE");
    lines.push("");
    lines.push("Выгрузка из HTML-редактора `common/web-edit/`.");
    lines.push(`Экспорт: ${new Date().toISOString()}`);
    lines.push("");
    lines.push("После правок передайте JSON/CSV/MD в чат: **«примени каталог»**.");
    lines.push("");
    for (const sec of catalog.sections) {
      lines.push(`## ${sec.title}`);
      lines.push("");
      if (sec.intro) {
        lines.push(sec.intro);
        lines.push("");
      }
      lines.push(
        "| # | Ст | Ключ | Подпись | Описание | Тип | Варианты | Дефолт | Пусто | JSON | Заметка |"
      );
      lines.push(
        "|---:|:--:|:-----|:--------|:---------|:----|:---------|:-------|:-----:|:-----|:--------|"
      );
      for (const f of sec.fields) {
        const variants =
          f.variants && f.variants.length
            ? f.variants
                .map((v, i) => {
                  const lab = Array.isArray(f.variant_labels)
                    ? String(f.variant_labels[i] || "").trim()
                    : "";
                  return lab ? `${mdCell(lab)} (${mdCell(v)})` : mdCell(v);
                })
                .join("<br>")
            : "—";
        lines.push(
          `| ${f.n} | \`${f.status}\` | \`${f.key}\` | ${mdCell(f.label)} | ${mdCell(
            f.description
          )} | ${mdCell(f.kind)} | ${variants} | ${mdCell(f.default || "—")} | ${
            f.allow_empty ? "да" : "нет"
          } | ${mdCell(f.json_target || "—")} | ${mdCell(f.note || "")} |`
        );
      }
      lines.push("");
    }
    download(`param_review_${stamp()}.md`, lines.join("\n"), "text/markdown");
  }

  function importJson(file) {
    if (!file) return;
    const name = String(file.name || "");
    if (name && !/\.json$/i.test(name)) {
      const ok = confirm(
        `Файл «${name}» без расширения .json. Всё равно открыть?`
      );
      if (!ok) return;
    }
    const reader = new FileReader();
    reader.onerror = () => {
      alert("Не удалось прочитать файл");
    };
    reader.onload = () => {
      try {
        let raw = String(reader.result || "");
        if (raw.charCodeAt(0) === 0xfeff) raw = raw.slice(1);
        const data = JSON.parse(raw);
        applyCatalog(data, { resetBaseline: true, persistDraft: true });
        showToast("Открыт catalog.json");
      } catch (err) {
        const msg = err && err.message ? err.message : String(err);
        alert("Не удалось открыть JSON: " + msg);
      }
    };
    reader.readAsText(file, "utf-8");
  }

  function bindFileImport(inputEl) {
    if (!inputEl) return;
    inputEl.addEventListener("change", (e) => {
      const f = e.target.files && e.target.files[0];
      if (f) importJson(f);
      e.target.value = "";
    });
  }

  function syncEditFilterUi() {
    const wrap = $("edit-filter");
    if (!wrap) return;
    const tipOn = {
      edited:
        "Отредактированные — сейчас в фильтре (поля видны). Нажмите, чтобы скрыть. Экспорт метки не снимает.",
      clean:
        "Не отредактированные — сейчас в фильтре (поля видны). Нажмите, чтобы скрыть поля без правок.",
    };
    const tipOff = {
      edited:
        "Отредактированные — сейчас скрыто. Нажмите, чтобы снова показать изменённые поля.",
      clean:
        "Не отредактированные — сейчас скрыто. Нажмите, чтобы снова показать поля без правок.",
    };
    wrap.querySelectorAll("[data-edit-filter]").forEach((btn) => {
      const key = btn.getAttribute("data-edit-filter");
      const on = editFilter.has(key);
      btn.classList.toggle("active", on);
      btn.setAttribute("aria-pressed", on ? "true" : "false");
      btn.setAttribute("data-tip", (on ? tipOn : tipOff)[key] || "");
    });
  }

  function resetEditFilterUi() {
    editFilter = new Set(["edited", "clean"]);
    syncEditFilterUi();
  }

  function initGlassTips() {
    const tip = $("glassTip");
    if (!tip) return;
    const OFFSET = 14;

    function show(text, x, y) {
      const t = String(text || "").trim();
      if (!t) {
        hide();
        return;
      }
      tip.textContent = t;
      tip.classList.add("show");
      tip.setAttribute("aria-hidden", "false");
      const rect = tip.getBoundingClientRect();
      let left = x + OFFSET;
      let top = y + OFFSET;
      if (left + rect.width > window.innerWidth - 8) {
        left = window.innerWidth - rect.width - 8;
      }
      if (top + rect.height > window.innerHeight - 8) {
        top = y - rect.height - 8;
      }
      if (left < 8) left = 8;
      if (top < 8) top = 8;
      tip.style.left = left + "px";
      tip.style.top = top + "px";
    }

    function hide() {
      tip.classList.remove("show");
      tip.setAttribute("aria-hidden", "true");
    }

    document.addEventListener(
      "mousemove",
      (e) => {
        const node = e.target.closest("[data-tip]");
        if (node) {
          show(node.getAttribute("data-tip") || "", e.clientX, e.clientY);
          return;
        }
        hide();
      },
      true
    );

    window.addEventListener("scroll", hide, true);
  }

  function syncStatusFilterUi() {
    const wrap = $("status-filter");
    if (!wrap) return;
    const tipOn = {
      "[ ]":
        "Не готово — сейчас в фильтре (поля видны). Нажмите, чтобы скрыть. Можно выбрать несколько статусов.",
      "[w]":
        "В работе — сейчас в фильтре (поля видны). Нажмите, чтобы скрыть. Можно выбрать несколько статусов.",
      "[v]":
        "Готово — сейчас в фильтре (поля видны). Нажмите, чтобы скрыть. Можно выбрать несколько статусов.",
    };
    const tipOff = {
      "[ ]":
        "Не готово — сейчас скрыто. Нажмите, чтобы снова показать поля с этим статусом.",
      "[w]":
        "В работе — сейчас скрыто. Нажмите, чтобы снова показать поля с этим статусом.",
      "[v]":
        "Готово — сейчас скрыто. Нажмите, чтобы снова показать поля с этим статусом.",
    };
    wrap.querySelectorAll("[data-status-filter]").forEach((btn) => {
      const st = btn.getAttribute("data-status-filter");
      const on = statusFilter.has(st);
      btn.classList.toggle("active", on);
      btn.setAttribute("aria-pressed", on ? "true" : "false");
      btn.setAttribute("data-tip", (on ? tipOn : tipOff)[st] || "");
    });
  }

  function wire() {
    bindFileImport($("import-json"));

    initGlassTips();

    // закрыть календарь при клике / тапе вне блока даты и вне попапа
    const onOutsideDatePicker = (e) => {
      const open = document.querySelector(".date-pop.is-open");
      if (!open) return;
      if (isDatePickerUiTarget(e.target)) return;
      closeActiveDatePop();
    };
    document.addEventListener("pointerdown", onOutsideDatePicker, true);
    document.addEventListener("mousedown", onOutsideDatePicker, true);
    document.addEventListener("touchstart", onOutsideDatePicker, true);

    document.addEventListener(
      "focusin",
      (e) => {
        const open = document.querySelector(".date-pop.is-open");
        if (!open) return;
        if (isDatePickerUiTarget(e.target)) return;
        closeActiveDatePop();
      },
      true
    );

    document.addEventListener("keydown", (e) => {
      if (e.key !== "Escape") return;
      if (!document.querySelector(".date-pop.is-open")) return;
      closeActiveDatePop();
    });

    window.addEventListener(
      "scroll",
      () => {
        if (!document.querySelector(".date-pop.is-open")) return;
        closeActiveDatePop();
      },
      true
    );

    $("search").addEventListener("input", () => {
      if (!catalog) return;
      renderNav();
      renderFields();
    });

    const statusFilterEl = $("status-filter");
    if (statusFilterEl) {
      statusFilterEl.addEventListener("click", (e) => {
        const btn = e.target.closest("[data-status-filter]");
        if (!btn) return;
        const st = btn.getAttribute("data-status-filter");
        if (!st) return;
        if (statusFilter.has(st)) {
          // не даём снять последний выбранный — иначе пустой список
          if (statusFilter.size <= 1) {
            showToast("Оставьте хотя бы один статус");
            return;
          }
          statusFilter.delete(st);
        } else {
          statusFilter.add(st);
        }
        syncStatusFilterUi();
        if (!catalog) return;
        renderNav();
        renderFields();
      });
      syncStatusFilterUi();
    }

    const editFilterEl = $("edit-filter");
    if (editFilterEl) {
      editFilterEl.addEventListener("click", (e) => {
        const btn = e.target.closest("[data-edit-filter]");
        if (!btn) return;
        const key = btn.getAttribute("data-edit-filter");
        if (!key) return;
        if (editFilter.has(key)) {
          if (editFilter.size <= 1) {
            showToast("Оставьте хотя бы один фильтр правок");
            return;
          }
          editFilter.delete(key);
        } else {
          editFilter.add(key);
        }
        syncEditFilterUi();
        if (!catalog) return;
        renderNav();
        renderFields();
      });
      syncEditFilterUi();
    }

    const revertAllBtn = $("btn-revert-all");
    if (revertAllBtn) {
      syncRevertAllBtn();
      revertAllBtn.addEventListener("click", () => {
        if (!catalog || revertAllBtn.disabled) return;
        const n = countEdited();
        if (!n) {
          showToast("Нет отредактированных полей");
          return;
        }
        if (
          !confirm(
            `Вернуть все отредактированные поля (${n}) к исходному состоянию после загрузки / импорта?`
          )
        ) {
          return;
        }
        const restored = restoreAllFromBaseline();
        persist();
        showToast(
          restored
            ? `Возвращено полей: ${restored}`
            : "Нечего возвращать"
        );
        renderAll();
      });
    }

    $("btn-export-json").addEventListener("click", exportJson);
    $("btn-export-csv").addEventListener("click", exportCsv);
    $("btn-export-md").addEventListener("click", exportMd);

    $("btn-reload").addEventListener("click", () => {
      if (
        !confirm(
          "Сбросить черновик в браузере и заново прочитать catalog.json?"
        )
      ) {
        return;
      }
      localStorage.removeItem(LS_KEY);
      localStorage.removeItem(LS_BASELINE_KEY);
      localStorage.removeItem(LS_SOURCE_KEY);
      resetEditFilterUi();
      bootFromFile({ forceFile: true });
    });
  }

  wire();
  bootFromFile();
})();
