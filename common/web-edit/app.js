/* Редактор каталога параметров BADGE-формы */
(function () {
  "use strict";

  const LS_KEY = "spod_param_review_catalog_v3";
  const LS_BASELINE_KEY = "spod_param_review_baseline_v3";
  const LS_SOURCE_KEY = "spod_param_review_source_v3";
  /** Сначала рядом со страницей; иначе ../param_catalog_review/ */
  const CATALOG_DIR_LOCAL = "./";
  const CATALOG_DIR_SIBLING = "../param_catalog_review/";
  const CATALOG_URL = CATALOG_DIR_LOCAL + "catalog.json";
  const KINDS = ["dropdown", "text", "number", "list", "json", "date"];
  const KIND_LABELS = {
    dropdown: "Выбор из списка",
    text: "Свободный текст",
    number: "Число",
    list: "Массив значений",
    json: "JSON формат {[ ]}",
    date: "Дата (формат YYYY-MM-DD)",
  };
  const KIND_SHORT = {
    dropdown: "Список",
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
    text: "Свободный текст — произвольная строка без кавычек-ограничений списка",
    number: "Число — значение без кавычек (в JSON/SPOD как number)",
    list: "Массив значений — несколько элементов через «;» в Excel; варианты по строкам",
    json: "JSON формат {[ ]} — структура как в SPOD",
    date: "Дата в формате YYYY-MM-DD (например 4000-01-01)",
  };

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
    const gate = $("gate");
    const root = $("app-root");
    if (gate) gate.hidden = true;
    if (root) root.hidden = false;
  }

  function showGate(message, { showRetry = true } = {}) {
    const gate = $("gate");
    const root = $("app-root");
    const text = $("gate-text");
    const retry = $("gate-retry");
    if (root) root.hidden = true;
    if (gate) gate.hidden = false;
    if (text) text.innerHTML = message;
    if (retry) retry.hidden = !showRetry;
  }

  function applyCatalog(data, { resetBaseline = true, persistDraft = true } = {}) {
    catalog = cloneCatalog(validateCatalog(data));
    sourceStamp = catalogStamp(catalog);
    activeSectionId = catalog.sections[0]?.id || null;
    if (resetBaseline) captureBaseline();
    else if (!loadBaseline()) captureBaseline();
    if (persistDraft) persist();
    showApp();
    renderAll();
  }

  function fieldSnapshot(field) {
    return {
      status: field.status || "[ ]",
      label: field.label || "",
      kind: field.kind || "text",
      variants: Array.isArray(field.variants) ? field.variants.slice() : [],
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
    field.default = snap.default || "";
    field.allow_empty = !!snap.allow_empty;
    field.description = snap.description || "";
    field.note = snap.note || "";
    if (snap.json_target != null) field.json_target = snap.json_target;
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
    const pageUrl = window.location.href;
    const candidates = [
      new URL("./catalog.json", pageUrl).href,
      new URL(CATALOG_DIR_SIBLING + "catalog.json", pageUrl).href,
    ];
    const errors = [];
    for (const url of candidates) {
      try {
        const res = await fetch(url, { cache: "no-store" });
        if (!res.ok) {
          errors.push(`${url} → HTTP ${res.status}`);
          continue;
        }
        return validateCatalog(await res.json());
      } catch (err) {
        errors.push(`${url} → ${err && err.message ? err.message : err}`);
      }
    }
    throw new Error(errors.join("; ") || "fetch catalog.json не удался");
  }

  function loadCatalogJsFallback() {
    if (window.PARAM_REVIEW_CATALOG && window.PARAM_REVIEW_CATALOG.sections) {
      return Promise.resolve(validateCatalog(window.PARAM_REVIEW_CATALOG));
    }
    return new Promise((resolve, reject) => {
      const trySrc = [
        new URL("./catalog.js", window.location.href).href,
        new URL(CATALOG_DIR_SIBLING + "catalog.js", window.location.href).href,
      ];
      let i = 0;
      const tryNext = () => {
        if (i >= trySrc.length) {
          reject(
            new Error(
              "Не удалось загрузить catalog.js (web-edit/ и param_catalog_review/)"
            )
          );
          return;
        }
        const s = document.createElement("script");
        s.src = trySrc[i++] + "?t=" + Date.now();
        s.onload = () => {
          try {
            if (!window.PARAM_REVIEW_CATALOG) {
              tryNext();
              return;
            }
            resolve(validateCatalog(window.PARAM_REVIEW_CATALOG));
          } catch (err) {
            reject(err);
          }
        };
        s.onerror = () => tryNext();
        document.head.appendChild(s);
      };
      tryNext();
    });
  }

  async function loadSourceCatalog() {
    try {
      return await fetchCatalogJson();
    } catch (fetchErr) {
      try {
        return await loadCatalogJsFallback();
      } catch (jsErr) {
        const isFile = window.location.protocol === "file:";
        const hint = isFile
          ? "Открытие через file:// часто блокирует чтение JSON. Нужен Live Server / HTTP, либо импорт файла вручную. Рядом с index.html должны быть catalog.json и catalog.js."
          : "Проверьте catalog.json в common/web-edit/ (или common/param_catalog_review/).";
        throw new Error(
          `${hint}<br/><br/><span class="gate-error">${escapeHtml(
            String(fetchErr.message || fetchErr)
          )}</span>`
        );
      }
    }
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

  async function bootFromFile({ forceFile = false } = {}) {
    showGate("Загружаем <code>catalog.json</code>…", {
      showRetry: false,
    });
    try {
      const fileData = await loadSourceCatalog();
      const fileStamp = catalogStamp(fileData);
      if (!forceFile) {
        const draft = tryRestoreDraft(fileStamp);
        if (draft) {
          sourceStamp = fileStamp || catalogStamp(draft);
          catalog = draft;
          if (!loadBaseline()) captureBaseline();
          showApp();
          renderAll();
          showToast("Черновик из браузера");
          return;
        }
      }
      applyCatalog(fileData, { resetBaseline: true, persistDraft: true });
      showToast("Загружен каталог");
    } catch (err) {
      showGate(
        `Не удалось автоматически загрузить каталог.<br/><br/>` +
          (err && err.message ? err.message : escapeHtml(String(err))) +
          `<br/><br/>Импортируйте JSON вручную (файл рядом со страницей или ранее сохранённую выгрузку).`,
        { showRetry: true }
      );
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
    const blob = [
      field.key,
      field.label,
      field.description,
      field.note,
      field.default,
      field.json_target,
      field.kind,
      variants,
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

  function afterFieldEdit(card, field, sectionId) {
    const dirty = isEdited(sectionId, field);
    applyEditedUi(card, dirty);
    if (dirty && !editFilter.has("edited")) card.hidden = true;
    else if (!dirty && !editFilter.has("clean")) card.hidden = true;
    else card.hidden = false;
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
    if (kind === "dropdown") {
      return "Каждое значение с новой строки:\nПРОМ\nТЕСТ";
    }
    if (kind === "list") {
      return "Каждый элемент массива с новой строки:\nKMMMB\nKMKKSB\nCSM";
    }
    return "";
  }

  function syncVariantsUi(card, field) {
    const ta = card.querySelector("[data-role='variants']");
    const hint = card.querySelector("[data-role='variants-hint']");
    const wrap = card.querySelector("[data-role='variants-wrap']");
    if (!ta) return;
    const active = field.kind === "dropdown" || field.kind === "list";
    ta.disabled = !active;
    ta.placeholder = active ? variantsPlaceholder(field.kind) : "";
    if (wrap) wrap.classList.toggle("is-disabled", !active);
    if (hint) {
      hint.hidden = !active;
      hint.textContent =
        field.kind === "list"
          ? "Массив значений: каждый элемент с новой строки (в Excel потом через ;)."
          : "Выбор из списка: каждое значение с новой строки.";
    }
    if (!active) {
      // неактивно — без подсказок и без редактирования списка
      ta.value = (field.variants || []).join("\n");
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
    if (kind === "dropdown") {
      return "Выберите одно значение из вариантов (попадёт в BLANK).";
    }
    if (kind === "list") {
      return "Отметьте галочками нужные значения из вариантов (в BLANK через ;).";
    }
    if (kind === "number") {
      return "Число без кавычек. Пусто = не предзаполнять.";
    }
    return "Предзаполнение ячейки в шаблоне Excel. Пусто = не предзаполнять.";
  }

  function syncDefaultUi(card, field, sectionId) {
    const host = card.querySelector("[data-role='default-host']");
    const hint = card.querySelector("[data-role='default-hint']");
    if (!host) return;

    const variants = Array.isArray(field.variants) ? field.variants.filter(Boolean) : [];
    const kind = field.kind || "text";
    host.innerHTML = "";

    if (kind === "dropdown") {
      if (!variants.length) {
        host.innerHTML =
          `<div class="default-empty" data-tip="Заполните блок «Варианты» — здесь появится выбор одного значения">Сначала введите варианты — появится выпадающий список.</div>`;
        if (field.default) {
          field.default = "";
        }
      } else {
        if (field.default && !variants.includes(field.default)) {
          field.default = "";
        }
        const sel = document.createElement("select");
        sel.dataset.role = "default";
        sel.setAttribute(
          "data-tip",
          "Выберите одно значение из вариантов для предзаполнения BLANK"
        );
        const emptyOpt = document.createElement("option");
        emptyOpt.value = "";
        emptyOpt.textContent = "— не предзаполнять —";
        sel.appendChild(emptyOpt);
        for (const v of variants) {
          const opt = document.createElement("option");
          opt.value = v;
          opt.textContent = v;
          if (field.default === v) opt.selected = true;
          sel.appendChild(opt);
        }
        sel.addEventListener("change", () => {
          field.default = sel.value;
          afterFieldEdit(card, field, sectionId);
        });
        host.appendChild(sel);
      }
    } else if (kind === "list") {
      if (!variants.length) {
        host.innerHTML =
          `<div class="default-empty" data-tip="Заполните блок «Варианты» — здесь появятся галочки">Сначала введите варианты — появятся галочки для выбора.</div>`;
        if (field.default) {
          field.default = "";
        }
      } else {
        let selected = parseDefaultList(field.default).filter((v) =>
          variants.includes(v)
        );
        if (formatDefaultList(selected) !== (field.default || "")) {
          field.default = formatDefaultList(selected);
        }
        const box = document.createElement("div");
        box.className = "default-checks";
        box.dataset.role = "default";
        box.setAttribute(
          "data-tip",
          "Отметьте одно или несколько значений — в BLANK уйдут через ;"
        );
        for (const v of variants) {
          const id =
            "def_" +
            String(field.key || "f").replace(/\W+/g, "_") +
            "_" +
            String(v).replace(/\W+/g, "_").slice(0, 40);
          const lab = document.createElement("label");
          lab.className = "default-check";
          lab.setAttribute("data-tip", `Включить в дефолт: ${v}`);
          const cb = document.createElement("input");
          cb.type = "checkbox";
          cb.value = v;
          cb.id = id;
          cb.checked = selected.includes(v);
          cb.addEventListener("change", () => {
            const vals = Array.from(
              box.querySelectorAll("input[type='checkbox']:checked")
            ).map((el) => el.value);
            field.default = formatDefaultList(vals);
            afterFieldEdit(card, field, sectionId);
          });
          const span = document.createElement("span");
          span.textContent = v;
          lab.appendChild(cb);
          lab.appendChild(span);
          box.appendChild(lab);
        }
        host.appendChild(box);
      }
    } else {
      const input = document.createElement("input");
      input.type = "text";
      input.dataset.role = "default";
      input.value = field.default || "";
      input.setAttribute(
        "data-tip",
        kind === "number"
          ? "Число без кавычек для предзаполнения BLANK"
          : kind === "date"
            ? "Дата YYYY-MM-DD для предзаполнения BLANK"
            : "Свободный текст для предзаполнения ячейки в BLANK"
      );
      input.placeholder =
        kind === "number"
          ? "Например: 0 или 1.5"
          : kind === "date"
            ? "Например: 4000-01-01"
            : "Свободный текст — попадёт в BLANK";
      input.addEventListener("input", () => {
        field.default = input.value;
        afterFieldEdit(card, field, sectionId);
      });
      host.appendChild(input);
    }

    if (hint) hint.textContent = defaultHint(kind);
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
    if (field.kind !== "dropdown" && field.kind !== "list") {
      field.variants = [];
      const ta = card.querySelector("[data-role='variants']");
      if (ta) ta.value = "";
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
    const variantsActive = field.kind === "dropdown" || field.kind === "list";
    const jsonFact = (field.json_target || "").trim();

    card.innerHTML = `
      <div class="card-head">
        <div class="card-key-wrap">
          <span class="card-n" data-tip="Порядковый номер поля в разделе">#${field.n}</span>
          <span class="card-key" data-tip="Технический ключ параметра в форме / SPOD">${escapeHtml(field.key)}</span>
          ${
            jsonFact
              ? `<span class="json-fact" data-tip="Куда уходит параметр в SPOD (факт раздела, не редактируется): ${escapeHtml(
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
          <label class="allow-empty-chip glass-switch${field.allow_empty ? " is-on" : ""}" data-role="allow_empty_wrap" data-tip="Разрешить пустое значение в Excel. Да — можно не заполнять; нет — ожидается значение.">
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
          <label data-tip="Список допустимых значений: каждое с новой строки (для Список и Массив)">Варианты</label>
          <textarea class="variants" data-role="variants" data-tip="По одному значению в строке" ${variantsActive ? "" : "disabled"}>${escapeHtml(variantsText)}</textarea>
          <div class="field-hint" data-role="variants-hint" ${variantsActive ? "" : "hidden"}></div>
        </div>
        <div class="field">
          <label data-tip="Внутренний комментарий к полю (в Excel не попадает)">Заметка</label>
          <textarea data-role="note" class="note-tall" data-tip="Заметка для себя / для применения каталога">${escapeHtml(field.note || "")}</textarea>
        </div>
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
      afterFieldEdit(card, field, sectionId);
    });
    bind("description", (e) => {
      field.description = e.target.value;
      afterFieldEdit(card, field, sectionId);
    });
    bind("variants", (e) => {
      if (e.target.disabled) return;
      field.variants = e.target.value
        .split(/\r?\n/)
        .map((x) => x.trim())
        .filter(Boolean);
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
            ? f.variants.map(mdCell).join("<br>")
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
    const reader = new FileReader();
    reader.onload = () => {
      try {
        const data = JSON.parse(String(reader.result || ""));
        applyCatalog(data, { resetBaseline: true, persistDraft: true });
        showToast("Импортирован JSON");
      } catch (err) {
        alert("Не удалось импортировать JSON: " + err.message);
      }
    };
    reader.readAsText(file, "utf-8");
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
    initGlassTips();
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

    const onImportFile = (e) => {
      const f = e.target.files && e.target.files[0];
      if (f) importJson(f);
      e.target.value = "";
    };
    $("import-json").addEventListener("change", onImportFile);
    $("gate-import-json").addEventListener("change", onImportFile);

    $("gate-retry").addEventListener("click", () => {
      bootFromFile({ forceFile: true });
    });

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
