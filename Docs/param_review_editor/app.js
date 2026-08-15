/* Редактор каталога параметров BADGE-формы */
(function () {
  "use strict";

  const LS_KEY = "spod_param_review_catalog_v3";
  const LS_BASELINE_KEY = "spod_param_review_baseline_v3";
  const LS_SOURCE_KEY = "spod_param_review_source_v3";
  const CATALOG_URL = "catalog.json";
  const KINDS = ["dropdown", "text", "number", "list", "json", "date"];
  const KIND_LABELS = {
    dropdown: "Выбор из списка",
    text: "Свободный текст",
    number: "Число",
    list: "Массив значений",
    json: "JSON формат {[ ]}",
    date: "Дата (формат YYYY-MM-DD)",
  };
  const STATUSES = ["[ ]", "[w]", "[v]"];

  /** @type {any} */
  let catalog = null;
  /** @type {Record<string, string>} снимок после загрузки / импорта / сохранения JSON */
  let baseline = {};
  let activeSectionId = null;
  let saveTimer = null;
  /** @type {"all" | "edited" | "clean"} */
  let editFilter = "all";
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

  function fieldFingerprint(field) {
    return JSON.stringify({
      status: field.status || "[ ]",
      label: field.label || "",
      kind: field.kind || "text",
      variants: Array.isArray(field.variants) ? field.variants : [],
      default: field.default || "",
      allow_empty: !!field.allow_empty,
      description: field.description || "",
      note: field.note || "",
      json_target: field.json_target || "",
    });
  }

  function fieldBaselineKey(sectionId, fieldKey) {
    return `${sectionId}::${fieldKey}`;
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
    for (const sec of catalog.sections) {
      for (const f of sec.fields) {
        if (isEdited(sec.id, f)) n += 1;
      }
    }
    return n;
  }

  async function fetchCatalogJson() {
    const pageUrl = window.location.href;
    const candidates = [
      new URL("./catalog.json", pageUrl).href,
      new URL("catalog.json", pageUrl).href,
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
      const s = document.createElement("script");
      s.src = new URL("./catalog.js", window.location.href).href + "?t=" + Date.now();
      s.onload = () => {
        try {
          if (!window.PARAM_REVIEW_CATALOG) {
            reject(new Error("catalog.js загружен, но PARAM_REVIEW_CATALOG пуст"));
            return;
          }
          resolve(validateCatalog(window.PARAM_REVIEW_CATALOG));
        } catch (err) {
          reject(err);
        }
      };
      s.onerror = () =>
        reject(new Error("Не удалось загрузить catalog.js рядом со страницей"));
      document.head.appendChild(s);
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
          ? "Открытие через file:// часто блокирует чтение JSON. Нужен Live Server / HTTP, либо импорт файла вручную. Также должен лежать catalog.js (сборка)."
          : "Проверьте, что catalog.json лежит рядом с index.html.";
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
    showGate("Загружаем <code>catalog.json</code> рядом со страницей…", {
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
      btn.title = sec.sheet
        ? `${sec.intro || ""}\nЛист: ${sec.sheet}`
        : sec.intro || "";
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
      `<strong style="color:var(--glass-green)">[v]</strong> ${c.v}` +
      ` · <strong style="color:var(--glass-orange)">[w]</strong> ${c.w}` +
      ` · <strong style="color:var(--glass-gray)">[ ]</strong> ${c.open}` +
      ` · всего ${c.total}` +
      (edited
        ? ` · <span class="stats-edited">отред. ${edited}</span>`
        : "");
  }

  function activeSection() {
    return catalog.sections.find((s) => s.id === activeSectionId) || catalog.sections[0];
  }

  function matchesFilter(field, sectionId) {
    const onlyOpen = $("only-open").checked;
    if (onlyOpen && field.status === "[v]") return false;
    const dirty = isEdited(sectionId, field);
    if (editFilter === "edited" && !dirty) return false;
    if (editFilter === "clean" && dirty) return false;
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
  }

  function afterFieldEdit(card, field, sectionId) {
    const dirty = isEdited(sectionId, field);
    applyEditedUi(card, dirty);
    if (editFilter === "edited" && !dirty) card.hidden = true;
    if (editFilter === "clean" && dirty) card.hidden = true;
    schedulePersist();
    const edited = countEdited();
    const stats = $("sidebar-stats");
    if (stats) {
      const c = counts();
      stats.innerHTML =
        `<strong style="color:var(--glass-green)">[v]</strong> ${c.v}` +
        ` · <strong style="color:var(--glass-orange)">[w]</strong> ${c.w}` +
        ` · <strong style="color:var(--glass-gray)">[ ]</strong> ${c.open}` +
        ` · всего ${c.total}` +
        (edited
          ? ` · <span class="stats-edited">отред. ${edited}</span>`
          : "");
    }
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
          `<div class="default-empty">Сначала введите варианты слева — появится выпадающий список.</div>`;
        if (field.default) {
          field.default = "";
        }
      } else {
        if (field.default && !variants.includes(field.default)) {
          field.default = "";
        }
        const sel = document.createElement("select");
        sel.dataset.role = "default";
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
          `<div class="default-empty">Сначала введите варианты слева — появятся галочки для выбора.</div>`;
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
        for (const v of variants) {
          const id =
            "def_" +
            String(field.key || "f").replace(/\W+/g, "_") +
            "_" +
            String(v).replace(/\W+/g, "_").slice(0, 40);
          const lab = document.createElement("label");
          lab.className = "default-check";
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

  function fieldCard(field, sectionId) {
    const card = document.createElement("article");
    card.className = "card";
    card.dataset.status = field.status;
    card.dataset.key = field.key;
    const dirty = isEdited(sectionId, field);
    if (dirty) card.classList.add("is-edited");

    const statusBtns = STATUSES.map(
      (st) =>
        `<button type="button" class="status-btn${field.status === st ? " active" : ""}" data-st="${st}">${st}</button>`
    ).join("");

    const kindOpts = KINDS.map(
      (k) =>
        `<option value="${k}"${field.kind === k ? " selected" : ""}>${
          KIND_LABELS[k] || k
        }</option>`
    ).join("");

    const variantsText = (field.variants || []).join("\n");
    const variantsActive = field.kind === "dropdown" || field.kind === "list";

    card.innerHTML = `
      <div class="card-head">
        <div class="card-key-wrap">
          <span class="card-n">#${field.n}</span>
          <span class="card-key">${escapeHtml(field.key)}</span>
          <span class="edited-badge" data-role="edited-badge"${dirty ? "" : " hidden"}>отредактировано</span>
        </div>
        <div class="card-head-actions">
          <div class="status-group" data-role="status" title="Готовность">${statusBtns}</div>
          <label class="allow-empty-chip glass-switch${field.allow_empty ? " is-on" : ""}" data-role="allow_empty_wrap" title="Можно ли оставлять поле пустым">
            <input type="checkbox" data-role="allow_empty" role="switch" ${field.allow_empty ? "checked" : ""} />
            <span class="glass-switch__track" aria-hidden="true"><span class="glass-switch__thumb"></span></span>
            <span class="allow-empty-text">Можно пусто</span>
            <span class="glass-switch__label">${field.allow_empty ? "да" : "нет"}</span>
          </label>
        </div>
      </div>
      <div class="grid grid-top">
        <div class="field">
          <label>Подпись</label>
          <input type="text" data-role="label" value="${escapeHtml(field.label)}" />
        </div>
        <div class="field">
          <label>Тип ввода</label>
          <select data-role="kind">${kindOpts}</select>
        </div>
      </div>
      <div class="field full field-default">
        <label>Значение по умолчанию</label>
        <div class="default-host" data-role="default-host"></div>
        <div class="field-hint" data-role="default-hint"></div>
      </div>
      <div class="field full">
        <label>Описание</label>
        <textarea data-role="description">${escapeHtml(field.description || "")}</textarea>
      </div>
      <div class="grid-2">
        <div class="field${variantsActive ? "" : " is-disabled"}" data-role="variants-wrap">
          <label>Варианты</label>
          <textarea class="variants" data-role="variants" ${variantsActive ? "" : "disabled"}>${escapeHtml(variantsText)}</textarea>
          <div class="field-hint" data-role="variants-hint" ${variantsActive ? "" : "hidden"}></div>
        </div>
        <div class="field">
          <label>JSON-цель</label>
          <input type="text" data-role="json_target" value="${escapeHtml(field.json_target || "")}" placeholder="CONTEST_FEATURE / REWARD_ADD_DATA / …" />
          <label style="margin-top:10px">Заметка</label>
          <textarea data-role="note" style="min-height:64px">${escapeHtml(field.note || "")}</textarea>
        </div>
      </div>
    `;

    syncVariantsUi(card, field);
    syncDefaultUi(card, field, sectionId);

    card.querySelectorAll("[data-role='status'] .status-btn").forEach((btn) => {
      btn.addEventListener("click", () => {
        field.status = btn.getAttribute("data-st");
        card.dataset.status = field.status;
        card.querySelectorAll(".status-btn").forEach((b) => {
          b.classList.toggle("active", b.getAttribute("data-st") === field.status);
        });
        afterFieldEdit(card, field, sectionId);
        renderNav();
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
    bind("kind", (e) => {
      field.kind = e.target.value;
      if (field.kind !== "dropdown" && field.kind !== "list") {
        field.variants = [];
        const ta = card.querySelector("[data-role='variants']");
        if (ta) ta.value = "";
      }
      syncVariantsUi(card, field);
      syncDefaultUi(card, field, sectionId);
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
    bind("json_target", (e) => {
      field.json_target = e.target.value.trim();
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
          `<button type="button" class="search-group-jump" data-sec="${escapeHtml(sec.id)}">` +
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
    lines.push("Выгрузка из HTML-редактора `Docs/param_review_editor/`.");
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

  function resetEditFilterUi() {
    editFilter = "all";
    const ef = $("edit-filter");
    if (ef) {
      ef.querySelectorAll(".edit-filter-btn").forEach((b) => {
        b.classList.toggle("active", b.getAttribute("data-edit-filter") === "all");
      });
    }
  }

  function wire() {
    $("search").addEventListener("input", () => {
      if (!catalog) return;
      renderNav();
      renderFields();
    });
    const onlyOpen = $("only-open");
    const onlyWrap = onlyOpen && onlyOpen.closest(".glass-switch");
    const syncOnly = () => {
      if (onlyWrap) onlyWrap.classList.toggle("is-on", !!onlyOpen.checked);
    };
    onlyOpen.addEventListener("change", () => {
      syncOnly();
      if (!catalog) return;
      renderNav();
      renderFields();
    });
    syncOnly();

    const editFilterEl = $("edit-filter");
    if (editFilterEl) {
      editFilterEl.addEventListener("click", (e) => {
        const btn = e.target.closest("[data-edit-filter]");
        if (!btn || !catalog) return;
        editFilter = btn.getAttribute("data-edit-filter") || "all";
        editFilterEl.querySelectorAll(".edit-filter-btn").forEach((b) => {
          b.classList.toggle("active", b === btn);
        });
        renderNav();
        renderFields();
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
          "Сбросить черновик в браузере и заново прочитать catalog.json рядом со страницей?"
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
