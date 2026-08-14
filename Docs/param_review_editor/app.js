/* Редактор каталога параметров BADGE-формы */
(function () {
  "use strict";

  const LS_KEY = "spod_param_review_catalog_v2";
  const KINDS = ["dropdown", "text", "list", "json", "date"];
  const STATUSES = ["[ ]", "[w]", "[v]"];

  /** @type {any} */
  let catalog = null;
  let activeSectionId = null;
  let saveTimer = null;

  const $ = (id) => document.getElementById(id);

  function cloneCatalog(src) {
    return JSON.parse(JSON.stringify(src));
  }

  function loadInitial() {
    const fromFile = window.PARAM_REVIEW_CATALOG;
    if (!fromFile || !fromFile.sections) {
      alert("Не найден catalog.js — запустите: python src/Tools/build_param_review_editor.py");
      return;
    }
    try {
      const raw = localStorage.getItem(LS_KEY);
      if (raw) {
        const saved = JSON.parse(raw);
        if (saved && saved.sections && Array.isArray(saved.sections)) {
          catalog = saved;
          return;
        }
      }
    } catch (_) {
      /* ignore */
    }
    catalog = cloneCatalog(fromFile);
  }

  function persist() {
    localStorage.setItem(LS_KEY, JSON.stringify(catalog));
    const toast = $("save-toast");
    toast.hidden = false;
    clearTimeout(saveTimer);
    saveTimer = setTimeout(() => {
      toast.hidden = true;
    }, 900);
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
        renderAll();
      });
      nav.appendChild(btn);
    }
    const c = counts();
    $("sidebar-stats").innerHTML =
      `<strong style="color:var(--glass-green)">[v]</strong> ${c.v}` +
      ` &nbsp;·&nbsp; <strong style="color:var(--glass-orange)">[w]</strong> ${c.w}` +
      ` &nbsp;·&nbsp; <strong style="color:var(--glass-gray)">[ ]</strong> ${c.open}` +
      `<br/>всего ${c.total}`;
  }

  function activeSection() {
    return catalog.sections.find((s) => s.id === activeSectionId) || catalog.sections[0];
  }

  function matchesFilter(field) {
    const q = ($("search").value || "").trim().toLowerCase();
    const onlyOpen = $("only-open").checked;
    if (onlyOpen && field.status === "[v]") return false;
    if (!q) return true;
    const blob = `${field.key} ${field.label} ${field.description} ${field.note}`.toLowerCase();
    return blob.includes(q);
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
      return "Пример реальных значений (по одному в строке):\nПРОМ\nТЕСТ";
    }
    if (kind === "list") {
      return "Пример элементов списка (по одному в строке;\nв Excel потом через ; ):\nKMMMB\nKMKKSB\nCSM";
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
          ? "Только для list: каждый пункт — элемент списка (в форме через ;)."
          : "Только для dropdown: каждый пункт — значение выпадающего списка.";
    }
    if (!active) {
      // неактивно — без подсказок и без редактирования списка
      ta.value = (field.variants || []).join("\n");
    }
  }

  function fieldCard(field, sectionId) {
    const card = document.createElement("article");
    card.className = "card";
    card.dataset.status = field.status;
    card.dataset.key = field.key;

    const statusBtns = STATUSES.map(
      (st) =>
        `<button type="button" class="status-btn${field.status === st ? " active" : ""}" data-st="${st}">${st}</button>`
    ).join("");

    const kindOpts = KINDS.map(
      (k) => `<option value="${k}"${field.kind === k ? " selected" : ""}>${k}</option>`
    ).join("");

    const variantsText = (field.variants || []).join("\n");
    const variantsActive = field.kind === "dropdown" || field.kind === "list";

    card.innerHTML = `
      <div class="card-head">
        <div class="card-key-wrap">
          <span class="card-n">#${field.n}</span>
          <span class="card-key">${escapeHtml(field.key)}</span>
        </div>
        <div class="status-group" data-role="status">${statusBtns}</div>
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
      <div class="grid grid-default">
        <div class="field">
          <label>Значение по умолчанию</label>
          <input type="text" data-role="default" value="${escapeHtml(field.default || "")}" placeholder="Например: АКТИВНЫЙ или 4000-01-01 — попадёт в BLANK" />
          <div class="field-hint">Предзаполнение ячейки в шаблоне Excel. Пусто = не предзаполнять.</div>
        </div>
        <div class="field switch-field">
          <span class="switch-caption">Можно пусто</span>
          <label class="glass-switch${field.allow_empty ? " is-on" : ""}" data-role="allow_empty_wrap">
            <input type="checkbox" data-role="allow_empty" role="switch" ${field.allow_empty ? "checked" : ""} />
            <span class="glass-switch__track" aria-hidden="true"><span class="glass-switch__thumb"></span></span>
            <span class="glass-switch__label">${field.allow_empty ? "да" : "нет"}</span>
          </label>
        </div>
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

    card.querySelectorAll("[data-role='status'] .status-btn").forEach((btn) => {
      btn.addEventListener("click", () => {
        field.status = btn.getAttribute("data-st");
        card.dataset.status = field.status;
        card.querySelectorAll(".status-btn").forEach((b) => {
          b.classList.toggle("active", b.getAttribute("data-st") === field.status);
        });
        schedulePersist();
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
      schedulePersist();
    });
    bind("kind", (e) => {
      field.kind = e.target.value;
      if (field.kind !== "dropdown" && field.kind !== "list") {
        field.variants = [];
        const ta = card.querySelector("[data-role='variants']");
        if (ta) ta.value = "";
      }
      syncVariantsUi(card, field);
      schedulePersist();
    });
    bind("default", (e) => {
      field.default = e.target.value;
      schedulePersist();
    });
    bind("allow_empty", (e) => {
      field.allow_empty = !!e.target.checked;
      const wrap = card.querySelector("[data-role='allow_empty_wrap']");
      const lab = wrap && wrap.querySelector(".glass-switch__label");
      if (wrap) wrap.classList.toggle("is-on", field.allow_empty);
      if (lab) lab.textContent = field.allow_empty ? "да" : "нет";
      schedulePersist();
    });
    bind("description", (e) => {
      field.description = e.target.value;
      schedulePersist();
    });
    bind("variants", (e) => {
      if (e.target.disabled) return;
      field.variants = e.target.value
        .split(/\r?\n/)
        .map((x) => x.trim())
        .filter(Boolean);
      schedulePersist();
    });
    bind("json_target", (e) => {
      field.json_target = e.target.value.trim();
      schedulePersist();
    });
    bind("note", (e) => {
      field.note = e.target.value;
      schedulePersist();
    });

    return card;
  }

  function renderFields() {
    const sec = activeSection();
    const kind = sec.kind || "table";
    const kindLabel = kind === "json" ? "JSON" : "TABLE";
    const sheetBit = sec.sheet ? ` · лист ${sec.sheet}` : "";
    $("section-title").textContent = `${kindLabel}: ${sec.menu_label || sec.title}`;
    $("section-intro").textContent =
      (sec.intro || "") +
      (sec.parent ? ` (внутри ${sec.parent})` : "") +
      sheetBit;
    const wrap = $("fields");
    wrap.innerHTML = "";
    let shown = 0;
    for (const field of sec.fields) {
      if (!matchesFilter(field)) continue;
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
    persist();
    const payload = {
      ...catalog,
      exported_at: new Date().toISOString(),
    };
    download(
      `param_review_${stamp()}.json`,
      JSON.stringify(payload, null, 2) + "\n",
      "application/json"
    );
  }

  function csvEscape(v) {
    const s = String(v ?? "");
    if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
  }

  function exportCsv() {
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
        if (!data.sections || !Array.isArray(data.sections)) {
          throw new Error("Нет sections[]");
        }
        catalog = data;
        activeSectionId = catalog.sections[0]?.id || null;
        persist();
        renderAll();
      } catch (err) {
        alert("Не удалось импортировать JSON: " + err.message);
      }
    };
    reader.readAsText(file, "utf-8");
  }

  function wire() {
    $("search").addEventListener("input", renderFields);
    const onlyOpen = $("only-open");
    const onlyWrap = onlyOpen && onlyOpen.closest(".glass-switch");
    const syncOnly = () => {
      if (onlyWrap) onlyWrap.classList.toggle("is-on", !!onlyOpen.checked);
    };
    onlyOpen.addEventListener("change", () => {
      syncOnly();
      renderFields();
    });
    syncOnly();
    $("btn-export-json").addEventListener("click", exportJson);
    $("btn-export-csv").addEventListener("click", exportCsv);
    $("btn-export-md").addEventListener("click", exportMd);
    $("import-json").addEventListener("change", (e) => {
      const f = e.target.files && e.target.files[0];
      if (f) importJson(f);
      e.target.value = "";
    });
    $("btn-reload").addEventListener("click", () => {
      if (!confirm("Сбросить правки из браузера и загрузить catalog.js заново?")) return;
      localStorage.removeItem(LS_KEY);
      catalog = cloneCatalog(window.PARAM_REVIEW_CATALOG);
      activeSectionId = catalog.sections[0]?.id || null;
      renderAll();
    });
  }

  loadInitial();
  wire();
  renderAll();
})();
