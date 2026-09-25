/**
 * Трейс-лог страницы для разбора внезапных перезагрузок/сбоев.
 * Подключается ПЕРВЫМ скриптом. Когда включён (переключатель «Лог» в шапке),
 * пишет подробный журнал: клики, изменения полей, выбор файлов, действия
 * приложения с длительностью, ошибки, события жизни страницы.
 * Каждая запись СРАЗУ дописывается в localStorage — при внезапной перезагрузке
 * вкладки журнал не теряется. При старте страницы лог всегда выключен; если прошлая
 * сессия с логом оборвалась, в журнал дописывается отметка «НЕ ЗАВЕРШЕНА ШТАТНО».
 * Кнопка «скачать» всегда отдаёт ВЕСЬ журнал (а не часть после прошлого сохранения).
 */
(function (root) {
  "use strict";

  var KEY_ON = "spod_web_report_trace_on";
  var KEY_LOG = "spod_web_report_trace_log";
  var KEY_ALIVE = "spod_web_report_trace_alive";
  var MAX_CHARS = 2500000; // ~2.5 млн символов; старые строки отрезаются с начала

  var enabled = false;
  var storageOk = true;
  var cache = "";
  var seq = 0;
  var sessionId = Math.random().toString(36).slice(2, 7);
  var listeners = [];
  var inputTimers = Object.create(null);

  function safeGet(key) {
    try {
      return root.localStorage.getItem(key);
    } catch (err) {
      storageOk = false;
      return null;
    }
  }

  function safeSet(key, value) {
    try {
      root.localStorage.setItem(key, value);
      return true;
    } catch (err) {
      storageOk = false;
      return false;
    }
  }

  function pad(n, w) {
    var s = String(n);
    while (s.length < (w || 2)) s = "0" + s;
    return s;
  }

  function stamp() {
    var d = new Date();
    return (
      d.getFullYear() +
      "-" +
      pad(d.getMonth() + 1) +
      "-" +
      pad(d.getDate()) +
      " " +
      pad(d.getHours()) +
      ":" +
      pad(d.getMinutes()) +
      ":" +
      pad(d.getSeconds()) +
      "." +
      pad(d.getMilliseconds(), 3)
    );
  }

  function memInfo() {
    var m = root.performance && root.performance.memory;
    if (!m) return "";
    return " mem=" + Math.round(m.usedJSHeapSize / 1048576) + "MB/" + Math.round(m.jsHeapSizeLimit / 1048576) + "MB";
  }

  function safeJson(data) {
    try {
      var text = JSON.stringify(data);
      return text.length > 2000 ? text.slice(0, 2000) + "…" : text;
    } catch (err) {
      return String(data);
    }
  }

  function notify() {
    listeners.forEach(function (fn) {
      try {
        fn();
      } catch (err) {
        /* noop */
      }
    });
  }

  function append(line) {
    cache += line + "\n";
    if (cache.length > MAX_CHARS) {
      var cut = cache.indexOf("\n", cache.length - MAX_CHARS);
      cache = "… (начало журнала обрезано по размеру)\n" + cache.slice(cut + 1);
    }
    safeSet(KEY_LOG, cache);
  }

  /** Записать строку журнала (только когда режим включён). */
  function log(category, message, data) {
    if (!enabled) return;
    seq += 1;
    var line =
      stamp() +
      " [" +
      sessionId +
      " #" +
      seq +
      "] " +
      category +
      ": " +
      message +
      (data !== undefined ? " | " + safeJson(data) : "") +
      memInfo();
    append(line);
    notify();
  }

  /** Короткое описание элемента: тег#id.класс [name] «текст». */
  function describeEl(el) {
    if (!el || !el.tagName) return String(el);
    var parts = el.tagName.toLowerCase();
    if (el.id) parts += "#" + el.id;
    var cls = typeof el.className === "string" ? el.className.trim().split(/\s+/).slice(0, 3).join(".") : "";
    if (cls) parts += "." + cls;
    var act = el.getAttribute && el.getAttribute("data-act");
    if (act) parts += "[data-act=" + act + "]";
    var name = el.getAttribute && el.getAttribute("name");
    if (name) parts += "[name=" + name + "]";
    var text = (el.innerText || el.textContent || "").replace(/\s+/g, " ").trim();
    if (text && el.tagName !== "SELECT") parts += " «" + text.slice(0, 60) + "»";
    return parts;
  }

  function fieldValue(el) {
    if (!el) return undefined;
    if (el.type === "file") {
      return Array.prototype.map.call(el.files || [], function (f) {
        return f.name + " (" + Math.round(f.size / 1024) + " КБ)";
      });
    }
    if (el.type === "checkbox" || el.type === "radio") return el.checked;
    if (el.tagName === "SELECT") {
      var opt = el.options[el.selectedIndex];
      return opt ? opt.value : "";
    }
    return el.value;
  }

  /** Замерить действие приложения: лог начала/конца, длительность, ошибка. */
  function time(name, fn, data) {
    if (!enabled) return fn();
    var t0 = root.performance ? root.performance.now() : Date.now();
    log("ACTION", "→ " + name, data);
    try {
      var result = fn();
      if (result && typeof result.then === "function") {
        return result.then(
          function (v) {
            log("ACTION", "← " + name + " (" + Math.round((root.performance ? root.performance.now() : Date.now()) - t0) + " мс)");
            return v;
          },
          function (err) {
            log("ERROR", name + " упало: " + (err && err.message), err && err.stack ? String(err.stack).slice(0, 800) : undefined);
            throw err;
          }
        );
      }
      log("ACTION", "← " + name + " (" + Math.round((root.performance ? root.performance.now() : Date.now()) - t0) + " мс)");
      return result;
    } catch (err) {
      log("ERROR", name + " упало: " + (err && err.message), err && err.stack ? String(err.stack).slice(0, 800) : undefined);
      throw err;
    }
  }

  function navigationType() {
    try {
      var nav = root.performance.getEntriesByType("navigation")[0];
      return nav ? nav.type : "";
    } catch (err) {
      return "";
    }
  }

  function startSession(reason) {
    var prev = safeGet(KEY_ALIVE) || "";
    var prevState = !prev
      ? "нет данных"
      : prev.indexOf("open:") === 0
        ? "НЕ ЗАВЕРШЕНА ШТАТНО (сессия " + prev.slice(5) + ": вкладку перезагрузил/выгрузил браузер или она упала — pagehide не пришёл)"
        : "завершена штатно (" + prev + ")";
    safeSet(KEY_ALIVE, "open:" + sessionId);
    log("PAGE", "===== " + reason + " =====", {
      navigation: navigationType(),
      prevSession: prevState,
      url: String(root.location && root.location.href).slice(0, 200),
      ua: root.navigator ? root.navigator.userAgent : "",
      storage: storageOk ? "localStorage ок" : "localStorage НЕДОСТУПЕН — сохраняйте журнал кнопкой",
    });
  }

  function setOn(on) {
    var next = !!on;
    if (next === enabled) return;
    if (!next) log("PAGE", "трассировка выключена пользователем");
    enabled = next;
    safeSet(KEY_ON, enabled ? "1" : "0");
    var toggleEl = document.getElementById("trace-toggle");
    if (toggleEl) toggleEl.checked = enabled;
    if (enabled) {
      startSession("трассировка включена");
    } else {
      safeSet(KEY_ALIVE, "closed:" + sessionId);
    }
    notify();
  }

  function getText() {
    var header =
      "SPOD web-report — трейс-лог\n" +
      "Сохранён: " +
      stamp() +
      "\nБраузер: " +
      (root.navigator ? root.navigator.userAgent : "") +
      "\nХранилище: " +
      (storageOk ? "localStorage ок" : "localStorage недоступен (журнал только в памяти этой сессии)") +
      "\nСтрок: " +
      count() +
      "\n\n";
    return header + cache;
  }

  function count() {
    if (!cache) return 0;
    return cache.split("\n").length - 1;
  }

  function download() {
    var d = new Date();
    var name =
      "web_report_trace_" +
      d.getFullYear() +
      pad(d.getMonth() + 1) +
      pad(d.getDate()) +
      "_" +
      pad(d.getHours()) +
      pad(d.getMinutes()) +
      pad(d.getSeconds()) +
      ".txt";
    log("PAGE", "журнал сохранён в файл " + name + " (строк: " + count() + ")");
    var blob = new Blob([getText()], { type: "text/plain;charset=utf-8" });
    var url = URL.createObjectURL(blob);
    var a = document.createElement("a");
    a.href = url;
    a.download = name;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(function () {
      URL.revokeObjectURL(url);
    }, 1500);
  }

  function clear() {
    cache = "";
    seq = 0;
    safeSet(KEY_LOG, "");
    if (enabled) log("PAGE", "журнал очищен");
    notify();
  }

  // ---------- захват событий страницы (пишутся только при включённом режиме) ----------

  function interesting(el) {
    return el && el.closest
      ? el.closest("button, a, label, input, select, textarea, summary, [data-act], [data-status], [data-stage], .contest-tab") || el
      : el;
  }

  document.addEventListener(
    "click",
    function (ev) {
      if (!enabled) return;
      log("CLICK", describeEl(interesting(ev.target)));
    },
    true
  );

  document.addEventListener(
    "change",
    function (ev) {
      if (!enabled) return;
      var el = ev.target;
      log("CHANGE", describeEl(el), { value: fieldValue(el) });
    },
    true
  );

  // ввод печатанием — не каждую букву, а итог через 500 мс тишины
  document.addEventListener(
    "input",
    function (ev) {
      if (!enabled) return;
      var el = ev.target;
      if (!el || el.type === "file") return;
      var key = el.id || describeEl(el);
      clearTimeout(inputTimers[key]);
      inputTimers[key] = setTimeout(function () {
        log("INPUT", describeEl(el), { value: fieldValue(el) });
      }, 500);
    },
    true
  );

  // служебные клавиши и любые сочетания с Cmd/Ctrl/Alt (Cmd+[, Cmd+←, Cmd+R …) — без текста ввода
  var SERVICE_KEYS = /^(Enter|Escape|Tab|Backspace|Delete|F5|ArrowUp|ArrowDown|ArrowLeft|ArrowRight|PageUp|PageDown|Home|End)$/;
  document.addEventListener(
    "keydown",
    function (ev) {
      if (!enabled) return;
      var combo = ev.metaKey || ev.ctrlKey || ev.altKey;
      if (!combo && !SERVICE_KEYS.test(ev.key)) return;
      log(
        "KEY",
        (ev.metaKey ? "Cmd+" : "") + (ev.ctrlKey ? "Ctrl+" : "") + (ev.altKey ? "Alt+" : "") + (ev.shiftKey && combo ? "Shift+" : "") + ev.key +
          " на " + describeEl(ev.target)
      );
    },
    true
  );

  // прокрутка/свайп: горизонтальные жесты (кандидаты на «Назад» в Safari) и колесо над полями-числами
  var lastWheelLog = 0;
  document.addEventListener(
    "wheel",
    function (ev) {
      if (!enabled) return;
      var horizontal = Math.abs(ev.deltaX) > Math.abs(ev.deltaY) && Math.abs(ev.deltaX) > 4;
      var overNumber = ev.target && ev.target.tagName === "INPUT" && ev.target.type === "number";
      if (!horizontal && !overNumber) return;
      var now = Date.now();
      if (now - lastWheelLog < 400) return;
      lastWheelLog = now;
      var scroller = ev.target.closest ? ev.target.closest(".preview-table-wrap, .main, .contest-tabs-scroll") : null;
      log("WHEEL", (horizontal ? "горизонтальный свайп" : "прокрутка над полем-числом") + " на " + describeEl(ev.target), {
        dx: Math.round(ev.deltaX),
        dy: Math.round(ev.deltaY),
        scroller: scroller ? scroller.className.split(" ")[0] : "",
        scrollLeft: scroller ? Math.round(scroller.scrollLeft) : null,
      });
    },
    { capture: true, passive: true }
  );

  document.addEventListener(
    "mousedown",
    function (ev) {
      if (!enabled || ev.button < 3) return;
      log("MOUSE", "кнопка мыши " + ev.button + (ev.button === 3 ? " («Назад»)" : ev.button === 4 ? " («Вперёд»)" : ""));
    },
    true
  );

  root.addEventListener("popstate", function (ev) {
    if (!enabled) return;
    log("PAGE", "popstate (шаг по истории)", { state: ev.state });
  });

  document.addEventListener(
    "toggle",
    function (ev) {
      if (!enabled) return;
      log("TOGGLE", describeEl(ev.target), { open: !!ev.target.open });
    },
    true
  );

  root.addEventListener("error", function (ev) {
    if (!enabled) return;
    log("ERROR", (ev.message || "ошибка ресурса") + " @ " + (ev.filename || describeEl(ev.target)) + ":" + (ev.lineno || "") + ":" + (ev.colno || ""),
      ev.error && ev.error.stack ? String(ev.error.stack).slice(0, 800) : undefined);
  }, true);

  root.addEventListener("unhandledrejection", function (ev) {
    if (!enabled) return;
    var r = ev.reason;
    log("ERROR", "необработанный promise: " + (r && r.message ? r.message : String(r)), r && r.stack ? String(r.stack).slice(0, 800) : undefined);
  });

  root.addEventListener("beforeunload", function () {
    if (!enabled) return;
    log("PAGE", "beforeunload — страница начинает выгружаться");
  });

  root.addEventListener("pagehide", function (ev) {
    if (!enabled) return;
    log("PAGE", "pagehide — страница выгружена (persisted=" + !!ev.persisted + ")");
    safeSet(KEY_ALIVE, "closed:" + sessionId);
  });

  root.addEventListener("pageshow", function (ev) {
    if (!enabled || !ev.persisted) return;
    safeSet(KEY_ALIVE, "open:" + sessionId);
    log("PAGE", "pageshow из кэша (bfcache)");
  });

  document.addEventListener("visibilitychange", function () {
    if (!enabled) return;
    log("PAGE", "видимость: " + document.visibilityState);
  });

  // console.error приложения тоже в журнал
  if (root.console && typeof root.console.error === "function") {
    var origError = root.console.error;
    root.console.error = function () {
      if (enabled) {
        log("CONSOLE", Array.prototype.map.call(arguments, function (a) {
          return a && a.message ? a.message : String(a);
        }).join(" "));
      }
      return origError.apply(root.console, arguments);
    };
  }

  // ---------- кнопки в шапке ----------

  function bindUi() {
    var toggle = document.getElementById("trace-toggle");
    var save = document.getElementById("trace-save");
    var clr = document.getElementById("trace-clear");
    var cnt = document.getElementById("trace-count");
    var box = document.getElementById("trace-ctl");
    // Галочку НЕ трогаем здесь: refresh вызывается на каждую запись журнала, в том числе
    // на запись о самом клике по переключателю — до события change. Раньше это возвращало
    // галочку во «вкл» посреди клика, и выключить лог было невозможно.
    function refresh() {
      if (box) {
        box.classList.toggle("is-on", enabled);
        box.classList.toggle("is-no-storage", !storageOk);
      }
      if (cnt) cnt.textContent = String(count());
      if (save) save.disabled = !cache;
      if (clr) clr.disabled = !cache;
    }
    if (toggle) {
      toggle.addEventListener("change", function () {
        setOn(toggle.checked);
      });
    }
    if (save) save.addEventListener("click", download);
    if (clr) {
      clr.addEventListener("click", function () {
        if (confirm("Очистить весь трейс-лог?")) clear();
      });
    }
    listeners.push(refresh);
    if (toggle) toggle.checked = enabled;
    refresh();
  }

  // ---------- старт ----------

  // При старте лог всегда выключен (включается вручную переключателем). Но если прошлая
  // сессия писала лог и оборвалась без pagehide (сбой/перезагрузка браузером), в журнал
  // дописывается отметка об этом — чтобы скачанный журнал показывал место сбоя.
  cache = safeGet(KEY_LOG) || "";
  var prevAlive = safeGet(KEY_ALIVE) || "";
  if (prevAlive.indexOf("open:") === 0) {
    append(
      stamp() +
        " [" +
        sessionId +
        " #0] PAGE: ===== загрузка страницы: прошлая сессия " +
        prevAlive.slice(5) +
        " НЕ ЗАВЕРШЕНА ШТАТНО (вкладку перезагрузил/выгрузил браузер или она упала) | {\"navigation\":\"" +
        navigationType() +
        "\"} — лог при старте выключен, включите «Лог», чтобы продолжить запись"
    );
    safeSet(KEY_ALIVE, "closed:" + prevAlive.slice(5));
  }
  enabled = false;
  safeSet(KEY_ON, "0");

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bindUi);
  } else {
    bindUi();
  }

  root.ReportTrace = {
    log: log,
    time: time,
    isOn: function () {
      return enabled;
    },
    setOn: setOn,
    getText: getText,
    count: count,
    download: download,
    clear: clear,
    storageOk: function () {
      return storageOk;
    },
  };
})(typeof globalThis !== "undefined" ? globalThis : this);
