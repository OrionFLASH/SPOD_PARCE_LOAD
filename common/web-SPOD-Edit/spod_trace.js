/**
 * Общий трейс-лог web-SPOD-Edit (оболочка + web-report + web-fill).
 *
 * Два режима (выбирается сам):
 *  - ХОЗЯИН — страница открыта сама по себе (оболочка web_spod_edit.html или любая из
 *    программ отдельно): хранит ЕДИНЫЙ журнал в localStorage (каждая запись — сразу,
 *    переживает внезапную перезагрузку), держит переключатель «Лог»/скачать/очистить,
 *    при старте отмечает оборванную прошлую сессию.
 *  - ВЛОЖЕННЫЙ — страница открыта во фрейме оболочки: сама ничего не хранит, ловит
 *    события своего документа и шлёт строки хозяину (postMessage), а включён/выключен —
 *    узнаёт от хозяина. Свой переключатель (#trace-ctl, если есть) прячет.
 * Область записи — атрибут data-area у <script src="…spod_trace.js">: shell / report / fill.
 * При открытии страницы лог всегда выключен.
 * API (как у прежнего report_trace.js): window.ReportTrace = window.SpodTrace =
 *   { log, time, isOn, setOn, getText, count, download, clear, storageOk }.
 */
(function (root) {
  "use strict";

  var KEY_ON = "spod_edit_trace_on";
  var KEY_LOG = "spod_edit_trace_log";
  var KEY_ALIVE = "spod_edit_trace_alive";
  var MAX_CHARS = 2500000;
  var MSG = "spod-trace";

  var scriptEl = document.currentScript;
  var AREA = (scriptEl && scriptEl.getAttribute("data-area")) || "page";
  var isChild = false;
  try {
    isChild = root.parent && root.parent !== root;
  } catch (err) {
    isChild = true;
  }

  var enabled = false;
  var storageOk = true;
  var cache = "";
  var seq = 0;
  var sessionId = Math.random().toString(36).slice(2, 7);
  var listeners = [];
  var inputTimers = Object.create(null);
  var frames = []; // у хозяина: окна фреймов, которым рассылаем состояние

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
      d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-" + pad(d.getDate()) + " " +
      pad(d.getHours()) + ":" + pad(d.getMinutes()) + ":" + pad(d.getSeconds()) + "." + pad(d.getMilliseconds(), 3)
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

  // ---------- хозяин: хранение ----------

  function append(line) {
    cache += line + "\n";
    if (cache.length > MAX_CHARS) {
      var cut = cache.indexOf("\n", cache.length - MAX_CHARS);
      cache = "… (начало журнала обрезано по размеру)\n" + cache.slice(cut + 1);
    }
    safeSet(KEY_LOG, cache);
  }

  function formatLine(area, sid, n, category, message, data, mem) {
    return (
      stamp() + " [" + area + " " + sid + " #" + n + "] " + category + ": " + message +
      (data !== undefined ? " | " + safeJson(data) : "") + (mem || "")
    );
  }

  /** Записать строку журнала (только когда режим включён). */
  function log(category, message, data) {
    if (!enabled) return;
    seq += 1;
    var line = formatLine(AREA, sessionId, seq, category, message, data, memInfo());
    if (isChild) {
      try {
        root.parent.postMessage({ type: MSG, kind: "line", line: line }, "*");
      } catch (err) {
        /* noop */
      }
      return;
    }
    append(line);
    notify();
  }

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

  function now() {
    return root.performance ? root.performance.now() : Date.now();
  }

  /** Замерить действие программы: лог начала/конца, длительность, ошибка. */
  function time(name, fn, data) {
    if (!enabled) return fn();
    var t0 = now();
    log("ACTION", "→ " + name, data);
    function fail(err) {
      log("ERROR", name + " упало: " + (err && err.message), err && err.stack ? String(err.stack).slice(0, 800) : undefined);
    }
    try {
      var result = fn();
      if (result && typeof result.then === "function") {
        return result.then(
          function (v) {
            log("ACTION", "← " + name + " (" + Math.round(now() - t0) + " мс)");
            return v;
          },
          function (err) {
            fail(err);
            throw err;
          }
        );
      }
      log("ACTION", "← " + name + " (" + Math.round(now() - t0) + " мс)");
      return result;
    } catch (err) {
      fail(err);
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
    safeSet(KEY_ALIVE, "open:" + sessionId);
    log("PAGE", "===== " + reason + " =====", {
      navigation: navigationType(),
      url: String(root.location && root.location.href).slice(0, 200),
      ua: root.navigator ? root.navigator.userAgent : "",
      storage: storageOk ? "localStorage ок" : "localStorage НЕДОСТУПЕН — сохраняйте журнал кнопкой",
    });
  }

  function broadcast() {
    frames.forEach(function (w) {
      try {
        w.postMessage({ type: MSG, kind: "state", enabled: enabled }, "*");
      } catch (err) {
        /* noop */
      }
    });
  }

  function setOn(on) {
    var next = !!on;
    if (next === enabled) return;
    if (isChild) return; // во фрейме включает только хозяин
    if (!next) log("PAGE", "трассировка выключена пользователем");
    enabled = next;
    safeSet(KEY_ON, enabled ? "1" : "0");
    var toggleEl = document.getElementById("trace-toggle");
    if (toggleEl) toggleEl.checked = enabled;
    if (enabled) startSession("трассировка включена");
    else safeSet(KEY_ALIVE, "closed:" + sessionId);
    broadcast();
    notify();
  }

  function count() {
    if (!cache) return 0;
    return cache.split("\n").length - 1;
  }

  function getText() {
    return (
      "SPOD web-SPOD-Edit — трейс-лог\nСохранён: " + stamp() +
      "\nБраузер: " + (root.navigator ? root.navigator.userAgent : "") +
      "\nХранилище: " + (storageOk ? "localStorage ок" : "localStorage недоступен (журнал только в памяти этой сессии)") +
      "\nСтрок: " + count() +
      "\nОбласти: shell — оболочка и вкладки, report — web-report, fill — web-fill\n\n" + cache
    );
  }

  function download() {
    var d = new Date();
    var name =
      "spod_edit_trace_" + d.getFullYear() + pad(d.getMonth() + 1) + pad(d.getDate()) + "_" +
      pad(d.getHours()) + pad(d.getMinutes()) + pad(d.getSeconds()) + ".txt";
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

  // ---------- захват событий документа ----------

  function interesting(el) {
    return el && el.closest
      ? el.closest("button, a, label, input, select, textarea, summary, [data-act], [data-status], [data-stage], [data-tab], .contest-tab") || el
      : el;
  }

  document.addEventListener("click", function (ev) {
    if (enabled) log("CLICK", describeEl(interesting(ev.target)));
  }, true);

  document.addEventListener("change", function (ev) {
    if (enabled) log("CHANGE", describeEl(ev.target), { value: fieldValue(ev.target) });
  }, true);

  document.addEventListener("input", function (ev) {
    if (!enabled) return;
    var el = ev.target;
    if (!el || el.type === "file") return;
    var key = el.id || describeEl(el);
    clearTimeout(inputTimers[key]);
    inputTimers[key] = setTimeout(function () {
      log("INPUT", describeEl(el), { value: fieldValue(el) });
    }, 500);
  }, true);

  var SERVICE_KEYS = /^(Enter|Escape|Tab|Backspace|Delete|F5|ArrowUp|ArrowDown|ArrowLeft|ArrowRight|PageUp|PageDown|Home|End)$/;
  document.addEventListener("keydown", function (ev) {
    if (!enabled) return;
    var combo = ev.metaKey || ev.ctrlKey || ev.altKey;
    if (!combo && !SERVICE_KEYS.test(ev.key)) return;
    log("KEY",
      (ev.metaKey ? "Cmd+" : "") + (ev.ctrlKey ? "Ctrl+" : "") + (ev.altKey ? "Alt+" : "") +
      (ev.shiftKey && combo ? "Shift+" : "") + ev.key + " на " + describeEl(ev.target));
  }, true);

  var lastWheelLog = 0;
  document.addEventListener("wheel", function (ev) {
    if (!enabled) return;
    var horizontal = Math.abs(ev.deltaX) > Math.abs(ev.deltaY) && Math.abs(ev.deltaX) > 4;
    var overNumber = ev.target && ev.target.tagName === "INPUT" && ev.target.type === "number";
    if (!horizontal && !overNumber) return;
    var t = Date.now();
    if (t - lastWheelLog < 400) return;
    lastWheelLog = t;
    var scroller = ev.target.closest ? ev.target.closest(".preview-table-wrap, .main, .contest-tabs-scroll") : null;
    log("WHEEL", (horizontal ? "горизонтальный свайп" : "прокрутка над полем-числом") + " на " + describeEl(ev.target), {
      dx: Math.round(ev.deltaX),
      dy: Math.round(ev.deltaY),
      scroller: scroller ? scroller.className.split(" ")[0] : "",
      scrollLeft: scroller ? Math.round(scroller.scrollLeft) : null,
    });
  }, { capture: true, passive: true });

  document.addEventListener("mousedown", function (ev) {
    if (enabled && ev.button >= 3) {
      log("MOUSE", "кнопка мыши " + ev.button + (ev.button === 3 ? " («Назад»)" : ev.button === 4 ? " («Вперёд»)" : ""));
    }
  }, true);

  document.addEventListener("toggle", function (ev) {
    if (enabled) log("TOGGLE", describeEl(ev.target), { open: !!ev.target.open });
  }, true);

  root.addEventListener("error", function (ev) {
    if (!enabled) return;
    log("ERROR",
      (ev.message || "ошибка ресурса") + " @ " + (ev.filename || describeEl(ev.target)) + ":" + (ev.lineno || "") + ":" + (ev.colno || ""),
      ev.error && ev.error.stack ? String(ev.error.stack).slice(0, 800) : undefined);
  }, true);

  root.addEventListener("unhandledrejection", function (ev) {
    if (!enabled) return;
    var r = ev.reason;
    log("ERROR", "необработанный promise: " + (r && r.message ? r.message : String(r)), r && r.stack ? String(r.stack).slice(0, 800) : undefined);
  });

  root.addEventListener("popstate", function (ev) {
    if (enabled) log("PAGE", "popstate (шаг по истории)", { state: ev.state });
  });

  root.addEventListener("beforeunload", function () {
    if (enabled) log("PAGE", "beforeunload — страница начинает выгружаться");
  });

  root.addEventListener("pagehide", function (ev) {
    if (!enabled) return;
    log("PAGE", "pagehide — страница выгружена (persisted=" + !!ev.persisted + ")");
    if (!isChild) safeSet(KEY_ALIVE, "closed:" + sessionId);
  });

  document.addEventListener("visibilitychange", function () {
    if (enabled) log("PAGE", "видимость: " + document.visibilityState);
  });

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

  // ---------- связь хозяин ↔ фреймы ----------

  root.addEventListener("message", function (ev) {
    var d = ev.data;
    if (!d || d.type !== MSG) return;
    if (isChild) {
      if (d.kind === "state") {
        var was = enabled;
        enabled = !!d.enabled;
        if (enabled && !was) log("PAGE", "область подключена к журналу", { url: String(root.location.href).split("/").slice(-2).join("/") });
      }
      return;
    }
    // хозяин
    if (d.kind === "hello" && ev.source) {
      if (frames.indexOf(ev.source) < 0) frames.push(ev.source);
      try {
        ev.source.postMessage({ type: MSG, kind: "state", enabled: enabled }, "*");
      } catch (err) {
        /* noop */
      }
    } else if (d.kind === "line" && enabled && typeof d.line === "string") {
      append(d.line);
      notify();
    }
  });

  // ---------- кнопки (только у хозяина) ----------

  function bindUi() {
    var toggle = document.getElementById("trace-toggle");
    var save = document.getElementById("trace-save");
    var clr = document.getElementById("trace-clear");
    var cnt = document.getElementById("trace-count");
    var box = document.getElementById("trace-ctl");
    if (isChild) {
      // во фрейме переключатель — в оболочке (display, т.к. CSS программы перебивает [hidden])
      if (box) {
        box.hidden = true;
        box.style.display = "none";
      }
      return;
    }
    // Галочку здесь НЕ трогаем (refresh вызывается на каждую запись, в т.ч. на клик по самому
    // переключателю — до события change; иначе выключить лог было бы нельзя).
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

  if (isChild) {
    try {
      root.parent.postMessage({ type: MSG, kind: "hello", area: AREA }, "*");
    } catch (err) {
      /* noop */
    }
  } else {
    cache = safeGet(KEY_LOG) || "";
    var prevAlive = safeGet(KEY_ALIVE) || "";
    if (prevAlive.indexOf("open:") === 0) {
      append(
        stamp() + " [" + AREA + " " + sessionId + " #0] PAGE: ===== загрузка страницы: прошлая сессия " + prevAlive.slice(5) +
          " НЕ ЗАВЕРШЕНА ШТАТНО (вкладку перезагрузил/выгрузил браузер или она упала) | {\"navigation\":\"" + navigationType() +
          "\"} — лог при старте выключен, включите «Лог», чтобы продолжить запись"
      );
      safeSet(KEY_ALIVE, "closed:" + prevAlive.slice(5));
    }
    enabled = false;
    safeSet(KEY_ON, "0");
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bindUi);
  } else {
    bindUi();
  }

  var api = {
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
    isChild: function () {
      return isChild;
    },
    area: AREA,
  };
  root.SpodTrace = api;
  root.ReportTrace = api;
})(typeof globalThis !== "undefined" ? globalThis : this);
