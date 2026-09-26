/**
 * web-SPOD-Edit — оболочка: переключение программ (web-report / web-fill) во фреймах,
 * защита от «Назад», подсказки панели. Программы сами по себе не меняются.
 */
(function () {
  "use strict";

  var TABS = ["report", "fill"];
  var DEFAULT_TAB = "report"; // по требованию — при открытии всегда web-report

  function $(id) {
    return document.getElementById(id);
  }

  function trace(cat, msg, data) {
    if (window.SpodTrace) window.SpodTrace.log(cat, msg, data);
  }

  var toastTimer = null;
  function toast(text) {
    var el = $("shell-toast");
    if (!el) return;
    el.textContent = text;
    el.hidden = false;
    clearTimeout(toastTimer);
    toastTimer = setTimeout(function () {
      el.hidden = true;
    }, 2600);
  }

  function frameOf(tab) {
    return $("frame-" + tab);
  }

  /** Показать программу: фрейм уже загружен, поэтому переключение мгновенное и без потери состояния. */
  function activate(tab, reason) {
    if (TABS.indexOf(tab) < 0) tab = DEFAULT_TAB;
    document.body.setAttribute("data-active", tab);
    TABS.forEach(function (t) {
      var on = t === tab;
      var btn = $("tab-" + t);
      var fr = frameOf(t);
      if (btn) {
        btn.classList.toggle("is-active", on);
        btn.setAttribute("aria-selected", on ? "true" : "false");
      }
      if (fr) fr.classList.toggle("is-active", on);
    });
    trace("TAB", "открыта программа «" + tab + "»" + (reason ? " (" + reason + ")" : ""));
    // фокус в программу — чтобы клавиатура сразу работала в ней
    var fr2 = frameOf(tab);
    try {
      if (fr2 && fr2.contentWindow) fr2.contentWindow.focus();
    } catch (err) {
      /* noop */
    }
  }

  function initTabs() {
    TABS.forEach(function (t) {
      var btn = $("tab-" + t);
      if (btn) {
        btn.addEventListener("click", function () {
          activate(t, "вкладка");
        });
      }
    });
    // стрелки ←/→ на вкладках
    var bar = document.querySelector(".shell-tabs");
    if (bar) {
      bar.addEventListener("keydown", function (ev) {
        if (ev.key !== "ArrowLeft" && ev.key !== "ArrowRight") return;
        var cur = document.body.getAttribute("data-active");
        var i = TABS.indexOf(cur);
        var next = TABS[(i + (ev.key === "ArrowRight" ? 1 : TABS.length - 1)) % TABS.length];
        activate(next, "клавиатура");
        var btn = $("tab-" + next);
        if (btn) btn.focus();
        ev.preventDefault();
      });
    }
    TABS.forEach(function (t) {
      var fr = frameOf(t);
      if (fr) {
        fr.addEventListener("load", function () {
          trace("PAGE", "программа «" + t + "» загружена во фрейм");
        });
      }
    });
    activate(DEFAULT_TAB, "старт");
  }

  /**
   * «Назад» по истории (свайп по трекпаду, Cmd+[, кнопка мыши) перезагрузил бы оболочку —
   * и обе программы потеряли бы данные. Страж в истории: шаг назад только снимает его.
   */
  function initNavigationGuard() {
    try {
      history.pushState({ spodShellGuard: true }, "");
    } catch (err) {
      trace("PAGE", "защита от «Назад» недоступна: " + (err && err.message));
      return;
    }
    window.addEventListener("popstate", function () {
      trace("PAGE", "переход «Назад» по истории перехвачен оболочкой");
      try {
        history.pushState({ spodShellGuard: true }, "");
      } catch (err) {
        /* noop */
      }
      toast("«Назад» отключён — иначе данные в программах пропадут");
    });
  }

  /** Подсказки панели: сверху не помещаются — всегда снизу, в пределах окна. */
  function initTips() {
    var tip = $("shell-tip");
    if (!tip) return;
    document.addEventListener("mouseover", function (ev) {
      var el = ev.target.closest ? ev.target.closest("[data-tip]") : null;
      if (!el) {
        tip.hidden = true;
        return;
      }
      tip.textContent = el.getAttribute("data-tip") || "";
      tip.hidden = false;
      var r = el.getBoundingClientRect();
      var vw = document.documentElement.clientWidth;
      var tw = tip.offsetWidth;
      var left = Math.max(8, Math.min(r.left + r.width / 2 - tw / 2, vw - tw - 8));
      tip.style.left = Math.round(left) + "px";
      tip.style.top = Math.round(r.bottom + 8) + "px";
    });
    document.addEventListener("mousedown", function () {
      tip.hidden = true;
    }, true);
  }

  initTabs();
  initNavigationGuard();
  initTips();
})();
