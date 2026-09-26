/* fill_trace_hooks.js — точки трейс-лога для web-fill (web-SPOD-Edit).
   Подключается ПЕРЕД game_fill_boot.js: оборачивает глобальные функции fill-full
   (они объявлены в обычных скриптах, т.е. это свойства window), чтобы в общем журнале
   были действия с длительностью. Код самих функций не меняется. */
"use strict";
(function () {
  var T = window.SpodTrace;
  if (!T) return;
  // действия пользователя — с замером (async-функции тоже: time() ждёт promise)
  ["render", "exportAll", "exportOne", "saveProjectFile", "onImportProjectFile", "addContest",
    "openCopyContestModal", "resetToCatalogDefaults", "showStartGate", "boot"].forEach(function (name) {
    var fn = window[name];
    if (typeof fn !== "function") return;
    window[name] = function () {
      var self = this;
      var args = arguments;
      var info = name === "exportOne" ? { table: args[0] } : name === "onImportProjectFile" && args[0] ? { file: args[0].name, sizeKB: Math.round(args[0].size / 1024) } : undefined;
      return T.time(name, function () {
        return fn.apply(self, args);
      }, info);
    };
  });
  // всплывающие сообщения fill — как строка статуса
  if (typeof window.toast === "function") {
    var origToast = window.toast;
    window.toast = function (msg) {
      T.log("STATUS", String(msg));
      return origToast.apply(this, arguments);
    };
  }
})();
