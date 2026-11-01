// Run synchronously before the app and its first paint. Keeping this in a
// same-origin file lets production CSP forbid inline JavaScript.
(function () {
  var h = location.hash;
  if (h.slice(0, 2) === "#/") {
    var s = h.slice(2).split("/")[0];
    if (["activity", "code", "tasks", "detail", "settings", "search"].indexOf(s) >= 0) {
      location.replace("/hub/" + h.slice(2) + location.search);
    }
  }
  try {
    var t = localStorage.getItem("gp-theme");
    if (t === "light" || t === "dark") document.documentElement.dataset.theme = t;
  } catch (_) {
    // Storage denial falls through to the CSS prefers-color-scheme rule.
  }
})();
