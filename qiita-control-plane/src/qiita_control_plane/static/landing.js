(function () {
  // Flip the status badge to reflect /health's report. The landing page
  // renders fully before this runs, so a /health that hangs or 500s
  // leaves the badge in its initial "checking…" state instead of taking
  // down the page itself.
  var badge = document.getElementById("status-badge");
  if (!badge) return;

  fetch("/health", { headers: { Accept: "application/json" } })
    .then(function (res) {
      if (!res.ok) throw new Error("status " + res.status);
      return res.json();
    })
    .then(function (data) {
      badge.classList.remove("status-unknown");
      if (data && data.status === "ok") {
        badge.classList.add("status-ok");
        badge.textContent = "healthy";
      } else {
        badge.classList.add("status-bad");
        badge.textContent = data && data.status ? data.status : "degraded";
      }
    })
    .catch(function () {
      badge.classList.remove("status-unknown");
      badge.classList.add("status-bad");
      badge.textContent = "unreachable";
    });
})();
