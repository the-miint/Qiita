(function () {
  // Flip each status pill to reflect the corresponding entry in
  // /health.services. The landing page renders fully before this
  // runs, so a /health that hangs or 500s leaves the pills in
  // their initial "checking…" state instead of taking down the
  // page itself.
  //
  // The three pill IDs are pinned by qiita_control_plane.health's
  // _KEY_CP / _KEY_CO / _KEY_DP constants. Adding a fourth service
  // would extend both sides (the dict key on the CP and the pill
  // here); renaming a key is a breaking contract that needs a
  // template change alongside.
  var SERVICES = ["cp", "co", "dp"];

  function findBadge(key) {
    return document.getElementById("status-badge-" + key);
  }

  function setBadge(badge, klass, text) {
    badge.classList.remove("status-unknown", "status-ok", "status-bad");
    badge.classList.add(klass);
    badge.textContent = text;
  }

  // Map a per-service status string (drawn from the CP-side
  // {ok, degraded, unreachable, unconfigured} alphabet) to a pill
  // class + text. Unknown values fall through to the "bad" class
  // so a future server-side status the JS doesn't recognize
  // surfaces as visible failure rather than silent success — same
  // posture as the compute-readiness probe-log parser.
  function pillFor(status) {
    if (status === "ok") return ["status-ok", "ok"];
    if (status === "unconfigured") return ["status-unknown", "not configured"];
    if (status === "degraded") return ["status-bad", "degraded"];
    if (status === "unreachable") return ["status-bad", "unreachable"];
    return ["status-bad", status || "unknown"];
  }

  function applyServices(services) {
    SERVICES.forEach(function (key) {
      var badge = findBadge(key);
      if (!badge) return;
      var pair = pillFor(services[key]);
      setBadge(badge, pair[0], pair[1]);
    });
  }

  function markAllUnreachable() {
    SERVICES.forEach(function (key) {
      var badge = findBadge(key);
      if (badge) setBadge(badge, "status-bad", "unreachable");
    });
  }

  fetch("/health", { headers: { Accept: "application/json" } })
    .then(function (res) {
      if (!res.ok) throw new Error("status " + res.status);
      return res.json();
    })
    .then(function (data) {
      if (data && data.services && typeof data.services === "object") {
        applyServices(data.services);
        return;
      }
      // Pre-aggregate /health (legacy shape, just `status`). Show
      // every pill in the aggregate state so the page doesn't sit
      // in "checking…" forever; an operator updating a deploy can
      // tell from this that the CP is alive but the new shape
      // hasn't rolled out yet.
      var legacy = pillFor(data && data.status === "ok" ? "ok" : "degraded");
      SERVICES.forEach(function (key) {
        var badge = findBadge(key);
        if (badge) setBadge(badge, legacy[0], legacy[1]);
      });
    })
    .catch(function () {
      markAllUnreachable();
    });
})();
