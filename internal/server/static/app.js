(function () {
  const panels = { guards: document.getElementById('guards'), cluster: document.getElementById('cluster'), actions: document.getElementById('actions') };
  const updated = document.getElementById('updated');

  document.querySelectorAll('nav button').forEach(function (btn) {
    btn.addEventListener('click', function () {
      document.querySelectorAll('nav button').forEach(function (b) { b.classList.remove('active'); });
      document.querySelectorAll('.panel').forEach(function (p) { p.classList.remove('active'); });
      btn.classList.add('active');
      panels[btn.dataset.tab].classList.add('active');
    });
  });

  function esc(s) {
    return String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;');
  }

  function renderGuards(st) {
    const rows = (st.guards || []).map(function (g) {
      const role = esc(g.role) + (g.leader ? ' <span class="badge leader">leader</span>' : ' <span class="badge">follower</span>');
      return '<tr><td>' + esc(g.node) + '</td><td>' + role + '</td><td>' + esc(g.mode) + '</td><td>' +
        esc(g.loop_renew_age_sec != null ? g.loop_renew_age_sec.toFixed(1) + 's' : '-') + '</td><td>' +
        esc(g.loop_health_age_sec != null ? g.loop_health_age_sec.toFixed(1) + 's' : '-') + '</td><td>' +
        esc(g.loop_watch_age_sec != null ? g.loop_watch_age_sec.toFixed(1) + 's' : '-') + '</td></tr>';
    }).join('');
    panels.guards.innerHTML = '<table><thead><tr><th>Node</th><th>Role</th><th>Mode</th><th>Renew loop</th><th>Health loop</th><th>Watch loop</th></tr></thead><tbody>' +
      (rows || '<tr><td colspan="6">No guards</td></tr>') + '</tbody></table>';
  }

  function renderCluster(st) {
    const maint = st.maintenance ? '<span class="badge warn">maintenance ON</span> ' + esc(st.maintenance_message || '') : '<span class="badge">maintenance off</span>';
    const head = '<div class="grid"><div class="card"><h3>Primary</h3><p>' + esc(st.primary || '-') + ' · epoch ' + esc(st.epoch) + '</p></div>' +
      '<div class="card"><h3>Maintenance</h3><p>' + maint + '</p></div></div>';
    const rows = (st.nodes || []).map(function (n) {
      return '<tr><td>' + esc(n.node) + '</td><td><code>' + esc((n.gtid_executed || '').slice(0, 48)) + '</code></td><td>' +
        esc(n.lag_seconds) + '</td><td>' + (n.repl_state === 'unknown' ? 'repl ?' : (n.io_thread ? 'IO✓' : 'IO✗') + ' ' + (n.sql_thread ? 'SQL✓' : 'SQL✗')) + '</td><td>' +
        (n.read_only ? 'RO' : 'RW') + (n.super_read_only ? '+SRO' : '') + '</td><td>' +
        esc(n.proxysql_status || '-') + ' HG' + esc(n.proxysql_hostgroup || '') + '</td></tr>';
    }).join('');
    var pxRows = (st.proxysql_servers || []).map(function (s) {
      return '<tr><td>' + esc(s.hostname) + ':' + esc(s.port) + '</td><td>HG ' + esc(s.hostgroup) + '</td><td>' + esc(s.status) + '</td></tr>';
    }).join('');
    var pxTable = pxRows ? '<h3>ProxySQL runtime_mysql_servers</h3><table><thead><tr><th>Backend</th><th>HG</th><th>Status</th></tr></thead><tbody>' + pxRows + '</tbody></table>' : '';
    panels.cluster.innerHTML = head + '<table><thead><tr><th>Node</th><th>GTID</th><th>Lag</th><th>Repl</th><th>Readonly</th><th>ProxySQL</th></tr></thead><tbody>' +
      rows + '</tbody></table>' + pxTable;
  }

  // asList accepts an array, {entries: [...]}, null or anything else.
  function asList(v) {
    if (Array.isArray(v)) return v;
    if (v && Array.isArray(v.entries)) return v.entries;
    return [];
  }

  function renderActions(list) {
    const rows = asList(list).slice().reverse().map(function (e) {
      const cls = e.result === 'blocked' || e.result === 'error' ? 'badge bad' : (e.result === 'warn' ? 'badge warn' : 'badge');
      return '<tr><td>' + esc(e.timestamp) + '</td><td>' + esc(e.epoch) + '</td><td>' + esc(e.guard) + '</td><td>' +
        esc(e.action) + '</td><td>' + esc(e.reason) + '</td><td><span class="' + cls + '">' + esc(e.result) + '</span></td><td>' + esc(e.step) + '</td></tr>';
    }).join('');
    panels.actions.innerHTML = '<table><thead><tr><th>Time</th><th>Epoch</th><th>Guard</th><th>Action</th><th>Reason</th><th>Result</th><th>Step</th></tr></thead><tbody>' +
      (rows || '<tr><td colspan="7">No actions</td></tr>') + '</tbody></table>';
  }

  function tick() {
    Promise.all([
      fetch('/status').then(function (r) { return r.json(); }),
      fetch('/api/actions').then(function (r) { return r.json(); })
    ]).then(function (res) {
      renderGuards(res[0]);
      renderCluster(res[0]);
      renderActions(res[1]);
      updated.textContent = 'Updated ' + new Date().toLocaleTimeString();
    }).catch(function (err) {
      updated.textContent = 'Refresh failed: ' + err;
    });
  }

  setInterval(tick, 3000);
  tick();
})();
