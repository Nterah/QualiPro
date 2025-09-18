/* app/static/js/pqp_ai.js
   PQP AI Import – Choose → Preview → Commit (multi-file)
   - Renders file rows
   - Shows snapshot tables (first 5 rows / section) after preview
   - Sends job_id + override code on commit
*/

(function () {
  // ------- Endpoint detection (several fallbacks) -------
  function discoverEndpoints() {
    // 1) Preferred: global window.PQP_AI_ENDPOINTS = { preview, commit }
    if (window.PQP_AI_ENDPOINTS && window.PQP_AI_ENDPOINTS.preview && window.PQP_AI_ENDPOINTS.commit) {
      return window.PQP_AI_ENDPOINTS;
    }
    // 2) JSON from <script id="pqp-ai-endpoints"> {preview, commit}
    try {
      const tag = document.getElementById('pqp-ai-endpoints');
      if (tag) {
        const data = JSON.parse(tag.textContent || tag.innerText || '{}');
        if (data.preview && data.commit) return data;
      }
    } catch (e) {}
    // 3) Data attributes on #ai-root
    const root = document.getElementById('ai-root');
    if (root && (root.dataset.previewUrl || root.dataset.commitUrl)) {
      return { preview: root.dataset.previewUrl || '', commit: root.dataset.commitUrl || '' };
    }
    // 4) Absolute last resort: look for Flask url_for placeholders in the DOM (unlikely)
    return { preview: '/pqp/import/ai/preview', commit: '/pqp/import/ai/commit' };
  }

  const EP = discoverEndpoints();

  // ------- DOM refs (tolerant to your IDs) -------
  const refs = {
    table: document.getElementById('aiTable'),
    tbody: document.querySelector('#aiTable tbody') || document.getElementById('aiTbody'),
    chooseBtn: document.getElementById('aiChoose') || document.getElementById('btnChoose') || document.getElementById('choose-files'),
    input: document.getElementById('aiFiles') || document.getElementById('pqp-files'),
    previewBtn: document.getElementById('aiPreview') || document.getElementById('btnPreviewSel'),
    commitBtn: document.getElementById('aiCommit') || document.getElementById('btnCommitChecked') || document.getElementById('bulk-import'),
    checkAll: document.getElementById('checkAll'),
    log: document.getElementById('aiLog'),
  };

  // Friendly logger
  function log(msg) {
    if (!refs.log) return;
    const p = document.createElement('div');
    p.textContent = msg;
    refs.log.appendChild(p);
    refs.log.scrollTop = refs.log.scrollHeight;
  }

  // -------- File storage tied to rows --------
  const rowFile = new WeakMap();  // <tr> -> File object

  function makeEl(tag, cls, text) {
    const el = document.createElement(tag);
    if (cls) el.className = cls;
    if (text != null) el.textContent = text;
    return el;
  }

  function ensureTableHeader() {
    if (!refs.table) return;
    if (refs.table.getAttribute('data-initialized') === '1') return;
    // if your template already has headers, we leave them; otherwise create
    if (!refs.table.querySelector('thead')) {
      const thead = document.createElement('thead');
      thead.className = 'table-dark';
      const trh = document.createElement('tr');
      ['','Filename','Detected Code','Override Code','Status','Issues','Actions'].forEach(h => {
        const th = document.createElement('th'); th.textContent = h; trh.appendChild(th);
      });
      thead.appendChild(trh);
      refs.table.appendChild(thead);
    }
    refs.table.setAttribute('data-initialized','1');
  }

  function addFileRows(files) {
    ensureTableHeader();
    if (!refs.tbody) return;

    Array.from(files || []).forEach(f => {
      const tr = document.createElement('tr');

      // Checkbox
      const tdSel = document.createElement('td');
      const cb = document.createElement('input');
      cb.type = 'checkbox';
      tdSel.appendChild(cb);

      // Filename
      const tdName = makeEl('td', '', f.name);

      // Detected code
      const tdDetected = makeEl('td', 'detected', '');

      // Override code (textbox)
      const tdOverride = document.createElement('td');
      const inp = document.createElement('input');
      inp.type = 'text';
      inp.className = 'form-control form-control-sm';
      inp.placeholder = 'Override code (optional)';
      tdOverride.appendChild(inp);

      // Status
      const tdStatus = makeEl('td', 'status', 'New');

      // Issues (and snapshot will be appended below)
      const tdIssues = makeEl('td', 'issues', '');

      // Row actions (per-row preview/commit optional)
      const tdAct = document.createElement('td');
      const btnPrev = makeEl('button', 'btn btn-outline-primary btn-sm me-1', 'Preview');
      const btnCommit = makeEl('button', 'btn btn-success btn-sm', 'Commit');

      btnPrev.addEventListener('click', () => previewRow(tr));
      btnCommit.addEventListener('click', () => commitRow(tr));

      tdAct.appendChild(btnPrev);
      tdAct.appendChild(btnCommit);

      tr.appendChild(tdSel);
      tr.appendChild(tdName);
      tr.appendChild(tdDetected);
      tr.appendChild(tdOverride);
      tr.appendChild(tdStatus);
      tr.appendChild(tdIssues);
      tr.appendChild(tdAct);

      // store file for this row
      rowFile.set(tr, f);

      refs.tbody.appendChild(tr);
    });
  }

  // Choose button
  if (refs.chooseBtn && refs.input) {
    refs.chooseBtn.addEventListener('click', () => refs.input.click());
  }
  if (refs.input) {
    refs.input.addEventListener('change', (e) => {
      addFileRows(e.target.files);
      // reset so the same file can be chosen again later
      e.target.value = '';
    });
  }

  // Check all
  if (refs.checkAll && refs.tbody) {
    refs.checkAll.addEventListener('change', function () {
      const rows = Array.from(refs.tbody.querySelectorAll('tr'));
      rows.forEach(r => {
        const cb = r.querySelector('input[type="checkbox"]');
        if (cb) cb.checked = refs.checkAll.checked;
      });
    });
  }

  // Preview selected (top button)
  if (refs.previewBtn) {
    refs.previewBtn.addEventListener('click', async () => {
      const rows = getCheckedRows();
      if (!rows.length) return alert('Select at least one row to preview.');
      for (const tr of rows) {
        await previewRow(tr);
      }
    });
  }

  // Commit checked (top button)
  if (refs.commitBtn) {
    refs.commitBtn.addEventListener('click', async () => {
      const rows = getCheckedRows();
      if (!rows.length) return alert('Select at least one row to commit.');
      for (const tr of rows) {
        await commitRow(tr);
      }
    });
  }

  function getCheckedRows() {
    if (!refs.tbody) return [];
    return Array.from(refs.tbody.querySelectorAll('tr')).filter(tr => {
      const cb = tr.querySelector('input[type="checkbox"]');
      return cb && cb.checked;
    });
  }

  // -------- Preview a single row ----------
  async function previewRow(tr) {
    const f = rowFile.get(tr);
    if (!f) { alert('Missing file for this row.'); return; }

    const override = tr.querySelector('td:nth-child(4) input');
    const detected = tr.querySelector('.detected');
    const status = tr.querySelector('.status');
    const issuesCell = tr.querySelector('.issues');

    status.textContent = 'Previewing…';

    const fd = new FormData();
    fd.append('file', f);
    if (override && override.value.trim()) {
      fd.append('code', override.value.trim());
    }

    try {
      const r = await fetch(EP.preview, { method: 'POST', body: fd });
      const j = await r.json();

      if (!j || j.ok !== true) {
        status.textContent = 'Preview failed';
        issuesCell.textContent = (j && (j.error || (j.issues || []).join('; '))) || 'Unknown error';
        return;
      }

      // save job id on row (used later by commit)
      tr.dataset.jobId = String(j.job_id || '');

      // show detected code (if any)
      detected.textContent = j.detected_code || (override ? override.value.trim() : '');

      // show snapshot tables
      renderSnapshotUnderRow(issuesCell, j.snapshot);

      status.textContent = 'Previewed';
      log(`Previewed ${f.name}: code=${detected.textContent} sections=${Object.keys(j.snapshot || {}).length}`);
    } catch (e) {
      console.error(e);
      status.textContent = 'Preview failed';
      issuesCell.textContent = String(e);
    }
  }

  // -------- Commit a single row ----------
  async function commitRow(tr) {
    const jobId = tr.dataset.jobId || '';
    const status = tr.querySelector('.status');
    const issuesCell = tr.querySelector('.issues');
    const override = tr.querySelector('td:nth-child(4) input');

    if (!jobId) {
      return alert('Please preview this row first (no job_id yet).');
    }

    status.textContent = 'Committing…';

    const fd = new FormData();
    fd.append('job_id', jobId);
    if (override && override.value.trim()) {
      fd.append('code', override.value.trim()); // send full override (e.g., "291RT P700")
    }

    try {
      const r = await fetch(EP.commit, { method: 'POST', body: fd });
      const j = await r.json();

      if (j && j.ok) {
        status.textContent = 'Committed';
        issuesCell.textContent = (j.issues && j.issues.length) ? j.issues.join('; ') : '';
        log(`Committed job ${jobId}`);
      } else {
        status.textContent = 'Commit failed';
        issuesCell.textContent = (j && (j.error || (j.issues || []).join('; '))) || 'Unknown error';
      }
    } catch (e) {
      console.error(e);
      status.textContent = 'Commit failed';
      issuesCell.textContent = String(e);
    }
  }

  // -------- Snapshot renderer (first 5 rows per section) ----------
  function renderSnapshotUnderRow(containerCell, snapshot) {
    // Clear previous snapshot (keep any text errors)
    Array.from(containerCell.querySelectorAll('details[data-role="snapshot"]')).forEach(n => n.remove());

    const snap = snapshot || {};
    const keys = Object.keys(snap).sort((a, b) => Number(a) - Number(b));
    if (!keys.length) return; // nothing to show

    const details = document.createElement('details');
    details.setAttribute('data-role', 'snapshot');
    details.className = 'mt-2';

    const summary = document.createElement('summary');
    summary.textContent = 'Preview snapshot (first 5 rows per section)';
    details.appendChild(summary);

    keys.forEach(k => {
      const sec = snap[k] || {};
      const cols = sec.columns || [];
      const rows = sec.rows || [];
      const h = document.createElement('div');
      h.className = 'fw-bold mt-2';
      h.textContent = `Section ${k} — ${sec.row_count ?? rows.length} row(s) detected`;
      details.appendChild(h);

      const wrap = document.createElement('div');
      wrap.className = 'table-responsive';
      const tbl = document.createElement('table');
      tbl.className = 'table table-sm table-bordered mb-2';

      const thead = document.createElement('thead');
      const trh = document.createElement('tr');
      cols.forEach(c => { const th = document.createElement('th'); th.textContent = c; trh.appendChild(th); });
      thead.appendChild(trh);

      const tbody = document.createElement('tbody');
      rows.forEach(r => {
        const tr = document.createElement('tr');
        cols.forEach(c => {
          const td = document.createElement('td');
          td.textContent = (r && typeof r === 'object') ? (r[c] ?? '') : '';
          tr.appendChild(td);
        });
        tbody.appendChild(tr);
      });

      tbl.appendChild(thead);
      tbl.appendChild(tbody);
      wrap.appendChild(tbl);
      details.appendChild(wrap);
    });

    containerCell.appendChild(details);
  }

  // -------- Public helpers if you call from HTML buttons --------
  window.PQP_AI = window.PQP_AI || {};
  window.PQP_AI.previewSelected = async function () {
    const rows = getCheckedRows();
    if (!rows.length) return alert('Select at least one row to preview.');
    for (const tr of rows) await previewRow(tr);
  };
  window.PQP_AI.commitAll = async function () {
    const rows = getCheckedRows();
    if (!rows.length) return alert('Select at least one row to commit.');
    for (const tr of rows) await commitRow(tr);
  };

})();

