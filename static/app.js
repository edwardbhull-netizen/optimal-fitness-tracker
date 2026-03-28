/* ── Optimal Fitness Tracker — app.js ─────────────────────────────────────── */

// ── Toast helper ──────────────────────────────────────────────────────────────
function showToast(msg, duration = 2000) {
  let el = document.getElementById('toast');
  if (!el) {
    el = document.createElement('div');
    el.id = 'toast';
    el.className = 'toast';
    document.body.appendChild(el);
  }
  el.textContent = msg;
  el.classList.add('show');
  clearTimeout(el._timer);
  el._timer = setTimeout(() => el.classList.remove('show'), duration);
}

// ── Login page ────────────────────────────────────────────────────────────────
function initLogin() {
  const tabs = document.querySelectorAll('.login-tab');
  const clientForm = document.getElementById('client-form');
  const coachForm = document.getElementById('coach-form');
  if (!tabs.length) return;

  tabs.forEach(tab => {
    tab.addEventListener('click', () => {
      tabs.forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      const target = tab.dataset.target;
      if (target === 'client') {
        clientForm.classList.remove('hidden');
        coachForm.classList.add('hidden');
      } else {
        clientForm.classList.add('hidden');
        coachForm.classList.remove('hidden');
      }
    });
  });
}

// ── Session page ──────────────────────────────────────────────────────────────

let activeSessionId = null;
let currentExercise = null;
let sessionLoggedExercises = new Set();
let exerciseSetsLogged = {};

function initSession() {
  // Get or create session
  const dayKey = document.getElementById('programme-day')?.value;
  if (!dayKey) return;

  // Check if session id is already stored
  const storedId = sessionStorage.getItem('session_id_' + dayKey);
  if (storedId) {
    activeSessionId = parseInt(storedId);
    loadSessionLogs();
  }

  // Start session button
  const startBtn = document.getElementById('start-session-btn');
  if (startBtn) {
    startBtn.addEventListener('click', startSession);
  }

  // Finish session
  const finishBtn = document.getElementById('finish-session-btn');
  if (finishBtn) {
    finishBtn.addEventListener('click', finishSession);
  }

  // Log set buttons
  document.querySelectorAll('.log-set-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const exercise = btn.dataset.exercise;
      const label = btn.dataset.label;
      openLogModal(exercise, label);
    });
  });

  // Modal close
  const overlay = document.getElementById('log-modal-overlay');
  if (overlay) {
    overlay.addEventListener('click', e => {
      if (e.target === overlay) closeLogModal();
    });
  }

  // Modal form submit
  const logForm = document.getElementById('log-form');
  if (logForm) {
    logForm.addEventListener('submit', submitLog);
  }

  // RPE buttons
  document.querySelectorAll('.rpe-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.rpe-btn').forEach(b => b.classList.remove('selected'));
      btn.classList.add('selected');
      document.getElementById('rpe-input').value = btn.dataset.rpe;
    });
  });
}

async function startSession() {
  const dayKey = document.getElementById('programme-day').value;
  const btn = document.getElementById('start-session-btn');
  btn.disabled = true;

  try {
    const form = new FormData();
    form.append('programme_day', dayKey);
    const res = await fetch('/client/session/start', { method: 'POST', body: form });
    const data = await res.json();
    activeSessionId = data.session_id;
    sessionStorage.setItem('session_id_' + dayKey, activeSessionId);

    // Show the exercise list
    document.getElementById('session-locked').classList.add('hidden');
    document.getElementById('session-exercises').classList.remove('hidden');
    document.getElementById('finish-bar').classList.remove('hidden');
    showToast('Session started!');
  } catch (e) {
    showToast('Error starting session');
    btn.disabled = false;
  }
}

async function loadSessionLogs() {
  if (!activeSessionId) return;
  try {
    const res = await fetch(`/client/session-logs/${activeSessionId}`);
    const data = await res.json();

    // Show exercise list if session exists
    document.getElementById('session-locked')?.classList.add('hidden');
    document.getElementById('session-exercises')?.classList.remove('hidden');
    document.getElementById('finish-bar')?.classList.remove('hidden');

    // Group logs by exercise
    const grouped = {};
    data.logs.forEach(log => {
      if (!grouped[log.exercise_name]) grouped[log.exercise_name] = [];
      grouped[log.exercise_name].push(log);
    });

    // Update exercise cards
    Object.entries(grouped).forEach(([name, logs]) => {
      exerciseSetsLogged[name] = logs;
      updateExerciseCard(name, logs);
    });

    updateProgress();
  } catch (e) {
    console.error('Error loading logs', e);
  }
}

function updateExerciseCard(exerciseName, logs) {
  const safeId = exerciseName.replace(/[^a-z0-9]/gi, '_').toLowerCase();
  const card = document.getElementById('card_' + safeId);
  if (!card) return;

  if (logs && logs.length > 0) {
    card.classList.add('logged');
    sessionLoggedExercises.add(exerciseName);

    // Update sets display
    let setsHtml = '';
    logs.forEach(log => {
      let detail = `Set ${log.set_number}: `;
      const parts = [];
      if (log.weight_kg) parts.push(`${log.weight_kg}kg`);
      if (log.reps) parts.push(`${log.reps} reps`);
      if (log.duration_seconds) parts.push(`${log.duration_seconds}s`);
      if (log.calories) parts.push(`${log.calories} cal`);
      if (log.rpe) parts.push(`RPE ${log.rpe}`);
      detail += parts.join(' · ');
      setsHtml += `
        <div class="set-row">
          <span class="set-number">S${log.set_number}</span>
          <span class="set-details">${detail}</span>
          <button class="set-delete" onclick="deleteLog(${log.id}, '${exerciseName}')" title="Remove">✕</button>
        </div>`;
    });

    const setsContainer = card.querySelector('.sets-logged');
    if (setsContainer) {
      setsContainer.innerHTML = setsHtml;
      setsContainer.style.display = 'block';
    }
  }

  updateProgress();
}

function updateProgress() {
  const total = document.querySelectorAll('.exercise-card').length;
  const done = sessionLoggedExercises.size;
  const bar = document.getElementById('progress-bar');
  const label = document.getElementById('progress-label');
  if (bar) bar.style.width = total > 0 ? `${(done / total) * 100}%` : '0%';
  if (label) label.textContent = `${done} / ${total} exercises logged`;
}

async function openLogModal(exerciseName, label) {
  if (!activeSessionId) {
    showToast('Tap "Start Session" first');
    return;
  }

  currentExercise = { name: exerciseName, label };

  // Set modal title
  document.getElementById('modal-exercise-name').textContent = exerciseName;

  // Work out next set number
  const prevLogs = exerciseSetsLogged[exerciseName] || [];
  const nextSet = prevLogs.length + 1;
  document.getElementById('set-number-input').value = nextSet;
  document.getElementById('modal-set-label').textContent = `Logging Set ${nextSet}`;

  // Fetch history
  try {
    const res = await fetch(`/client/exercise/${encodeURIComponent(exerciseName)}/history`);
    const data = await res.json();
    const prevEl = document.getElementById('modal-prev-data');

    let html = '';
    if (data.last_sets && data.last_sets.length > 0) {
      const ls = data.last_sets;
      html += `<strong>Last time:</strong> `;
      html += ls.map(s => {
        const parts = [];
        if (s.weight_kg) parts.push(`${s.weight_kg}kg`);
        if (s.reps) parts.push(`${s.reps}r`);
        if (s.rpe) parts.push(`RPE${s.rpe}`);
        return `Set ${s.set_number}: ${parts.join(' ')}`;
      }).join(' · ');
      html += '<br>';
    }
    if (data.pb && (data.pb.best_weight || data.pb.best_reps)) {
      html += `<strong>PB:</strong> `;
      if (data.pb.best_weight) html += `${data.pb.best_weight}kg`;
      if (data.pb.best_weight && data.pb.best_reps) html += ' / ';
      if (data.pb.best_reps) html += `${data.pb.best_reps} reps`;
    }

    prevEl.innerHTML = html || '<span class="text-muted">No previous data</span>';
  } catch (e) {
    console.error('Error fetching history', e);
  }

  // Reset form
  document.getElementById('log-form').reset();
  document.querySelectorAll('.rpe-btn').forEach(b => b.classList.remove('selected'));
  document.getElementById('rpe-input').value = '';

  // Open modal
  const overlay = document.getElementById('log-modal-overlay');
  overlay.classList.add('open');
  setTimeout(() => document.getElementById('weight-input')?.focus(), 300);
}

function closeLogModal() {
  document.getElementById('log-modal-overlay')?.classList.remove('open');
  currentExercise = null;
}

async function submitLog(e) {
  e.preventDefault();
  if (!activeSessionId || !currentExercise) return;

  const btn = document.getElementById('log-submit-btn');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span>';

  const form = new FormData();
  form.append('session_id', activeSessionId);
  form.append('exercise_name', currentExercise.name);
  form.append('label', currentExercise.label || '');
  form.append('set_number', document.getElementById('set-number-input').value);

  const weight = document.getElementById('weight-input').value;
  const reps = document.getElementById('reps-input').value;
  const duration = document.getElementById('duration-input').value;
  const calories = document.getElementById('calories-input').value;
  const rpe = document.getElementById('rpe-input').value;
  const notes = document.getElementById('notes-input').value;

  if (weight) form.append('weight_kg', weight);
  if (reps) form.append('reps', reps);
  if (duration) form.append('duration_seconds', duration);
  if (calories) form.append('calories', calories);
  if (rpe) form.append('rpe', rpe);
  if (notes) form.append('notes', notes);

  try {
    const res = await fetch('/client/log-set', { method: 'POST', body: form });
    const data = await res.json();

    if (data.ok) {
      // Add to local state
      if (!exerciseSetsLogged[currentExercise.name]) {
        exerciseSetsLogged[currentExercise.name] = [];
      }
      exerciseSetsLogged[currentExercise.name].push({
        id: data.log_id,
        set_number: parseInt(document.getElementById('set-number-input').value),
        weight_kg: weight ? parseFloat(weight) : null,
        reps: reps ? parseInt(reps) : null,
        duration_seconds: duration ? parseInt(duration) : null,
        calories: calories ? parseInt(calories) : null,
        rpe: rpe ? parseInt(rpe) : null,
      });

      updateExerciseCard(currentExercise.name, exerciseSetsLogged[currentExercise.name]);
      closeLogModal();
      showToast('Set logged!');
    } else {
      showToast('Error logging set');
    }
  } catch (err) {
    showToast('Network error');
  } finally {
    btn.disabled = false;
    btn.innerHTML = 'Log Set';
  }
}

async function deleteLog(logId, exerciseName) {
  try {
    await fetch(`/client/log/${logId}`, { method: 'DELETE' });
    // Remove from local state
    if (exerciseSetsLogged[exerciseName]) {
      exerciseSetsLogged[exerciseName] = exerciseSetsLogged[exerciseName].filter(l => l.id !== logId);
      if (exerciseSetsLogged[exerciseName].length === 0) {
        sessionLoggedExercises.delete(exerciseName);
      }
      updateExerciseCard(exerciseName, exerciseSetsLogged[exerciseName]);
    }
    showToast('Set removed');
  } catch (e) {
    showToast('Error removing set');
  }
}

async function finishSession() {
  if (!activeSessionId) return;

  // First tap — show the calories row
  const calsRow = document.getElementById('finish-calories-row');
  const finishBtn = document.getElementById('finish-session-btn');
  if (calsRow && calsRow.style.display === 'none') {
    calsRow.style.display = 'block';
    finishBtn.style.display = 'none';

    // Wire up confirm button
    const confirmBtn = document.getElementById('finish-confirm-btn');
    if (confirmBtn) {
      confirmBtn.addEventListener('click', () => doFinishSession());
    }
    return;
  }
  doFinishSession();
}

async function doFinishSession() {
  const confirmBtn = document.getElementById('finish-confirm-btn');
  if (confirmBtn) { confirmBtn.disabled = true; confirmBtn.textContent = 'Finishing...'; }

  const calsInput = document.getElementById('finish-cals-input');
  const sourceSelect = document.getElementById('finish-cals-source');
  const deviceCals = calsInput ? calsInput.value : '';
  const deviceSource = sourceSelect ? sourceSelect.value : 'Manual';

  try {
    const form = new FormData();
    form.append('notes', '');
    if (deviceCals) {
      form.append('device_calories', deviceCals);
      form.append('device_source', deviceSource);
    }
    await fetch(`/client/session/${activeSessionId}/complete`, { method: 'POST', body: form });
    const dayKey = document.getElementById('programme-day').value;
    sessionStorage.removeItem('session_id_' + dayKey);
    showToast('Session complete!', 1500);
    setTimeout(() => window.location.href = '/client/history', 1600);
  } catch (e) {
    showToast('Error finishing session');
    if (confirmBtn) { confirmBtn.disabled = false; confirmBtn.textContent = 'Confirm & Finish'; }
  }
}

// ── History page ──────────────────────────────────────────────────────────────
function initHistory() {
  document.querySelectorAll('.history-session-header').forEach(header => {
    header.addEventListener('click', () => {
      const body = header.nextElementSibling;
      if (body) body.classList.toggle('open');
      const arrow = header.querySelector('.history-arrow');
      if (arrow) arrow.textContent = body.classList.contains('open') ? '▲' : '▼';
    });
  });
}

// ── Coach dashboard ───────────────────────────────────────────────────────────
function initCoach() {
  const addClientForm = document.getElementById('add-client-form');
  if (!addClientForm) return;

  addClientForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const form = new FormData(addClientForm);
    try {
      const res = await fetch('/coach/add-client', { method: 'POST', body: form });
      const data = await res.json();
      if (data.ok) {
        showToast('Client added!');
        setTimeout(() => location.reload(), 1200);
      } else {
        showToast('Error adding client');
      }
    } catch (err) {
      showToast('Network error');
    }
  });

  // Toggle add-client form
  const toggleBtn = document.getElementById('toggle-add-client');
  const addClientSection = document.getElementById('add-client-section');
  if (toggleBtn && addClientSection) {
    toggleBtn.addEventListener('click', () => {
      addClientSection.classList.toggle('hidden');
    });
  }
}

// ── PWA service worker registration ──────────────────────────────────────────
function registerSW() {
  if ('serviceWorker' in navigator) {
    navigator.serviceWorker.register('/static/sw.js').catch(err => {
      console.log('SW registration failed:', err);
    });
  }
}

// ── Init ──────────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  registerSW();
  initLogin();
  initSession();
  initHistory();
  initCoach();
});
