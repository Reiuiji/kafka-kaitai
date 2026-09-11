import { Events, WML } from "@wailsio/runtime";
import { KafkaService, KaitaiService } from "../bindings/github.com/okuu/kafka_kaitai/services";
import { store } from "./store";
import { renderDashboard } from "./views/dashboard";
import { renderConnections } from "./views/connections";
import { renderTopics } from "./views/topics";
import { renderProducer } from "./views/producer";
import { renderClusterFlow, updateClusterFlowLive } from "./views/flow";
import { renderKaitai } from "./views/kaitai";
import { renderSchemaRegistry } from "./views/schemaregistry";
import "./styles/theme.css";

WML.Enable();

// Root Application Shell
function renderAppShell() {
  const root = document.getElementById("app")!;
  root.innerHTML = `
    <div class="bg-mesh" aria-hidden="true"></div>
    <div class="app-shell">
      <!-- Sidebar -->
      <aside class="sidebar">
        <div class="brand-header">
          <div class="brand-icon">K</div>
          <div>
            <div class="brand-title">KafkaKaitai</div>
          </div>
          <span class="brand-badge">v2.1</span>
        </div>

        <nav class="nav-links">
          <div class="nav-item ${store.activeView === 'dashboard' ? 'active' : ''}" data-view="dashboard">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/></svg>
            Dashboard
          </div>
          <div class="nav-item ${store.activeView === 'flow' ? 'active' : ''}" data-view="flow">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="6" cy="6" r="3"/><circle cx="6" cy="18" r="3"/><circle cx="18" cy="12" r="3"/><line x1="8.5" y1="7.5" x2="15.5" y2="10.5"/><line x1="8.5" y1="16.5" x2="15.5" y2="13.5"/></svg>
            Cluster Flow
          </div>
          <div class="nav-item ${store.activeView === 'topics' ? 'active' : ''}" data-view="topics">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
            Topics & Consumer
          </div>
          <div class="nav-item ${store.activeView === 'producer' ? 'active' : ''}" data-view="producer">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>
            Producer & Stress
          </div>
          <div class="nav-item ${store.activeView === 'kaitai' ? 'active' : ''}" data-view="kaitai">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/><polyline points="10 9 9 9 8 9"/></svg>
            Kaitai Formats
          </div>
          <div class="nav-item ${store.activeView === 'connections' ? 'active' : ''}" data-view="connections">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><circle cx="12" cy="12" r="10"/><path d="M12 2a14.5 14.5 0 0 0 0 20 14.5 14.5 0 0 0 0-20"/><path d="M2 12h20"/></svg>
            Connections
          </div>
          <div class="nav-item ${store.activeView === 'schemaregistry' ? 'active' : ''}" data-view="schemaregistry">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><rect x="2" y="2" width="20" height="8" rx="2" ry="2"/><rect x="2" y="14" width="20" height="8" rx="2" ry="2"/><line x1="6" y1="6" x2="6.01" y2="6"/><line x1="6" y1="18" x2="6.01" y2="18"/></svg>
            Schema Registry
          </div>
        </nav>

        <!-- Sidebar Cluster Status -->
        <div class="sidebar-status">
          <div class="status-title">
            <span class="status-dot ${store.connection.connected ? 'connected' : 'disconnected'}"></span>
            <span>${store.connection.connected ? 'Connected' : 'Disconnected'}</span>
          </div>
          <div class="status-subtitle">
            ${store.connection.connected ? `${store.connection.brokers[0] || 'Cluster'} (${store.topics.length} topics)` : 'No active cluster'}
          </div>
        </div>
      </aside>

      <!-- Content Area -->
      <main class="content-area">
        <!-- Topbar -->
        <header class="topbar">
          <div class="topbar-title">
            <span style="text-transform: capitalize;">${store.activeView}</span>
          </div>

          <div class="topbar-stats">
            <div class="stat-chip">
              <span>Ping:</span>
              <b id="topbar-ping">${store.connection.connected ? `${store.connection.latencyMs.toFixed(0)}ms` : '-'}</b>
            </div>
            <div class="stat-chip">
              <span>Stress:</span>
              <b id="topbar-stress" style="color: ${store.stressMetrics.active ? 'var(--accent-cyan)' : 'var(--text-muted)'};">${store.stressMetrics.active ? `${store.stressMetrics.currentMsgRate.toFixed(0)} msg/s` : 'OFF'}</b>
            </div>
            <div class="stat-chip">
              <span>Consumer:</span>
              <b id="topbar-cons" style="color: ${store.consumerMetrics.active ? 'var(--accent-emerald)' : 'var(--text-muted)'};">${store.consumerMetrics.active ? `${store.consumerMetrics.currentMsgRate.toFixed(0)} msg/s` : 'OFF'}</b>
            </div>
            <div class="stat-chip">
              <span id="topbar-clock">--:--:--</span>
            </div>
          </div>
        </header>

        <!-- Dynamic View Container -->
        <div class="view-container" id="view-mount"></div>
      </main>
    </div>
  `;

  // Attach nav handlers
  root.querySelectorAll(".nav-item").forEach((item) => {
    item.addEventListener("click", () => {
      const view = item.getAttribute("data-view") || "dashboard";
      store.setView(view);
    });
  });

  // Mount active view
  mountView();
}

function mountView() {
  const mount = document.getElementById("view-mount");
  if (!mount) return;

  mount.innerHTML = "";
  switch (store.activeView) {
    case "dashboard":
      mount.appendChild(renderDashboard());
      break;
    case "flow":
      mount.appendChild(renderClusterFlow());
      break;
    case "topics":
      mount.appendChild(renderTopics());
      break;
    case "producer":
      mount.appendChild(renderProducer());
      break;
    case "kaitai":
      mount.appendChild(renderKaitai());
      break;
    case "connections":
      mount.appendChild(renderConnections());
      break;
    case "schemaregistry":
      mount.appendChild(renderSchemaRegistry());
      break;
    default:
      mount.appendChild(renderDashboard());
  }
}

// Subscribe to store updates
store.subscribe(() => {
  renderAppShell();
});

// Initialize background telemetry and data
async function init() {
  renderAppShell();

  // Listen to time event
  Events.On("time", (e: any) => {
    const clock = document.getElementById("topbar-clock");
    if (clock) clock.innerText = e.data;
  });

  // Listen to Go telemetry
  Events.On("telemetry", (e: any) => {
    store.systemTelemetry = e.data;
    const gEl = document.getElementById("telemetry-goroutines");
    const hEl = document.getElementById("telemetry-heap");
    const sEl = document.getElementById("telemetry-sys");
    const gcEl = document.getElementById("telemetry-gc");
    if (gEl) gEl.innerText = String(e.data.goroutines);
    if (hEl) hEl.innerHTML = `${e.data.allocMb.toFixed(1)} <span class="metric-unit">MB</span>`;
    if (sEl) sEl.innerHTML = `${e.data.sysMb.toFixed(1)} <span class="metric-unit">MB</span>`;
    if (gcEl) gcEl.innerText = String(e.data.numGc);
  });

  // Load formats from backend
  try {
    const formats = await KaitaiService.ListFormats();
    store.setFormats(formats || []);
  } catch (err) {
    console.error("Failed to load formats:", err);
  }

  // Load initial connection status
  try {
    const status = await KafkaService.GetStatus();
    if (status && status.connected) {
      store.setConnection(status);
      const topics = await KafkaService.ListTopics();
      store.setTopics(topics || []);
    }
  } catch (err) {
    console.error("Failed to get connection status:", err);
  }

  // High-frequency polling for active stress test & consumer benchmarks
  setInterval(async () => {
    try {
      const stress = await KafkaService.GetStressTestMetrics();
      if (stress) {
        store.stressMetrics = stress;
        const prodMsgEl = document.getElementById("dash-prod-msgrate");
        const prodByteEl = document.getElementById("dash-prod-byterate");
        const prodErrEl = document.getElementById("dash-prod-errors");
        const prodErrBox = document.getElementById("dash-prod-err-box");
        const topStressEl = document.getElementById("topbar-stress");
        if (prodMsgEl) prodMsgEl.innerHTML = `${stress.currentMsgRate.toFixed(0)} <span class="metric-unit">msg/s</span>`;
        if (prodByteEl) prodByteEl.innerHTML = `${(stress.currentByteRate / (1024 * 1024)).toFixed(2)} <span class="metric-unit">MB/s</span>`;
        if (prodErrEl) prodErrEl.innerText = stress.errorsCount.toLocaleString();
        if (prodErrBox) {
          prodErrBox.style.display = stress.errorsCount > 0 ? 'block' : 'none';
          prodErrBox.innerText = (stress as any).lastError || '';
        }
        if (topStressEl) topStressEl.innerText = stress.active ? `${stress.currentMsgRate.toFixed(0)} msg/s` : "OFF";

        // Update Producer View Live Telemetry if mounted
        const pMsgEl = document.getElementById("prod-stat-msgrate");
        const pByteEl = document.getElementById("prod-stat-byterate");
        const pLatEl = document.getElementById("prod-stat-p99");
        const pTotEl = document.getElementById("prod-stat-total");
        const pDot = document.getElementById("prod-status-dot");
        const pBadge = document.getElementById("prod-status-badge");
        const pCard = document.getElementById("prod-telemetry-card");
        const pErrBox = document.getElementById("prod-error-box");
        const pErrCount = document.getElementById("prod-stat-errors");
        const pErrText = document.getElementById("prod-stat-lasterror");

        if (pMsgEl) pMsgEl.innerHTML = `${stress.currentMsgRate.toFixed(0)} <span class="metric-unit">msg/s</span>`;
        if (pByteEl) pByteEl.innerHTML = `${(stress.currentByteRate / (1024 * 1024)).toFixed(2)} <span class="metric-unit">MB/s</span>`;
        if (pLatEl) pLatEl.innerHTML = `${stress.latencyP99.toFixed(2)} <span class="metric-unit">ms</span>`;
        if (pTotEl) pTotEl.innerText = stress.sentMessages.toLocaleString();
        if (pDot) pDot.className = `status-dot ${stress.active ? 'connected' : 'disconnected'}`;
        if (pBadge) pBadge.innerText = stress.active ? 'RUNNING' : 'STOPPED';
        if (pCard) pCard.style.borderColor = stress.active ? 'var(--accent-cyan)' : (stress.errorsCount > 0 ? 'var(--accent-rose)' : 'var(--border-subtle)');
        if (pErrBox) pErrBox.style.display = stress.errorsCount > 0 ? 'block' : 'none';
        if (pErrCount) pErrCount.innerText = stress.errorsCount.toLocaleString();
        if (pErrText) pErrText.innerText = (stress as any).lastError || '';
      }

      const cons = await KafkaService.GetConsumerMetrics();
      if (cons) {
        store.consumerMetrics = cons;
        const consMsgEl = document.getElementById("dash-cons-msgrate");
        const consByteEl = document.getElementById("dash-cons-byterate");
        const topConsEl = document.getElementById("topbar-cons");
        if (consMsgEl) consMsgEl.innerHTML = `${cons.currentMsgRate.toFixed(0)} <span class="metric-unit">msg/s</span>`;
        if (consByteEl) consByteEl.innerHTML = `${(cons.currentByteRate / (1024 * 1024)).toFixed(2)} <span class="metric-unit">MB/s</span>`;
        if (topConsEl) topConsEl.innerText = cons.active ? `${cons.currentMsgRate.toFixed(0)} msg/s` : "OFF";
      }

      if (store.activeView === "flow") {
        updateClusterFlowLive();
      }
    } catch {}
  }, 400);
}

init();
