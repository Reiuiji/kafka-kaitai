import { store } from '../store';

export function renderDashboard(): HTMLElement {
  const container = document.createElement('div');
  container.innerHTML = `
    <div style="display: flex; flex-direction: column; gap: 24px;">
      <!-- Welcome / Overview Header -->
      <div style="display: flex; justify-content: space-between; align-items: center;">
        <div>
          <h1 style="font-size: 22px; font-weight: 700; letter-spacing: -0.02em; margin-bottom: 4px;">Cluster & Telemetry Dashboard</h1>
          <p style="color: var(--text-secondary); font-size: 13px;">High-throughput real-time Kafka monitoring and Kaitai binary decoding engine.</p>
        </div>
        <div style="display: flex; gap: 12px;">
          <button class="btn btn-secondary btn-sm" id="btn-dash-connect">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/></svg>
            ${store.connection.connected ? 'Cluster Connected' : 'Configure Connection'}
          </button>
          <button class="btn btn-primary btn-sm" id="btn-dash-stress">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
            Stress Test Bench
          </button>
        </div>
      </div>

      <!-- Cluster Overview Banner -->
      <div class="card" style="background: linear-gradient(135deg, rgba(13, 21, 39, 0.9), rgba(19, 30, 54, 0.7)); border-color: rgba(6, 182, 212, 0.2);">
        <div style="display: flex; align-items: center; justify-content: space-between;">
          <div style="display: flex; align-items: center; gap: 16px;">
            <span class="status-dot ${store.connection.connected ? 'connected' : 'disconnected'}" style="width: 14px; height: 14px;"></span>
            <div>
              <div style="font-weight: 600; font-size: 16px;">
                ${store.connection.connected ? `Connected: ${store.connection.clusterId || 'Kafka Cluster'}` : 'Kafka Disconnected'}
              </div>
              <div style="color: var(--text-secondary); font-size: 12px; font-family: var(--font-mono);">
                ${store.connection.connected ? `Brokers: ${store.connection.brokers.length} | Controller: Node ${store.connection.controllerId} | Round-trip Ping: ${store.connection.latencyMs.toFixed(1)}ms` : 'Go to Connections to specify broker addresses and authentication credentials.'}
              </div>
            </div>
          </div>
          <div style="display: flex; gap: 24px; text-align: right;">
            <div>
              <div class="metric-label">Topics</div>
              <div style="font-size: 20px; font-weight: 700; font-family: var(--font-mono); color: var(--accent-cyan);">
                ${store.connection.connected ? store.topics.length : 0}
              </div>
            </div>
            <div>
              <div class="metric-label">Kaitai Formats</div>
              <div style="font-size: 20px; font-weight: 700; font-family: var(--font-mono); color: var(--accent-violet);">
                ${store.formats.length}
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- Cluster Broker Topology Card -->
      ${store.connection.connected && store.connection.brokers.length > 0 ? `
        <div class="card" style="padding: 16px;">
          <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
            <div style="font-weight: 600; font-size: 13px; display: flex; align-items: center; gap: 8px;">
              <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="2" width="20" height="8" rx="2" ry="2"/><rect x="2" y="14" width="20" height="8" rx="2" ry="2"/><line x1="6" y1="6" x2="6.01" y2="6"/><line x1="6" y1="18" x2="6.01" y2="18"/></svg>
              Active Broker Topology (${store.connection.brokers.length} ${store.connection.brokers.length === 1 ? 'Node' : 'Nodes'})
            </div>
            ${store.connection.brokers.length < 3 ? `
              <div style="font-size: 11px; color: var(--text-muted);">
                Tip: Run <code style="color: var(--accent-cyan);">make cluster-up</code> to scale to 3 brokers for 1,000+ MB/s.
              </div>
            ` : `
              <span class="badge" style="background: rgba(16, 185, 129, 0.15); color: var(--accent-emerald); font-size: 11px; padding: 2px 8px; border-radius: 9999px;">
                High-Performance Multi-Node Cluster
              </span>
            `}
          </div>
          <div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 10px;">
            ${store.connection.brokers.map(b => {
              const isController = b.includes(`node ${store.connection.controllerId}`) || b.endsWith(`(${store.connection.controllerId})`);
              return `
                <div style="background: var(--bg-tertiary); border: 1px solid var(--border-subtle); border-radius: 6px; padding: 10px 12px; display: flex; align-items: center; justify-content: space-between;">
                  <div style="display: flex; align-items: center; gap: 8px; overflow: hidden;">
                    <span class="status-dot connected" style="width: 8px; height: 8px; flex-shrink: 0;"></span>
                    <span style="font-family: var(--font-mono); font-size: 12px; font-weight: 500; text-overflow: ellipsis; overflow: hidden; white-space: nowrap;">
                      ${b}
                    </span>
                  </div>
                  ${isController ? `
                    <span style="background: rgba(6, 182, 212, 0.15); color: var(--accent-cyan); font-size: 10px; font-weight: 600; padding: 1px 6px; border-radius: 4px; flex-shrink: 0;">
                      Controller
                    </span>
                  ` : ''}
                </div>
              `;
            }).join('')}
          </div>
        </div>
      ` : ''}

      <!-- Real-Time Throughput Meters (Producer & Consumer) -->
      <div class="grid-2">
        <!-- Producer Metrics Card -->
        <div class="card">
          <div class="card-title">
            <span style="display: flex; align-items: center; gap: 8px;">
              <span class="status-dot ${store.stressMetrics.active ? 'connected' : 'disconnected'}"></span>
              Producer Stress Performance
            </span>
            <span class="brand-badge">${store.stressMetrics.active ? 'ACTIVE' : 'IDLE'}</span>
          </div>

          <div class="grid-2" style="margin-bottom: 16px;">
            <div class="metric-box">
              <span class="metric-label">Ingress Rate</span>
              <span class="metric-value" id="dash-prod-msgrate">${store.stressMetrics.currentMsgRate.toFixed(0)} <span class="metric-unit">msg/s</span></span>
            </div>
            <div class="metric-box">
              <span class="metric-label">Bandwidth</span>
              <span class="metric-value" id="dash-prod-byterate">${(store.stressMetrics.currentByteRate / (1024 * 1024)).toFixed(2)} <span class="metric-unit">MB/s</span></span>
            </div>
          </div>

          <div class="grid-3">
            <div class="metric-box" style="padding: 10px;">
              <span class="metric-label">Sent Count</span>
              <span style="font-size: 16px; font-weight: 700; font-family: var(--font-mono);">${store.stressMetrics.sentMessages.toLocaleString()}</span>
            </div>
            <div class="metric-box" style="padding: 10px;">
              <span class="metric-label">Latency P95</span>
              <span style="font-size: 16px; font-weight: 700; font-family: var(--font-mono); color: var(--accent-amber);">${store.stressMetrics.latencyP95.toFixed(2)}ms</span>
            </div>
            <div class="metric-box" style="padding: 10px;">
              <span class="metric-label">Errors</span>
              <span id="dash-prod-errors" style="font-size: 16px; font-weight: 700; font-family: var(--font-mono); color: ${store.stressMetrics.errorsCount > 0 ? 'var(--accent-rose)' : 'var(--text-secondary)'};">${store.stressMetrics.errorsCount}</span>
            </div>
          </div>

          <div id="dash-prod-err-box" style="display: ${store.stressMetrics.errorsCount > 0 ? 'block' : 'none'}; margin-top: 10px; background: rgba(239, 68, 68, 0.12); border: 1px solid var(--accent-rose); border-radius: 6px; padding: 6px 10px; color: var(--accent-rose); font-size: 11px; font-family: var(--font-mono);">
            ${store.stressMetrics.lastError || ''}
          </div>
        </div>

        <!-- Consumer Metrics Card -->
        <div class="card">
          <div class="card-title">
            <span style="display: flex; align-items: center; gap: 8px;">
              <span class="status-dot ${store.consumerMetrics.active ? 'connected' : 'disconnected'}"></span>
              Consumer Benchmark / Ingress
            </span>
            <span class="brand-badge">${store.consumerMetrics.active ? (store.consumerMetrics.mode.toUpperCase()) : 'IDLE'}</span>
          </div>

          <div class="grid-2" style="margin-bottom: 16px;">
            <div class="metric-box">
              <span class="metric-label">Consumption Rate</span>
              <span class="metric-value" id="dash-cons-msgrate">${store.consumerMetrics.currentMsgRate.toFixed(0)} <span class="metric-unit">msg/s</span></span>
            </div>
            <div class="metric-box">
              <span class="metric-label">Throughput</span>
              <span class="metric-value" id="dash-cons-byterate">${(store.consumerMetrics.currentByteRate / (1024 * 1024)).toFixed(2)} <span class="metric-unit">MB/s</span></span>
            </div>
          </div>

          <div class="grid-3">
            <div class="metric-box" style="padding: 10px;">
              <span class="metric-label">Consumed</span>
              <span style="font-size: 16px; font-weight: 700; font-family: var(--font-mono);">${store.consumerMetrics.consumedMessages.toLocaleString()}</span>
            </div>
            <div class="metric-box" style="padding: 10px;">
              <span class="metric-label">Dropped (Benchmark)</span>
              <span style="font-size: 16px; font-weight: 700; font-family: var(--font-mono); color: var(--accent-cyan);">${store.consumerMetrics.droppedMessages.toLocaleString()}</span>
            </div>
            <div class="metric-box" style="padding: 10px;">
              <span class="metric-label">Stored to Disk</span>
              <span style="font-size: 16px; font-weight: 700; font-family: var(--font-mono); color: var(--accent-emerald);">${store.consumerMetrics.storedMessages.toLocaleString()}</span>
            </div>
          </div>
        </div>
      </div>

      <!-- Engine Telemetry -->
      <div class="card">
        <div class="card-title">Go Core Runtime Telemetry</div>
        <div class="grid-4">
          <div class="metric-box">
            <span class="metric-label">Goroutines</span>
            <span class="metric-value" id="telemetry-goroutines">${store.systemTelemetry ? store.systemTelemetry.goroutines : '-'}</span>
          </div>
          <div class="metric-box">
            <span class="metric-label">Heap Memory</span>
            <span class="metric-value" id="telemetry-heap">${store.systemTelemetry ? store.systemTelemetry.allocMb.toFixed(1) : '-'} <span class="metric-unit">MB</span></span>
          </div>
          <div class="metric-box">
            <span class="metric-label">System Memory</span>
            <span class="metric-value" id="telemetry-sys">${store.systemTelemetry ? store.systemTelemetry.sysMb.toFixed(1) : '-'} <span class="metric-unit">MB</span></span>
          </div>
          <div class="metric-box">
            <span class="metric-label">GC Cycles</span>
            <span class="metric-value" id="telemetry-gc">${store.systemTelemetry ? store.systemTelemetry.numGc : '-'}</span>
          </div>
        </div>
      </div>
    </div>
  `;

  // Attach button handlers
  container.querySelector('#btn-dash-connect')?.addEventListener('click', () => {
    store.setView('connections');
  });

  container.querySelector('#btn-dash-stress')?.addEventListener('click', () => {
    store.setView('producer');
  });

  return container;
}
