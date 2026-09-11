import { KafkaService } from '../../bindings/github.com/okuu/kafka_kaitai/services';
import type { ClusterFlowData, StressTestConfig, ConsumerBenchmarkConfig } from '../../bindings/github.com/okuu/kafka_kaitai/models';
import { store } from '../store';
import { showToast } from '../components/toast';

let flowDataCache: ClusterFlowData | null = null;
let activeTopic = 'perf-highspeed-30kb';

export function renderClusterFlow(): HTMLElement {
  const container = document.createElement('div');
  container.style.display = 'flex';
  container.style.flexDirection = 'column';
  container.style.gap = '20px';

  const topicsList = store.topics.length > 0 ? store.topics : [{ name: 'perf-highspeed-30kb', partitionsCount: 24 }];
  if (!topicsList.some(t => t.name === activeTopic) && topicsList.length > 0) {
    activeTopic = topicsList[0].name;
  }

  container.innerHTML = `
    <!-- Top Header & Master Toolbar -->
    <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 14px;">
      <div>
        <h1 style="font-size: 22px; font-weight: 700; letter-spacing: -0.02em; margin-bottom: 4px; display: flex; align-items: center; gap: 10px;">
          <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="var(--accent-cyan)" stroke-width="2"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
          Cluster Topology & Partition Flow
        </h1>
        <p style="color: var(--text-secondary); font-size: 13px;">Live visual data flow wiring Producers, Broker Nodes & Partitions, and Consumer Groups in real time.</p>
      </div>

      <div style="display: flex; align-items: center; gap: 12px;">
        <div style="display: flex; align-items: center; gap: 8px; background: rgba(15, 23, 42, 0.8); border: 1px solid var(--border-medium); border-radius: var(--radius-md); padding: 4px 10px;">
          <label style="font-size: 11px; font-weight: 600; color: var(--text-muted); text-transform: uppercase;">Topic:</label>
          <select id="flow-topic-select" style="background: transparent; border: none; color: var(--accent-cyan); font-weight: 600; font-size: 13px; outline: none; cursor: pointer;">
            ${topicsList.map(t => `<option value="${t.name}" ${t.name === activeTopic ? 'selected' : ''}>${t.name} (${t.partitionsCount || 24}P)</option>`).join('')}
          </select>
        </div>

        <button class="btn btn-secondary btn-sm" id="btn-flow-refresh">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M23 4v6h-6"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/></svg>
          Sync Topology
        </button>

        <button class="btn btn-primary btn-sm" id="btn-master-pipeline">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="5 3 19 12 5 21 5 3"/></svg>
          Run Full Pipeline Test
        </button>
      </div>
    </div>

    <!-- 3-Stage Pipeline Container with SVG Wiring -->
    <div style="position: relative; width: 100%; min-height: 520px;" id="flow-viewport">
      <svg class="flow-svg-overlay" id="flow-cables-svg"></svg>

      <div class="flow-container">
        <!-- ================= STAGE 1: PRODUCER ================= -->
        <div class="flow-col flow-col-producers">
          <div class="flow-stage-header">
            <span class="flow-stage-title" style="color: var(--accent-cyan);">
              <span class="status-dot ${store.stressMetrics.active ? 'connected' : 'disconnected'}" id="flow-prod-dot"></span>
              1. Producer Ingress
            </span>
            <span class="badge ${store.stressMetrics.active ? 'badge-cyan' : 'badge-gray'}" id="flow-prod-badge">
              ${store.stressMetrics.active ? 'PUMPING' : 'IDLE'}
            </span>
          </div>

          <div class="flow-station-card ${store.stressMetrics.active ? 'active-producer' : ''}" id="card-producer">
            <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 12px;">
              <div>
                <div style="font-size: 11px; color: var(--text-muted); font-weight: 600; text-transform: uppercase;">Worker Pool</div>
                <div style="font-size: 15px; font-weight: 700; color: var(--text-primary);">16 Concurrent Goroutines</div>
              </div>
              <span style="font-family: var(--font-mono); font-size: 11px; background: rgba(6, 182, 212, 0.15); color: var(--accent-cyan); padding: 2px 6px; border-radius: 4px;">
                30 KB Records
              </span>
            </div>

            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-bottom: 14px;">
              <div class="metric-box" style="padding: 10px;">
                <span class="metric-label">Data Rate</span>
                <span class="metric-value" id="flow-prod-byterate" style="font-size: 18px; color: var(--accent-cyan);">
                  ${(store.stressMetrics.currentByteRate / (1024 * 1024)).toFixed(1)} <span class="metric-unit">MB/s</span>
                </span>
              </div>
              <div class="metric-box" style="padding: 10px;">
                <span class="metric-label">Msg Speed</span>
                <span class="metric-value" id="flow-prod-msgrate" style="font-size: 18px;">
                  ${store.stressMetrics.currentMsgRate.toFixed(0)} <span class="metric-unit">msg/s</span>
                </span>
              </div>
            </div>

            <div style="display: flex; justify-content: space-between; align-items: center; font-size: 12px; color: var(--text-secondary); margin-bottom: 14px;">
              <span>Total Produced:</span>
              <b id="flow-prod-total" style="font-family: var(--font-mono); color: var(--text-primary);">${store.stressMetrics.sentMessages.toLocaleString()}</b>
            </div>

            <div style="display: flex; justify-content: space-between; align-items: center; border-top: 1px solid var(--border-subtle); padding-top: 12px;">
              <div style="font-size: 11px; color: var(--text-muted);">
                Partitioner: <code style="color: var(--accent-cyan);">Round-Robin</code>
              </div>
              <button class="btn btn-sm ${store.stressMetrics.active ? 'btn-danger' : 'btn-primary'}" id="btn-toggle-prod" style="padding: 4px 12px;">
                ${store.stressMetrics.active ? 'Halt' : 'Start'} Producer
              </button>
            </div>

            <div id="anchor-prod-out" style="position: absolute; right: 0; top: 50%; width: 1px; height: 1px;"></div>
          </div>
        </div>

        <!-- ================= STAGE 2: CLUSTER NODES & PARTITIONS ================= -->
        <div class="flow-col flow-col-cluster">
          <div class="flow-stage-header">
            <span class="flow-stage-title" style="color: var(--accent-violet);">
              <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="2" width="20" height="8" rx="2" ry="2"/><rect x="2" y="14" width="20" height="8" rx="2" ry="2"/><line x1="6" y1="6" x2="6.01" y2="6"/><line x1="6" y1="18" x2="6.01" y2="18"/></svg>
              2. Kafka Cluster Nodes & Partition Distribution
            </span>
            <span style="font-size: 11px; color: var(--text-muted); font-family: var(--font-mono);" id="flow-total-parts-label">
              24 Partitions
            </span>
          </div>

          <div id="flow-brokers-container" style="display: flex; flex-direction: column; gap: 12px;">
            <!-- Broker cards loaded dynamically -->
            <div style="color: var(--text-muted); font-size: 13px; text-align: center; padding: 40px;">
              Connecting to cluster nodes...
            </div>
          </div>
        </div>

        <!-- ================= STAGE 3: CONSUMER ================= -->
        <div class="flow-col flow-col-consumers">
          <div class="flow-stage-header">
            <span class="flow-stage-title" style="color: var(--accent-emerald);">
              <span class="status-dot ${store.consumerMetrics.active ? 'connected' : 'disconnected'}" id="flow-cons-dot"></span>
              3. Consumer Ingress
            </span>
            <span class="badge ${store.consumerMetrics.active ? 'badge-emerald' : 'badge-gray'}" id="flow-cons-badge">
              ${store.consumerMetrics.active ? 'READING' : 'IDLE'}
            </span>
          </div>

          <div class="flow-station-card ${store.consumerMetrics.active ? 'active-consumer' : ''}" id="card-consumer">
            <div id="anchor-cons-in" style="position: absolute; left: 0; top: 50%; width: 1px; height: 1px;"></div>

            <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 12px;">
              <div>
                <div style="font-size: 11px; color: var(--text-muted); font-weight: 600; text-transform: uppercase;">Ingress Engine</div>
                <div style="font-size: 15px; font-weight: 700; color: var(--text-primary);">Parallel Reader Client</div>
              </div>
              <span style="font-family: var(--font-mono); font-size: 11px; background: rgba(16, 185, 129, 0.15); color: var(--accent-emerald); padding: 2px 6px; border-radius: 4px;">
                Drop / Benchmark
              </span>
            </div>

            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-bottom: 14px;">
              <div class="metric-box" style="padding: 10px;">
                <span class="metric-label">Read Rate</span>
                <span class="metric-value" id="flow-cons-byterate" style="font-size: 18px; color: var(--accent-emerald);">
                  ${(store.consumerMetrics.currentByteRate / (1024 * 1024)).toFixed(1)} <span class="metric-unit">MB/s</span>
                </span>
              </div>
              <div class="metric-box" style="padding: 10px;">
                <span class="metric-label">Read Speed</span>
                <span class="metric-value" id="flow-cons-msgrate" style="font-size: 18px;">
                  ${store.consumerMetrics.currentMsgRate.toFixed(0)} <span class="metric-unit">msg/s</span>
                </span>
              </div>
            </div>

            <div style="display: flex; justify-content: space-between; align-items: center; font-size: 12px; color: var(--text-secondary); margin-bottom: 14px;">
              <span>Total Consumed:</span>
              <b id="flow-cons-total" style="font-family: var(--font-mono); color: var(--text-primary);">${store.consumerMetrics.consumedMessages.toLocaleString()}</b>
            </div>

            <div style="display: flex; justify-content: space-between; align-items: center; border-top: 1px solid var(--border-subtle); padding-top: 12px;">
              <div style="font-size: 11px; color: var(--text-muted);">
                Offset: <code style="color: var(--accent-emerald);">Start (0)</code>
              </div>
              <button class="btn btn-sm ${store.consumerMetrics.active ? 'btn-danger' : 'btn-secondary'}" id="btn-toggle-cons" style="padding: 4px 12px;">
                ${store.consumerMetrics.active ? 'Stop' : 'Start'} Consumer
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  `;

  // Draw and update wiring lines
  function drawCables() {
    const svg = container.querySelector('#flow-cables-svg') as SVGSVGElement;
    const viewport = container.querySelector('#flow-viewport') as HTMLElement;
    if (!svg || !viewport) return;

    const vpRect = viewport.getBoundingClientRect();
    svg.setAttribute('width', `${vpRect.width}`);
    svg.setAttribute('height', `${vpRect.height}`);

    const prodAnchor = container.querySelector('#anchor-prod-out');
    const consAnchor = container.querySelector('#anchor-cons-in');
    const brokerCards = container.querySelectorAll('.broker-node-card');

    if (!prodAnchor || !consAnchor || brokerCards.length === 0) {
      svg.innerHTML = '';
      return;
    }

    const pRect = prodAnchor.getBoundingClientRect();
    const cRect = consAnchor.getBoundingClientRect();

    const pX = pRect.left - vpRect.left;
    const pY = pRect.top - vpRect.top;

    const cX = cRect.left - vpRect.left;
    const cY = cRect.top - vpRect.top;

    const isProdActive = store.stressMetrics.active;
    const isConsActive = store.consumerMetrics.active;

    let pathsHtml = '';

    brokerCards.forEach(bCard => {
      const bRect = bCard.getBoundingClientRect();
      const bInX = bRect.left - vpRect.left;
      const bInY = bRect.top + bRect.height / 2 - vpRect.top;
      const bOutX = bRect.right - vpRect.left;
      const bOutY = bInY;

      // Cable 1: Producer -> Broker
      const cp1X = pX + (bInX - pX) * 0.5;
      const cp1Y = pY;
      const cp2X = pX + (bInX - pX) * 0.5;
      const cp2Y = bInY;

      const dProd = `M ${pX} ${pY} C ${cp1X} ${cp1Y}, ${cp2X} ${cp2Y}, ${bInX} ${bInY}`;
      pathsHtml += `<path d="${dProd}" class="flow-cable ${isProdActive ? 'cable-active-prod' : ''}"></path>`;

      // Cable 2: Broker -> Consumer
      const cp3X = bOutX + (cX - bOutX) * 0.5;
      const cp3Y = bOutY;
      const cp4X = bOutX + (cX - bOutX) * 0.5;
      const cp4Y = cY;

      const dCons = `M ${bOutX} ${bOutY} C ${cp3X} ${cp3Y}, ${cp4X} ${cp4Y}, ${cX} ${cY}`;
      pathsHtml += `<path d="${dCons}" class="flow-cable ${isConsActive ? 'cable-active-cons' : ''}"></path>`;
    });

    svg.innerHTML = pathsHtml;
  }

  // Render Brokers & Partitions
  function updateBrokersUI(flow: ClusterFlowData) {
    flowDataCache = flow;
    const brokersBox = container.querySelector('#flow-brokers-container') as HTMLElement;
    const partsLabel = container.querySelector('#flow-total-parts-label');
    if (partsLabel) {
      partsLabel.innerText = `${flow.totalPartitions} Partitions | ${(flow.totalMessages || 0).toLocaleString()} Total Msgs`;
    }

    if (!brokersBox) return;

    if (!flow.brokers || flow.brokers.length === 0) {
      brokersBox.innerHTML = `
        <div style="text-align: center; color: var(--text-muted); padding: 30px;">
          No brokers detected for topic '${flow.topic}'.
        </div>
      `;
      return;
    }

    brokersBox.innerHTML = flow.brokers.map(b => {
      const isController = b.isController;
      const isProducing = store.stressMetrics.active;
      const brokerMb = b.throughputMb || (isProducing ? (store.stressMetrics.currentByteRate / flow.brokers.length / (1024 * 1024)) : 0);
      const brokerMsg = b.msgRate || (isProducing ? (store.stressMetrics.currentMsgRate / flow.brokers.length) : 0);

      return `
        <div class="broker-node-card">
          <div class="broker-header">
            <div style="display: flex; align-items: center; gap: 8px;">
              <span class="status-dot connected" style="width: 8px; height: 8px;"></span>
              <span style="font-weight: 700; font-size: 13px; color: var(--text-primary);">Node ${b.nodeId}</span>
              <span style="font-family: var(--font-mono); font-size: 11px; color: var(--text-secondary);">${b.host}:${b.port}</span>
            </div>
            <div style="display: flex; align-items: center; gap: 8px;">
              ${isController ? `
                <span class="badge" style="background: rgba(6, 182, 212, 0.15); color: var(--accent-cyan); font-size: 10px; padding: 1px 6px;">
                  KRaft Controller
                </span>
              ` : ''}
              <span class="broker-throughput-label" style="font-family: var(--font-mono); font-size: 11px; font-weight: 600; color: ${isProducing ? 'var(--accent-cyan)' : 'var(--text-muted)'};">
                ${brokerMb > 0 ? `${brokerMb.toFixed(1)} MB/s` : 'Idle'}
              </span>
            </div>
          </div>

          <!-- Partition pills led by this broker -->
          <div>
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; font-size: 10px; color: var(--text-muted); text-transform: uppercase; font-weight: 600;">
              <span>Led Partitions (${b.partitions.length})</span>
              <span>${brokerMsg > 0 ? `${brokerMsg.toFixed(0)} msg/s` : ''}</span>
            </div>
            <div class="partitions-grid">
              ${b.partitions.map(p => `
                <div class="partition-pill ${isProducing ? 'active-traffic' : ''}">
                  <span class="part-id">P-${p.id}</span>
                  <span class="part-offset">${(p.highWatermark || 0).toLocaleString()}</span>
                </div>
              `).join('')}
            </div>
          </div>
        </div>
      `;
    }).join('');

    setTimeout(drawFlowCables, 50);
  }

  // Fetch initial flow topology
  async function loadFlow() {
    try {
      const data = await KafkaService.GetClusterFlow(activeTopic);
      if (data) {
        updateBrokersUI(data);
      }
    } catch (err) {
      console.error('Failed to load cluster flow:', err);
    }
  }

  loadFlow();

  // Resize listener
  window.addEventListener('resize', drawFlowCables);

  // Topic selector change
  container.querySelector('#flow-topic-select')?.addEventListener('change', (e) => {
    activeTopic = (e.target as HTMLSelectElement).value;
    loadFlow();
  });

  // Sync button
  container.querySelector('#btn-flow-refresh')?.addEventListener('click', () => {
    loadFlow();
    showToast('Topology refreshed', 'info');
  });

  // Producer Toggle Button
  const btnToggleProd = container.querySelector('#btn-toggle-prod') as HTMLButtonElement;
  btnToggleProd?.addEventListener('click', async () => {
    if (store.stressMetrics.active) {
      await KafkaService.StopStressTest();
      store.stressMetrics.active = false;
      showToast('Producer halted', 'info');
    } else {
      const cfg: StressTestConfig = {
        topic: activeTopic,
        concurrency: 16,
        targetRate: 0,
        durationSeconds: 0,
        messageSizeBytes: 30720,
        payloadType: 'random',
        customPayload: '',
        kaitaiFormatId: '',
        keyPattern: 'none',
        compression: 'none',
      };
      try {
        await KafkaService.StartStressTest(cfg);
        store.stressMetrics.active = true;
        showToast(`Producer started on ${activeTopic}!`, 'success');
      } catch (err: any) {
        showToast(`Producer error: ${err.message || String(err)}`, 'error');
      }
    }
    loadFlow();
  });

  // Consumer Toggle Button
  const btnToggleCons = container.querySelector('#btn-toggle-cons') as HTMLButtonElement;
  btnToggleCons?.addEventListener('click', async () => {
    if (store.consumerMetrics.active) {
      await KafkaService.StopConsumer();
      store.consumerMetrics.active = false;
      showToast('Consumer halted', 'info');
    } else {
      const cfg: ConsumerBenchmarkConfig = {
        topic: activeTopic,
        groupId: `flow-bench-${Date.now()}`,
        mode: 'drop',
        storeFilePath: '',
        storeFormat: 'raw',
        autoOffsetReset: 'earliest',
      };
      try {
        await KafkaService.StartConsumer(cfg);
        store.consumerMetrics.active = true;
        showToast(`Consumer reading from ${activeTopic}!`, 'success');
      } catch (err: any) {
        showToast(`Consumer error: ${err.message || String(err)}`, 'error');
      }
    }
    loadFlow();
  });

  // Master Run Full Pipeline Button
  const btnMaster = container.querySelector('#btn-master-pipeline') as HTMLButtonElement;
  btnMaster?.addEventListener('click', async () => {
    const isRunning = store.stressMetrics.active || store.consumerMetrics.active;
    if (isRunning) {
      await KafkaService.StopStressTest();
      await KafkaService.StopConsumer();
      store.stressMetrics.active = false;
      store.consumerMetrics.active = false;
      btnMaster.innerText = 'Run Full Pipeline Test';
      btnMaster.className = 'btn btn-primary btn-sm';
      showToast('Pipeline halted', 'info');
    } else {
      btnMaster.innerText = 'Halt Pipeline Test';
      btnMaster.className = 'btn btn-danger btn-sm';

      // 1. Launch Consumer
      const consCfg: ConsumerBenchmarkConfig = {
        topic: activeTopic,
        groupId: `flow-bench-${Date.now()}`,
        mode: 'drop',
        storeFilePath: '',
        storeFormat: 'raw',
        autoOffsetReset: 'earliest',
      };
      try {
        await KafkaService.StartConsumer(consCfg);
        store.consumerMetrics.active = true;
      } catch {}

      // 2. Launch Producer
      const prodCfg: StressTestConfig = {
        topic: activeTopic,
        concurrency: 16,
        targetRate: 0,
        durationSeconds: 0,
        messageSizeBytes: 30720,
        payloadType: 'random',
        customPayload: '',
        kaitaiFormatId: '',
        keyPattern: 'none',
        compression: 'none',
      };
      try {
        await KafkaService.StartStressTest(prodCfg);
        store.stressMetrics.active = true;
        showToast('Full Pipeline Test launched (Producer + Consumer)!', 'success');
      } catch (err: any) {
        showToast(`Pipeline launch error: ${err.message || String(err)}`, 'error');
      }
    }
    loadFlow();
  });

  return container;
}

// Draw and update wiring lines
export function drawFlowCables() {
  const viewport = document.getElementById('flow-viewport');
  if (!viewport) return;

  const svg = viewport.querySelector('#flow-cables-svg') as SVGSVGElement;
  if (!svg) return;

  const vpRect = viewport.getBoundingClientRect();
  if (vpRect.width === 0 || vpRect.height === 0) return;

  svg.setAttribute('width', `${vpRect.width}`);
  svg.setAttribute('height', `${vpRect.height}`);

  const prodAnchor = viewport.querySelector('#anchor-prod-out');
  const consAnchor = viewport.querySelector('#anchor-cons-in');
  const brokerCards = viewport.querySelectorAll('.broker-node-card');

  if (!prodAnchor || !consAnchor || brokerCards.length === 0) {
    svg.innerHTML = '';
    return;
  }

  const pRect = prodAnchor.getBoundingClientRect();
  const cRect = consAnchor.getBoundingClientRect();

  const pX = pRect.left - vpRect.left;
  const pY = pRect.top - vpRect.top;

  const cX = cRect.left - vpRect.left;
  const cY = cRect.top - vpRect.top;

  const isProdActive = store.stressMetrics.active;
  const isConsActive = store.consumerMetrics.active;

  let pathsHtml = '';

  brokerCards.forEach(bCard => {
    const bRect = bCard.getBoundingClientRect();
    const bInX = bRect.left - vpRect.left;
    const bInY = bRect.top + bRect.height / 2 - vpRect.top;
    const bOutX = bRect.right - vpRect.left;
    const bOutY = bInY;

    // Cable 1: Producer -> Broker
    const cp1X = pX + (bInX - pX) * 0.5;
    const cp1Y = pY;
    const cp2X = pX + (bInX - pX) * 0.5;
    const cp2Y = bInY;

    const dProd = `M ${pX} ${pY} C ${cp1X} ${cp1Y}, ${cp2X} ${cp2Y}, ${bInX} ${bInY}`;
    pathsHtml += `<path d="${dProd}" class="flow-cable ${isProdActive ? 'cable-active-prod' : ''}"></path>`;

    // Cable 2: Broker -> Consumer
    const cp3X = bOutX + (cX - bOutX) * 0.5;
    const cp3Y = bOutY;
    const cp4X = bOutX + (cX - bOutX) * 0.5;
    const cp4Y = cY;

    const dCons = `M ${bOutX} ${bOutY} C ${cp3X} ${cp3Y}, ${cp4X} ${cp4Y}, ${cX} ${cY}`;
    pathsHtml += `<path d="${dCons}" class="flow-cable ${isConsActive ? 'cable-active-cons' : ''}"></path>`;
  });

  svg.innerHTML = pathsHtml;
}

// Live ticker update called by main.ts polling loop
export function updateClusterFlowLive() {
  const viewport = document.getElementById('flow-viewport');
  if (!viewport) return;

  const isProd = store.stressMetrics.active;
  const isCons = store.consumerMetrics.active;

  // Producer DOM
  const prodByte = document.getElementById('flow-prod-byterate');
  const prodMsg = document.getElementById('flow-prod-msgrate');
  const prodTot = document.getElementById('flow-prod-total');
  const prodDot = document.getElementById('flow-prod-dot');
  const prodBadge = document.getElementById('flow-prod-badge');
  const prodCard = document.getElementById('card-producer');
  const prodBtn = document.getElementById('btn-toggle-prod');

  if (prodByte) prodByte.innerHTML = `${(store.stressMetrics.currentByteRate / (1024 * 1024)).toFixed(1)} <span class="metric-unit">MB/s</span>`;
  if (prodMsg) prodMsg.innerHTML = `${store.stressMetrics.currentMsgRate.toFixed(0)} <span class="metric-unit">msg/s</span>`;
  if (prodTot) prodTot.innerText = store.stressMetrics.sentMessages.toLocaleString();
  if (prodDot) prodDot.className = `status-dot ${isProd ? 'connected' : 'disconnected'}`;
  if (prodBadge) {
    prodBadge.className = `badge ${isProd ? 'badge-cyan' : 'badge-gray'}`;
    prodBadge.innerText = isProd ? 'PUMPING' : 'IDLE';
  }
  if (prodCard) {
    if (isProd) prodCard.classList.add('active-producer');
    else prodCard.classList.remove('active-producer');
  }
  if (prodBtn) {
    prodBtn.innerText = isProd ? 'Halt Producer' : 'Start Producer';
    prodBtn.className = `btn btn-sm ${isProd ? 'btn-danger' : 'btn-primary'}`;
  }

  // Consumer DOM
  const consByte = document.getElementById('flow-cons-byterate');
  const consMsg = document.getElementById('flow-cons-msgrate');
  const consTot = document.getElementById('flow-cons-total');
  const consDot = document.getElementById('flow-cons-dot');
  const consBadge = document.getElementById('flow-cons-badge');
  const consCard = document.getElementById('card-consumer');
  const consBtn = document.getElementById('btn-toggle-cons');

  if (consByte) consByte.innerHTML = `${(store.consumerMetrics.currentByteRate / (1024 * 1024)).toFixed(1)} <span class="metric-unit">MB/s</span>`;
  if (consMsg) consMsg.innerHTML = `${store.consumerMetrics.currentMsgRate.toFixed(0)} <span class="metric-unit">msg/s</span>`;
  if (consTot) consTot.innerText = store.consumerMetrics.consumedMessages.toLocaleString();
  if (consDot) consDot.className = `status-dot ${isCons ? 'connected' : 'disconnected'}`;
  if (consBadge) {
    consBadge.className = `badge ${isCons ? 'badge-emerald' : 'badge-gray'}`;
    consBadge.innerText = isCons ? 'READING' : 'IDLE';
  }
  if (consCard) {
    if (isCons) consCard.classList.add('active-consumer');
    else consCard.classList.remove('active-consumer');
  }
  if (consBtn) {
    consBtn.innerText = isCons ? 'Stop Consumer' : 'Start Consumer';
    consBtn.className = `btn btn-sm ${isCons ? 'btn-danger' : 'btn-secondary'}`;
  }

  // Master button sync
  const btnMaster = document.getElementById('btn-master-pipeline') as HTMLButtonElement | null;
  if (btnMaster) {
    const isAnyRunning = isProd || isCons;
    btnMaster.innerText = isAnyRunning ? 'Halt Pipeline Test' : 'Run Full Pipeline Test';
    btnMaster.className = `btn btn-sm ${isAnyRunning ? 'btn-danger' : 'btn-primary'}`;
  }

  // Broker cards throughput labels & partition active classes
  const brokerCards = document.querySelectorAll('.broker-node-card');
  const numBrokers = brokerCards.length || 3;
  brokerCards.forEach(card => {
    const rateEl = card.querySelector('.broker-throughput-label');
    if (rateEl) {
      if (isProd) {
        const shareMb = (store.stressMetrics.currentByteRate / numBrokers / (1024 * 1024));
        rateEl.textContent = `${shareMb.toFixed(1)} MB/s`;
        (rateEl as HTMLElement).style.color = 'var(--accent-cyan)';
      } else {
        rateEl.textContent = 'Idle';
        (rateEl as HTMLElement).style.color = 'var(--text-muted)';
      }
    }
  });

  const partPills = document.querySelectorAll('.partition-pill');
  partPills.forEach(pill => {
    if (isProd) {
      pill.classList.add('active-traffic');
    } else {
      pill.classList.remove('active-traffic');
    }
  });

  drawFlowCables();
}

