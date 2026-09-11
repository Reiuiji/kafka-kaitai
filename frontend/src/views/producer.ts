import { KafkaService } from '../../bindings/github.com/okuu/kafka_kaitai/services';
import type { ProduceRequest, StressTestConfig } from '../../bindings/github.com/okuu/kafka_kaitai/models';
import { store } from '../store';
import { showToast } from '../components/toast';

export function renderProducer(): HTMLElement {
  const container = document.createElement('div');

  const selectedTopic = store.stressConfig.topic || store.selectedTopic || (store.topics.length > 0 ? store.topics[0].name : '');

  container.innerHTML = `
    <div style="display: flex; flex-direction: column; gap: 24px; max-width: 1000px; margin: 0 auto;">
      <div style="display: flex; justify-content: space-between; align-items: center;">
        <div>
          <h1 style="font-size: 22px; font-weight: 700; letter-spacing: -0.02em; margin-bottom: 4px;">Producer & Stress Testing</h1>
          <p style="color: var(--text-secondary); font-size: 13px;">Publish single binary/JSON messages or run high-speed goroutine load tests.</p>
        </div>

        <!-- Mode Toggle -->
        <div style="display: flex; background: rgba(15, 23, 42, 0.8); border-radius: var(--radius-md); padding: 4px; border: 1px solid var(--border-medium); gap: 4px;">
          <button class="btn btn-prod-tab active" data-tab="stress">Stress Testing Bench</button>
          <button class="btn btn-prod-tab" data-tab="single">Single Message</button>
        </div>
      </div>

      <!-- STRESS TESTING PANEL -->
      <div id="tab-stress-panel" style="display: flex; flex-direction: column; gap: 20px;">
        <!-- Real-time metrics banner -->
        <div class="card" id="prod-telemetry-card" style="border-color: ${store.stressMetrics.active ? 'var(--accent-cyan)' : 'var(--border-subtle)'};">
          <div class="card-title">
            <span style="display: flex; align-items: center; gap: 8px;">
              <span id="prod-status-dot" class="status-dot ${store.stressMetrics.active ? 'connected' : 'disconnected'}"></span>
              Stress Test Live Telemetry
            </span>
            <span id="prod-status-badge" class="brand-badge">${store.stressMetrics.active ? 'RUNNING' : 'STOPPED'}</span>
          </div>

          <div class="grid-4" style="margin-bottom: 16px;">
            <div class="metric-box">
              <span class="metric-label">Message Throughput</span>
              <span class="metric-value" id="prod-stat-msgrate">${store.stressMetrics.currentMsgRate.toFixed(0)} <span class="metric-unit">msg/s</span></span>
            </div>
            <div class="metric-box">
              <span class="metric-label">Data Bandwidth</span>
              <span class="metric-value" id="prod-stat-byterate">${(store.stressMetrics.currentByteRate / (1024 * 1024)).toFixed(2)} <span class="metric-unit">MB/s</span></span>
            </div>
            <div class="metric-box">
              <span class="metric-label">Latency P99</span>
              <span class="metric-value" id="prod-stat-p99" style="color: var(--accent-amber);">${store.stressMetrics.latencyP99.toFixed(2)} <span class="metric-unit">ms</span></span>
            </div>
            <div class="metric-box">
              <span class="metric-label">Total Messages</span>
              <span class="metric-value" id="prod-stat-total">${store.stressMetrics.sentMessages.toLocaleString()}</span>
            </div>
          </div>

          <div style="display: flex; justify-content: flex-end; gap: 12px;" id="prod-action-btn-container">
            ${store.stressMetrics.active ? `
              <button class="btn btn-danger" id="btn-stress-stop">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="6" y="6" width="12" height="12"/></svg>
                Halt Stress Test
              </button>
            ` : `
              <button class="btn btn-primary" id="btn-stress-start">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="5 3 19 12 5 21 5 3"/></svg>
                Launch Load Test
              </button>
            `}
          </div>
        </div>

        <!-- Stress Test Configuration Form -->
        <div class="card">
          <div class="card-title">Test Parameters</div>

          <div class="grid-2">
            <div class="form-group">
              <label class="form-label">Target Topic</label>
              <select class="select" id="stress-topic">
                ${store.topics.length === 0 ? '<option value="">No topics in cluster (type manual below)</option>' : ''}
                ${store.topics.map(t => `<option value="${t.name}" ${t.name === selectedTopic ? 'selected' : ''}>${t.name} (${t.partitionsCount} partitions)</option>`).join('')}
              </select>
            </div>
            <div class="form-group">
              <label class="form-label">Manual Topic Override</label>
              <input class="input" id="stress-topic-manual" value="${store.stressConfig.topicManual || ''}" placeholder="Leave blank to use selected topic above" />
            </div>
          </div>

          <div class="grid-3">
            <div class="form-group">
              <label class="form-label">Concurrency (Worker Goroutines)</label>
              <input type="number" class="input" id="stress-concurrency" value="${store.stressConfig.concurrency}" min="1" max="128" />
            </div>
            <div class="form-group">
              <label class="form-label">Target Rate (0 = Unbounded Max Speed)</label>
              <input type="number" class="input" id="stress-rate" value="${store.stressConfig.targetRate}" min="0" placeholder="0 for max" />
            </div>
            <div class="form-group">
              <label class="form-label">Duration in Seconds (0 = Indefinite)</label>
              <input type="number" class="input" id="stress-duration" value="${store.stressConfig.durationSeconds}" min="0" />
            </div>
          </div>

          <div class="grid-3">
            <div class="form-group">
              <label class="form-label">Payload Type</label>
              <select class="select" id="stress-payload-type">
                <option value="random" ${store.stressConfig.payloadType === 'random' ? 'selected' : ''}>Random Binary Bytes</option>
                <option value="custom" ${store.stressConfig.payloadType === 'custom' ? 'selected' : ''}>Custom Text / JSON</option>
              </select>
            </div>
            <div class="form-group">
              <label class="form-label">Message Size (Bytes)</label>
              <input type="number" class="input" id="stress-payload-size" value="${store.stressConfig.messageSizeBytes}" min="1" />
            </div>
            <div class="form-group">
              <label class="form-label">Key Generation Pattern</label>
              <select class="select" id="stress-key-pattern">
                <option value="uuid" ${store.stressConfig.keyPattern === 'uuid' ? 'selected' : ''}>Random UUID</option>
                <option value="counter" ${store.stressConfig.keyPattern === 'counter' ? 'selected' : ''}>Sequential Counter (key-1, key-2)</option>
                <option value="none" ${store.stressConfig.keyPattern === 'none' ? 'selected' : ''}>Null Key (Round Robin Partitions)</option>
              </select>
            </div>
          </div>

          <div class="form-group" id="stress-custom-payload-group" style="display: ${store.stressConfig.payloadType === 'custom' ? 'block' : 'none'};">
            <label class="form-label">Custom Payload Template</label>
            <textarea class="textarea" id="stress-custom-payload" placeholder='{"event": "telemetry", "status": "active"}'>${store.stressConfig.customPayload}</textarea>
          </div>
        </div>
      </div>

      <!-- SINGLE MESSAGE PRODUCER PANEL -->
      <div id="tab-single-panel" style="display: none;" class="card">
        <div class="card-title">Single Record Publisher</div>

        <div class="grid-2">
          <div class="form-group">
            <label class="form-label">Target Topic</label>
            <select class="select" id="single-topic">
              ${store.topics.map(t => `<option value="${t.name}" ${t.name === (store.selectedTopic || '') ? 'selected' : ''}>${t.name}</option>`).join('')}
            </select>
          </div>
          <div class="form-group">
            <label class="form-label">Target Partition (-1 = Auto Partition)</label>
            <input type="number" class="input" id="single-partition" value="-1" />
          </div>
        </div>

        <div class="grid-2">
          <div class="form-group">
            <label class="form-label">Record Key</label>
            <input class="input" id="single-key" placeholder="Optional key" />
          </div>
          <div class="form-group">
            <label class="form-label">Key Format</label>
            <select class="select" id="single-key-format">
              <option value="string" selected>Plain String</option>
              <option value="hex">Hex Encoded</option>
              <option value="base64">Base64 Encoded</option>
            </select>
          </div>
        </div>

        <div class="form-group">
          <label class="form-label">Value Format</label>
          <select class="select" id="single-val-format">
            <option value="string" selected>Plain String / Text</option>
            <option value="json">JSON</option>
            <option value="hex">Hex Bytes (e.g. 4500003c1c464000...)</option>
            <option value="base64">Base64 Binary</option>
          </select>
        </div>

        <div class="form-group">
          <label class="form-label">Payload Value</label>
          <textarea class="textarea" id="single-value" style="min-height: 120px;" placeholder="Enter message payload..."></textarea>
        </div>

        <div style="display: flex; justify-content: flex-end; margin-top: 10px;">
          <button class="btn btn-primary" id="btn-single-send">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>
            Publish Record
          </button>
        </div>

        <div id="single-send-result" style="margin-top: 14px; display: none;"></div>
      </div>
    </div>
  `;

  // Tab switching
  const tabs = container.querySelectorAll('.btn-prod-tab');
  const stressPanel = container.querySelector('#tab-stress-panel') as HTMLElement;
  const singlePanel = container.querySelector('#tab-single-panel') as HTMLElement;

  tabs.forEach(tab => {
    tab.addEventListener('click', (e) => {
      tabs.forEach(t => t.classList.remove('active'));
      const target = e.currentTarget as HTMLElement;
      target.classList.add('active');
      const which = target.getAttribute('data-tab');
      stressPanel.style.display = which === 'stress' ? 'flex' : 'none';
      singlePanel.style.display = which === 'single' ? 'block' : 'none';
    });
  });

  // Payload type toggle & sync
  const payloadTypeSelect = container.querySelector('#stress-payload-type') as HTMLSelectElement;
  const customPayloadGroup = container.querySelector('#stress-custom-payload-group') as HTMLElement;
  payloadTypeSelect.addEventListener('change', () => {
    customPayloadGroup.style.display = payloadTypeSelect.value === 'custom' ? 'block' : 'none';
    store.updateStressConfig({ payloadType: payloadTypeSelect.value });
  });

  // Attach live persistence listeners to all stress parameters so they stick
  const inputTopic = container.querySelector('#stress-topic') as HTMLSelectElement;
  const inputTopicManual = container.querySelector('#stress-topic-manual') as HTMLInputElement;
  const inputConcurrency = container.querySelector('#stress-concurrency') as HTMLInputElement;
  const inputRate = container.querySelector('#stress-rate') as HTMLInputElement;
  const inputDuration = container.querySelector('#stress-duration') as HTMLInputElement;
  const inputSize = container.querySelector('#stress-payload-size') as HTMLInputElement;
  const inputKeyPattern = container.querySelector('#stress-key-pattern') as HTMLSelectElement;
  const inputCustomPayload = container.querySelector('#stress-custom-payload') as HTMLTextAreaElement;

  inputTopic?.addEventListener('change', () => store.updateStressConfig({ topic: inputTopic.value }));
  inputTopicManual?.addEventListener('input', () => store.updateStressConfig({ topicManual: inputTopicManual.value }));
  inputConcurrency?.addEventListener('input', () => store.updateStressConfig({ concurrency: parseInt(inputConcurrency.value, 10) || 1 }));
  inputRate?.addEventListener('input', () => store.updateStressConfig({ targetRate: parseInt(inputRate.value, 10) || 0 }));
  inputDuration?.addEventListener('input', () => store.updateStressConfig({ durationSeconds: parseInt(inputDuration.value, 10) || 0 }));
  inputSize?.addEventListener('input', () => store.updateStressConfig({ messageSizeBytes: parseInt(inputSize.value, 10) || 256 }));
  inputKeyPattern?.addEventListener('change', () => store.updateStressConfig({ keyPattern: inputKeyPattern.value }));
  inputCustomPayload?.addEventListener('input', () => store.updateStressConfig({ customPayload: inputCustomPayload.value }));

  // Helper to attach Start / Stop handlers dynamically without re-rendering the whole view
  function attachStressActionHandlers() {
    const startBtn = container.querySelector('#btn-stress-start');
    const stopBtn = container.querySelector('#btn-stress-stop');
    const btnContainer = container.querySelector('#prod-action-btn-container') as HTMLElement;

    startBtn?.addEventListener('click', async () => {
      const topicManual = inputTopicManual?.value.trim() || '';
      const topicSelect = inputTopic?.value || '';
      const topic = topicManual || topicSelect;

      if (!topic) {
        showToast('Please specify a target topic', 'error');
        return;
      }

      // Read current form values
      const concurrency = parseInt(inputConcurrency.value, 10) || 4;
      const targetRate = parseInt(inputRate.value, 10) || 0;
      const durationSeconds = parseInt(inputDuration.value, 10) || 0;
      const messageSizeBytes = parseInt(inputSize.value, 10) || 256;
      const payloadType = payloadTypeSelect.value;
      const customPayload = inputCustomPayload.value;
      const keyPattern = inputKeyPattern.value;

      // Save parameters permanently
      store.updateStressConfig({
        topic,
        topicManual,
        concurrency,
        targetRate,
        durationSeconds,
        messageSizeBytes,
        payloadType,
        customPayload,
        keyPattern,
      });

      const cfg: StressTestConfig = {
        topic: topic,
        concurrency: concurrency,
        targetRate: targetRate,
        durationSeconds: durationSeconds,
        messageSizeBytes: messageSizeBytes,
        payloadType: payloadType,
        customPayload: customPayload,
        kaitaiFormatId: '',
        keyPattern: keyPattern,
        compression: 'none',
      };

      try {
        await KafkaService.StartStressTest(cfg);
        store.stressMetrics.active = true;
        showToast(`Stress test launched on ${topic}!`, 'success');

        // Update UI in-place (no full-page wipe)
        const dot = container.querySelector('#prod-status-dot');
        const badge = container.querySelector('#prod-status-badge');
        const card = container.querySelector('#prod-telemetry-card') as HTMLElement;
        if (dot) dot.className = 'status-dot connected';
        if (badge) badge.innerText = 'RUNNING';
        if (card) card.style.borderColor = 'var(--accent-cyan)';

        btnContainer.innerHTML = `
          <button class="btn btn-danger" id="btn-stress-stop">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="6" y="6" width="12" height="12"/></svg>
            Halt Stress Test
          </button>
        `;
        attachStressActionHandlers();
      } catch (err: any) {
        showToast(`Stress test error: ${err.message || String(err)}`, 'error');
      }
    });

    stopBtn?.addEventListener('click', async () => {
      try {
        await KafkaService.StopStressTest();
        store.stressMetrics.active = false;
        showToast('Stress test halted', 'info');

        // Update UI in-place (no full-page wipe)
        const dot = container.querySelector('#prod-status-dot');
        const badge = container.querySelector('#prod-status-badge');
        const card = container.querySelector('#prod-telemetry-card') as HTMLElement;
        if (dot) dot.className = 'status-dot disconnected';
        if (badge) badge.innerText = 'STOPPED';
        if (card) card.style.borderColor = 'var(--border-subtle)';

        btnContainer.innerHTML = `
          <button class="btn btn-primary" id="btn-stress-start">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="5 3 19 12 5 21 5 3"/></svg>
            Launch Load Test
          </button>
        `;
        attachStressActionHandlers();
      } catch (err: any) {
        showToast(`Error stopping stress test: ${err.message}`, 'error');
      }
    });
  }

  attachStressActionHandlers();

  // Send single message
  container.querySelector('#btn-single-send')?.addEventListener('click', async () => {
    const resBox = container.querySelector('#single-send-result') as HTMLElement;
    resBox.style.display = 'block';
    resBox.innerHTML = '<div style="color: var(--accent-cyan); font-size: 13px;">Publishing record...</div>';

    const req: ProduceRequest = {
      topic: (container.querySelector('#single-topic') as HTMLSelectElement).value,
      partition: parseInt((container.querySelector('#single-partition') as HTMLInputElement).value, 10),
      key: (container.querySelector('#single-key') as HTMLInputElement).value,
      keyFormat: (container.querySelector('#single-key-format') as HTMLSelectElement).value,
      value: (container.querySelector('#single-value') as HTMLTextAreaElement).value,
      valueFormat: (container.querySelector('#single-val-format') as HTMLSelectElement).value,
      headers: {},
      kaitaiFormatId: '',
    };

    try {
      const resp = await KafkaService.Produce(req);
      if (resp && resp.success) {
        resBox.innerHTML = `
          <div class="metric-box" style="border-color: var(--accent-emerald);">
            <div style="color: var(--accent-emerald); font-weight: 600;">✓ Record published successfully!</div>
            <div style="font-size: 12px; color: var(--text-secondary); margin-top: 4px;">
              Topic: <b>${resp.topic}</b> | Partition: <b>${resp.partition}</b> | Offset: <b>${resp.offset}</b>
            </div>
          </div>
        `;
        showToast(`Published to ${resp.topic} [P:${resp.partition}, Off:${resp.offset}]`, 'success');
      }
    } catch (err: any) {
      resBox.innerHTML = `
        <div class="metric-box" style="border-color: var(--accent-rose);">
          <div style="color: var(--accent-rose); font-weight: 600;">✗ Publish failed</div>
          <div style="font-size: 12px; color: var(--text-secondary); margin-top: 4px;">${err.message || String(err)}</div>
        </div>
      `;
    }
  });

  return container;
}
