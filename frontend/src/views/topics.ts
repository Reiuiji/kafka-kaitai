import { KafkaService, KaitaiService } from '../../bindings/github.com/okuu/kafka_kaitai/services';
import type { KafkaRecord, TopicDetail } from '../../bindings/github.com/okuu/kafka_kaitai/models';
import { store } from '../store';
import { showToast } from '../components/toast';
import { renderHexViewer, base64ToUint8 } from '../components/hexviewer';
import { renderTreeView } from '../components/treeview';

export function renderTopics(): HTMLElement {
  const container = document.createElement('div');
  container.style.height = '100%';

  let currentRecords: KafkaRecord[] = [];
  let activeMainTab: 'consumer' | 'management' = 'consumer';
  let activeConsumerMode = 'tail';
  let currentTopicDetail: TopicDetail | null = null;
  let configSearchQuery = '';
  let showOverriddenOnly = false;

  container.innerHTML = `
    <div style="display: flex; height: 100%; gap: 20px;">
      <!-- Left: Topics List Sidebar -->
      <div class="card" style="width: 290px; display: flex; flex-direction: column; padding: 16px; flex-shrink: 0;">
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
          <span style="font-weight: 700; font-size: 14px; color: var(--text-primary);" id="topics-count-heading">
            Topics (${store.topics.length})
          </span>
          <div style="display: flex; gap: 6px;">
            <button class="btn btn-primary btn-sm" id="btn-open-create-topic" title="Create New Topic" style="padding: 4px 8px; font-weight: 600;">
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
              New
            </button>
            <button class="btn btn-secondary btn-sm" id="btn-refresh-topics" title="Refresh topics list" style="padding: 4px 7px;">
              <svg id="refresh-spinner-icon" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></svg>
            </button>
          </div>
        </div>

        <input class="input" id="topic-search" placeholder="Filter topics..." style="font-size: 12px; padding: 7px 10px; margin-bottom: 12px;" />

        <div id="topics-list" style="overflow-y: auto; flex: 1; display: flex; flex-direction: column; gap: 4px;">
          <!-- Topic items rendered by renderTopicsList() -->
        </div>
      </div>

      <!-- Right: Main Consumer & Management Workspace -->
      <div style="flex: 1; display: flex; flex-direction: column; gap: 16px; overflow-y: auto;">
        <!-- Top Workspace Bar & Tabs -->
        <div class="card" style="padding: 14px 18px;">
          <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 12px;">
            <div style="display: flex; align-items: center; gap: 12px;">
              <div>
                <div style="display: flex; align-items: center; gap: 8px;">
                  <span id="header-topic-name" style="font-weight: 700; font-size: 16px; font-family: var(--font-mono); color: var(--text-primary);">
                    ${store.selectedTopic ? store.selectedTopic : 'Select a Topic'}
                  </span>
                  <span id="header-topic-badges"></span>
                </div>
                <div style="font-size: 12px; color: var(--text-secondary); margin-top: 2px;">
                  Kafka Topic Operations & Inspection
                </div>
              </div>
            </div>

            <!-- Top Level Workspace Tabs -->
            <div style="display: flex; background: rgba(15, 23, 42, 0.9); border-radius: var(--radius-md); padding: 4px; border: 1px solid var(--border-medium); gap: 4px;">
              <button class="btn btn-mode active" id="tab-btn-consumer" data-main-tab="consumer">
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="margin-right: 4px;"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
                Consumer Workbench
              </button>
              <button class="btn btn-mode" id="tab-btn-management" data-main-tab="management">
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="margin-right: 4px;"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>
                Topic Config & Partitions
              </button>
            </div>
          </div>
        </div>

        <!-- VIEW 1: Consumer Workbench -->
        <div id="view-consumer-workbench" style="display: flex; flex-direction: column; gap: 16px; flex: 1;">
          <!-- Consumer Mode Controls Card -->
          <div class="card" style="padding: 16px;">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
              <span style="font-size: 13px; font-weight: 600; color: var(--text-secondary);">Consumer Mode:</span>
              <div style="display: flex; background: rgba(15, 23, 42, 0.8); border-radius: var(--radius-md); padding: 4px; border: 1px solid var(--border-medium); gap: 4px;">
                <button class="btn btn-mode active" data-cons-mode="tail">Tail Last N</button>
                <button class="btn btn-mode" data-cons-mode="benchmark">Benchmark (Drop)</button>
                <button class="btn btn-mode" data-cons-mode="store">Store to File</button>
              </div>
            </div>

            <!-- Tail Controls -->
            <div id="panel-tail-controls" style="display: flex; align-items: center; gap: 12px; flex-wrap: wrap;">
              <span style="font-size: 12px; color: var(--text-secondary);">Fetch count:</span>
              <select class="select select-sm" id="tail-count" style="width: 110px;">
                <option value="10">Last 10</option>
                <option value="25" selected>Last 25</option>
                <option value="50">Last 50</option>
                <option value="100">Last 100</option>
              </select>

              <span style="font-size: 12px; color: var(--text-secondary); margin-left: 8px;">Auto-decode Kaitai:</span>
              <select class="select select-sm" id="tail-kaitai-format" style="width: 190px;">
                <option value="">None (Raw Binary)</option>
                ${store.formats.map(f => `<option value="${f.id}">${f.name}</option>`).join('')}
              </select>

              <button class="btn btn-primary btn-sm" id="btn-tail-fetch" style="margin-left: auto;">
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="7 13 12 18 17 13"/><polyline points="7 6 12 11 17 6"/></svg>
                Peek Latest Messages
              </button>
            </div>

            <!-- Benchmark / Drop Mode Controls -->
            <div id="panel-benchmark-controls" style="display: none; align-items: center; justify-content: space-between;">
              <div style="display: flex; align-items: center; gap: 12px;">
                <span class="status-dot ${store.consumerMetrics.active ? 'connected' : 'disconnected'}"></span>
                <span style="font-size: 13px;">
                  ${store.consumerMetrics.active ? 'Consumer Stress Benchmark Running' : 'Benchmark Mode (Drops records instantly to measure max Kafka ingress speed)'}
                </span>
              </div>

              <div style="display: flex; gap: 10px;">
                ${store.consumerMetrics.active ? `
                  <button class="btn btn-danger btn-sm" id="btn-stop-benchmark">Stop Benchmark</button>
                ` : `
                  <button class="btn btn-success btn-sm" id="btn-start-benchmark">Start Drop Benchmark</button>
                `}
              </div>
            </div>

            <!-- Store to File Controls -->
            <div id="panel-store-controls" style="display: none; flex-direction: column; gap: 10px;">
              <div class="grid-2">
                <div class="form-group" style="margin: 0;">
                  <label class="form-label">Destination File Path</label>
                  <input class="input" id="store-filepath" placeholder="/tmp/kafka_dump.jsonl" value="/tmp/kafka_dump.jsonl" style="font-size: 12px;" />
                </div>
                <div class="form-group" style="margin: 0;">
                  <label class="form-label">Dump Format</label>
                  <select class="select select-sm" id="store-format">
                    <option value="jsonl">JSON Lines (Base64 key & payload)</option>
                    <option value="csv_hex">CSV (Hex encoded)</option>
                    <option value="raw">Raw Binary Chunks</option>
                  </select>
                </div>
              </div>

              <div style="display: flex; justify-content: flex-end; margin-top: 6px;">
                ${store.consumerMetrics.active ? `
                  <button class="btn btn-danger btn-sm" id="btn-stop-store">Stop Storing</button>
                ` : `
                  <button class="btn btn-primary btn-sm" id="btn-start-store">Start Storing to File</button>
                `}
              </div>
            </div>
          </div>

          <!-- Messages List & Split Inspector -->
          <div style="display: grid; grid-template-columns: 320px 1fr; gap: 16px; flex: 1; min-height: 420px;">
            <!-- Records Table / List -->
            <div class="card" style="padding: 14px; display: flex; flex-direction: column;">
              <div style="font-weight: 600; font-size: 13px; margin-bottom: 8px; display: flex; justify-content: space-between;">
                <span>Records</span>
                <span id="record-count-badge" style="color: var(--text-muted); font-size: 11px;">0 loaded</span>
              </div>

              <div id="records-list" style="overflow-y: auto; flex: 1; display: flex; flex-direction: column; gap: 6px;">
                <div style="color: var(--text-muted); font-size: 12px; text-align: center; margin-top: 40px;">
                  Click "Peek Latest Messages" to load records.
                </div>
              </div>
            </div>

            <!-- Message Detail / Hex / Kaitai Inspector -->
            <div class="card" style="padding: 16px; display: flex; flex-direction: column; gap: 14px;" id="record-detail-panel">
              <div style="color: var(--text-muted); font-size: 13px; text-align: center; margin: auto;">
                Select a message on the left to view Hex Dump and Kaitai Struct tree.
              </div>
            </div>
          </div>
        </div>

        <!-- VIEW 2: Topic Config & Management Panel -->
        <div id="view-topic-management" style="display: none; flex-direction: column; gap: 16px; flex: 1;">
          <!-- Actions & Overview Header -->
          <div class="card" style="padding: 16px; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 12px;">
            <div style="display: flex; gap: 12px; align-items: center;">
              <div id="mgmt-stat-chips" style="display: flex; gap: 8px;">
                <!-- Filled dynamically -->
              </div>
            </div>

            <div style="display: flex; gap: 10px;">
              <button class="btn btn-secondary btn-sm" id="btn-refresh-mgmt-detail">
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></svg>
                Refresh Details
              </button>
              <button class="btn btn-primary btn-sm" id="btn-mgmt-produce">
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>
                Produce to Topic
              </button>
              <button class="btn btn-danger btn-sm" id="btn-delete-topic">
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>
                Delete Topic
              </button>
            </div>
          </div>

          <!-- Partitions Breakdown Table -->
          <div class="card" style="padding: 16px;">
            <div style="font-weight: 600; font-size: 14px; margin-bottom: 12px; display: flex; justify-content: space-between; align-items: center;">
              <span>Topic Partitions (<span id="mgmt-partitions-count">0</span>)</span>
            </div>
            <div class="table-container">
              <table class="table">
                <thead>
                  <tr>
                    <th>Partition ID</th>
                    <th>Leader Broker</th>
                    <th>Replicas</th>
                    <th>In-Sync Replicas (ISR)</th>
                    <th>Status</th>
                  </tr>
                </thead>
                <tbody id="mgmt-partitions-tbody">
                  <tr><td colspan="5" style="text-align: center; color: var(--text-muted);">Loading partition metadata...</td></tr>
                </tbody>
              </table>
            </div>
          </div>

          <!-- Topic Broker Configurations Table -->
          <div class="card" style="padding: 16px; flex: 1; display: flex; flex-direction: column;">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; flex-wrap: wrap; gap: 10px;">
              <div style="display: flex; align-items: center; gap: 10px;">
                <span style="font-weight: 600; font-size: 14px;">Broker Configurations</span>
                <span id="mgmt-configs-count" class="badge badge-gray">0 entries</span>
              </div>

              <div style="display: flex; align-items: center; gap: 10px;">
                <label style="display: flex; align-items: center; gap: 6px; font-size: 12px; color: var(--text-secondary); cursor: pointer;">
                  <input type="checkbox" id="toggle-overridden-only" style="cursor: pointer;" />
                  Show Overridden Only
                </label>
                <input class="input" id="mgmt-config-search" placeholder="Filter configs (e.g. retention, cleanup)..." style="font-size: 12px; padding: 5px 10px; width: 220px;" />
              </div>
            </div>

            <div class="table-container" style="max-height: 420px; overflow-y: auto;">
              <table class="table">
                <thead>
                  <tr>
                    <th style="width: 35%;">Configuration Key</th>
                    <th style="width: 35%;">Current Value</th>
                    <th style="width: 15%;">Source</th>
                    <th style="width: 15%; text-align: right;">Action</th>
                  </tr>
                </thead>
                <tbody id="mgmt-configs-tbody">
                  <tr><td colspan="4" style="text-align: center; color: var(--text-muted);">Select a topic to view configuration.</td></tr>
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Create Topic Modal Dialog -->
    <div id="modal-create-topic" class="modal-overlay" style="display: none;">
      <div class="modal-box">
        <div class="modal-header">
          <div class="modal-title">
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="var(--accent-cyan)" stroke-width="2.5"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
            Create New Kafka Topic
          </div>
          <button class="modal-close-btn" id="btn-close-create-modal">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
          </button>
        </div>

        <div class="modal-body">
          <div class="form-group">
            <label class="form-label">Topic Name <span style="color: var(--accent-rose);">*</span></label>
            <input class="input" id="create-topic-name" placeholder="e.g. telemetry.events or app.orders" />
            <span style="font-size: 11px; color: var(--text-muted);">Use letters, numbers, dots (.), underscores (_), or dashes (-).</span>
          </div>

          <div class="grid-2">
            <div class="form-group">
              <label class="form-label">Number of Partitions</label>
              <input type="number" class="input" id="create-topic-partitions" value="1" min="1" max="1000" />
            </div>
            <div class="form-group">
              <label class="form-label">Replication Factor</label>
              <input type="number" class="input" id="create-topic-rf" value="1" min="1" max="10" />
            </div>
          </div>

          <div style="font-size: 12px; font-weight: 600; color: var(--accent-cyan); text-transform: uppercase; letter-spacing: 0.05em; margin-top: 4px;">
            Configuration Presets
          </div>

          <div class="grid-2">
            <div class="form-group">
              <label class="form-label">Cleanup Policy (cleanup.policy)</label>
              <select class="select select-sm" id="create-cleanup-policy">
                <option value="delete" selected>delete (Discard old logs after retention)</option>
                <option value="compact">compact (Retain latest record per key)</option>
                <option value="compact,delete">compact,delete (Compact and discard expired)</option>
              </select>
            </div>

            <div class="form-group">
              <label class="form-label">Retention Time (retention.ms)</label>
              <select class="select select-sm" id="create-retention-ms">
                <option value="604800000" selected>7 Days (604,800,000 ms)</option>
                <option value="86400000">1 Day (86,400,000 ms)</option>
                <option value="259200000">3 Days (259,200,000 ms)</option>
                <option value="1209600000">14 Days (1,209,600,000 ms)</option>
                <option value="2592000000">30 Days (2,592,000,000 ms)</option>
                <option value="-1">Unlimited (-1)</option>
                <option value="custom">Custom value...</option>
              </select>
              <input class="input" id="create-retention-ms-custom" placeholder="Milliseconds" style="display: none; margin-top: 6px; font-size: 12px;" />
            </div>
          </div>

          <div class="grid-2">
            <div class="form-group">
              <label class="form-label">Max Retention Size (retention.bytes)</label>
              <select class="select select-sm" id="create-retention-bytes">
                <option value="-1" selected>Unlimited (-1)</option>
                <option value="104857600">100 MB</option>
                <option value="524288000">500 MB</option>
                <option value="1073741824">1 GB</option>
                <option value="10737418240">10 GB</option>
                <option value="custom">Custom bytes...</option>
              </select>
              <input class="input" id="create-retention-bytes-custom" placeholder="Bytes" style="display: none; margin-top: 6px; font-size: 12px;" />
            </div>

            <div class="form-group">
              <label class="form-label">Max Message Size (max.message.bytes)</label>
              <select class="select select-sm" id="create-max-message-bytes">
                <option value="1048576" selected>1 MB (Default 1,048,576 bytes)</option>
                <option value="5242880">5 MB (5,242,880 bytes)</option>
                <option value="10485760">10 MB (10,485,760 bytes)</option>
                <option value="20971520">20 MB (20,971,520 bytes)</option>
                <option value="custom">Custom bytes...</option>
              </select>
              <input class="input" id="create-max-message-bytes-custom" placeholder="Bytes" style="display: none; margin-top: 6px; font-size: 12px;" />
            </div>
          </div>

          <!-- Dynamic Custom Config Entries -->
          <div>
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
              <span style="font-size: 12px; font-weight: 600; color: var(--text-secondary);">Custom Configuration Properties</span>
              <button type="button" class="btn btn-secondary btn-sm" id="btn-add-custom-config" style="padding: 2px 8px; font-size: 11px;">
                + Add Property
              </button>
            </div>
            <div id="custom-configs-container" style="display: flex; flex-direction: column; gap: 8px;"></div>
          </div>
        </div>

        <div class="modal-footer">
          <button type="button" class="btn btn-secondary" id="btn-cancel-create-topic">Cancel</button>
          <button type="button" class="btn btn-primary" id="btn-submit-create-topic">
            Create Topic
          </button>
        </div>
      </div>
    </div>

    <!-- Edit Config Modal Dialog -->
    <div id="modal-edit-config" class="modal-overlay" style="display: none;">
      <div class="modal-box" style="max-width: 480px;">
        <div class="modal-header">
          <div class="modal-title">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="var(--accent-cyan)" stroke-width="2"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>
            Edit Topic Configuration
          </div>
          <button class="modal-close-btn" id="btn-close-edit-config">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
          </button>
        </div>

        <div class="modal-body">
          <div class="form-group">
            <label class="form-label">Configuration Key</label>
            <input class="input" id="edit-config-key" readonly style="background: rgba(0,0,0,0.3); font-family: var(--font-mono);" />
          </div>
          <div class="form-group">
            <label class="form-label">New Value</label>
            <input class="input" id="edit-config-value" placeholder="Enter value, or clear to reset to broker default" style="font-family: var(--font-mono);" />
            <span style="font-size: 11px; color: var(--text-muted);">Leave empty to delete override and restore the cluster default value.</span>
          </div>
        </div>

        <div class="modal-footer">
          <button type="button" class="btn btn-secondary" id="btn-cancel-edit-config">Cancel</button>
          <button type="button" class="btn btn-primary" id="btn-save-edit-config">Apply Configuration</button>
        </div>
      </div>
    </div>
  `;

  // ---------------------------------------------------------------------------
  // Topic List & Search Filter Rendering
  // ---------------------------------------------------------------------------
  function renderTopicsList() {
    const listContainer = container.querySelector('#topics-list') as HTMLElement;
    const countHeading = container.querySelector('#topics-count-heading') as HTMLElement;
    const filterText = (container.querySelector('#topic-search') as HTMLInputElement)?.value.toLowerCase().trim() || '';

    const filtered = store.topics.filter(t => t.name.toLowerCase().includes(filterText));
    countHeading.innerText = `Topics (${store.topics.length})`;

    if (store.topics.length === 0) {
      listContainer.innerHTML = `
        <div style="color: var(--text-muted); font-size: 12px; text-align: center; margin-top: 24px;">
          ${store.connection.connected ? 'No topics found in cluster' : 'Connect to cluster first'}
        </div>
      `;
      return;
    }

    if (filtered.length === 0) {
      listContainer.innerHTML = `
        <div style="color: var(--text-muted); font-size: 12px; text-align: center; margin-top: 24px;">
          No topics match "${filterText}"
        </div>
      `;
      return;
    }

    listContainer.innerHTML = filtered.map(t => {
      const isSelected = t.name === store.selectedTopic;
      return `
        <div class="topic-item ${isSelected ? 'active' : ''}" data-topic="${t.name}" style="
          padding: 8px 10px; border-radius: var(--radius-md); font-size: 12px; font-family: var(--font-mono);
          cursor: pointer; display: flex; justify-content: space-between; align-items: center; transition: all 0.15s ease;
          background: ${isSelected ? 'rgba(6, 182, 212, 0.15)' : 'transparent'};
          border: 1px solid ${isSelected ? 'rgba(6, 182, 212, 0.4)' : 'transparent'};
          color: ${isSelected ? 'var(--accent-cyan)' : 'var(--text-secondary)'};
        ">
          <span style="overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-weight: ${isSelected ? '700' : '500'};">${t.name}</span>
          <span class="badge ${isSelected ? 'badge-cyan' : 'badge-gray'}" style="font-size: 10px; padding: 2px 6px;">P:${t.partitionsCount}</span>
        </div>
      `;
    }).join('');

    // Attach click events
    listContainer.querySelectorAll('.topic-item').forEach(item => {
      item.addEventListener('click', () => {
        const topic = item.getAttribute('data-topic') || '';
        selectTopic(topic);
      });
    });
  }

  function selectTopic(topicName: string) {
    store.selectedTopic = topicName;
    updateHeader();
    renderTopicsList();

    if (activeMainTab === 'management') {
      loadTopicManagementDetails();
    }
  }

  function updateHeader() {
    const headerName = container.querySelector('#header-topic-name') as HTMLElement;
    const headerBadges = container.querySelector('#header-topic-badges') as HTMLElement;
    const found = store.topics.find(t => t.name === store.selectedTopic);

    if (store.selectedTopic) {
      headerName.innerText = store.selectedTopic;
      if (found) {
        headerBadges.innerHTML = `
          <span class="badge badge-cyan">${found.partitionsCount} Partitions</span>
          <span class="badge badge-gray">RF: ${found.replicationFactor || 1}</span>
        `;
      } else {
        headerBadges.innerHTML = '';
      }
    } else {
      headerName.innerText = 'Select a Topic';
      headerBadges.innerHTML = '';
    }
  }

  // Live filter search
  container.querySelector('#topic-search')?.addEventListener('input', () => {
    renderTopicsList();
  });

  // Refresh topics button
  container.querySelector('#btn-refresh-topics')?.addEventListener('click', async () => {
    const icon = container.querySelector('#refresh-spinner-icon') as HTMLElement;
    icon?.classList.add('rotate-anim');
    try {
      const topics = await KafkaService.ListTopics();
      store.setTopics(topics || []);
      renderTopicsList();
      updateHeader();
      showToast(`Refreshed ${store.topics.length} topics from Kafka`, 'info');
      if (activeMainTab === 'management' && store.selectedTopic) {
        loadTopicManagementDetails();
      }
    } catch (err: any) {
      showToast(`Failed to refresh topics: ${err.message}`, 'error');
    } finally {
      icon?.classList.remove('rotate-anim');
    }
  });

  // ---------------------------------------------------------------------------
  // Main Tab Navigation: Consumer Workbench vs Topic Management
  // ---------------------------------------------------------------------------
  const tabBtnConsumer = container.querySelector('#tab-btn-consumer') as HTMLElement;
  const tabBtnMgmt = container.querySelector('#tab-btn-management') as HTMLElement;
  const viewConsumer = container.querySelector('#view-consumer-workbench') as HTMLElement;
  const viewMgmt = container.querySelector('#view-topic-management') as HTMLElement;

  tabBtnConsumer.addEventListener('click', () => {
    activeMainTab = 'consumer';
    tabBtnConsumer.classList.add('active');
    tabBtnMgmt.classList.remove('active');
    viewConsumer.style.display = 'flex';
    viewMgmt.style.display = 'none';
  });

  tabBtnMgmt.addEventListener('click', () => {
    activeMainTab = 'management';
    tabBtnMgmt.classList.add('active');
    tabBtnConsumer.classList.remove('active');
    viewConsumer.style.display = 'none';
    viewMgmt.style.display = 'flex';
    if (store.selectedTopic) {
      loadTopicManagementDetails();
    }
  });

  // ---------------------------------------------------------------------------
  // Topic Management & Config Logic
  // ---------------------------------------------------------------------------
  async function loadTopicManagementDetails() {
    if (!store.selectedTopic) return;

    const partitionsTbody = container.querySelector('#mgmt-partitions-tbody') as HTMLElement;
    const configsTbody = container.querySelector('#mgmt-configs-tbody') as HTMLElement;
    const partitionsCountEl = container.querySelector('#mgmt-partitions-count') as HTMLElement;
    const configsCountEl = container.querySelector('#mgmt-configs-count') as HTMLElement;
    const statChips = container.querySelector('#mgmt-stat-chips') as HTMLElement;

    partitionsTbody.innerHTML = `<tr><td colspan="5" style="text-align: center; color: var(--accent-cyan); padding: 20px;">Fetching partition metadata...</td></tr>`;
    configsTbody.innerHTML = `<tr><td colspan="4" style="text-align: center; color: var(--accent-cyan); padding: 20px;">Fetching broker configuration...</td></tr>`;

    try {
      const detail = await KafkaService.GetTopicDetail(store.selectedTopic);
      currentTopicDetail = detail;

      if (!detail) {
        partitionsTbody.innerHTML = `<tr><td colspan="5" style="text-align: center; color: var(--accent-rose);">Topic details not found</td></tr>`;
        return;
      }

      // Summary chips
      partitionsCountEl.innerText = `${detail.partitionsCount}`;
      configsCountEl.innerText = `${detail.configs.length} entries`;
      statChips.innerHTML = `
        <span class="badge badge-cyan" style="font-size: 12px; padding: 4px 10px;">${detail.partitionsCount} Partitions</span>
        <span class="badge badge-emerald" style="font-size: 12px; padding: 4px 10px;">RF: ${detail.replicationFactor}</span>
        <span class="badge badge-gray" style="font-size: 12px; padding: 4px 10px;">Cluster Node: ${store.connection.brokers[0] || 'localhost'}</span>
      `;

      // Render partitions table
      if (detail.partitions.length === 0) {
        partitionsTbody.innerHTML = `<tr><td colspan="5" style="text-align: center; color: var(--text-muted);">No partition details available</td></tr>`;
      } else {
        partitionsTbody.innerHTML = detail.partitions.map(p => {
          const isrSynced = p.isr.length >= p.replicas.length;
          return `
            <tr>
              <td style="font-family: var(--font-mono); font-weight: 600; color: var(--accent-cyan);">${p.id}</td>
              <td style="font-family: var(--font-mono);">Node ${p.leader}</td>
              <td style="font-family: var(--font-mono); color: var(--text-secondary);">[${p.replicas.join(', ')}]</td>
              <td style="font-family: var(--font-mono); color: var(--text-secondary);">[${p.isr.join(', ')}]</td>
              <td>
                <span class="badge ${isrSynced ? 'badge-emerald' : 'badge-amber'}">
                  ${isrSynced ? 'In Sync' : 'Under Replicated'}
                </span>
              </td>
            </tr>
          `;
        }).join('');
      }

      // Render broker configurations
      renderConfigTable();
    } catch (err: any) {
      showToast(`Failed to load topic details: ${err.message}`, 'error');
      partitionsTbody.innerHTML = `<tr><td colspan="5" style="text-align: center; color: var(--accent-rose);">Error: ${err.message}</td></tr>`;
      configsTbody.innerHTML = `<tr><td colspan="4" style="text-align: center; color: var(--accent-rose);">Error: ${err.message}</td></tr>`;
    }
  }

  function renderConfigTable() {
    if (!currentTopicDetail) return;
    const configsTbody = container.querySelector('#mgmt-configs-tbody') as HTMLElement;
    const configsCountEl = container.querySelector('#mgmt-configs-count') as HTMLElement;

    const query = configSearchQuery.toLowerCase().trim();
    let entries = currentTopicDetail.configs || [];

    if (showOverriddenOnly) {
      entries = entries.filter(c => !c.isDefault);
    }

    if (query) {
      entries = entries.filter(c => c.name.toLowerCase().includes(query) || (c.value && c.value.toLowerCase().includes(query)));
    }

    configsCountEl.innerText = `${entries.length} of ${currentTopicDetail.configs.length} shown`;

    if (entries.length === 0) {
      configsTbody.innerHTML = `
        <tr>
          <td colspan="4" style="text-align: center; color: var(--text-muted); padding: 24px;">
            ${showOverriddenOnly ? 'No overridden configurations found on this topic.' : 'No configurations match query.'}
          </td>
        </tr>
      `;
      return;
    }

    configsTbody.innerHTML = entries.map(c => {
      // Add human-friendly display annotations for common Kafka configs
      let formattedVal = c.value || '[empty]';
      if (c.name === 'retention.ms' && c.value) {
        const ms = parseInt(c.value, 10);
        if (ms === -1) formattedVal += ' (Unlimited)';
        else if (ms >= 86400000) formattedVal += ` (~${(ms / 86400000).toFixed(1)} days)`;
        else if (ms >= 3600000) formattedVal += ` (~${(ms / 3600000).toFixed(1)} hours)`;
      } else if ((c.name === 'retention.bytes' || c.name === 'segment.bytes' || c.name === 'max.message.bytes') && c.value) {
        const bytes = parseInt(c.value, 10);
        if (bytes === -1) formattedVal += ' (Unlimited)';
        else if (bytes >= 1073741824) formattedVal += ` (~${(bytes / 1073741824).toFixed(2)} GB)`;
        else if (bytes >= 1048576) formattedVal += ` (~${(bytes / 1048576).toFixed(1)} MB)`;
      }

      return `
        <tr>
          <td style="font-family: var(--font-mono); font-weight: 600; color: ${c.isDefault ? 'var(--text-primary)' : 'var(--accent-cyan)'};">
            ${c.name}
            ${!c.isDefault ? `<span class="badge badge-cyan" style="font-size: 9px; margin-left: 6px;">CUSTOM</span>` : ''}
          </td>
          <td style="font-family: var(--font-mono); color: ${c.isDefault ? 'var(--text-secondary)' : 'var(--text-primary)'}; word-break: break-all;">
            ${formattedVal}
          </td>
          <td>
            <span class="badge ${c.isDefault ? 'badge-gray' : 'badge-cyan'}" style="font-size: 10px;">
              ${c.source || (c.isDefault ? 'DEFAULT' : 'OVERRIDDEN')}
            </span>
          </td>
          <td style="text-align: right;">
            <button class="btn btn-secondary btn-sm btn-edit-config" data-key="${c.name}" data-val="${c.value || ''}" style="padding: 2px 8px; font-size: 11px;">
              Edit
            </button>
          </td>
        </tr>
      `;
    }).join('');

    // Wire up Edit buttons
    configsTbody.querySelectorAll('.btn-edit-config').forEach(btn => {
      btn.addEventListener('click', () => {
        const key = btn.getAttribute('data-key') || '';
        const val = btn.getAttribute('data-val') || '';
        openEditConfigModal(key, val);
      });
    });
  }

  // Management controls listeners
  container.querySelector('#mgmt-config-search')?.addEventListener('input', (e) => {
    configSearchQuery = (e.target as HTMLInputElement).value;
    renderConfigTable();
  });

  container.querySelector('#toggle-overridden-only')?.addEventListener('change', (e) => {
    showOverriddenOnly = (e.target as HTMLInputElement).checked;
    renderConfigTable();
  });

  container.querySelector('#btn-refresh-mgmt-detail')?.addEventListener('click', () => {
    loadTopicManagementDetails();
  });

  container.querySelector('#btn-mgmt-produce')?.addEventListener('click', () => {
    store.setView('producer');
  });

  // Delete Topic
  container.querySelector('#btn-delete-topic')?.addEventListener('click', async () => {
    if (!store.selectedTopic) return;
    const topicToDelete = store.selectedTopic;
    const confirmed = window.confirm(`Are you sure you want to delete topic "${topicToDelete}"?\n\nAll partitions, logs, and consumer offsets for this topic will be permanently removed.`);
    if (!confirmed) return;

    try {
      showToast(`Deleting topic "${topicToDelete}"...`, 'info');
      await KafkaService.DeleteTopic(topicToDelete);
      showToast(`Topic "${topicToDelete}" deleted successfully`, 'success');

      // Refresh topics list
      const topics = await KafkaService.ListTopics();
      store.setTopics(topics || []);
      if (store.topics.length > 0) {
        selectTopic(store.topics[0].name);
      } else {
        store.selectedTopic = '';
        updateHeader();
        renderTopicsList();
      }
    } catch (err: any) {
      showToast(`Failed to delete topic: ${err.message}`, 'error');
    }
  });

  // ---------------------------------------------------------------------------
  // Edit Topic Configuration Modal
  // ---------------------------------------------------------------------------
  const modalEditConfig = container.querySelector('#modal-edit-config') as HTMLElement;
  const editConfigKeyInput = container.querySelector('#edit-config-key') as HTMLInputElement;
  const editConfigValInput = container.querySelector('#edit-config-value') as HTMLInputElement;

  function openEditConfigModal(key: string, currentValue: string) {
    editConfigKeyInput.value = key;
    editConfigValInput.value = currentValue;
    modalEditConfig.style.display = 'flex';
  }

  function closeEditConfigModal() {
    modalEditConfig.style.display = 'none';
  }

  container.querySelector('#btn-close-edit-config')?.addEventListener('click', closeEditConfigModal);
  container.querySelector('#btn-cancel-edit-config')?.addEventListener('click', closeEditConfigModal);

  container.querySelector('#btn-save-edit-config')?.addEventListener('click', async () => {
    const key = editConfigKeyInput.value.trim();
    const val = editConfigValInput.value.trim();
    if (!key || !store.selectedTopic) return;

    try {
      showToast(`Updating config '${key}' on topic '${store.selectedTopic}'...`, 'info');
      await KafkaService.AlterTopicConfig(store.selectedTopic, key, val);
      showToast(`Configuration '${key}' updated!`, 'success');
      closeEditConfigModal();
      loadTopicManagementDetails();
    } catch (err: any) {
      showToast(`Failed to update config: ${err.message}`, 'error');
    }
  });

  // ---------------------------------------------------------------------------
  // Create Topic Modal & Dynamic Fields
  // ---------------------------------------------------------------------------
  const modalCreateTopic = container.querySelector('#modal-create-topic') as HTMLElement;
  const customConfigsContainer = container.querySelector('#custom-configs-container') as HTMLElement;

  function openCreateTopicModal() {
    modalCreateTopic.style.display = 'flex';
    (container.querySelector('#create-topic-name') as HTMLInputElement).value = '';
    (container.querySelector('#create-topic-partitions') as HTMLInputElement).value = '1';
    (container.querySelector('#create-topic-rf') as HTMLInputElement).value = '1';
    customConfigsContainer.innerHTML = '';
  }

  function closeCreateTopicModal() {
    modalCreateTopic.style.display = 'none';
  }

  container.querySelector('#btn-open-create-topic')?.addEventListener('click', openCreateTopicModal);
  container.querySelector('#btn-close-create-modal')?.addEventListener('click', closeCreateTopicModal);
  container.querySelector('#btn-cancel-create-topic')?.addEventListener('click', closeCreateTopicModal);

  // Preset custom input toggles
  const setupPresetToggle = (selectId: string, inputId: string) => {
    const sel = container.querySelector(`#${selectId}`) as HTMLSelectElement;
    const inp = container.querySelector(`#${inputId}`) as HTMLInputElement;
    sel?.addEventListener('change', () => {
      inp.style.display = sel.value === 'custom' ? 'block' : 'none';
    });
  };

  setupPresetToggle('create-retention-ms', 'create-retention-ms-custom');
  setupPresetToggle('create-retention-bytes', 'create-retention-bytes-custom');
  setupPresetToggle('create-max-message-bytes', 'create-max-message-bytes-custom');

  // Add custom config row
  container.querySelector('#btn-add-custom-config')?.addEventListener('click', () => {
    const row = document.createElement('div');
    row.className = 'custom-config-row';
    row.style.cssText = 'display: flex; gap: 8px; align-items: center;';
    row.innerHTML = `
      <input class="input config-prop-key" placeholder="Config key (e.g. min.insync.replicas)" style="flex: 1; font-size: 12px; font-family: var(--font-mono);" />
      <input class="input config-prop-val" placeholder="Value" style="flex: 1; font-size: 12px; font-family: var(--font-mono);" />
      <button type="button" class="btn btn-secondary btn-sm btn-remove-row" style="color: var(--accent-rose); padding: 6px 10px;">✕</button>
    `;
    row.querySelector('.btn-remove-row')?.addEventListener('click', () => row.remove());
    customConfigsContainer.appendChild(row);
  });

  // Submit Create Topic
  container.querySelector('#btn-submit-create-topic')?.addEventListener('click', async () => {
    const nameInput = container.querySelector('#create-topic-name') as HTMLInputElement;
    const partitionsInput = container.querySelector('#create-topic-partitions') as HTMLInputElement;
    const rfInput = container.querySelector('#create-topic-rf') as HTMLInputElement;
    const submitBtn = container.querySelector('#btn-submit-create-topic') as HTMLButtonElement;

    const name = nameInput.value.trim();
    if (!name) {
      showToast('Topic name is required', 'error');
      nameInput.focus();
      return;
    }

    const partitions = parseInt(partitionsInput.value, 10) || 1;
    const rf = parseInt(rfInput.value, 10) || 1;

    // Build configuration key-values
    const configs: Record<string, string> = {};

    // Cleanup policy
    const cleanupPolicy = (container.querySelector('#create-cleanup-policy') as HTMLSelectElement).value;
    if (cleanupPolicy) configs['cleanup.policy'] = cleanupPolicy;

    // Retention ms
    const retentionMsSel = (container.querySelector('#create-retention-ms') as HTMLSelectElement).value;
    if (retentionMsSel === 'custom') {
      const customMs = (container.querySelector('#create-retention-ms-custom') as HTMLInputElement).value.trim();
      if (customMs) configs['retention.ms'] = customMs;
    } else if (retentionMsSel) {
      configs['retention.ms'] = retentionMsSel;
    }

    // Retention bytes
    const retentionBytesSel = (container.querySelector('#create-retention-bytes') as HTMLSelectElement).value;
    if (retentionBytesSel === 'custom') {
      const customBytes = (container.querySelector('#create-retention-bytes-custom') as HTMLInputElement).value.trim();
      if (customBytes) configs['retention.bytes'] = customBytes;
    } else if (retentionBytesSel && retentionBytesSel !== '-1') {
      configs['retention.bytes'] = retentionBytesSel;
    }

    // Max message bytes
    const maxMsgSel = (container.querySelector('#create-max-message-bytes') as HTMLSelectElement).value;
    if (maxMsgSel === 'custom') {
      const customMax = (container.querySelector('#create-max-message-bytes-custom') as HTMLInputElement).value.trim();
      if (customMax) configs['max.message.bytes'] = customMax;
    } else if (maxMsgSel && maxMsgSel !== '1048576') {
      configs['max.message.bytes'] = maxMsgSel;
    }

    // Add any dynamic custom config rows
    customConfigsContainer.querySelectorAll('.custom-config-row').forEach(row => {
      const k = (row.querySelector('.config-prop-key') as HTMLInputElement)?.value.trim();
      const v = (row.querySelector('.config-prop-val') as HTMLInputElement)?.value.trim();
      if (k && v) {
        configs[k] = v;
      }
    });

    submitBtn.disabled = true;
    submitBtn.innerText = 'Creating...';

    try {
      showToast(`Creating topic '${name}' with ${partitions} partitions...`, 'info');
      await KafkaService.CreateTopic({
        name: name,
        partitions: partitions,
        replicationFactor: rf,
        configs: configs,
      });

      showToast(`Topic '${name}' created successfully!`, 'success');
      closeCreateTopicModal();

      // Refresh topics list from cluster
      const topics = await KafkaService.ListTopics();
      store.setTopics(topics || []);

      // Switch selection to new topic and open management view
      selectTopic(name);
      tabBtnMgmt.click();
    } catch (err: any) {
      showToast(`Failed to create topic: ${err.message || String(err)}`, 'error');
    } finally {
      submitBtn.disabled = false;
      submitBtn.innerText = 'Create Topic';
    }
  });

  // ---------------------------------------------------------------------------
  // Consumer Workbench Logic (Tailing, Benchmarking, Storing, Hex, Kaitai)
  // ---------------------------------------------------------------------------
  // Consumer mode button handlers
  container.querySelectorAll('.btn-mode[data-cons-mode]').forEach(btn => {
    btn.addEventListener('click', (e) => {
      container.querySelectorAll('.btn-mode[data-cons-mode]').forEach(b => b.classList.remove('active'));
      const target = e.currentTarget as HTMLElement;
      target.classList.add('active');
      activeConsumerMode = target.getAttribute('data-cons-mode') || 'tail';

      const tailP = container.querySelector('#panel-tail-controls') as HTMLElement;
      const benchP = container.querySelector('#panel-benchmark-controls') as HTMLElement;
      const storeP = container.querySelector('#panel-store-controls') as HTMLElement;

      tailP.style.display = activeConsumerMode === 'tail' ? 'flex' : 'none';
      benchP.style.display = activeConsumerMode === 'benchmark' ? 'flex' : 'none';
      storeP.style.display = activeConsumerMode === 'store' ? 'flex' : 'none';
    });
  });

  // Fetch / Tail records
  container.querySelector('#btn-tail-fetch')?.addEventListener('click', async () => {
    if (!store.selectedTopic) {
      showToast('Please select a topic first', 'error');
      return;
    }

    const count = parseInt((container.querySelector('#tail-count') as HTMLSelectElement).value, 10);
    const formatId = (container.querySelector('#tail-kaitai-format') as HTMLSelectElement).value;

    try {
      showToast(`Fetching last ${count} records from ${store.selectedTopic}...`, 'info');
      const records = await KafkaService.TailTopic({
        topic: store.selectedTopic,
        partition: -1,
        count: count,
        kaitaiFormatId: formatId,
      });

      currentRecords = records || [];
      renderRecordsList();
    } catch (err: any) {
      showToast(`Tail failed: ${err.message || String(err)}`, 'error');
    }
  });

  function renderRecordsList() {
    const listContainer = container.querySelector('#records-list') as HTMLElement;
    const badge = container.querySelector('#record-count-badge') as HTMLElement;
    badge.innerText = `${currentRecords.length} loaded`;

    if (currentRecords.length === 0) {
      listContainer.innerHTML = '<div style="color: var(--text-muted); font-size: 12px; text-align: center; margin-top: 40px;">No messages received from topic.</div>';
      return;
    }

    listContainer.innerHTML = currentRecords.map((r, idx) => `
      <div class="record-row" data-idx="${idx}" style="
        padding: 8px 10px; border-radius: var(--radius-sm); border: 1px solid var(--border-subtle);
        background: rgba(255, 255, 255, 0.02); cursor: pointer; display: flex; flex-direction: column; gap: 4px; font-size: 12px;
        transition: all 0.15s ease;
      ">
        <div style="display: flex; justify-content: space-between; font-family: var(--font-mono); color: var(--accent-cyan);">
          <span>Off: ${r.offset} (P:${r.partition})</span>
          <span style="color: var(--text-muted); font-size: 10px;">${new Date(r.timestamp).toLocaleTimeString()}</span>
        </div>
        <div style="color: var(--text-secondary); font-family: var(--font-mono); font-size: 11px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">
          ${r.key ? `Key: ${r.key} | ` : ''}Val: ${r.valueHex ? r.valueHex.substring(0, 24) + '...' : '[empty]'}
        </div>
      </div>
    `).join('');

    listContainer.querySelectorAll('.record-row').forEach(row => {
      row.addEventListener('click', () => {
        const idx = parseInt(row.getAttribute('data-idx') || '0', 10);
        showRecordDetail(currentRecords[idx]);
      });
    });

    // Auto-select first
    if (currentRecords.length > 0) {
      showRecordDetail(currentRecords[0]);
    }
  }

  function showRecordDetail(rec: KafkaRecord) {
    const detailPanel = container.querySelector('#record-detail-panel') as HTMLElement;
    detailPanel.innerHTML = `
      <div style="display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--border-subtle); padding-bottom: 10px; flex-wrap: wrap; gap: 8px;">
        <div>
          <div style="font-weight: 600; font-size: 14px;">Offset ${rec.offset} on Partition ${rec.partition}</div>
          <div style="color: var(--text-muted); font-size: 11px; font-family: var(--font-mono);">
            Topic: ${rec.topic} | Time: ${new Date(rec.timestamp).toISOString()}
          </div>
        </div>

        <div style="display: flex; align-items: center; gap: 8px;">
          <select class="select select-sm" id="detail-kaitai-select" style="width: 180px;">
            <option value="">Select Kaitai Parser</option>
            ${store.formats.map(f => `<option value="${f.id}">${f.name}</option>`).join('')}
          </select>
          <button class="btn btn-primary btn-sm" id="btn-decode-record">
            Decode Binary
          </button>
        </div>
      </div>

      <!-- Hex Dump Section -->
      <div>
        <div style="font-size: 12px; font-weight: 600; color: var(--text-secondary); margin-bottom: 6px;">
          Hex & ASCII Dump (${rec.valueBase64 ? Math.floor(rec.valueBase64.length * 0.75) : 0} bytes)
        </div>
        <div id="hex-view-container"></div>
      </div>

      <!-- Decoded Kaitai Struct Section -->
      <div style="flex: 1; display: flex; flex-direction: column;">
        <div style="font-size: 12px; font-weight: 600; color: var(--text-secondary); margin-bottom: 6px; display: flex; justify-content: space-between;">
          <span>Decoded Kaitai Struct</span>
          <span id="kaitai-parse-time" style="color: var(--accent-emerald); font-size: 11px; font-family: var(--font-mono);"></span>
        </div>
        <div id="tree-view-container" style="flex: 1;">
          <div style="color: var(--text-muted); font-size: 12px; padding: 12px; background: rgba(0,0,0,0.2); border-radius: var(--radius-md);">
            Select a Kaitai parser above and click "Decode Binary" to parse into structured fields.
          </div>
        </div>
      </div>
    `;

    // Render hex dump
    const rawBytes = base64ToUint8(rec.valueBase64);
    const hexContainer = detailPanel.querySelector('#hex-view-container') as HTMLElement;
    hexContainer.appendChild(renderHexViewer(rawBytes));

    // If already has parsed JSON
    if (rec.parsedJson) {
      try {
        const parsedObj = JSON.parse(rec.parsedJson);
        const treeContainer = detailPanel.querySelector('#tree-view-container') as HTMLElement;
        treeContainer.innerHTML = '';
        treeContainer.appendChild(renderTreeView(parsedObj));
      } catch {}
    }

    // Decode button click
    detailPanel.querySelector('#btn-decode-record')?.addEventListener('click', async () => {
      const formatId = (detailPanel.querySelector('#detail-kaitai-select') as HTMLSelectElement).value;
      if (!formatId) {
        showToast('Please choose a Kaitai Format to decode with', 'error');
        return;
      }

      const treeContainer = detailPanel.querySelector('#tree-view-container') as HTMLElement;
      const parseTimeEl = detailPanel.querySelector('#kaitai-parse-time') as HTMLElement;
      treeContainer.innerHTML = '<div style="color: var(--accent-cyan); font-size: 12px; padding: 12px;">Running Kaitai native binary parser...</div>';

      try {
        const result = await KaitaiService.ParseBinary({
          formatId: formatId,
          dataBase64: rec.valueBase64,
          dataHex: '',
        });

        if (result && result.success) {
          parseTimeEl.innerText = `Parsed in ${result.elapsedMs.toFixed(2)}ms`;
          treeContainer.innerHTML = '';
          treeContainer.appendChild(renderTreeView(result.result));
          showToast(`Decoded with Kaitai in ${result.elapsedMs.toFixed(1)}ms!`, 'success');
        } else {
          treeContainer.innerHTML = `<div style="color: var(--accent-rose); font-size: 12px; padding: 12px;">Error: ${result?.error || 'Parse failed'}</div>`;
        }
      } catch (err: any) {
        treeContainer.innerHTML = `<div style="color: var(--accent-rose); font-size: 12px; padding: 12px;">Decode error: ${err.message || String(err)}</div>`;
      }
    });
  }

  // Benchmark drop controls
  container.querySelector('#btn-start-benchmark')?.addEventListener('click', async () => {
    if (!store.selectedTopic) {
      showToast('Select a topic first', 'error');
      return;
    }
    try {
      await KafkaService.StartConsumer({
        topic: store.selectedTopic,
        groupId: `bench-${Date.now()}`,
        mode: 'drop',
        storeFilePath: '',
        storeFormat: '',
        autoOffsetReset: 'latest',
        kaitaiFormatId: '',
      });
      showToast('Consumer benchmark started (Drop mode)!', 'success');
      store.setView('dashboard');
    } catch (err: any) {
      showToast(`Failed to start benchmark: ${err.message}`, 'error');
    }
  });

  container.querySelector('#btn-stop-benchmark')?.addEventListener('click', async () => {
    try {
      await KafkaService.StopConsumer();
      showToast('Consumer benchmark stopped', 'info');
      store.notify();
    } catch (err: any) {
      showToast(`Error stopping: ${err.message}`, 'error');
    }
  });

  // Store controls
  container.querySelector('#btn-start-store')?.addEventListener('click', async () => {
    if (!store.selectedTopic) {
      showToast('Select a topic first', 'error');
      return;
    }
    const path = (container.querySelector('#store-filepath') as HTMLInputElement).value;
    const fmt = (container.querySelector('#store-format') as HTMLSelectElement).value;

    try {
      await KafkaService.StartConsumer({
        topic: store.selectedTopic,
        groupId: `store-${Date.now()}`,
        mode: 'store',
        storeFilePath: path,
        storeFormat: fmt,
        autoOffsetReset: 'latest',
        kaitaiFormatId: '',
      });
      showToast(`Streaming topic records to ${path}`, 'success');
      store.setView('dashboard');
    } catch (err: any) {
      showToast(`Failed to start storing: ${err.message}`, 'error');
    }
  });

  container.querySelector('#btn-stop-store')?.addEventListener('click', async () => {
    try {
      await KafkaService.StopConsumer();
      showToast('Stream storage stopped', 'info');
      store.notify();
    } catch (err: any) {
      showToast(`Error stopping: ${err.message}`, 'error');
    }
  });

  // Initial render
  renderTopicsList();
  updateHeader();

  return container;
}
