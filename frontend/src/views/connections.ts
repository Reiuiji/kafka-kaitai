import { KafkaService } from '../../bindings/github.com/okuu/kafka_kaitai/services';
import type { ConnectionConfig } from '../../bindings/github.com/okuu/kafka_kaitai/models';
import { store } from '../store';
import { showToast } from '../components/toast';

export function renderConnections(): HTMLElement {
  const container = document.createElement('div');
  container.innerHTML = `
    <div style="display: flex; flex-direction: column; gap: 24px; max-width: 900px; margin: 0 auto;">
      <div style="display: flex; justify-content: space-between; align-items: center;">
        <div>
          <h1 style="font-size: 22px; font-weight: 700; letter-spacing: -0.02em; margin-bottom: 4px;">Connection Manager</h1>
          <p style="color: var(--text-secondary); font-size: 13px;">Manage clusters, SASL/SCRAM credentials, mTLS certificates, and Schema Registry.</p>
        </div>
        <div>
          ${store.connection.connected ? `
            <button class="btn btn-danger" id="btn-disconnect">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
              Disconnect
            </button>
          ` : ''}
        </div>
      </div>

      <div class="card">
        <div class="card-title">Cluster Connection Profile</div>

        <div class="form-group">
          <label class="form-label">Profile Name</label>
          <input class="input" id="cfg-name" value="Local Cluster" placeholder="e.g. Production Cluster or Local Redpanda" />
        </div>

        <div class="form-group">
          <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px;">
            <label class="form-label" style="margin-bottom: 0;">Seed Brokers (comma separated)</label>
            <div style="display: flex; gap: 8px;">
              <button type="button" class="btn btn-secondary btn-sm" id="preset-single" style="font-size: 11px; padding: 2px 8px; height: 24px;">
                Single (9092)
              </button>
              <button type="button" class="btn btn-primary btn-sm" id="preset-cluster" style="font-size: 11px; padding: 2px 8px; height: 24px;">
                ⚡ 3-Broker Cluster (9092-9094)
              </button>
            </div>
          </div>
          <input class="input" id="cfg-brokers" value="localhost:9092" placeholder="localhost:9092, kafka-broker-2:9092" />
          <div style="font-size: 11px; color: var(--text-muted); margin-top: 6px; display: flex; align-items: center; gap: 6px;">
            <span style="color: var(--accent-cyan);">💡 Performance Tip:</span>
            <span>Distribute load across multiple brokers to exceed 1,000 MB/s. Launch via <code style="background: rgba(255,255,255,0.06); padding: 1px 4px; border-radius: 4px; color: var(--accent-cyan);">make cluster-up</code>.</span>
          </div>
        </div>

        <div class="grid-2">
          <div class="form-group">
            <label class="form-label">Authentication Mechanism</label>
            <select class="select" id="cfg-auth">
              <option value="none" selected>No Authentication (PLAINTEXT)</option>
              <option value="plain">SASL / PLAIN</option>
              <option value="scram-sha-256">SASL / SCRAM-SHA-256</option>
              <option value="scram-sha-512">SASL / SCRAM-SHA-512</option>
              <option value="ssl">SSL / TLS (One-way)</option>
              <option value="mtls">mTLS (Mutual TLS with client cert)</option>
              <option value="oauthbearer">SASL / OAUTHBEARER</option>
            </select>
          </div>

          <div class="form-group" id="group-tls">
            <label class="form-label">TLS / SSL Encryption</label>
            <div style="display: flex; align-items: center; gap: 12px; height: 38px;">
              <label style="display: flex; align-items: center; gap: 8px; font-size: 13px; cursor: pointer;">
                <input type="checkbox" id="cfg-tls" /> Enable TLS
              </label>
              <label style="display: flex; align-items: center; gap: 8px; font-size: 13px; cursor: pointer;">
                <input type="checkbox" id="cfg-insecure" /> Skip Verify (Insecure)
              </label>
            </div>
          </div>
        </div>

        <!-- SASL Credentials Section -->
        <div id="section-sasl" class="grid-2" style="display: none; border-top: 1px solid var(--border-subtle); padding-top: 14px; margin-top: 4px;">
          <div class="form-group">
            <label class="form-label">Username</label>
            <input class="input" id="cfg-user" placeholder="API key or username" />
          </div>
          <div class="form-group">
            <label class="form-label">Password</label>
            <input type="password" class="input" id="cfg-pass" placeholder="API secret or password" />
          </div>
        </div>

        <!-- OAuth Token Section -->
        <div id="section-oauth" class="form-group" style="display: none;">
          <label class="form-label">OAuth Bearer Token</label>
          <textarea class="textarea" id="cfg-token" placeholder="Bearer eyJhbGci..."></textarea>
        </div>

        <!-- Certificates Section -->
        <div id="section-certs" class="grid-3" style="display: none; border-top: 1px solid var(--border-subtle); padding-top: 14px;">
          <div class="form-group">
            <label class="form-label">CA Certificate Path</label>
            <input class="input" id="cfg-ca-path" placeholder="/path/to/ca.pem" />
          </div>
          <div class="form-group">
            <label class="form-label">Client Certificate Path</label>
            <input class="input" id="cfg-cert-path" placeholder="/path/to/client.crt" />
          </div>
          <div class="form-group">
            <label class="form-label">Client Private Key Path</label>
            <input class="input" id="cfg-key-path" placeholder="/path/to/client.key" />
          </div>
        </div>

        <!-- Schema Registry Section -->
        <div style="border-top: 1px solid var(--border-subtle); padding-top: 14px; margin-top: 10px;">
          <div style="font-weight: 600; font-size: 13px; margin-bottom: 10px; color: var(--text-primary);">Schema Registry (Optional)</div>
          <div class="grid-3">
            <div class="form-group">
              <label class="form-label">Registry URL</label>
              <input class="input" id="cfg-sr-url" placeholder="http://localhost:8081" />
            </div>
            <div class="form-group">
              <label class="form-label">Registry User</label>
              <input class="input" id="cfg-sr-user" placeholder="Optional" />
            </div>
            <div class="form-group">
              <label class="form-label">Registry Password</label>
              <input type="password" class="input" id="cfg-sr-pass" placeholder="Optional" />
            </div>
          </div>
        </div>

        <!-- Actions -->
        <div style="display: flex; gap: 14px; justify-content: flex-end; margin-top: 20px; border-top: 1px solid var(--border-subtle); padding-top: 16px;">
          <button class="btn btn-secondary" id="btn-test">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 14 14"/></svg>
            Test Ping
          </button>
          <button class="btn btn-primary" id="btn-connect">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/></svg>
            Connect to Cluster
          </button>
        </div>

        <div id="test-result" style="margin-top: 14px; display: none;"></div>
      </div>
    </div>
  `;

  // Dynamic form toggle logic based on AuthType
  const authSelect = container.querySelector('#cfg-auth') as HTMLSelectElement;
  const sectionSasl = container.querySelector('#section-sasl') as HTMLElement;
  const sectionOauth = container.querySelector('#section-oauth') as HTMLElement;
  const sectionCerts = container.querySelector('#section-certs') as HTMLElement;
  const tlsCheckbox = container.querySelector('#cfg-tls') as HTMLInputElement;

  function updateAuthFields() {
    const val = authSelect.value;
    sectionSasl.style.display = ['plain', 'scram-sha-256', 'scram-sha-512'].includes(val) ? 'grid' : 'none';
    sectionOauth.style.display = val === 'oauthbearer' ? 'block' : 'none';
    sectionCerts.style.display = ['ssl', 'mtls'].includes(val) || tlsCheckbox.checked ? 'grid' : 'none';
    if (val === 'ssl' || val === 'mtls') {
      tlsCheckbox.checked = true;
    }
  }

  authSelect.addEventListener('change', updateAuthFields);
  tlsCheckbox.addEventListener('change', updateAuthFields);

  container.querySelector('#preset-single')?.addEventListener('click', () => {
    (container.querySelector('#cfg-brokers') as HTMLInputElement).value = 'localhost:9092';
    (container.querySelector('#cfg-name') as HTMLInputElement).value = 'Single Broker';
    showToast('Applied Single Broker preset (localhost:9092)', 'info');
  });

  container.querySelector('#preset-cluster')?.addEventListener('click', () => {
    (container.querySelector('#cfg-brokers') as HTMLInputElement).value = 'localhost:9092, localhost:9093, localhost:9094';
    (container.querySelector('#cfg-name') as HTMLInputElement).value = '3-Broker KRaft Cluster';
    showToast('Applied 3-Broker Cluster preset (ports 9092, 9093, 9094)', 'info');
  });

  function getFormConfig(): ConnectionConfig {
    const brokers = (container.querySelector('#cfg-brokers') as HTMLInputElement).value
      .split(',')
      .map(s => s.trim())
      .filter(s => s.length > 0);

    return {
      id: 'conn-default',
      name: (container.querySelector('#cfg-name') as HTMLInputElement).value || 'Kafka Cluster',
      brokers: brokers.length > 0 ? brokers : ['localhost:9092'],
      authType: authSelect.value,
      username: (container.querySelector('#cfg-user') as HTMLInputElement).value,
      password: (container.querySelector('#cfg-pass') as HTMLInputElement).value,
      tlsEnabled: tlsCheckbox.checked,
      insecureSkipVerify: (container.querySelector('#cfg-insecure') as HTMLInputElement).checked,
      caCertPath: (container.querySelector('#cfg-ca-path') as HTMLInputElement).value,
      clientCertPath: (container.querySelector('#cfg-cert-path') as HTMLInputElement).value,
      clientKeyPath: (container.querySelector('#cfg-key-path') as HTMLInputElement).value,
      token: (container.querySelector('#cfg-token') as HTMLTextAreaElement).value,
      schemaRegistryUrl: (container.querySelector('#cfg-sr-url') as HTMLInputElement).value,
      schemaRegistryUser: (container.querySelector('#cfg-sr-user') as HTMLInputElement).value,
      schemaRegistryPass: (container.querySelector('#cfg-sr-pass') as HTMLInputElement).value,
    };
  }

  // Test connection button
  container.querySelector('#btn-test')?.addEventListener('click', async () => {
    const testBox = container.querySelector('#test-result') as HTMLElement;
    testBox.style.display = 'block';
    testBox.innerHTML = '<div style="color: var(--accent-cyan); font-size: 13px;">Testing connection to cluster...</div>';

    try {
      const cfg = getFormConfig();
      const status = await KafkaService.TestConnection(cfg);
      if (status && status.connected) {
        testBox.innerHTML = `
          <div class="metric-box" style="border-color: var(--accent-emerald);">
            <div style="color: var(--accent-emerald); font-weight: 600;">✓ Successfully connected!</div>
            <div style="font-size: 12px; color: var(--text-secondary); margin-top: 4px;">
              Cluster ID: <b>${status.clusterId || 'n/a'}</b> | Controller ID: <b>${status.controllerId}</b> | Latency: <b>${status.latencyMs.toFixed(1)}ms</b>
            </div>
          </div>
        `;
      }
    } catch (err: any) {
      testBox.innerHTML = `
        <div class="metric-box" style="border-color: var(--accent-rose);">
          <div style="color: var(--accent-rose); font-weight: 600;">✗ Connection failed</div>
          <div style="font-size: 12px; color: var(--text-secondary); margin-top: 4px;">${err.message || String(err)}</div>
        </div>
      `;
    }
  });

  // Connect button
  container.querySelector('#btn-connect')?.addEventListener('click', async () => {
    try {
      const cfg = getFormConfig();
      const status = await KafkaService.Connect(cfg);
      if (status && status.connected) {
        store.setConnection(status);
        showToast('Connected to Kafka cluster!', 'success');

        // Fetch topics
        try {
          const topics = await KafkaService.ListTopics();
          store.setTopics(topics || []);
        } catch {}

        store.setView('dashboard');
      }
    } catch (err: any) {
      showToast(`Connection error: ${err.message || String(err)}`, 'error');
    }
  });

  // Disconnect button
  container.querySelector('#btn-disconnect')?.addEventListener('click', async () => {
    try {
      await KafkaService.Disconnect();
      store.setConnection({
        connected: false,
        clusterId: '',
        controllerId: 0,
        brokers: [],
        topicsCount: 0,
        latencyMs: 0,
      });
      store.setTopics([]);
      showToast('Disconnected from Kafka', 'info');
      store.setView('connections');
    } catch (err: any) {
      showToast(`Disconnect error: ${err.message || String(err)}`, 'error');
    }
  });

  return container;
}
