import { KaitaiService } from '../../bindings/github.com/okuu/kafka_kaitai/services';
import type { KaitaiFormat } from '../../bindings/github.com/okuu/kafka_kaitai/models';
import { store } from '../store';
import { showToast } from '../components/toast';
import { renderTreeView } from '../components/treeview';

export function renderKaitai(): HTMLElement {
  const container = document.createElement('div');
  let selectedFormat: KaitaiFormat | null = store.formats.length > 0 ? store.formats[0] : null;

  container.innerHTML = `
    <div style="display: flex; height: 100%; gap: 20px;">
      <!-- Left: Formats Registry Sidebar -->
      <div class="card" style="width: 300px; display: flex; flex-direction: column; padding: 16px; flex-shrink: 0;">
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
          <span style="font-weight: 600; font-size: 14px;">Kaitai Formats (${store.formats.length})</span>
          <button class="btn btn-primary btn-sm" id="btn-open-import" title="Import new .ksy schema">
            + New KSY
          </button>
        </div>

        <div id="kaitai-format-list" style="overflow-y: auto; flex: 1; display: flex; flex-direction: column; gap: 6px;">
          ${store.formats.map(f => `
            <div class="format-card ${selectedFormat?.id === f.id ? 'active' : ''}" data-id="${f.id}" style="
              padding: 10px 12px; border-radius: var(--radius-md); border: 1px solid ${selectedFormat?.id === f.id ? 'var(--accent-cyan)' : 'var(--border-subtle)'};
              background: ${selectedFormat?.id === f.id ? 'rgba(6, 182, 212, 0.1)' : 'rgba(255, 255, 255, 0.02)'};
              cursor: pointer; display: flex; flex-direction: column; gap: 4px;
            ">
              <div style="display: flex; justify-content: space-between; align-items: center;">
                <span style="font-weight: 600; font-size: 13px; color: ${selectedFormat?.id === f.id ? 'var(--accent-cyan)' : 'var(--text-primary)'};">${f.name}</span>
                <span class="brand-badge" style="font-size: 9px;">${f.compiled ? 'COMPILED' : 'SOURCE'}</span>
              </div>
              <div style="font-size: 11px; color: var(--text-muted); font-family: var(--font-mono);">
                Struct: ${f.structName || 'Unknown'}
              </div>
            </div>
          `).join('')}
        </div>
      </div>

      <!-- Right: Format Detail & Binary Test Bench -->
      <div style="flex: 1; display: flex; flex-direction: column; gap: 16px; overflow-y: auto;">
        <!-- Top Banner / Format Metadata -->
        <div class="card" style="padding: 16px;">
          <div style="display: flex; justify-content: space-between; align-items: flex-start;">
            <div>
              <div style="display: flex; align-items: center; gap: 10px;">
                <h2 style="font-size: 18px; font-weight: 700;">${selectedFormat?.name || 'No format selected'}</h2>
                <span class="brand-badge">${selectedFormat?.compiled ? 'NATIVE PARSER READY' : 'NOT COMPILED'}</span>
              </div>
              <p style="color: var(--text-secondary); font-size: 12px; margin-top: 4px;">
                ${selectedFormat?.description || 'Declarative Kaitai Struct specification'}
              </p>
            </div>

            <div style="display: flex; gap: 10px;">
              <button class="btn btn-primary btn-sm" id="btn-compile-format">
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg>
                Compile Parser Binary
              </button>
            </div>
          </div>

          <div class="grid-3" style="margin-top: 14px; padding-top: 12px; border-top: 1px solid var(--border-subtle); font-size: 11px; font-family: var(--font-mono);">
            <div>Root Struct: <b style="color: var(--text-primary);">${selectedFormat?.structName}</b></div>
            <div>Checksum: <b style="color: var(--text-primary);">${selectedFormat?.checksum.substring(0, 16)}...</b></div>
            <div>Binary: <b style="color: var(--accent-cyan);">${selectedFormat?.binaryPath ? selectedFormat.binaryPath.split('/').pop() : 'none'}</b></div>
          </div>
        </div>

        <!-- Split: KSY Editor vs Binary Test Bench -->
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 16px; flex: 1; min-height: 440px;">
          <!-- KSY Source Code View -->
          <div class="card" style="padding: 16px; display: flex; flex-direction: column;">
            <div style="font-weight: 600; font-size: 13px; margin-bottom: 8px;">Kaitai Struct Schema (.ksy)</div>
            <textarea class="textarea" id="format-ksy-editor" style="flex: 1; min-height: 320px; font-size: 12px; line-height: 1.5;">${selectedFormat?.ksySrc || ''}</textarea>
          </div>

          <!-- Live Binary Test Bench -->
          <div class="card" style="padding: 16px; display: flex; flex-direction: column; gap: 12px;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
              <span style="font-weight: 600; font-size: 13px;">Binary Parser Test Bench</span>
              <button class="btn btn-secondary btn-sm" id="btn-load-sample-bytes" style="font-size: 11px;">
                Load Sample IPv4 Hex
              </button>
            </div>

            <div class="form-group" style="margin: 0;">
              <label class="form-label">Binary Payload (Hex or Base64)</label>
              <textarea class="textarea" id="test-payload-input" style="min-height: 70px; font-size: 12px;" placeholder="Paste hex bytes (e.g. 4500003c1c4640004006b1e6c0a80001c0a80002)"></textarea>
            </div>

            <div style="display: flex; justify-content: space-between; align-items: center;">
              <div style="display: flex; gap: 12px; font-size: 12px;">
                <label style="display: flex; align-items: center; gap: 6px; cursor: pointer;">
                  <input type="radio" name="test-input-type" value="hex" checked /> Hex
                </label>
                <label style="display: flex; align-items: center; gap: 6px; cursor: pointer;">
                  <input type="radio" name="test-input-type" value="base64" /> Base64
                </label>
              </div>

              <button class="btn btn-primary btn-sm" id="btn-run-parse">
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="5 3 19 12 5 21 5 3"/></svg>
                Parse Binary
              </button>
            </div>

            <!-- Output Tree Viewer -->
            <div style="flex: 1; display: flex; flex-direction: column; overflow: hidden;">
              <div style="font-size: 11px; font-weight: 600; color: var(--text-muted); margin-bottom: 4px; display: flex; justify-content: space-between;">
                <span>Parsed Struct Output</span>
                <span id="test-bench-time" style="color: var(--accent-emerald); font-family: var(--font-mono);"></span>
              </div>
              <div id="test-bench-result" style="flex: 1; overflow-y: auto;">
                <div style="color: var(--text-muted); font-size: 12px; padding: 20px; text-align: center;">
                  Provide binary input above and click "Parse Binary" to inspect decoded fields.
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Import Modal Overlay -->
    <div id="modal-import" style="display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.7); backdrop-filter: blur(8px); z-index: 999; align-items: center; justify-content: center;">
      <div class="card" style="width: 580px; max-width: 90vw; background: var(--bg-tertiary);">
        <div class="card-title">
          <span>Import Kaitai Struct Schema</span>
          <button class="btn btn-sm btn-secondary" id="btn-close-modal">✕</button>
        </div>

        <div class="form-group">
          <label class="form-label">Format Name</label>
          <input class="input" id="import-name" placeholder="e.g. PNG Image Header or Custom Protocol Frame" />
        </div>

        <div class="form-group">
          <label class="form-label">KSY Source (YAML)</label>
          <textarea class="textarea" id="import-src" style="min-height: 200px;" placeholder="meta:&#10;  id: my_format&#10;  endian: be&#10;seq:&#10;  - id: magic&#10;    size: 4"></textarea>
        </div>

        <div style="display: flex; justify-content: flex-end; gap: 10px; margin-top: 14px;">
          <button class="btn btn-secondary" id="btn-cancel-import">Cancel</button>
          <button class="btn btn-primary" id="btn-submit-import">Import & Register</button>
        </div>
      </div>
    </div>
  `;

  // Select format
  container.querySelectorAll('.format-card').forEach(card => {
    card.addEventListener('click', () => {
      const id = card.getAttribute('data-id');
      selectedFormat = store.formats.find(f => f.id === id) || null;
      renderKaitaiView();
    });
  });

  function renderKaitaiView() {
    const parent = container.parentElement;
    if (parent) {
      parent.innerHTML = '';
      parent.appendChild(renderKaitai());
    }
  }

  // Compile button
  container.querySelector('#btn-compile-format')?.addEventListener('click', async () => {
    if (!selectedFormat) return;
    showToast(`Compiling Kaitai parser for ${selectedFormat.name}...`, 'info');

    try {
      const updated = await KaitaiService.BuildFormat(selectedFormat.id);
      if (updated) {
        selectedFormat = updated;
        const formats = await KaitaiService.ListFormats();
        store.setFormats(formats);
        showToast(`Parser compiled successfully!`, 'success');
      }
    } catch (err: any) {
      showToast(`Compile failed: ${err.message || String(err)}`, 'error');
    }
  });

  // Load sample bytes (IPv4 packet: 45 00 00 3c 1c 46 40 00 40 06 b1 e6 c0 a8 00 01 c0 a8 00 02)
  container.querySelector('#btn-load-sample-bytes')?.addEventListener('click', () => {
    const input = container.querySelector('#test-payload-input') as HTMLTextAreaElement;
    input.value = '4500003c1c4640004006b1e6c0a80001c0a80002';
    showToast('Loaded IPv4 sample packet hex bytes', 'info');
  });

  // Run parse test bench
  container.querySelector('#btn-run-parse')?.addEventListener('click', async () => {
    if (!selectedFormat) {
      showToast('Select a format first', 'error');
      return;
    }

    const val = (container.querySelector('#test-payload-input') as HTMLTextAreaElement).value.trim();
    if (!val) {
      showToast('Please enter hex or base64 bytes', 'error');
      return;
    }

    const typeRadio = container.querySelector('input[name="test-input-type"]:checked') as HTMLInputElement;
    const isHex = typeRadio?.value === 'hex';

    const resultBox = container.querySelector('#test-bench-result') as HTMLElement;
    const timeBox = container.querySelector('#test-bench-time') as HTMLElement;

    resultBox.innerHTML = '<div style="color: var(--accent-cyan); font-size: 12px; padding: 20px;">Executing standalone parser binary...</div>';

    try {
      const res = await KaitaiService.ParseBinary({
        formatId: selectedFormat.id,
        dataHex: isHex ? val : '',
        dataBase64: isHex ? '' : val,
      });

      if (res && res.success) {
        timeBox.innerText = `Executed in ${res.elapsedMs.toFixed(2)}ms`;
        resultBox.innerHTML = '';
        resultBox.appendChild(renderTreeView(res.result));
        showToast(`Parsed binary successfully in ${res.elapsedMs.toFixed(1)}ms!`, 'success');
      } else {
        resultBox.innerHTML = `<div style="color: var(--accent-rose); font-size: 12px; padding: 16px;">Error: ${res?.error || 'Parse failed'}</div>`;
      }
    } catch (err: any) {
      resultBox.innerHTML = `<div style="color: var(--accent-rose); font-size: 12px; padding: 16px;">Execution error: ${err.message || String(err)}</div>`;
    }
  });

  // Modal handlers
  const modal = container.querySelector('#modal-import') as HTMLElement;
  container.querySelector('#btn-open-import')?.addEventListener('click', () => {
    modal.style.display = 'flex';
  });
  container.querySelector('#btn-close-modal')?.addEventListener('click', () => {
    modal.style.display = 'none';
  });
  container.querySelector('#btn-cancel-import')?.addEventListener('click', () => {
    modal.style.display = 'none';
  });

  container.querySelector('#btn-submit-import')?.addEventListener('click', async () => {
    const name = (container.querySelector('#import-name') as HTMLInputElement).value.trim();
    const src = (container.querySelector('#import-src') as HTMLTextAreaElement).value.trim();

    if (!name || !src) {
      showToast('Name and KSY schema content are required', 'error');
      return;
    }

    try {
      const created = await KaitaiService.ImportFormat(name, src);
      if (created) {
        modal.style.display = 'none';
        const formats = await KaitaiService.ListFormats();
        store.setFormats(formats);
        selectedFormat = created;
        showToast(`Registered format "${name}"!`, 'success');
      }
    } catch (err: any) {
      showToast(`Import error: ${err.message || String(err)}`, 'error');
    }
  });

  return container;
}
