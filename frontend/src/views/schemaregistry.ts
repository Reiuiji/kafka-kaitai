import { SchemaRegistryService } from '../../bindings/github.com/okuu/kafka_kaitai/services';
import { showToast } from '../components/toast';

export function renderSchemaRegistry(): HTMLElement {
  const container = document.createElement('div');
  container.innerHTML = `
    <div style="display: flex; flex-direction: column; gap: 20px; max-width: 900px; margin: 0 auto;">
      <div>
        <h1 style="font-size: 22px; font-weight: 700; letter-spacing: -0.02em; margin-bottom: 4px;">Schema Registry</h1>
        <p style="color: var(--text-secondary); font-size: 13px;">Inspect registered Avro, Protobuf, and JSON schemas.</p>
      </div>

      <div class="card">
        <div class="card-title">Registry Subjects</div>
        <div style="display: flex; gap: 10px; margin-bottom: 16px;">
          <button class="btn btn-secondary btn-sm" id="btn-fetch-subjects">
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></svg>
            Fetch Subjects
          </button>
        </div>

        <div id="subjects-container" style="color: var(--text-muted); font-size: 13px;">
          Configure Schema Registry URL in Connections to list and view schema subjects.
        </div>
      </div>
    </div>
  `;

  container.querySelector('#btn-fetch-subjects')?.addEventListener('click', async () => {
    const listEl = container.querySelector('#subjects-container') as HTMLElement;
    listEl.innerText = 'Querying registry...';
    try {
      const subjects = await SchemaRegistryService.ListSubjects();
      if (!subjects || subjects.length === 0) {
        listEl.innerHTML = '<div style="color: var(--text-muted);">No subjects found in Schema Registry.</div>';
      } else {
        listEl.innerHTML = subjects.map(s => `
          <div style="padding: 8px 10px; border-radius: var(--radius-sm); background: rgba(255,255,255,0.02); margin-bottom: 6px; font-family: var(--font-mono);">
            ${s}
          </div>
        `).join('');
      }
    } catch (err: any) {
      listEl.innerHTML = `<div style="color: var(--accent-rose);">Error: ${err.message || String(err)}</div>`;
    }
  });

  return container;
}
