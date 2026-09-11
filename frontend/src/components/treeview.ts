export function renderTreeView(data: any, indent = 0): HTMLElement {
  const container = document.createElement('div');
  container.className = 'tree-viewer';

  function buildNode(key: string | null, value: any, depth: number): HTMLElement {
    const node = document.createElement('div');
    node.className = 'tree-node';
    node.style.paddingLeft = `${depth * 16}px`;

    const keyLabel = key !== null ? `<span class="tree-key">${escapeHtml(key)}:</span> ` : '';

    if (value === null) {
      node.innerHTML = `${keyLabel}<span style="color: var(--text-muted);">null</span>`;
    } else if (typeof value === 'boolean') {
      node.innerHTML = `${keyLabel}<span class="tree-val-bool">${value}</span>`;
    } else if (typeof value === 'number') {
      node.innerHTML = `${keyLabel}<span class="tree-val-num">${value}</span>`;
    } else if (typeof value === 'string') {
      node.innerHTML = `${keyLabel}<span class="tree-val-string">"${escapeHtml(value)}"</span>`;
    } else if (Array.isArray(value)) {
      const header = document.createElement('div');
      header.innerHTML = `${keyLabel}<span style="color: var(--text-secondary); cursor: pointer;">▼ Array(${value.length})</span>`;
      node.appendChild(header);

      const childrenContainer = document.createElement('div');
      value.forEach((item, idx) => {
        childrenContainer.appendChild(buildNode(`[${idx}]`, item, depth + 1));
      });
      node.appendChild(childrenContainer);

      header.onclick = () => {
        const isHidden = childrenContainer.style.display === 'none';
        childrenContainer.style.display = isHidden ? 'block' : 'none';
        header.innerHTML = `${keyLabel}<span style="color: var(--text-secondary); cursor: pointer;">${isHidden ? '▼' : '▶'} Array(${value.length})</span>`;
      };
    } else if (typeof value === 'object') {
      const keys = Object.keys(value);
      const header = document.createElement('div');
      header.innerHTML = `${keyLabel}<span style="color: var(--text-secondary); cursor: pointer;">▼ Object(${keys.length})</span>`;
      node.appendChild(header);

      const childrenContainer = document.createElement('div');
      keys.forEach((k) => {
        childrenContainer.appendChild(buildNode(k, value[k], depth + 1));
      });
      node.appendChild(childrenContainer);

      header.onclick = () => {
        const isHidden = childrenContainer.style.display === 'none';
        childrenContainer.style.display = isHidden ? 'block' : 'none';
        header.innerHTML = `${keyLabel}<span style="color: var(--text-secondary); cursor: pointer;">${isHidden ? '▼' : '▶'} Object(${keys.length})</span>`;
      };
    }

    return node;
  }

  function escapeHtml(text: string) {
    return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  container.appendChild(buildNode(null, data, 0));
  return container;
}
