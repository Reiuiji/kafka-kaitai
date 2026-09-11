export function renderHexViewer(rawBytes: Uint8Array): HTMLElement {
  const container = document.createElement('div');
  container.className = 'hex-viewer';

  if (!rawBytes || rawBytes.length === 0) {
    container.innerHTML = '<span style="color: var(--text-muted);">[Empty binary payload]</span>';
    return container;
  }

  const chunkSize = 16;
  for (let i = 0; i < rawBytes.length; i += chunkSize) {
    const chunk = rawBytes.slice(i, i + chunkSize);
    const row = document.createElement('div');
    row.className = 'hex-row';

    // Offset (00000000)
    const offset = document.createElement('span');
    offset.className = 'hex-offset';
    offset.innerText = i.toString(16).padStart(8, '0');

    // Hex bytes
    const hexBytes = document.createElement('span');
    hexBytes.className = 'hex-bytes';
    const hexParts: string[] = [];
    for (let j = 0; j < chunkSize; j++) {
      if (j < chunk.length) {
        hexParts.push(chunk[j].toString(16).padStart(2, '0').toUpperCase());
      } else {
        hexParts.push('  ');
      }
      if (j === 7) hexParts.push(' '); // extra space in middle
    }
    hexBytes.innerText = hexParts.join(' ');

    // ASCII gutter
    const ascii = document.createElement('span');
    ascii.className = 'hex-ascii';
    let asciiStr = '';
    for (let j = 0; j < chunk.length; j++) {
      const b = chunk[j];
      asciiStr += (b >= 32 && b <= 126) ? String.fromCharCode(b) : '.';
    }
    ascii.innerText = asciiStr;

    row.appendChild(offset);
    row.appendChild(hexBytes);
    row.appendChild(ascii);
    container.appendChild(row);
  }

  return container;
}

export function base64ToUint8(base64: string): Uint8Array {
  try {
    const binaryStr = atob(base64);
    const bytes = new Uint8Array(binaryStr.length);
    for (let i = 0; i < binaryStr.length; i++) {
      bytes[i] = binaryStr.charCodeAt(i);
    }
    return bytes;
  } catch {
    return new TextEncoder().encode(base64);
  }
}
