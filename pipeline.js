import fs from 'fs';
import path from 'path';
import { createRequire } from 'module';
import { fileURLToPath } from 'url';

const require = createRequire(import.meta.url);
const CONFIG = require('./config.json');

// ── PDF upload ────────────────────────────────────────────────────────────────

async function uploadPdf(pdfPath) {
  const fileBuffer = fs.readFileSync(pdfPath);
  const fileName = path.basename(pdfPath);

  const form = new FormData();
  form.append('file', new File([fileBuffer], fileName, { type: 'application/pdf' }));
  form.append('user', CONFIG.userId);

  const res = await fetch(`${CONFIG.difyBaseUrl}/v1/files/upload`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${CONFIG.workflowApiKey}` },
    body: form,
  });

  const json = await res.json();
  if (!res.ok) throw new Error(`Upload failed: ${res.status} ${JSON.stringify(json)}`);
  return json.id;
}

// ── Run workflow ──────────────────────────────────────────────────────────────

async function runWorkflow(fileIds, orderId) {
  const res = await fetch(`${CONFIG.difyBaseUrl}/v1/workflows/run`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${CONFIG.workflowApiKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      inputs: {
        pdfs: fileIds.map(id => ({ transfer_method: 'local_file', upload_file_id: id, type: 'document' })),
        OrderId: orderId,
      },
      response_mode: 'blocking',
      user: CONFIG.userId,
    }),
  });

  const json = await res.json();
  if (!res.ok) throw new Error(`Workflow failed: ${res.status} ${JSON.stringify(json)}`);
  if (json.data?.status !== 'succeeded') throw new Error(`Workflow status: ${json.data?.status}`);
  return json.data.outputs;
}

// ── Download files to memory ──────────────────────────────────────────────────

async function downloadFilesToMemory(outputs) {
  const files = Object.values(outputs).flat().filter(f => f && f.url && f.filename);
  const result = [];
  for (const file of files) {
    const url = file.url.startsWith('http') ? file.url : `${CONFIG.difyBaseUrl}${file.url}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${CONFIG.workflowApiKey}` } });
    if (!res.ok) throw new Error(`Download failed for ${file.filename}: ${res.status}`);
    const buf = await res.arrayBuffer();
    result.push({ filename: file.filename, content: Buffer.from(buf) });
  }
  return result;
}

// ── Knowledge upload helpers ──────────────────────────────────────────────────

function datasetAuthHeaders() {
  return {
    Authorization: `Bearer ${CONFIG.datasetApiKey}`,
    'Content-Type': 'application/json',
  };
}

function parseFilename(filename) {
  const base = filename.endsWith('.md') ? filename.slice(0, -3) : filename;
  const lastUnderscore = base.lastIndexOf('_');
  if (lastUnderscore === -1) return null;
  return { prefix: base.slice(0, lastUnderscore), orderId: base.slice(lastUnderscore + 1) };
}

async function getOrCreateMetadataField(datasetId) {
  const url = `${CONFIG.difyBaseUrl}/v1/datasets/${datasetId}/metadata`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${CONFIG.datasetApiKey}` } });
  if (!res.ok) throw new Error(`GET metadata failed: ${res.status} ${await res.text()}`);
  const data = await res.json();
  const existing = (data.doc_metadata || []).find(f => f.name === 'project_number');
  if (existing) return existing.id;

  const createRes = await fetch(url, {
    method: 'POST',
    headers: datasetAuthHeaders(),
    body: JSON.stringify({ type: 'string', name: 'project_number' }),
  });
  if (!createRes.ok) throw new Error(`POST metadata failed: ${createRes.status} ${await createRes.text()}`);
  const created = await createRes.json();
  return created.id;
}

async function uploadByText(datasetId, filename, text) {
  const url = `${CONFIG.difyBaseUrl}/v1/datasets/${datasetId}/document/create-by-text`;
  const res = await fetch(url, {
    method: 'POST',
    headers: datasetAuthHeaders(),
    body: JSON.stringify({
      name: filename,
      text,
      indexing_technique: 'high_quality',
      process_rule: {
        mode: 'custom',
        rules: {
          pre_processing_rules: [
            { id: 'remove_extra_spaces', enabled: true },
            { id: 'remove_urls_emails', enabled: false },
          ],
          segmentation: { separator: '\n\n', max_tokens: 1024, chunk_overlap: 50 },
        },
      },
    }),
  });
  if (!res.ok) throw new Error(`upload failed: ${res.status} ${await res.text()}`);
  const data = await res.json();
  return data.document.id;
}

async function setDocumentMetadata(datasetId, documentId, fieldId, orderId) {
  const url = `${CONFIG.difyBaseUrl}/v1/datasets/${datasetId}/documents/metadata`;
  const res = await fetch(url, {
    method: 'POST',
    headers: datasetAuthHeaders(),
    body: JSON.stringify({
      operation_data: [{
        document_id: documentId,
        metadata_list: [{ id: fieldId, name: 'project_number', value: orderId }],
      }],
    }),
  });
  if (!res.ok) throw new Error(`set metadata failed: ${res.status} ${await res.text()}`);
}

// ── Upload files to knowledge bases ──────────────────────────────────────────

async function uploadToKnowledge(mdFiles) {
  const fieldIdCache = {};
  let success = 0, skipped = 0, failed = 0;

  for (const { filename, content } of mdFiles) {
    const parsed = parseFilename(filename);
    if (!parsed) {
      console.log(`  - ${filename} → skipped (parse error)`);
      skipped++;
      continue;
    }

    const { prefix, orderId } = parsed;
    const datasetId = CONFIG.knowledgeMap[prefix];

    if (!datasetId) {
      console.log(`  - ${filename} → skipped (no mapping)`);
      skipped++;
      continue;
    }

    if (datasetId.startsWith('DATASET_ID_')) {
      console.log(`  - ${filename} → skipped (placeholder dataset)`);
      skipped++;
      continue;
    }

    try {
      if (!fieldIdCache[datasetId]) {
        fieldIdCache[datasetId] = await getOrCreateMetadataField(datasetId);
      }
      const fieldId = fieldIdCache[datasetId];
      const text = content.toString('utf-8');
      const documentId = await uploadByText(datasetId, filename, text);
      await setDocumentMetadata(datasetId, documentId, fieldId, orderId);
      console.log(`  ✓ ${filename} → ${datasetId}`);
      success++;
    } catch (err) {
      console.error(`  ✗ ${filename} → ${err.message}`);
      failed++;
    }
  }

  return { success, skipped, failed };
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function main() {
  const inputDir = fileURLToPath(new URL('./inputFiles', import.meta.url));
  if (!fs.existsSync(inputDir)) throw new Error(`inputFiles/ not found`);

  const allPdfs = fs.readdirSync(inputDir).filter(f => f.toLowerCase().endsWith('.pdf'));
  const orderMap = {};
  for (const filename of allPdfs) {
    const idx = filename.indexOf('_');
    if (idx === -1) continue;
    const orderId = filename.slice(0, idx);
    (orderMap[orderId] ||= []).push(filename);
  }

  if (Object.keys(orderMap).length === 0) {
    console.log('No PDF files found in inputFiles/');
    process.exit(0);
  }

  let totalSuccess = 0, totalSkipped = 0, totalFailed = 0;

  for (const [orderId, filenames] of Object.entries(orderMap)) {
    console.log(`\n===== ${orderId} (${filenames.length} PDF) =====`);

    console.log(`[1/4] Uploading PDFs...`);
    const fileIds = [];
    for (const filename of filenames) {
      const fileId = await uploadPdf(path.join(inputDir, filename));
      console.log(`      ${filename} → ${fileId}`);
      fileIds.push(fileId);
    }

    console.log(`[2/4] Running workflow...`);
    const start = Date.now();
    const outputs = await runWorkflow(fileIds, orderId);
    const elapsed = Math.round((Date.now() - start) / 1000);
    console.log(`      done (${elapsed}s)`);

    console.log(`[3/4] Downloading files...`);
    const mdFiles = await downloadFilesToMemory(outputs);
    console.log(`      done (${mdFiles.length} files)`);

    console.log(`[4/4] Uploading to knowledge...`);
    const { success, skipped, failed } = await uploadToKnowledge(mdFiles);
    console.log(`      成功 ${success} / スキップ ${skipped} / 失敗 ${failed}`);

    totalSuccess += success;
    totalSkipped += skipped;
    totalFailed += failed;
  }

  console.log(`\n===== 完了 =====`);
  console.log(`合計: 成功 ${totalSuccess} / スキップ ${totalSkipped} / 失敗 ${totalFailed}`);
  if (totalFailed > 0) process.exit(1);
}

main().catch(err => {
  console.error(err.message || err);
  process.exit(1);
});
