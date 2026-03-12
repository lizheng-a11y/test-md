import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const cfg = JSON.parse(fs.readFileSync(path.join(__dirname, 'config.json'), 'utf-8'));

const ASSETS_DIR = path.join(__dirname, 'assets');
const CONFIG = {
  DIFY_BASE_URL: cfg.difyBaseUrl,
  API_KEY: cfg.apiKey,
  USER_ID: cfg.userId,
  MD_DIR: path.join(ASSETS_DIR, cfg.mdSubDir),
  CSV_PATH: fs.readdirSync(ASSETS_DIR).map(f => path.join(ASSETS_DIR, f)).find(f => f.normalize('NFC').includes(cfg.csvKeyword) && f.endsWith('.csv')),
  OUTPUT_PATH: path.join(__dirname, cfg.outputFile),
  UNMATCHED_LOG: path.join(__dirname, cfg.unmatchedLog),
};

// ── CSV パーサー (RFC 4180) ────────────────────────────────────────
function parseCSV(text) {
  const result = [];
  let row = [];
  let field = '';
  let inQuotes = false;

  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (inQuotes) {
      if (ch === '"' && text[i + 1] === '"') { field += '"'; i++; }
      else if (ch === '"') { inQuotes = false; }
      else { field += ch; }
    } else {
      if (ch === '"') { inQuotes = true; }
      else if (ch === ',') { row.push(field); field = ''; }
      else if (ch === '\r') { /* skip */ }
      else if (ch === '\n') { row.push(field); result.push(row); row = []; field = ''; }
      else { field += ch; }
    }
  }
  if (field || row.length > 0) { row.push(field); result.push(row); }
  return result;
}

// ── CSV フィールドエスケープ ───────────────────────────────────────
function csvField(val) {
  const s = String(val ?? '');
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

// ── 1. MDファイルをパース ──────────────────────────────────────────
function parseMdFiles() {
  const rows = [];
  const files = fs.readdirSync(CONFIG.MD_DIR).filter(f => f.endsWith('.md')).sort();

  for (const fname of files) {
    const text = fs.readFileSync(path.join(CONFIG.MD_DIR, fname), 'utf-8');
    let caseNum = null;
    for (const line of text.split('\n')) {
      const caseMatch = line.match(/^-\s*案件管理番号[：:]\s*(\S+)/);
      if (caseMatch) { caseNum = caseMatch[1]; continue; }
      const itemMatch = line.match(/^-\s*([^：:]+)[：:](.+)/);
      if (itemMatch && caseNum) {
        const sysName = itemMatch[1].trim();
        const content = itemMatch[2].trim();
        if (sysName !== '案件管理番号') {
          rows.push({ caseNum, sysName, content });
        }
      }
    }
  }
  console.log(`[parse] MDファイル: ${files.length}件, 行数: ${rows.length}`);
  return rows;
}

// ── 2. CSVから (案件番号+システム名) → {合計工数, 案件名} を構築 ──
function buildCSVLookup() {
  const text = fs.readFileSync(CONFIG.CSV_PATH, 'utf-8');
  const rows = parseCSV(text);

  const sumMap = {};
  const nameMap = {};

  for (let i = 1; i < rows.length; i++) { // skip header
    const row = rows[i];
    const caseNum = row[0];
    const caseName = row[1];
    const sysName = row[8];
    const xVal = parseFloat(row[23]) || 0;

    if (!caseNum || !sysName) continue;

    const cn = caseNum.trim();
    const key = `${cn}__${sysName}`;

    sumMap[key] = (sumMap[key] || 0) + xVal;
    if (caseName && !nameMap[cn]) nameMap[cn] = caseName;
  }

  console.log(`[csv] ユニークキー数: ${Object.keys(sumMap).length}`);
  return { sumMap, nameMap };
}

// ── 3. マッチング ──────────────────────────────────────────────────
function matchRows(mdRows, sumMap, nameMap) {
  const matched = [];
  const unmatched = [];

  for (const row of mdRows) {
    const key = `${row.caseNum}__${row.sysName}`;
    if (sumMap[key] !== undefined) {
      matched.push({
        ...row,
        caseName: nameMap[row.caseNum] || '',
        totalEffort: sumMap[key],
      });
    } else {
      unmatched.push(row);
    }
  }

  console.log(`[match] 成功: ${matched.length}, 失敗: ${unmatched.length}`);
  return { matched, unmatched };
}

// ── 4. unmatchedをログ出力 ─────────────────────────────────────────
function writeUnmatchedLog(unmatched) {
  if (unmatched.length === 0) {
    console.log('[log] unmatchedなし');
    return;
  }

  fs.mkdirSync(path.dirname(CONFIG.UNMATCHED_LOG), { recursive: true });

  const lines = [
    `unmatched log - ${new Date().toISOString()}`,
    `合計: ${unmatched.length}行`,
    '',
    '案件管理番号 | システム名 | 対応内容',
    '-'.repeat(80),
    ...unmatched.map(r => `${r.caseNum} | ${r.sysName} | ${r.content}`),
  ];

  fs.writeFileSync(CONFIG.UNMATCHED_LOG, lines.join('\n'), 'utf-8');
  console.log(`[log] unmatchedログ出力: ${CONFIG.UNMATCHED_LOG} (${unmatched.length}行)`);
}

// ── 5. Dify API呼び出し ───────────────────────────────────────────
async function callDify(inputTable) {
  const res = await fetch(`${CONFIG.DIFY_BASE_URL}/v1/workflows/run`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${CONFIG.API_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      inputs: { input_table: inputTable },
      response_mode: 'blocking',
      user: CONFIG.USER_ID,
    }),
  });

  const body = await res.json();
  if (!res.ok) throw new Error(`Dify error: ${res.status} ${JSON.stringify(body)}`);

  const resultStr = body?.data?.outputs?.result || '[]';
  try {
    return JSON.parse(resultStr);
  } catch {
    throw new Error(`Dify response parse error: ${resultStr}`);
  }
}

// ── 6. グループ単位でDify呼び出し → 工数を付与 ────────────────────
async function calcEffortWithDify(matched) {
  const groups = {};
  for (const row of matched) {
    const key = `${row.caseNum}__${row.sysName}`;
    if (!groups[key]) groups[key] = [];
    groups[key].push(row);
  }

  const result = [];
  const groupKeys = Object.keys(groups);
  console.log(`[dify] グループ数: ${groupKeys.length}`);

  for (let i = 0; i < groupKeys.length; i++) {
    const key = groupKeys[i];
    const rows = groups[key];
    const { caseNum, sysName, caseName, totalEffort } = rows[0];

    console.log(`[dify] ${i + 1}/${groupKeys.length} ${caseNum} | ${sysName} (${rows.length}行, 合計${totalEffort}人月)`);

    const tableHeader = '| 案件管理番号 | システム名 | 対応内容 | 工数（人月） |';
    const tableSep = '|---|---|---|---|';
    const tableRows = rows.map(r =>
      `| ${caseNum} | ${sysName} | ${r.content} | ${totalEffort} |`
    );
    const inputTable = [tableHeader, tableSep, ...tableRows].join('\n');

    let difyResult;
    try {
      difyResult = await callDify(inputTable);
    } catch (err) {
      console.error(`  [dify] エラー: ${err.message} → 工数を均等按分`);
      const avg = Math.round((totalEffort / rows.length) * 100) / 100;
      difyResult = rows.map(r => ({ 対応内容: r.content, 工数: avg }));
    }

    const difyEfforts = rows.map(row => {
      const difyRow = difyResult.find(d => d['対応内容'] === row.content);
      return difyRow ? difyRow['工数'] : null;
    });

    const difySum = difyEfforts.reduce((s, v) => s + (v || 0), 0);
    let normalizedEfforts;

    if (difySum <= 0) {
      console.log(`  [normalize] Dify合計=0 → 均等按分`);
      const avg = Math.round((totalEffort / rows.length) * 100) / 100;
      normalizedEfforts = rows.map(() => avg);
    } else {
      const scale = totalEffort / difySum;
      if (Math.abs(difySum - totalEffort) > 0.001) {
        console.log(`  [normalize] Dify合計=${difySum.toFixed(3)} → 補正倍率=${scale.toFixed(4)}`);
      }
      normalizedEfforts = difyEfforts.map(v =>
        v !== null ? Math.round(v * scale * 100) / 100 : Math.round((totalEffort / rows.length) * 100) / 100
      );
    }

    const assignedSum = normalizedEfforts.slice(0, -1).reduce((s, v) => s + v, 0);
    normalizedEfforts[normalizedEfforts.length - 1] = Math.round((totalEffort - assignedSum) * 100) / 100;

    for (let j = 0; j < rows.length; j++) {
      result.push({
        caseNum: rows[j].caseNum,
        caseName,
        sysName: rows[j].sysName,
        content: rows[j].content,
        totalEffort,
        effort: normalizedEfforts[j],
      });
    }
  }

  return result;
}

// ── 7. CSV出力 ────────────────────────────────────────────────────
function writeCSV(outputRows) {
  fs.mkdirSync(path.dirname(CONFIG.OUTPUT_PATH), { recursive: true });

  const headers = ['案件管理番号', '案件名', 'システム名', '対応内容', '合計工数（人月）', '工数（人月）'];
  const lines = [headers.map(csvField).join(',')];

  for (const row of outputRows) {
    lines.push([
      row.caseNum,
      row.caseName,
      row.sysName,
      row.content,
      row.totalEffort,
      row.effort,
    ].map(csvField).join(','));
  }

  fs.writeFileSync(CONFIG.OUTPUT_PATH, lines.join('\n'), 'utf-8');
  console.log(`[output] CSV出力完了: ${CONFIG.OUTPUT_PATH} (${outputRows.length}行)`);
}

// ── Main ──────────────────────────────────────────────────────────
async function main() {
  console.log('=== 対応内容別工数一覧 生成スクリプト ===\n');

  const mdRows = parseMdFiles();
  const { sumMap, nameMap } = buildCSVLookup();
  const { matched, unmatched } = matchRows(mdRows, sumMap, nameMap);

  writeUnmatchedLog(unmatched);

  const outputRows = await calcEffortWithDify(matched);
  writeCSV(outputRows);

  console.log('\n=== 完了 ===');
  console.log(`出力: ${CONFIG.OUTPUT_PATH}`);
  if (unmatched.length > 0) {
    console.log(`未マッチlog: ${CONFIG.UNMATCHED_LOG}`);
  }
}

main().catch(err => {
  console.error('Fatal:', err.message);
  process.exit(1);
});
