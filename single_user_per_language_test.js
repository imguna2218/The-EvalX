// single_user_batch20_per_language.js
const BASE_URL = 'http://localhost:3000';
const POLLING_INTERVAL_MS = 250;
const MAX_POLLS = 120; // allow longer for heavy compilers here

const sleep = ms => new Promise(r => setTimeout(r, ms));

const batchOf20 = (payload) => {
  const arr = [];
  for (let i = 0; i < 20; i++) {
    // for extra realism, include same code + varying stdin per case
    arr.push(Object.assign({}, payload, { stdin: String(Math.floor(Math.random()*10)+1) }));
  }
  return arr;
};

// minimal code payloads — adjust as your server expects
const payloads = {
  go: {
    language: "go",
    version: "1.21",
    code: `package main
import ("fmt"; "bufio"; "os")
func main(){fmt.Println("OK")}`,
  },
  swift: {
    language: "swift",
    version: "5.9",
    code: `import Foundation\nprint("OK")`,
  },
  typescript: {
    language: "typescript",
    version: "5.0",
    code: `console.log("OK");`,
  },
  php: {
    language: "php",
    version: "8.2",
    code: `<?php echo "OK\\n"; ?>`,
  }
};

async function runOne(langKey) {
  const payload = payloads[langKey];
  const batch = batchOf20(payload);
  const start = Date.now();

  const initial = await fetch(`${BASE_URL}/execute/batch`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(batch)
  });

  const timeToTokenMs = Date.now() - start;
  if (!initial.ok) {
    console.error(`${langKey}: initial POST failed ${initial.status}`);
    return;
  }
  const { token } = await initial.json();
  console.log(`\n--- ${langKey} (batch=20) ---`);
  console.log(`timeToToken ${(timeToTokenMs/1000).toFixed(3)}s  token=${token}`);

  // poll until completed
  for (let i = 0; i < MAX_POLLS; i++) {
    await sleep(POLLING_INTERVAL_MS);
    const st = await fetch(`${BASE_URL}/submissions/${token}`);
    if (!st.ok) {
      console.error(`${langKey}: poll status ${st.status}`);
      continue;
    }
    const res = await st.json();
    if (res.status === 'Completed' || res.status === 'Failed') {
      const totalMs = Date.now() - start;
      const tokenToCompleteMs = totalMs - timeToTokenMs;
      console.log(`${langKey}: COMPLETED status=${res.status} | total ${(totalMs/1000).toFixed(3)}s | timeToToken ${(timeToTokenMs/1000).toFixed(3)}s | tokenToComplete ${(tokenToCompleteMs/1000).toFixed(3)}s`);
      if (res.results && res.results.length) {
        const ok = res.results.filter(r => r.exit_code === 0).length;
        console.log(`   results: ${ok}/${res.results.length} passed`);
      }
      return;
    }
  }
  console.log(`${langKey}: POLLING TIMED OUT after ${(MAX_POLLS * POLLING_INTERVAL_MS)/1000}s`);
}

(async () => {
  for (const k of Object.keys(payloads)) {
    await runOne(k);
    await new Promise(r => setTimeout(r, 500)); // tiny gap between runs
  }
})();
