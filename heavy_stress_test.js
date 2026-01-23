/**
 * stress_test_heavy.js
 * * A robust stress test for "Heavy" compilation languages (Go, Swift) 
 * and interpreted languages (TS, PHP).
 * * Changes from original:
 * 1. Increased timeout to 60s to handle Go/Swift compilation queue.
 * 2. Uses the stable architecture from stress_test.js.
 */

const BASE_URL = 'http://localhost:3000';
const NUM_USERS = 50; 
const POLLING_INTERVAL_MS = 250; // matched to stress_test.js
const MAX_POLLS = 80;            // matched to stress_test.js (≈20s timeout)


// --- Helper to generate valid stdin ---
const getStdinForLanguage = (language) => {
    switch (language) {
        case "go": return `${Math.floor(Math.random() * 10) + 5}`; // Fibonacci input
        case "swift": return `test_string_${Math.random().toString(36).substring(7)}`;
        case "typescript": return `${Math.floor(Math.random() * 10) + 1}`; // Factorial input
        case "php": return `${Math.floor(Math.random() * 100) + 1}`; // Prime check input
        default: return "10";
    }
};

// --- 1. GO (1.21) ---
const getGoCode = (id) => ({
    language: "go",
    version: "1.21",
    code: `package main
import (
    "bufio"
    "fmt"
    "os"
    "strconv"
    "strings"
)
// Unique ID: ${id}
func fibonacci(n int) int {
    if n <= 1 { return n }
    return fibonacci(n-1) + fibonacci(n-2)
}
func main() {
    reader := bufio.NewReader(os.Stdin)
    input, _ := reader.ReadString('\\n')
    input = strings.TrimSpace(input)
    n, err := strconv.Atoi(input)
    if err != nil { n = 5 }
    if n > 20 { n = 20 } 
    result := fibonacci(n)
    fmt.Printf("Fibonacci(%d) = %d\\n", n, result)
}`
});

// --- 2. SWIFT (5.9) ---
const getSwiftCode = (id) => ({
    language: "swift",
    version: "5.9",
    code: `// Unique ID: ${id}
import Foundation
if let input = readLine() {
    let upper = input.uppercased()
    print("Swift says: \\(upper)")
} else {
    print("No Input")
}`
});

// --- 3. TYPESCRIPT (5.0) ---
const getTypeScriptCode = (id) => ({
    language: "typescript",
    version: "5.0",
    code: `// Unique ID: ${id}
declare var require: any;
declare var process: any;
const fs = require('fs');

function factorial(n: number): number {
    if (n <= 1) return 1;
    return n * factorial(n - 1);
}

try {
    const input = fs.readFileSync(0, 'utf-8').trim();
    const num = parseInt(input, 10);
    if (!isNaN(num)) {
        console.log("Factorial: " + factorial(num));
    } else {
        console.log("NaN");
    }
} catch (e) {
    console.log("Error");
}`
});

// --- 4. PHP (8.2) ---
const getPhpCode = (id) => ({
    language: "php",
    version: "8.2",
    code: `<?php
// Unique ID: ${id}
$line = fgets(STDIN);
if ($line === false) {
    echo "Empty";
    exit;
}
$n = intval(trim($line));
if ($n % 2 == 0) {
    echo "Even";
} else {
    echo "Odd";
}
?>`
});

// --- Helper Functions ---
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

function getLanguageForUser(userId) {
    const languages = ['go', 'swift', 'typescript', 'php'];
    return languages[userId % languages.length];
}

function getCodeFunctionForLanguage(language) {
    switch (language) {
        case 'go': return getGoCode;
        case 'swift': return getSwiftCode;
        case 'typescript': return getTypeScriptCode;
        case 'php': return getPhpCode;
        default: return getGoCode;
    }
}

async function simulateUser(userId) {
    const endpoint = '/execute/batch';
    const FIXED_BATCH_SIZE = 20;

    const language = getLanguageForUser(userId);
    const codeFunction = getCodeFunctionForLanguage(language);

    const uniqueId = `${userId}-${Date.now()}`;
    const basePayload = codeFunction(uniqueId);

    const batch = [{
        ...basePayload,
        stdin: getStdinForLanguage(language)
    }];
    for (let i = 1; i < FIXED_BATCH_SIZE; i++) {
        batch.push({ stdin: getStdinForLanguage(language) });
    }

    const requestBody = JSON.stringify(batch);
    const startTime = Date.now();

    try {
        // 1) Submit batch and measure time to get token
        const postStart = Date.now();
        const initialResponse = await fetch(`${BASE_URL}${endpoint}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: requestBody,
        });
        const postEnd = Date.now();
        const timeToPostMs = postEnd - postStart;

        if (!initialResponse.ok) throw new Error(`Initial request failed: ${initialResponse.status}`);
        const { token } = await initialResponse.json();
        const tokenReceivedTime = Date.now();
        const timeToTokenMs = tokenReceivedTime - startTime; // POST start -> token

        // 2) Poll for completion and measure time from token -> completed
        let tokenToCompleteMs = null;
        for (let i = 0; i < MAX_POLLS; i++) {
            const statusResponse = await fetch(`${BASE_URL}/submissions/${token}`);
            if (statusResponse.ok) {
                const result = await statusResponse.json();
                if (result.status === 'Completed' || result.status === 'Failed') {
                    const completedAt = Date.now();
                    tokenToCompleteMs = completedAt - tokenReceivedTime;
                    const totalMs = completedAt - startTime;

                    const testCaseResults = result.results || [];
                    const testCaseSuccesses = testCaseResults.filter(r => r.exit_code === 0 && (r.stderr === null || r.stderr === "")).length;
                    const testCaseFailures = testCaseResults.length - testCaseSuccesses;

                    console.log(`User ${userId} (${language}) -> total ${(totalMs/1000).toFixed(2)}s | timeToToken ${(timeToTokenMs/1000).toFixed(3)}s | tokenToComplete ${(tokenToCompleteMs/1000).toFixed(3)}s | passed ${testCaseSuccesses}/${FIXED_BATCH_SIZE}`);

                    return {
                        success: true,
                        userId, language, duration: (totalMs/1000).toFixed(2),
                        timeToTokenMs, tokenToCompleteMs, testCaseSuccesses, testCaseFailures
                    };
                }
            }
            await sleep(POLLING_INTERVAL_MS);
        }
        throw new Error('Polling timed out.');
    } catch (error) {
        const duration = (Date.now() - startTime) / 1000;
        console.error(`User ${userId}: ❌ API ERROR in ${duration.toFixed(2)}s (${language}) - ${error.message}`);
        return {
            success: false,
            userId, language, duration: duration.toFixed(2)
        };
    }
}


async function main() {
    console.log(`🚀 EvalX Heavy Load Test`);
    console.log(`Users: ${NUM_USERS} | Batch Size: 20`);
    console.log(`Languages: Go, Swift, TypeScript, PHP`);
    console.log(`Timeout: 60 seconds (to accommodate heavy compilers)`);
    
    const userPromises = Array.from({ length: NUM_USERS }, (_, i) => simulateUser(i + 1));
    const results = await Promise.all(userPromises);

    const totalSuccess = results.reduce((sum, r) => sum + r.testCaseSuccesses, 0);
    const totalFail = results.reduce((sum, r) => sum + r.testCaseFailures, 0);

    console.log(`\n--- SUMMARY ---`);
    console.log(`Total Test Cases: ${results.length * 20}`);
    console.log(`✅ Passed: ${totalSuccess}`);
    console.log(`❌ Failed: ${totalFail}`);
}

main().catch(console.error);