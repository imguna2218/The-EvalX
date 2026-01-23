const BASE_URL = 'http://18.61.203.61:3000'; // <--- VERIFY THIS IS YOUR CURRENT SERVER IP
const NUM_USERS = 50; 
const BATCH_SIZE = 20;

// Helper for unique input
const getStdinForLanguage = (language) => {
    return `${Math.floor(Math.random() * 10000)}`; // Random input to ensure execution
};

const getCCode = (id) => ({
    language: "c",
    version: "11",
    code: `#include <stdio.h>
// Unique ID: ${id}
int main() {
    int num;
    if (scanf("%d", &num)) { printf("Processed: %d\\n", num); }
    return 0;
}`
});

const codeSnippets = [getCCode]; // We only need one language to test scaling

async function fireBatch(batchId) {
    const requests = [];
    
    // Generate 50 users * 20 requests = 1000 jobs
    for (let i = 0; i < NUM_USERS; i++) {
        const uniqueId = `Batch${batchId}-User${i}-${Date.now()}`; // Unique ID prevents Cache Hits
        const payload = getCCode(uniqueId);
        
        // Build the batch for this user
        const batch = [{ ...payload, stdin: getStdinForLanguage('c') }];
        for (let j = 1; j < BATCH_SIZE; j++) {
            batch.push({ stdin: getStdinForLanguage('c') });
        }
        
        // Push the fetch promise (Fire and Forget)
        requests.push(fetch(`${BASE_URL}/execute/batch`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(batch),
        }).catch(e => null)); // Ignore network errors, just keep pushing
    }

    await Promise.all(requests);
    console.log(`[${new Date().toLocaleTimeString()}] 🚀 Batch ${batchId} fired! Added ${NUM_USERS * BATCH_SIZE} jobs to the queue.`);
}

async function main() {
    console.log("🌊 STARTING FLOOD TEST (Ctrl+C to stop)");
    console.log(`Target: ${BASE_URL}`);
    let batchId = 1;

    while (true) {
        await fireBatch(batchId++);
        // Wait 5 seconds. Workers take time to compile; this keeps the queue growing.
        await new Promise(r => setTimeout(r, 5000)); 
    }
}

main();