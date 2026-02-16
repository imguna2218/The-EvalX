const BASE_URL = 'http://localhost:3000';
const NUM_USERS = 50; 
const BATCH_SIZE = 20;
const GLOBAL_POLL_INTERVAL_MS = 100;

const getCCode = () => ({
    language: "c", version: "11",
    code: `#include <stdio.h>\nint main() { printf("C_OK_INTERNAL"); return 0; }`
});

const getCppCode = () => ({
    language: "cpp", version: "11",
    code: `#include <iostream>\nint main() { std::cout << "CPP_OK_INTERNAL"; return 0; }`
});

const getPythonCode = () => ({
    language: "python", version: "3.9",
    code: `print("PY_OK_INTERNAL")`
});

const getJavaCode = () => ({
    language: "java21", version: "21",
    code: `public class Main { public static void main(String[] args) { System.out.println("JAVA_OK_INTERNAL"); } }`
});

const getPayload = (id) => {
    const configs = [getCCode, getCppCode, getPythonCode, getJavaCode];
    const config = configs[id % configs.length]();
    return Array(BATCH_SIZE).fill(null).map(() => ({ ...config, stdin: "10", timeout: 10 }));
};

async function runStressTest() {
    console.log(`🚀 STARTING BURST TEST: ${NUM_USERS} Users | ${NUM_USERS * BATCH_SIZE} Internal Executions`);
    const globalStart = Date.now();
    
    // 1. ATOMIC SUBMISSION BURST
    const submissionPromises = Array.from({ length: NUM_USERS }, async (_, i) => {
        const userId = i + 1;
        const body = getPayload(i);
        const lang = body[0].language;
        try {
            const res = await fetch(`${BASE_URL}/execute/batch`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body)
            });
            const { token } = await res.json();
            console.log(`📡 [User ${userId.toString().padStart(2, '0')}] 🚀 Request Queued (${lang})`);
            return { userId, token, lang, start: Date.now(), done: false };
        } catch (e) {
            console.error(`❌ [User ${userId}] Submission Error: ${e.message}`);
            return { userId, error: e.message, done: true };
        }
    });

    let activeTasks = (await Promise.all(submissionPromises)).filter(t => !t.error);
    const finalMetrics = [];

    // 2. CONCURRENT COLLECTOR LOOP
    let completed = 0;
    while (completed < activeTasks.length) {
        await Promise.all(activeTasks.map(async (task) => {
            if (task.done) return;

            try {
                const res = await fetch(`${BASE_URL}/submissions/${task.token}`);
                if (!res.ok) return;
                const data = await res.json();

                if (data.status === 'Completed' || data.status === 'Failed') {
                    task.done = true;
                    completed++;
                    
                    // METRIC CALCULATION: 
                    // Retrieval = Compile (1st case) + Sum of Run times (all cases)
                    const compileTime = data.results && data.results[0] ? (data.results[0].compile_time || 0) : 0;
                    const totalRunTime = data.results ? data.results.reduce((acc, r) => acc + (r.run_time || 0), 0) : 0;
                    const calculatedRetrieval = compileTime + totalRunTime;
                    
                    const proof = data.results && data.results[0] ? data.results[0].stdout.trim() : "N/A";
                    
                    console.log(`✅ [User ${task.userId.toString().padStart(2, '0')}] Internal Processing Finished (${task.lang})`);

                    finalMetrics.push({
                        User: task.userId,
                        Language: task.lang,
                        "Compile(s)": compileTime.toFixed(4),
                        "Run_Sum(s)": totalRunTime.toFixed(4),
                        "Retrieval(s)": calculatedRetrieval.toFixed(4), // Formula applied here
                        Proof: proof
                    });
                }
            } catch (e) { }
        }));
        if (completed < activeTasks.length) await new Promise(r => setTimeout(r, GLOBAL_POLL_INTERVAL_MS));
    }

    const wallClockTime = (Date.now() - globalStart) / 1000;

    // 3. GENERATE METRICS TABLE
    console.log('\n\n📊 ENGINE PERFORMANCE METRICS (QUEUE TIME EXCLUDED)');
    console.table(finalMetrics.sort((a, b) => a.User - b.User));

    const totalInternalTime = finalMetrics.reduce((a, b) => a + parseFloat(b["Retrieval(s)"]), 0);

    console.log('====================================================');
    console.log(`Total Test cases: ${NUM_USERS * 20}`);
    console.log(`Total Wall Clock Time:  ${wallClockTime.toFixed(2)}s (Real time spent)`);
    console.log(`Throughput:             ${(NUM_USERS / wallClockTime).toFixed(2)} batches/sec`);
    console.log(`Total Successes:        ${finalMetrics.length}/${NUM_USERS}`);
    console.log('====================================================');
}

runStressTest();