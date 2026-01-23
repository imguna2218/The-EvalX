const readline = require('readline');

// --- Configuration ---
const BASE_URL = 'http://98.130.50.3:3000';
const POLLING_INTERVAL_MS = 250;

// --- Java 21 Code Payload ---
const getJavaPayload = (userId) => ({
    language: "java21",
    version: "21",
    code: `import java.util.Scanner;
public class Main {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        String name = scanner.nextLine();
        System.out.println("Hello, " + name);
    }
}`,
    stdin: `User-${userId}`
});

// --- Helper Functions ---
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function simulateUser(userId, maxPolls) {
    const endpoint = '/execute'; // Single execution endpoint
    const payload = getJavaPayload(userId);
    const requestBody = JSON.stringify(payload);
    
    // We treat this single request as 1 test case
    const TOTAL_TEST_CASES = 1; 

    const startTime = Date.now();
    try {
        // 1. Send Execution Request
        const initialResponse = await fetch(`${BASE_URL}${endpoint}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: requestBody,
        });

        if (!initialResponse.ok) throw new Error(`Initial request failed with status ${initialResponse.status}`);
        const { token } = await initialResponse.json();

        // 2. Poll for Status
        for (let i = 0; i < maxPolls; i++) {
            const statusResponse = await fetch(`${BASE_URL}/submissions/${token}`);
            if (statusResponse.ok) {
                const result = await statusResponse.json();
                
                // Check if processing is finished
                if (result.status === 'Completed' || result.status === 'Failed') {
                    const duration = (Date.now() - startTime) / 1000;
                    
                    // Logic for Single Execution Result
                    // Look for exit_code 0 and empty/null stderr
                    const isSuccess = result.exit_code === 0 && (!result.stderr || result.stderr.trim() === "");
                    
                    const testCaseSuccesses = isSuccess ? 1 : 0;
                    const testCaseFailures = isSuccess ? 0 : 1;

                    if (!isSuccess) {
                         console.error(`User ${userId}: ❌ FAILED in ${duration.toFixed(2)}s`);
                    } else {
                        console.log(`User ${userId}: ✅ SUCCESS in ${duration.toFixed(2)}s`);
                    }

                    return { 
                        success: true, // The API cycle completed successfully
                        userId, 
                        duration: duration.toFixed(2), 
                        numTestCases: TOTAL_TEST_CASES,
                        testCaseSuccesses,
                        testCaseFailures,
                        error: isSuccess ? null : (result.stderr || "Non-zero exit code")
                    };
                }
            }
            await sleep(POLLING_INTERVAL_MS);
        }
        throw new Error('Polling timed out.');
    } catch (error) {
        const duration = (Date.now() - startTime) / 1000;
        console.error(`User ${userId}: ❌ API ERROR in ${duration.toFixed(2)}s - ${error.message}`);
        return { 
            success: false, // The API cycle failed (network error, timeout, etc)
            userId, 
            duration: duration.toFixed(2), 
            numTestCases: TOTAL_TEST_CASES,
            testCaseSuccesses: 0,
            testCaseFailures: TOTAL_TEST_CASES,
            error: error.message
        };
    }
}

// --- Main Execution ---
async function main() {
    const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout
    });

    rl.question('Enter number of concurrent users: ', async (input) => {
        const NUM_USERS = parseInt(input);

        if (isNaN(NUM_USERS) || NUM_USERS <= 0) {
            console.error("Please enter a valid number greater than 0.");
            process.exit(1);
        }

        rl.close();

        // Dynamic Timeout Calculation
        // Base 80 polls (20s). For every 50 users, add 5 seconds (20 polls) buffer to handle queue depth.
        const DYNAMIC_MAX_POLLS = 80 + Math.ceil(NUM_USERS / 2.5); 
        const TIMEOUT_SECONDS = (DYNAMIC_MAX_POLLS * POLLING_INTERVAL_MS) / 1000;

        console.log(`\n🚀 Starting EvalX Load Test (Single Mode)`);
        console.log('====================================================');
        console.log(`Concurrent Users:        ${NUM_USERS}`);
        console.log(`Request Type:            /execute (Single Java Request)`);
        console.log(`Calculated Timeout:      ~${TIMEOUT_SECONDS} seconds`);
        console.log('====================================================\n');

        // Create all promises
        const userPromises = Array.from({ length: NUM_USERS }, (_, i) => simulateUser(i + 1, DYNAMIC_MAX_POLLS));
        
        // Fire all requests concurrently
        const allResults = await Promise.all(userPromises);

        // --- Summary Calculation ---
        const userSuccesses = allResults.filter(r => r.success).length; // API Success (Network/Flow)
        const userFailures = allResults.length - userSuccesses; 
        
        const totalTestCases = allResults.reduce((sum, r) => sum + r.numTestCases, 0);
        const testCaseSuccesses = allResults.reduce((sum, r) => sum + r.testCaseSuccesses, 0);
        const testCaseFailures = allResults.reduce((sum, r) => sum + r.testCaseFailures, 0);

        console.log('\n\n✅ All requests have completed. Final Summary:');
        console.log('====================================================');
        console.log(`Total Number of Users: ${NUM_USERS}`);
        console.log(`Total Number of Test Cases: ${totalTestCases}`);
        console.log(`Total Requests: ${allResults.length}`);
        console.log('---------------------');
        console.log(`API Requests:`);
        console.log(`✅ Successes:    ${userSuccesses}`);
        console.log(`❌ Failures:     ${userFailures}`);
        console.log('---------------------');
        console.log(`Individual Testcases:`);
        console.log(`✅ Successes:    ${testCaseSuccesses}`);
        console.log(`❌ Failures:     ${testCaseFailures}`);
        console.log('====================================================');

        // Optional: Show error details if any API failures occurred
        if (userFailures > 0 || testCaseFailures > 0) {
            console.log("\n⚠️  Error Details (Sample):");
            const errors = allResults.filter(r => !r.success || r.testCaseFailures > 0).slice(0, 10);
            errors.forEach(e => {
                console.log(`User ${e.userId}: ${e.error}`);
            });
            if (errors.length < (userFailures + testCaseFailures)) {
                console.log(`...and ${(userFailures + testCaseFailures) - errors.length} more.`);
            }
        }
    });
}

main().catch(console.error);