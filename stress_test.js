const BASE_URL = 'http://localhost:3000';
const NUM_USERS = 50; // Number of concurrent users
const POLLING_INTERVAL_MS = 250;
const MAX_POLLS = 80; // 80 * 250ms = 20 seconds timeout

// --- Helper to generate valid stdin for each language ---
const getStdinForLanguage = (language) => {
    switch (language) {
        case "c":
            return `${Math.floor(Math.random() * 10) + 5}`; // Factorial of 5-14
        case "cpp":
            return `A_random_string_${Math.random().toString(36).substring(7)}`;
        case "python":
            return `${Math.floor(Math.random() * 150) + 50}`; // Primes up to 50-199
        case "java24":
            return "the quick brown fox jumps over the lazy dog";
        default:
            return "default_input";
    }
};

// --- Code Snippets for Different Languages ---
const getCCode = (id) => ({
    language: "c",
    version: "11",
    code: `#include <stdio.h>
// Unique ID: ${id}
int factorial(int n) {
    if (n < 0) return -1;
    if (n <= 1) return 1;
    return n * factorial(n - 1);
}
int main() {
    int num = 0;
    scanf("%d", &num);
    printf("Factorial of %d is %d\\n", num, factorial(num));
    return 0;
}`
});

const getCppCode = (id) => ({
    language: "cpp",
    version: "11",
    code: `#include <iostream>
#include <string>
#include <algorithm>
// Unique ID: ${id}
int main() {
    std::string line;
    std::getline(std::cin, line);
    std::reverse(line.begin(), line.end());
    std::cout << "Reversed string: " << line << std::endl;
    return 0;
}`
});

const getPythonCode = (id) => ({
    language: "python",
    version: "3.9",
    code: `import sys
# Unique ID: ${id}  <-- CORRECTED: Was '//' which is a SyntaxError in Python
def is_prime(n):
    if n < 2: return False
    for i in range(2, int(n**0.5) + 1):
        if n % i == 0: return False
    return True
try:
    limit = int(sys.stdin.readline())
    primes = [str(i) for i in range(2, limit + 1) if is_prime(i)]
    print(f"Primes up to {limit}: {', '.join(primes)}")
except (ValueError, IndexError):
    print("Invalid input. Please provide an integer.")`
});

const getJavaCode = (id) => ({
    language: "java24",
    version: "24",
    code: `import java.util.HashMap;
import java.util.Map;
import java.util.Scanner; // ADDED: Import Scanner
// Unique ID: ${id}
void main() {
    // CORRECTED: System.console() is null in a non-interactive sandbox.
    // Use Scanner(System.in) to read the redirected stdin.
    Scanner scanner = new Scanner(System.in);
    String text = scanner.nextLine(); 
    
    String[] words = text.toLowerCase().split("\\\\s+");
    Map<String, Integer> wordCount = new HashMap<>();
    for (String word : words) {
        wordCount.put(word, wordCount.getOrDefault(word, 0) + 1);
    }
    wordCount.forEach((key, value) -> System.out.println(key + ": " + value));
}`
});

const codeSnippets = [getCCode, getCppCode, getPythonCode, getJavaCode];

// --- Helper Functions ---
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Language distribution helper for consistent mix
function getLanguageForUser(userId) {
    const languages = ['c', 'cpp', 'python', 'java24'];
    return languages[userId % languages.length];
}

// Get code function based on language
function getCodeFunctionForLanguage(language) {
    switch (language) {
        case 'c': return getCCode;
        case 'cpp': return getCppCode;
        case 'python': return getPythonCode;
        case 'java24': return getJavaCode;
        default: return getCCode;
    }
}

async function simulateUser(userId) {
    const endpoint = '/execute/batch'; // Always use batch endpoint
    const FIXED_BATCH_SIZE = 20; // Fixed batch size as requested
    
    // Get language with consistent distribution
    const language = getLanguageForUser(userId);
    const codeFunction = getCodeFunctionForLanguage(language);
    
    const uniqueId = `${userId}-${Date.now()}`;
    const basePayload = codeFunction(uniqueId);

    // Create batch with exactly 15 test cases
    const batch = [{
        ...basePayload,
        stdin: getStdinForLanguage(language)
    }];
    
    // Add additional test cases to reach fixed batch size of 15
    for (let i = 1; i < FIXED_BATCH_SIZE; i++) {
        batch.push({ 
            stdin: getStdinForLanguage(language) 
        });
    }
    
    const requestBody = JSON.stringify(batch);
    const requestDescription = `User ${userId}: Sent ${language} to ${endpoint} (${FIXED_BATCH_SIZE} test cases)`;

    const startTime = Date.now();
    try {
        const initialResponse = await fetch(`${BASE_URL}${endpoint}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: requestBody,
        });

        if (!initialResponse.ok) throw new Error(`Initial request failed with status ${initialResponse.status}`);
        const { token } = await initialResponse.json();

        for (let i = 0; i < MAX_POLLS; i++) {
            const statusResponse = await fetch(`${BASE_URL}/submissions/${token}`);
            if (statusResponse.ok) {
                const result = await statusResponse.json();
                if (result.status === 'Completed' || result.status === 'Failed') {
                    const duration = (Date.now() - startTime) / 1000;
                    
                    // Check if the result *itself* indicates failure
                    const testCaseResults = result.results || [];
                    const testCaseSuccesses = testCaseResults.filter(r => r.exit_code === 0 && (r.stderr === null || r.stderr === "")).length;
                    const testCaseFailures = testCaseResults.length - testCaseSuccesses;
                    
                    if (testCaseFailures > 0) {
                         // Log as error if any test case failed
                        console.error(`User ${userId}: ❌ FAILED in ${duration.toFixed(2)}s (${language}) (${testCaseSuccesses}/${FIXED_BATCH_SIZE} passed)`);
                    } else {
                        console.log(`User ${userId}: ✅ SUCCESS in ${duration.toFixed(2)}s (${language}) (${FIXED_BATCH_SIZE} test cases)`);
                    }

                    return { 
                        success: true, // API request succeeded
                        description: requestDescription, 
                        result, 
                        userId, 
                        language: language, 
                        duration: duration.toFixed(2), 
                        numTestCases: FIXED_BATCH_SIZE,
                        testCaseSuccesses, // Report actual test case results
                        testCaseFailures
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
            success: false, // API request failed
            description: requestDescription, 
            error: error.message, 
            userId, 
            language: language, 
            duration: duration.toFixed(2), 
            numTestCases: FIXED_BATCH_SIZE,
            testCaseSuccesses: 0,
            testCaseFailures: FIXED_BATCH_SIZE
        };
    }
}

// --- Main Execution ---
async function main() {
    console.log(`🚀 Starting EvalX Load Test`);
    console.log('====================================================');
    console.log(`Concurrent Users: ${NUM_USERS}`);
    console.log(`Request Type: /execute/batch only`);
    console.log(`Batch Size: 20 test cases per request`);
    console.log(`Target Total Test Cases: ${NUM_USERS * 20}`);
    console.log(`Language Distribution: C, C++, Python, Java24 (round-robin)`);
    console.log('====================================================\n');

    const userPromises = Array.from({ length: NUM_USERS }, (_, i) => simulateUser(i + 1));
    const allResults = await Promise.all(userPromises);

    console.log('\n\n✅ All requests have completed. Final Summary:');
    console.log('====================================================');

    const userSuccesses = allResults.filter(r => r.success).length; // API Success
    const userFailures = allResults.length - userSuccesses; // API Failures
    const totalTestCases = allResults.reduce((sum, r) => sum + r.numTestCases, 0);
    const testCaseSuccesses = allResults.reduce((sum, r) => sum + r.testCaseSuccesses, 0);
    const testCaseFailures = allResults.reduce((sum, r) => sum + r.testCaseFailures, 0);

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
    
    if (userFailures > 0) {
        console.log("\nDetails for failed API requests:");
        allResults.forEach(({ success, description, error }) => {
            if (!success) {
                console.log(`- ${description} -> ERROR: ${error}`);
            }
        });
    }

    console.log('\nDetailed Results (JSON):');
    console.log(JSON.stringify(allResults.map(({ userId, language, duration, numTestCases, success, error, result, testCaseSuccesses, testCaseFailures }) => ({
        userId,
        language,
        duration,
        numTestCases,
        apiSuccess: success,
        error: success ? undefined : error,
        // result: success ? result : undefined, // Optionally hide full result on API failure
        result,
        testCaseSuccesses,
        testCaseFailures
    })), null, 2));
}

main().catch(console.error);