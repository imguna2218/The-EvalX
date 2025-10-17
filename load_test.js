// A comprehensive load testing script for the evalX application.
// This script simulates 12 concurrent users sending a variety of code execution
// requests to your local server and then fetches the results.
//
// How to Run:
// 1. Make sure you have Node.js (v18 or newer) installed.
// 2. Save this file as `load_test.js`.
// 3. Open your terminal and run the command: node load_test.js
//
// The script will then begin sending requests and printing the results as they come in.

const BASE_URL = 'http://localhost:3000';
const NUM_USERS = 12;
const POLLING_INTERVAL_MS = 500; // How often to check for results
const MAX_POLLS = 40; // Max attempts to poll for a result (40 * 500ms = 20 seconds)

// --- Code Snippets for Different Languages ---
// Each function takes a unique ID to ensure the code is different every time,
// preventing the server from returning a cached result.

const getCCode = (id) => ({
    language: "c",
    version: "11",
    code: `#include <stdio.h>
// Unique ID: ${id}
int factorial(int n) {
    if (n <= 1) return 1;
    return n * factorial(n - 1);
}
int main() {
    int num;
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
#include <vector>
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
# Unique ID: ${id}
def is_prime(n):
    if n < 2:
        return False
    for i in range(2, int(n**0.5) + 1):
        if n % i == 0:
            return False
    return True

try:
    limit = int(sys.stdin.readline())
    primes = [str(i) for i in range(2, limit + 1) if is_prime(i)]
    print(f"Primes up to {limit}: {', '.join(primes)}")
except (ValueError, IndexError):
    print("Invalid input. Please provide an integer.")`
});

const getJavaCode = (id) => ({
    language: "java11",
    version: "11",
    code: `import java.util.Scanner;
import java.util.HashMap;
import java.util.Map;
// Unique ID: ${id}
public class Main {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Analyzing word frequency...");
        String text = scanner.nextLine();
        String[] words = text.toLowerCase().split("\\\\s+");
        Map<String, Integer> wordCount = new HashMap<>();
        for (String word : words) {
            wordCount.put(word, wordCount.getOrDefault(word, 0) + 1);
        }
        System.out.println("Word Count Result:");
        wordCount.forEach((key, value) -> System.out.println(key + ": " + value));
    }
}`
});

const codeSnippets = [getCCode, getCppCode, getPythonCode, getJavaCode];

// --- Helper Functions ---

// A simple delay function
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// This function simulates a single user's actions
async function simulateUser(userId) {
    // 1. Randomly select a request type and language
    const isBatch = Math.random() > 0.5;
    const endpoint = isBatch ? '/execute/batch' : '/execute';
    const randomCodeFunc = codeSnippets[Math.floor(Math.random() * codeSnippets.length)];
    const uniqueId = `${userId}-${Date.now()}`;
    const basePayload = randomCodeFunc(uniqueId);

    let requestBody;
    let requestDescription;

    // 2. Construct the request body
    if (isBatch) {
        const numTestCases = Math.floor(Math.random() * 19) + 2; // 2 to 20 test cases
        const batch = [{ ...basePayload, stdin: `BatchInput-1-for-${basePayload.language}` }];
        for (let i = 2; i <= numTestCases; i++) {
            batch.push({ stdin: `BatchInput-${i}-for-${basePayload.language}` });
        }
        requestBody = JSON.stringify(batch);
        requestDescription = `User ${userId}: Sent ${basePayload.language} to ${endpoint} (${numTestCases} test cases)`;
    } else {
        requestBody = JSON.stringify({ ...basePayload, stdin: `SingleInput-for-${basePayload.language}` });
        requestDescription = `User ${userId}: Sent ${basePayload.language} to ${endpoint}`;
    }

    try {
        // 3. Send the initial request to get a token
        console.log(`User ${userId}: Sending request...`);
        const initialResponse = await fetch(`${BASE_URL}${endpoint}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: requestBody,
        });

        if (!initialResponse.ok) {
            const errorText = await initialResponse.text();
            throw new Error(`Initial request failed with status ${initialResponse.status}: ${errorText}`);
        }

        const { token } = await initialResponse.json();
        console.log(`User ${userId}: Got token ${token}`);

        // 4. Poll for the result
        for (let i = 0; i < MAX_POLLS; i++) {
            const statusResponse = await fetch(`${BASE_URL}/submissions/${token}`);
            if (!statusResponse.ok) {
                // Retry on server error, but don't log every time to avoid clutter
                await sleep(POLLING_INTERVAL_MS * (i + 1)); // Exponential backoff
                continue;
            }

            const result = await statusResponse.json();
            if (result.status === 'Completed' || result.status === 'Failed') {
                return { description: requestDescription, result };
            }

            // Wait before polling again
            await sleep(POLLING_INTERVAL_MS);
        }

        throw new Error('Polling timed out.');

    } catch (error) {
        return { description: requestDescription, error: error.message };
    }
}

// --- Main Execution ---

async function main() {
    console.log(`🚀 Starting EvalX load test with ${NUM_USERS} concurrent users...`);
    console.log('----------------------------------------------------');

    const userPromises = [];
    for (let i = 1; i <= NUM_USERS; i++) {
        userPromises.push(simulateUser(i));
    }

    // Wait for all users to complete their simulation
    const allResults = await Promise.all(userPromises);

    console.log('\n\n✅ All requests have completed. Final Results:');
    console.log('----------------------------------------------------');

    // Display the results
    allResults.forEach(({ description, result, error }) => {
        console.log(`\n====================================================`);
        console.log(description);
        console.log(`====================================================`);
        if (error) {
            console.error(`  ERROR: ${error}`);
        } else if (result) {
            console.log(`  Status: ${result.status}`);
            console.log(`  Token: ${result.token}`);
            if (result.results && result.results.length > 0) {
                result.results.forEach((res, index) => {
                    console.log(`\n  --- Result for Test Case ${index + 1} ---`);
                    console.log(`  Exit Code: ${res.exit_code}`);
                    console.log(`  Run Time: ${res.run_time.toFixed(4)}s`);
                    console.log(`  STDOUT:\n\`\`\`\n${res.stdout.trim()}\n\`\`\``);
                    if (res.stderr) {
                        console.log(`  STDERR:\n\`\`\`\n${res.stderr.trim()}\n\`\`\``);
                    }
                });
            } else {
                 console.log("  No results returned.");
            }
        }
        console.log(`====================================================\n`);
    });
}

main().catch(console.error);
