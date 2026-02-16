// comprehensive_benchmark.js
const BASE_URL = 'http://localhost:3000';
const POLLING_INTERVAL_MS = 250;
const MAX_POLLS = 240; // 60 seconds max wait time

const sleep = ms => new Promise(r => setTimeout(r, ms));

// Test case generator - bubble sort with varying inputs
const generateBubbleSortTestCases = (language) => {
  const baseCode = {
    'c': `#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void bubbleSort(int arr[], int n) {
    for (int i = 0; i < n - 1; i++) {
        for (int j = 0; j < n - i - 1; j++) {
            if (arr[j] > arr[j + 1]) {
                int temp = arr[j];
                arr[j] = arr[j + 1];
                arr[j + 1] = temp;
            }
        }
    }
}

int main() {
    char input[10000];
    fgets(input, sizeof(input), stdin);
    input[strcspn(input, "\\n")] = 0;
    
    if (strlen(input) == 0) {
        printf("[]\\n");
        return 0;
    }
    
    int count = 1;
    for (char* p = input; *p != '\\0'; p++) {
        if (*p == ',') count++;
    }
    
    int* arr = (int*)malloc(count * sizeof(int));
    char* token = strtok(input, ",");
    int i = 0;
    while (token != NULL) {
        arr[i++] = atoi(token);
        token = strtok(NULL, ",");
    }
    
    bubbleSort(arr, count);
    
    printf("[");
    for (int i = 0; i < count; i++) {
        printf("%d", arr[i]);
        if (i < count - 1) printf(", ");
    }
    printf("]\\n");
    
    free(arr);
    return 0;
}`,

    'cpp': `#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <algorithm>

void bubbleSort(std::vector<int>& arr) {
    int n = arr.size();
    for (int i = 0; i < n - 1; i++) {
        for (int j = 0; j < n - i - 1; j++) {
            if (arr[j] > arr[j + 1]) {
                std::swap(arr[j], arr[j + 1]);
            }
        }
    }
}

int main() {
    std::string line;
    std::getline(std::cin, line);
    
    if (line.empty()) {
        std::cout << "[]" << std::endl;
        return 0;
    }
    
    std::vector<int> arr;
    std::stringstream ss(line);
    std::string token;
    
    while (std::getline(ss, token, ',')) {
        arr.push_back(std::stoi(token));
    }
    
    bubbleSort(arr);
    
    std::cout << "[";
    for (size_t i = 0; i < arr.size(); i++) {
        std::cout << arr[i];
        if (i < arr.size() - 1) std::cout << ", ";
    }
    std::cout << "]" << std::endl;
    
    return 0;
}`,

    'java21': `import java.util.Arrays;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        String[] input = sc.nextLine().split(",");
        int[] arr = new int[input.length];
        for (int i = 0; i < input.length; i++) {
            arr[i] = Integer.parseInt(input[i].trim());
        }
        
        bubbleSort(arr);
        System.out.println(Arrays.toString(arr));
    }
    
    public static void bubbleSort(int[] arr) {
        int n = arr.length;
        for (int i = 0; i < n - 1; i++) {
            for (int j = 0; j < n - i - 1; j++) {
                if (arr[j] > arr[j + 1]) {
                    int temp = arr[j];
                    arr[j] = arr[j + 1];
                    arr[j + 1] = temp;
                }
            }
        }
    }
}`,

    'python': `import sys

def bubble_sort(arr):
    n = len(arr)
    for i in range(n):
        for j in range(0, n - i - 1):
            if arr[j] > arr[j + 1]:
                arr[j], arr[j + 1] = arr[j + 1], arr[j]
    return arr

if __name__ == "__main__":
    input_str = sys.stdin.readline().strip()
    if input_str:
        arr = [int(x) for x in input_str.split(",")]
        sorted_arr = bubble_sort(arr)
        print(f"[{', '.join(map(str, sorted_arr))}]")
    else:
        print("[]")`,

    'javascript': `const readline = require('readline');

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  terminal: false
});

rl.on('line', (input) => {
  if (!input || input.trim() === '') {
    console.log('[]');
    return;
  }
  
  const arr = input.split(',').map(Number);
  const n = arr.length;
  
  for (let i = 0; i < n - 1; i++) {
    for (let j = 0; j < n - i - 1; j++) {
      if (arr[j] > arr[j + 1]) {
        const temp = arr[j];
        arr[j] = arr[j + 1];
        arr[j + 1] = temp;
      }
    }
  }
  
  console.log('[' + arr.join(', ') + ']');
});`,

    'typescript': `declare const process: any;
declare const require: any;

const fs = require("fs");

function bubbleSort(arr: number[]): number[] {
    const n = arr.length;
    for (let i = 0; i < n - 1; i++) {
        for (let j = 0; j < n - i - 1; j++) {
            if (arr[j] > arr[j + 1]) {
                const temp = arr[j];
                arr[j] = arr[j + 1];
                arr[j + 1] = temp;
            }
        }
    }
    return arr;
}

try {
    const stdinBuffer = fs.readFileSync(0);
    const input = stdinBuffer.toString().trim();
    
    if (!input) {
        console.log("[]");
    } else {
        const nums = input.split(",").map((x: string) => parseInt(x.trim(), 10));
        const sorted = bubbleSort(nums);
        console.log(\`[\${sorted.join(", ")}]\`);
    }
} catch (e) {
    console.log("[]");
}`,

    'go': `package main

import (
    "bufio"
    "fmt"
    "os"
    "strconv"
    "strings"
)

func bubbleSort(arr []int) []int {
    n := len(arr)
    for i := 0; i < n-1; i++ {
        for j := 0; j < n-i-1; j++ {
            if arr[j] > arr[j+1] {
                arr[j], arr[j+1] = arr[j+1], arr[j]
            }
        }
    }
    return arr
}

func main() {
    reader := bufio.NewReader(os.Stdin)
    input, _ := reader.ReadString('\\n')
    input = strings.TrimSpace(input)
    
    if input == "" {
        fmt.Println("[]")
        return
    }
    
    parts := strings.Split(input, ",")
    var arr []int
    for _, p := range parts {
        num, err := strconv.Atoi(strings.TrimSpace(p))
        if err == nil {
            arr = append(arr, num)
        }
    }
    
    sorted := bubbleSort(arr)
    
    fmt.Print("[")
    for i, num := range sorted {
        fmt.Print(num)
        if i < len(sorted)-1 {
            fmt.Print(", ")
        }
    }
    fmt.Println("]")
}`,

    'ruby': `input = $stdin.read
if input.nil? || input.strip.empty?
  puts "[]"
  exit
end

arr = input.split(',').map(&:to_i)

n = arr.length
loop do
  swapped = false
  (n - 1).times do |i|
    if arr[i] > arr[i + 1]
      arr[i], arr[i + 1] = arr[i + 1], arr[i]
      swapped = true
    end
  end
  break unless swapped
end

print "["
print arr.join(", ")
puts "]"`,

    'swift': `import Foundation

func bubbleSort(_ array: inout [Int]) {
    guard array.count > 1 else { return }
    for i in 0..<array.count {
        for j in 0..<array.count - i - 1 {
            if array[j] > array[j + 1] {
                let temp = array[j]
                array[j] = array[j + 1]
                array[j + 1] = temp
            }
        }
    }
}

if let line = readLine() {
    let trimmed = line.trimmingCharacters(in: .whitespacesAndNewlines)
    if !trimmed.isEmpty {
        let parts = trimmed.split(separator: ",")
        var numbers = parts.compactMap { Int($0.trimmingCharacters(in: .whitespaces)) }
        bubbleSort(&numbers)
        let result = numbers.map { String($0) }.joined(separator: ", ")
        print("[\\\\(result)]")
    } else {
        print("[]")
    }
} else {
    print("[]")
}`,

    'php': `<?php
$line = fgets(STDIN);

if ($line === false || trim($line) === "") {
    echo "[]";
    exit;
}

$parts = explode(",", trim($line));
$arr = [];
foreach ($parts as $p) {
    $val = trim($p);
    if ($val !== "") {
        $arr[] = intval($val);
    }
}

$n = count($arr);
for ($i = 0; $i < $n; $i++) {
    for ($j = 0; $j < $n - $i - 1; $j++) {
        if ($arr[$j] > $arr[$j + 1]) {
            $temp = $arr[$j];
            $arr[$j] = $arr[$j + 1];
            $arr[$j + 1] = $temp;
        }
    }
}

echo "[" . implode(", ", $arr) . "]";
?>`,

    'r': `input <- readLines("stdin", n=1)
if (length(input) == 0) {
  cat("[]\\n")
  quit()
}

parts <- strsplit(input, ",")[[1]]
arr <- as.integer(parts)

n <- length(arr)
for (i in 1:(n-1)) {
  for (j in 1:(n-i)) {
    if (arr[j] > arr[j+1]) {
      temp <- arr[j]
      arr[j] <- arr[j+1]
      arr[j+1] <- temp
    }
  }
}

cat("[")
cat(paste(arr, collapse=", "))
cat("]\\n")`,

    'csharp': `using System;
using System.Collections.Generic;
using System.Linq;

class Program {
    static void Main() {
        string input = Console.ReadLine();
        
        if (string.IsNullOrEmpty(input)) {
            Console.WriteLine("[]");
            return;
        }
        
        string[] parts = input.Split(',');
        List<int> arr = new List<int>();
        
        foreach (string part in parts) {
            if (int.TryParse(part, out int num)) {
                arr.Add(num);
            }
        }
        
        int n = arr.Count;
        for (int i = 0; i < n - 1; i++) {
            for (int j = 0; j < n - i - 1; j++) {
                if (arr[j] > arr[j + 1]) {
                    int temp = arr[j];
                    arr[j] = arr[j + 1];
                    arr[j + 1] = temp;
                }
            }
        }
        
        Console.Write("[");
        for (int i = 0; i < arr.Count; i++) {
            Console.Write(arr[i]);
            if (i < arr.Count - 1) {
                Console.Write(", ");
            }
        }
        Console.WriteLine("]");
    }
}`,

    'kotlin': `import java.util.Scanner

fun bubbleSort(arr: MutableList<Int>): MutableList<Int> {
    val n = arr.size
    for (i in 0 until n - 1) {
        for (j in 0 until n - i - 1) {
            if (arr[j] > arr[j + 1]) {
                val temp = arr[j]
                arr[j] = arr[j + 1]
                arr[j + 1] = temp
            }
        }
    }
    return arr
}

fun main() {
    val scanner = Scanner(System.\`in\`)
    val input = scanner.nextLine()
    
    if (input.isEmpty()) {
        println("[]")
        return
    }
    
    val parts = input.split(",")
    val arr = mutableListOf<Int>()
    for (part in parts) {
        try {
            arr.add(part.trim().toInt())
        } catch (e: NumberFormatException) {
            // ignore non-integer values
        }
    }
    
    val sortedArr = bubbleSort(arr)
    
    print("[")
    for (i in sortedArr.indices) {
        print(sortedArr[i])
        if (i < sortedArr.size - 1) {
            print(", ")
        }
    }
    println("]")
}`
  };
  
  // First check if language exists
  const code = baseCode[language];
  if (code) {
    return code;
  } else {
    console.warn(`Warning: No code template for ${language}, using C as fallback`);
    return baseCode['c']; // Make sure this is actually C code
  }
};

// Test inputs of varying sizes
const testInputs = [
  // Small (10 elements)
  "10,9,8,7,6,5,4,3,2,1",
  
  // Medium (50 elements) - reverse sorted
  "50,49,48,47,46,45,44,43,42,41,40,39,38,37,36,35,34,33,32,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1",
  
  // Medium (50 elements) - random
  "50,12,1,99,4,33,76,100,2,5,8,9,10,44,22,11,66,77,88,33,21,43,65,87,98,12,34,56,78,90,1,3,5,7,9,2,4,6,8,10,25,75,60,55,45,35,85,95,15,20",
  
  // Large (100 elements) - reverse sorted
  "100,99,98,97,96,95,94,93,92,91,90,89,88,87,86,85,84,83,82,81,80,79,78,77,76,75,74,73,72,71,70,69,68,67,66,65,64,63,62,61,60,59,58,57,56,55,54,53,52,51,50,49,48,47,46,45,44,43,42,41,40,39,38,37,36,35,34,33,32,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1",
  
  // Very Large (200 elements) - reverse sorted
  "200,199,198,197,196,195,194,193,192,191,190,189,188,187,186,185,184,183,182,181,180,179,178,177,176,175,174,173,172,171,170,169,168,167,166,165,164,163,162,161,160,159,158,157,156,155,154,153,152,151,150,149,148,147,146,145,144,143,142,141,140,139,138,137,136,135,134,133,132,131,130,129,128,127,126,125,124,123,122,121,120,119,118,117,116,115,114,113,112,111,110,109,108,107,106,105,104,103,102,101,100,99,98,97,96,95,94,93,92,91,90,89,88,87,86,85,84,83,82,81,80,79,78,77,76,75,74,73,72,71,70,69,68,67,66,65,64,63,62,61,60,59,58,57,56,55,54,53,52,51,50,49,48,47,46,45,44,43,42,41,40,39,38,37,36,35,34,33,32,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1"
];

// Language configurations
const languages = [
  { key: 'c', name: 'C', version: '11' },
  { key: 'cpp', name: 'C++', version: '11' },
  { key: 'java21', name: 'Java', version: '21' },
  { key: 'python', name: 'Python', version: '3.9' },
  { key: 'javascript', name: 'JavaScript', version: '18' },
  { key: 'typescript', name: 'TypeScript', version: '5.0' },
  { key: 'go', name: 'Go', version: '1.21' },
  { key: 'ruby', name: 'Ruby', version: '3.2' },
  { key: 'swift', name: 'Swift', version: '5.9' },
  { key: 'php', name: 'PHP', version: '8.2' },
  { key: 'r', name: 'R', version: '4.3' },
  { key: 'csharp', name: 'C#', version: '11' },
  { key: 'kotlin', name: 'Kotlin', version: '1.9' }
];

class BenchmarkResult {
  constructor(language, size) {
    this.language = language;
    this.size = size;
    this.startTime = 0;
    this.tokenTime = 0;
    this.completionTime = 0;
    this.totalTime = 0;
    this.status = 'pending';
    this.successCount = 0;
    this.totalCount = 0;
    this.error = null;
  }
}

async function runLanguageBenchmark(langConfig) {
  const results = [];
  console.log(`\n${'='.repeat(80)}`);
  console.log(`BENCHMARKING: ${langConfig.name} ${langConfig.version}`);
  console.log(`${'='.repeat(80)}`);
  
  // Create batch payload
  const basePayload = {
    language: langConfig.key,
    version: langConfig.version,
    code: generateBubbleSortTestCases(langConfig.key),
    timeout: 30
  };
  
  const batch = testInputs.map((stdin, idx) => ({
    ...basePayload,
    stdin: stdin,
    // Add metadata for tracking
    _testId: idx,
    _size: stdin.split(',').length
  }));
  
  const batchSize = batch.length;
  const result = new BenchmarkResult(langConfig.name, batchSize);
  result.startTime = Date.now();
  
  try {
    // Send batch request
    console.log(`Sending batch of ${batchSize} requests...`);
    const startPost = Date.now();
    const response = await fetch(`${BASE_URL}/execute/batch`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(batch)
    });
    
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }
    
    const responseData = await response.json();
    result.tokenTime = Date.now() - startPost;
    const token = responseData.token;
    
    console.log(`✓ Batch accepted, token: ${token}`);
    console.log(`  Time to token: ${result.tokenTime}ms`);
    
    // Poll for results
    let attempts = 0;
    while (attempts < MAX_POLLS) {
      await sleep(POLLING_INTERVAL_MS);
      attempts++;
      
      const statusResponse = await fetch(`${BASE_URL}/submissions/${token}`);
      if (!statusResponse.ok) continue;
      
      const status = await statusResponse.json();
      
      if (status.status === 'Completed' || status.status === 'Failed') {
        result.completionTime = Date.now() - (result.startTime + result.tokenTime);
        result.totalTime = Date.now() - result.startTime;
        result.status = status.status;
        
        if (status.results && Array.isArray(status.results)) {
          result.totalCount = status.results.length;
          result.successCount = status.results.filter(r => 
            r.exit_code === 0 && 
            r.stdout && 
            r.stdout.includes('[') && 
            r.stdout.includes(']')
          ).length;
          
          // Detailed analysis
          console.log(`\n📊 RESULTS ANALYSIS:`);
          console.log(`  Success rate: ${result.successCount}/${result.totalCount} (${((result.successCount/result.totalCount)*100).toFixed(1)}%)`);
          
          // Check execution times
          const execTimes = status.results
            .filter(r => r.execution_time)
            .map(r => r.execution_time);
          
          if (execTimes.length > 0) {
            const avgTime = execTimes.reduce((a, b) => a + b, 0) / execTimes.length;
            const maxTime = Math.max(...execTimes);
            const minTime = Math.min(...execTimes);
            
            console.log(`  Execution times (ms):`);
            console.log(`    Average: ${avgTime.toFixed(2)}`);
            console.log(`    Min: ${minTime.toFixed(2)}`);
            console.log(`    Max: ${maxTime.toFixed(2)}`);
          }
          
          // Check memory usage
          const memUsages = status.results
            .filter(r => r.memory_usage)
            .map(r => r.memory_usage);
          
          if (memUsages.length > 0) {
            const avgMem = memUsages.reduce((a, b) => a + b, 0) / memUsages.length;
            console.log(`  Average memory: ${(avgMem / 1024).toFixed(2)} KB`);
          }
          
          // Show failures
          const failures = status.results.filter(r => r.exit_code !== 0);
          if (failures.length > 0) {
            console.log(`\n⚠️  FAILURES (${failures.length}):`);
            failures.forEach((f, idx) => {
              console.log(`  Test ${idx + 1}: Exit ${f.exit_code}, ${f.stderr || 'No error message'}`);
            });
          }
        }
        
        break;
      }
      
      // Show progress every 5 seconds
      if (attempts % 20 === 0) {
        const elapsed = (attempts * POLLING_INTERVAL_MS) / 1000;
        console.log(`  Polling... ${elapsed.toFixed(1)}s elapsed`);
      }
    }
    
    if (result.status === 'pending') {
      result.status = 'timeout';
      console.log(`⏱️  Polling timeout after ${MAX_POLLS * POLLING_INTERVAL_MS / 1000}s`);
    }
    
  } catch (error) {
    result.status = 'error';
    result.error = error.message;
    console.error(`❌ Error: ${error.message}`);
  }
  
  // Summary
  console.log(`\n📈 SUMMARY FOR ${langConfig.name}:`);
  console.log(`  Status: ${result.status}`);
  console.log(`  Total time: ${result.totalTime}ms`);
  console.log(`  Token time: ${result.tokenTime}ms`);
  console.log(`  Execution time: ${result.completionTime}ms`);
  console.log(`  Success rate: ${result.successCount}/${result.totalCount}`);
  
  results.push(result);
  return result;
}

async function runComprehensiveBenchmark() {
  console.log('='.repeat(80));
  console.log('COMPREHENSIVE LANGUAGE BENCHMARK SUITE');
  console.log('='.repeat(80));
  console.log(`Testing ${languages.length} languages with bubble sort algorithm`);
  console.log(`Test sizes: ${testInputs.map(inp => inp.split(',').length).join(', ')} elements`);
  console.log('='.repeat(80));
  
  const allResults = [];
  const summaryTable = [];
  
  for (const lang of languages) {
    // Clear cache before each language test
    console.log('\n🧹 Clearing cache...');
    try {
      // This would require Redis access - for now just a message
      console.log('  (Cache clearing would happen here)');
      await sleep(500);
    } catch (e) {
      console.log('  Cache clear skipped');
    }
    
    const result = await runLanguageBenchmark(lang);
    allResults.push(result);
    
    // Add to summary table
    summaryTable.push({
      Language: lang.name,
      Version: lang.version,
      'Total Time (ms)': result.totalTime,
      'Token Time (ms)': result.tokenTime,
      'Exec Time (ms)': result.completionTime,
      'Success Rate': `${result.successCount}/${result.totalCount}`,
      Status: result.status
    });
    
    // Brief pause between languages
    await sleep(2000);
  }
  
  // Generate comprehensive report
  console.log('\n' + '='.repeat(80));
  console.log('FINAL BENCHMARK REPORT');
  console.log('='.repeat(80));
  
  // Table display
  console.log('\n📋 SUMMARY TABLE:');
  console.log('-'.repeat(120));
  console.log('Language      | Version | Total(ms) | Token(ms) | Exec(ms) | Success | Status');
  console.log('-'.repeat(120));
  
  summaryTable.forEach(row => {
    console.log(
      `${row.Language.padEnd(12)} | ` +
      `${row.Version.padEnd(7)} | ` +
      `${row['Total Time (ms)'].toString().padEnd(9)} | ` +
      `${row['Token Time (ms)'].toString().padEnd(9)} | ` +
      `${row['Exec Time (ms)'].toString().padEnd(8)} | ` +
      `${row['Success Rate'].padEnd(7)} | ` +
      `${row.Status}`
    );
  });
  
  console.log('-'.repeat(120));
  
  // Performance rankings
  console.log('\n🏆 PERFORMANCE RANKINGS:');
  
  // Fastest total time
  const fastestTotal = [...allResults]
    .filter(r => r.status === 'Completed')
    .sort((a, b) => a.totalTime - b.totalTime);
  
  if (fastestTotal.length > 0) {
    console.log(`\n🥇 Fastest Total Time: ${fastestTotal[0].language} (${fastestTotal[0].totalTime}ms)`);
    if (fastestTotal.length > 1) {
      console.log(`🥈 Runner-up: ${fastestTotal[1].language} (${fastestTotal[1].totalTime}ms)`);
    }
  }
  
  // Best success rate
  const bestSuccess = [...allResults]
    .filter(r => r.totalCount > 0)
    .sort((a, b) => (b.successCount/b.totalCount) - (a.successCount/a.totalCount));
  
  if (bestSuccess.length > 0) {
    console.log(`\n✅ Best Success Rate: ${bestSuccess[0].language} (${bestSuccess[0].successCount}/${bestSuccess[0].totalCount})`);
  }
  
  // Languages that failed
  const failed = allResults.filter(r => r.status !== 'Completed' || r.successCount === 0);
  if (failed.length > 0) {
    console.log('\n❌ LANGUAGES WITH ISSUES:');
    failed.forEach(f => {
      console.log(`  ${f.language}: ${f.status}${f.error ? ` - ${f.error}` : ''}`);
    });
  }
  
  // Calculate averages
  const completed = allResults.filter(r => r.status === 'Completed');
  if (completed.length > 0) {
    const avgTotal = completed.reduce((sum, r) => sum + r.totalTime, 0) / completed.length;
    const avgToken = completed.reduce((sum, r) => sum + r.tokenTime, 0) / completed.length;
    const avgExec = completed.reduce((sum, r) => sum + r.completionTime, 0) / completed.length;
    
    console.log(`\n📊 AVERAGES (${completed.length} languages):`);
    console.log(`  Total time: ${avgTotal.toFixed(0)}ms`);
    console.log(`  Token time: ${avgToken.toFixed(0)}ms`);
    console.log(`  Execution time: ${avgExec.toFixed(0)}ms`);
  }
  
  console.log('\n' + '='.repeat(80));
  console.log('BENCHMARK COMPLETE');
  console.log('='.repeat(80));
  
  // Export results as JSON
  const exportData = {
    timestamp: new Date().toISOString(),
    testConfig: {
      batchSizes: testInputs.map(inp => inp.split(',').length),
      totalTests: testInputs.length,
      pollingInterval: POLLING_INTERVAL_MS,
      maxPolls: MAX_POLLS
    },
    results: allResults.map(r => ({
      language: r.language,
      size: r.size,
      totalTime: r.totalTime,
      tokenTime: r.tokenTime,
      completionTime: r.completionTime,
      status: r.status,
      successCount: r.successCount,
      totalCount: r.totalCount,
      error: r.error
    }))
  };
  
  console.log('\n💾 Results available as JSON (see above for export)');
  return exportData;
}

// Helper function to clear Redis cache (requires docker access)
async function clearCache() {
  console.log('🧹 Attempting to clear Redis cache...');
  try {
    // This would require proper docker/redis access
    // For now, we'll just log what would happen
    console.log('  Command would be: docker exec evalx_redis redis-cli KEYS "evalx:*" | xargs -r docker exec evalx_redis redis-cli DEL');
    console.log('  (Cache clear simulated)');
    return true;
  } catch (error) {
    console.log('  Cache clear failed (may need proper docker permissions)');
    return false;
  }
}

// Main execution
(async () => {
  try {
    // Optional: Clear cache before starting
    const clearCacheFlag = process.argv.includes('--clear-cache');
    if (clearCacheFlag) {
      await clearCache();
      await sleep(1000);
    }
    
    const results = await runComprehensiveBenchmark();
    
    // Save results to file if needed
    if (process.argv.includes('--save')) {
      const fs = require('fs');
      const filename = `benchmark_${Date.now()}.json`;
      fs.writeFileSync(filename, JSON.stringify(results, null, 2));
      console.log(`\n💾 Results saved to ${filename}`);
    }
    
  } catch (error) {
    console.error('Fatal error:', error);
    process.exit(1);
  }
})();