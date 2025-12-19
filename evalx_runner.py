import requests
import json
import time
import subprocess
import concurrent.futures
import statistics
from tabulate import tabulate

# --- CONFIGURATION ---
API_URL = "http://localhost:3000"
REDIS_CONTAINER = "evalx_redis"
POLL_INTERVAL = 0.2
MAX_WORKERS = 8  # Utilization of your 8 cores

# --- ANSI COLORS ---
class C:
    HEADER = '\033[95m'
    G = '\033[92m'      # Green (Pass)
    R = '\033[91m'      # Red (Fail)
    Y = '\033[93m'      # Yellow (Warn)
    B = '\033[94m'      # Blue (Info)
    CYAN = '\033[96m'   # Cyan (Title)
    E = '\033[0m'       # End/Reset
    # Aliases for compatibility
    ENDC = E

# --- INPUT DATA ---
STD_INPUTS = [
    "100,46,45,44,43,42,41,40,39,38,37,36,35,34,33,32,31,30,29,99,98,97,96,95,94,93,92,91,90,89,88,87,86,85,84,83,82,81,80,79,78,77,76,75,74,73,72,71,70,69,68,67,66,65,64,63,62,61,60,59,58,57,56,55,54,53,52,51,50,49,48,47,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1",
    "50,12,1,99,4,33,76,100,2,5,8,9,10,44,22,11,66,77,88,33,21,43,65,87,98,12,34,56,78,90,1,3,5,7,9,2,4,6,8,10",
    "20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1",
    "5,4,3,2,1", 
    "10,20,5,15",
    "1000,999,998,997,996,995,994,993,992,991,990,989,988,987,986,985,984,983,982,981,980,979,978,977,976,975,974,973,972,971,970,969,968,967,966,965,964,963,962,961,960,959,958,957,956,955,954,953,952,951,950,949,948,947,946,945,944,943,942,941,940,939,938,937,936,935,934,933,932,931,930,929,928,927,926,925,924,923,922,921,920,919,918,917,916,915,914,913,912,911,910,909,908,907,906,905,904,903,902,901,900"
]

# --- EXACT SOURCE CODES ---
# Raw strings (r"...") used to prevent Python from interpreting escape characters like \n inside the code strings.

C_CODE = r"""#include <stdio.h>
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
    if (fgets(input, sizeof(input), stdin) == NULL) {
        printf("[]\n");
        return 0;
    }
    input[strcspn(input, "\n")] = 0;
    int count = 0;
    const char* p = input;
    if (*p != '\0') {
        count = 1;
        while (*p != '\0') {
            if (*p == ',') count++;
            p++;
        }
    }
    if (count == 0) {
        printf("[]\n");
        return 0;
    }
    int* arr = (int*)malloc(count * sizeof(int));
    char* token = strtok(input, ",");
    int i = 0;
    while (token != NULL && i < count) {
        arr[i++] = atoi(token);
        token = strtok(NULL, ",");
    }
    bubbleSort(arr, i);
    printf("[");
    for (int j = 0; j < i; j++) {
        printf("%d", arr[j]);
        if (j < i - 1) printf(", ");
    }
    printf("]\n");
    free(arr);
    return 0;
}"""

CPP_CODE = r"""#include <iostream>
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
    std::string input_line;
    if (!std::getline(std::cin, input_line) || input_line.empty()) {
         std::cout << "[]" << std::endl;
         return 0;
    }
    std::vector<int> arr;
    std::stringstream ss(input_line);
    std::string token;
    while (std::getline(ss, token, ',')) {
        try {
             token.erase(0, token.find_first_not_of(" \t\n\r\f\v"));
             token.erase(token.find_last_not_of(" \t\n\r\f\v") + 1);
             if (!token.empty()) arr.push_back(std::stoi(token));
        } catch (...) {}
    }
    bubbleSort(arr);
    std::cout << "[";
    for (size_t i = 0; i < arr.size(); i++) {
        std::cout << arr[i];
        if (i < arr.size() - 1) std::cout << ", ";
    }
    std::cout << "]" << std::endl;
    return 0;
}"""

PYTHON_CODE = r"""import sys

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
        print("[]")"""

JAVA21_CODE = r"""import java.util.Arrays;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        if(sc.hasNextLine()) {
            String[] input = sc.nextLine().split(",");
            int[] arr = new int[input.length];
            for (int i = 0; i < input.length; i++) {
                arr[i] = Integer.parseInt(input[i].trim());
            }
            bubbleSort(arr);
            System.out.println(Arrays.toString(arr));
        } else {
            System.out.println("[]");
        }
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
}"""

GO_CODE = r"""package main
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
    input, _ := reader.ReadString('\n')
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
}"""

PHP_CODE = r"""<?php
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
if ($n === 0) {
    echo "[]";
    exit;
}
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
?>"""

SQLITE_CODE = r"""PRAGMA foreign_keys = ON;
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);
BEGIN TRANSACTION;
INSERT INTO users (name, score) VALUES ('Alice', 100);
INSERT INTO users (name, score) VALUES ('Bob', 50);
INSERT INTO users (name, score) VALUES ('Charlie', 75);
COMMIT;
UPDATE users SET score = score + 10 WHERE score < 80;
.mode box
SELECT * FROM users ORDER BY score DESC;"""

R_CODE = r"""input <- readLines("stdin", n=1)
if (length(input) == 0) {
  cat("[]\n")
  quit()
}
parts <- strsplit(input, ",")[[1]]
arr <- as.integer(parts)
n <- length(arr)
if (n > 0) {
  for (i in 1:(n-1)) {
    for (j in 1:(n-i)) {
      if (arr[j] > arr[j+1]) {
        temp <- arr[j]
        arr[j] <- arr[j+1]
        arr[j+1] <- temp
      }
    }
  }
}
cat("[")
cat(paste(arr, collapse=", "))
cat("]\n")"""

RUBY_CODE = r"""input = $stdin.read
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
puts "]" """

TYPESCRIPT_CODE = r"""declare const process: any;
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
        console.log(`[${sorted.join(", ")}]`);
    }
} catch (e) {
    console.log("[]");
}"""

SWIFT_CODE = r"""import Foundation
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
        print("[\(result)]")
    } else {
        print("[]")
    }
} else {
    print("[]")
}"""

JAVASCRIPT_CODE = r"""const readline = require('readline');
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  terminal: false
});

rl.on('line', (input) => {
  if (!input || input.trim() === '') {
    console.log('[]');
    process.exit(0);
  }
  
  const parts = input.split(',');
  const arr = parts.map(Number);
  const n = arr.length;
  
  if (n > 0) {
    for (let i = 0; i < n - 1; i++) {
      for (let j = 0; j < n - i - 1; j++) {
        if (arr[j] > arr[j + 1]) {
          const temp = arr[j];
          arr[j] = arr[j + 1];
          arr[j + 1] = temp;
        }
      }
    }
  }
  
  console.log('[' + arr.join(', ') + ']');
  process.exit(0);
});"""

CSHARP_CODE = r"""using System;
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
}"""

INFINITE_CODE = r"""#include <stdio.h>
int main() { while (1); printf("This will never be printed.\n"); return 0; }"""

# Map of Language Name -> (Version, CodeVariable)
CODES_DB = {
    "java21": ("21", JAVA21_CODE),
    "c": ("11", C_CODE),
    "cpp": ("11", CPP_CODE),
    "python": ("3.9", PYTHON_CODE),
    "go": ("1.21", GO_CODE),
    "r": ("4.3", R_CODE),
    "ruby": ("3.2", RUBY_CODE),
    "typescript": ("5.0", TYPESCRIPT_CODE),
    "javascript": ("18", JAVASCRIPT_CODE),
    "csharp": ("11", CSHARP_CODE),
    "swift": ("5.9", SWIFT_CODE),
    "php": ("8.2", PHP_CODE),
    "sqlite": ("3.43", SQLITE_CODE)
}

# --- RUNNER LOGIC ---

def clear_redis():
    """Clears the redis cache to ensure cold starts."""
    try:
        subprocess.run(f"docker exec {REDIS_CONTAINER} redis-cli KEYS 'evalx:artifact:*' | xargs -r docker exec {REDIS_CONTAINER} redis-cli DEL", 
                       shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except: pass

def clean_str(s):
    """Aggressively remove whitespace for comparison."""
    if not s: return ""
    return s.replace('[', '').replace(']', '').replace('\n', '').replace(' ', '').replace('\r', '')

def get_expected(stdin):
    """Calculates expected sorted output in Python."""
    try:
        nums = [int(x.strip()) for x in stdin.split(',')]
        nums.sort()
        return ",".join(map(str, nums))
    except: return ""

def execute_language(name, version, code):
    is_sqlite = (name == "sqlite")

    if is_sqlite:
        payload = {"language": name, "version": version, "code": code, "stdin": "ignored", "timeout": 5}
        url = f"{API_URL}/execute"
        inputs = [payload]
    else:
        payload = [{"language": name, "version": version, "code": code, "stdin": STD_INPUTS[0], "timeout": 10}]
        for inp in STD_INPUTS[1:]:
            payload.append({"stdin": inp})
        url = f"{API_URL}/execute/batch"
        inputs = payload

    try:
        resp = requests.post(url, json=payload, headers={"Content-Type": "application/json"})
        if resp.status_code != 200:
            return (name, version, "FAIL", {}, [])
        
        token = resp.json().get("token")
        
        # Poll for completion
        results = None
        for _ in range(60): # 12s timeout to allow compilation
            r = requests.get(f"{API_URL}/submissions/{token}").json()
            if r['status'] == 'Completed':
                results = r['results'] if isinstance(r['results'], list) else [r['results']]
                break
            if r['status'] == 'Failed':
                return (name, version, "FAIL", {}, [])
            time.sleep(POLL_INTERVAL)
        
        if not results:
            return (name, version, "TIMEOUT", {}, [])

        # Collect Detailed Results
        passed_count = 0
        total_runtime = 0
        max_runtime = 0
        total_compile = 0
        max_memory_kb = 0
        test_details = []

        for i, res in enumerate(results):
            test_status = "PASS"
            stderr = res.get('stderr') or ""
            stdout = res.get('stdout') or ""
            exit_code = res.get('exit_code')
            
            # --- CHECK 1: Exit Code ---
            if exit_code != 0:
                test_status = "FAIL"
            
            # --- CHECK 2: Output Exists ---
            if test_status == "PASS" and not stdout:
                test_status = "FAIL"
                stderr += "\n[Runner] Empty Output"

            # --- CHECK 3: Logic Validation ---
            expected_val = ""
            
            if is_sqlite:
                # SQLite: Pass if exit code 0 + has output (simple check)
                if test_status == "PASS":
                    passed_count += 1
                expected_val = "(Table Data)"
            else:
                # Sort: Pass if output matches sorted input numerically
                actual_val = clean_str(stdout)
                expected_val = get_expected(inputs[i]['stdin'])
                
                if test_status == "PASS":
                    if expected_val == actual_val:
                        passed_count += 1
                    else:
                        test_status = "FAIL"
                        stderr += f"\n[Runner] Logic Mismatch.\nExpected: {expected_val[:30]}...\nGot: {actual_val[:30]}..."
            
            # --- Stats ---
            r_time = res.get('run_time', 0)
            total_runtime += r_time
            if r_time > max_runtime: max_runtime = r_time
            total_compile += res.get('compile_time', 0)

            # Parse Memory
            try:
                mem_val = float(res.get('space_consumed', '0 KB').split()[0])
                if mem_val > max_memory_kb: max_memory_kb = mem_val
            except: pass
            
            # Store detail
            test_details.append({
                'index': i + 1,
                'input': inputs[i].get('stdin', 'N/A')[:20] + "...",
                'exit_code': exit_code,
                'time': f"{r_time:.3f}s",
                'status': test_status,
                'stderr': stderr.strip()
            })

        avg_t = total_runtime / len(results) if results else 0
        avg_c = total_compile / len(results) if results else 0
        status = "PASS" if passed_count == len(inputs) else "FAIL"
        
        stats = {
            "pass": passed_count,
            "total": len(inputs),
            "avg_time": avg_t,
            "max_time": max_runtime,
            "avg_comp": avg_c,
            "max_mem": max_memory_kb
        }
        
        return (name, version, status, stats, test_details)

    except Exception as e:
        return (name, version, "ERROR", {}, [])

# --- MAIN ---
def main():
    print(f"{C.HEADER}{C.CYAN}EVALX AUTOMATION RUNNER{C.ENDC}")
    
    # 1. Clear Cache
    print(f"{C.B}Clearing Redis Cache...{C.E}")
    clear_redis()
    
    # 2. Parallel Execution
    print(f"{C.B}Launching {MAX_WORKERS} parallel workers for {len(CODES_DB)} languages...{C.E}\n")

    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for lang, (ver, code) in CODES_DB.items():
            futures.append(executor.submit(execute_language, lang, ver, code))

    all_results = []
    
    for f in concurrent.futures.as_completed(futures):
        all_results.append(f.result())
        
    # Sort results by language name
    all_results.sort(key=lambda x: x[0])

    # 3. PRINT DETAILED TABLES (Per Language)
    for name, ver, status, stats, details in all_results:
        status_color = C.G if status == "PASS" else C.R
        print(f"\n{C.HEADER}--- {name} ({ver}) : {status_color}{status}{C.E} ---")
        
        if not details:
            print(f"{C.R}No results returned or Error occurred.{C.E}")
            continue

        # Prepare table rows for this language
        rows = []
        for d in details:
            status_icon = f"{C.G}PASS{C.E}" if d['status'] == "PASS" else f"{C.R}FAIL{C.E}"
            
            # Show stderr only if it exists
            error_msg = d['stderr'] if d['stderr'] else ""
            if len(error_msg) > 50: error_msg = error_msg[:50] + "..." # Truncate for table
            
            rows.append([d['index'], d['input'], d['exit_code'], d['time'], status_icon, error_msg])

        print(tabulate(rows, headers=["#", "Input", "Exit", "Time", "Status", "Stderr/Error"], tablefmt="simple"))

    # 4. Infinite Loop Validation
    print("\n" + "-" * 60)
    inf_payload = [{"language": "c", "version": "11", "code": INFINITE_CODE, "stdin": "t", "timeout": 1}]
    inf_token = requests.post(f"{API_URL}/execute/batch", json=inf_payload).json().get("token")
    inf_res_status = "UNKNOWN"
    for _ in range(15):
        r = requests.get(f"{API_URL}/submissions/{inf_token}").json()
        if r['status'] == 'Completed':
            res_list = r.get('results', [])
            if res_list and (res_list[0].get('status') == 'timeout' or 'Time Limit' in str(res_list[0].get('stderr', ''))):
                 inf_res_status = "HANDLED (TIMEOUT)"
            else:
                 inf_res_status = "HANDLED (EXIT)"
            break
        time.sleep(0.5)
    
    print(f"Infinite Loop Resilience Check: {C.G if 'HANDLED' in inf_res_status else C.R}{inf_res_status}{C.E}")
    print("-" * 60)

    # 5. Final Summary Table
    print(f"\n{C.CYAN}=== FINAL SUMMARY ==={C.E}")
    summary_rows = []
    for name, ver, status, stats, _ in all_results:
        if not stats:
             summary_rows.append([name, ver, f"{C.R}ERROR{C.E}", "0/0", "N/A"])
             continue
        
        s_str = f"{C.G}PASS{C.E}" if status == "PASS" else f"{C.R}FAIL{C.E}"
        summary_rows.append([
            name, 
            ver, 
            s_str, 
            f"{stats['pass']}/{stats['total']}", 
            f"{stats['avg_time']:.3f}s",
            f"{stats['max_mem']:.0f} KB"
        ])
    
    print(tabulate(summary_rows, headers=["Language", "Ver", "Status", "Pass Rate", "Avg Time", "Max Mem"], tablefmt="simple"))

if __name__ == "__main__":
    main()