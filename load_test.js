import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  vus: 5,
  duration: '20s',
};

export default function () {
  const batchPayload = JSON.stringify([
    { language: 'python', version: '3.9', code: 'print("Batch 1")', stdin: '' },
    { language: 'python', version: '3.9', code: 'print("Batch 2")', stdin: '' },
  ]);

  const res = http.post('http://localhost:3000/execute/batch', batchPayload, {
    headers: { 'Content-Type': 'application/json' },
  });

  check(res, {
    'status is 200': (r) => r.status === 200,
  });

  sleep(1);
}