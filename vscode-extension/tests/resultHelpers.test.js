const assert = require('node:assert/strict');
const test = require('node:test');

const {
  getNextResultToken,
  shouldRetryInitialResult,
} = require('../out/services/resultHelpers');

test('empty PAYLOAD without next URI is complete, not a retry condition', () => {
  assert.equal(
    shouldRetryInitialResult({
      resultType: 'PAYLOAD',
      results: { data: [] },
    }),
    false
  );
});

test('empty PAYLOAD with same-token next URI retries the current token', () => {
  assert.equal(
    shouldRetryInitialResult(
      {
        resultType: 'PAYLOAD',
        results: { data: [] },
        nextResultUri: '/v1/sessions/s/operations/o/result/0',
      },
      0
    ),
    true
  );
});

test('getNextResultToken extracts result token and falls back to increment', () => {
  assert.equal(getNextResultToken('/v1/sessions/s/operations/o/result/42', 7), 42);
  assert.equal(getNextResultToken(undefined, 7), 8);
});
