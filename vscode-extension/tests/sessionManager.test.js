const assert = require('node:assert/strict');
const test = require('node:test');

const { SessionManager } = require('../out/services/sessionManager');

class FakeGatewayClient {
  constructor() {
    this.created = [];
    this.closed = [];
    this.existing = new Set();
  }

  async createSession(name, properties) {
    const sessionHandle = `${name}-${this.created.length + 1}`;
    this.created.push({ name, properties, sessionHandle });
    this.existing.add(sessionHandle);
    return { sessionHandle };
  }

  async getSession(sessionHandle) {
    if (!this.existing.has(sessionHandle)) {
      throw new Error('missing session');
    }
    return { sessionHandle };
  }

  async closeSession(sessionHandle) {
    this.closed.push(sessionHandle);
    this.existing.delete(sessionHandle);
  }
}

test('SessionManager keeps separate sessions per scope', async () => {
  const gateway = new FakeGatewayClient();
  const manager = new SessionManager(gateway, {
    executionModeProvider: () => 'batch',
  });

  const firstNotebook = await manager.getOrCreateSession('notebook-a');
  const secondNotebook = await manager.getOrCreateSession('notebook-b');
  const firstNotebookAgain = await manager.getOrCreateSession('notebook-a');

  assert.equal(firstNotebookAgain, firstNotebook);
  assert.notEqual(firstNotebook, secondNotebook);
  assert.deepEqual(
    gateway.created.map((entry) => entry.name),
    ['notebook-notebook-a', 'notebook-notebook-b']
  );
  assert.deepEqual(gateway.created[0].properties, {
    'execution.runtime-mode': 'batch',
  });
});

test('SessionManager can recycle one scope without closing others', async () => {
  const gateway = new FakeGatewayClient();
  const manager = new SessionManager(gateway);

  const notebookSession = await manager.getOrCreateSession('notebook-a');
  const catalogSession = await manager.getOrCreateSession('catalog');
  const recycled = await manager.recycleSession('notebook-a');

  assert.notEqual(recycled, notebookSession);
  assert.equal(await manager.getOrCreateSession('catalog'), catalogSession);
  assert.deepEqual(gateway.closed, [notebookSession]);
});
