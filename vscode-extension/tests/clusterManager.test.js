const assert = require('node:assert/strict');
const test = require('node:test');

const { ClusterManager } = require('../out/services/clusterManager');

test('ClusterManager exposes configurable gateway and web UI URLs', () => {
  const manager = new ClusterManager({
    jarPath: '/tmp/missing-flink-minicluster.jar',
    gatewayPort: 18083,
    webUiPort: 18081,
    rpcPort: 16123,
  });

  assert.equal(manager.getGatewayUrl(), 'http://localhost:18083');
  assert.equal(manager.getWebUIUrl(), 'http://localhost:18081');
});
