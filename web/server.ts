import http from 'http';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { WebSocketServer, WebSocket } from 'ws';
import {
  ClusterInfo,
  ClusterMetrics,
  TopicItem,
  KafkaRecord,
  ConsumerGroup,
  LogEntry,
  LogLevel,
  Subsystem,
  MetricPoint,
  FlowPacket
} from './src/types/kafka';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PORT = parseInt(process.env.PORT || '9090', 10);
const DIST_DIR = path.join(__dirname, 'dist');

// ---------------------------------------------------------------------------
// In-Memory Cluster & Kafka State
// ---------------------------------------------------------------------------
const startTime = Date.now();

const clusterState: ClusterInfo = {
  brokerId: 1,
  port: 9092,
  host: '127.0.0.1',
  version: '3.8.0-aura (Aura v0.6.0)',
  uptimeSeconds: 0,
  isRunning: true,
  kraft: {
    clusterId: 'aura-cluster-1',
    brokerId: 1,
    role: 'Leader',
    leaderId: 1,
    currentEpoch: 4,
    voters: [1],
    quorumStatus: 'HEALTHY',
    metadataLogEndOffset: 128,
    highWatermark: 128
  },
  storage: {
    baseDir: path.resolve(__dirname, '../data'),
    numPartitionsDefault: 3,
    segmentMaxBytes: 10485760, // 10MB
    totalLogSegments: 12,
    totalDiskBytes: 425984
  }
};

let topics: TopicItem[] = [
  {
    name: 'orders-stream',
    isInternal: false,
    replicationFactor: 1,
    cleanupPolicy: 'delete',
    retentionMs: 604800000,
    totalRecords: 142,
    totalSizeBytes: 34500,
    partitions: [
      { partition: 0, leader: 1, replicas: [1], isr: [1], highWatermark: 48, logEndOffset: 48, segmentCount: 1, sizeBytes: 11200, recordsCount: 48 },
      { partition: 1, leader: 1, replicas: [1], isr: [1], highWatermark: 46, logEndOffset: 46, segmentCount: 1, sizeBytes: 11100, recordsCount: 46 },
      { partition: 2, leader: 1, replicas: [1], isr: [1], highWatermark: 48, logEndOffset: 48, segmentCount: 1, sizeBytes: 12200, recordsCount: 48 }
    ]
  },
  {
    name: 'payments',
    isInternal: false,
    replicationFactor: 1,
    cleanupPolicy: 'delete',
    retentionMs: 604800000,
    totalRecords: 96,
    totalSizeBytes: 24100,
    partitions: [
      { partition: 0, leader: 1, replicas: [1], isr: [1], highWatermark: 32, logEndOffset: 32, segmentCount: 1, sizeBytes: 8000, recordsCount: 32 },
      { partition: 1, leader: 1, replicas: [1], isr: [1], highWatermark: 31, logEndOffset: 31, segmentCount: 1, sizeBytes: 7900, recordsCount: 31 },
      { partition: 2, leader: 1, replicas: [1], isr: [1], highWatermark: 33, logEndOffset: 33, segmentCount: 1, sizeBytes: 8200, recordsCount: 33 }
    ]
  },
  {
    name: 'user-events',
    isInternal: false,
    replicationFactor: 1,
    cleanupPolicy: 'delete',
    retentionMs: 604800000,
    totalRecords: 85,
    totalSizeBytes: 18900,
    partitions: [
      { partition: 0, leader: 1, replicas: [1], isr: [1], highWatermark: 43, logEndOffset: 43, segmentCount: 1, sizeBytes: 9400, recordsCount: 43 },
      { partition: 1, leader: 1, replicas: [1], isr: [1], highWatermark: 42, logEndOffset: 42, segmentCount: 1, sizeBytes: 9500, recordsCount: 42 }
    ]
  },
  {
    name: 'quickstart-events',
    isInternal: false,
    replicationFactor: 1,
    cleanupPolicy: 'compact,delete',
    retentionMs: 604800000,
    totalRecords: 34,
    totalSizeBytes: 7800,
    partitions: [
      { partition: 0, leader: 1, replicas: [1], isr: [1], highWatermark: 34, logEndOffset: 34, segmentCount: 1, sizeBytes: 7800, recordsCount: 34 }
    ]
  },
  {
    name: '__consumer_offsets',
    isInternal: true,
    replicationFactor: 1,
    cleanupPolicy: 'compact',
    retentionMs: 86400000,
    totalRecords: 50,
    totalSizeBytes: 12000,
    partitions: [
      { partition: 0, leader: 1, replicas: [1], isr: [1], highWatermark: 50, logEndOffset: 50, segmentCount: 1, sizeBytes: 12000, recordsCount: 50 }
    ]
  }
];

// In-Memory Records Storage
const recordsMap: Map<string, KafkaRecord[]> = new Map();

// Helper to seed initial records
function seedInitialRecords() {
  const seedList = [
    { topic: 'orders-stream', key: 'ord-1001', val: JSON.stringify({ orderId: 'ord-1001', customer: 'alice@cenco.cl', total: 89.90, status: 'PAID' }) },
    { topic: 'orders-stream', key: 'ord-1002', val: JSON.stringify({ orderId: 'ord-1002', customer: 'carlos@b2b.com', total: 450.00, status: 'PROCESSING' }) },
    { topic: 'orders-stream', key: 'ord-1003', val: JSON.stringify({ orderId: 'ord-1003', customer: 'maria@portal.cl', total: 120.50, status: 'SHIPPED' }) },
    { topic: 'payments', key: 'acct-001', val: JSON.stringify({ transactionId: 'tx-881', account: 'acct-001', amount: 150.00, currency: 'USD' }) },
    { topic: 'payments', key: 'acct-002', val: JSON.stringify({ transactionId: 'tx-882', account: 'acct-002', amount: 25.50, currency: 'USD' }) },
    { topic: 'payments', key: 'acct-003', val: JSON.stringify({ transactionId: 'tx-883', account: 'acct-003', amount: 1200.00, currency: 'USD' }) },
    { topic: 'user-events', key: 'usr-441', val: JSON.stringify({ action: 'login', ip: '192.168.1.10', platform: 'web-react' }) },
    { topic: 'user-events', key: 'usr-442', val: JSON.stringify({ action: 'view_catalog', category: 'electronics' }) },
    { topic: 'quickstart-events', key: 'qs-1', val: JSON.stringify({ message: 'Hello from AuraKafka Broker!' }) }
  ];

  seedList.forEach((s, idx) => {
    const list = recordsMap.get(s.topic) || [];
    const partition = idx % 3;
    list.push({
      topic: s.topic,
      partition,
      offset: list.filter(r => r.partition === partition).length,
      timestamp: Date.now() - (seedList.length - idx) * 3500,
      key: s.key,
      value: s.val,
      headers: { 'source': 'aurakafka-seeder', 'app': 'portal-web' },
      sizeBytes: s.val.length + s.key.length + 32
    });
    recordsMap.set(s.topic, list);
  });
}
seedInitialRecords();

// Consumer Groups
let consumerGroups: ConsumerGroup[] = [
  {
    groupId: 'fraud-detector-group',
    state: 'Stable',
    protocolType: 'consumer',
    protocol: 'range',
    leaderId: 'fraud-worker-node-1_a9b8c',
    members: [
      {
        memberId: 'fraud-worker-node-1_a9b8c',
        clientId: 'fraud-detector-client',
        clientHost: '/127.0.0.1',
        assignedPartitions: [
          { topic: 'payments', partition: 0 },
          { topic: 'payments', partition: 1 },
          { topic: 'payments', partition: 2 }
        ]
      }
    ],
    offsets: [
      { topic: 'payments', partition: 0, committedOffset: 31, logEndOffset: 32, lag: 1 },
      { topic: 'payments', partition: 1, committedOffset: 30, logEndOffset: 31, lag: 1 },
      { topic: 'payments', partition: 2, committedOffset: 33, logEndOffset: 33, lag: 0 }
    ],
    totalLag: 2
  },
  {
    groupId: 'analytics-worker',
    state: 'Stable',
    protocolType: 'consumer',
    protocol: 'roundrobin',
    leaderId: 'analytics-pod-87_f12c',
    members: [
      {
        memberId: 'analytics-pod-87_f12c',
        clientId: 'analytics-engine',
        clientHost: '/127.0.0.1',
        assignedPartitions: [
          { topic: 'orders-stream', partition: 0 },
          { topic: 'orders-stream', partition: 1 },
          { topic: 'orders-stream', partition: 2 }
        ]
      }
    ],
    offsets: [
      { topic: 'orders-stream', partition: 0, committedOffset: 46, logEndOffset: 48, lag: 2 },
      { topic: 'orders-stream', partition: 1, committedOffset: 45, logEndOffset: 46, lag: 1 },
      { topic: 'orders-stream', partition: 2, committedOffset: 47, logEndOffset: 48, lag: 1 }
    ],
    totalLag: 4
  },
  {
    groupId: 'console-consumer-group',
    state: 'Stable',
    protocolType: 'consumer',
    protocol: 'range',
    leaderId: 'console-client-0',
    members: [
      {
        memberId: 'console-client-0',
        clientId: 'console-consumer',
        clientHost: '/127.0.0.1',
        assignedPartitions: [
          { topic: 'quickstart-events', partition: 0 }
        ]
      }
    ],
    offsets: [
      { topic: 'quickstart-events', partition: 0, committedOffset: 34, logEndOffset: 34, lag: 0 }
    ],
    totalLag: 0
  }
];

// Operational Logs Buffer
let logsBuffer: LogEntry[] = [
  { id: '1', timestamp: new Date(Date.now() - 30000).toISOString(), level: 'INFO', subsystem: 'BROKER', message: '[KafkaBroker-1] Startup complete on 127.0.0.1:9092 (KRaft Mode Enabled)' },
  { id: '2', timestamp: new Date(Date.now() - 28000).toISOString(), level: 'INFO', subsystem: 'KRAFT', message: '[KRaftController] Quorum elected node 1 as Active Controller (epoch 4)' },
  { id: '3', timestamp: new Date(Date.now() - 25000).toISOString(), level: 'INFO', subsystem: 'STORAGE', message: '[LogManager] Loaded 5 topics with active partition segments in data/' },
  { id: '4', timestamp: new Date(Date.now() - 20000).toISOString(), level: 'INFO', subsystem: 'COORDINATOR', message: '[GroupCoordinator] Group fraud-detector-group state transition: Empty -> Stable' },
  { id: '5', timestamp: new Date(Date.now() - 15000).toISOString(), level: 'INFO', subsystem: 'COORDINATOR', message: '[GroupCoordinator] Group analytics-worker state transition: Empty -> Stable' },
  { id: '6', timestamp: new Date(Date.now() - 10000).toISOString(), level: 'DEBUG', subsystem: 'PRODUCER', message: '[Producer-101] Batch buffer flushed successfully (6 records, 1420 bytes)' },
  { id: '7', timestamp: new Date(Date.now() - 5000).toISOString(), level: 'INFO', subsystem: 'CONSUMER', message: '[Consumer-fraud-detector-group] Offset commit acknowledged on topic payments' }
];

function addLog(level: LogLevel, subsystem: Subsystem, message: string, details?: any) {
  const entry: LogEntry = {
    id: Math.random().toString(36).substring(2, 9),
    timestamp: new Date().toISOString(),
    level,
    subsystem,
    message,
    details
  };
  logsBuffer.unshift(entry);
  if (logsBuffer.length > 300) logsBuffer.pop();
  broadcast({ type: 'LOG', payload: entry });
  return entry;
}

// Real-Time Metrics & History (30 points)
let historyPoints: MetricPoint[] = [];
const nowSec = Math.floor(Date.now() / 1000);
for (let i = 29; i >= 0; i--) {
  const d = new Date((nowSec - i) * 1000);
  const timeStr = `${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}:${d.getSeconds().toString().padStart(2, '0')}`;
  historyPoints.push({
    time: timeStr,
    producedRate: Math.floor(18 + Math.sin(i / 3) * 8 + Math.random() * 6),
    consumedRate: Math.floor(16 + Math.sin(i / 3) * 7 + Math.random() * 5),
    bytesInRate: +(24.5 + Math.random() * 8).toFixed(1),
    bytesOutRate: +(22.1 + Math.random() * 7).toFixed(1),
    latencyMs: +(1.2 + Math.random() * 0.8).toFixed(2)
  });
}

let metrics: ClusterMetrics = {
  totalMessagesProduced: 407,
  totalMessagesConsumed: 401,
  totalBytesIngested: 97500,
  currentThroughputMsgsPerSec: 22,
  currentThroughputBytesPerSec: 28400,
  activeTopicsCount: topics.filter(t => !t.isInternal).length,
  totalPartitionsCount: topics.reduce((acc, t) => acc + t.partitions.length, 0),
  totalConsumerLag: consumerGroups.reduce((acc, g) => acc + g.totalLag, 0),
  activeConsumerGroupsCount: consumerGroups.length,
  avgLatencyMs: 1.45,
  history: historyPoints
};

// Simulation State
let simulationActive = true;
let simulationRate = 3; // msgs per tick

// ---------------------------------------------------------------------------
// HTTP Server & Static File Delivery
// ---------------------------------------------------------------------------
const server = http.createServer((req, res) => {
  // CORS Headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  const parsedUrl = new URL(req.url || '/', `http://${req.headers.host}`);
  const pathname = parsedUrl.pathname;

  // JSON helper
  const sendJson = (status: number, data: any) => {
    res.writeHead(status, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(data));
  };

  // Helper to parse JSON body
  const parseBody = (callback: (body: any) => void) => {
    let bodyStr = '';
    req.on('data', chunk => { bodyStr += chunk; });
    req.on('end', () => {
      try {
        const json = bodyStr ? JSON.parse(bodyStr) : {};
        callback(json);
      } catch (e: any) {
        sendJson(400, { error: 'Invalid JSON payload: ' + e.message });
      }
    });
  };

  // --- REST API ENDPOINTS ---
  if (pathname.startsWith('/api/')) {
    // GET /api/cluster
    if (pathname === '/api/cluster' && req.method === 'GET') {
      clusterState.uptimeSeconds = Math.floor((Date.now() - startTime) / 1000);
      return sendJson(200, clusterState);
    }

    // GET /api/metrics
    if (pathname === '/api/metrics' && req.method === 'GET') {
      return sendJson(200, metrics);
    }

    // GET /api/topics
    if (pathname === '/api/topics' && req.method === 'GET') {
      return sendJson(200, topics);
    }

    // POST /api/topics
    if (pathname === '/api/topics' && req.method === 'POST') {
      return parseBody(body => {
        const { name, partitions = 1, replicationFactor = 1, cleanupPolicy = 'delete', retentionMs = 604800000 } = body;
        if (!name || typeof name !== 'string') {
          return sendJson(400, { error: 'Topic name is required and must be a string' });
        }
        if (topics.some(t => t.name === name)) {
          return sendJson(409, { error: `Topic '${name}' already exists in cluster` });
        }

        const partitionList = [];
        for (let p = 0; p < partitions; p++) {
          partitionList.push({
            partition: p,
            leader: 1,
            replicas: [1],
            isr: [1],
            highWatermark: 0,
            logEndOffset: 0,
            segmentCount: 1,
            sizeBytes: 0,
            recordsCount: 0
          });
        }

        const newTopic: TopicItem = {
          name,
          isInternal: false,
          replicationFactor,
          cleanupPolicy,
          retentionMs,
          totalRecords: 0,
          totalSizeBytes: 0,
          partitions: partitionList
        };

        topics.push(newTopic);
        metrics.activeTopicsCount = topics.filter(t => !t.isInternal).length;
        metrics.totalPartitionsCount = topics.reduce((acc, t) => acc + t.partitions.length, 0);

        addLog('INFO', 'STORAGE', `[AdminClient] Created topic '${name}' (${partitions} partitions, RF: ${replicationFactor})`);
        broadcast({ type: 'TOPICS_UPDATED', payload: topics });
        return sendJson(201, newTopic);
      });
    }

    // DELETE /api/topics/:name
    if (pathname.startsWith('/api/topics/') && req.method === 'DELETE') {
      const topicName = pathname.replace('/api/topics/', '');
      const idx = topics.findIndex(t => t.name === topicName);
      if (idx === -1) {
        return sendJson(404, { error: `Topic '${topicName}' not found` });
      }
      if (topics[idx].isInternal) {
        return sendJson(403, { error: `Cannot delete internal topic '${topicName}'` });
      }

      topics.splice(idx, 1);
      recordsMap.delete(topicName);
      metrics.activeTopicsCount = topics.filter(t => !t.isInternal).length;
      metrics.totalPartitionsCount = topics.reduce((acc, t) => acc + t.partitions.length, 0);

      addLog('WARN', 'STORAGE', `[AdminClient] Deleted topic '${topicName}' from broker catalog`);
      broadcast({ type: 'TOPICS_UPDATED', payload: topics });
      return sendJson(200, { success: true, message: `Topic '${topicName}' deleted` });
    }

    // GET /api/topics/:name/records
    if (pathname.startsWith('/api/topics/') && pathname.endsWith('/records') && req.method === 'GET') {
      const parts = pathname.split('/');
      const topicName = parts[3];
      const partitionParam = parsedUrl.searchParams.get('partition');
      const limit = parseInt(parsedUrl.searchParams.get('limit') || '50', 10);

      const records = recordsMap.get(topicName) || [];
      let filtered = records;
      if (partitionParam !== null && partitionParam !== undefined && partitionParam !== '') {
        const pNum = parseInt(partitionParam, 10);
        filtered = filtered.filter(r => r.partition === pNum);
      }
      const slice = filtered.slice(-limit).reverse();
      return sendJson(200, { topic: topicName, total: filtered.length, records: slice });
    }

    // POST /api/produce
    if (pathname === '/api/produce' && req.method === 'POST') {
      return parseBody(body => {
        const { topic, key = '', value, partition: reqPartition, headers = {} } = body;
        if (!topic || value === undefined) {
          return sendJson(400, { error: 'Topic and value are required' });
        }

        const t = topics.find(item => item.name === topic);
        if (!t) {
          return sendJson(404, { error: `Topic '${topic}' not found` });
        }

        // Determine partition: specified or hashed from key
        let targetPart = 0;
        if (reqPartition !== undefined && reqPartition !== null) {
          targetPart = Math.max(0, Math.min(t.partitions.length - 1, Number(reqPartition)));
        } else if (key) {
          // Simple Murmur/Hash representation
          let hash = 0;
          for (let i = 0; i < key.length; i++) {
            hash = ((hash << 5) - hash) + key.charCodeAt(i);
            hash |= 0;
          }
          targetPart = Math.abs(hash) % t.partitions.length;
        }

        const pMeta = t.partitions[targetPart];
        const offset = pMeta.logEndOffset;
        pMeta.logEndOffset += 1;
        pMeta.highWatermark += 1;
        pMeta.recordsCount += 1;
        const recordBytes = (typeof value === 'string' ? value.length : JSON.stringify(value).length) + key.length + 32;
        pMeta.sizeBytes += recordBytes;
        t.totalRecords += 1;
        t.totalSizeBytes += recordBytes;

        const recordVal = typeof value === 'string' ? value : JSON.stringify(value);
        const newRecord: KafkaRecord = {
          topic,
          partition: targetPart,
          offset,
          timestamp: Date.now(),
          key: String(key),
          value: recordVal,
          headers: headers || {},
          sizeBytes: recordBytes
        };

        const list = recordsMap.get(topic) || [];
        list.push(newRecord);
        if (list.length > 500) list.shift();
        recordsMap.set(topic, list);

        // Update Global Metrics
        metrics.totalMessagesProduced += 1;
        metrics.totalBytesIngested += recordBytes;

        // Log entry
        addLog('INFO', 'PRODUCER', `Produced record to ${topic} [P:${targetPart} | Off:${offset}] Key: '${key}' (${recordBytes} bytes)`);

        // Emit Flow Packet animation
        const packet: FlowPacket = {
          id: Math.random().toString(36).substring(2, 8),
          fromNodeId: 'node-producer',
          toNodeId: `node-topic-${topic}`,
          key: String(key),
          topic,
          partition: targetPart,
          progress: 0,
          color: '#06b6d4'
        };
        broadcast({ type: 'FLOW_PACKET', payload: packet });

        return sendJson(200, {
          success: true,
          topic,
          partition: targetPart,
          offset,
          timestamp: newRecord.timestamp
        });
      });
    }

    // GET /api/groups
    if (pathname === '/api/groups' && req.method === 'GET') {
      // Recalculate lags
      consumerGroups.forEach(g => {
        let totalLag = 0;
        g.offsets.forEach(off => {
          const t = topics.find(item => item.name === off.topic);
          if (t) {
            const p = t.partitions.find(part => part.partition === off.partition);
            if (p) {
              off.logEndOffset = p.logEndOffset;
              off.lag = Math.max(0, off.logEndOffset - off.committedOffset);
              totalLag += off.lag;
            }
          }
        });
        g.totalLag = totalLag;
      });
      metrics.totalConsumerLag = consumerGroups.reduce((acc, g) => acc + g.totalLag, 0);
      return sendJson(200, consumerGroups);
    }

    // GET /api/logs
    if (pathname === '/api/logs' && req.method === 'GET') {
      const level = parsedUrl.searchParams.get('level');
      const subsystem = parsedUrl.searchParams.get('subsystem');
      const search = parsedUrl.searchParams.get('search');

      let filtered = logsBuffer;
      if (level && level !== 'ALL') {
        filtered = filtered.filter(l => l.level === level);
      }
      if (subsystem && subsystem !== 'ALL') {
        filtered = filtered.filter(l => l.subsystem === subsystem);
      }
      if (search) {
        const query = search.toLowerCase();
        filtered = filtered.filter(l => l.message.toLowerCase().includes(query) || (l.details && JSON.stringify(l.details).toLowerCase().includes(query)));
      }
      return sendJson(200, filtered);
    }

    // POST /api/logs/clear
    if (pathname === '/api/logs/clear' && req.method === 'POST') {
      logsBuffer = [];
      addLog('INFO', 'SYSTEM', 'Operational logs buffer cleared by administrator');
      return sendJson(200, { success: true });
    }

    // POST /api/simulation
    if (pathname === '/api/simulation' && req.method === 'POST') {
      return parseBody(body => {
        if (body.active !== undefined) simulationActive = Boolean(body.active);
        if (body.rate !== undefined) simulationRate = Number(body.rate);
        addLog('INFO', 'SYSTEM', `Traffic simulation ${simulationActive ? 'ENABLED' : 'PAUSED'} (Rate: ${simulationRate} msgs/tick)`);
        broadcast({ type: 'SIMULATION_STATUS', payload: { active: simulationActive, rate: simulationRate } });
        return sendJson(200, { active: simulationActive, rate: simulationRate });
      });
    }

    // POST /api/demo/run
    if (pathname === '/api/demo/run' && req.method === 'POST') {
      addLog('INFO', 'SYSTEM', 'Executing End-to-End Pub/Sub Demo Pipeline...');
      // Produce burst to payments and orders-stream
      for (let i = 0; i < 6; i++) {
        const key = `acct-00${(i % 3) + 1}`;
        const amt = +(50 + Math.random() * 200).toFixed(2);
        const val = JSON.stringify({ account: key, amount: amt, time: Date.now() });
        const t = topics.find(item => item.name === 'payments');
        if (t) {
          const part = i % t.partitions.length;
          const p = t.partitions[part];
          p.logEndOffset += 1;
          p.highWatermark += 1;
          p.recordsCount += 1;
          t.totalRecords += 1;
          metrics.totalMessagesProduced += 1;
        }
      }
      // Also advance consumer groups
      const fraudGroup = consumerGroups.find(g => g.groupId === 'fraud-detector-group');
      if (fraudGroup) {
        fraudGroup.offsets.forEach(off => {
          off.committedOffset = off.logEndOffset;
          off.lag = 0;
        });
        fraudGroup.totalLag = 0;
      }
      metrics.totalMessagesConsumed += 6;
      addLog('INFO', 'CONSUMER', '[fraud-detector-group] Consumed 6 payment transactions, lag caught up to HighWatermark');
      broadcast({ type: 'DEMO_COMPLETED', payload: { success: true } });
      return sendJson(200, { success: true, message: 'Demo pipeline executed successfully' });
    }

    return sendJson(404, { error: 'API route not found' });
  }

  // --- STATIC ASSET SERVING (Vite production dist) ---
  let filePath = path.join(DIST_DIR, pathname === '/' ? 'index.html' : pathname);
  if (!fs.existsSync(filePath) || fs.statSync(filePath).isDirectory()) {
    filePath = path.join(DIST_DIR, 'index.html');
  }

  if (fs.existsSync(filePath)) {
    const ext = path.extname(filePath).toLowerCase();
    const mimeTypes: Record<string, string> = {
      '.html': 'text/html',
      '.js': 'application/javascript',
      '.css': 'text/css',
      '.json': 'application/json',
      '.svg': 'image/svg+xml',
      '.png': 'image/png',
      '.jpg': 'image/jpeg',
      '.woff2': 'font/woff2'
    };
    const contentType = mimeTypes[ext] || 'application/octet-stream';
    res.writeHead(200, { 'Content-Type': contentType });
    fs.createReadStream(filePath).pipe(res);
  } else {
    // If dist does not exist yet (running dev mode), return informative page
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(`
      <html>
        <head><title>AuraKafka Embedded Web Gateway</title></head>
        <body style="font-family: sans-serif; background: #07090e; color: #f8fafc; padding: 40px; text-align: center;">
          <h1 style="color: #06b6d4;">AuraKafka Control Center Gateway Active</h1>
          <p>The backend REST &amp; WebSocket server is listening on port ${PORT}.</p>
          <p>To run with Vite Hot Reload: <code>npm run dev</code> (port 3000)</p>
          <p>Or build static bundle: <code>npm run build</code> and refresh this page.</p>
        </body>
      </html>
    `);
  }
});

// ---------------------------------------------------------------------------
// WebSocket Server for Live Real-Time Push
// ---------------------------------------------------------------------------
const wss = new WebSocketServer({ server, path: '/ws' });
const clients = new Set<WebSocket>();

wss.on('connection', ws => {
  clients.add(ws);
  // Send initial snapshot
  ws.send(JSON.stringify({
    type: 'SNAPSHOT',
    payload: {
      cluster: clusterState,
      metrics,
      topics,
      groups: consumerGroups,
      simulation: { active: simulationActive, rate: simulationRate }
    }
  }));

  ws.on('message', data => {
    try {
      const msg = JSON.parse(data.toString());
      if (msg.type === 'PING') ws.send(JSON.stringify({ type: 'PONG' }));
    } catch (e) {}
  });

  ws.on('close', () => clients.delete(ws));
});

function broadcast(msg: any) {
  const json = JSON.stringify(msg);
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(json);
    }
  }
}

// ---------------------------------------------------------------------------
// Real-Time Background Simulation & Ticker (1Hz)
// ---------------------------------------------------------------------------
setInterval(() => {
  clusterState.uptimeSeconds = Math.floor((Date.now() - startTime) / 1000);

  let currentProducedRate = 0;
  let currentConsumedRate = 0;

  if (simulationActive && simulationRate > 0) {
    // Simulate traffic on active topics
    const sampleTopics = ['orders-stream', 'payments', 'user-events'];
    const chosenTopic = sampleTopics[Math.floor(Math.random() * sampleTopics.length)];
    const t = topics.find(item => item.name === chosenTopic);

    if (t) {
      for (let k = 0; k < simulationRate; k++) {
        const partIdx = Math.floor(Math.random() * t.partitions.length);
        const p = t.partitions[partIdx];
        const offset = p.logEndOffset;
        p.logEndOffset += 1;
        p.highWatermark += 1;
        p.recordsCount += 1;
        t.totalRecords += 1;

        const fakeKey = `k-${Math.floor(Math.random() * 900 + 100)}`;
        const bytes = Math.floor(180 + Math.random() * 240);
        p.sizeBytes += bytes;
        t.totalSizeBytes += bytes;

        metrics.totalMessagesProduced += 1;
        metrics.totalBytesIngested += bytes;
        currentProducedRate += 1;

        // Auto-consume simulation for active consumer groups
        const group = consumerGroups.find(g => g.offsets.some(o => o.topic === chosenTopic));
        if (group) {
          const off = group.offsets.find(o => o.topic === chosenTopic && o.partition === partIdx);
          if (off) {
            // Randomly catch up or lag slightly
            if (Math.random() > 0.3) {
              off.committedOffset += 1;
              metrics.totalMessagesConsumed += 1;
              currentConsumedRate += 1;
            }
            off.logEndOffset = p.logEndOffset;
            off.lag = Math.max(0, off.logEndOffset - off.committedOffset);
          }
        }

        // Random flow packet
        if (k === 0 && Math.random() > 0.4) {
          broadcast({
            type: 'FLOW_PACKET',
            payload: {
              id: Math.random().toString(36).substring(2, 7),
              fromNodeId: 'node-producer',
              toNodeId: `node-topic-${chosenTopic}`,
              key: fakeKey,
              topic: chosenTopic,
              partition: partIdx,
              progress: 0,
              color: chosenTopic === 'payments' ? '#10b981' : chosenTopic === 'orders-stream' ? '#06b6d4' : '#8b5cf6'
            }
          });
        }
      }
    }
  }

  // Latency calculation with slight variation
  const latency = +(1.1 + Math.random() * 0.7).toFixed(2);
  const now = new Date();
  const timeStr = `${now.getHours().toString().padStart(2, '0')}:${now.getMinutes().toString().padStart(2, '0')}:${now.getSeconds().toString().padStart(2, '0')}`;

  metrics.currentThroughputMsgsPerSec = currentProducedRate * (simulationRate > 0 ? simulationRate : 1);
  metrics.currentThroughputBytesPerSec = metrics.currentThroughputMsgsPerSec * 210;
  metrics.avgLatencyMs = latency;
  metrics.totalConsumerLag = consumerGroups.reduce((acc, g) => acc + g.totalLag, 0);

  // Push new history point
  metrics.history.push({
    time: timeStr,
    producedRate: metrics.currentThroughputMsgsPerSec,
    consumedRate: currentConsumedRate * (simulationRate > 0 ? simulationRate : 1),
    bytesInRate: +(metrics.currentThroughputBytesPerSec / 1024).toFixed(1),
    bytesOutRate: +((metrics.currentThroughputBytesPerSec * 0.95) / 1024).toFixed(1),
    latencyMs: latency
  });
  if (metrics.history.length > 30) metrics.history.shift();

  // Broadcast tick to all connected UI clients
  broadcast({
    type: 'TICK',
    payload: {
      cluster: clusterState,
      metrics,
      topics,
      groups: consumerGroups
    }
  });
}, 1000);

// Start listening
server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n================================================================================`);
  console.log(`🚀 AuraKafka Embedded Web Server & Observability Portal Active`);
  console.log(`  • Web Interface:    http://localhost:${PORT}`);
  console.log(`  • WebSocket Stream: ws://localhost:${PORT}/ws`);
  console.log(`  • Cluster Mode:     KRaft Controller Quorum (Broker ID: 1)`);
  console.log(`================================================================================\n`);
});
