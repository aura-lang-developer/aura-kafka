import { ClusterInfo, ClusterMetrics, TopicItem, ConsumerGroup, LogEntry, KafkaRecord } from '../types/kafka';

const API_BASE = '/api';

export async function fetchClusterInfo(): Promise<ClusterInfo> {
  const res = await fetch(`${API_BASE}/cluster`);
  if (!res.ok) throw new Error('Failed to fetch cluster info');
  return res.json();
}

export async function fetchMetrics(): Promise<ClusterMetrics> {
  const res = await fetch(`${API_BASE}/metrics`);
  if (!res.ok) throw new Error('Failed to fetch metrics');
  return res.json();
}

export async function fetchTopics(): Promise<TopicItem[]> {
  const res = await fetch(`${API_BASE}/topics`);
  if (!res.ok) throw new Error('Failed to fetch topics');
  return res.json();
}

export async function createTopic(data: {
  name: string;
  partitions: number;
  replicationFactor: number;
  cleanupPolicy: string;
}): Promise<TopicItem> {
  const res = await fetch(`${API_BASE}/topics`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data)
  });
  if (!res.ok) {
    const err = await res.json();
    throw new Error(err.error || 'Failed to create topic');
  }
  return res.json();
}

export async function deleteTopic(name: string): Promise<void> {
  const res = await fetch(`${API_BASE}/topics/${encodeURIComponent(name)}`, {
    method: 'DELETE'
  });
  if (!res.ok) {
    const err = await res.json();
    throw new Error(err.error || 'Failed to delete topic');
  }
}

export async function fetchTopicRecords(
  topic: string,
  partition?: number,
  limit: number = 50
): Promise<{ topic: string; total: number; records: KafkaRecord[] }> {
  let url = `${API_BASE}/topics/${encodeURIComponent(topic)}/records?limit=${limit}`;
  if (partition !== undefined && partition !== null) {
    url += `&partition=${partition}`;
  }
  const res = await fetch(url);
  if (!res.ok) throw new Error('Failed to fetch records');
  return res.json();
}

export async function produceRecord(data: {
  topic: string;
  key?: string;
  value: any;
  partition?: number;
  headers?: Record<string, string>;
}): Promise<any> {
  const res = await fetch(`${API_BASE}/produce`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data)
  });
  if (!res.ok) {
    const err = await res.json();
    throw new Error(err.error || 'Failed to produce record');
  }
  return res.json();
}

export async function fetchConsumerGroups(): Promise<ConsumerGroup[]> {
  const res = await fetch(`${API_BASE}/groups`);
  if (!res.ok) throw new Error('Failed to fetch consumer groups');
  return res.json();
}

export async function fetchLogs(params?: {
  level?: string;
  subsystem?: string;
  search?: string;
}): Promise<LogEntry[]> {
  const query = new URLSearchParams();
  if (params?.level) query.set('level', params.level);
  if (params?.subsystem) query.set('subsystem', params.subsystem);
  if (params?.search) query.set('search', params.search);

  const res = await fetch(`${API_BASE}/logs?${query.toString()}`);
  if (!res.ok) throw new Error('Failed to fetch logs');
  return res.json();
}

export async function clearLogs(): Promise<void> {
  const res = await fetch(`${API_BASE}/logs/clear`, { method: 'POST' });
  if (!res.ok) throw new Error('Failed to clear logs');
}

export async function setSimulation(active: boolean, rate: number): Promise<{ active: boolean; rate: number }> {
  const res = await fetch(`${API_BASE}/simulation`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ active, rate })
  });
  if (!res.ok) throw new Error('Failed to update simulation');
  return res.json();
}

export async function runDemoPipeline(): Promise<{ success: boolean; message: string }> {
  const res = await fetch(`${API_BASE}/demo/run`, { method: 'POST' });
  if (!res.ok) throw new Error('Failed to run demo pipeline');
  return res.json();
}
