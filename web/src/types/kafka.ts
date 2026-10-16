export type LogLevel = 'INFO' | 'WARN' | 'ERROR' | 'DEBUG';
export type Subsystem = 'BROKER' | 'KRAFT' | 'STORAGE' | 'COORDINATOR' | 'PRODUCER' | 'CONSUMER' | 'SYSTEM';

export interface LogEntry {
  id: string;
  timestamp: string;
  level: LogLevel;
  subsystem: Subsystem;
  message: string;
  details?: Record<string, any>;
}

export interface PartitionMeta {
  partition: number;
  leader: number;
  replicas: number[];
  isr: number[];
  highWatermark: number;
  logEndOffset: number;
  segmentCount: number;
  sizeBytes: number;
  recordsCount: number;
}

export interface TopicItem {
  name: string;
  isInternal: boolean;
  partitions: PartitionMeta[];
  totalRecords: number;
  totalSizeBytes: number;
  replicationFactor: number;
  cleanupPolicy: string;
  retentionMs: number;
}

export interface KafkaRecord {
  topic: string;
  partition: number;
  offset: number;
  timestamp: number;
  key: string;
  value: string;
  headers: Record<string, string>;
  sizeBytes: number;
}

export interface GroupMember {
  memberId: string;
  clientId: string;
  clientHost: string;
  assignedPartitions: { topic: string; partition: number }[];
}

export interface ConsumerGroup {
  groupId: string;
  state: 'Stable' | 'PreparingRebalance' | 'CompletingRebalance' | 'Dead' | 'Empty';
  protocolType: string;
  protocol: string;
  leaderId: string;
  members: GroupMember[];
  offsets: {
    topic: string;
    partition: number;
    committedOffset: number;
    logEndOffset: number;
    lag: number;
  }[];
  totalLag: number;
}

export interface KRaftQuorumState {
  clusterId: string;
  brokerId: number;
  role: 'Leader' | 'Follower' | 'Candidate';
  leaderId: number;
  currentEpoch: number;
  voters: number[];
  quorumStatus: 'HEALTHY' | 'DEGRADED';
  metadataLogEndOffset: number;
  highWatermark: number;
}

export interface ClusterInfo {
  brokerId: number;
  port: number;
  host: string;
  version: string;
  uptimeSeconds: number;
  isRunning: boolean;
  kraft: KRaftQuorumState;
  storage: {
    baseDir: string;
    numPartitionsDefault: number;
    segmentMaxBytes: number;
    totalLogSegments: number;
    totalDiskBytes: number;
  };
}

export interface MetricPoint {
  time: string;
  producedRate: number; // msgs/sec
  consumedRate: number; // msgs/sec
  bytesInRate: number;  // KB/sec
  bytesOutRate: number; // KB/sec
  latencyMs: number;
}

export interface ClusterMetrics {
  totalMessagesProduced: number;
  totalMessagesConsumed: number;
  totalBytesIngested: number;
  currentThroughputMsgsPerSec: number;
  currentThroughputBytesPerSec: number;
  activeTopicsCount: number;
  totalPartitionsCount: number;
  totalConsumerLag: number;
  activeConsumerGroupsCount: number;
  avgLatencyMs: number;
  history: MetricPoint[];
}

export interface FlowNode {
  id: string;
  name: string;
  type: 'producer' | 'broker' | 'topic' | 'partition' | 'consumer_group';
  status: 'active' | 'idle' | 'warning';
  rate?: number;
  details?: Record<string, any>;
  x?: number;
  y?: number;
}

export interface FlowPacket {
  id: string;
  fromNodeId: string;
  toNodeId: string;
  key: string;
  topic: string;
  partition: number;
  progress: number; // 0 to 1
  color: string;
}
