import React, { useState, useEffect } from 'react';
import { 
  Play, 
  Pause, 
  Zap, 
  Layers, 
  Server, 
  Cpu, 
  Send, 
  DownloadCloud, 
  Info,
  CheckCircle2,
  HardDrive
} from 'lucide-react';
import { TopicItem, ConsumerGroup, ClusterInfo, FlowPacket } from '../types/kafka';

interface FlowVisualizerViewProps {
  topics: TopicItem[];
  consumerGroups: ConsumerGroup[];
  cluster: ClusterInfo | null;
  simulationActive: boolean;
  onToggleSimulation: () => void;
  onProduceBurst: (count: number) => void;
  livePackets: FlowPacket[];
}

export const FlowVisualizerView: React.FC<FlowVisualizerViewProps> = ({
  topics,
  consumerGroups,
  cluster,
  simulationActive,
  onToggleSimulation,
  onProduceBurst,
  livePackets
}) => {
  const [selectedNode, setSelectedNode] = useState<{
    id: string;
    title: string;
    type: string;
    stats: Record<string, any>;
  } | null>(null);

  const [activePackets, setActivePackets] = useState<FlowPacket[]>([]);
  const nonInternalTopics = topics.filter(t => !t.isInternal);

  // Animate packets over time
  useEffect(() => {
    if (livePackets.length > 0) {
      setActivePackets(prev => {
        const ids = new Set(prev.map(p => p.id));
        const newOnes = livePackets.filter(p => !ids.has(p.id));
        return [...prev, ...newOnes].slice(-15);
      });
    }
  }, [livePackets]);

  useEffect(() => {
    const interval = setInterval(() => {
      setActivePackets(prev => {
        return prev
          .map(p => ({ ...p, progress: p.progress + 0.05 }))
          .filter(p => p.progress < 1.0);
      });
    }, 50);
    return () => clearInterval(interval);
  }, []);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      {/* Visualizer Header & Toolbar */}
      <div className="glass-panel" style={{ padding: '20px 24px', display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 16 }}>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <h2 style={{ fontSize: '1.25rem', fontWeight: 800 }}>
              Topología y Flujo de Datos en Tiempo Real
            </h2>
            <span className="badge badge-cyan" style={{ fontSize: '0.65rem' }}>Pipeline Interactivo</span>
          </div>
          <p style={{ fontSize: '0.82rem', color: 'var(--text-muted)', marginTop: 4 }}>
            Visualización interactiva de la ruta de los eventos: Productores &rarr; Broker KRaft &rarr; Logs de Partición &rarr; Grupos de Consumidores
          </p>
        </div>

        {/* Toolbar Controls */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <button
            onClick={onToggleSimulation}
            className={`btn ${simulationActive ? 'btn-secondary' : 'btn-emerald'}`}
            style={{ fontSize: '0.82rem' }}
          >
            {simulationActive ? <Pause size={14} color="#f59e0b" /> : <Play size={14} />}
            <span>{simulationActive ? 'Pausar Flujo' : 'Reanudar Flujo'}</span>
          </button>

          <button
            onClick={() => onProduceBurst(5)}
            className="btn btn-primary"
            style={{ fontSize: '0.82rem' }}
          >
            <Zap size={14} />
            <span>Inyectar Eventos (5 msgs)</span>
          </button>
        </div>
      </div>

      {/* Main Visualizer Diagram Canvas */}
      <div className="glass-panel" style={{
        padding: '36px 24px',
        position: 'relative',
        minHeight: 520,
        background: 'radial-gradient(ellipse at center, rgba(13, 20, 36, 0.7) 0%, rgba(7, 9, 14, 0.95) 100%)',
        overflow: 'hidden'
      }}>
        {/* Animated Background Grid Lines */}
        <div style={{
          position: 'absolute',
          inset: 0,
          backgroundImage: 'radial-gradient(rgba(255, 255, 255, 0.08) 1px, transparent 1px)',
          backgroundSize: '24px 24px',
          opacity: 0.35,
          pointerEvents: 'none'
        }} />

        {/* 4 Pipeline Stages Columns Grid */}
        <div style={{
          display: 'grid',
          gridTemplateColumns: '1fr 1.2fr 1.4fr 1.2fr',
          gap: 28,
          position: 'relative',
          zIndex: 2
        }}>
          {/* ================= STAGE 1: PRODUCERS ================= */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
              <Send size={16} color="#06b6d4" />
              <span style={{ fontSize: '0.82rem', fontWeight: 700, textTransform: 'uppercase', color: 'var(--text-secondary)' }}>
                1. Productores
              </span>
            </div>

            {/* Producer 1: Web Ingress */}
            <div
              className="glass-panel"
              onClick={() => setSelectedNode({
                id: 'prod-web',
                title: 'Web Ingress Producer',
                type: 'KafkaProducer (Aura)',
                stats: { acks: 'all', batchSize: '16KB', compression: 'none', clientId: 'web-producer-app' }
              })}
              style={{
                padding: '16px',
                cursor: 'pointer',
                borderColor: selectedNode?.id === 'prod-web' ? 'var(--accent-cyan)' : 'var(--border-subtle)',
                background: selectedNode?.id === 'prod-web' ? 'rgba(6, 182, 212, 0.1)' : 'var(--bg-card)'
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <span style={{ fontWeight: 600, fontSize: '0.85rem' }}>Portal Web Client</span>
                <span className="status-dot status-dot-cyan" />
              </div>
              <p style={{ fontSize: '0.72rem', color: 'var(--text-muted)', marginTop: 4 }}>
                Envío interactivo &amp; ráfagas
              </p>
              <div style={{ marginTop: 10, display: 'flex', gap: 6 }}>
                <span className="badge badge-cyan" style={{ fontSize: '0.62rem' }}>acks=all</span>
                <span className="badge badge-gray" style={{ fontSize: '0.62rem' }}>Partition Hash</span>
              </div>
            </div>

            {/* Producer 2: Payment Gateway */}
            <div
              className="glass-panel"
              onClick={() => setSelectedNode({
                id: 'prod-pay',
                title: 'Payment Gateway Streamer',
                type: 'KafkaProducer (Aura)',
                stats: { acks: '1', batchSize: '32KB', targetTopic: 'payments', retries: 5 }
              })}
              style={{
                padding: '16px',
                cursor: 'pointer',
                borderColor: selectedNode?.id === 'prod-pay' ? 'var(--accent-cyan)' : 'var(--border-subtle)',
                background: selectedNode?.id === 'prod-pay' ? 'rgba(6, 182, 212, 0.1)' : 'var(--bg-card)'
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <span style={{ fontWeight: 600, fontSize: '0.85rem' }}>Payment Gateway</span>
                <span className="status-dot status-dot-emerald" />
              </div>
              <p style={{ fontSize: '0.72rem', color: 'var(--text-muted)', marginTop: 4 }}>
                Transacciones monetarias
              </p>
              <div style={{ marginTop: 10, display: 'flex', gap: 6 }}>
                <span className="badge badge-emerald" style={{ fontSize: '0.62rem' }}>Target: payments</span>
              </div>
            </div>

            {/* Producer 3: Sensor/Events */}
            <div
              className="glass-panel"
              onClick={() => setSelectedNode({
                id: 'prod-events',
                title: 'User Activity Streamer',
                type: 'KafkaProducer (Aura)',
                stats: { targetTopic: 'user-events', bufferMs: 100, lingerMs: 5 }
              })}
              style={{
                padding: '16px',
                cursor: 'pointer',
                borderColor: selectedNode?.id === 'prod-events' ? 'var(--accent-cyan)' : 'var(--border-subtle)',
                background: selectedNode?.id === 'prod-events' ? 'rgba(6, 182, 212, 0.1)' : 'var(--bg-card)'
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <span style={{ fontWeight: 600, fontSize: '0.85rem' }}>User Events Pod</span>
                <span className="status-dot status-dot-cyan" />
              </div>
              <p style={{ fontSize: '0.72rem', color: 'var(--text-muted)', marginTop: 4 }}>
                Telemetry &amp; Audit Logs
              </p>
              <div style={{ marginTop: 10, display: 'flex', gap: 6 }}>
                <span className="badge badge-violet" style={{ fontSize: '0.62rem' }}>Target: user-events</span>
              </div>
            </div>
          </div>

          {/* ================= STAGE 2: KAFKA BROKER / KRAFT ================= */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
              <Server size={16} color="#3b82f6" />
              <span style={{ fontSize: '0.82rem', fontWeight: 700, textTransform: 'uppercase', color: 'var(--text-secondary)' }}>
                2. Broker KRaft
              </span>
            </div>

            <div
              className="glass-panel"
              onClick={() => setSelectedNode({
                id: 'broker-core',
                title: `AuraKafka Broker #${cluster?.brokerId || 1}`,
                type: 'KafkaBroker (Aura Core)',
                stats: {
                  port: cluster?.port || 9092,
                  kraftLeader: cluster?.kraft.leaderId,
                  epoch: cluster?.kraft.currentEpoch,
                  hwmManager: 'Active',
                  replicaManager: 'Active',
                  storageDir: cluster?.storage.baseDir
                }
              })}
              style={{
                padding: '20px',
                cursor: 'pointer',
                border: '1px solid #3b82f6',
                background: 'rgba(59, 130, 246, 0.08)',
                boxShadow: '0 0 25px rgba(59, 130, 246, 0.15)'
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 10 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <Cpu size={18} color="#38bdf8" />
                  <span style={{ fontWeight: 700, fontSize: '0.92rem' }}>KafkaBroker-1</span>
                </div>
                <span className="badge badge-emerald">LEADER</span>
              </div>

              <div style={{ display: 'flex', flexDirection: 'column', gap: 8, fontSize: '0.75rem', color: 'var(--text-secondary)' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                  <span>Puerto Wire:</span>
                  <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-primary)' }}>:9092</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                  <span>KRaft Quórum:</span>
                  <span style={{ color: '#34d399', fontWeight: 600 }}>HEALTHY (Epoch {cluster?.kraft.currentEpoch})</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                  <span>Group Coordinator:</span>
                  <span style={{ color: 'var(--text-primary)' }}>3 Grupos Activos</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                  <span>HighWatermark Mgr:</span>
                  <span style={{ color: '#06b6d4' }}>Auto-Advance ON</span>
                </div>
              </div>

              <div style={{ marginTop: 14, paddingTop: 10, borderTop: '1px solid var(--border-subtle)', display: 'flex', alignItems: 'center', gap: 8 }}>
                <HardDrive size={13} color="var(--text-muted)" />
                <span style={{ fontSize: '0.7rem', color: 'var(--text-muted)' }}>Almacenamiento Append-Only (Aura)</span>
              </div>
            </div>
          </div>

          {/* ================= STAGE 3: TOPICS & PARTITIONS ================= */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
              <Layers size={16} color="#8b5cf6" />
              <span style={{ fontSize: '0.82rem', fontWeight: 700, textTransform: 'uppercase', color: 'var(--text-secondary)' }}>
                3. Logs de Partición
              </span>
            </div>

            {nonInternalTopics.slice(0, 3).map(topic => (
              <div
                key={topic.name}
                className="glass-panel"
                onClick={() => setSelectedNode({
                  id: `topic-${topic.name}`,
                  title: `Tópico '${topic.name}'`,
                  type: 'PartitionLog Catalog (LogManager)',
                  stats: {
                    partitions: topic.partitions.length,
                    recordsTotal: topic.totalRecords,
                    replication: topic.replicationFactor,
                    retention: `${topic.retentionMs / 3600000}h`
                  }
                })}
                style={{
                  padding: '14px 16px',
                  cursor: 'pointer',
                  borderColor: selectedNode?.id === `topic-${topic.name}` ? 'var(--accent-violet)' : 'var(--border-subtle)'
                }}
              >
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
                  <span style={{ fontWeight: 600, fontSize: '0.85rem', color: '#c084fc' }}>{topic.name}</span>
                  <span style={{ fontSize: '0.72rem', fontFamily: 'var(--font-mono)', color: 'var(--text-secondary)' }}>
                    {topic.totalRecords} msgs
                  </span>
                </div>

                {/* Individual Partitions Visual Bars */}
                <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                  {topic.partitions.map(p => (
                    <div key={p.partition} style={{
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'space-between',
                      background: 'rgba(0, 0, 0, 0.3)',
                      padding: '4px 8px',
                      borderRadius: 4,
                      fontSize: '0.7rem',
                      fontFamily: 'var(--font-mono)'
                    }}>
                      <span style={{ color: 'var(--text-muted)' }}>P-{p.partition}</span>
                      <span style={{ color: '#06b6d4' }}>HWM: {p.highWatermark}</span>
                      <span style={{ color: '#10b981' }}>LEO: {p.logEndOffset}</span>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>

          {/* ================= STAGE 4: CONSUMER GROUPS ================= */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
              <DownloadCloud size={16} color="#10b981" />
              <span style={{ fontSize: '0.82rem', fontWeight: 700, textTransform: 'uppercase', color: 'var(--text-secondary)' }}>
                4. Consumidores
              </span>
            </div>

            {consumerGroups.map(group => (
              <div
                key={group.groupId}
                className="glass-panel"
                onClick={() => setSelectedNode({
                  id: `group-${group.groupId}`,
                  title: `Grupo '${group.groupId}'`,
                  type: 'ConsumerGroup (GroupCoordinator)',
                  stats: {
                    state: group.state,
                    membersCount: group.members.length,
                    protocol: group.protocol,
                    totalLag: group.totalLag
                  }
                })}
                style={{
                  padding: '16px',
                  cursor: 'pointer',
                  borderColor: selectedNode?.id === `group-${group.groupId}` ? 'var(--accent-emerald)' : 'var(--border-subtle)'
                }}
              >
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                  <span style={{ fontWeight: 600, fontSize: '0.85rem' }}>{group.groupId}</span>
                  <span className="badge badge-emerald" style={{ fontSize: '0.62rem' }}>{group.state}</span>
                </div>
                <div style={{ marginTop: 8, display: 'flex', alignItems: 'center', justifyContent: 'space-between', fontSize: '0.72rem' }}>
                  <span style={{ color: 'var(--text-muted)' }}>Lag Acumulado:</span>
                  <span style={{
                    fontFamily: 'var(--font-mono)',
                    fontWeight: 700,
                    color: group.totalLag > 0 ? '#f59e0b' : '#34d399'
                  }}>
                    {group.totalLag} msgs
                  </span>
                </div>
                <div style={{ marginTop: 6, fontSize: '0.68rem', color: 'var(--text-dim)' }}>
                  Miembro: {group.members[0]?.clientId || 'worker'}
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Live Flow Packets Floating Animation */}
        <div style={{ position: 'absolute', inset: 0, pointerEvents: 'none', zIndex: 10 }}>
          {activePackets.map(pkt => {
            const leftPct = Math.round(pkt.progress * 85 + 5);
            return (
              <div
                key={pkt.id}
                style={{
                  position: 'absolute',
                  left: `${leftPct}%`,
                  top: '45%',
                  transform: 'translateY(-50%)',
                  background: pkt.color,
                  boxShadow: `0 0 15px ${pkt.color}`,
                  borderRadius: 20,
                  padding: '3px 8px',
                  color: '#07090e',
                  fontSize: '0.68rem',
                  fontWeight: 700,
                  fontFamily: 'var(--font-mono)',
                  display: 'flex',
                  alignItems: 'center',
                  gap: 4,
                  transition: 'left 0.05s linear'
                }}
              >
                <Zap size={10} />
                <span>{pkt.topic}[P:{pkt.partition}]</span>
              </div>
            );
          })}
        </div>
      </div>

      {/* Selected Node Details Drawer */}
      {selectedNode && (
        <div className="glass-panel" style={{ padding: '20px 24px', borderLeft: '4px solid var(--accent-cyan)' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
              <Info size={18} color="#06b6d4" />
              <h3 style={{ fontSize: '1.05rem', fontWeight: 700 }}>{selectedNode.title}</h3>
              <span className="badge badge-cyan" style={{ fontSize: '0.68rem' }}>{selectedNode.type}</span>
            </div>
            <button
              className="btn btn-secondary"
              onClick={() => setSelectedNode(null)}
              style={{ fontSize: '0.75rem', padding: '4px 8px' }}
            >
              Cerrar
            </button>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 14 }}>
            {Object.entries(selectedNode.stats).map(([key, val]) => (
              <div key={key} style={{ background: 'rgba(0,0,0,0.3)', padding: '10px 14px', borderRadius: 6 }}>
                <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)', textTransform: 'uppercase' }}>{key}</div>
                <div style={{ fontSize: '0.85rem', fontWeight: 600, color: 'var(--text-primary)', marginTop: 2, fontFamily: 'var(--font-mono)' }}>
                  {String(val)}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};
