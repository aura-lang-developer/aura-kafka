import React from 'react';
import { 
  Cpu, 
  Server, 
  HardDrive, 
  CheckCircle2, 
  Layers, 
  KeyRound, 
  ShieldCheck, 
  Activity 
} from 'lucide-react';
import { ClusterInfo } from '../types/kafka';

interface ClusterViewProps {
  cluster: ClusterInfo | null;
}

export const ClusterView: React.FC<ClusterViewProps> = ({ cluster }) => {
  const apiKeys = [
    { key: 0, name: 'Produce', versions: 'v0-v9', status: 'ACTIVE' },
    { key: 1, name: 'Fetch', versions: 'v0-v12', status: 'ACTIVE' },
    { key: 2, name: 'ListOffsets', versions: 'v0-v7', status: 'ACTIVE' },
    { key: 3, name: 'Metadata', versions: 'v0-v12', status: 'ACTIVE' },
    { key: 4, name: 'LeaderAndIsr', versions: 'v0-v7', status: 'ACTIVE' },
    { key: 8, name: 'OffsetCommit', versions: 'v0-v8', status: 'ACTIVE' },
    { key: 9, name: 'OffsetFetch', versions: 'v0-v8', status: 'ACTIVE' },
    { key: 10, name: 'FindCoordinator', versions: 'v0-v4', status: 'ACTIVE' },
    { key: 11, name: 'JoinGroup', versions: 'v0-v9', status: 'ACTIVE' },
    { key: 12, name: 'Heartbeat', versions: 'v0-v4', status: 'ACTIVE' },
    { key: 13, name: 'LeaveGroup', versions: 'v0-v5', status: 'ACTIVE' },
    { key: 14, name: 'SyncGroup', versions: 'v0-v5', status: 'ACTIVE' },
    { key: 18, name: 'ApiVersions', versions: 'v0-v3', status: 'ACTIVE' },
    { key: 19, name: 'CreateTopics', versions: 'v0-v7', status: 'ACTIVE' },
    { key: 20, name: 'DeleteTopics', versions: 'v0-v6', status: 'ACTIVE' }
  ];

  const formatBytes = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      {/* Header */}
      <div className="glass-panel" style={{ padding: '20px 24px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <Cpu size={20} color="#3b82f6" />
          <h2 style={{ fontSize: '1.25rem', fontWeight: 800 }}>
            Arquitectura del Clúster, KRaft Quórum &amp; Motor de Almacenamiento
          </h2>
          <span className="badge badge-cyan">KRaft Native</span>
        </div>
        <p style={{ fontSize: '0.82rem', color: 'var(--text-muted)', marginTop: 4 }}>
          Implementación nativa de Apache Kafka sobre el lenguaje Aura (v0.6.0) sin dependencia de ZooKeeper.
        </p>
      </div>

      {/* Grid: KRaft Quorum + Storage */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20 }}>
        {/* KRaft Controller Quorum */}
        <div className="glass-panel" style={{ padding: '24px 26px', borderTop: '4px solid #10b981' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
              <ShieldCheck size={20} color="#10b981" />
              <h3 style={{ fontSize: '1.05rem', fontWeight: 700 }}>KRaft Controller Quorum</h3>
            </div>
            <span className="badge badge-emerald">
              <CheckCircle2 size={12} /> {cluster?.kraft.quorumStatus || 'HEALTHY'}
            </span>
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', gap: 10, fontSize: '0.82rem' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', padding: '8px 12px', background: 'rgba(0,0,0,0.25)', borderRadius: 6 }}>
              <span style={{ color: 'var(--text-muted)' }}>Cluster ID:</span>
              <span style={{ fontFamily: 'var(--font-mono)', fontWeight: 600, color: 'var(--accent-cyan)' }}>
                {cluster?.kraft.clusterId}
              </span>
            </div>

            <div style={{ display: 'flex', justifyContent: 'space-between', padding: '8px 12px', background: 'rgba(0,0,0,0.25)', borderRadius: 6 }}>
              <span style={{ color: 'var(--text-muted)' }}>Rol de este Nodo:</span>
              <span style={{ fontWeight: 700, color: '#34d399' }}>{cluster?.kraft.role} (Broker #{cluster?.kraft.brokerId})</span>
            </div>

            <div style={{ display: 'flex', justifyContent: 'space-between', padding: '8px 12px', background: 'rgba(0,0,0,0.25)', borderRadius: 6 }}>
              <span style={{ color: 'var(--text-muted)' }}>Líder del Quórum Activo:</span>
              <span style={{ fontFamily: 'var(--font-mono)' }}>Node #{cluster?.kraft.leaderId}</span>
            </div>

            <div style={{ display: 'flex', justifyContent: 'space-between', padding: '8px 12px', background: 'rgba(0,0,0,0.25)', borderRadius: 6 }}>
              <span style={{ color: 'var(--text-muted)' }}>Época Actual (Epoch):</span>
              <span style={{ fontFamily: 'var(--font-mono)' }}>{cluster?.kraft.currentEpoch}</span>
            </div>

            <div style={{ display: 'flex', justifyContent: 'space-between', padding: '8px 12px', background: 'rgba(0,0,0,0.25)', borderRadius: 6 }}>
              <span style={{ color: 'var(--text-muted)' }}>Votantes en Quórum:</span>
              <span style={{ fontFamily: 'var(--font-mono)' }}>[{cluster?.kraft.voters.join(', ')}]</span>
            </div>

            <div style={{ display: 'flex', justifyContent: 'space-between', padding: '8px 12px', background: 'rgba(0,0,0,0.25)', borderRadius: 6 }}>
              <span style={{ color: 'var(--text-muted)' }}>Log de Metadatos (HWM):</span>
              <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--accent-cyan)' }}>
                Offset {cluster?.kraft.highWatermark}
              </span>
            </div>
          </div>
        </div>

        {/* Storage Engine LogManager */}
        <div className="glass-panel" style={{ padding: '24px 26px', borderTop: '4px solid #3b82f6' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
              <HardDrive size={20} color="#3b82f6" />
              <h3 style={{ fontSize: '1.05rem', fontWeight: 700 }}>Motor de Almacenamiento (LogManager)</h3>
            </div>
            <span className="badge badge-cyan">Append-Only Monotónico</span>
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', gap: 10, fontSize: '0.82rem' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', padding: '8px 12px', background: 'rgba(0,0,0,0.25)', borderRadius: 6 }}>
              <span style={{ color: 'var(--text-muted)' }}>Directorio Base de Logs:</span>
              <span style={{ fontFamily: 'var(--font-mono)', fontSize: '0.75rem', color: '#38bdf8' }}>
                {cluster?.storage.baseDir}
              </span>
            </div>

            <div style={{ display: 'flex', justifyContent: 'space-between', padding: '8px 12px', background: 'rgba(0,0,0,0.25)', borderRadius: 6 }}>
              <span style={{ color: 'var(--text-muted)' }}>Espacio Total Ocupado:</span>
              <span style={{ fontFamily: 'var(--font-mono)', fontWeight: 600 }}>
                {formatBytes(cluster?.storage.totalDiskBytes ?? 0)}
              </span>
            </div>

            <div style={{ display: 'flex', justifyContent: 'space-between', padding: '8px 12px', background: 'rgba(0,0,0,0.25)', borderRadius: 6 }}>
              <span style={{ color: 'var(--text-muted)' }}>Segmentos de Log Abiertos:</span>
              <span style={{ fontFamily: 'var(--font-mono)' }}>{cluster?.storage.totalLogSegments} segmentos (.log / .index)</span>
            </div>

            <div style={{ display: 'flex', justifyContent: 'space-between', padding: '8px 12px', background: 'rgba(0,0,0,0.25)', borderRadius: 6 }}>
              <span style={{ color: 'var(--text-muted)' }}>Tamaño Máximo por Segmento:</span>
              <span style={{ fontFamily: 'var(--font-mono)' }}>
                {formatBytes(cluster?.storage.segmentMaxBytes ?? 10485760)}
              </span>
            </div>

            <div style={{ display: 'flex', justifyContent: 'space-between', padding: '8px 12px', background: 'rgba(0,0,0,0.25)', borderRadius: 6 }}>
              <span style={{ color: 'var(--text-muted)' }}>Estrategia de Rolling:</span>
              <span>Por tamaño límite de segmento (Active Rolling)</span>
            </div>

            <div style={{ display: 'flex', justifyContent: 'space-between', padding: '8px 12px', background: 'rgba(0,0,0,0.25)', borderRadius: 6 }}>
              <span style={{ color: 'var(--text-muted)' }}>High Watermark Manager:</span>
              <span style={{ color: '#10b981', fontWeight: 600 }}>Sincronización ISR Continua</span>
            </div>
          </div>
        </div>
      </div>

      {/* Wire Protocol ApiKeys Matrix */}
      <div className="glass-panel" style={{ padding: '24px 26px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 6 }}>
          <KeyRound size={18} color="#06b6d4" />
          <h3 style={{ fontSize: '1.05rem', fontWeight: 700 }}>
            Matriz de ApiKeys del Protocolo Wire Kafka Implementadas en Aura
          </h3>
        </div>
        <p style={{ fontSize: '0.78rem', color: 'var(--text-muted)', marginBottom: 18 }}>
          Soporte nativo de esquemas de petición/respuesta según especificación Apache Kafka v3.8.0
        </p>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 12 }}>
          {apiKeys.map(k => (
            <div
              key={k.key}
              style={{
                background: 'rgba(0,0,0,0.25)',
                border: '1px solid var(--border-subtle)',
                borderRadius: 8,
                padding: '12px 14px',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between'
              }}
            >
              <div>
                <div style={{ fontSize: '0.85rem', fontWeight: 600, color: 'var(--text-primary)' }}>
                  {k.name}
                </div>
                <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>
                  ApiKey {k.key} ({k.versions})
                </div>
              </div>
              <span className="badge badge-emerald" style={{ fontSize: '0.62rem' }}>
                {k.status}
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};
