import React from 'react';
import { 
  TrendingUp, 
  Layers, 
  Clock, 
  Database, 
  AlertTriangle, 
  CheckCircle2, 
  ArrowUpRight, 
  ArrowDownRight,
  Zap,
  Radio,
  HardDrive
} from 'lucide-react';
import { ClusterInfo, ClusterMetrics, TopicItem, ConsumerGroup } from '../types/kafka';

interface DashboardViewProps {
  cluster: ClusterInfo | null;
  metrics: ClusterMetrics | null;
  topics: TopicItem[];
  consumerGroups: ConsumerGroup[];
  onProduceBurst: (count: number) => void;
  onNavigateTab: (tab: any) => void;
}

export const DashboardView: React.FC<DashboardViewProps> = ({
  cluster,
  metrics,
  topics,
  consumerGroups,
  onProduceBurst,
  onNavigateTab
}) => {
  const history = metrics?.history || [];
  const maxProducedRate = Math.max(...history.map(p => p.producedRate), 30);
  const maxBytesRate = Math.max(...history.map(p => p.bytesInRate), 40);

  // Formatting helpers
  const formatBytes = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
  };

  const formatUptime = (seconds: number) => {
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    if (mins < 60) return `${mins}m ${secs}s`;
    const hours = Math.floor(mins / 60);
    return `${hours}h ${mins % 60}m`;
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>
      {/* Top Welcome / Header Banner */}
      <div className="glass-panel glass-panel-glow" style={{ padding: '24px 28px', position: 'relative', overflow: 'hidden' }}>
        <div style={{
          position: 'absolute',
          top: -40,
          right: -40,
          width: 250,
          height: 250,
          borderRadius: '50%',
          background: 'radial-gradient(circle, rgba(6, 182, 212, 0.15) 0%, transparent 70%)',
          pointerEvents: 'none'
        }} />

        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 20 }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 6 }}>
              <h1 style={{ fontSize: '1.6rem', fontWeight: 800, letterSpacing: '-0.02em' }}>
                Dashboard de Observabilidad &amp; KPIs
              </h1>
              <span className="badge badge-emerald">
                <Radio size={12} className="status-dot-emerald" /> En Vivo
              </span>
            </div>
            <p style={{ color: 'var(--text-secondary)', fontSize: '0.9rem', maxWidth: 700 }}>
              Monitoreo en tiempo real del clúster AuraKafka: motor de almacenamiento append-only, quórum KRaft, throughput de eventos y latencia de consumidores.
            </p>
          </div>

          {/* Quick Actions */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <button 
              className="btn btn-secondary" 
              onClick={() => onProduceBurst(10)}
              style={{ display: 'flex', alignItems: 'center', gap: 8 }}
            >
              <Zap size={15} color="#06b6d4" />
              <span>Generar Ráfaga (10 msgs)</span>
            </button>
            <button 
              className="btn btn-primary" 
              onClick={() => onNavigateTab('topics')}
            >
              <Layers size={15} />
              <span>Gestionar Tópicos</span>
            </button>
          </div>
        </div>
      </div>

      {/* Primary KPI Cards Grid */}
      <div style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))',
        gap: 16
      }}>
        {/* KPI 1: Ingestion Throughput */}
        <div className="glass-panel" style={{ padding: '20px 22px' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
            <span style={{ fontSize: '0.78rem', textTransform: 'uppercase', letterSpacing: '0.05em', color: 'var(--text-muted)', fontWeight: 600 }}>
              Ingestión (Produced)
            </span>
            <div style={{ padding: 6, borderRadius: 8, background: 'rgba(6, 182, 212, 0.1)', color: '#06b6d4' }}>
              <ArrowUpRight size={18} />
            </div>
          </div>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
            <span style={{ fontSize: '1.9rem', fontWeight: 800, color: 'var(--text-primary)', fontFamily: 'var(--font-mono)' }}>
              {metrics?.totalMessagesProduced ?? 0}
            </span>
            <span style={{ fontSize: '0.85rem', color: '#06b6d4', fontWeight: 600 }}>
              {metrics?.currentThroughputMsgsPerSec ?? 0} msg/s
            </span>
          </div>
          <div style={{ marginTop: 12, display: 'flex', alignItems: 'center', justifyContent: 'space-between', fontSize: '0.75rem', color: 'var(--text-secondary)' }}>
            <span>Tasa de Ingestión:</span>
            <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-primary)', fontWeight: 600 }}>
              {formatBytes(metrics?.currentThroughputBytesPerSec ?? 0)}/s
            </span>
          </div>
        </div>

        {/* KPI 2: Consumption Rate */}
        <div className="glass-panel" style={{ padding: '20px 22px' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
            <span style={{ fontSize: '0.78rem', textTransform: 'uppercase', letterSpacing: '0.05em', color: 'var(--text-muted)', fontWeight: 600 }}>
              Consumo Procesado
            </span>
            <div style={{ padding: 6, borderRadius: 8, background: 'rgba(16, 185, 129, 0.1)', color: '#10b981' }}>
              <ArrowDownRight size={18} />
            </div>
          </div>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
            <span style={{ fontSize: '1.9rem', fontWeight: 800, color: 'var(--text-primary)', fontFamily: 'var(--font-mono)' }}>
              {metrics?.totalMessagesConsumed ?? 0}
            </span>
            <span style={{ fontSize: '0.85rem', color: '#10b981', fontWeight: 600 }}>
              {Math.max(0, (metrics?.currentThroughputMsgsPerSec ?? 0) - 2)} msg/s
            </span>
          </div>
          <div style={{ marginTop: 12, display: 'flex', alignItems: 'center', justifyContent: 'space-between', fontSize: '0.75rem', color: 'var(--text-secondary)' }}>
            <span>Grupos de Consumo:</span>
            <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-primary)', fontWeight: 600 }}>
              {consumerGroups.length} activos
            </span>
          </div>
        </div>

        {/* KPI 3: Consumer Lag */}
        <div className="glass-panel" style={{ padding: '20px 22px' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
            <span style={{ fontSize: '0.78rem', textTransform: 'uppercase', letterSpacing: '0.05em', color: 'var(--text-muted)', fontWeight: 600 }}>
              Consumer Lag Total
            </span>
            <div style={{ padding: 6, borderRadius: 8, background: 'rgba(245, 158, 11, 0.1)', color: '#f59e0b' }}>
              <AlertTriangle size={18} />
            </div>
          </div>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
            <span style={{ fontSize: '1.9rem', fontWeight: 800, color: (metrics?.totalConsumerLag || 0) > 10 ? '#f59e0b' : '#34d399', fontFamily: 'var(--font-mono)' }}>
              {metrics?.totalConsumerLag ?? 0}
            </span>
            <span style={{ fontSize: '0.85rem', color: 'var(--text-muted)' }}>mensajes pendientes</span>
          </div>
          <div style={{ marginTop: 12, display: 'flex', alignItems: 'center', justifyContent: 'space-between', fontSize: '0.75rem', color: 'var(--text-secondary)' }}>
            <span>Estado de Grupos:</span>
            <span className="badge badge-emerald" style={{ fontSize: '0.65rem' }}>STABLE</span>
          </div>
        </div>

        {/* KPI 4: Active Topics & Partitions */}
        <div className="glass-panel" style={{ padding: '20px 22px' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
            <span style={{ fontSize: '0.78rem', textTransform: 'uppercase', letterSpacing: '0.05em', color: 'var(--text-muted)', fontWeight: 600 }}>
              Tópicos &amp; Particiones
            </span>
            <div style={{ padding: 6, borderRadius: 8, background: 'rgba(139, 92, 246, 0.1)', color: '#8b5cf6' }}>
              <Layers size={18} />
            </div>
          </div>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
            <span style={{ fontSize: '1.9rem', fontWeight: 800, color: 'var(--text-primary)', fontFamily: 'var(--font-mono)' }}>
              {metrics?.activeTopicsCount ?? 0}
            </span>
            <span style={{ fontSize: '0.85rem', color: '#a78bfa', fontWeight: 600 }}>
              ({metrics?.totalPartitionsCount ?? 0} particiones)
            </span>
          </div>
          <div style={{ marginTop: 12, display: 'flex', alignItems: 'center', justifyContent: 'space-between', fontSize: '0.75rem', color: 'var(--text-secondary)' }}>
            <span>Tópicos Internos:</span>
            <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-primary)' }}>1 (__consumer_offsets)</span>
          </div>
        </div>

        {/* KPI 5: Storage Log Segments */}
        <div className="glass-panel" style={{ padding: '20px 22px' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
            <span style={{ fontSize: '0.78rem', textTransform: 'uppercase', letterSpacing: '0.05em', color: 'var(--text-muted)', fontWeight: 600 }}>
              Almacenamiento en Disco
            </span>
            <div style={{ padding: 6, borderRadius: 8, background: 'rgba(59, 130, 246, 0.1)', color: '#3b82f6' }}>
              <HardDrive size={18} />
            </div>
          </div>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
            <span style={{ fontSize: '1.9rem', fontWeight: 800, color: 'var(--text-primary)', fontFamily: 'var(--font-mono)' }}>
              {formatBytes(cluster?.storage.totalDiskBytes ?? 0)}
            </span>
            <span style={{ fontSize: '0.85rem', color: 'var(--text-muted)' }}>
              {cluster?.storage.totalLogSegments ?? 0} segmentos
            </span>
          </div>
          <div style={{ marginTop: 12, display: 'flex', alignItems: 'center', justifyContent: 'space-between', fontSize: '0.75rem', color: 'var(--text-secondary)' }}>
            <span>Segmento Máximo:</span>
            <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-primary)' }}>
              {formatBytes(cluster?.storage.segmentMaxBytes ?? 10485760)}
            </span>
          </div>
        </div>

        {/* KPI 6: KRaft Controller Quorum */}
        <div className="glass-panel" style={{ padding: '20px 22px' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
            <span style={{ fontSize: '0.78rem', textTransform: 'uppercase', letterSpacing: '0.05em', color: 'var(--text-muted)', fontWeight: 600 }}>
              KRaft Quórum &amp; Uptime
            </span>
            <div style={{ padding: 6, borderRadius: 8, background: 'rgba(16, 185, 129, 0.1)', color: '#10b981' }}>
              <CheckCircle2 size={18} />
            </div>
          </div>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
            <span style={{ fontSize: '1.9rem', fontWeight: 800, color: '#34d399', fontFamily: 'var(--font-mono)' }}>
              {cluster?.kraft.role} #{cluster?.kraft.leaderId}
            </span>
            <span style={{ fontSize: '0.85rem', color: 'var(--text-muted)' }}>Epoch {cluster?.kraft.currentEpoch}</span>
          </div>
          <div style={{ marginTop: 12, display: 'flex', alignItems: 'center', justifyContent: 'space-between', fontSize: '0.75rem', color: 'var(--text-secondary)' }}>
            <span>Tiempo Activo (Uptime):</span>
            <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-primary)', fontWeight: 600 }}>
              {formatUptime(cluster?.uptimeSeconds ?? 0)}
            </span>
          </div>
        </div>
      </div>

      {/* Main Charts Row */}
      <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: 20 }}>
        {/* Real-Time Throughput Line/Area Chart */}
        <div className="glass-panel" style={{ padding: '22px 26px' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 20 }}>
            <div>
              <h2 style={{ fontSize: '1.05rem', fontWeight: 700 }}>
                Throughput de Mensajes en Tiempo Real (msgs/seg)
              </h2>
              <p style={{ fontSize: '0.78rem', color: 'var(--text-muted)', marginTop: 2 }}>
                Historial de los últimos 30 segundos (Ingestión de Productores vs Consumo)
              </p>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 16, fontSize: '0.78rem' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <span style={{ width: 10, height: 10, borderRadius: 2, background: '#06b6d4' }} />
                <span style={{ color: 'var(--text-secondary)' }}>Produced</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <span style={{ width: 10, height: 10, borderRadius: 2, background: '#10b981' }} />
                <span style={{ color: 'var(--text-secondary)' }}>Consumed</span>
              </div>
            </div>
          </div>

          {/* SVG Line / Bar Chart */}
          <div style={{ height: 220, width: '100%', position: 'relative' }}>
            <svg viewBox="0 0 600 200" preserveAspectRatio="none" style={{ width: '100%', height: '100%', overflow: 'visible' }}>
              <defs>
                <linearGradient id="cyanGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#06b6d4" stopOpacity="0.4" />
                  <stop offset="100%" stopColor="#06b6d4" stopOpacity="0.0" />
                </linearGradient>
                <linearGradient id="greenGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#10b981" stopOpacity="0.3" />
                  <stop offset="100%" stopColor="#10b981" stopOpacity="0.0" />
                </linearGradient>
              </defs>

              {/* Grid Lines */}
              <line x1="0" y1="50" x2="600" y2="50" stroke="rgba(255,255,255,0.05)" strokeDasharray="4 4" />
              <line x1="0" y1="100" x2="600" y2="100" stroke="rgba(255,255,255,0.05)" strokeDasharray="4 4" />
              <line x1="0" y1="150" x2="600" y2="150" stroke="rgba(255,255,255,0.05)" strokeDasharray="4 4" />

              {/* Area & Path for Produced */}
              {history.length > 1 && (() => {
                const step = 600 / (history.length - 1);
                const points = history.map((pt, i) => {
                  const x = i * step;
                  const y = 180 - (pt.producedRate / maxProducedRate) * 160;
                  return `${x},${y}`;
                });
                const dArea = `M0,190 L${points.join(' L')} L600,190 Z`;
                const dLine = `M${points.join(' L')}`;
                return (
                  <>
                    <path d={dArea} fill="url(#cyanGrad)" />
                    <path d={dLine} fill="none" stroke="#06b6d4" strokeWidth="2.5" strokeLinecap="round" />
                  </>
                );
              })()}

              {/* Path for Consumed */}
              {history.length > 1 && (() => {
                const step = 600 / (history.length - 1);
                const points = history.map((pt, i) => {
                  const x = i * step;
                  const y = 180 - (pt.consumedRate / maxProducedRate) * 160;
                  return `${x},${y}`;
                });
                const dLine = `M${points.join(' L')}`;
                return (
                  <path d={dLine} fill="none" stroke="#10b981" strokeWidth="2" strokeDasharray="3 3" />
                );
              })()}
            </svg>

            {/* X Axis Timestamps preview */}
            <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 8, fontSize: '0.7rem', color: 'var(--text-dim)', fontFamily: 'var(--font-mono)' }}>
              <span>{history[0]?.time || '00:00:00'}</span>
              <span>-15s</span>
              <span>{history[history.length - 1]?.time || '00:00:00'} (Ahora)</span>
            </div>
          </div>
        </div>

        {/* Partition Traffic Distribution Bar View */}
        <div className="glass-panel" style={{ padding: '22px 24px' }}>
          <h2 style={{ fontSize: '1.05rem', fontWeight: 700, marginBottom: 4 }}>
            Distribución por Tópico
          </h2>
          <p style={{ fontSize: '0.78rem', color: 'var(--text-muted)', marginBottom: 20 }}>
            Volumen de registros almacenados por partición
          </p>

          <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
            {topics.filter(t => !t.isInternal).map(topic => {
              const maxTopicRecords = Math.max(...topics.map(t => t.totalRecords), 50);
              const pct = Math.min(100, Math.round((topic.totalRecords / maxTopicRecords) * 100));

              return (
                <div key={topic.name}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.82rem', marginBottom: 6 }}>
                    <span style={{ fontWeight: 600, color: 'var(--text-primary)' }}>{topic.name}</span>
                    <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--accent-cyan)' }}>
                      {topic.totalRecords} msgs
                    </span>
                  </div>
                  <div style={{ height: 7, borderRadius: 4, background: 'rgba(255,255,255,0.06)', overflow: 'hidden' }}>
                    <div style={{
                      width: `${pct}%`,
                      height: '100%',
                      background: 'linear-gradient(90deg, #06b6d4, #8b5cf6)',
                      borderRadius: 4,
                      transition: 'width 0.4s ease'
                    }} />
                  </div>
                  <div style={{ display: 'flex', gap: 8, marginTop: 4, fontSize: '0.68rem', color: 'var(--text-muted)' }}>
                    <span>{topic.partitions.length} particiones</span>
                    <span>&bull;</span>
                    <span>{formatBytes(topic.totalSizeBytes)}</span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/* Consumer Groups & Lag Overview Table */}
      <div className="glass-panel" style={{ padding: '22px 24px' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
          <div>
            <h2 style={{ fontSize: '1.05rem', fontWeight: 700 }}>
              Estado de Grupos de Consumidores (Consumer Lag)
            </h2>
            <p style={{ fontSize: '0.78rem', color: 'var(--text-muted)', marginTop: 2 }}>
              Monitoreo del desfasaje entre el Log End Offset (LEO) y el Committed Offset por partición
            </p>
          </div>
          <button 
            className="btn btn-secondary" 
            style={{ fontSize: '0.78rem' }}
            onClick={() => onNavigateTab('consumer')}
          >
            Ver Detalle en Consola
          </button>
        </div>

        <div className="data-table-container">
          <table className="data-table">
            <thead>
              <tr>
                <th>Consumer Group</th>
                <th>Estado</th>
                <th>Miembros</th>
                <th>Tópico Asignado</th>
                <th>Committed Offset</th>
                <th>Log End Offset</th>
                <th>Lag de Mensajes</th>
              </tr>
            </thead>
            <tbody>
              {consumerGroups.map(group => {
                return group.offsets.map((off, oIdx) => (
                  <tr key={`${group.groupId}-${off.topic}-${off.partition}`}>
                    {oIdx === 0 && (
                      <td rowSpan={group.offsets.length} style={{ fontWeight: 600, color: 'var(--text-primary)' }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                          <span className="status-dot status-dot-emerald" />
                          <span>{group.groupId}</span>
                        </div>
                      </td>
                    )}
                    {oIdx === 0 && (
                      <td rowSpan={group.offsets.length}>
                        <span className="badge badge-emerald">{group.state}</span>
                      </td>
                    )}
                    {oIdx === 0 && (
                      <td rowSpan={group.offsets.length} style={{ color: 'var(--text-secondary)', fontSize: '0.8rem' }}>
                        {group.members.length} worker (protocol: {group.protocol})
                      </td>
                    )}
                    <td style={{ fontFamily: 'var(--font-mono)', color: '#38bdf8' }}>
                      {off.topic} [P:{off.partition}]
                    </td>
                    <td style={{ fontFamily: 'var(--font-mono)' }}>{off.committedOffset}</td>
                    <td style={{ fontFamily: 'var(--font-mono)' }}>{off.logEndOffset}</td>
                    <td>
                      <span className={`badge ${off.lag > 5 ? 'badge-amber' : 'badge-cyan'}`}>
                        {off.lag} msgs
                      </span>
                    </td>
                  </tr>
                ));
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};
