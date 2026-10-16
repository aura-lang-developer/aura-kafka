import React, { useState, useEffect } from 'react';
import { 
  DownloadCloud, 
  Users, 
  Layers, 
  Play, 
  Pause, 
  RefreshCw, 
  AlertCircle, 
  CheckCircle2, 
  Filter, 
  FileJson,
  Hash
} from 'lucide-react';
import { TopicItem, ConsumerGroup, KafkaRecord } from '../types/kafka';
import { fetchTopicRecords } from '../services/api';

interface ConsumerViewProps {
  topics: TopicItem[];
  consumerGroups: ConsumerGroup[];
  onRefreshGroups: () => void;
}

export const ConsumerView: React.FC<ConsumerViewProps> = ({
  topics,
  consumerGroups,
  onRefreshGroups
}) => {
  const [activeTab, setActiveTab] = useState<'groups' | 'stream'>('groups');
  const nonInternalTopics = topics.filter(t => !t.isInternal);

  // Live Stream Poller State
  const [streamTopic, setStreamTopic] = useState(nonInternalTopics[0]?.name || 'orders-stream');
  const [streamPartition, setStreamPartition] = useState<string>('all');
  const [isPolling, setIsPolling] = useState(true);
  const [streamRecords, setStreamRecords] = useState<KafkaRecord[]>([]);
  const [selectedRecord, setSelectedRecord] = useState<KafkaRecord | null>(null);

  // Poll records periodically when activeTab is stream
  useEffect(() => {
    let timer: any = null;
    const load = async () => {
      try {
        const part = streamPartition === 'all' ? undefined : parseInt(streamPartition, 10);
        const res = await fetchTopicRecords(streamTopic, part, 40);
        setStreamRecords(res.records);
      } catch (e) {}
    };

    if (activeTab === 'stream' && isPolling) {
      load();
      timer = setInterval(load, 2000);
    }

    return () => {
      if (timer) clearInterval(timer);
    };
  }, [activeTab, streamTopic, streamPartition, isPolling]);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      {/* Header & Sub-tabs */}
      <div className="glass-panel" style={{ padding: '20px 24px', display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 16 }}>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <DownloadCloud size={20} color="#10b981" />
            <h2 style={{ fontSize: '1.25rem', fontWeight: 800 }}>Consola de Consumidores (KafkaConsumer)</h2>
            <span className="badge badge-emerald">GroupCoordinator</span>
          </div>
          <p style={{ fontSize: '0.82rem', color: 'var(--text-muted)', marginTop: 4 }}>
            Inspección de grupos de consumidores, cálculo de Consumer Lag y suscripción en vivo a particiones.
          </p>
        </div>

        {/* Sub-tab Switcher */}
        <div style={{ display: 'flex', background: 'rgba(0,0,0,0.3)', padding: 4, borderRadius: 8, border: '1px solid var(--border-subtle)' }}>
          <button
            onClick={() => setActiveTab('groups')}
            style={{
              padding: '6px 14px',
              borderRadius: 6,
              background: activeTab === 'groups' ? 'var(--bg-card-active)' : 'transparent',
              color: activeTab === 'groups' ? '#38bdf8' : 'var(--text-muted)',
              border: 'none',
              fontWeight: 600,
              fontSize: '0.82rem',
              cursor: 'pointer'
            }}
          >
            Grupos de Consumo ({consumerGroups.length})
          </button>
          <button
            onClick={() => setActiveTab('stream')}
            style={{
              padding: '6px 14px',
              borderRadius: 6,
              background: activeTab === 'stream' ? 'var(--bg-card-active)' : 'transparent',
              color: activeTab === 'stream' ? '#10b981' : 'var(--text-muted)',
              border: 'none',
              fontWeight: 600,
              fontSize: '0.82rem',
              cursor: 'pointer'
            }}
          >
            Lector de Stream en Vivo
          </button>
        </div>
      </div>

      {/* VIEW 1: CONSUMER GROUPS */}
      {activeTab === 'groups' && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
          {consumerGroups.map(group => (
            <div key={group.groupId} className="glass-panel" style={{ padding: '22px 24px' }}>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16, flexWrap: 'wrap', gap: 12 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                  <div style={{
                    width: 36,
                    height: 36,
                    borderRadius: 8,
                    background: 'rgba(16, 185, 129, 0.1)',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center'
                  }}>
                    <Users size={18} color="#10b981" />
                  </div>
                  <div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                      <h3 style={{ fontSize: '1.05rem', fontWeight: 700 }}>{group.groupId}</h3>
                      <span className="badge badge-emerald">{group.state}</span>
                      <span className="badge badge-gray" style={{ fontSize: '0.62rem' }}>Protocol: {group.protocol}</span>
                    </div>
                    <p style={{ fontSize: '0.72rem', color: 'var(--text-muted)', marginTop: 2 }}>
                      Líder del Grupo: {group.leaderId} &bull; Protocol Type: {group.protocolType}
                    </p>
                  </div>
                </div>

                <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                  <div style={{ textAlign: 'right' }}>
                    <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)', textTransform: 'uppercase' }}>Lag Total</div>
                    <div style={{
                      fontSize: '1.2rem',
                      fontWeight: 800,
                      fontFamily: 'var(--font-mono)',
                      color: group.totalLag > 0 ? '#f59e0b' : '#34d399'
                    }}>
                      {group.totalLag} mensajes
                    </div>
                  </div>
                </div>
              </div>

              {/* Members List */}
              <div style={{ marginBottom: 16, background: 'rgba(0,0,0,0.25)', padding: '12px 16px', borderRadius: 8 }}>
                <div style={{ fontSize: '0.75rem', fontWeight: 600, color: 'var(--text-secondary)', marginBottom: 8 }}>
                  Miembros Activos del Grupo:
                </div>
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 10 }}>
                  {group.members.map(m => (
                    <div key={m.memberId} style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: '0.75rem', background: 'rgba(255,255,255,0.04)', padding: '6px 10px', borderRadius: 6, fontFamily: 'var(--font-mono)' }}>
                      <span className="status-dot status-dot-emerald" />
                      <span style={{ color: 'var(--text-primary)' }}>{m.clientId}</span>
                      <span style={{ color: 'var(--text-dim)' }}>({m.clientHost})</span>
                    </div>
                  ))}
                </div>
              </div>

              {/* Offsets & Lag Table */}
              <div className="data-table-container">
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>Tópico</th>
                      <th>Partición</th>
                      <th>Committed Offset</th>
                      <th>Log End Offset (LEO)</th>
                      <th>Lag</th>
                      <th>Progreso</th>
                    </tr>
                  </thead>
                  <tbody>
                    {group.offsets.map(off => {
                      const total = Math.max(off.logEndOffset, 1);
                      const pct = Math.min(100, Math.round((off.committedOffset / total) * 100));
                      return (
                        <tr key={`${off.topic}-${off.partition}`}>
                          <td style={{ fontWeight: 600, color: '#38bdf8' }}>{off.topic}</td>
                          <td style={{ fontFamily: 'var(--font-mono)' }}>Partición #{off.partition}</td>
                          <td style={{ fontFamily: 'var(--font-mono)' }}>{off.committedOffset}</td>
                          <td style={{ fontFamily: 'var(--font-mono)' }}>{off.logEndOffset}</td>
                          <td>
                            <span className={`badge ${off.lag > 0 ? 'badge-amber' : 'badge-emerald'}`}>
                              {off.lag} msgs
                            </span>
                          </td>
                          <td style={{ width: 140 }}>
                            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                              <div style={{ flex: 1, height: 6, borderRadius: 3, background: 'rgba(255,255,255,0.06)', overflow: 'hidden' }}>
                                <div style={{ width: `${pct}%`, height: '100%', background: '#10b981', borderRadius: 3 }} />
                              </div>
                              <span style={{ fontSize: '0.7rem', color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>
                                {pct}%
                              </span>
                            </div>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* VIEW 2: LIVE STREAM READER */}
      {activeTab === 'stream' && (
        <div style={{ display: 'grid', gridTemplateColumns: '1.4fr 1fr', gap: 20 }}>
          {/* Stream List */}
          <div className="glass-panel" style={{ padding: '22px 24px' }}>
            {/* Filters bar */}
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16, flexWrap: 'wrap', gap: 12 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                <select
                  value={streamTopic}
                  onChange={e => setStreamTopic(e.target.value)}
                  className="select-input"
                  style={{ width: 180, fontSize: '0.8rem' }}
                >
                  {nonInternalTopics.map(t => (
                    <option key={t.name} value={t.name}>{t.name}</option>
                  ))}
                </select>

                <select
                  value={streamPartition}
                  onChange={e => setStreamPartition(e.target.value)}
                  className="select-input"
                  style={{ width: 150, fontSize: '0.8rem' }}
                >
                  <option value="all">Todas las Particiones</option>
                  {topics.find(t => t.name === streamTopic)?.partitions.map(p => (
                    <option key={p.partition} value={p.partition}>Partición #{p.partition}</option>
                  ))}
                </select>
              </div>

              <button
                onClick={() => setIsPolling(!isPolling)}
                className={`btn ${isPolling ? 'btn-secondary' : 'btn-emerald'}`}
                style={{ fontSize: '0.78rem' }}
              >
                {isPolling ? <Pause size={14} color="#f59e0b" /> : <Play size={14} />}
                <span>{isPolling ? 'Pausar Polling' : 'Reanudar Polling'}</span>
              </button>
            </div>

            {/* Records List */}
            {streamRecords.length === 0 ? (
              <div style={{ textAlign: 'center', padding: '60px 20px', color: 'var(--text-muted)' }}>
                No se encontraron registros en el tópico '{streamTopic}'.
              </div>
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8, maxHeight: 520, overflowY: 'auto' }}>
                {streamRecords.map(r => (
                  <div
                    key={`${r.partition}-${r.offset}`}
                    onClick={() => setSelectedRecord(r)}
                    style={{
                      background: selectedRecord?.offset === r.offset && selectedRecord?.partition === r.partition ? 'rgba(16, 185, 129, 0.12)' : 'rgba(0,0,0,0.25)',
                      border: '1px solid',
                      borderColor: selectedRecord?.offset === r.offset && selectedRecord?.partition === r.partition ? 'var(--accent-emerald)' : 'var(--border-subtle)',
                      borderRadius: 8,
                      padding: '12px 14px',
                      cursor: 'pointer',
                      transition: 'all 0.15s'
                    }}
                  >
                    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 6 }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                        <span className="badge badge-emerald" style={{ fontSize: '0.62rem', fontFamily: 'var(--font-mono)' }}>
                          P:{r.partition} | Off:{r.offset}
                        </span>
                        {r.key && (
                          <span style={{ fontSize: '0.75rem', fontFamily: 'var(--font-mono)', color: 'var(--accent-amber)' }}>
                            Key: {r.key}
                          </span>
                        )}
                      </div>
                      <span style={{ fontSize: '0.7rem', color: 'var(--text-dim)' }}>
                        {new Date(r.timestamp).toLocaleTimeString()}
                      </span>
                    </div>

                    <pre style={{
                      fontFamily: 'var(--font-mono)',
                      fontSize: '0.78rem',
                      color: 'var(--text-primary)',
                      whiteSpace: 'nowrap',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis'
                    }}>
                      {r.value}
                    </pre>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Record Inspector Drawer */}
          <div className="glass-panel" style={{ padding: '22px 24px' }}>
            <h3 style={{ fontSize: '1rem', fontWeight: 700, marginBottom: 14, display: 'flex', alignItems: 'center', gap: 8 }}>
              <FileJson size={17} color="#10b981" />
              <span>Inspector de Registro</span>
            </h3>

            {selectedRecord ? (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10, fontSize: '0.75rem' }}>
                  <div style={{ background: 'rgba(0,0,0,0.3)', padding: '8px 10px', borderRadius: 6 }}>
                    <div style={{ color: 'var(--text-muted)' }}>TÓPICO</div>
                    <div style={{ fontWeight: 600, color: 'var(--accent-cyan)', fontFamily: 'var(--font-mono)' }}>{selectedRecord.topic}</div>
                  </div>
                  <div style={{ background: 'rgba(0,0,0,0.3)', padding: '8px 10px', borderRadius: 6 }}>
                    <div style={{ color: 'var(--text-muted)' }}>PARTICIÓN / OFFSET</div>
                    <div style={{ fontWeight: 600, fontFamily: 'var(--font-mono)' }}>P:{selectedRecord.partition} | #{selectedRecord.offset}</div>
                  </div>
                  <div style={{ background: 'rgba(0,0,0,0.3)', padding: '8px 10px', borderRadius: 6 }}>
                    <div style={{ color: 'var(--text-muted)' }}>TIMESTAMP</div>
                    <div style={{ fontFamily: 'var(--font-mono)' }}>{new Date(selectedRecord.timestamp).toISOString()}</div>
                  </div>
                  <div style={{ background: 'rgba(0,0,0,0.3)', padding: '8px 10px', borderRadius: 6 }}>
                    <div style={{ color: 'var(--text-muted)' }}>TAMAÑO TOTAL</div>
                    <div style={{ fontFamily: 'var(--font-mono)' }}>{selectedRecord.sizeBytes} Bytes</div>
                  </div>
                </div>

                {/* Headers */}
                {Object.keys(selectedRecord.headers || {}).length > 0 && (
                  <div>
                    <div style={{ fontSize: '0.75rem', fontWeight: 600, color: 'var(--text-secondary)', marginBottom: 6 }}>
                      Headers Kafka:
                    </div>
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                      {Object.entries(selectedRecord.headers).map(([k, v]) => (
                        <span key={k} className="badge badge-gray" style={{ fontSize: '0.68rem', fontFamily: 'var(--font-mono)' }}>
                          {k}: {v}
                        </span>
                      ))}
                    </div>
                  </div>
                )}

                {/* Formatted JSON Payload */}
                <div>
                  <div style={{ fontSize: '0.75rem', fontWeight: 600, color: 'var(--text-secondary)', marginBottom: 6 }}>
                    Payload Decodificado:
                  </div>
                  <pre style={{
                    background: '#07090e',
                    border: '1px solid var(--border-subtle)',
                    borderRadius: 8,
                    padding: '14px',
                    fontFamily: 'var(--font-mono)',
                    fontSize: '0.78rem',
                    color: '#38bdf8',
                    overflowX: 'auto',
                    whiteSpace: 'pre-wrap',
                    wordBreak: 'break-all',
                    maxHeight: 260
                  }}>
                    {(() => {
                      try {
                        return JSON.stringify(JSON.parse(selectedRecord.value), null, 2);
                      } catch (e) {
                        return selectedRecord.value;
                      }
                    })()}
                  </pre>
                </div>
              </div>
            ) : (
              <div style={{ textAlign: 'center', padding: '50px 20px', color: 'var(--text-muted)' }}>
                Selecciona un registro de la lista para inspeccionar sus encabezados, clave y payload decodificado.
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};
