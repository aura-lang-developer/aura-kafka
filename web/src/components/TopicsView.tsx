import React, { useState } from 'react';
import { 
  Layers, 
  Plus, 
  Trash2, 
  Search, 
  Eye, 
  AlertCircle, 
  HardDrive, 
  Check, 
  X,
  FileText
} from 'lucide-react';
import { TopicItem, KafkaRecord } from '../types/kafka';
import { createTopic, deleteTopic, fetchTopicRecords } from '../services/api';

interface TopicsViewProps {
  topics: TopicItem[];
  onRefresh: () => void;
  onNavigateProduce: (topicName: string) => void;
}

export const TopicsView: React.FC<TopicsViewProps> = ({
  topics,
  onRefresh,
  onNavigateProduce
}) => {
  const [search, setSearch] = useState('');
  const [isCreateOpen, setIsCreateOpen] = useState(false);
  const [selectedTopic, setSelectedTopic] = useState<TopicItem | null>(null);
  const [topicRecords, setTopicRecords] = useState<KafkaRecord[]>([]);
  const [loadingRecords, setLoadingRecords] = useState(false);
  const [selectedPartition, setSelectedPartition] = useState<number | undefined>(undefined);

  // New Topic Form State
  const [newTopicName, setNewTopicName] = useState('');
  const [newTopicPartitions, setNewTopicPartitions] = useState(3);
  const [newTopicRF, setNewTopicRF] = useState(1);
  const [newTopicPolicy, setNewTopicPolicy] = useState('delete');
  const [creating, setCreating] = useState(false);
  const [errorMessage, setErrorMessage] = useState('');

  const filteredTopics = topics.filter(t => 
    t.name.toLowerCase().includes(search.toLowerCase())
  );

  const formatBytes = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
  };

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault();
    setErrorMessage('');
    if (!newTopicName.trim()) {
      setErrorMessage('El nombre del tópico es obligatorio');
      return;
    }

    try {
      setCreating(true);
      await createTopic({
        name: newTopicName.trim(),
        partitions: newTopicPartitions,
        replicationFactor: newTopicRF,
        cleanupPolicy: newTopicPolicy
      });
      setIsCreateOpen(false);
      setNewTopicName('');
      onRefresh();
    } catch (err: any) {
      setErrorMessage(err.message || 'Error al crear tópico');
    } finally {
      setCreating(false);
    }
  };

  const handleDelete = async (topicName: string) => {
    if (!confirm(`¿Estás seguro de que deseas eliminar el tópico '${topicName}'? Se eliminarán todas las particiones y segmentos.`)) {
      return;
    }
    try {
      await deleteTopic(topicName);
      if (selectedTopic?.name === topicName) {
        setSelectedTopic(null);
      }
      onRefresh();
    } catch (err: any) {
      alert(err.message || 'Error al eliminar el tópico');
    }
  };

  const handleInspectTopic = async (topic: TopicItem) => {
    setSelectedTopic(topic);
    setSelectedPartition(undefined);
    setLoadingRecords(true);
    try {
      const res = await fetchTopicRecords(topic.name, undefined, 30);
      setTopicRecords(res.records);
    } catch (e) {
      console.error(e);
    } finally {
      setLoadingRecords(false);
    }
  };

  const handleFilterPartition = async (p?: number) => {
    if (!selectedTopic) return;
    setSelectedPartition(p);
    setLoadingRecords(true);
    try {
      const res = await fetchTopicRecords(selectedTopic.name, p, 30);
      setTopicRecords(res.records);
    } catch (e) {
      console.error(e);
    } finally {
      setLoadingRecords(false);
    }
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      {/* Header & Actions */}
      <div className="glass-panel" style={{ padding: '20px 24px', display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 16 }}>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <h2 style={{ fontSize: '1.25rem', fontWeight: 800 }}>
              Catálogo de Tópicos &amp; Particiones
            </h2>
            <span className="badge badge-violet" style={{ fontSize: '0.65rem' }}>
              {topics.length} Tópicos
            </span>
          </div>
          <p style={{ fontSize: '0.82rem', color: 'var(--text-muted)', marginTop: 4 }}>
            Administración de esquemas de partición, High Watermark, réplicas ISR y retención en disco
          </p>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          {/* Search Input */}
          <div style={{ position: 'relative', width: 240 }}>
            <Search size={15} style={{ position: 'absolute', left: 10, top: '50%', transform: 'translateY(-50%)', color: 'var(--text-muted)' }} />
            <input
              type="text"
              placeholder="Buscar tópico..."
              value={search}
              onChange={e => setSearch(e.target.value)}
              className="input-text"
              style={{ paddingLeft: 32 }}
            />
          </div>

          <button
            onClick={() => setIsCreateOpen(true)}
            className="btn btn-primary"
            style={{ fontSize: '0.82rem' }}
          >
            <Plus size={16} />
            <span>Crear Tópico</span>
          </button>
        </div>
      </div>

      {/* Topics Table */}
      <div className="glass-panel" style={{ padding: '20px 24px' }}>
        <div className="data-table-container">
          <table className="data-table">
            <thead>
              <tr>
                <th>Nombre del Tópico</th>
                <th>Particiones</th>
                <th>Factor Réplica</th>
                <th>Mensajes</th>
                <th>Tamaño en Disco</th>
                <th>Política Cleanup</th>
                <th>Acciones</th>
              </tr>
            </thead>
            <tbody>
              {filteredTopics.map(topic => (
                <tr key={topic.name}>
                  <td>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                      <Layers size={16} color={topic.isInternal ? '#f59e0b' : '#38bdf8'} />
                      <span style={{ fontWeight: 600, color: 'var(--text-primary)' }}>{topic.name}</span>
                      {topic.isInternal && <span className="badge badge-amber" style={{ fontSize: '0.6rem' }}>INTERNO</span>}
                    </div>
                  </td>
                  <td>
                    <span className="badge badge-cyan" style={{ fontFamily: 'var(--font-mono)' }}>
                      {topic.partitions.length} P
                    </span>
                  </td>
                  <td style={{ fontFamily: 'var(--font-mono)' }}>{topic.replicationFactor}x</td>
                  <td style={{ fontFamily: 'var(--font-mono)', fontWeight: 600, color: 'var(--accent-cyan)' }}>
                    {topic.totalRecords}
                  </td>
                  <td style={{ fontFamily: 'var(--font-mono)' }}>{formatBytes(topic.totalSizeBytes)}</td>
                  <td>
                    <span className="badge badge-gray" style={{ fontSize: '0.65rem' }}>{topic.cleanupPolicy}</span>
                  </td>
                  <td>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                      <button
                        onClick={() => handleInspectTopic(topic)}
                        className="btn btn-secondary"
                        style={{ fontSize: '0.75rem', padding: '5px 10px' }}
                        title="Ver Particiones y Registros"
                      >
                        <Eye size={13} />
                        <span>Detalles</span>
                      </button>

                      {!topic.isInternal && (
                        <>
                          <button
                            onClick={() => onNavigateProduce(topic.name)}
                            className="btn btn-secondary"
                            style={{ fontSize: '0.75rem', padding: '5px 10px', color: '#38bdf8' }}
                            title="Producir Mensaje"
                          >
                            Producir
                          </button>
                          <button
                            onClick={() => handleDelete(topic.name)}
                            className="btn btn-danger"
                            style={{ fontSize: '0.75rem', padding: '5px 8px' }}
                            title="Eliminar Tópico"
                          >
                            <Trash2 size={13} />
                          </button>
                        </>
                      )}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Topic Details Drawer / Section */}
      {selectedTopic && (
        <div className="glass-panel" style={{ padding: '24px 28px', borderLeft: '4px solid var(--accent-violet)' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 20 }}>
            <div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                <h3 style={{ fontSize: '1.2rem', fontWeight: 800 }}>
                  Detalles del Tópico: <span style={{ color: '#c084fc' }}>{selectedTopic.name}</span>
                </h3>
                <span className="badge badge-violet">{selectedTopic.partitions.length} Particiones</span>
              </div>
              <p style={{ fontSize: '0.78rem', color: 'var(--text-muted)', marginTop: 4 }}>
                Inspección de réplicas en sincronía (ISR), High Watermark y visualizador de registros append-only.
              </p>
            </div>

            <button
              onClick={() => setSelectedTopic(null)}
              className="btn btn-secondary"
              style={{ fontSize: '0.78rem', padding: '5px 10px' }}
            >
              <X size={15} />
              <span>Cerrar</span>
            </button>
          </div>

          {/* Partitions Grid */}
          <div style={{ marginBottom: 24 }}>
            <h4 style={{ fontSize: '0.85rem', fontWeight: 700, textTransform: 'uppercase', color: 'var(--text-secondary)', marginBottom: 12 }}>
              Particiones Activas
            </h4>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))', gap: 12 }}>
              {selectedTopic.partitions.map(p => (
                <div
                  key={p.partition}
                  onClick={() => handleFilterPartition(selectedPartition === p.partition ? undefined : p.partition)}
                  style={{
                    background: selectedPartition === p.partition ? 'rgba(139, 92, 246, 0.15)' : 'rgba(0,0,0,0.3)',
                    border: '1px solid',
                    borderColor: selectedPartition === p.partition ? 'var(--accent-violet)' : 'var(--border-subtle)',
                    borderRadius: 8,
                    padding: '14px',
                    cursor: 'pointer',
                    transition: 'all 0.2s'
                  }}
                >
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                    <span style={{ fontWeight: 700, fontSize: '0.88rem' }}>Partición #{p.partition}</span>
                    <span className="badge badge-emerald" style={{ fontSize: '0.62rem' }}>Leader #{p.leader}</span>
                  </div>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 4, fontSize: '0.74rem', fontFamily: 'var(--font-mono)' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', color: 'var(--text-secondary)' }}>
                      <span>High Watermark:</span>
                      <span style={{ color: '#06b6d4' }}>{p.highWatermark}</span>
                    </div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', color: 'var(--text-secondary)' }}>
                      <span>Log End Offset (LEO):</span>
                      <span style={{ color: '#10b981' }}>{p.logEndOffset}</span>
                    </div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', color: 'var(--text-secondary)' }}>
                      <span>ISR Réplicas:</span>
                      <span>[{p.isr.join(', ')}]</span>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Records Browser */}
          <div>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                <h4 style={{ fontSize: '0.85rem', fontWeight: 700, textTransform: 'uppercase', color: 'var(--text-secondary)' }}>
                  Registros en el Log {selectedPartition !== undefined ? `(Partición ${selectedPartition})` : '(Todas las Particiones)'}
                </h4>
                {selectedPartition !== undefined && (
                  <button
                    onClick={() => handleFilterPartition(undefined)}
                    className="btn btn-secondary"
                    style={{ fontSize: '0.7rem', padding: '2px 8px' }}
                  >
                    Ver todas
                  </button>
                )}
              </div>
              <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>
                {topicRecords.length} registros cargados
              </span>
            </div>

            {loadingRecords ? (
              <div style={{ textAlign: 'center', padding: '30px', color: 'var(--text-muted)' }}>
                Cargando registros...
              </div>
            ) : topicRecords.length === 0 ? (
              <div style={{ textAlign: 'center', padding: '30px', color: 'var(--text-muted)', background: 'rgba(0,0,0,0.2)', borderRadius: 8 }}>
                No hay mensajes aún en este tópico/partición.
              </div>
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8, maxHeight: 360, overflowY: 'auto' }}>
                {topicRecords.map((r, idx) => (
                  <div
                    key={`${r.partition}-${r.offset}-${idx}`}
                    style={{
                      background: 'rgba(13, 18, 29, 0.7)',
                      border: '1px solid var(--border-subtle)',
                      borderRadius: 8,
                      padding: '12px 16px',
                      display: 'flex',
                      alignItems: 'flex-start',
                      justifyContent: 'space-between',
                      gap: 16
                    }}
                  >
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 4, flex: 1 }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                        <span className="badge badge-cyan" style={{ fontSize: '0.65rem', fontFamily: 'var(--font-mono)' }}>
                          P:{r.partition} | Off:{r.offset}
                        </span>
                        {r.key && (
                          <span style={{ fontSize: '0.75rem', fontFamily: 'var(--font-mono)', color: 'var(--accent-amber)' }}>
                            Key: {r.key}
                          </span>
                        )}
                        <span style={{ fontSize: '0.7rem', color: 'var(--text-dim)' }}>
                          {new Date(r.timestamp).toLocaleTimeString()}
                        </span>
                      </div>
                      <pre style={{
                        marginTop: 4,
                        fontFamily: 'var(--font-mono)',
                        fontSize: '0.78rem',
                        color: 'var(--text-primary)',
                        background: 'rgba(0,0,0,0.3)',
                        padding: '8px 12px',
                        borderRadius: 6,
                        overflowX: 'auto',
                        whiteSpace: 'pre-wrap',
                        wordBreak: 'break-all'
                      }}>
                        {r.value}
                      </pre>
                    </div>

                    <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>
                      {r.sizeBytes} B
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      )}

      {/* Modal: Create Topic */}
      {isCreateOpen && (
        <div className="modal-overlay">
          <div className="modal-box">
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 20 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                <Plus size={20} color="#06b6d4" />
                <h3 style={{ fontSize: '1.2rem', fontWeight: 800 }}>Crear Nuevo Tópico Kafka</h3>
              </div>
              <button onClick={() => setIsCreateOpen(false)} className="btn-icon">
                <X size={18} />
              </button>
            </div>

            {errorMessage && (
              <div style={{ background: 'rgba(244, 63, 94, 0.15)', border: '1px solid rgba(244, 63, 94, 0.3)', padding: '10px 14px', borderRadius: 8, color: '#fca5a5', fontSize: '0.8rem', marginBottom: 16 }}>
                {errorMessage}
              </div>
            )}

            <form onSubmit={handleCreate} style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
              <div>
                <label style={{ display: 'block', fontSize: '0.8rem', fontWeight: 600, marginBottom: 6 }}>
                  Nombre del Tópico *
                </label>
                <input
                  type="text"
                  placeholder="ej: orders-stream, payments, audit-log"
                  value={newTopicName}
                  onChange={e => setNewTopicName(e.target.value)}
                  className="input-text input-mono"
                  required
                />
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 14 }}>
                <div>
                  <label style={{ display: 'block', fontSize: '0.8rem', fontWeight: 600, marginBottom: 6 }}>
                    Número de Particiones
                  </label>
                  <input
                    type="number"
                    min="1"
                    max="12"
                    value={newTopicPartitions}
                    onChange={e => setNewTopicPartitions(parseInt(e.target.value, 10))}
                    className="input-text input-mono"
                  />
                </div>
                <div>
                  <label style={{ display: 'block', fontSize: '0.8rem', fontWeight: 600, marginBottom: 6 }}>
                    Factor de Replicación
                  </label>
                  <input
                    type="number"
                    min="1"
                    max="3"
                    value={newTopicRF}
                    onChange={e => setNewTopicRF(parseInt(e.target.value, 10))}
                    className="input-text input-mono"
                  />
                </div>
              </div>

              <div>
                <label style={{ display: 'block', fontSize: '0.8rem', fontWeight: 600, marginBottom: 6 }}>
                  Política de Limpieza (Cleanup Policy)
                </label>
                <select
                  value={newTopicPolicy}
                  onChange={e => setNewTopicPolicy(e.target.value)}
                  className="select-input"
                >
                  <option value="delete">delete (Eliminación por tiempo de retención)</option>
                  <option value="compact">compact (Compactación por clave)</option>
                  <option value="compact,delete">compact,delete (Compactación + Eliminación)</option>
                </select>
              </div>

              <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 10, marginTop: 12 }}>
                <button
                  type="button"
                  onClick={() => setIsCreateOpen(false)}
                  className="btn btn-secondary"
                >
                  Cancelar
                </button>
                <button
                  type="submit"
                  disabled={creating}
                  className="btn btn-primary"
                >
                  {creating ? 'Creando...' : 'Crear Tópico'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
};
