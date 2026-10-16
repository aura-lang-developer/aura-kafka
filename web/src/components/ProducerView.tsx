import React, { useState } from 'react';
import { 
  Send, 
  Zap, 
  CheckCircle, 
  AlertTriangle, 
  Copy, 
  RefreshCw, 
  Layers, 
  Tag, 
  Code 
} from 'lucide-react';
import { TopicItem } from '../types/kafka';
import { produceRecord } from '../services/api';

interface ProducerViewProps {
  topics: TopicItem[];
  preselectedTopic?: string;
}

export const ProducerView: React.FC<ProducerViewProps> = ({
  topics,
  preselectedTopic
}) => {
  const nonInternalTopics = topics.filter(t => !t.isInternal);
  const [selectedTopic, setSelectedTopic] = useState(
    preselectedTopic || nonInternalTopics[0]?.name || 'orders-stream'
  );
  const [key, setKey] = useState('user-101');
  const [partitionSelect, setPartitionSelect] = useState<string>('auto');
  const [headers, setHeaders] = useState<{ key: string; value: string }[]>([
    { key: 'source', value: 'portal-web' },
    { key: 'correlationId', value: 'corr-8831' }
  ]);
  const [payload, setPayload] = useState(JSON.stringify({
    orderId: 'ord-9921',
    customer: 'cliente@cenco.cl',
    amount: 149.99,
    status: 'PENDING',
    timestamp: Date.now()
  }, null, 2));

  const [producing, setProducing] = useState(false);
  const [resultStatus, setResultStatus] = useState<{ success: boolean; msg: string } | null>(null);
  const [history, setHistory] = useState<{
    id: string;
    topic: string;
    partition: number;
    offset: number;
    key: string;
    timestamp: number;
    bytes: number;
  }[]>([]);

  // Presets
  const applyPreset = (type: string) => {
    if (type === 'order') {
      setKey(`cust-${Math.floor(Math.random() * 900 + 100)}`);
      setPayload(JSON.stringify({
        orderId: `ord-${Math.floor(Math.random() * 9000 + 1000)}`,
        customer: 'rojas.mauricio@cenco.cl',
        items: [{ sku: 'SKU-771', qty: 2, price: 49.99 }],
        total: 99.98,
        currency: 'CLP',
        status: 'CONFIRMED',
        timestamp: Date.now()
      }, null, 2));
    } else if (type === 'payment') {
      setKey(`acct-00${Math.floor(Math.random() * 4 + 1)}`);
      setPayload(JSON.stringify({
        transactionId: `tx-${Math.random().toString(36).substring(2, 9)}`,
        account: `acct-00${Math.floor(Math.random() * 4 + 1)}`,
        amount: +(Math.random() * 500 + 10).toFixed(2),
        currency: 'USD',
        channel: 'POS-ONLINE',
        authCode: 'AUTH-99128'
      }, null, 2));
    } else if (type === 'iot') {
      setKey(`sensor-mac-${Math.floor(Math.random() * 80 + 10)}`);
      setPayload(JSON.stringify({
        deviceId: 'iot-temp-sensor-04',
        temperature: +(20 + Math.random() * 8).toFixed(1),
        humidity: +(45 + Math.random() * 15).toFixed(1),
        batteryPct: 94,
        status: 'OPTIMAL'
      }, null, 2));
    }
  };

  const handleSend = async (e: React.FormEvent) => {
    e.preventDefault();
    setResultStatus(null);
    setProducing(true);

    try {
      let parsedValue: any = payload;
      try {
        parsedValue = JSON.parse(payload);
      } catch (err) {}

      const headerObj: Record<string, string> = {};
      headers.forEach(h => {
        if (h.key.trim()) headerObj[h.key.trim()] = h.value;
      });

      const partNum = partitionSelect === 'auto' ? undefined : parseInt(partitionSelect, 10);

      const res = await produceRecord({
        topic: selectedTopic,
        key: key.trim(),
        value: parsedValue,
        partition: partNum,
        headers: headerObj
      });

      setResultStatus({
        success: true,
        msg: `✓ Mensaje publicado en [Partición ${res.partition}] en el Offset ${res.offset}`
      });

      setHistory(prev => [
        {
          id: Math.random().toString(36).substring(2, 8),
          topic: selectedTopic,
          partition: res.partition,
          offset: res.offset,
          key,
          timestamp: res.timestamp,
          bytes: payload.length + key.length + 32
        },
        ...prev
      ].slice(0, 20));
    } catch (err: any) {
      setResultStatus({
        success: false,
        msg: `✕ Error: ${err.message || 'Fallo en KafkaProducer'}`
      });
    } finally {
      setProducing(false);
    }
  };

  const addHeader = () => {
    setHeaders([...headers, { key: '', value: '' }]);
  };

  const removeHeader = (idx: number) => {
    setHeaders(headers.filter((_, i) => i !== idx));
  };

  const currentTopicItem = topics.find(t => t.name === selectedTopic);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      {/* Header */}
      <div className="glass-panel" style={{ padding: '20px 24px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <Send size={20} color="#06b6d4" />
          <h2 style={{ fontSize: '1.25rem', fontWeight: 800 }}>Consola de Publicación (KafkaProducer)</h2>
          <span className="badge badge-cyan">Interactivo</span>
        </div>
        <p style={{ fontSize: '0.82rem', color: 'var(--text-muted)', marginTop: 4 }}>
          Publica mensajes con particionado por Clave (Hash Murmur2) o asignación manual directa al motor de almacenamiento.
        </p>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1.2fr 1fr', gap: 20 }}>
        {/* Form Container */}
        <div className="glass-panel" style={{ padding: '24px 26px' }}>
          {/* Quick Presets */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 18, flexWrap: 'wrap' }}>
            <span style={{ fontSize: '0.75rem', fontWeight: 600, color: 'var(--text-muted)' }}>Plantillas:</span>
            <button type="button" onClick={() => applyPreset('order')} className="btn btn-secondary" style={{ fontSize: '0.72rem', padding: '4px 10px' }}>
              📦 Pedido / E-Commerce
            </button>
            <button type="button" onClick={() => applyPreset('payment')} className="btn btn-secondary" style={{ fontSize: '0.72rem', padding: '4px 10px' }}>
              💳 Pago / Transacción
            </button>
            <button type="button" onClick={() => applyPreset('iot')} className="btn btn-secondary" style={{ fontSize: '0.72rem', padding: '4px 10px' }}>
              📡 Telemetría IoT
            </button>
          </div>

          <form onSubmit={handleSend} style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
            {/* Topic & Partition Selectors */}
            <div style={{ display: 'grid', gridTemplateColumns: '1.4fr 1fr', gap: 12 }}>
              <div>
                <label style={{ display: 'block', fontSize: '0.8rem', fontWeight: 600, marginBottom: 6 }}>
                  Tópico Destino *
                </label>
                <select
                  value={selectedTopic}
                  onChange={e => setSelectedTopic(e.target.value)}
                  className="select-input"
                >
                  {nonInternalTopics.map(t => (
                    <option key={t.name} value={t.name}>
                      {t.name} ({t.partitions.length} particiones)
                    </option>
                  ))}
                </select>
              </div>

              <div>
                <label style={{ display: 'block', fontSize: '0.8rem', fontWeight: 600, marginBottom: 6 }}>
                  Partición
                </label>
                <select
                  value={partitionSelect}
                  onChange={e => setPartitionSelect(e.target.value)}
                  className="select-input"
                >
                  <option value="auto">Auto (Hash por Key)</option>
                  {currentTopicItem?.partitions.map(p => (
                    <option key={p.partition} value={p.partition}>
                      Partición #{p.partition}
                    </option>
                  ))}
                </select>
              </div>
            </div>

            {/* Key input */}
            <div>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
                <label style={{ fontSize: '0.8rem', fontWeight: 600 }}>
                  Clave del Registro (Record Key)
                </label>
                <button
                  type="button"
                  onClick={() => setKey(`uuid-${Math.random().toString(36).substring(2, 9)}`)}
                  style={{ background: 'none', border: 'none', color: 'var(--accent-cyan)', fontSize: '0.72rem', cursor: 'pointer' }}
                >
                  Generar UUID
                </button>
              </div>
              <input
                type="text"
                value={key}
                onChange={e => setKey(e.target.value)}
                className="input-text input-mono"
                placeholder="ej: order-1002, acct-889"
              />
            </div>

            {/* Value Payload */}
            <div>
              <label style={{ display: 'block', fontSize: '0.8rem', fontWeight: 600, marginBottom: 6 }}>
                Contenido del Mensaje (Payload / JSON) *
              </label>
              <textarea
                rows={7}
                value={payload}
                onChange={e => setPayload(e.target.value)}
                className="textarea-input input-mono"
                style={{ fontSize: '0.8rem', lineHeight: '1.4' }}
                required
              />
            </div>

            {/* Headers accordion/list */}
            <div>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
                <label style={{ fontSize: '0.8rem', fontWeight: 600, color: 'var(--text-secondary)' }}>
                  Encabezados Kafka (Headers)
                </label>
                <button
                  type="button"
                  onClick={addHeader}
                  className="btn btn-secondary"
                  style={{ fontSize: '0.7rem', padding: '2px 8px' }}
                >
                  + Header
                </button>
              </div>

              <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                {headers.map((h, idx) => (
                  <div key={idx} style={{ display: 'flex', gap: 8 }}>
                    <input
                      type="text"
                      placeholder="Header Key"
                      value={h.key}
                      onChange={e => {
                        const copy = [...headers];
                        copy[idx].key = e.target.value;
                        setHeaders(copy);
                      }}
                      className="input-text input-mono"
                      style={{ fontSize: '0.75rem', padding: '6px 10px' }}
                    />
                    <input
                      type="text"
                      placeholder="Header Value"
                      value={h.value}
                      onChange={e => {
                        const copy = [...headers];
                        copy[idx].value = e.target.value;
                        setHeaders(copy);
                      }}
                      className="input-text input-mono"
                      style={{ fontSize: '0.75rem', padding: '6px 10px' }}
                    />
                    <button
                      type="button"
                      onClick={() => removeHeader(idx)}
                      className="btn btn-secondary"
                      style={{ padding: '6px 8px', color: '#fda4af' }}
                    >
                      &times;
                    </button>
                  </div>
                ))}
              </div>
            </div>

            {/* Status Message */}
            {resultStatus && (
              <div style={{
                background: resultStatus.success ? 'rgba(16, 185, 129, 0.15)' : 'rgba(244, 63, 94, 0.15)',
                border: '1px solid',
                borderColor: resultStatus.success ? 'rgba(16, 185, 129, 0.3)' : 'rgba(244, 63, 94, 0.3)',
                padding: '10px 14px',
                borderRadius: 8,
                fontSize: '0.8rem',
                color: resultStatus.success ? '#34d399' : '#fca5a5',
                fontFamily: 'var(--font-mono)'
              }}>
                {resultStatus.msg}
              </div>
            )}

            <button
              type="submit"
              disabled={producing}
              className="btn btn-primary"
              style={{ justifyContent: 'center', padding: '11px', marginTop: 8 }}
            >
              <Send size={16} />
              <span>{producing ? 'Publicando al Broker...' : 'Publicar Registro (Send)'}</span>
            </button>
          </form>
        </div>

        {/* Live Published History */}
        <div className="glass-panel" style={{ padding: '24px 26px', display: 'flex', flexDirection: 'column' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
            <h3 style={{ fontSize: '1rem', fontWeight: 700 }}>Historial de Envíos de la Sesión</h3>
            <span style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>{history.length} enviados</span>
          </div>

          {history.length === 0 ? (
            <div style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', color: 'var(--text-muted)', textAlign: 'center', padding: '40px' }}>
              <Layers size={36} style={{ opacity: 0.3, marginBottom: 12 }} />
              <p style={{ fontSize: '0.85rem' }}>Aún no has publicado registros desde esta consola.</p>
              <p style={{ fontSize: '0.75rem', marginTop: 4 }}>Usa el formulario para enviar tu primer mensaje.</p>
            </div>
          ) : (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8, overflowY: 'auto', maxHeight: 480 }}>
              {history.map(item => (
                <div
                  key={item.id}
                  style={{
                    background: 'rgba(0,0,0,0.3)',
                    border: '1px solid var(--border-subtle)',
                    borderRadius: 8,
                    padding: '12px 14px'
                  }}
                >
                  <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                    <span style={{ fontWeight: 600, fontSize: '0.82rem', color: 'var(--accent-cyan)' }}>
                      {item.topic}
                    </span>
                    <span className="badge badge-emerald" style={{ fontSize: '0.62rem', fontFamily: 'var(--font-mono)' }}>
                      P:{item.partition} | Off:{item.offset}
                    </span>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 6, fontSize: '0.72rem', color: 'var(--text-secondary)' }}>
                    <span>Clave: <code style={{ color: '#fbbf24' }}>{item.key || '(none)'}</code></span>
                    <span>{new Date(item.timestamp).toLocaleTimeString()} &bull; {item.bytes} B</span>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};
