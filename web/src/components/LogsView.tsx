import React, { useState, useRef, useEffect } from 'react';
import { 
  Terminal, 
  Trash2, 
  Download, 
  Search, 
  Pause, 
  Play, 
  Filter, 
  ArrowDown, 
  Copy, 
  Check 
} from 'lucide-react';
import { LogEntry, LogLevel, Subsystem } from '../types/kafka';
import { clearLogs } from '../services/api';

interface LogsViewProps {
  logs: LogEntry[];
  onRefresh: () => void;
}

export const LogsView: React.FC<LogsViewProps> = ({ logs, onRefresh }) => {
  const [selectedLevel, setSelectedLevel] = useState<string>('ALL');
  const [selectedSubsystem, setSelectedSubsystem] = useState<string>('ALL');
  const [search, setSearch] = useState('');
  const [autoScroll, setAutoScroll] = useState(true);
  const [isPaused, setIsPaused] = useState(false);
  const [copiedId, setCopiedId] = useState<string | null>(null);
  const terminalEndRef = useRef<HTMLDivElement>(null);

  const levels: (LogLevel | 'ALL')[] = ['ALL', 'INFO', 'WARN', 'ERROR', 'DEBUG'];
  const subsystems: (Subsystem | 'ALL')[] = ['ALL', 'BROKER', 'KRAFT', 'STORAGE', 'COORDINATOR', 'PRODUCER', 'CONSUMER', 'SYSTEM'];

  // Filtered logs
  const filteredLogs = logs.filter(l => {
    if (selectedLevel !== 'ALL' && l.level !== selectedLevel) return false;
    if (selectedSubsystem !== 'ALL' && l.subsystem !== selectedSubsystem) return false;
    if (search.trim()) {
      const q = search.toLowerCase();
      return l.message.toLowerCase().includes(q) || l.subsystem.toLowerCase().includes(q);
    }
    return true;
  });

  // Auto-scroll effect
  useEffect(() => {
    if (autoScroll && !isPaused && terminalEndRef.current) {
      terminalEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [filteredLogs.length, autoScroll, isPaused]);

  const handleClear = async () => {
    if (confirm('¿Deseas vaciar el buffer de logs de operación?')) {
      await clearLogs();
      onRefresh();
    }
  };

  const handleDownload = () => {
    const text = filteredLogs
      .map(l => `[${l.timestamp}] [${l.level}] [${l.subsystem}] ${l.message}`)
      .join('\n');
    const blob = new Blob([text], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `aurakafka-logs-${Date.now()}.txt`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const copyLogLine = (log: LogEntry) => {
    navigator.clipboard.writeText(`[${log.timestamp}] [${log.level}] [${log.subsystem}] ${log.message}`);
    setCopiedId(log.id);
    setTimeout(() => setCopiedId(null), 1500);
  };

  const getLevelBadgeClass = (lvl: LogLevel) => {
    switch (lvl) {
      case 'INFO': return 'badge-cyan';
      case 'WARN': return 'badge-amber';
      case 'ERROR': return 'badge-rose';
      case 'DEBUG': return 'badge-violet';
      default: return 'badge-gray';
    }
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      {/* Header & Controls Toolbar */}
      <div className="glass-panel" style={{ padding: '20px 24px', display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 16 }}>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <Terminal size={20} color="#06b6d4" />
            <h2 style={{ fontSize: '1.25rem', fontWeight: 800 }}>Consola de Logs de Operación</h2>
            <span className="badge badge-cyan">{filteredLogs.length} Entradas</span>
          </div>
          <p style={{ fontSize: '0.82rem', color: 'var(--text-muted)', marginTop: 4 }}>
            Eventos en tiempo real del broker, elecciones KRaft, almacenamiento y transacciones pub/sub
          </p>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
          {/* Pause / Resume */}
          <button
            onClick={() => setIsPaused(!isPaused)}
            className={`btn ${isPaused ? 'btn-secondary' : 'btn-emerald'}`}
            style={{ fontSize: '0.78rem' }}
          >
            {isPaused ? <Play size={13} /> : <Pause size={13} color="#f59e0b" />}
            <span>{isPaused ? 'Reanudar Stream' : 'Pausar'}</span>
          </button>

          {/* Auto Scroll toggle */}
          <button
            onClick={() => setAutoScroll(!autoScroll)}
            className="btn btn-secondary"
            style={{ fontSize: '0.78rem', color: autoScroll ? '#38bdf8' : 'var(--text-muted)' }}
          >
            <ArrowDown size={13} />
            <span>Auto-Scroll: {autoScroll ? 'ON' : 'OFF'}</span>
          </button>

          {/* Export */}
          <button
            onClick={handleDownload}
            className="btn btn-secondary"
            style={{ fontSize: '0.78rem' }}
            title="Descargar Logs en formato .txt"
          >
            <Download size={13} />
            <span>Exportar</span>
          </button>

          {/* Clear */}
          <button
            onClick={handleClear}
            className="btn btn-danger"
            style={{ fontSize: '0.78rem' }}
            title="Vaciar logs"
          >
            <Trash2 size={13} />
            <span>Limpiar</span>
          </button>
        </div>
      </div>

      {/* Filter Bar */}
      <div className="glass-panel" style={{ padding: '14px 20px', display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 14 }}>
        {/* Severity Filters */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>
          <span style={{ fontSize: '0.75rem', fontWeight: 600, color: 'var(--text-muted)', marginRight: 4 }}>Nivel:</span>
          {levels.map(lvl => (
            <button
              key={lvl}
              onClick={() => setSelectedLevel(lvl)}
              style={{
                fontSize: '0.72rem',
                padding: '3px 9px',
                borderRadius: 6,
                border: '1px solid',
                borderColor: selectedLevel === lvl ? 'var(--accent-cyan)' : 'var(--border-subtle)',
                background: selectedLevel === lvl ? 'rgba(6, 182, 212, 0.15)' : 'rgba(0,0,0,0.2)',
                color: selectedLevel === lvl ? '#38bdf8' : 'var(--text-secondary)',
                cursor: 'pointer',
                fontWeight: 600
              }}
            >
              {lvl}
            </button>
          ))}
        </div>

        {/* Subsystem Filters */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>
          <span style={{ fontSize: '0.75rem', fontWeight: 600, color: 'var(--text-muted)', marginRight: 4 }}>Subsistema:</span>
          {subsystems.map(sub => (
            <button
              key={sub}
              onClick={() => setSelectedSubsystem(sub)}
              style={{
                fontSize: '0.72rem',
                padding: '3px 9px',
                borderRadius: 6,
                border: '1px solid',
                borderColor: selectedSubsystem === sub ? 'var(--accent-violet)' : 'var(--border-subtle)',
                background: selectedSubsystem === sub ? 'rgba(139, 92, 246, 0.15)' : 'rgba(0,0,0,0.2)',
                color: selectedSubsystem === sub ? '#c084fc' : 'var(--text-secondary)',
                cursor: 'pointer',
                fontWeight: 600
              }}
            >
              {sub}
            </button>
          ))}
        </div>

        {/* Search */}
        <div style={{ position: 'relative', width: 220 }}>
          <Search size={14} style={{ position: 'absolute', left: 10, top: '50%', transform: 'translateY(-50%)', color: 'var(--text-muted)' }} />
          <input
            type="text"
            placeholder="Filtrar por texto..."
            value={search}
            onChange={e => setSearch(e.target.value)}
            className="input-text"
            style={{ paddingLeft: 30, fontSize: '0.78rem', padding: '6px 10px 6px 30px' }}
          />
        </div>
      </div>

      {/* Terminal Display */}
      <div className="glass-panel" style={{
        background: '#05070c',
        border: '1px solid var(--border-strong)',
        padding: '20px',
        fontFamily: 'var(--font-mono)',
        fontSize: '0.8rem',
        minHeight: 520,
        maxHeight: 650,
        overflowY: 'auto',
        borderRadius: 12,
        boxShadow: 'inset 0 2px 10px rgba(0,0,0,0.8)'
      }}>
        {filteredLogs.length === 0 ? (
          <div style={{ textAlign: 'center', padding: '80px 20px', color: 'var(--text-dim)' }}>
            No hay logs disponibles con los filtros actuales.
          </div>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            {filteredLogs.map(log => (
              <div
                key={log.id}
                style={{
                  display: 'flex',
                  alignItems: 'flex-start',
                  gap: 12,
                  padding: '4px 6px',
                  borderRadius: 4,
                  lineHeight: '1.5',
                  transition: 'background 0.15s'
                }}
                onMouseEnter={e => e.currentTarget.style.background = 'rgba(255,255,255,0.03)'}
                onMouseLeave={e => e.currentTarget.style.background = 'transparent'}
              >
                {/* Timestamp */}
                <span style={{ color: 'var(--text-dim)', fontSize: '0.72rem', minWidth: 80, userSelect: 'none' }}>
                  {new Date(log.timestamp).toLocaleTimeString()}
                </span>

                {/* Level Badge */}
                <span className={`badge ${getLevelBadgeClass(log.level)}`} style={{ fontSize: '0.62rem', minWidth: 54, justifyContent: 'center' }}>
                  {log.level}
                </span>

                {/* Subsystem */}
                <span style={{ color: '#a78bfa', fontSize: '0.72rem', minWidth: 100, fontWeight: 600 }}>
                  [{log.subsystem}]
                </span>

                {/* Message */}
                <span style={{ color: 'var(--text-primary)', flex: 1, wordBreak: 'break-word' }}>
                  {log.message}
                </span>

                {/* Copy button */}
                <button
                  onClick={() => copyLogLine(log)}
                  style={{
                    background: 'none',
                    border: 'none',
                    color: copiedId === log.id ? '#34d399' : 'var(--text-dim)',
                    cursor: 'pointer',
                    padding: '2px 4px'
                  }}
                  title="Copiar línea de log"
                >
                  {copiedId === log.id ? <Check size={13} /> : <Copy size={13} />}
                </button>
              </div>
            ))}
            <div ref={terminalEndRef} />
          </div>
        )}
      </div>
    </div>
  );
};
