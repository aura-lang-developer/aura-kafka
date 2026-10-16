import React from 'react';
import { 
  Activity, 
  Layers, 
  Send, 
  DownloadCloud, 
  Terminal, 
  Cpu, 
  GitBranch, 
  Play, 
  Pause, 
  RefreshCw,
  Server
} from 'lucide-react';
import { ClusterInfo } from '../types/kafka';

export type NavTab = 'dashboard' | 'flow' | 'topics' | 'producer' | 'consumer' | 'logs' | 'cluster';

interface NavbarProps {
  currentTab: NavTab;
  onTabChange: (tab: NavTab) => void;
  cluster: ClusterInfo | null;
  simulationActive: boolean;
  onToggleSimulation: () => void;
  onRunDemo: () => void;
  demoRunning: boolean;
}

export const Navbar: React.FC<NavbarProps> = ({
  currentTab,
  onTabChange,
  cluster,
  simulationActive,
  onToggleSimulation,
  onRunDemo,
  demoRunning
}) => {
  const tabs: { id: NavTab; label: string; icon: React.ReactNode; badge?: string }[] = [
    { id: 'dashboard', label: 'Dashboard & KPIs', icon: <Activity size={17} /> },
    { id: 'flow', label: 'Flujo de Datos', icon: <GitBranch size={17} />, badge: 'LIVE' },
    { id: 'topics', label: 'Tópicos & Particiones', icon: <Layers size={17} /> },
    { id: 'producer', label: 'Consola Producer', icon: <Send size={17} /> },
    { id: 'consumer', label: 'Consumer & Grupos', icon: <DownloadCloud size={17} /> },
    { id: 'logs', label: 'Logs de Operación', icon: <Terminal size={17} /> },
    { id: 'cluster', label: 'KRaft & Almacenamiento', icon: <Cpu size={17} /> }
  ];

  return (
    <header style={{
      background: 'rgba(11, 15, 25, 0.85)',
      backdropFilter: 'blur(20px)',
      borderBottom: '1px solid var(--border-subtle)',
      position: 'sticky',
      top: 0,
      zIndex: 50
    }}>
      <div style={{
        maxWidth: 1600,
        margin: '0 auto',
        padding: '0 32px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        height: 72
      }}>
        {/* Left: Brand & Status */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 20 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 12, cursor: 'pointer' }} onClick={() => onTabChange('dashboard')}>
            <div style={{
              width: 38,
              height: 38,
              borderRadius: 10,
              background: 'linear-gradient(135deg, #06b6d4, #3b82f6)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              boxShadow: '0 0 20px rgba(6, 182, 212, 0.4)',
              border: '1px solid rgba(255, 255, 255, 0.2)'
            }}>
              <Server size={20} color="#fff" />
            </div>
            <div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <span style={{ fontSize: '1.2rem', fontWeight: 800, letterSpacing: '-0.02em', background: 'linear-gradient(to right, #ffffff, #38bdf8)', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent' }}>
                  AuraKafka
                </span>
                <span className="badge badge-cyan" style={{ fontSize: '0.65rem', padding: '1px 6px' }}>v3.8.0-aura</span>
              </div>
              <p style={{ fontSize: '0.72rem', color: 'var(--text-muted)', margin: 0 }}>Apache Kafka Engine en Lenguaje Aura</p>
            </div>
          </div>

          <div style={{ height: 28, width: 1, background: 'var(--border-subtle)' }} />

          {/* Cluster Status pill */}
          <div style={{
            display: 'flex',
            alignItems: 'center',
            gap: 10,
            background: 'rgba(255, 255, 255, 0.03)',
            padding: '5px 12px',
            borderRadius: 20,
            border: '1px solid var(--border-subtle)'
          }}>
            <span className="status-dot status-dot-emerald" />
            <span style={{ fontSize: '0.75rem', fontWeight: 600, color: '#34d399' }}>
              KRaft {cluster?.kraft.quorumStatus || 'HEALTHY'}
            </span>
            <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>&bull; Broker #{cluster?.brokerId ?? 1}</span>
          </div>
        </div>

        {/* Center: Navigation Tabs */}
        <nav style={{ display: 'flex', alignItems: 'center', gap: 6, overflowX: 'auto', padding: '0 8px' }}>
          {tabs.map(tab => {
            const isActive = currentTab === tab.id;
            return (
              <button
                key={tab.id}
                onClick={() => onTabChange(tab.id)}
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: 8,
                  padding: '8px 12px',
                  borderRadius: 8,
                  border: '1px solid',
                  borderColor: isActive ? 'var(--accent-cyan)' : 'transparent',
                  background: isActive ? 'rgba(6, 182, 212, 0.12)' : 'transparent',
                  color: isActive ? '#38bdf8' : 'var(--text-secondary)',
                  fontSize: '0.82rem',
                  fontWeight: isActive ? 600 : 500,
                  whiteSpace: 'nowrap',
                  flexShrink: 0,
                  cursor: 'pointer',
                  transition: 'all 0.18s ease',
                  outline: 'none'
                }}
              >
                {tab.icon}
                <span>{tab.label}</span>
                {tab.badge && (
                  <span style={{
                    fontSize: '0.62rem',
                    padding: '1px 5px',
                    borderRadius: 4,
                    background: '#10b981',
                    color: '#fff',
                    fontWeight: 700
                  }}>
                    {tab.badge}
                  </span>
                )}
              </button>
            );
          })}
        </nav>

        {/* Right: Quick Actions */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          {/* Traffic Simulation toggle */}
          <button
            onClick={onToggleSimulation}
            className="btn btn-secondary"
            title={simulationActive ? 'Pausar simulación de tráfico continuo' : 'Reanudar simulación de tráfico continuo'}
            style={{ fontSize: '0.8rem', padding: '7px 12px' }}
          >
            {simulationActive ? <Pause size={14} color="#f59e0b" /> : <Play size={14} color="#10b981" />}
            <span style={{ color: simulationActive ? '#f59e0b' : 'var(--text-secondary)' }}>
              {simulationActive ? 'Simulador ON' : 'Simulador OFF'}
            </span>
          </button>

          {/* Run Demo button */}
          <button
            onClick={onRunDemo}
            disabled={demoRunning}
            className="btn btn-primary"
            style={{ fontSize: '0.8rem', padding: '7px 14px' }}
          >
            <RefreshCw size={14} className={demoRunning ? 'float-slow' : ''} />
            <span>{demoRunning ? 'Ejecutando...' : 'Demo Pub/Sub'}</span>
          </button>
        </div>
      </div>
    </header>
  );
};
