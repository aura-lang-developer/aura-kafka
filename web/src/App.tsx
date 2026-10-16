import React, { useState, useEffect, useCallback } from 'react';
import { Navbar, NavTab } from './components/Navbar';
import { DashboardView } from './components/DashboardView';
import { FlowVisualizerView } from './components/FlowVisualizerView';
import { TopicsView } from './components/TopicsView';
import { ProducerView } from './components/ProducerView';
import { ConsumerView } from './components/ConsumerView';
import { LogsView } from './components/LogsView';
import { ClusterView } from './components/ClusterView';

import { 
  ClusterInfo, 
  ClusterMetrics, 
  TopicItem, 
  ConsumerGroup, 
  LogEntry, 
  FlowPacket 
} from './types/kafka';

import { 
  fetchClusterInfo, 
  fetchMetrics, 
  fetchTopics, 
  fetchConsumerGroups, 
  fetchLogs, 
  setSimulation, 
  runDemoPipeline, 
  produceRecord 
} from './services/api';
import { wsClient } from './services/websocket';

export const App: React.FC = () => {
  const [currentTab, setCurrentTab] = useState<NavTab>('dashboard');
  const [cluster, setCluster] = useState<ClusterInfo | null>(null);
  const [metrics, setMetrics] = useState<ClusterMetrics | null>(null);
  const [topics, setTopics] = useState<TopicItem[]>([]);
  const [consumerGroups, setConsumerGroups] = useState<ConsumerGroup[]>([]);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [livePackets, setLivePackets] = useState<FlowPacket[]>([]);
  const [simulationActive, setSimulationActive] = useState(true);
  const [demoRunning, setDemoRunning] = useState(false);
  const [preselectedTopic, setPreselectedTopic] = useState<string>('');

  // Initial Data Fetch
  const loadInitialData = useCallback(async () => {
    try {
      const [cInfo, mInfo, tInfo, gInfo, lInfo] = await Promise.all([
        fetchClusterInfo().catch(() => null),
        fetchMetrics().catch(() => null),
        fetchTopics().catch(() => []),
        fetchConsumerGroups().catch(() => []),
        fetchLogs().catch(() => [])
      ]);

      if (cInfo) setCluster(cInfo);
      if (mInfo) setMetrics(mInfo);
      if (tInfo) setTopics(tInfo);
      if (gInfo) setConsumerGroups(gInfo);
      if (lInfo) setLogs(lInfo);
    } catch (e) {
      console.error('Error loading initial data:', e);
    }
  }, []);

  useEffect(() => {
    loadInitialData();

    // Subscribe to live WebSocket Stream
    const unsubscribe = wsClient.subscribe((msg: any) => {
      switch (msg.type) {
        case 'SNAPSHOT':
        case 'TICK':
          if (msg.payload.cluster) setCluster(msg.payload.cluster);
          if (msg.payload.metrics) setMetrics(msg.payload.metrics);
          if (msg.payload.topics) setTopics(msg.payload.topics);
          if (msg.payload.groups) setConsumerGroups(msg.payload.groups);
          break;

        case 'LOG':
          setLogs(prev => [msg.payload, ...prev].slice(0, 300));
          break;

        case 'FLOW_PACKET':
          setLivePackets(prev => [...prev, msg.payload].slice(-20));
          break;

        case 'TOPICS_UPDATED':
          setTopics(msg.payload);
          break;

        case 'SIMULATION_STATUS':
          setSimulationActive(msg.payload.active);
          break;

        case 'DEMO_COMPLETED':
          setDemoRunning(false);
          loadInitialData();
          break;
      }
    });

    return () => {
      unsubscribe();
    };
  }, [loadInitialData]);

  // Actions
  const handleToggleSimulation = async () => {
    try {
      const res = await setSimulation(!simulationActive, 3);
      setSimulationActive(res.active);
    } catch (e) {
      console.error(e);
    }
  };

  const handleRunDemo = async () => {
    setDemoRunning(true);
    try {
      await runDemoPipeline();
    } catch (e) {
      alert('Error ejecutando demo pipeline');
      setDemoRunning(false);
    }
  };

  const handleProduceBurst = async (count: number) => {
    const targetTopics = ['orders-stream', 'payments', 'user-events'];
    const chosen = targetTopics[Math.floor(Math.random() * targetTopics.length)];

    for (let i = 0; i < count; i++) {
      await produceRecord({
        topic: chosen,
        key: `burst-${Math.floor(Math.random() * 900 + 100)}`,
        value: {
          burstId: i + 1,
          time: Date.now(),
          generatedBy: 'PortalWebDashboard',
          amount: +(Math.random() * 100).toFixed(2)
        }
      });
    }
  };

  const handleNavigateProduce = (topicName: string) => {
    setPreselectedTopic(topicName);
    setCurrentTab('producer');
  };

  return (
    <div className="app-container">
      {/* Top Navbar */}
      <Navbar
        currentTab={currentTab}
        onTabChange={setCurrentTab}
        cluster={cluster}
        simulationActive={simulationActive}
        onToggleSimulation={handleToggleSimulation}
        onRunDemo={handleRunDemo}
        demoRunning={demoRunning}
      />

      {/* Main Content Area */}
      <main className="main-content">
        {currentTab === 'dashboard' && (
          <DashboardView
            cluster={cluster}
            metrics={metrics}
            topics={topics}
            consumerGroups={consumerGroups}
            onProduceBurst={handleProduceBurst}
            onNavigateTab={setCurrentTab}
          />
        )}

        {currentTab === 'flow' && (
          <FlowVisualizerView
            topics={topics}
            consumerGroups={consumerGroups}
            cluster={cluster}
            simulationActive={simulationActive}
            onToggleSimulation={handleToggleSimulation}
            onProduceBurst={handleProduceBurst}
            livePackets={livePackets}
          />
        )}

        {currentTab === 'topics' && (
          <TopicsView
            topics={topics}
            onRefresh={loadInitialData}
            onNavigateProduce={handleNavigateProduce}
          />
        )}

        {currentTab === 'producer' && (
          <ProducerView
            topics={topics}
            preselectedTopic={preselectedTopic}
          />
        )}

        {currentTab === 'consumer' && (
          <ConsumerView
            topics={topics}
            consumerGroups={consumerGroups}
            onRefreshGroups={loadInitialData}
          />
        )}

        {currentTab === 'logs' && (
          <LogsView
            logs={logs}
            onRefresh={loadInitialData}
          />
        )}

        {currentTab === 'cluster' && (
          <ClusterView
            cluster={cluster}
          />
        )}
      </main>
    </div>
  );
};
