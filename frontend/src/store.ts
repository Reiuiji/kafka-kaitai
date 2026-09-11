import type { ConnectionStatus, TopicSummary, KaitaiFormat, StressTestMetrics, ConsumerBenchmarkMetrics } from '../bindings/github.com/okuu/kafka_kaitai/models';

type Listener = () => void;

export interface SavedStressConfig {
  topic: string;
  topicManual: string;
  concurrency: number;
  targetRate: number;
  durationSeconds: number;
  messageSizeBytes: number;
  payloadType: string;
  customPayload: string;
  keyPattern: string;
}

const DEFAULT_STRESS_CONFIG: SavedStressConfig = {
  topic: '',
  topicManual: '',
  concurrency: 8,
  targetRate: 0,
  durationSeconds: 0,
  messageSizeBytes: 256,
  payloadType: 'random',
  customPayload: '{"event": "telemetry", "status": "active"}',
  keyPattern: 'uuid',
};

function loadSavedStressConfig(): SavedStressConfig {
  try {
    const raw = localStorage.getItem('kafka_kaitai_stress_config');
    if (raw) {
      return { ...DEFAULT_STRESS_CONFIG, ...JSON.parse(raw) };
    }
  } catch (e) {
    console.error('Failed to load saved stress config', e);
  }
  return { ...DEFAULT_STRESS_CONFIG };
}

function saveStressConfigToStorage(cfg: SavedStressConfig) {
  try {
    localStorage.setItem('kafka_kaitai_stress_config', JSON.stringify(cfg));
  } catch (e) {
    console.error('Failed to persist stress config', e);
  }
}

class AppStore {
  activeView: string = 'dashboard';
  connection: ConnectionStatus = {
    connected: false,
    clusterId: '',
    controllerId: 0,
    brokers: [],
    topicsCount: 0,
    latencyMs: 0,
  };
  topics: TopicSummary[] = [];
  formats: KaitaiFormat[] = [];
  selectedTopic: string = '';
  stressConfig: SavedStressConfig = loadSavedStressConfig();
  stressMetrics: StressTestMetrics = {
    active: false,
    topic: '',
    elapsedSeconds: 0,
    sentMessages: 0,
    sentBytes: 0,
    currentMsgRate: 0,
    currentByteRate: 0,
    errorsCount: 0,
    latencyP50: 0,
    latencyP95: 0,
    latencyP99: 0,
    latencyAvg: 0,
  };
  consumerMetrics: ConsumerBenchmarkMetrics = {
    active: false,
    topic: '',
    mode: '',
    elapsedSeconds: 0,
    consumedMessages: 0,
    consumedBytes: 0,
    currentMsgRate: 0,
    currentByteRate: 0,
    droppedMessages: 0,
    storedMessages: 0,
    errorsCount: 0,
  };
  systemTelemetry: any = null;

  private listeners: Set<Listener> = new Set();

  subscribe(listener: Listener) {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  notify() {
    this.listeners.forEach((fn) => fn());
  }

  updateStressConfig(partial: Partial<SavedStressConfig>) {
    this.stressConfig = { ...this.stressConfig, ...partial };
    saveStressConfigToStorage(this.stressConfig);
  }

  setView(view: string) {
    this.activeView = view;
    this.notify();
  }

  setConnection(status: ConnectionStatus) {
    this.connection = status;
    this.notify();
  }

  setTopics(topics: TopicSummary[]) {
    this.topics = topics;
    if (!this.selectedTopic && topics.length > 0) {
      this.selectedTopic = topics[0].name;
    }
    this.notify();
  }

  setFormats(formats: KaitaiFormat[]) {
    this.formats = formats;
    this.notify();
  }
}

export const store = new AppStore();
