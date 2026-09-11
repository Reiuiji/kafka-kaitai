package models

// PartitionFlowNode represents partition state and traffic metrics
type PartitionFlowNode struct {
	ID             int32   `json:"id"`
	Leader         int32   `json:"leader"`
	Replicas       []int32 `json:"replicas"`
	ISR            []int32 `json:"isr"`
	HighWatermark  int64   `json:"highWatermark"`
	ProduceRate    float64 `json:"produceRate"`    // estimated or calculated msg/s
	ThroughputMbps float64 `json:"throughputMbps"` // MB/s
}

// BrokerFlowNode represents an individual Kafka broker and its hosted partitions
type BrokerFlowNode struct {
	NodeID       int32               `json:"nodeId"`
	Host         string              `json:"host"`
	Port         int32               `json:"port"`
	IsController bool                `json:"isController"`
	Rack         string              `json:"rack,omitempty"`
	Partitions   []PartitionFlowNode `json:"partitions"`
	ThroughputMb float64             `json:"throughputMb"` // MB/s across this broker
	MsgRate      float64             `json:"msgRate"`      // msg/s across this broker
}

// ClusterFlowData encapsulates the entire end-to-end live pipeline topology
type ClusterFlowData struct {
	Topic           string                   `json:"topic"`
	ClusterID       string                   `json:"clusterId"`
	ControllerID    int32                    `json:"controllerId"`
	Brokers         []BrokerFlowNode         `json:"brokers"`
	TotalPartitions int                      `json:"totalPartitions"`
	TotalMessages   int64                    `json:"totalMessages"`
	Producer        StressTestMetrics        `json:"producer"`
	Consumer        ConsumerBenchmarkMetrics `json:"consumer"`
}
