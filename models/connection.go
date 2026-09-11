package models

type ConnectionConfig struct {
	ID                 string   `json:"id"`
	Name               string   `json:"name"`
	Brokers            []string `json:"brokers"`
	AuthType           string   `json:"authType"` // "none", "plain", "scram-sha-256", "scram-sha-512", "ssl", "mtls", "oauthbearer"
	Username           string   `json:"username"`
	Password           string   `json:"password"`
	TLSEnabled         bool     `json:"tlsEnabled"`
	CACertPath         string   `json:"caCertPath"`
	ClientCertPath     string   `json:"clientCertPath"`
	ClientKeyPath      string   `json:"clientKeyPath"`
	InsecureSkipVerify bool     `json:"insecureSkipVerify"`
	Token              string   `json:"token"`
	SchemaRegistryURL  string   `json:"schemaRegistryUrl"`
	SchemaRegistryUser string   `json:"schemaRegistryUser"`
	SchemaRegistryPass string   `json:"schemaRegistryPass"`
}

type ConnectionStatus struct {
	Connected    bool     `json:"connected"`
	ClusterID    string   `json:"clusterId"`
	ControllerID int32    `json:"controllerId"`
	Brokers      []string `json:"brokers"`
	TopicsCount  int      `json:"topicsCount"`
	LatencyMs    float64  `json:"latencyMs"`
	Error        string   `json:"error,omitempty"`
}

type PartitionInfo struct {
	ID             int32   `json:"id"`
	Leader         int32   `json:"leader"`
	Replicas       []int32 `json:"replicas"`
	ISR            []int32 `json:"isr"`
	EarliestOffset int64   `json:"earliestOffset"`
	LatestOffset   int64   `json:"latestOffset"`
}

type TopicSummary struct {
	Name              string          `json:"name"`
	PartitionsCount   int             `json:"partitionsCount"`
	ReplicationFactor int             `json:"replicationFactor"`
	Partitions        []PartitionInfo `json:"partitions"`
}

type ConsumerGroupMember struct {
	MemberID   string `json:"memberId"`
	ClientID   string `json:"clientId"`
	ClientHost string `json:"clientHost"`
}

type ConsumerGroupInfo struct {
	GroupID      string                `json:"groupId"`
	State        string                `json:"state"`
	ProtocolType string                `json:"protocolType"`
	Protocol     string                `json:"protocol"`
	Members      []ConsumerGroupMember `json:"members"`
	LagTotal     int64                 `json:"lagTotal"`
}

type CreateTopicRequest struct {
	Name              string            `json:"name"`
	Partitions        int32             `json:"partitions"`
	ReplicationFactor int16             `json:"replicationFactor"`
	Configs           map[string]string `json:"configs"` // e.g. "retention.ms": "86400000", "cleanup.policy": "delete"
}

type TopicConfigEntry struct {
	Name      string `json:"name"`
	Value     string `json:"value"`
	IsDefault bool   `json:"isDefault"`
	Source    string `json:"source"`
}

type TopicDetail struct {
	Name              string             `json:"name"`
	PartitionsCount   int                `json:"partitionsCount"`
	ReplicationFactor int                `json:"replicationFactor"`
	Partitions        []PartitionInfo    `json:"partitions"`
	Configs           []TopicConfigEntry `json:"configs"`
}
