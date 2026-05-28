package ec

// ClusterConfig holds the static cluster configuration loaded from YAML.
type ClusterConfig struct {
	DataShards   int    `yaml:"data_shards"`
	ParityShards int    `yaml:"parity_shards"`
	Peers        []Peer `yaml:"peers"`
	Pool         int    `yaml:"pool"`
	Timeout      string `yaml:"timeout"`
}
