package consul

import "fmt"

// KeyPaths builds KV paths for a cluster (placeholders only in repo).
type KeyPaths struct {
	Cluster string
}

func (k KeyPaths) LeaderLock() string       { return fmt.Sprintf("mysql/%s/guard/leader", k.Cluster) }
func (k KeyPaths) WriterEpoch() string      { return fmt.Sprintf("mysql/%s/writer/epoch", k.Cluster) }
func (k KeyPaths) MasterKV() string         { return fmt.Sprintf("mysql/master/%s", k.Cluster) }
func (k KeyPaths) GTID(node string) string  { return fmt.Sprintf("mysql/%s/guard/gtid/%s", k.Cluster, node) }
func (k KeyPaths) Fenced(node string) string {
	return fmt.Sprintf("mysql/%s/guard/fenced/%s", k.Cluster, node)
}
func (k KeyPaths) LastFailover() string { return fmt.Sprintf("mysql/%s/guard/last-failover", k.Cluster) }
func (k KeyPaths) Maintenance() string  { return fmt.Sprintf("mysql/%s/guard/maintenance", k.Cluster) }
