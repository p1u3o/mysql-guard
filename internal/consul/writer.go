package consul

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	capi "github.com/hashicorp/consul/api"
)

// MasterFormat is how mysql/master/<cluster> is written.
//
//	epoch  (default): "<node>|<epoch>"
//	legacy:           "<host>:<port>" (the bash guard / repman post-script format);
//	                  the epoch lives only in mysql/<cluster>/writer/epoch and the
//	                  node name in mysql/<cluster>/writer/current ("<node>|<epoch>").
//
// Reads always accept both formats, so a cluster can be migrated in either
// direction with `mysql-guard migrate-key -to=<format>` while guards run.
type MasterFormat string

const (
	FormatEpoch  MasterFormat = "epoch"
	FormatLegacy MasterFormat = "legacy"
)

// faultSetMaster is nil in release builds; integration drills build with
// -tags faultinject (see fault_inject.go) to fail SetMaster on demand.
var faultSetMaster func() error

// WriterService describes the Consul catalog service that points at the writer
// (same shape the bash guard registers: external node "<name>-vnode").
type WriterService struct {
	Name string   // empty = disabled
	Tags []string // e.g. mysql,writer
}

// Options configures the master-key adapter and writer service.
type Options struct {
	Format    MasterFormat
	Endpoints map[string]string // node -> host:port (legacy write and read mapping)
	Writer    WriterService
}

// SetOptions installs adapter options (call once after NewStore).
func (s *Store) SetOptions(o Options) {
	if o.Format == "" {
		o.Format = FormatEpoch
	}
	s.opts = o
}

func (s *Store) Options() Options { return s.opts }

// EncodeMaster returns the master-key value for node/epoch in format f.
func EncodeMaster(f MasterFormat, endpoints map[string]string, node string, epoch uint64) (string, error) {
	if f == FormatLegacy {
		ep := endpoints[node]
		if ep == "" {
			return "", fmt.Errorf("legacy master format: no host:port for node %q (set -mysql-peer-endpoints)", node)
		}
		return ep, nil
	}
	return fmt.Sprintf("%s|%d", node, epoch), nil
}

// DecodeMaster parses either format. For legacy values the node is resolved via
// endpoints (exact host:port match) and epoch is 0 (caller reads the epoch key).
func DecodeMaster(val string, endpoints map[string]string) (node string, epoch uint64, legacy bool, err error) {
	val = strings.TrimSpace(val)
	if val == "" {
		return "", 0, false, nil
	}
	if n, e, ok := strings.Cut(val, "|"); ok {
		if e != "" {
			_, _ = fmt.Sscanf(e, "%d", &epoch)
		}
		return n, epoch, false, nil
	}
	if _, _, herr := net.SplitHostPort(val); herr != nil {
		// bare node name without epoch (hand-written key)
		return val, 0, false, nil
	}
	for n, ep := range endpoints {
		if ep == val {
			return n, 0, true, nil
		}
	}
	return "", 0, true, fmt.Errorf("legacy master value %q matches no peer endpoint", val)
}

func (s *Store) writerCurrentKey() string {
	return fmt.Sprintf("mysql/%s/writer/current", s.keys.Cluster)
}

// SetMaster writes the master key in the configured format (plus writer/current
// in legacy mode) and then syncs the writer service if enabled.
func (s *Store) SetMaster(ctx context.Context, node string, epoch uint64) error {
	if faultSetMaster != nil {
		if err := faultSetMaster(); err != nil {
			return err
		}
	}
	val, err := EncodeMaster(s.opts.Format, s.opts.Endpoints, node, epoch)
	if err != nil {
		return err
	}
	if s.opts.Format == FormatLegacy {
		if _, err := s.client.KV().Put(&capi.KVPair{Key: s.writerCurrentKey(), Value: []byte(fmt.Sprintf("%s|%d", node, epoch))}, nil); err != nil {
			return err
		}
	}
	if _, err := s.client.KV().Put(&capi.KVPair{Key: s.keys.MasterKV(), Value: []byte(val)}, nil); err != nil {
		return err
	}
	if s.opts.Writer.Name != "" {
		if _, err := s.SyncWriterService(ctx, node, epoch, "guard"); err != nil {
			return fmt.Errorf("master key set but writer service sync failed: %w", err)
		}
	}
	return nil
}

// GetPrimary returns the current primary node and epoch from the master key,
// accepting both formats. A legacy value takes the epoch from the epoch key.
func (s *Store) GetPrimary(ctx context.Context) (string, uint64, error) {
	pair, _, err := s.client.KV().Get(s.keys.MasterKV(), nil)
	if err != nil || pair == nil || len(pair.Value) == 0 {
		return "", 0, err
	}
	node, epoch, legacy, derr := DecodeMaster(string(pair.Value), s.opts.Endpoints)
	if derr != nil {
		return "", 0, derr
	}
	if legacy || epoch == 0 {
		if e, err := s.GetEpoch(ctx); err == nil {
			epoch = e
		}
	}
	return node, epoch, nil
}

// MigrateMaster rewrites the master key in format `to` with a CAS on its
// ModifyIndex (no-op if already in that format). Returns old and new values.
func (s *Store) MigrateMaster(ctx context.Context, to MasterFormat) (string, string, error) {
	pair, _, err := s.client.KV().Get(s.keys.MasterKV(), nil)
	if err != nil {
		return "", "", err
	}
	if pair == nil || len(pair.Value) == 0 {
		return "", "", fmt.Errorf("master key %s is empty", s.keys.MasterKV())
	}
	old := string(pair.Value)
	node, epoch, legacy, err := DecodeMaster(old, s.opts.Endpoints)
	if err != nil {
		return old, "", err
	}
	if legacy || epoch == 0 {
		if epoch, err = s.GetEpoch(ctx); err != nil {
			return old, "", err
		}
	}
	if epoch == 0 {
		// legacy cluster never had an epoch key: create epoch 1 (CAS create-only)
		if epoch, err = s.CasBumpEpoch(ctx, 0); err != nil {
			return old, "", err
		}
	}
	val, err := EncodeMaster(to, s.opts.Endpoints, node, epoch)
	if err != nil {
		return old, "", err
	}
	if to == FormatLegacy {
		if _, err := s.client.KV().Put(&capi.KVPair{Key: s.writerCurrentKey(), Value: []byte(fmt.Sprintf("%s|%d", node, epoch))}, nil); err != nil {
			return old, "", err
		}
	}
	if val == old {
		return old, val, nil
	}
	ok, _, err := s.client.KV().CAS(&capi.KVPair{Key: s.keys.MasterKV(), Value: []byte(val), ModifyIndex: pair.ModifyIndex}, nil)
	if err != nil {
		return old, "", err
	}
	if !ok {
		return old, "", fmt.Errorf("master key changed during migration; retry")
	}
	return old, val, nil
}

func (s *Store) writerVNode() string { return s.opts.Writer.Name + "-vnode" }

// WriterServiceTarget returns host:port the writer service currently points at ("" if absent).
func (s *Store) WriterServiceTarget(ctx context.Context) (string, error) {
	if s.opts.Writer.Name == "" {
		return "", nil
	}
	svcs, _, err := s.client.Catalog().Service(s.opts.Writer.Name, "", nil)
	if err != nil {
		return "", err
	}
	for _, sv := range svcs {
		if sv.Node == s.writerVNode() {
			addr := sv.ServiceAddress
			if addr == "" {
				addr = sv.Address
			}
			return net.JoinHostPort(addr, strconv.Itoa(sv.ServicePort)), nil
		}
	}
	return "", nil
}

// SyncWriterService (re)registers the writer catalog service on its vnode when
// it does not already point at node's endpoint. Returns true if it wrote.
func (s *Store) SyncWriterService(ctx context.Context, node string, epoch uint64, by string) (bool, error) {
	if s.opts.Writer.Name == "" {
		return false, nil
	}
	ep := s.opts.Endpoints[node]
	if ep == "" {
		return false, fmt.Errorf("writer service: no host:port for node %q", node)
	}
	cur, err := s.WriterServiceTarget(ctx)
	if err == nil && cur == ep {
		return false, nil
	}
	host, portStr, err := net.SplitHostPort(ep)
	if err != nil {
		return false, err
	}
	port, _ := strconv.Atoi(portStr)
	reg := &capi.CatalogRegistration{
		Node:           s.writerVNode(),
		Address:        host,
		SkipNodeUpdate: false,
		NodeMeta:       map[string]string{"external-node": "true", "external-probe": "false"},
		Service: &capi.AgentService{
			ID:      s.opts.Writer.Name,
			Service: s.opts.Writer.Name,
			Address: host,
			Port:    port,
			Tags:    s.opts.Writer.Tags,
			Meta: map[string]string{
				"cluster": s.keys.Cluster, "node": node,
				"epoch": strconv.FormatUint(epoch, 10), "promoted_by": by,
			},
		},
	}
	if _, err := s.client.Catalog().Register(reg, nil); err != nil {
		return false, err
	}
	return true, nil
}

// ForeignWriter reports a master key that was not written by a guard: every
// guard write bumps the epoch first and records it (in the value for the epoch
// format, in writer/current for legacy). A value whose epoch/node disagrees
// with those (e.g. repman's or the bash guard's post-failover script) is
// foreign; the guard then alerts and does not act on it until resolved.
func (s *Store) ForeignWriter(ctx context.Context) (string, bool, error) {
	pair, _, err := s.client.KV().Get(s.keys.MasterKV(), nil)
	if err != nil || pair == nil || len(pair.Value) == 0 {
		return "", false, err
	}
	val := string(pair.Value)
	node, epoch, legacy, derr := DecodeMaster(val, s.opts.Endpoints)
	if derr != nil {
		return val, true, nil
	}
	cur, err := s.GetEpoch(ctx)
	if err != nil {
		return "", false, err
	}
	if !legacy {
		if epoch != 0 && epoch != cur {
			return val, true, nil
		}
		return "", false, nil
	}
	if s.opts.Format == FormatEpoch {
		return val, true, nil
	}
	wn, we, err := s.WriterCurrent(ctx)
	if err != nil {
		return "", false, err
	}
	if wn == "" {
		return "", false, nil // pre-migration legacy cluster: nothing recorded yet
	}
	if wn != node || we != cur {
		return val, true, nil
	}
	return "", false, nil
}

// WriterCurrent reads mysql/<cluster>/writer/current ("node|epoch"), written by
// every guard SetMaster in legacy mode.
func (s *Store) WriterCurrent(ctx context.Context) (string, uint64, error) {
	p, _, err := s.client.KV().Get(s.writerCurrentKey(), nil)
	if err != nil || p == nil {
		return "", 0, err
	}
	n, e, _, _ := DecodeMaster(string(p.Value), nil)
	return n, e, nil
}
