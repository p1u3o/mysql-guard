package consul

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"
	"time"

	capi "github.com/hashicorp/consul/api"
)

// Member service (v0.3.6, -member-service): one Consul catalog entry per
// cluster member, each on its own external node "<service>-<node>-vnode", so
// DNS resolves writer.<service>.service.consul to the primary and
// reader.<service>.service.consul to healthy replicas only.
//
// Roles (= the single tag):
//
//	writer  the node in the master key
//	reader  a non-primary that is reachable, read-only, replicating from the
//	        primary with both threads on, not stalled and not lagging
//	lagging a replica whose lag is over -member-max-lag (check: warning)
//	fenced  anything else (fenced, needs-rebuild, unreachable, broken
//	        replication): never served as a reader (check: critical)
//
// Each entry carries a catalog check "<service>:<node>:mysql" (passing,
// warning, critical). Catalog checks have no TTL, so the leader rewrites every
// check at least every MemberRefresh and its Output starts with
// "as-of=<RFC3339> epoch=<n> by=<leader>": a check whose as-of is older than
// MemberStaleAfter is stale (documented in the README). When the leader dies
// the next db guard takes the session within SessionTTL and resumes writing.
//
// Entries are written only after the epoch CAS (with the master key) and by
// the leader's per-tick sync, and every write first re-reads the epoch key:
// a write for an older epoch is refused, so a stale leader cannot move tags.
const (
	MemberWriter  = "writer"
	MemberReader  = "reader"
	MemberFenced  = "fenced"
	MemberLagging = "lagging"
)

// MemberVNode is the external catalog node for a member entry.
func MemberVNode(service, node string) string { return service + "-" + node + "-vnode" }

// MemberServiceID is the service ID of a member entry (unique per vnode).
func MemberServiceID(service, node string) string { return service + "-" + node }

// MemberRefresh: rewrite an unchanged entry at least this often (as-of bump).
const MemberRefresh = 30 * time.Second

// MemberStaleAfter: consumers treat a check whose as-of is older than this as stale.
const MemberStaleAfter = 90 * time.Second

// MemberState is the desired catalog state of one member.
type MemberState struct {
	Role   string // writer, reader, lagging, fenced
	Status string // passing, warning, critical
	Reason string
}

func (m MemberState) norm() MemberState {
	switch m.Role {
	case MemberWriter, MemberReader, MemberLagging:
	default:
		m.Role = MemberFenced
	}
	switch m.Status {
	case capi.HealthPassing, capi.HealthWarning, capi.HealthCritical:
	default:
		m.Status = capi.HealthCritical
	}
	if m.Role == MemberFenced {
		m.Status = capi.HealthCritical
	}
	return m
}

// MemberCheckID is the catalog check ID of a member entry.
func MemberCheckID(service, node string) string { return service + ":" + node + ":mysql" }

// CheckOutput is the check Output: as-of stamp first (see MemberStaleAfter).
func CheckOutput(now time.Time, epoch uint64, by, role, reason string) string {
	return fmt.Sprintf("as-of=%s epoch=%d by=%s role=%s stale-after=%s: %s", now.UTC().Format(time.RFC3339), epoch, by, role, MemberStaleAfter, reason)
}

// MemberRegistration builds the catalog registration (entry + check) for one member.
func MemberRegistration(service, cluster, node, endpoint string, st MemberState, epoch uint64, by string, now time.Time) (*capi.CatalogRegistration, error) {
	st = st.norm()
	host, portStr, err := net.SplitHostPort(endpoint)
	if err != nil {
		return nil, fmt.Errorf("member service: bad endpoint %q for %s: %w", endpoint, node, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("member service: bad port %q for %s", portStr, node)
	}
	vnode := MemberVNode(service, node)
	sid := MemberServiceID(service, node)
	return &capi.CatalogRegistration{
		Node:     vnode,
		Address:  host,
		NodeMeta: map[string]string{"external-node": "true", "external-probe": "false"},
		Service: &capi.AgentService{
			ID:      sid,
			Service: service,
			Address: host,
			Port:    port,
			Tags:    []string{st.Role},
			Meta: map[string]string{
				"cluster": cluster, "node": node,
				"epoch": strconv.FormatUint(epoch, 10), "role": st.Role,
			},
		},
		Checks: capi.HealthChecks{{
			Node:      vnode,
			CheckID:   MemberCheckID(service, node),
			Name:      "mysql " + node + " (" + st.Role + ")",
			Status:    st.Status,
			Output:    CheckOutput(now, epoch, by, st.Role, st.Reason),
			Notes:     "written by the mysql-guard leader; stale when as-of is older than " + MemberStaleAfter.String(),
			ServiceID: sid,
		}},
	}, nil
}

type memberWritten struct {
	st    MemberState
	epoch uint64
	ep    string
	at    time.Time
}

// SyncMembers registers every member with its state at epoch (members missing
// from states are fenced/critical). An entry is rewritten when its role,
// status, endpoint or epoch changed, or MemberRefresh passed. Returns the
// members whose role or status changed.
func (s *Store) SyncMembers(ctx context.Context, epoch uint64, states map[string]MemberState, by string) ([]string, error) {
	svc := s.opts.Members.Name
	if svc == "" {
		return nil, nil
	}
	cur, err := s.GetEpoch(ctx)
	if err != nil {
		return nil, err
	}
	if cur != epoch {
		return nil, fmt.Errorf("member service: epoch %d is not current (%d); not written", epoch, cur)
	}
	now := time.Now()
	nodes := make([]string, 0, len(s.opts.Endpoints))
	for n := range s.opts.Endpoints {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	s.memMu.Lock()
	defer s.memMu.Unlock()
	if s.memLast == nil {
		s.memLast = map[string]memberWritten{}
	}
	var changed []string
	var firstErr error
	for _, n := range nodes {
		st := states[n].norm()
		ep := s.opts.Endpoints[n]
		last, ok := s.memLast[n]
		if ok && last.st == st && last.epoch == epoch && last.ep == ep && now.Sub(last.at) < MemberRefresh {
			continue
		}
		reg, err := MemberRegistration(svc, s.keys.Cluster, n, ep, st, epoch, by, now)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if _, err := s.client.Catalog().Register(reg, (&capi.WriteOptions{}).WithContext(ctx)); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if !ok || last.st.Role != st.Role || last.st.Status != st.Status {
			changed = append(changed, n+"="+st.Role+"/"+st.Status)
		}
		s.memLast[n] = memberWritten{st: st, epoch: epoch, ep: ep, at: now}
	}
	return changed, firstErr
}

// SyncWriterCheck keeps the "<writer-service>:mysql" check on the writer
// vnode (same as-of/stale convention). Rewritten on change or MemberRefresh.
func (s *Store) SyncWriterCheck(ctx context.Context, node string, epoch uint64, status, reason, by string) error {
	if s.opts.Writer.Name == "" {
		return nil
	}
	if status != capi.HealthPassing && status != capi.HealthWarning {
		status = capi.HealthCritical
	}
	now := time.Now()
	s.memMu.Lock()
	defer s.memMu.Unlock()
	key := "\x00writer"
	if s.memLast == nil {
		s.memLast = map[string]memberWritten{}
	}
	st := MemberState{Role: node, Status: status}
	if last, ok := s.memLast[key]; ok && last.st == st && last.epoch == epoch && now.Sub(last.at) < MemberRefresh {
		return nil
	}
	cur, err := s.GetEpoch(ctx)
	if err != nil {
		return err
	}
	if cur != epoch {
		return fmt.Errorf("writer check: epoch %d is not current (%d)", epoch, cur)
	}
	host, _, err := net.SplitHostPort(s.opts.Endpoints[node])
	if err != nil {
		return fmt.Errorf("writer check: no host:port for %q", node)
	}
	reg := &capi.CatalogRegistration{
		Node:           s.writerVNode(),
		Address:        host,
		SkipNodeUpdate: true,
		Check: &capi.AgentCheck{
			Node: s.writerVNode(), CheckID: s.opts.Writer.Name + ":mysql", Name: "mysql writer " + node,
			Status: status, Output: CheckOutput(now, epoch, by, "writer "+node, reason),
			Notes:     "written by the mysql-guard leader; stale when as-of is older than " + MemberStaleAfter.String(),
			ServiceID: s.opts.Writer.Name,
		},
	}
	if _, err := s.client.Catalog().Register(reg, (&capi.WriteOptions{}).WithContext(ctx)); err != nil {
		return err
	}
	s.memLast[key] = memberWritten{st: st, epoch: epoch, at: now}
	return nil
}
