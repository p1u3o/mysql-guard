package mysql

import (
	"context"
	"database/sql"
	"sync"

	"mysql-guard/internal/config"
)

// Pool holds one connection per peer (and local fallback).
type Pool struct {
	cfg *config.Config
	dbs map[string]*sql.DB
	mu  sync.Mutex
}

func NewPool(cfg *config.Config) (*Pool, error) {
	p := &Pool{cfg: cfg, dbs: map[string]*sql.DB{}}
	for _, peer := range cfg.Peers {
		if err := p.ensure(peer); err != nil {
			if len(cfg.MySQLPeerEndpoints) == 0 && peer != cfg.NodeName {
				continue
			}
			p.Close()
			return nil, err
		}
	}
	return p, nil
}

func (p *Pool) ensure(peer string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.dbs[peer] != nil {
		return nil
	}
	dsn, err := BuildPeerDSN(p.cfg, peer)
	if err != nil {
		return err
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	db.SetConnMaxLifetime(0)
	p.dbs[peer] = db
	return nil
}

func (p *Pool) DB(peer string) (*sql.DB, error) {
	if err := p.ensure(peer); err != nil {
		return nil, err
	}
	return p.dbs[peer], nil
}

func (p *Pool) Admin(peer string) (*Admin, error) {
	db, err := p.DB(peer)
	if err != nil {
		return nil, err
	}
	return NewAdminDB(db), nil
}

// ReadState loads GTID/replication state for a peer.
func (p *Pool) ReadState(ctx context.Context, peer string) (GTIDState, error) {
	db, err := p.DB(peer)
	if err != nil {
		return GTIDState{}, err
	}
	pr := &Prober{db: db, cfg: ProbeConfig{Timeout: p.cfg.ProbeTimeout}}
	return pr.ReadGTIDState(ctx)
}

func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, db := range p.dbs {
		_ = db.Close()
	}
	p.dbs = map[string]*sql.DB{}
}

// PeerHostPort returns replication source host/port for peer.
func PeerHostPort(cfg *config.Config, peer string) (string, int, error) {
	ep, ok := cfg.MySQLPeerEndpoints[peer]
	if !ok || ep == "" {
		return peer, cfg.MySQLPort, nil
	}
	h, port := splitHostPort(ep, cfg.MySQLPort)
	return h, port, nil
}

func ResolveOldNewPrimary(cfg *config.Config, newPrimary string) (old, new string) {
	new = newPrimary
	old = cfg.Peers[0]
	for _, p := range cfg.Peers {
		if p != newPrimary {
			old = p
			break
		}
	}
	return old, new
}
