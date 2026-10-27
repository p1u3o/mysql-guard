package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// SemiSync is the semi-synchronous replication state of one server.
// Installed is false when neither the source nor the replica plugin is loaded.
type SemiSync struct {
	Known          bool `json:"known"`
	Installed      bool `json:"installed"`
	SourceEnabled  bool `json:"source_enabled"`  // rpl_semi_sync_source_enabled
	SourceOn       bool `json:"source_on"`       // Rpl_semi_sync_source_status (ON = sync, OFF = fell back to async)
	ReplicaEnabled bool `json:"replica_enabled"` // rpl_semi_sync_replica_enabled
	ReplicaOn      bool `json:"replica_on"`      // Rpl_semi_sync_replica_status
	SourceClients  int  `json:"source_clients"`  // Rpl_semi_sync_source_clients
}

// FellBack reports a source with semi-sync enabled that is running async.
func (s SemiSync) FellBack() bool { return s.Known && s.SourceEnabled && !s.SourceOn }

// Mode is "off", "sync", "async" (fell back) or "unknown", from the source's view.
func (s SemiSync) Mode() string {
	switch {
	case !s.Known:
		return "unknown"
	case !s.SourceEnabled:
		return "off"
	case s.SourceOn:
		return "sync"
	default:
		return "async"
	}
}

// IOHealth is the receiver (IO thread) view of the connection to the source.
type IOHealth struct {
	Known      bool
	State      string  // ON, OFF, CONNECTING
	LastErrNo  int     // LAST_ERROR_NUMBER
	SilenceSec float64 // seconds since the last heartbeat or queued event (-1 = never)
	Heartbeats int64   // COUNT_RECEIVED_HEARTBEATS
}

// LostSource reports whether the IO thread has lost its source: connecting,
// erroring, or silent (no event and no heartbeat) for longer than stale.
// A stopped IO thread (OFF without error) is an operator action, not evidence.
func (h IOHealth) LostSource(stale time.Duration) (bool, string) {
	if !h.Known {
		return false, ""
	}
	switch strings.ToUpper(h.State) {
	case "CONNECTING":
		return true, "io thread connecting"
	case "OFF":
		if h.LastErrNo != 0 {
			return true, fmt.Sprintf("io thread stopped with error %d", h.LastErrNo)
		}
		return false, ""
	case "ON":
		if h.LastErrNo != 0 && h.SilenceSec < 0 {
			return true, fmt.Sprintf("io thread error %d", h.LastErrNo)
		}
		if stale > 0 && h.SilenceSec >= stale.Seconds() {
			return true, fmt.Sprintf("no event or heartbeat from source for %.0fs", h.SilenceSec)
		}
	}
	return false, ""
}

const qIOHealth = `SELECT SERVICE_STATE, LAST_ERROR_NUMBER, COUNT_RECEIVED_HEARTBEATS,
 TIMESTAMPDIFF(MICROSECOND, NULLIF(GREATEST(
   IFNULL(NULLIF(LAST_HEARTBEAT_TIMESTAMP,'0000-00-00 00:00:00.000000'),'1970-01-02'),
   IFNULL(NULLIF(LAST_QUEUED_TRANSACTION_END_QUEUE_TIMESTAMP,'0000-00-00 00:00:00.000000'),'1970-01-02')),'1970-01-02'), NOW(6))/1000000
 FROM performance_schema.replication_connection_status LIMIT 1`

func readIOHealth(ctx context.Context, db *sql.DB) IOHealth {
	var h IOHealth
	var silence sql.NullFloat64
	err := db.QueryRowContext(ctx, qIOHealth).Scan(&h.State, &h.LastErrNo, &h.Heartbeats, &silence)
	if err != nil {
		return IOHealth{}
	}
	h.Known = true
	h.SilenceSec = -1
	if silence.Valid {
		h.SilenceSec = silence.Float64
	}
	return h
}

const qSemiSync = `SELECT LOWER(VARIABLE_NAME), VARIABLE_VALUE FROM performance_schema.global_status
 WHERE VARIABLE_NAME IN ('Rpl_semi_sync_source_status','Rpl_semi_sync_master_status','Rpl_semi_sync_replica_status','Rpl_semi_sync_slave_status','Rpl_semi_sync_source_clients','Rpl_semi_sync_master_clients')
 UNION ALL
 SELECT LOWER(VARIABLE_NAME), VARIABLE_VALUE FROM performance_schema.global_variables
 WHERE VARIABLE_NAME IN ('rpl_semi_sync_source_enabled','rpl_semi_sync_master_enabled','rpl_semi_sync_replica_enabled','rpl_semi_sync_slave_enabled')`

func readSemiSync(ctx context.Context, db *sql.DB) SemiSync {
	rows, err := db.QueryContext(ctx, qSemiSync)
	if err != nil {
		return SemiSync{}
	}
	defer rows.Close()
	kv := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return SemiSync{}
		}
		kv[k] = v
	}
	if rows.Err() != nil {
		return SemiSync{}
	}
	return parseSemiSync(kv)
}

func parseSemiSync(kv map[string]string) SemiSync {
	on := func(keys ...string) (bool, bool) {
		for _, k := range keys {
			if v, ok := kv[k]; ok {
				return strings.EqualFold(v, "ON") || v == "1", true
			}
		}
		return false, false
	}
	s := SemiSync{Known: true}
	var srcInst, repInst bool
	s.SourceEnabled, srcInst = on("rpl_semi_sync_source_enabled", "rpl_semi_sync_master_enabled")
	s.SourceOn, _ = on("rpl_semi_sync_source_status", "rpl_semi_sync_master_status")
	s.ReplicaEnabled, repInst = on("rpl_semi_sync_replica_enabled", "rpl_semi_sync_slave_enabled")
	s.ReplicaOn, _ = on("rpl_semi_sync_replica_status", "rpl_semi_sync_slave_status")
	for _, k := range []string{"rpl_semi_sync_source_clients", "rpl_semi_sync_master_clients"} {
		if v, ok := kv[k]; ok {
			fmt.Sscanf(v, "%d", &s.SourceClients)
			break
		}
	}
	s.Installed = srcInst || repInst
	return s
}

// SetSemiSync sets the semi-sync source and replica switches of a server.
// Missing plugins are an error the caller logs; nothing else changes.
func (a *Admin) SetSemiSync(ctx context.Context, source, replica bool) error {
	v := func(b bool) string {
		if b {
			return "ON"
		}
		return "OFF"
	}
	if err := a.setEither(ctx, "rpl_semi_sync_source_enabled", "rpl_semi_sync_master_enabled", v(source)); err != nil {
		return err
	}
	return a.setEither(ctx, "rpl_semi_sync_replica_enabled", "rpl_semi_sync_slave_enabled", v(replica))
}

// setEither sets the 8.0.26+ variable name, falling back to the legacy one.
func (a *Admin) setEither(ctx context.Context, name, legacy, val string) error {
	err := a.exec(ctx, "SET GLOBAL "+name+"="+val)
	if err == nil {
		return nil
	}
	if err2 := a.exec(ctx, "SET GLOBAL "+legacy+"="+val); err2 == nil {
		return nil
	}
	return err
}

// RestartIOThread applies replica-side semi-sync (read at IO thread start).
func (a *Admin) RestartIOThread(ctx context.Context) error {
	if err := a.replExec(ctx, "STOP REPLICA IO_THREAD"); err != nil {
		return err
	}
	return a.replExec(ctx, "START REPLICA IO_THREAD")
}

// ReadSemiSync reads this server's semi-sync state.
func (a *Admin) ReadSemiSync(ctx context.Context) SemiSync { return readSemiSync(ctx, a.db) }
