package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	_ "modernc.org/sqlite"
)

// Store handles SQLite persistence for offline data
type Store struct {
	db       *sql.DB
	siteID   string
	deviceID string
}

// StateRecord represents a telemetry state record
type StateRecord struct {
	ID            int64
	SiteID        string
	DeviceID      string
	Seq           int64
	Timestamp     string
	Voltage       *int
	TargetRPM     *int
	ActualRPM     *int
	MotorAmps     *float64
	MotorFreq     *float64
	VFDTemp       *float64
	WheelsRunning *int
	PaddleRunning *int
	PositionDeg   *float64
	ReceivedAt    string
	Synced        int
	SyncedAt      *string
}

// CommandRecord represents a command audit record
type CommandRecord struct {
	ID          int64
	SiteID      string
	DeviceID    string
	Timestamp   string
	Source      string
	CommandType string
	Payload     string
	UserID      *string
	ReceivedAt  string
	Synced      int
	SyncedAt    *string
}

// FaultRecord represents a fault/alarm record
type FaultRecord struct {
	ID         int64
	SiteID     string
	DeviceID   string
	Timestamp  string
	FaultCode  int
	FaultDesc  *string
	ClearedAt  *string
	ReceivedAt string
	Synced     int
	SyncedAt   *string
}

// GatewayEvent represents a gateway status event
type GatewayEvent struct {
	ID        int64
	Timestamp string
	Event     string
	Details   *string
}

// NewStore creates a new SQLite store
func NewStore(dbPath, siteID, deviceID string) (*Store, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	// Set pragmas for better performance
	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA cache_size=10000",
		"PRAGMA temp_store=MEMORY",
	}
	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			log.Printf("Warning: failed to set %s: %v", pragma, err)
		}
	}

	store := &Store{
		db:       db,
		siteID:   siteID,
		deviceID: deviceID,
	}

	if err := store.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	return store, nil
}

// migrate creates the database schema
func (s *Store) migrate() error {
	schema := `
	-- State log (time-series telemetry)
	CREATE TABLE IF NOT EXISTS state_log (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		site_id TEXT NOT NULL,
		device_id TEXT NOT NULL,
		seq INTEGER NOT NULL,
		ts TEXT NOT NULL,
		voltage INTEGER,
		target_rpm INTEGER,
		actual_rpm INTEGER,
		motor_amps REAL,
		motor_freq REAL,
		vfd_temp REAL,
		wheels_running INTEGER,
		paddle_running INTEGER,
		position_deg REAL,
		received_at TEXT NOT NULL,
		synced INTEGER DEFAULT 0,
		synced_at TEXT,
		UNIQUE(site_id, device_id, seq)
	);

	CREATE INDEX IF NOT EXISTS idx_state_unsynced ON state_log(synced) WHERE synced = 0;
	CREATE INDEX IF NOT EXISTS idx_state_ts ON state_log(ts DESC);

	-- Command audit log
	CREATE TABLE IF NOT EXISTS command_log (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		site_id TEXT NOT NULL,
		device_id TEXT NOT NULL,
		ts TEXT NOT NULL,
		source TEXT NOT NULL,
		command_type TEXT NOT NULL,
		payload TEXT NOT NULL,
		user_id TEXT,
		received_at TEXT NOT NULL,
		synced INTEGER DEFAULT 0,
		synced_at TEXT
	);

	CREATE INDEX IF NOT EXISTS idx_cmd_unsynced ON command_log(synced) WHERE synced = 0;
	CREATE INDEX IF NOT EXISTS idx_cmd_ts ON command_log(ts DESC);

	-- Fault/alarm log
	CREATE TABLE IF NOT EXISTS fault_log (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		site_id TEXT NOT NULL,
		device_id TEXT NOT NULL,
		ts TEXT NOT NULL,
		fault_code INTEGER NOT NULL,
		fault_desc TEXT,
		cleared_at TEXT,
		received_at TEXT NOT NULL,
		synced INTEGER DEFAULT 0,
		synced_at TEXT
	);

	CREATE INDEX IF NOT EXISTS idx_fault_unsynced ON fault_log(synced) WHERE synced = 0;
	CREATE INDEX IF NOT EXISTS idx_fault_active ON fault_log(cleared_at) WHERE cleared_at IS NULL;

	-- Gateway status log
	CREATE TABLE IF NOT EXISTS gateway_log (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		ts TEXT NOT NULL,
		event TEXT NOT NULL,
		details TEXT
	);

	CREATE INDEX IF NOT EXISTS idx_gateway_ts ON gateway_log(ts DESC);
	`

	_, err := s.db.Exec(schema)
	return err
}

// Close closes the database connection
func (s *Store) Close() error {
	return s.db.Close()
}

// nowISO returns current time in ISO8601 format
func nowISO() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// StoreState stores a state message from MQTT
func (s *Store) StoreState(payload []byte) error {
	// Parse the JSON payload to extract fields
	var data map[string]interface{}
	if err := json.Unmarshal(payload, &data); err != nil {
		// Store raw payload if not valid JSON
		return s.storeRawState(payload)
	}

	// Extract fields with type safety
	var seq int64
	if v, ok := data["seq"].(float64); ok {
		seq = int64(v)
	}

	var ts string
	if v, ok := data["ts"].(string); ok {
		ts = v
	} else {
		ts = nowISO()
	}

	var voltage, targetRPM, actualRPM, wheelsRunning, paddleRunning *int
	var motorAmps, motorFreq, vfdTemp, positionDeg *float64

	if v, ok := data["voltage"].(float64); ok {
		i := int(v)
		voltage = &i
	}
	if v, ok := data["target_rpm"].(float64); ok {
		i := int(v)
		targetRPM = &i
	}
	if v, ok := data["actual_rpm"].(float64); ok {
		i := int(v)
		actualRPM = &i
	}
	if v, ok := data["motor_amps"].(float64); ok {
		motorAmps = &v
	}
	if v, ok := data["motor_freq"].(float64); ok {
		motorFreq = &v
	}
	if v, ok := data["vfd_temp"].(float64); ok {
		vfdTemp = &v
	}
	if v, ok := data["position_deg"].(float64); ok {
		positionDeg = &v
	}
	if v, ok := data["wheels_running"].(bool); ok {
		i := 0
		if v {
			i = 1
		}
		wheelsRunning = &i
	}
	if v, ok := data["paddle_running"].(bool); ok {
		i := 0
		if v {
			i = 1
		}
		paddleRunning = &i
	}

	_, err := s.db.Exec(`
		INSERT OR REPLACE INTO state_log
		(site_id, device_id, seq, ts, voltage, target_rpm, actual_rpm,
		 motor_amps, motor_freq, vfd_temp, wheels_running, paddle_running,
		 position_deg, received_at, synced)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
	`, s.siteID, s.deviceID, seq, ts, voltage, targetRPM, actualRPM,
		motorAmps, motorFreq, vfdTemp, wheelsRunning, paddleRunning,
		positionDeg, nowISO())

	return err
}

// storeRawState stores non-JSON state payload
func (s *Store) storeRawState(payload []byte) error {
	_, err := s.db.Exec(`
		INSERT INTO state_log
		(site_id, device_id, seq, ts, received_at, synced)
		VALUES (?, ?, 0, ?, ?, 0)
	`, s.siteID, s.deviceID, nowISO(), nowISO())
	return err
}

// StoreCommand stores a command message
func (s *Store) StoreCommand(payload []byte, source string) error {
	// Parse command to extract type
	var data map[string]interface{}
	commandType := "unknown"
	if err := json.Unmarshal(payload, &data); err == nil {
		// Determine command type from payload
		if _, ok := data["wheels_running"]; ok {
			commandType = "motor_control"
		} else if _, ok := data["target_rpm"]; ok {
			commandType = "set_rpm"
		} else if _, ok := data["estop"]; ok {
			commandType = "estop"
		}
	}

	ts := nowISO()
	if v, ok := data["ts"].(string); ok {
		ts = v
	}

	_, err := s.db.Exec(`
		INSERT INTO command_log
		(site_id, device_id, ts, source, command_type, payload, received_at, synced)
		VALUES (?, ?, ?, ?, ?, ?, ?, 0)
	`, s.siteID, s.deviceID, ts, source, commandType, string(payload), nowISO())

	return err
}

// StoreFault stores a fault/alarm event
func (s *Store) StoreFault(faultCode int, faultDesc string) error {
	_, err := s.db.Exec(`
		INSERT INTO fault_log
		(site_id, device_id, ts, fault_code, fault_desc, received_at, synced)
		VALUES (?, ?, ?, ?, ?, ?, 0)
	`, s.siteID, s.deviceID, nowISO(), faultCode, faultDesc, nowISO())
	return err
}

// ClearFault marks a fault as cleared
func (s *Store) ClearFault(faultCode int) error {
	_, err := s.db.Exec(`
		UPDATE fault_log
		SET cleared_at = ?
		WHERE site_id = ? AND device_id = ? AND fault_code = ? AND cleared_at IS NULL
	`, nowISO(), s.siteID, s.deviceID, faultCode)
	return err
}

// LogGatewayEvent logs a gateway status event
func (s *Store) LogGatewayEvent(event string, details string) error {
	var detailsPtr *string
	if details != "" {
		detailsPtr = &details
	}
	_, err := s.db.Exec(`
		INSERT INTO gateway_log (ts, event, details)
		VALUES (?, ?, ?)
	`, nowISO(), event, detailsPtr)
	return err
}

// GetUnsyncedStateCount returns count of unsynced state records
func (s *Store) GetUnsyncedStateCount() (int, error) {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM state_log WHERE synced = 0`).Scan(&count)
	return count, err
}

// GetUnsyncedCommandCount returns count of unsynced command records
func (s *Store) GetUnsyncedCommandCount() (int, error) {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM command_log WHERE synced = 0`).Scan(&count)
	return count, err
}

// GetUnsyncedStates returns unsynced state records (for future Supabase sync)
func (s *Store) GetUnsyncedStates(limit int) ([]StateRecord, error) {
	rows, err := s.db.Query(`
		SELECT id, site_id, device_id, seq, ts, voltage, target_rpm, actual_rpm,
		       motor_amps, motor_freq, vfd_temp, wheels_running, paddle_running,
		       position_deg, received_at, synced, synced_at
		FROM state_log
		WHERE synced = 0
		ORDER BY id ASC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []StateRecord
	for rows.Next() {
		var r StateRecord
		err := rows.Scan(&r.ID, &r.SiteID, &r.DeviceID, &r.Seq, &r.Timestamp,
			&r.Voltage, &r.TargetRPM, &r.ActualRPM, &r.MotorAmps, &r.MotorFreq,
			&r.VFDTemp, &r.WheelsRunning, &r.PaddleRunning, &r.PositionDeg,
			&r.ReceivedAt, &r.Synced, &r.SyncedAt)
		if err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

// GetUnsyncedCommands returns unsynced command records (for future Supabase sync)
func (s *Store) GetUnsyncedCommands(limit int) ([]CommandRecord, error) {
	rows, err := s.db.Query(`
		SELECT id, site_id, device_id, ts, source, command_type, payload,
		       user_id, received_at, synced, synced_at
		FROM command_log
		WHERE synced = 0
		ORDER BY id ASC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []CommandRecord
	for rows.Next() {
		var r CommandRecord
		err := rows.Scan(&r.ID, &r.SiteID, &r.DeviceID, &r.Timestamp, &r.Source,
			&r.CommandType, &r.Payload, &r.UserID, &r.ReceivedAt, &r.Synced, &r.SyncedAt)
		if err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

// MarkStatesSynced marks state records as synced
func (s *Store) MarkStatesSynced(ids []int64) error {
	if len(ids) == 0 {
		return nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`UPDATE state_log SET synced = 1, synced_at = ? WHERE id = ?`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	syncedAt := nowISO()
	for _, id := range ids {
		if _, err := stmt.Exec(syncedAt, id); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// MarkCommandsSynced marks command records as synced
func (s *Store) MarkCommandsSynced(ids []int64) error {
	if len(ids) == 0 {
		return nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`UPDATE command_log SET synced = 1, synced_at = ? WHERE id = ?`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	syncedAt := nowISO()
	for _, id := range ids {
		if _, err := stmt.Exec(syncedAt, id); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// PruneOldRecords removes synced records older than the retention period
func (s *Store) PruneOldRecords(retentionDays int) error {
	cutoff := time.Now().AddDate(0, 0, -retentionDays).UTC().Format(time.RFC3339)

	tables := []string{"state_log", "command_log", "fault_log"}
	for _, table := range tables {
		_, err := s.db.Exec(fmt.Sprintf(`
			DELETE FROM %s WHERE synced = 1 AND received_at < ?
		`, table), cutoff)
		if err != nil {
			return fmt.Errorf("prune %s: %w", table, err)
		}
	}

	// Prune gateway log (always prune, no sync needed)
	_, err := s.db.Exec(`DELETE FROM gateway_log WHERE ts < ?`, cutoff)
	if err != nil {
		return fmt.Errorf("prune gateway_log: %w", err)
	}

	// Vacuum to reclaim space
	_, _ = s.db.Exec("VACUUM")

	return nil
}

// Stats returns storage statistics
func (s *Store) Stats() (map[string]int, error) {
	stats := make(map[string]int)

	tables := []struct {
		name       string
		countKey   string
		unsyncKey  string
	}{
		{"state_log", "state_total", "state_unsynced"},
		{"command_log", "command_total", "command_unsynced"},
		{"fault_log", "fault_total", "fault_unsynced"},
		{"gateway_log", "gateway_total", ""},
	}

	for _, t := range tables {
		var total int
		if err := s.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", t.name)).Scan(&total); err != nil {
			return nil, err
		}
		stats[t.countKey] = total

		if t.unsyncKey != "" {
			var unsynced int
			if err := s.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE synced = 0", t.name)).Scan(&unsynced); err != nil {
				return nil, err
			}
			stats[t.unsyncKey] = unsynced
		}
	}

	return stats, nil
}
