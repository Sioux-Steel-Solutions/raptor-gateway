package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

// SupabaseSync handles syncing data to Supabase
type SupabaseSync struct {
	url        string
	apiKey     string
	httpClient *http.Client
	batchSize  int
}

// SupabaseTelemetry represents a telemetry record for Supabase
type SupabaseTelemetry struct {
	SiteID     string                 `json:"site_id"`
	DeviceID   string                 `json:"device_id"`
	EdgeID     int64                  `json:"edge_id"`
	EdgeSeq    int64                  `json:"edge_seq"`
	Ts         string                 `json:"ts"`
	Data       map[string]interface{} `json:"data"`
	ReceivedAt string                 `json:"received_at"`
}

// SupabaseCommand represents a command record for Supabase
type SupabaseCommand struct {
	SiteID      string                 `json:"site_id"`
	DeviceID    string                 `json:"device_id"`
	EdgeID      int64                  `json:"edge_id"`
	Ts          string                 `json:"ts"`
	Source      string                 `json:"source"`
	CommandType string                 `json:"command_type"`
	Data        map[string]interface{} `json:"data"`
	UserID      *string                `json:"user_id,omitempty"`
	ReceivedAt  string                 `json:"received_at"`
}

// NewSupabaseSync creates a new Supabase sync client
func NewSupabaseSync(url, apiKey string, batchSize int) *SupabaseSync {
	return &SupabaseSync{
		url:    url,
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		batchSize: batchSize,
	}
}

// SyncTelemetry syncs unsynced telemetry records to Supabase
func (s *SupabaseSync) SyncTelemetry(store *Store) (int, error) {
	records, err := store.GetUnsyncedStates(s.batchSize)
	if err != nil {
		return 0, fmt.Errorf("get unsynced states: %w", err)
	}

	if len(records) == 0 {
		return 0, nil
	}

	// Convert to Supabase format
	supaRecords := make([]SupabaseTelemetry, len(records))
	ids := make([]int64, len(records))

	for i, r := range records {
		data := map[string]interface{}{}
		if r.Voltage != nil {
			data["voltage"] = *r.Voltage
		}
		if r.TargetRPM != nil {
			data["target_rpm"] = *r.TargetRPM
		}
		if r.ActualRPM != nil {
			data["actual_rpm"] = *r.ActualRPM
		}
		if r.MotorAmps != nil {
			data["motor_amps"] = *r.MotorAmps
		}
		if r.MotorFreq != nil {
			data["motor_freq"] = *r.MotorFreq
		}
		if r.VFDTemp != nil {
			data["vfd_temp"] = *r.VFDTemp
		}
		if r.WheelsRunning != nil {
			data["wheels_running"] = *r.WheelsRunning == 1
		}
		if r.PaddleRunning != nil {
			data["paddle_running"] = *r.PaddleRunning == 1
		}
		if r.PositionDeg != nil {
			data["position_deg"] = *r.PositionDeg
		}

		supaRecords[i] = SupabaseTelemetry{
			SiteID:     r.SiteID,
			DeviceID:   r.DeviceID,
			EdgeID:     r.ID,
			EdgeSeq:    r.Seq,
			Ts:         r.Timestamp,
			Data:       data,
			ReceivedAt: r.ReceivedAt,
		}
		ids[i] = r.ID
	}

	// POST to Supabase
	if err := s.postToSupabase("telemetry", supaRecords); err != nil {
		return 0, fmt.Errorf("post telemetry: %w", err)
	}

	// Mark as synced
	if err := store.MarkStatesSynced(ids); err != nil {
		return 0, fmt.Errorf("mark synced: %w", err)
	}

	return len(records), nil
}

// SyncCommands syncs unsynced command records to Supabase
func (s *SupabaseSync) SyncCommands(store *Store) (int, error) {
	records, err := store.GetUnsyncedCommands(s.batchSize)
	if err != nil {
		return 0, fmt.Errorf("get unsynced commands: %w", err)
	}

	if len(records) == 0 {
		return 0, nil
	}

	// Convert to Supabase format
	supaRecords := make([]SupabaseCommand, len(records))
	ids := make([]int64, len(records))

	for i, r := range records {
		// Parse payload JSON
		var data map[string]interface{}
		if err := json.Unmarshal([]byte(r.Payload), &data); err != nil {
			data = map[string]interface{}{"raw": r.Payload}
		}

		supaRecords[i] = SupabaseCommand{
			SiteID:      r.SiteID,
			DeviceID:    r.DeviceID,
			EdgeID:      r.ID,
			Ts:          r.Timestamp,
			Source:      r.Source,
			CommandType: r.CommandType,
			Data:        data,
			UserID:      r.UserID,
			ReceivedAt:  r.ReceivedAt,
		}
		ids[i] = r.ID
	}

	// POST to Supabase
	if err := s.postToSupabase("commands", supaRecords); err != nil {
		return 0, fmt.Errorf("post commands: %w", err)
	}

	// Mark as synced
	if err := store.MarkCommandsSynced(ids); err != nil {
		return 0, fmt.Errorf("mark synced: %w", err)
	}

	return len(records), nil
}

// postToSupabase posts records to a Supabase table
func (s *SupabaseSync) postToSupabase(table string, records interface{}) error {
	body, err := json.Marshal(records)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	url := fmt.Sprintf("%s/rest/v1/%s", s.url, table)
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("apikey", s.apiKey)
	req.Header.Set("Authorization", "Bearer "+s.apiKey)
	// Merge duplicates on conflict (upsert behavior)
	req.Header.Set("Prefer", "resolution=merge-duplicates")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http post: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("supabase error %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// syncWorker runs the periodic sync process
func (g *Gateway) syncWorker() {
	if g.supabase == nil {
		log.Println("Supabase sync disabled (no URL configured)")
		return
	}

	// Wait for initial startup
	time.Sleep(10 * time.Second)

	ticker := time.NewTicker(time.Duration(g.config.SyncIntervalSec) * time.Second)
	defer ticker.Stop()

	sync := func() {
		if !g.IsOnline() {
			return
		}

		// Sync telemetry
		telemetrySynced, err := g.supabase.SyncTelemetry(g.store)
		if err != nil {
			log.Printf("Telemetry sync error: %v", err)
		} else if telemetrySynced > 0 {
			log.Printf("Synced %d telemetry records to Supabase", telemetrySynced)
		}

		// Sync commands
		commandsSynced, err := g.supabase.SyncCommands(g.store)
		if err != nil {
			log.Printf("Commands sync error: %v", err)
		} else if commandsSynced > 0 {
			log.Printf("Synced %d command records to Supabase", commandsSynced)
		}
	}

	// Run immediately
	sync()

	for {
		select {
		case <-g.stopCh:
			return
		case <-ticker.C:
			sync()
		}
	}
}
