package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Config holds all configuration from environment variables
type Config struct {
	// Network status
	NetworkStatusFile string

	// Local MQTT (on the Pi)
	LocalMQTTURL  string
	LocalMQTTUser string
	LocalMQTTPass string

	// Cloud MQTT (EC2)
	CloudMQTTURL  string
	CloudMQTTUser string
	CloudMQTTPass string

	// Identity
	SiteID   string
	DeviceID string

	// Storage
	SQLitePath    string
	RetentionDays int
}

// Gateway bridges local MQTT to cloud MQTT based on network status
type Gateway struct {
	config      Config
	localClient mqtt.Client
	cloudClient mqtt.Client
	store       *Store

	online   bool
	onlineMu sync.RWMutex

	// Debouncing for network status changes
	consecutiveOnline  int
	consecutiveOffline int
	debounceThreshold  int

	stopCh chan struct{}
}

func env(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func envInt(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		var i int
		if _, err := fmt.Sscanf(v, "%d", &i); err == nil {
			return i
		}
	}
	return defaultVal
}

func loadConfig() Config {
	return Config{
		NetworkStatusFile: env("NETWORK_STATUS_FILE", "/var/run/network-status"),

		LocalMQTTURL:  env("LOCAL_MQTT_URL", "tcp://localhost:1883"),
		LocalMQTTUser: os.Getenv("LOCAL_MQTT_USER"),
		LocalMQTTPass: os.Getenv("LOCAL_MQTT_PASS"),

		CloudMQTTURL:  env("CLOUD_MQTT_URL", "tcp://3.141.116.27:1883"),
		CloudMQTTUser: env("CLOUD_MQTT_USER", "raptor"),
		CloudMQTTPass: env("CLOUD_MQTT_PASS", "raptorMQTT2025"),

		SiteID:   env("RAPTOR_SITE", "shop"),
		DeviceID: env("RAPTOR_DEVICE", "revpi-135593"),

		SQLitePath:    env("SQLITE_PATH", "/var/lib/raptor/gateway.db"),
		RetentionDays: envInt("RETENTION_DAYS", 30),
	}
}

func NewGateway(config Config) *Gateway {
	return &Gateway{
		config:            config,
		stopCh:            make(chan struct{}),
		debounceThreshold: 3, // Require 3 consecutive checks (15s) before acting
	}
}

// checkNetworkStatus reads the network status file
// Returns true if online, false if offline
func (g *Gateway) checkNetworkStatus() bool {
	data, err := os.ReadFile(g.config.NetworkStatusFile)
	if err != nil {
		// If we can't read the file, assume offline
		log.Printf("Warning: cannot read network status file: %v", err)
		return false
	}
	status := strings.TrimSpace(string(data))
	return status == "1"
}

// IsOnline returns current online status (thread-safe)
func (g *Gateway) IsOnline() bool {
	g.onlineMu.RLock()
	defer g.onlineMu.RUnlock()
	return g.online
}

// setOnline updates online status (thread-safe)
func (g *Gateway) setOnline(online bool) {
	g.onlineMu.Lock()
	defer g.onlineMu.Unlock()
	g.online = online
}

// connectLocalMQTT establishes connection to local broker
func (g *Gateway) connectLocalMQTT() error {
	opts := mqtt.NewClientOptions().
		AddBroker(g.config.LocalMQTTURL).
		SetClientID(fmt.Sprintf("raptor-gateway-%s", g.config.DeviceID)).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetOrderMatters(false)

	if g.config.LocalMQTTUser != "" {
		opts.SetUsername(g.config.LocalMQTTUser)
		opts.SetPassword(g.config.LocalMQTTPass)
	}

	opts.SetOnConnectHandler(func(c mqtt.Client) {
		log.Println("Connected to local MQTT broker")
		// Subscribe to all raptor topics
		topic := "raptor/#"
		if token := c.Subscribe(topic, 1, g.handleLocalMessage); token.Wait() && token.Error() != nil {
			log.Printf("Failed to subscribe to %s: %v", topic, token.Error())
		} else {
			log.Printf("Subscribed to %s", topic)
		}
	})

	opts.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		log.Printf("Lost connection to local MQTT: %v", err)
		if g.store != nil {
			g.store.LogGatewayEvent("local_mqtt_disconnect", err.Error())
		}
	})

	g.localClient = mqtt.NewClient(opts)
	if token := g.localClient.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("local MQTT connect: %w", token.Error())
	}

	return nil
}

// connectCloudMQTT establishes connection to cloud broker
func (g *Gateway) connectCloudMQTT() error {
	opts := mqtt.NewClientOptions().
		AddBroker(g.config.CloudMQTTURL).
		SetClientID(fmt.Sprintf("raptor-gateway-%s-cloud", g.config.DeviceID)).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(5 * time.Second).
		SetOrderMatters(false)

	if g.config.CloudMQTTUser != "" {
		opts.SetUsername(g.config.CloudMQTTUser)
		opts.SetPassword(g.config.CloudMQTTPass)
	}

	// Set Last Will and Testament for disconnect detection
	willTopic := fmt.Sprintf("raptor/%s/%s/status", g.config.SiteID, g.config.DeviceID)
	willPayload := `{"online":false,"reason":"unexpected_disconnect"}`
	opts.SetWill(willTopic, willPayload, 1, true)

	opts.SetOnConnectHandler(func(c mqtt.Client) {
		log.Println("Connected to cloud MQTT broker")
		if g.store != nil {
			g.store.LogGatewayEvent("cloud_connect", g.config.CloudMQTTURL)
		}

		// Publish online status
		statusTopic := fmt.Sprintf("raptor/%s/%s/status", g.config.SiteID, g.config.DeviceID)
		statusPayload := `{"online":true}`
		c.Publish(statusTopic, 1, true, statusPayload)

		// Subscribe to cloud commands for bidirectional routing
		cmdTopic := fmt.Sprintf("raptor/%s/%s/cmd", g.config.SiteID, g.config.DeviceID)
		if token := c.Subscribe(cmdTopic, 2, g.handleCloudCommand); token.Wait() && token.Error() != nil {
			log.Printf("Failed to subscribe to cloud commands: %v", token.Error())
		} else {
			log.Printf("Subscribed to cloud commands: %s", cmdTopic)
		}
	})

	opts.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		log.Printf("Lost connection to cloud MQTT: %v", err)
		if g.store != nil {
			g.store.LogGatewayEvent("cloud_disconnect", err.Error())
		}
	})

	g.cloudClient = mqtt.NewClient(opts)
	if token := g.cloudClient.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("cloud MQTT connect: %w", token.Error())
	}

	return nil
}

// handleLocalMessage processes messages from local broker
func (g *Gateway) handleLocalMessage(client mqtt.Client, msg mqtt.Message) {
	topic := msg.Topic()
	payload := msg.Payload()

	// Determine message type from topic
	// raptor/{site}/{device}/state -> state message
	// raptor/{site}/{device}/cmd -> command message (from local HMI)
	// raptor/{site}/{device}/faults -> fault message
	parts := strings.Split(topic, "/")
	msgType := "unknown"
	if len(parts) >= 4 {
		msgType = parts[3]
	}

	log.Printf("Local msg [%s]: %s (%d bytes)", msgType, topic, len(payload))

	// Store locally based on message type
	if g.store != nil {
		var storeErr error
		switch msgType {
		case "state":
			storeErr = g.store.StoreState(payload)
		case "cmd":
			storeErr = g.store.StoreCommand(payload, "local")
		case "faults":
			// Parse fault payload and store
			// For now, just log it
			log.Printf("Fault message received: %s", string(payload))
		default:
			// Store as state for unknown types
			storeErr = g.store.StoreState(payload)
		}
		if storeErr != nil {
			log.Printf("Failed to store message: %v", storeErr)
		}
	}

	// If online, forward to cloud
	if g.IsOnline() && g.cloudClient != nil && g.cloudClient.IsConnected() {
		token := g.cloudClient.Publish(topic, 1, false, payload)
		if token.Wait() && token.Error() != nil {
			log.Printf("Failed to publish to cloud: %v", token.Error())
			// Message is already stored locally, will sync later
		}
	} else {
		log.Printf("Offline - message stored locally for later sync")
	}
}

// handleCloudCommand processes commands from cloud broker (bidirectional routing)
func (g *Gateway) handleCloudCommand(client mqtt.Client, msg mqtt.Message) {
	topic := msg.Topic()
	payload := msg.Payload()

	log.Printf("Cloud cmd: %s (%d bytes)", topic, len(payload))

	// Store the command locally for audit trail
	if g.store != nil {
		if err := g.store.StoreCommand(payload, "cloud"); err != nil {
			log.Printf("Failed to store cloud command: %v", err)
		}
	}

	// Forward to local broker so raptor-core can execute
	if g.localClient != nil && g.localClient.IsConnected() {
		token := g.localClient.Publish(topic, 1, false, payload)
		if token.Wait() && token.Error() != nil {
			log.Printf("Failed to forward cloud command to local: %v", token.Error())
		} else {
			log.Printf("Forwarded cloud command to local broker")
		}
	} else {
		log.Printf("Warning: Cannot forward cloud command - local broker not connected")
	}
}

// networkWatcher monitors network status and manages cloud connection
func (g *Gateway) networkWatcher() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-g.stopCh:
			return
		case <-ticker.C:
			nowOnline := g.checkNetworkStatus()
			wasOnline := g.IsOnline()

			// Debouncing logic: require consistent state for threshold checks
			if nowOnline {
				g.consecutiveOnline++
				g.consecutiveOffline = 0
			} else {
				g.consecutiveOffline++
				g.consecutiveOnline = 0
			}

			// Only act on state change after debounce threshold
			if nowOnline && !wasOnline && g.consecutiveOnline >= g.debounceThreshold {
				log.Printf("Network status changed: ONLINE (stable for %ds)", g.consecutiveOnline*5)
				g.setOnline(true)

				if g.store != nil {
					g.store.LogGatewayEvent("online", fmt.Sprintf("stable_checks=%d", g.consecutiveOnline))
				}

				// Just came online - connect to cloud
				if g.cloudClient == nil || !g.cloudClient.IsConnected() {
					if err := g.connectCloudMQTT(); err != nil {
						log.Printf("Failed to connect to cloud: %v", err)
					}
				}
			} else if !nowOnline && wasOnline && g.consecutiveOffline >= g.debounceThreshold {
				log.Printf("Network status changed: OFFLINE (stable for %ds)", g.consecutiveOffline*5)
				g.setOnline(false)

				if g.store != nil {
					g.store.LogGatewayEvent("offline", fmt.Sprintf("stable_checks=%d", g.consecutiveOffline))
				}

				// Just went offline - disconnect from cloud
				if g.cloudClient != nil && g.cloudClient.IsConnected() {
					g.cloudClient.Disconnect(250)
					log.Println("Disconnected from cloud MQTT")
				}
			}
		}
	}
}

// statsLogger periodically logs storage statistics
func (g *Gateway) statsLogger() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-g.stopCh:
			return
		case <-ticker.C:
			if g.store != nil {
				stats, err := g.store.Stats()
				if err != nil {
					log.Printf("Failed to get storage stats: %v", err)
					continue
				}
				log.Printf("Storage stats: state=%d (unsynced=%d), cmd=%d (unsynced=%d), faults=%d",
					stats["state_total"], stats["state_unsynced"],
					stats["command_total"], stats["command_unsynced"],
					stats["fault_total"])
			}
		}
	}
}

// pruneWorker periodically cleans up old synced records
func (g *Gateway) pruneWorker() {
	// Run once at startup after a delay
	time.Sleep(1 * time.Minute)

	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()

	prune := func() {
		if g.store != nil {
			log.Printf("Pruning records older than %d days...", g.config.RetentionDays)
			if err := g.store.PruneOldRecords(g.config.RetentionDays); err != nil {
				log.Printf("Failed to prune old records: %v", err)
			} else {
				log.Println("Prune complete")
			}
		}
	}

	// Run immediately
	prune()

	for {
		select {
		case <-g.stopCh:
			return
		case <-ticker.C:
			prune()
		}
	}
}

// initStore initializes the SQLite storage
func (g *Gateway) initStore() error {
	// Ensure directory exists
	dir := g.config.SQLitePath[:strings.LastIndex(g.config.SQLitePath, "/")]
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create storage directory: %w", err)
	}

	store, err := NewStore(g.config.SQLitePath, g.config.SiteID, g.config.DeviceID)
	if err != nil {
		return fmt.Errorf("init store: %w", err)
	}
	g.store = store

	// Log startup
	g.store.LogGatewayEvent("startup", fmt.Sprintf("version=1.1.0 site=%s device=%s", g.config.SiteID, g.config.DeviceID))

	return nil
}

// Run starts the gateway
func (g *Gateway) Run() error {
	log.Println("Starting raptor-gateway v1.1.0...")
	log.Printf("Local MQTT: %s", g.config.LocalMQTTURL)
	log.Printf("Cloud MQTT: %s", g.config.CloudMQTTURL)
	log.Printf("Network status file: %s", g.config.NetworkStatusFile)
	log.Printf("SQLite path: %s", g.config.SQLitePath)

	// Initialize SQLite storage
	if err := g.initStore(); err != nil {
		return fmt.Errorf("failed to init storage: %w", err)
	}
	defer g.store.Close()

	// Connect to local broker (always)
	if err := g.connectLocalMQTT(); err != nil {
		return fmt.Errorf("failed to connect to local MQTT: %w", err)
	}

	// Check initial network status (with immediate action, no debounce for startup)
	initialOnline := g.checkNetworkStatus()
	g.setOnline(initialOnline)
	if initialOnline {
		g.consecutiveOnline = g.debounceThreshold // Skip debounce on startup
	} else {
		g.consecutiveOffline = g.debounceThreshold
	}
	log.Printf("Initial network status: online=%v", g.IsOnline())

	// If online, connect to cloud
	if g.IsOnline() {
		if err := g.connectCloudMQTT(); err != nil {
			log.Printf("Warning: failed to connect to cloud MQTT: %v", err)
			// Don't fail - we can still operate locally
		}
	}

	// Start background workers
	go g.networkWatcher()
	go g.statsLogger()
	go g.pruneWorker()

	log.Println("Gateway running. Press Ctrl+C to exit.")

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	close(g.stopCh)

	// Log shutdown
	if g.store != nil {
		g.store.LogGatewayEvent("shutdown", "graceful")
	}

	if g.localClient != nil {
		g.localClient.Disconnect(250)
	}
	if g.cloudClient != nil {
		g.cloudClient.Disconnect(250)
	}

	return nil
}

func main() {
	config := loadConfig()
	gateway := NewGateway(config)

	if err := gateway.Run(); err != nil {
		log.Fatalf("Gateway error: %v", err)
	}
}
