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
}

// Gateway bridges local MQTT to cloud MQTT based on network status
type Gateway struct {
	config      Config
	localClient mqtt.Client
	cloudClient mqtt.Client

	online   bool
	onlineMu sync.RWMutex

	stopCh chan struct{}
}

func env(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
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
	}
}

func NewGateway(config Config) *Gateway {
	return &Gateway{
		config: config,
		stopCh: make(chan struct{}),
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

	opts.SetOnConnectHandler(func(c mqtt.Client) {
		log.Println("Connected to cloud MQTT broker")
	})

	opts.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		log.Printf("Lost connection to cloud MQTT: %v", err)
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

	log.Printf("Local msg: %s", topic)

	// If online, forward to cloud
	if g.IsOnline() && g.cloudClient != nil && g.cloudClient.IsConnected() {
		token := g.cloudClient.Publish(topic, 1, false, payload)
		if token.Wait() && token.Error() != nil {
			log.Printf("Failed to publish to cloud: %v", token.Error())
			// TODO: Store in SQLite for later sync
		}
	} else {
		// TODO: Store in SQLite for later sync
		log.Printf("Offline - would store message for later sync")
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
			wasOnline := g.IsOnline()
			nowOnline := g.checkNetworkStatus()

			if nowOnline != wasOnline {
				log.Printf("Network status changed: online=%v", nowOnline)
				g.setOnline(nowOnline)

				if nowOnline {
					// Just came online - connect to cloud
					if g.cloudClient == nil || !g.cloudClient.IsConnected() {
						if err := g.connectCloudMQTT(); err != nil {
							log.Printf("Failed to connect to cloud: %v", err)
						}
					}
					// TODO: Sync offline records to Supabase
				} else {
					// Just went offline - disconnect from cloud (optional)
					if g.cloudClient != nil && g.cloudClient.IsConnected() {
						g.cloudClient.Disconnect(250)
						log.Println("Disconnected from cloud MQTT")
					}
				}
			}
		}
	}
}

// Run starts the gateway
func (g *Gateway) Run() error {
	log.Println("Starting raptor-gateway...")
	log.Printf("Local MQTT: %s", g.config.LocalMQTTURL)
	log.Printf("Cloud MQTT: %s", g.config.CloudMQTTURL)
	log.Printf("Network status file: %s", g.config.NetworkStatusFile)

	// Connect to local broker (always)
	if err := g.connectLocalMQTT(); err != nil {
		return fmt.Errorf("failed to connect to local MQTT: %w", err)
	}

	// Check initial network status
	g.setOnline(g.checkNetworkStatus())
	log.Printf("Initial network status: online=%v", g.IsOnline())

	// If online, connect to cloud
	if g.IsOnline() {
		if err := g.connectCloudMQTT(); err != nil {
			log.Printf("Warning: failed to connect to cloud MQTT: %v", err)
			// Don't fail - we can still operate locally
		}
	}

	// Start network watcher
	go g.networkWatcher()

	log.Println("Gateway running. Press Ctrl+C to exit.")

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	close(g.stopCh)

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
