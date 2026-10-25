package config

import (
	"os"
	"strconv"
	"strings"
)

// Config holds runtime settings loaded from the environment.
type Config struct {
	HTTPAddr            string
	Brokers             []string
	Topic               string
	GroupID             string
	GinMode             string
	RedisAddr           string
	NotifyDefaultUserID string
	NotifyType          string
	NotifyFromUser      string
	NotifyMessage       string
	// DB_DSN is the PostgreSQL connection string.
	// Example: postgres://notif:notif@localhost:5432/notifications?sslmode=disable
	DBDSN string
	// FanoutThreshold is the follower count above which fan-out on read is used.
	// Accounts with followers <= threshold use fan-out on write.
	// Accounts with followers > threshold use fan-out on read (stub in Week 2).
	FanoutThreshold int
	// NotificationMode controls the fan-out strategy: FAN_OUT_READ, FAN_OUT_WRITE, or HYBRID.
	// HYBRID uses FanoutThreshold to switch between write and read paths.
	NotificationMode string
}

// ProducerConfig holds producer-only settings.
type ProducerConfig struct {
	HTTPAddr          string
	Brokers           []string
	Topic             string
	GinMode           string
	PeakLoadMsgPerSec int
}

// Load reads configuration from environment variables with sensible defaults.
func Load() Config {
	return Config{
		HTTPAddr:            getEnv("HTTP_ADDR", ":8080"),
		Brokers:             parseBrokers(),
		Topic:               getEnv("KAFKA_TOPIC", "worker-events"),
		GroupID:             getEnv("KAFKA_GROUP_ID", "worker-skeleton"),
		GinMode:             getEnv("GIN_MODE", "release"),
		RedisAddr:           getEnv("REDIS_ADDR", "localhost:6379"),
		NotifyDefaultUserID: getEnv("NOTIFY_DEFAULT_USER_ID", "default"),
		NotifyType:          getEnv("NOTIFY_TYPE", "new_post"),
		NotifyFromUser:      getEnv("NOTIFY_FROM_USER", "alice"),
		NotifyMessage:       getEnv("NOTIFY_MESSAGE", "Alice posted a photo"),
		DBDSN:               getEnv("DB_DSN", "postgres://notif:notif@localhost:5432/notifications?sslmode=disable"),
		FanoutThreshold:     getEnvInt("FANOUT_THRESHOLD", 1000),
		NotificationMode:    getEnv("NOTIFICATION_MODE", "FAN_OUT_HYBRID"),
	}
}

// LoadProducer reads producer configuration from the environment.
func LoadProducer() ProducerConfig {
	return ProducerConfig{
		HTTPAddr:          getEnv("HTTP_ADDR", ":8081"),
		Brokers:           parseBrokers(),
		Topic:             getEnv("KAFKA_TOPIC", "worker-events"),
		GinMode:           getEnv("GIN_MODE", "release"),
		PeakLoadMsgPerSec: getEnvInt("LOAD_PEAK_MSG_PER_SEC", 20),
	}
}

func parseBrokers() []string {
	brokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}
	return brokers
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	s := strings.TrimSpace(os.Getenv(key))
	if s == "" {
		return fallback
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return fallback
	}
	return v
}