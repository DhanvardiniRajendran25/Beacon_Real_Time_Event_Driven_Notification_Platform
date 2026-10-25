package main

import (
	"flag"
	"log"
	"time"
)

type Config struct {
	ALBURL         string
	IngestionURL   string
	SenderUser     string
	FollowerPrefix string
	FollowerStart  int
	FollowerCount  int
	EventRate     float64
	Duration      time.Duration
	CSVOut        string
	MetricsPort   int
	EventTimeout  time.Duration
}

func parseConfig() Config {
	var c Config
	flag.StringVar(&c.ALBURL, "alb-url", "", "ALB WebSocket URL, e.g. ws://alb.example.com/ws (required)")
	flag.StringVar(&c.IngestionURL, "ingestion-url", "", "Ingestion API base URL, e.g. http://api.example.com (required)")
	flag.StringVar(&c.SenderUser, "sender-user", "", "from_user value for all posted events (required)")
	flag.StringVar(&c.FollowerPrefix, "follower-prefix", "", "String prefix for user IDs, e.g. 'follower_d_' produces follower_d_1, follower_d_2, ...")
	flag.IntVar(&c.FollowerStart, "follower-start", 1, "First user ID number in the subscriber range")
	flag.IntVar(&c.FollowerCount, "follower-count", 5000, "Number of WebSocket connections to open")
	flag.Float64Var(&c.EventRate, "event-rate", 1.0, "Events to POST per second")
	flag.DurationVar(&c.Duration, "duration", 60*time.Second, "How long to run the test")
	flag.StringVar(&c.CSVOut, "csv-out", "results.csv", "Path to write CSV output")
	flag.IntVar(&c.MetricsPort, "metrics-port", 9090, "Port for the live metrics HTTP endpoint")
	flag.DurationVar(&c.EventTimeout, "event-timeout", 30*time.Second, "Max time to wait for all subscribers to receive a single event")
	flag.Parse()

	if c.ALBURL == "" || c.IngestionURL == "" || c.SenderUser == "" {
		log.Fatal("--alb-url, --ingestion-url, and --sender-user are required")
	}
	if c.EventRate <= 0 {
		log.Fatal("--event-rate must be > 0")
	}
	if c.FollowerCount <= 0 {
		log.Fatal("--follower-count must be > 0")
	}
	return c
}
