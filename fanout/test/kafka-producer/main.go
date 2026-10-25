package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"

	"github.com/Tanyayya/NotificationSystem/fanout/internal/config"
)

const loadRampDuration = 30 * time.Second

type publishRequest struct {
	ID        int64 `json:"id"`
	Type      string `json:"type" binding:"required"`
	Detail    string `json:"detail" binding:"required"`
	Timestamp int64 `json:"timestamp"`
	Key       string `json:"key,omitempty"`
}

// runLoadRamp sends messages for loadRampDuration with instantaneous rate λ(t) = peak * (t/T),
// t in seconds from 0 to T, so cumulative count ≈ peak * t² / (2T). Returns total messages sent.
func runLoadRamp(ctx context.Context, w *kafka.Writer, peak float64, runID string) (sent int64, err error) {
	T := loadRampDuration.Seconds()
	totalExpected := int64(math.Floor(peak * T * T / (2 * T)))

	start := time.Now()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return sent, ctx.Err()
		case <-ticker.C:
			elapsed := time.Since(start)
			var tSec float64
			if elapsed >= loadRampDuration {
				tSec = T
			} else {
				tSec = elapsed.Seconds()
			}

			expected := peak * tSec * tSec / (2 * T)
			target := int64(math.Floor(expected))
			if elapsed >= loadRampDuration {
				target = totalExpected
			}

			need := target - sent
			if need <= 0 {
				if elapsed >= loadRampDuration {
					return sent, nil
				}
				continue
			}

			msgs := make([]kafka.Message, 0, need)
			for i := int64(0); i < need; i++ {
				seq := sent + i + 1
				ts := time.Now().UnixMilli()
				body, mErr := json.Marshal(struct {
					ID        int64  `json:"id"`
					Type      string `json:"type"`
					Detail    string `json:"detail"`
					Timestamp int64  `json:"timestamp"`
				}{
					ID:        time.Now().UnixNano() + seq,
					Type:      "Post",
					Detail:    fmt.Sprintf("load ramp run=%s seq=%d elapsed_ms=%d", runID, seq, time.Since(start).Milliseconds()),
					Timestamp: ts,
				})
				if mErr != nil {
					return sent, fmt.Errorf("encode ramp payload: %w", mErr)
				}
				msgs = append(msgs, kafka.Message{
					Key:   []byte(strconv.FormatInt(seq, 10)),
					Value: body,
				})
			}

			if err := w.WriteMessages(ctx, msgs...); err != nil {
				return sent, err
			}
			sent += need

			if elapsed >= loadRampDuration {
				return sent, nil
			}
		}
	}
}

func main() {
	cfg := config.LoadProducer()
	gin.SetMode(cfg.GinMode)

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      cfg.Brokers,
		Topic:        cfg.Topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: 1,
		Async:        false,
	})
	defer func() {
		if err := w.Close(); err != nil {
			log.Printf("kafka writer close: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	router := gin.New()
	router.Use(gin.Recovery())

	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	router.POST("/messages", func(c *gin.Context) {
		var req publishRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		id := req.ID
		if id == 0 {
			id = time.Now().UnixNano()
		}
		ts := req.Timestamp
		if ts == 0 {
			ts = time.Now().UnixMilli()
		}
		body, err := json.Marshal(struct {
			ID        int64  `json:"id"`
			Type      string `json:"type"`
			Detail    string `json:"detail"`
			Timestamp int64  `json:"timestamp"`
		}{
			ID:        id,
			Type:      req.Type,
			Detail:    req.Detail,
			Timestamp: ts,
		})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "encode payload"})
			return
		}

		msg := kafka.Message{Value: body}
		if req.Key != "" {
			msg.Key = []byte(req.Key)
		}

		writeCtx := c.Request.Context()
		if err := w.WriteMessages(writeCtx, msg); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("kafka write: %v", err)
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusAccepted, gin.H{"status": "published"})
	})

	router.POST("/load/ramp", func(c *gin.Context) {
		peak := float64(cfg.PeakLoadMsgPerSec)
		if q := c.Query("peak_mps"); q != "" {
			v, err := strconv.Atoi(q)
			if err != nil || v <= 0 {
				c.JSON(http.StatusBadRequest, gin.H{"error": "peak_mps must be a positive integer"})
				return
			}
			peak = float64(v)
		}
		if peak <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "peak messages per second must be positive"})
			return
		}

		runID := strconv.FormatInt(time.Now().UnixNano(), 10)
		n, err := runLoadRamp(c.Request.Context(), w, peak, runID)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				c.JSON(http.StatusOK, gin.H{
					"status":       "canceled",
					"sent":         n,
					"duration_sec": int(loadRampDuration / time.Second),
					"peak_mps":     peak,
				})
				return
			}
			log.Printf("load ramp: %v", err)
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error(), "sent": n})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"status":       "complete",
			"sent":         n,
			"duration_sec": int(loadRampDuration / time.Second),
			"peak_mps":     peak,
		})
	})

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       5 * time.Minute,
		WriteTimeout:      5 * time.Minute,
	}

	go func() {
		log.Printf("kafka-producer http listening on %s (topic=%q)", cfg.HTTPAddr, cfg.Topic)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	log.Println("shutdown complete")
}
