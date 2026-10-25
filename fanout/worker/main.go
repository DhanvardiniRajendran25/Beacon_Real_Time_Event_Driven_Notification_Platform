package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Tanyayya/NotificationSystem/fanout/internal/config"
	"github.com/Tanyayya/NotificationSystem/fanout/internal/consumer"
	"github.com/Tanyayya/NotificationSystem/fanout/internal/db"
	"github.com/Tanyayya/NotificationSystem/fanout/internal/fanout"
	"github.com/Tanyayya/NotificationSystem/fanout/internal/notif"
)

func main() {
	cfg := config.Load()
	gin.SetMode(cfg.GinMode)

	// Redis publisher — sends notifications to connected clients via Pub/Sub
	pub := notif.NewPublisher(cfg.RedisAddr, cfg.NotifyType, cfg.NotifyFromUser, cfg.NotifyMessage)
	defer func() {
		if err := pub.Close(); err != nil {
			log.Printf("redis close: %v", err)
		}
	}()

	// PostgreSQL — persists notifications for offline users and stores follower graph
	database, err := db.New(cfg.DBDSN)
	if err != nil {
		log.Fatalf("postgres connect: %v", err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			log.Printf("postgres close: %v", err)
		}
	}()

	// FanOuter — wraps publisher + DB, implements consumer.Notifier
	// Switches between fan-out on write and fan-out on read based on follower count
	fo := fanout.New(database, pub, cfg.FanoutThreshold)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("http listening on %s", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server: %v", err)
		}
	}()

	// Pass FanOuter as the Notifier — consumer.Run calls fo.Publish per Kafka message
	consumerDone := make(chan error, 1)
	go func() {
		consumerDone <- consumer.Run(ctx, cfg.Brokers, cfg.Topic, cfg.GroupID, cfg.NotifyDefaultUserID, fo)
	}()

	<-ctx.Done()
	log.Println("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}

	if err := <-consumerDone; err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("consumer stopped with error: %v", err)
		os.Exit(1)
	}
	log.Println("shutdown complete")
}