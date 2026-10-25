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
)

func main() {
	addr := getEnv("HTTP_ADDR", ":8082")
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	ginMode := getEnv("GIN_MODE", "release")
	dbURL := getEnv("DATABASE_URL", "")

	gin.SetMode(ginMode)

	store := NewStore(redisAddr)
	defer func() {
		if err := store.Close(); err != nil {
			log.Printf("redis close: %v", err)
		}
	}()

	var db *PGStore
	if dbURL != "" {
		var err error
		db, err = NewPGStore(context.Background(), dbURL)
		if err != nil {
			log.Printf("postgres unavailable, running without fallback: %v", err)
		} else {
			defer db.Close()
			log.Println("postgres fallback enabled")
		}
	}

	h := NewHandler(store, db)

	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/notifications", h.GetNotifications)
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("readapi listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server: %v", err)
		}
	}()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
		os.Exit(1)
	}
	log.Println("shutdown complete")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
