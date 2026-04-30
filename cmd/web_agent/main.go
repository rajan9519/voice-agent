package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"

	"voiceagent/internal/ws"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  16384,
	WriteBufferSize: 16384,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins in development; restrict in production.
	},
}

func main() {
	_ = godotenv.Load()

	addr := flag.String("addr", ":8080", "Listen address (host:port)")
	logFile := flag.String("log-file", "", "Path to log file (logs to stdout only if empty)")
	flag.Parse()

	if *logFile != "" {
		f, err := os.OpenFile(*logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("[server] open log file: %v", err)
		}
		defer f.Close()
		log.SetOutput(io.MultiWriter(os.Stdout, f))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mux := http.NewServeMux()

	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("[server] upgrade error: %v", err)
			return
		}
		log.Printf("[server] new connection from %s", r.RemoteAddr)
		go ws.HandleConnection(ctx, conn)
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, `{"status":"ok"}`)
	})

	// Serve static files if the directory exists.
	staticDir := "cmd/web_agent/static"
	if info, err := os.Stat(staticDir); err == nil && info.IsDir() {
		mux.Handle("/", http.FileServer(http.Dir(staticDir)))
	}

	server := &http.Server{
		Addr:    *addr,
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		log.Println("[server] shutting down...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("[server] shutdown error: %v", err)
		}
	}()

	log.Printf("[server] listening on %s", *addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("[server] fatal: %v", err)
	}
}
