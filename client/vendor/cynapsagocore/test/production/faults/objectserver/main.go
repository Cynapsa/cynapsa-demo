// Command objectserver is a disposable bounded HTTP object endpoint used only
// by the production fault-injection environment.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

const maximumObjectBytes = 1 << 20

type objectStore struct {
	mu    sync.RWMutex
	value []byte
}

func main() {
	listen := flag.String("listen", ":8080", "HTTP listen address")
	flag.Parse()

	store := &objectStore{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ready", func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "text/plain")
		response.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(response, "ready\n")
	})
	mux.HandleFunc("PUT /object", store.put)
	mux.HandleFunc("GET /object", store.get)

	server := &http.Server{
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       5 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	log.Printf("fault object endpoint listening on %s", *listen)
	if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func (store *objectStore) put(response http.ResponseWriter, request *http.Request) {
	if request.ContentLength <= 0 || request.ContentLength > maximumObjectBytes {
		http.Error(response, "invalid size", http.StatusRequestEntityTooLarge)
		return
	}
	value, err := io.ReadAll(io.LimitReader(request.Body, maximumObjectBytes+1))
	if err != nil || int64(len(value)) != request.ContentLength || len(value) > maximumObjectBytes {
		zero(value)
		http.Error(response, "invalid body", http.StatusBadRequest)
		return
	}
	store.mu.Lock()
	zero(store.value)
	store.value = append(store.value[:0], value...)
	store.mu.Unlock()
	zero(value)
	response.WriteHeader(http.StatusCreated)
}

func (store *objectStore) get(response http.ResponseWriter, _ *http.Request) {
	store.mu.RLock()
	value := append([]byte(nil), store.value...)
	store.mu.RUnlock()
	defer zero(value)
	if len(value) == 0 {
		http.Error(response, "object unavailable", http.StatusNotFound)
		return
	}
	response.Header().Set("Content-Type", "application/octet-stream")
	response.Header().Set("Content-Length", fmt.Sprintf("%d", len(value)))
	response.WriteHeader(http.StatusOK)
	_, _ = response.Write(value)
}

func zero(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
