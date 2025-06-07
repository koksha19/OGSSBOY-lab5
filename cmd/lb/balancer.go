package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/koksha19/OGSSBOY-lab5/httptools"
	"github.com/koksha19/OGSSBOY-lab5/signal"
)

var (
	port       = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https      = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

var (
	timeout     time.Duration
	serversPool = []string{
		"server1:8080",
		"server2:8080",
		"server3:8080",
	}
)

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func health(dst string) bool {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func forward(dst string, rw http.ResponseWriter, r *http.Request) error {
	ctx, _ := context.WithTimeout(r.Context(), timeout)
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = dst

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err == nil {
		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		if *traceEnabled {
			rw.Header().Set("lb-from", dst)
		}
		log.Println("fwd", resp.StatusCode, resp.Request.URL)
		rw.WriteHeader(resp.StatusCode)
		defer resp.Body.Close()
		_, err := io.Copy(rw, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		}
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

type backend struct {
	address   string
	available bool
}

var (
	backends      []backend
	backendsMutex sync.RWMutex
	currentIndex  int
)

func updateHealthStatuses() {
	for i := range backends {
		addr := backends[i].address
		ok := health(addr)
		backendsMutex.Lock()
		backends[i].available = ok
		backendsMutex.Unlock()
		log.Println(addr, "healthy:", ok)
	}
}

func getNextAvailableBackend() (string, bool) {
	backendsMutex.RLock()
	defer backendsMutex.RUnlock()

	n := len(backends)
	for i := 0; i < n; i++ {
		idx := (currentIndex + i) % n
		if backends[idx].available {
			currentIndex = (idx + 1) % n
			return backends[idx].address, true
		}
	}
	return "", false
}

func main() {
	flag.Parse()
	timeout = time.Duration(*timeoutSec) * time.Second

	for _, s := range serversPool {
		backends = append(backends, backend{address: s, available: true})
	}

	go func() {
		for range time.Tick(10 * time.Second) {
			updateHealthStatuses()
		}
	}()

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		addr, ok := getNextAvailableBackend()
		if !ok {
			http.Error(rw, "No available backends", http.StatusServiceUnavailable)
			return
		}
		forward(addr, rw, r)
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}
