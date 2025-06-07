package main

import (
	"testing"
)

func TestBalancer_RoundRobinWithHealth(t *testing.T) {
	backends = []backend{
		{address: "server1:8080", available: true},
		{address: "server2:8080", available: true},
		{address: "server3:8080", available: true},
	}
	currentIndex = 0

	addr, ok := getNextAvailableBackend()
	if !ok || addr != "server1:8080" {
		t.Errorf("expected server1, got %s", addr)
	}

	addr, ok = getNextAvailableBackend()
	if !ok || addr != "server2:8080" {
		t.Errorf("expected server2, got %s", addr)
	}

	addr, ok = getNextAvailableBackend()
	if !ok || addr != "server3:8080" {
		t.Errorf("expected server3, got %s", addr)
	}

	addr, ok = getNextAvailableBackend()
	if !ok || addr != "server1:8080" {
		t.Errorf("expected server1 again, got %s", addr)
	}
}

func TestBalancer_SkipUnavailable(t *testing.T) {
	backends = []backend{
		{address: "server1:8080", available: false},
		{address: "server2:8080", available: true},
		{address: "server3:8080", available: true},
	}
	currentIndex = 0

	addr, ok := getNextAvailableBackend()
	if !ok || addr != "server2:8080" {
		t.Errorf("expected server2 (server1 is down), got %s", addr)
	}

	addr, ok = getNextAvailableBackend()
	if !ok || addr != "server3:8080" {
		t.Errorf("expected server3, got %s", addr)
	}

	addr, ok = getNextAvailableBackend()
	if !ok || addr != "server2:8080" {
		t.Errorf("expected server2 again, got %s", addr)
	}
}

func TestBalancer_NoAvailableServers(t *testing.T) {
	backends = []backend{
		{address: "server1:8080", available: false},
		{address: "server2:8080", available: false},
		{address: "server3:8080", available: false},
	}
	currentIndex = 0

	addr, ok := getNextAvailableBackend()
	if ok {
		t.Errorf("expected no available backends, got %s", addr)
	}
}
