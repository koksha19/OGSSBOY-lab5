package integration

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 3 * time.Second,
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	backendSet := make(map[string]struct{})

	for i := 0; i < 10; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err != nil {
			t.Errorf("Request #%d failed: %v", i+1, err)
			continue
		}
		resp.Body.Close()

		from := resp.Header.Get("lb-from")
		if from == "" {
			t.Errorf("Request #%d: missing lb-from header", i+1)
		} else {
			t.Logf("Request #%d: response from [%s]", i+1, from)
			backendSet[from] = struct{}{}
		}
	}

	if len(backendSet) < 2 {
		t.Errorf("Expected requests to be distributed across at least 2 servers, got %d: %v", len(backendSet), keys(backendSet))
	}
}

func BenchmarkBalancer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err != nil {
			b.Fatalf("Request failed: %v", err)
		}
		resp.Body.Close()
	}
}

func keys(m map[string]struct{}) []string {
	result := make([]string, 0, len(m))
	for k := range m {
		result = append(result, k)
	}
	return result
}
