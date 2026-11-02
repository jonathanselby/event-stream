package eventstream

import (
	"os"
	"strings"
	"testing"
)

func TestVersionMatchesFile(t *testing.T) {
	data, err := os.ReadFile("VERSION")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(data)); got != Version {
		t.Fatalf("VERSION=%q package=%q", got, Version)
	}
}
