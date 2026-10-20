package safeerr

import (
	"errors"
	"testing"
)

func TestStringRedactsUserinfo(t *testing.T) {
	in := `dial amqp://monitor:SuperSecret@broker:5672/%2F: connection refused`
	out := String(in)
	if contains(out, "SuperSecret") {
		t.Fatalf("password leaked: %s", out)
	}
	if !contains(out, "***") {
		t.Fatalf("expected redaction: %s", out)
	}
}

func TestErrNil(t *testing.T) {
	if Err(nil) != "" {
		t.Fatal("expected empty")
	}
	if !contains(Err(errors.New("password=abc123 failed")), "***") {
		t.Fatal("expected kv redaction")
	}
}

func TestSanitizeSourceError(t *testing.T) {
	in := `/api/overview: HTTP 401: {"error":"not_authorised","reason":"no access"}`
	out := SanitizeSourceError(in)
	if contains(out, "not_authorised") || contains(out, "no access") {
		t.Fatalf("body leaked: %s", out)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
