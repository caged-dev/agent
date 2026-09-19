package main

import (
	"testing"
	"time"
)

func TestEnvBoolOrDefault(t *testing.T) {
	// File watching defaults to ON, so a typo in an operator's env must not
	// silently turn observation off. Only the explicit falsey spellings do.
	cases := map[string]bool{
		"":      true,
		"0":     false,
		"false": false,
		"FALSE": false,
		"no":    false,
		"off":   false,
		"1":     true,
		"true":  true,
		"yes":   true,
	}
	for value, want := range cases {
		t.Run("value="+value, func(t *testing.T) {
			if value != "" {
				t.Setenv("CAGED_TEST_BOOL", value)
			}
			if got := envBoolOrDefault("CAGED_TEST_BOOL", true); got != want {
				t.Errorf("envBoolOrDefault(%q) = %v, want %v", value, got, want)
			}
		})
	}
}

func TestEnvIntOrDefault(t *testing.T) {
	if got := envIntOrDefault("CAGED_TEST_INT_UNSET", 200); got != 200 {
		t.Errorf("unset = %d, want 200", got)
	}
	t.Setenv("CAGED_TEST_INT", "50")
	if got := envIntOrDefault("CAGED_TEST_INT", 200); got != 50 {
		t.Errorf("set = %d, want 50", got)
	}
	t.Setenv("CAGED_TEST_INT", "not-a-number")
	if got := envIntOrDefault("CAGED_TEST_INT", 200); got != 200 {
		t.Errorf("garbage = %d, want the default 200", got)
	}
}

func TestEnvOrDefault(t *testing.T) {
	if got := envOrDefault("CAGED_TEST_STR_UNSET", "/workspace"); got != "/workspace" {
		t.Errorf("unset = %q", got)
	}
	t.Setenv("CAGED_TEST_STR", "/srv")
	if got := envOrDefault("CAGED_TEST_STR", "/workspace"); got != "/srv" {
		t.Errorf("set = %q", got)
	}
}

func TestEnvDurationOrDefault(t *testing.T) {
	if got := envDurationOrDefault("CAGED_TEST_DUR_UNSET", time.Second); got != time.Second {
		t.Errorf("unset = %v", got)
	}
	t.Setenv("CAGED_TEST_DUR", "250ms")
	if got := envDurationOrDefault("CAGED_TEST_DUR", time.Second); got != 250*time.Millisecond {
		t.Errorf("set = %v", got)
	}
	t.Setenv("CAGED_TEST_DUR", "nonsense")
	if got := envDurationOrDefault("CAGED_TEST_DUR", time.Second); got != time.Second {
		t.Errorf("garbage = %v, want the default", got)
	}
}
