package collector

import (
	"encoding/json"
	"testing"
)

func TestGhUserEvent_Unmarshal(t *testing.T) {
	raw := `{
		"id": "12345678901",
		"type": "PushEvent",
		"repo": {
			"id": 42,
			"name": "octocat/Hello-World",
			"url": "https://api.github.com/repos/octocat/Hello-World"
		},
		"created_at": "2024-01-15T10:30:00Z"
	}`

	var event ghUserEvent
	if err := json.Unmarshal([]byte(raw), &event); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if event.ID != 12345678901 {
		t.Errorf("ID = %d, want 12345678901", event.ID)
	}
	if event.Type != "PushEvent" {
		t.Errorf("Type = %q, want %q", event.Type, "PushEvent")
	}
	if event.Repo.ID != 42 {
		t.Errorf("Repo.ID = %d, want 42", event.Repo.ID)
	}
	if event.Repo.Name != "octocat/Hello-World" {
		t.Errorf("Repo.Name = %q", event.Repo.Name)
	}
	if event.Repo.URL != "https://api.github.com/repos/octocat/Hello-World" {
		t.Errorf("Repo.URL = %q", event.Repo.URL)
	}
	if event.CreatedAt != "2024-01-15T10:30:00Z" {
		t.Errorf("CreatedAt = %q", event.CreatedAt)
	}
}

func TestGhUserEvent_EmptyRepo(t *testing.T) {
	raw := `{
		"id": "999",
		"type": "WatchEvent",
		"repo": {},
		"created_at": "2024-01-01T00:00:00Z"
	}`

	var event ghUserEvent
	if err := json.Unmarshal([]byte(raw), &event); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Empty repo should be skipped by the worker (URL and Name check).
	if event.Repo.URL != "" {
		t.Errorf("expected empty URL, got %q", event.Repo.URL)
	}
	if event.Repo.Name != "" {
		t.Errorf("expected empty Name, got %q", event.Repo.Name)
	}
}

func TestGhUserEvent_StringID(t *testing.T) {
	// GitHub events API returns id as a string, not an integer.
	raw := `{"id": "98765432109", "type": "CreateEvent", "repo": {"id": 1, "name": "a/b", "url": "https://api.github.com/repos/a/b"}, "created_at": "2024-06-01T00:00:00Z"}`

	var event ghUserEvent
	if err := json.Unmarshal([]byte(raw), &event); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if event.ID != 98765432109 {
		t.Errorf("ID = %d, want 98765432109", event.ID)
	}
}
