package db

import (
	"testing"
)

// TestScancodeFileEntry_HasExpectedFields verifies the per-file scancode
// result struct has the fields needed for the web GUI table.
func TestScancodeFileEntry_HasExpectedFields(t *testing.T) {
	entry := ScancodeFileEntry{
		Path:      "src/main.go",
		License:   "MIT",
		Copyright: "Copyright 2024 ACME Corp",
	}
	if entry.Path != "src/main.go" {
		t.Errorf("Path = %q", entry.Path)
	}
	if entry.License != "MIT" {
		t.Errorf("License = %q", entry.License)
	}
	if entry.Copyright != "Copyright 2024 ACME Corp" {
		t.Errorf("Copyright = %q", entry.Copyright)
	}
}

// TestTruncateCopyright verifies long copyright text is truncated.
func TestTruncateCopyright_Short(t *testing.T) {
	if got := truncateCopyright("Copyright 2024 ACME", 200); got != "Copyright 2024 ACME" {
		t.Errorf("short: %q", got)
	}
}

func TestTruncateCopyright_Long(t *testing.T) {
	long := "Copyright (c) 2008-2011, AQR Capital Management, LLC, Lambda Foundry, Inc. and PyData Development Team All rights reserved. Copyright (c) 2011-2026, Open source contributors. Redistribution and use in source and binary forms..."
	got := truncateCopyright(long, 120)
	if len(got) > 123 { // 120 + "..."
		t.Errorf("truncated length = %d, want <= 123", len(got))
	}
	if got[len(got)-3:] != "..." {
		t.Error("should end with ...")
	}
}

func TestTruncateCopyright_Empty(t *testing.T) {
	if got := truncateCopyright("", 200); got != "" {
		t.Errorf("empty: %q", got)
	}
}

func TestTruncateCopyright_ExactLimit(t *testing.T) {
	exact := "exactly twenty chars"
	if got := truncateCopyright(exact, 20); got != exact {
		t.Errorf("exact: %q", got)
	}
}

// TestExtractFirstHolder extracts just the holder name from a copyright JSON array.
func TestExtractFirstHolder_Normal(t *testing.T) {
	json := `[{"value":"Copyright (c) 2024 ACME Corp","start_line":1}]`
	got := extractFirstCopyrightHolder([]byte(json))
	if got != "Copyright (c) 2024 ACME Corp" {
		t.Errorf("got %q", got)
	}
}

func TestExtractFirstHolder_Multiple(t *testing.T) {
	json := `[{"value":"Copyright 2024 A"},{"value":"Copyright 2024 B"}]`
	got := extractFirstCopyrightHolder([]byte(json))
	// Should return first + count indicator.
	if got == "" {
		t.Error("should not be empty for multiple holders")
	}
}

func TestExtractFirstHolder_Empty(t *testing.T) {
	got := extractFirstCopyrightHolder([]byte("[]"))
	if got != "" {
		t.Errorf("empty array: %q", got)
	}
}

func TestExtractFirstHolder_Nil(t *testing.T) {
	got := extractFirstCopyrightHolder(nil)
	if got != "" {
		t.Errorf("nil: %q", got)
	}
}
