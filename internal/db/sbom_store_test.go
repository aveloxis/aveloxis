package db

import (
	"testing"
)

// TestSBOMRecordHasFormatField verifies that SBOMRecord has format tracking.
func TestSBOMRecordHasFormatField(t *testing.T) {
	r := SBOMRecord{
		Format:  "cyclonedx",
		Version: "1.5",
	}
	if r.Format != "cyclonedx" {
		t.Errorf("SBOMRecord.Format = %q, want %q", r.Format, "cyclonedx")
	}
	if r.Version != "1.5" {
		t.Errorf("SBOMRecord.Version = %q, want %q", r.Version, "1.5")
	}
}

// TestSBOMRecordHasSPDXFormat verifies SPDX format tracking.
func TestSBOMRecordHasSPDXFormat(t *testing.T) {
	r := SBOMRecord{
		Format:  "spdx",
		Version: "2.3",
	}
	if r.Format != "spdx" {
		t.Errorf("SBOMRecord.Format = %q, want %q", r.Format, "spdx")
	}
}
