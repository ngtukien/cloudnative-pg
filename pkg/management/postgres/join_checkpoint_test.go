package postgres

import (
	"slices"
	"testing"
)

func TestPgBaseBackupCheckpointOptions(t *testing.T) {
	fast := pgBaseBackupOptions("host=source", "/data", "/wal", true)
	if !slices.Contains(fast, "--checkpoint=fast") {
		t.Fatalf("external bootstrap must request fast checkpoint: %v", fast)
	}
	joined := pgBaseBackupOptions("host=primary", "/data", "", false)
	if slices.Contains(joined, "--checkpoint=fast") {
		t.Fatalf("internal join must keep default checkpoint: %v", joined)
	}
}
