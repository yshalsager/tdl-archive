package main

import (
	"encoding/json"
	"fmt"
	"io"
	"time"
)

type peerResult struct {
	Selector string `json:"selector"`
	Title    string `json:"title"`
	Type     string `json:"type"`
	ID       int64  `json:"id"`
}

type mediaStats struct {
	Downloaded int   `json:"downloaded"`
	Reused     int   `json:"reused"`
	Skipped    int   `json:"skipped"`
	Failed     int   `json:"failed"`
	Pending    int   `json:"pending"`
	FailedIDs  []int `json:"failed_ids,omitempty"`
}

type syncResult struct {
	StartingCursor *int
	EndingCursor   *int
	Selected       int
	Saved          int
	Media          mediaStats
	Warnings       []string
}

type runResult struct {
	Version            int         `json:"version"`
	Status             string      `json:"status"`
	DryRun             bool        `json:"dry_run"`
	ConfigPath         string      `json:"config_path"`
	DataPath           string      `json:"data_path"`
	Peer               *peerResult `json:"peer,omitempty"`
	StartingCursor     *int        `json:"starting_cursor,omitempty"`
	EndingCursor       *int        `json:"ending_cursor,omitempty"`
	DialogTopMessageID *int        `json:"dialog_top_message_id,omitempty"`
	Selected           int         `json:"selected"`
	Saved              int         `json:"saved"`
	Media              mediaStats  `json:"media"`
	JSONDump           bool        `json:"json_dump"`
	DurationMS         int64       `json:"duration_ms"`
	Warnings           []string    `json:"warnings,omitempty"`
	Error              string      `json:"error,omitempty"`
}

func (r *runResult) finish(started time.Time, err error) {
	r.DurationMS = time.Since(started).Milliseconds()
	if err != nil {
		r.Status = "failed"
		r.Error = err.Error()
	} else if len(r.Warnings) > 0 {
		r.Status = "completed_with_warnings"
	} else {
		r.Status = "success"
	}
}

func writeResult(w io.Writer, result runResult) error {
	if err := json.NewEncoder(w).Encode(result); err != nil {
		return fmt.Errorf("encode result: %w", err)
	}
	return nil
}
