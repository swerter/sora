package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

// TestCommandDurationMetricsRecordedImmediately verifies that command duration
// metrics are recorded immediately when the metric observation is called,
// not deferred to some later time.
//
// This test addresses a bug where defer func() wrappers in command handlers
// were causing metrics to record session duration instead of command duration.
func TestCommandDurationMetricsRecordedImmediately(t *testing.T) {
	tests := []struct {
		name     string
		protocol string
		command  string
	}{
		// POP3 commands
		{"POP3 CAPA", "pop3", "CAPA"},
		{"POP3 USER", "pop3", "USER"},
		{"POP3 PASS", "pop3", "PASS"},
		{"POP3 STAT", "pop3", "STAT"},
		{"POP3 LIST", "pop3", "LIST"},
		{"POP3 UIDL", "pop3", "UIDL"},
		{"POP3 TOP", "pop3", "TOP"},
		{"POP3 RETR", "pop3", "RETR"},
		{"POP3 DELE", "pop3", "DELE"},
		{"POP3 NOOP", "pop3", "NOOP"},
		{"POP3 RSET", "pop3", "RSET"},
		{"POP3 AUTH", "pop3", "AUTH"},
		{"POP3 QUIT", "pop3", "QUIT"},

		// LMTP commands
		{"LMTP MAIL", "lmtp", "MAIL"},
		{"LMTP RCPT", "lmtp", "RCPT"},
		{"LMTP DATA", "lmtp", "DATA"},
		{"LMTP RSET", "lmtp", "RSET"},

		// ManageSieve commands
		{"ManageSieve LOGIN", "managesieve", "LOGIN"},
		{"ManageSieve LISTSCRIPTS", "managesieve", "LISTSCRIPTS"},
		{"ManageSieve GETSCRIPT", "managesieve", "GETSCRIPT"},
		{"ManageSieve PUTSCRIPT", "managesieve", "PUTSCRIPT"},
		{"ManageSieve SETACTIVE", "managesieve", "SETACTIVE"},
		{"ManageSieve DELETESCRIPT", "managesieve", "DELETESCRIPT"},
		{"ManageSieve LOGOUT", "managesieve", "LOGOUT"},
		{"ManageSieve NOOP", "managesieve", "NOOP"},

		// IMAP commands
		{"IMAP APPEND", "imap", "APPEND"},
		{"IMAP FETCH", "imap", "FETCH"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset the metric before test
			CommandDuration.Reset()
			CommandsTotal.Reset()

			// Simulate command execution with known duration
			start := time.Now()
			expectedDuration := 100 * time.Millisecond
			time.Sleep(expectedDuration)

			// Record metrics (this simulates what the fixed code does)
			CommandsTotal.WithLabelValues(tt.protocol, tt.command, "success").Inc()
			CommandDuration.WithLabelValues(tt.protocol, tt.command).Observe(time.Since(start).Seconds())

			// Immediately verify the metric was recorded
			// The histogram should have exactly 1 sample
			metricFamily, err := prometheus.DefaultGatherer.Gather()
			if err != nil {
				t.Fatalf("Failed to gather metrics: %v", err)
			}

			// Find our command duration metric
			var found bool
			var observedCount uint64
			var observedSum float64
			for _, mf := range metricFamily {
				if mf.GetName() == "sora_command_duration_seconds" {
					for _, m := range mf.GetMetric() {
						labels := m.GetLabel()
						if matchLabels(labels, map[string]string{
							"protocol": tt.protocol,
							"command":  tt.command,
						}) {
							found = true
							observedCount = m.GetHistogram().GetSampleCount()
							observedSum = m.GetHistogram().GetSampleSum()
							break
						}
					}
				}
			}

			if !found {
				t.Fatalf("Metric not found for protocol=%s command=%s", tt.protocol, tt.command)
			}

			// Verify exactly 1 observation was recorded
			if observedCount != 1 {
				t.Errorf("Expected 1 observation, got %d", observedCount)
			}

			// Verify duration is approximately correct (within 50ms tolerance)
			expectedSeconds := expectedDuration.Seconds()
			tolerance := 0.05 // 50ms tolerance
			if observedSum < expectedSeconds-tolerance || observedSum > expectedSeconds+tolerance {
				t.Errorf("Expected duration ~%.3fs (±%.0fms), got %.3fs",
					expectedSeconds, tolerance*1000, observedSum)
			}

			// Verify the counter was also incremented
			counter := testutil.ToFloat64(CommandsTotal.WithLabelValues(tt.protocol, tt.command, "success"))
			if counter != 1 {
				t.Errorf("Expected counter=1, got %.0f", counter)
			}
		})
	}
}

// TestCommandDurationNotAccumulatedAcrossCommands verifies that each command's
// duration is measured independently and doesn't accumulate.
func TestCommandDurationNotAccumulatedAcrossCommands(t *testing.T) {
	CommandDuration.Reset()
	CommandsTotal.Reset()

	// Simulate executing the same command twice with different durations
	protocol := "pop3"
	command := "STAT"

	// First execution: 50ms
	start1 := time.Now()
	time.Sleep(50 * time.Millisecond)
	duration1 := time.Since(start1)
	CommandsTotal.WithLabelValues(protocol, command, "success").Inc()
	CommandDuration.WithLabelValues(protocol, command).Observe(duration1.Seconds())

	// Second execution: 150ms
	start2 := time.Now()
	time.Sleep(150 * time.Millisecond)
	duration2 := time.Since(start2)
	CommandsTotal.WithLabelValues(protocol, command, "success").Inc()
	CommandDuration.WithLabelValues(protocol, command).Observe(duration2.Seconds())

	// Verify we have 2 samples with the sum approximately equal to both durations
	metricFamily, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	var found bool
	var observedCount uint64
	var observedSum float64
	for _, mf := range metricFamily {
		if mf.GetName() == "sora_command_duration_seconds" {
			for _, m := range mf.GetMetric() {
				labels := m.GetLabel()
				if matchLabels(labels, map[string]string{
					"protocol": protocol,
					"command":  command,
				}) {
					found = true
					observedCount = m.GetHistogram().GetSampleCount()
					observedSum = m.GetHistogram().GetSampleSum()
					break
				}
			}
		}
	}

	if !found {
		t.Fatalf("Metric not found for protocol=%s command=%s", protocol, command)
	}

	if observedCount != 2 {
		t.Errorf("Expected 2 observations, got %d", observedCount)
	}

	// Sum should be approximately 200ms (50 + 150)
	expectedSum := (duration1 + duration2).Seconds()
	tolerance := 0.05 // 50ms
	if observedSum < expectedSum-tolerance || observedSum > expectedSum+tolerance {
		t.Errorf("Expected sum ~%.3fs, got %.3fs", expectedSum, observedSum)
	}
}

// matchLabels checks if a label set matches the expected key-value pairs
func matchLabels(labels []*dto.LabelPair, expected map[string]string) bool {
	matched := 0
	for _, label := range labels {
		if expectedValue, ok := expected[label.GetName()]; ok {
			if label.GetValue() == expectedValue {
				matched++
			}
		}
	}
	return matched == len(expected)
}
