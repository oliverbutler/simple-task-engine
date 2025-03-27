package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestNewMetrics(t *testing.T) {
	// Create a new registry for testing
	reg := prometheus.NewRegistry()

	// Create metrics with the test registry
	metrics := NewMetricsWithRegistry(reg)

	// Verify that a metric was registered correctly
	metrics.TasksProcessed.Inc()

	// Check that the metric was incremented
	if val := testutil.ToFloat64(metrics.TasksProcessed); val != 1 {
		t.Errorf("Expected TasksProcessed to be 1, got %f", val)
	}
}

func TestNewNoopMetrics(t *testing.T) {
	// Create noop metrics
	metrics := NewNoopMetrics()

	// Operations on noop metrics should not panic
	metrics.TasksProcessed.Inc()
	metrics.TasksByType.WithLabelValues("test").Inc()
}
