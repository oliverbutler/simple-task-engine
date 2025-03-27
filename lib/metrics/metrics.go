package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds all Prometheus metrics for the application
type Metrics struct {
	// Task processing metrics
	TasksProcessed prometheus.Counter
	TasksSucceeded prometheus.Counter
	TasksFailed    prometheus.Counter
	TasksRetried   prometheus.Counter

	// Buffer metrics
	BufferSize     prometheus.Gauge
	BufferCapacity prometheus.Gauge

	// In-flight metrics
	InFlightTasks      prometheus.Gauge
	MaxConcurrentTasks prometheus.Gauge

	// Channel metrics
	ResultsChannelSize prometheus.Gauge
	ResultsChannelCap  prometheus.Gauge

	// API metrics
	APIRequestDuration prometheus.Histogram
	APIRequestsTotal   prometheus.Counter
	APIRequestErrors   prometheus.Counter

	// Task processing time
	TaskProcessingDuration prometheus.Histogram

	// Task types
	TasksByType *prometheus.CounterVec

	// Task priorities
	TasksByPriority *prometheus.CounterVec

	// Task status updates
	TaskStatusUpdates *prometheus.CounterVec
}

// NewMetrics creates and registers all Prometheus metrics with the default registry
func NewMetrics() *Metrics {
	return NewMetricsWithRegistry(prometheus.DefaultRegisterer)
}

// NewMetricsWithRegistry creates metrics with a custom registry
// This is useful for testing to avoid duplicate registration errors
func NewMetricsWithRegistry(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		TasksProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "task_engine_tasks_processed_total",
			Help: "The total number of processed tasks",
		}),
		TasksSucceeded: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "task_engine_tasks_succeeded_total",
			Help: "The total number of successfully processed tasks",
		}),

		TasksFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "task_engine_tasks_failed_total",
			Help: "The total number of failed tasks",
		}),
		TasksRetried: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "task_engine_tasks_retried_total",
			Help: "The total number of retried tasks",
		}),
		BufferSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "task_engine_buffer_size",
			Help: "Current number of tasks in the buffer",
		}),
		BufferCapacity: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "task_engine_buffer_capacity",
			Help: "Maximum capacity of the task buffer",
		}),
		InFlightTasks: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "task_engine_inflight_tasks",
			Help: "Current number of in-flight tasks",
		}),
		MaxConcurrentTasks: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "task_engine_max_concurrent_tasks",
			Help: "Maximum number of concurrent tasks",
		}),
		ResultsChannelSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "task_engine_results_channel_size",
			Help: "Current size of the results channel",
		}),
		ResultsChannelCap: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "task_engine_results_channel_capacity",
			Help: "Capacity of the results channel",
		}),
		APIRequestDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "task_engine_api_request_duration_seconds",
			Help:    "Duration of API requests in seconds",
			Buckets: prometheus.DefBuckets,
		}),
		APIRequestsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "task_engine_api_requests_total",
			Help: "Total number of API requests",
		}),
		APIRequestErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "task_engine_api_request_errors_total",
			Help: "Total number of API request errors",
		}),
		TaskProcessingDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "task_engine_task_processing_duration_seconds",
			Help:    "Duration of task processing in seconds",
			Buckets: prometheus.DefBuckets,
		}),
		TasksByType: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "task_engine_tasks_by_type_total",
				Help: "Total number of tasks by type",
			},
			[]string{"type"},
		),
		TasksByPriority: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "task_engine_tasks_by_priority_total",
				Help: "Total number of tasks by priority",
			},
			[]string{"priority"},
		),
		TaskStatusUpdates: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "task_engine_task_status_updates_total",
				Help: "Total number of task status updates by status",
			},
			[]string{"status"},
		),
	}

	// Register all metrics with the provided registry
	if reg != nil {
		reg.MustRegister(
			m.TasksProcessed,
			m.TasksSucceeded,
			m.TasksFailed,
			m.TasksRetried,
			m.BufferSize,
			m.BufferCapacity,
			m.InFlightTasks,
			m.MaxConcurrentTasks,
			m.ResultsChannelSize,
			m.ResultsChannelCap,
			m.APIRequestDuration,
			m.APIRequestsTotal,
			m.APIRequestErrors,
			m.TaskProcessingDuration,
			m.TasksByType,
			m.TasksByPriority,
			m.TaskStatusUpdates,
		)
	}

	return m
}

// NewNoopMetrics creates metrics that don't register with any registry
// This is useful for testing to avoid duplicate registration errors
func NewNoopMetrics() *Metrics {
	return NewMetricsWithRegistry(nil)
}
