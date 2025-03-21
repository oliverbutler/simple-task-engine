package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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

// NewMetrics creates and registers all Prometheus metrics
func NewMetrics() *Metrics {
	m := &Metrics{
		TasksProcessed: promauto.NewCounter(prometheus.CounterOpts{
			Name: "task_engine_tasks_processed_total",
			Help: "The total number of processed tasks",
		}),
		TasksSucceeded: promauto.NewCounter(prometheus.CounterOpts{
			Name: "task_engine_tasks_succeeded_total",
			Help: "The total number of successfully processed tasks",
		}),

		TasksFailed: promauto.NewCounter(prometheus.CounterOpts{
			Name: "task_engine_tasks_failed_total",
			Help: "The total number of failed tasks",
		}),
		TasksRetried: promauto.NewCounter(prometheus.CounterOpts{
			Name: "task_engine_tasks_retried_total",
			Help: "The total number of retried tasks",
		}),
		BufferSize: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "task_engine_buffer_size",
			Help: "Current number of tasks in the buffer",
		}),
		BufferCapacity: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "task_engine_buffer_capacity",
			Help: "Maximum capacity of the task buffer",
		}),
		InFlightTasks: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "task_engine_inflight_tasks",
			Help: "Current number of in-flight tasks",
		}),
		MaxConcurrentTasks: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "task_engine_max_concurrent_tasks",
			Help: "Maximum number of concurrent tasks",
		}),
		ResultsChannelSize: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "task_engine_results_channel_size",
			Help: "Current size of the results channel",
		}),
		ResultsChannelCap: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "task_engine_results_channel_capacity",
			Help: "Capacity of the results channel",
		}),
		APIRequestDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "task_engine_api_request_duration_seconds",
			Help:    "Duration of API requests in seconds",
			Buckets: prometheus.DefBuckets,
		}),
		APIRequestsTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "task_engine_api_requests_total",
			Help: "Total number of API requests",
		}),
		APIRequestErrors: promauto.NewCounter(prometheus.CounterOpts{
			Name: "task_engine_api_request_errors_total",
			Help: "Total number of API request errors",
		}),
		TaskProcessingDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "task_engine_task_processing_duration_seconds",
			Help:    "Duration of task processing in seconds",
			Buckets: prometheus.DefBuckets,
		}),
		TasksByType: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "task_engine_tasks_by_type_total",
				Help: "Total number of tasks by type",
			},
			[]string{"type"},
		),
		TasksByPriority: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "task_engine_tasks_by_priority_total",
				Help: "Total number of tasks by priority",
			},
			[]string{"priority"},
		),
		TaskStatusUpdates: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "task_engine_task_status_updates_total",
				Help: "Total number of task status updates by status",
			},
			[]string{"status"},
		),
	}

	return m
}
