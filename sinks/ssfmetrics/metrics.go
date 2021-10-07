// Package ssfmetrics provides sinks that are used by veneur internally.
package ssfmetrics

import (
	"sync/atomic"

	"github.com/sirupsen/logrus"
	"github.com/stripe/veneur/v14/protocol"
	"github.com/stripe/veneur/v14/routing"
	"github.com/stripe/veneur/v14/samplers"
	"github.com/stripe/veneur/v14/sinks"
	"github.com/stripe/veneur/v14/ssf"
	"github.com/stripe/veneur/v14/trace"
	"github.com/stripe/veneur/v14/trace/metrics"
)

// metricExtractionSink enqueues ssf spans or udp metrics for processing in the next pipeline iteration.
type metricExtractionSink struct {
	workers                  []Processor
	indicatorSpanTimerName   string
	objectiveSpanTimerName   string
	log                      *logrus.Logger
	traceClient              *trace.Client
	spansProcessed           int64
	metricsGenerated         int64
	parser                   samplers.Parser
	computationRoutingConfig routing.ComputationRoutingConfig
}

var _ sinks.SpanSink = &metricExtractionSink{}

// Processor represents a thing that can process UDPMetrics.
type Processor interface {
	// IngestUDP takes a single UDPMetric and processes it in the worker.
	IngestUDP(samplers.UDPMetric)
}

// DerivedMetricsSink composes the functionality of a SpanSink and DerivedMetricsProcessor
type DerivedMetricsSink interface {
	sinks.SpanSink
	samplers.DerivedMetricsProcessor
}

// NewMetricExtractionSink sets up and creates a span sink that
// extracts metrics ("samples") from SSF spans and reports them to a
// veneur's metrics workers.
func NewMetricExtractionSink(mw []Processor, indicatorTimerName, objectiveTimerName string, cl *trace.Client, log *logrus.Logger, p *samplers.Parser, computationRoutingConfig ComputationRoutingConfig) (DerivedMetricsSink, error) {
	return &metricExtractionSink{
		workers:                  mw,
		indicatorSpanTimerName:   indicatorTimerName,
		objectiveSpanTimerName:   objectiveTimerName,
		traceClient:              cl,
		log:                      log,
		parser:                   *p,
		computationRoutingConfig: computationRoutingConfig,
	}, nil
}

// Name returns "metric_extraction".
func (m *metricExtractionSink) Name() string {
	return "metric_extraction"
}

// Start is a no-op.
func (m *metricExtractionSink) Start(*trace.Client) error {
	return nil
}

// sendMetrics enqueues the metrics into the worker channels
func (m *metricExtractionSink) sendMetrics(metrics []samplers.UDPMetric) {
	for _, metric := range metrics {
		m.workers[metric.Digest%uint32(len(m.workers))].IngestUDP(metric)
	}
}

func (m *metricExtractionSink) SendSample(sample *ssf.SSFSample) error {
	metric, err := m.parser.ParseMetricSSF(sample)
	if err != nil {
		return err
	}
	m.sendMetrics([]samplers.UDPMetric{metric})
	return nil
}

// Ingest extracts metrics from an SSF span, and feeds them into the
// appropriate metric sinks.
func (m *metricExtractionSink) Ingest(span *ssf.SSFSpan) error {
	var metricsCount int
	defer func() {
		atomic.AddInt64(&m.metricsGenerated, int64(metricsCount))
		atomic.AddInt64(&m.spansProcessed, 1)
	}()
	metrics, err := m.parser.ConvertMetrics(span)
	if err != nil {
		if _, ok := err.(samplers.InvalidMetrics); ok {
			m.log.WithError(err).
				Warn("Could not parse metrics from SSF Message")
			tags := map[string]string{
				"packet_type": "ssf_metric",
				"step":        "extract_metrics",
				"reason":      "invalid_metrics",
			}
			m.SendSample(ssf.Count("ssf.error_total", 1, m.withComputationRoutingTags(tags)))
		} else {
			m.log.WithError(err).Error("Unexpected error extracting metrics from SSF Message")
			tags := map[string]string{
				"packet_type": "ssf_metric",
				"step":        "extract_metrics",
				"reason":      "unexpected_error",
				"error":       err.Error(),
			}
			m.SendSample(ssf.Count("ssf.error_total", 1, m.withComputationRoutingTags(tags)))
			return err
		}
	}
	filteredMetrics := make([]samplers.UDPMetric, 0, len(metrics))
	for _, metric := range metrics {
		if m.computationRoutingConfig.MatcherConfigs.Match(metric.Name, metric.Tags) {
			filteredMetrics = append(filteredMetrics, metric)
		}
	}
	metricsCount += len(filteredMetrics)
	m.sendMetrics(filteredMetrics)

	if err := protocol.ValidateTrace(span); err != nil {
		return err
	}
	// If we made it here, we are dealing with a fully-fledged
	// trace span, not just a mere carrier for Samples:

	indicatorMetrics, err := m.parser.ConvertIndicatorMetrics(span, m.indicatorSpanTimerName, m.objectiveSpanTimerName)
	if err != nil {
		m.log.WithError(err).
			WithField("span_name", span.Name).
			Warn("Couldn't extract indicator metrics for span")
		return err
	}

	spanMetrics, err := m.parser.ConvertSpanUniquenessMetrics(span, 0.01)
	if err != nil {
		m.log.WithError(err).
			WithField("span_name", span.Name).
			Warn("Couldn't extract uniqueness metrics for span")
		return err
	}

	joined := append(indicatorMetrics, spanMetrics...)
	filteredFullTraceSpanMetrics := make([]samplers.UDPMetric, 0, len(joined))
	for _, metric := range filteredFullTraceSpanMetrics {
		if m.computationRoutingConfig.MatcherConfigs.Match(metric.Name, metric.Tags) {
			filteredFullTraceSpanMetrics = append(filteredFullTraceSpanMetrics, metric)
		}
	}
	metricsCount += len(filteredFullTraceSpanMetrics)
	m.sendMetrics(filteredFullTraceSpanMetrics)
	return nil
}

func (m *metricExtractionSink) Flush() {
	tags := map[string]string{"sink": m.Name()}
	metrics.ReportBatch(m.traceClient, []*ssf.SSFSample{
		ssf.Count(sinks.MetricKeyTotalSpansFlushed, float32(atomic.SwapInt64(&m.spansProcessed, 0)), m.withComputationRoutingTags(tags)),
		ssf.Count(sinks.MetricKeyTotalMetricsFlushed, float32(atomic.SwapInt64(&m.metricsGenerated, 0)), m.withComputationRoutingTags(tags)),
	})
}

func (m *metricExtractionSink) withComputationRoutingTags(tags map[string]string) map[string]string {
	ret := make(map[string]string, len(tags)+1)
	for k, v := range tags {
		ret[k] = v
	}
	ret["computation_routing_name"] = m.computationRoutingConfig.Name
	return ret
}
