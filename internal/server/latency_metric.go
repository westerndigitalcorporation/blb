// Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package server

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	dto "github.com/prometheus/client_model/go"

	"github.com/westerndigitalcorporation/blb/internal/core"
)

// OpMetric is a wrapper around metric objects that helps with tracking counts
// and latencies for "operations". Operations for the purpose can be either
// things done by a server on behalf of a client (e.g. handling an RPC), or
// chunks of work initiated internally.
//
// OpMetric will create three metric sets:
//   - A CounterSet with the given name, label "result", and any additional labels.
//     Using Start/End will increment this counter with "result"="all".
//     Additionally you can call Failed or TooBusy on the op object to increment
//     the counters with "result"="failed" and "too_busy".
//   - A SketchSet with the given name + "_latency" and any additional labels.
//     Using Start/End will add latencies to this sketch set, only if
//     TooBusy/Failed was not called before End.
//   - A IntGaugeSet with the given name + "_pending" and any additional labels.
//     Using Start/End will ensure that this metric reflects the number of
//     pending operations.
//
// Suggested usage:
//
// h.opMetricInstance = NewOpMetric("my_server_ops", "op")
//
// func (h *myHandler) someOpHandler() {
//     op := h.opMetricInstance.Start("some_op")
//     defer op.End()
//     ...
//     if queueTooBig {
//         op.TooBusy()
//     }
//     if err != nil {
//         op.Failed()
//     }
//     return err
// }
type OpMetric struct {
	name      string
	counters  *prometheus.CounterVec
	latencies *prometheus.SummaryVec
	pending   *prometheus.GaugeVec
}

// NewOpMetric returns a new op metric.
func NewOpMetric(name string, labels ...string) *OpMetric {
	labelsWithResult := append([]string{"result"}, labels...)
	return &OpMetric{
		name:      name,
		counters:  promauto.NewCounterVec(prometheus.CounterOpts{Name: name}, labelsWithResult),
		latencies: promauto.NewSummaryVec(prometheus.SummaryOpts{Name: name + "_latency"}, labels),
		pending:   promauto.NewGaugeVec(prometheus.GaugeOpts{Name: name + "_pending"}, labels),
	}
}

// Start marks that a new operation has started and begins measuring the latency.
func (m *OpMetric) Start(values ...string) *latencyMeasurer {
	lm := &latencyMeasurer{opm: m, values: values}
	lm.Result("all") // this resets start, so set it below
	lm.start = time.Now().UnixNano()
	lm.opm.pending.WithLabelValues(values...).Inc()
	return lm
}

// Count returns how many times Start has been called on the OpMetric
func (m *OpMetric) Count(result string, values ...string) uint64 {
	valuesWithAll := append([]string{result}, values...)
	mtr := m.counters.WithLabelValues(valuesWithAll...)
	var value dto.Metric
	if mtr.Write(&value) != nil {
		return 0
	}
	return uint64(*value.Counter.Value)
}

// String returns a nice string with latency information.
func (m *OpMetric) String(values ...string) string {
	out := SummaryString(m.latencies.WithLabelValues(values...))
	out += fmt.Sprintf(" / %d rejected / %d failed", m.Count("too_busy", values...), m.Count("failed", values...))

	var value dto.Metric
	if m.pending.WithLabelValues(values...).Write(&value) != nil {
		out += fmt.Sprintf(" / %d pending", int64(*value.Gauge.Value))
	}

	return out
}

// Strings returns a map with results from String. Note that it only calls
// String with a single argument at a time, so it can only be used when the
// OpMetric has one label. But that is the common case.
func (m *OpMetric) Strings(keys ...string) map[string]string {
	out := make(map[string]string)
	for _, key := range keys {
		out[key] = m.String(key)
	}
	return out
}

// latencyMeasurer is an internal type to enable some syntactic sugar.
type latencyMeasurer struct {
	start  int64
	opm    *OpMetric
	values []string
}

// Failed records that the RPC returned an error.
func (lm *latencyMeasurer) Failed() {
	lm.Result("failed")
}

// TooBusy records that the RPC was rejected as the server is too busy.
func (lm *latencyMeasurer) TooBusy() {
	lm.Result("too_busy")
}

// Result records an arbitrary error result.
func (lm *latencyMeasurer) Result(result string) {
	lm.start = 0 // zero this so that End won't try to record latency
	valuesWithResult := append([]string{result}, lm.values...)
	lm.opm.counters.WithLabelValues(valuesWithResult...).Inc()
}

// End records the elapsed time since the latencyMeasurer was created.
func (lm *latencyMeasurer) End() {
	if lm.start != 0 {
		d := time.Duration(time.Now().UnixNano() - lm.start)
		lm.opm.latencies.WithLabelValues(lm.values...).Observe(float64(d) / 1e9)
	}
	lm.opm.pending.WithLabelValues(lm.values...).Dec()
}

// EndWithBlbError checks if berr is core.NoError: if so, it calls Failed().
// It always calls End.
func (lm *latencyMeasurer) EndWithBlbError(berr *core.Error) {
	if *berr != core.NoError {
		lm.Failed()
	}
	lm.End()
}

func SummaryString(obs prometheus.Observer) string {
	sum, ok := obs.(prometheus.Summary)
	if !ok {
		return ""
	}
	var value dto.Metric
	if sum.Write(&value) != nil || value.Summary == nil {
		return ""
	}
	out := fmt.Sprintf("Total count=%d;", *value.Summary.SampleCount)
	for _, q := range value.Summary.Quantile {
		out += fmt.Sprintf(" %gth=%.3f;", *q.Quantile*100, *q.Value)
	}
	return out[:len(out)-1]
}
