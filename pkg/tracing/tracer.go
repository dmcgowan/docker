package tracing

import (
	"github.com/Sirupsen/logrus"
	"github.com/opentracing/basictracer-go"
	"github.com/opentracing/opentracing-go"
)

type logrusRecorder struct {
}

func (logrusRecorder) RecordSpan(span basictracer.RawSpan) {
	fields := logrus.Fields{
		"ot.op":       span.Operation,
		"ot.span":     span.SpanID,
		"ot.trace":    span.TraceID,
		"ot.start":    span.Start.UnixNano(),
		"ot.duration": span.Duration,
	}
	if span.ParentSpanID > 0 {
		fields["ot.parent"] = span.ParentSpanID
	}
	if span.Sampled {
		fields["ot.sampled"] = span.Sampled
	}
	for k, v := range span.Tags {
		fields[k] = v
	}
	logrus.WithFields(fields).Info()
}

type nilRecorder struct {
}

func (nilRecorder) RecordSpan(span basictracer.RawSpan) {}

func NewLogrusTracer() opentracing.Tracer {
	return basictracer.New(logrusRecorder{})
}

func NewNilTracer() opentracing.Tracer {
	return basictracer.New(nilRecorder{})
}
