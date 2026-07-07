package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestSlogHandlerStampsTraceIDs(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(NewSlogHandler(slog.NewJSONHandler(&buf, nil)))

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
		SpanID:  trace.SpanID{0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x01, 0x02},
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	logger.InfoContext(ctx, "with span")

	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("unmarshal log line: %v", err)
	}
	if line["trace_id"] != sc.TraceID().String() {
		t.Errorf("trace_id = %v, want %s", line["trace_id"], sc.TraceID())
	}
	if line["span_id"] != sc.SpanID().String() {
		t.Errorf("span_id = %v, want %s", line["span_id"], sc.SpanID())
	}
}

func TestSlogHandlerNoSpanNoStamp(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(NewSlogHandler(slog.NewJSONHandler(&buf, nil)))

	logger.InfoContext(context.Background(), "no span")

	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("unmarshal log line: %v", err)
	}
	if _, ok := line["trace_id"]; ok {
		t.Error("trace_id present on record without span")
	}
	if _, ok := line["span_id"]; ok {
		t.Error("span_id present on record without span")
	}
}

func TestSlogHandlerPreservesWrapperThroughWithAttrs(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(NewSlogHandler(slog.NewJSONHandler(&buf, nil))).
		With("component", "test").WithGroup("grp")

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{0xff, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
		SpanID:  trace.SpanID{0xfa, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x01, 0x02},
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	logger.InfoContext(ctx, "wrapped", "k", "v")

	if !bytes.Contains(buf.Bytes(), []byte(sc.TraceID().String())) {
		t.Errorf("trace_id lost after With/WithGroup: %s", buf.String())
	}
}
