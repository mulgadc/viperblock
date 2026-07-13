package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"

	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/trace"

	loggerglobal "go.opentelemetry.io/otel/log/global"
	lognoop "go.opentelemetry.io/otel/log/noop"
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

// TestSetDefaultJSONLoggerNoLoggerProviderIsStdoutOnly is the no-op
// guarantee: with no real LoggerProvider installed (Init did not run, or
// ran without an OTLP endpoint), SetDefaultJSONLogger must produce a
// stdout-only default and never wrap it in a fanoutHandler — standalone
// logging behavior stays unchanged with OTLP unconfigured.
func TestSetDefaultJSONLoggerNoLoggerProviderIsStdoutOnly(t *testing.T) {
	prevLP := loggerglobal.GetLoggerProvider()
	defer loggerglobal.SetLoggerProvider(prevLP)
	loggerglobal.SetLoggerProvider(lognoop.NewLoggerProvider())

	prevHandler := slog.Default().Handler()
	defer slog.SetDefault(slog.New(prevHandler))

	SetDefaultJSONLogger("viperblockd", slog.LevelInfo)

	if _, ok := slog.Default().Handler().(*fanoutHandler); ok {
		t.Error("expected stdout-only handler without a real LoggerProvider, got fanoutHandler")
	}
}

// recordingLogExporter is a minimal sdk/log.Exporter that records every
// exported record for assertions.
type recordingLogExporter struct {
	mu      sync.Mutex
	records []sdklog.Record
}

var _ sdklog.Exporter = (*recordingLogExporter)(nil)

func (e *recordingLogExporter) Export(_ context.Context, records []sdklog.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, r := range records {
		e.records = append(e.records, r.Clone())
	}
	return nil
}

func (e *recordingLogExporter) Shutdown(context.Context) error   { return nil }
func (e *recordingLogExporter) ForceFlush(context.Context) error { return nil }

func (e *recordingLogExporter) snapshot() []sdklog.Record {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]sdklog.Record(nil), e.records...)
}

// TestSetDefaultJSONLoggerBridgesWithoutClobbering exercises the exact
// wiring Init installs (LoggerProvider -> otelslog bridge) via a recording
// exporter standing in for the OTLP gRPC exporter. It proves the bridge
// carries the logging context's trace_id, that stdout output is preserved
// alongside the bridge, and that a second SetDefaultJSONLogger call (mirrors
// a second standalone entrypoint re-asserting its level) still exports
// rather than clobbering the bridge.
func TestSetDefaultJSONLoggerBridgesWithoutClobbering(t *testing.T) {
	exp := &recordingLogExporter{}
	lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(exp)))
	defer func() { _ = lp.Shutdown(context.Background()) }()

	prevLP := loggerglobal.GetLoggerProvider()
	defer loggerglobal.SetLoggerProvider(prevLP)
	loggerglobal.SetLoggerProvider(lp)

	prevHandler := slog.Default().Handler()
	defer slog.SetDefault(slog.New(prevHandler))

	SetDefaultJSONLogger("viperblockd", slog.LevelInfo)
	if _, ok := slog.Default().Handler().(*fanoutHandler); !ok {
		t.Fatalf("expected fanoutHandler once a real LoggerProvider is installed, got %T", slog.Default().Handler())
	}

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
		SpanID:     trace.SpanID{0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x01, 0x02},
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	slog.InfoContext(ctx, "first record")

	// Simulate a second standalone bootstrap path re-asserting the level.
	SetDefaultJSONLogger("viperblockd", slog.LevelInfo)
	if _, ok := slog.Default().Handler().(*fanoutHandler); !ok {
		t.Fatalf("expected fanoutHandler to survive a second SetDefaultJSONLogger call, got %T", slog.Default().Handler())
	}
	slog.InfoContext(ctx, "second record")

	records := exp.snapshot()
	if len(records) != 2 {
		t.Fatalf("got %d exported records, want 2 (bridge should survive repeated SetDefaultJSONLogger calls)", len(records))
	}
	for i, rec := range records {
		if rec.TraceID() != sc.TraceID() {
			t.Errorf("record %d TraceID = %s, want %s", i, rec.TraceID(), sc.TraceID())
		}
	}
}

// recordingSlogHandler is a bare slog.Handler that records whether Handle
// was called, standing in for the pre-existing stdout handler in fan-out
// tests.
type recordingSlogHandler struct {
	mu     sync.Mutex
	called int
}

var _ slog.Handler = (*recordingSlogHandler)(nil)

func (h *recordingSlogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingSlogHandler) Handle(context.Context, slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.called++
	return nil
}

func (h *recordingSlogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingSlogHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordingSlogHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.called
}

// TestFanoutHandlerWritesToAllHandlers proves the existing stdout handler
// still fires when the OTLP bridge handler is fanned in alongside it.
func TestFanoutHandlerWritesToAllHandlers(t *testing.T) {
	stdout := &recordingSlogHandler{}
	bridge := &recordingSlogHandler{}
	logger := slog.New(newFanoutHandler(stdout, bridge))

	logger.Info("dual write")

	if stdout.count() != 1 {
		t.Errorf("stdout handler called %d times, want 1", stdout.count())
	}
	if bridge.count() != 1 {
		t.Errorf("bridge handler called %d times, want 1", bridge.count())
	}
}
