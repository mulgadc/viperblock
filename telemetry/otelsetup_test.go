package telemetry

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	loggerglobal "go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func TestInitWithoutEndpointIsNoop(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "")

	prevMP := otel.GetMeterProvider()
	prevLP := loggerglobal.GetLoggerProvider()
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		loggerglobal.SetLoggerProvider(prevLP)
	})

	shutdown, err := Init(context.Background(), "test-svc")
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if otel.GetMeterProvider() != prevMP {
		t.Error("Init installed a MeterProvider without an endpoint configured")
	}
	if loggerglobal.GetLoggerProvider() != prevLP {
		t.Error("Init installed a LoggerProvider without an endpoint configured")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("shutdown: %v", err)
	}
}

func TestInitWithEndpointInstallsProviders(t *testing.T) {
	// Point at a dead endpoint: exporters dial lazily, so Init must still
	// succeed and install real providers.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	prevMP := otel.GetMeterProvider()
	prevLP := loggerglobal.GetLoggerProvider()
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		loggerglobal.SetLoggerProvider(prevLP)
	})

	shutdown, err := Init(context.Background(), "test-svc")
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if _, ok := otel.GetMeterProvider().(*sdkmetric.MeterProvider); !ok {
		t.Errorf("expected a real sdk/metric MeterProvider, got %T", otel.GetMeterProvider())
	}
	if _, ok := loggerglobal.GetLoggerProvider().(*sdklog.LoggerProvider); !ok {
		t.Errorf("expected a real sdk/log LoggerProvider, got %T", loggerglobal.GetLoggerProvider())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = shutdown(ctx)
}

func TestExportEnabled(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want bool
	}{
		{"nothing set", nil, false},
		{"endpoint set", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4317"}, true},
		{"metrics endpoint only", map[string]string{"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "http://localhost:4317"}, true},
		{"logs endpoint only", map[string]string{"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT": "http://localhost:4317"}, true},
		{"disabled overrides endpoint", map[string]string{
			"OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4317",
			"OTEL_SDK_DISABLED":           "true",
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, key := range []string{
				"OTEL_EXPORTER_OTLP_ENDPOINT",
				"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
				"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
				"OTEL_SDK_DISABLED",
			} {
				t.Setenv(key, tt.env[key])
			}
			if got := exportEnabled(); got != tt.want {
				t.Errorf("exportEnabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewResourceAttributes(t *testing.T) {
	t.Setenv("MULGA_ENV", "env19")
	t.Setenv("MULGA_SOURCE", "ci")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "ci.run_id=12345")

	res, err := newResource(context.Background(), "test-svc")
	if err != nil {
		t.Fatalf("newResource: %v", err)
	}
	got := map[string]string{}
	for _, kv := range res.Attributes() {
		got[string(kv.Key)] = kv.Value.Emit()
	}
	for key, want := range map[string]string{
		"service.name": "test-svc",
		"mulga.env":    "env19",
		"mulga.source": "ci",
		"ci.run_id":    "12345",
	} {
		if got[key] != want {
			t.Errorf("resource attr %s = %q, want %q", key, got[key], want)
		}
	}
	if got["host.name"] == "" {
		t.Error("resource attr host.name missing")
	}
}
