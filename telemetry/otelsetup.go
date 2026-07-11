package telemetry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	loggerglobal "go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

const shutdownTimeout = 10 * time.Second

// Init installs the global MeterProvider and LoggerProvider exporting OTLP
// over gRPC, mirroring predastore/otelsetup.Init but without a TracerProvider:
// viperblock is usually embedded in a host (spx) that already owns tracing,
// and standalone (nbdkit) has no server loop to root spans from. The
// returned shutdown func flushes and stops both providers; it is always safe
// to call. With no OTLP endpoint configured (or OTEL_SDK_DISABLED=true),
// Init is a no-op and the globals stay whatever the caller already installed
// — critical when embedded, so viperblock never clobbers a host's providers.
func Init(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	if !exportEnabled() {
		return func(context.Context) error { return nil }, nil
	}

	res, err := newResource(ctx, serviceName)
	if err != nil {
		return nil, fmt.Errorf("otel resource for %s: %w", serviceName, err)
	}

	metricExp, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("otlp metric exporter for %s: %w", serviceName, err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp)),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	logExp, err := otlploggrpc.New(ctx)
	if err != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		return nil, errors.Join(
			fmt.Errorf("otlp log exporter for %s: %w", serviceName, err),
			mp.Shutdown(shutdownCtx))
	}
	lp := sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExp)),
	)
	loggerglobal.SetLoggerProvider(lp)

	shutdown := func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, shutdownTimeout)
		defer cancel()
		return errors.Join(mp.Shutdown(ctx), lp.Shutdown(ctx))
	}
	return shutdown, nil
}

// exportEnabled reports whether any standard OTLP endpoint is configured and
// the SDK is not explicitly disabled.
func exportEnabled() bool {
	if strings.EqualFold(os.Getenv("OTEL_SDK_DISABLED"), "true") {
		return false
	}
	for _, key := range []string{
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
		"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
	} {
		if os.Getenv(key) != "" {
			return true
		}
	}
	return false
}

// newResource builds the service resource: identity attrs set here, plus
// host detection and anything in OTEL_RESOURCE_ATTRIBUTES (ci.run_id etc.).
func newResource(ctx context.Context, serviceName string) (*resource.Resource, error) {
	attrs := []attribute.KeyValue{semconv.ServiceName(serviceName)}
	if v := buildVersion(); v != "" {
		attrs = append(attrs, semconv.ServiceVersion(v))
	}
	if env := os.Getenv("MULGA_ENV"); env != "" {
		attrs = append(attrs,
			attribute.String("mulga.env", env),
			semconv.DeploymentEnvironment(env))
	}
	if src := os.Getenv("MULGA_SOURCE"); src != "" {
		attrs = append(attrs, attribute.String("mulga.source", src))
	}

	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithHost(),
		resource.WithTelemetrySDK(),
		resource.WithAttributes(attrs...),
	)
	// Schema-URL conflicts between detectors still yield a usable merged
	// resource; only a nil resource is fatal.
	if err != nil && res == nil {
		return nil, err
	}
	return res, nil
}

// buildVersion returns the module version or embedded VCS revision, if any.
func buildVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	if v := info.Main.Version; v != "" && v != "(devel)" {
		return v
	}
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" && len(s.Value) >= 12 {
			return s.Value[:12]
		}
	}
	return ""
}
