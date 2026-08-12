package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/mulgadc/bluebottle/pkg/otelsetup"
)

// serviceName identifies this process to OTLP and the otelslog bridge.
const serviceName = "viperblockd"

func main() {
	// No TracerProvider: viperblock is usually embedded in a host (spx) that
	// already owns tracing, and standalone has no server loop to root spans from.
	shutdown, err := otelsetup.Init(context.Background(), serviceName,
		otelsetup.WithoutTracing(), otelsetup.WithoutRuntimeMetrics())
	if err != nil {
		slog.Error("otel telemetry init failed, continuing without OTLP export", "err", err)
	} else {
		defer func() {
			if err := shutdown(context.Background()); err != nil {
				slog.Error("otel shutdown failed", "err", err)
			}
		}()
	}
	level := slog.LevelInfo
	if os.Getenv("VIPERBLOCK_DEBUG") == "1" {
		level = slog.LevelDebug
	}
	// Stderr, not stdout: viperblock also runs as an nbdkit plugin, whose fd 1
	// nbdkit repoints at /dev/null while leaving fd 2 on journald.
	otelsetup.SetDefaultJSONLogger(serviceName, level, otelsetup.WithWriter(os.Stderr))

	fmt.Println("Viperblock daemon - NBD service, WIP!")
}
