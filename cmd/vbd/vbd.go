package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/mulgadc/viperblock/telemetry"
)

// serviceName identifies this process to OTLP and the otelslog bridge.
const serviceName = "viperblockd"

func main() {
	shutdown, err := telemetry.Init(context.Background(), serviceName)
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
	telemetry.SetDefaultJSONLogger(serviceName, level)

	fmt.Println("Viperblock daemon - NBD service, WIP!")
}
