package telemetry

import (
	"context"
	"os"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// ReadSizeEnv turns on the backend read-size histogram when set to "1". It is
// read once at process start; a read checks only the cached result.
const ReadSizeEnv = "VIPERBLOCK_READ_SIZE_METRICS"

var (
	readSizeOn = os.Getenv(ReadSizeEnv) == "1"

	readSizeOnce sync.Once
	readSizeHist metric.Int64Histogram
)

// readSizeBounds separate the backend read sizes that matter: the encrypted
// chunk header probe, a 4 KiB block plain and encrypted, a 64 KiB frame,
// sequential and read-ahead reads, a 4 MiB chunk and a whole chunk object.
var readSizeBounds = []float64{4, 4096, 4112, 16384, 65536, 262144, 1 << 20, 2 << 20, 4 << 20, 4<<20 + 10}

// ReadSizeOn reports whether this process records backend read sizes.
func ReadSizeOn() bool { return readSizeOn }

// RecordBackendReadSize records one object-store read of size bytes. fileType
// names the object kind, source is "own" or "inherited", and volume is the
// volume whose object was read. Callers check ReadSizeOn first.
func RecordBackendReadSize(ctx context.Context, fileType, source, volume string, size int) {
	readSizeOnce.Do(func() {
		var err error
		readSizeHist, err = otel.Meter(meterName).Int64Histogram("viperblock.backend.read.size",
			metric.WithDescription("Size of each object-store read: the requested range length, or the object's length for a whole-object read. Recorded only when "+ReadSizeEnv+"=1."),
			metric.WithUnit("By"),
			metric.WithExplicitBucketBoundaries(readSizeBounds...))
		if err != nil {
			otel.Handle(err)
		}
	})
	if readSizeHist == nil {
		return
	}
	readSizeHist.Record(ctx, int64(size), metric.WithAttributeSet(attribute.NewSet(
		attribute.String("file_type", fileType),
		attribute.String("source", source),
		attribute.String("volume.name", volume),
	)))
}
