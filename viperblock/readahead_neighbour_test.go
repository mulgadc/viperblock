package viperblock

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// streamCounter delays every chunk read by a fixed time and counts the ones
// made for the sequential stream, told apart from a neighbour's by context.
type streamCounter struct {
	types.Backend

	delay       time.Duration
	streamReads atomic.Int64
	streamBytes atomic.Int64
	otherReads  atomic.Int64
}

var _ types.Backend = (*streamCounter)(nil)

type streamKey struct{}

func (s *streamCounter) ReadCtx(ctx context.Context, fileType types.FileType, objectID uint64, offset, length uint32) ([]byte, error) {
	if fileType == types.FileTypeChunk {
		if ctx.Value(streamKey{}) != nil {
			s.streamReads.Add(1)
			s.streamBytes.Add(int64(length))
		} else {
			s.otherReads.Add(1)
		}
		time.Sleep(s.delay)
	}
	return s.Backend.ReadCtx(ctx, fileType, objectID, offset, length)
}

// streamHalfBytes is the part of the volume the stream reads.
const streamHalfBytes = readAmpVolumeBytes / 2

type streamRun struct {
	streamReads, streamBytes int64
	// neighbourReads counts each neighbour's reads that completed while the
	// stream was running.
	neighbourReads []int64
	elapsed        time.Duration
}

// readStreamBeside reads the first half of the volume as a depth-1 stream of
// 64 KiB requests on a cold reader, while neighbours depth-1 random 4 KiB readers
// of the other half run until it ends, so they never cache the stream's blocks.
func readStreamBeside(t testing.TB, root, volumeName string, written []byte, neighbours int) streamRun {
	t.Helper()
	reader, counter := coldReaderVB(t, root, volumeName, nil)
	sc := &streamCounter{Backend: counter.Backend, delay: time.Millisecond}
	reader.Backend = sc

	var streaming atomic.Bool
	during := make([]atomic.Int64, neighbours)
	stop := make(chan struct{})
	started := make(chan struct{}, neighbours)
	var wg sync.WaitGroup
	stopNeighbours := sync.OnceFunc(func() { close(stop); wg.Wait() })
	defer stopNeighbours()
	for n := range neighbours {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(n), 1))
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				half := streamHalfBytes / uint64(DefaultBlockSize)
				block := half + rng.Uint64N(half)
				_, err := reader.ReadAtCtx(context.Background(), block*uint64(DefaultBlockSize), uint64(DefaultBlockSize))
				assert.NoError(t, err)
				if streaming.Load() {
					during[n].Add(1)
				}
				if i == 0 {
					started <- struct{}{}
				}
			}
		})
	}
	for range neighbours {
		<-started
	}

	ctx := context.WithValue(context.Background(), streamKey{}, true)
	streaming.Store(true)
	start := time.Now()
	got := make([]byte, 0, streamHalfBytes)
	for off := uint64(0); off < streamHalfBytes; off += readAmpRequestBytes {
		data, err := reader.ReadAtCtx(ctx, off, readAmpRequestBytes)
		require.NoError(t, err)
		got = append(got, data...)
	}
	elapsed := time.Since(start)
	streaming.Store(false)
	stopNeighbours()
	require.Equal(t, written[:streamHalfBytes], got, "the stream came back wrong")

	run := streamRun{streamReads: sc.streamReads.Load(), streamBytes: sc.streamBytes.Load(), elapsed: elapsed}
	for n := range during {
		run.neighbourReads = append(run.neighbourReads, during[n].Load())
	}
	return run
}

// TestSequentialStream_KeepsReadaheadBesideRandomReaders: a depth-1 stream
// next to unrelated random readers is still widened, so it makes about as many
// backend reads as it does alone.
func TestSequentialStream_KeepsReadaheadBesideRandomReaders(t *testing.T) {
	const volumeName = "vol-readahead-neighbour"
	root, written := seedSequentialVolume(t, volumeName, nil)

	alone := readStreamBeside(t, root, volumeName, written, 0)
	t.Logf("alone: %d stream reads in %v", alone.streamReads, alone.elapsed)
	for _, neighbours := range []int{1, 2} {
		run := readStreamBeside(t, root, volumeName, written, neighbours)
		t.Logf("beside %d random readers: %d stream reads in %v, neighbour reads during it %v",
			neighbours, run.streamReads, run.elapsed, run.neighbourReads)
		// Each neighbour must overlap the stream for the comparison to mean
		// anything: at 1 ms a read, each makes dozens while it runs.
		for n, reads := range run.neighbourReads {
			require.GreaterOrEqualf(t, reads, int64(10), "neighbour %d did not overlap the stream", n)
		}
		assert.LessOrEqualf(t, run.streamReads, 2*alone.streamReads,
			"beside %d random readers the stream lost its read-ahead", neighbours)
	}
}

// TestRandomRead_InterleavedIsNotWidened reads random blocks, so a request
// that is widened must have started where one of the last few ended by chance.
func TestRandomRead_InterleavedIsNotWidened(t *testing.T) {
	const volumeName = "vol-readahead-random"
	const reads = 512
	root, _ := seedSequentialVolume(t, volumeName, nil)
	reader, counter := coldReaderVB(t, root, volumeName, nil)

	rng := rand.New(rand.NewPCG(7, 7))
	for range reads {
		block := rng.Uint64N(readAmpVolumeBytes / uint64(DefaultBlockSize))
		_, err := reader.ReadAtCtx(context.Background(), block*uint64(DefaultBlockSize), uint64(DefaultBlockSize))
		require.NoError(t, err)
	}
	assert.LessOrEqual(t, counter.bytes.Load(), int64(reads*int(DefaultBlockSize))*11/10,
		"random reads were widened")
}

// BenchmarkSequentialStream times a stream of 64 KiB reads at 1 ms a chunk
// read: 8 MiB at depth 1, alone and beside random readers, and 16 MiB at depth 16.
func BenchmarkSequentialStream(b *testing.B) {
	const volumeName = "vol-readahead-bench"
	root, written := seedSequentialVolume(b, volumeName, nil)
	for _, neighbours := range []int{0, 1, 2} {
		b.Run(fmt.Sprintf("depth1-beside-%d", neighbours), func(b *testing.B) {
			var bytes int64
			var streamTime time.Duration
			for b.Loop() {
				run := readStreamBeside(b, root, volumeName, written, neighbours)
				bytes += run.streamBytes
				streamTime += run.elapsed
			}
			b.ReportMetric(float64(bytes)/float64(b.N)/streamHalfBytes, "backend-bytes/stream-byte")
			b.ReportMetric(streamTime.Seconds()/float64(b.N), "stream-sec/op")
		})
	}
	b.Run("depth16", func(b *testing.B) {
		var bytes int64
		for b.Loop() {
			reader, counter := coldReaderVB(b, root, volumeName, nil)
			sc := &streamCounter{Backend: counter.Backend, delay: time.Millisecond}
			reader.Backend = sc
			offsets := make(chan uint64)
			var wg sync.WaitGroup
			for range 16 {
				wg.Go(func() {
					ctx := context.WithValue(context.Background(), streamKey{}, true)
					for off := range offsets {
						_, err := reader.ReadAtCtx(ctx, off, readAmpRequestBytes)
						assert.NoError(b, err)
					}
				})
			}
			for off := uint64(0); off < readAmpVolumeBytes; off += readAmpRequestBytes {
				offsets <- off
			}
			close(offsets)
			wg.Wait()
			bytes += sc.streamBytes.Load()
		}
		b.ReportMetric(float64(bytes)/float64(b.N)/readAmpVolumeBytes, "backend-bytes/stream-byte")
	})
}
