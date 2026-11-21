package viperblock

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mulgadc/bluebottle/pkg/safecast"
	"github.com/mulgadc/viperblock/types"
)

// Map formats a volume's VBState can record. Zero is format 1.
const (
	MapFormatLegacy uint8 = 1
	MapFormatDelta  uint8 = 2
)

const (
	mapHeadPrefix  = "map/head."
	mapDeltaPrefix = "map/delta."
	mapBasePrefix  = "map/base."

	mapKindBase  byte = 1
	mapKindDelta byte = 2
	mapKindHead  byte = 3

	mapObjectVersion byte = 1

	// DefaultMapDeleteDeferral is twice the volume lease TTL: an object a
	// commit stops naming is deleted no sooner than this after it.
	DefaultMapDeleteDeferral = 90 * time.Second

	// A base is written once the deltas above it reach its size (with a floor,
	// so a near-empty volume does not rewrite its base every drain), once
	// this many deltas accumulate, or after mapBaseMaxAge with any delta.
	mapBaseMinTriggerBytes = 256 << 10
	mapBaseMaxDeltas       = 512
	mapBaseMaxAge          = time.Hour

	mapCommitAttempts = 3
)

var (
	mapObjectMagic = [4]byte{'V', 'B', 'M', '2'}
	mapFenceMagic  = [4]byte{'V', 'B', 'M', 'F'}
	crc32cTable    = crc32.MakeTable(crc32.Castagnoli)
)

// Errors a format-2 volume's map can fail with. Each fails the open or the
// commit rather than serve or persist a map that may be stale.
var (
	ErrMapHeadMissing     = errors.New("map: no committed head")
	ErrMapObjectCorrupt   = errors.New("map: object missing or fails its digest")
	ErrMapEpochNotNewer   = errors.New("map: newest head's epoch is not below this launch's")
	ErrMapNoLeaseEpoch    = errors.New("map: a format-2 commit needs a lease epoch")
	ErrMapNotClaimed      = errors.New("map: this launch has not committed its first head")
	ErrMapFenced          = errors.New("map: a newer lease holder has committed; this writer is fenced")
	ErrMapFenceStubAbsent = errors.New("map: a format-2 volume is missing its v1 fence stub")
)

// MapFenceStub is what a format-2 volume holds at both v1 checkpoint keys.
// Every v1 parser rejects it on its magic before touching the map.
func MapFenceStub() []byte {
	stub := make([]byte, 0, 64)
	stub = append(stub, mapFenceMagic[:]...)
	stub = binary.BigEndian.AppendUint16(stub, 0xFFFF)
	stub = append(stub, "viperblock map format 2: open with a build that reads it\n"...)
	return stub
}

// IsMapFenceStub reports whether data is the format-2 fence stub.
func IsMapFenceStub(data []byte) bool {
	return len(data) >= 6 && bytes.Equal(data[:4], mapFenceMagic[:]) && binary.BigEndian.Uint16(data[4:6]) == 0xFFFF
}

func mapObjectKey(prefix string, epoch, id uint64) string {
	return fmt.Sprintf("%s%020d.%020d", prefix, epoch, id)
}

// parseMapObjectKey extracts (epoch, id) from a key ending in prefix+E.N,
// whether or not the volume name precedes it.
func parseMapObjectKey(key, prefix string) (epoch, id uint64, ok bool) {
	_, rest, found := strings.Cut(key, prefix)
	if !found {
		return 0, 0, false
	}
	dot := strings.IndexByte(rest, '.')
	if dot <= 0 {
		return 0, 0, false
	}
	e, err1 := strconv.ParseUint(rest[:dot], 10, 64)
	n, err2 := strconv.ParseUint(rest[dot+1:], 10, 64)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return e, n, true
}

// mapObjRef names one base or delta object and the digest of its bytes.
type mapObjRef struct {
	Epoch, ID uint64
	Size      uint32
	CRC       uint32
}

func (r mapObjRef) key(prefix string) string { return mapObjectKey(prefix, r.Epoch, r.ID) }

// mapHead is the commit point: the base and the exact deltas above it.
type mapHead struct {
	Base   mapObjRef
	Deltas []mapObjRef
}

func (h mapHead) clone() mapHead {
	return mapHead{Base: h.Base, Deltas: slices.Clone(h.Deltas)}
}

// frameMapObject wraps body with the header binding it to its kind, volume,
// epoch and id, and a trailing CRC32C over everything before it.
func frameMapObject(kind byte, volume string, epoch, id uint64, body []byte) []byte {
	out := make([]byte, 0, 32+len(volume)+len(body))
	out = append(out, mapObjectMagic[:]...)
	out = append(out, kind, mapObjectVersion)
	out = binary.AppendUvarint(out, safecast.IntToUint64(len(volume)))
	out = append(out, volume...)
	out = binary.BigEndian.AppendUint64(out, epoch)
	out = binary.BigEndian.AppendUint64(out, id)
	out = binary.AppendUvarint(out, safecast.IntToUint64(len(body)))
	out = append(out, body...)
	return binary.BigEndian.AppendUint32(out, crc32.Checksum(out, crc32cTable))
}

func unframeMapObject(data []byte, kind byte, volume string, epoch, id uint64) ([]byte, error) {
	if len(data) < 4+2+1+16+1+4 {
		return nil, fmt.Errorf("%w: %d bytes", ErrMapObjectCorrupt, len(data))
	}
	payload, sum := data[:len(data)-4], binary.BigEndian.Uint32(data[len(data)-4:])
	if crc32.Checksum(payload, crc32cTable) != sum {
		return nil, fmt.Errorf("%w: crc mismatch", ErrMapObjectCorrupt)
	}
	if !bytes.Equal(payload[:4], mapObjectMagic[:]) || payload[4] != kind || payload[5] != mapObjectVersion {
		return nil, fmt.Errorf("%w: header", ErrMapObjectCorrupt)
	}
	p := payload[6:]
	vlen, n := binary.Uvarint(p)
	if n <= 0 || safecast.IntToUint64(len(p)-n) < vlen {
		return nil, fmt.Errorf("%w: volume name", ErrMapObjectCorrupt)
	}
	end := n + safecast.Uint64ToInt(vlen)
	if string(p[n:end]) != volume {
		return nil, fmt.Errorf("%w: object belongs to volume %q", ErrMapObjectCorrupt, p[n:end])
	}
	p = p[end:]
	if len(p) < 16 || binary.BigEndian.Uint64(p) != epoch || binary.BigEndian.Uint64(p[8:]) != id {
		return nil, fmt.Errorf("%w: epoch or id", ErrMapObjectCorrupt)
	}
	p = p[16:]
	blen, n := binary.Uvarint(p)
	if n <= 0 || safecast.IntToUint64(len(p)-n) != blen {
		return nil, fmt.Errorf("%w: body length", ErrMapObjectCorrupt)
	}
	return p[n:], nil
}

// deltaEnc codes cur against prev as a magnitude with the sign in the low
// bit, so small moves either way stay small varints.
func deltaEnc(cur, prev uint64) uint64 {
	if cur >= prev {
		return (cur - prev) << 1
	}
	return (prev-cur)<<1 - 1
}

func deltaDec(prev, v uint64) uint64 {
	if v&1 == 0 {
		return prev + v>>1
	}
	return prev - (v+1)>>1
}

// encodeMapRuns encodes runs in the order given, each field a varint delta
// against the previous record. Every block keeps its own sequence number:
// a run of consecutive seqs stores its first and a flag, any other run one
// zigzag delta per further block.
func encodeMapRuns(runs []BlockLookup) []byte {
	out := binary.AppendUvarint(nil, safecast.IntToUint64(len(runs)))
	var prevEnd, prevObj, prevSeq uint64
	for _, r := range runs {
		out = binary.AppendUvarint(out, deltaEnc(r.StartBlock, prevEnd))
		out = binary.AppendUvarint(out, safecast.IntToUint64(int(r.NumBlocks)))
		out = binary.AppendUvarint(out, deltaEnc(r.ObjectID, prevObj))
		out = binary.AppendUvarint(out, uint64(r.ObjectOffset))
		first := r.seqNumAt(0)
		out = binary.AppendUvarint(out, deltaEnc(first, prevSeq))
		consecutive := true
		for i := 1; i < int(r.NumBlocks); i++ {
			if r.seqNumAt(i) != first+safecast.IntToUint64(i) {
				consecutive = false
				break
			}
		}
		if consecutive {
			out = append(out, 0)
		} else {
			out = append(out, 1)
			prev := first
			for i := 1; i < int(r.NumBlocks); i++ {
				s := r.seqNumAt(i)
				out = binary.AppendUvarint(out, deltaEnc(s, prev))
				prev = s
			}
		}
		prevEnd, prevObj, prevSeq = r.end(), r.ObjectID, first
	}
	return out
}

func decodeMapRuns(body []byte) ([]BlockLookup, error) {
	p := body
	next := func() (uint64, error) {
		v, n := binary.Uvarint(p)
		if n <= 0 {
			return 0, fmt.Errorf("%w: truncated run record", ErrMapObjectCorrupt)
		}
		p = p[n:]
		return v, nil
	}
	count, err := next()
	if err != nil {
		return nil, err
	}
	if count > safecast.IntToUint64(len(body)) {
		return nil, fmt.Errorf("%w: run count %d", ErrMapObjectCorrupt, count)
	}
	runs := make([]BlockLookup, 0, count)
	var prevEnd, prevObj, prevSeq uint64
	for range count {
		var f [5]uint64
		for i := range f {
			if f[i], err = next(); err != nil {
				return nil, err
			}
		}
		if f[1] == 0 || f[1] > 0xFFFF || f[3] > 0xFFFFFFFF || len(p) == 0 {
			return nil, fmt.Errorf("%w: run fields", ErrMapObjectCorrupt)
		}
		r := BlockLookup{
			StartBlock:   deltaDec(prevEnd, f[0]),
			NumBlocks:    safecast.IntToUint16(safecast.Uint64ToInt(f[1])),
			ObjectID:     deltaDec(prevObj, f[2]),
			ObjectOffset: safecast.Uint64ToUint32(f[3]),
		}
		first := deltaDec(prevSeq, f[4])
		flag := p[0]
		p = p[1:]
		seqs := make([]uint64, r.NumBlocks)
		seqs[0] = first
		switch flag {
		case 0:
			for i := 1; i < len(seqs); i++ {
				seqs[i] = first + safecast.IntToUint64(i)
			}
		case 1:
			for i := 1; i < len(seqs); i++ {
				d, derr := next()
				if derr != nil {
					return nil, derr
				}
				seqs[i] = deltaDec(seqs[i-1], d)
			}
		default:
			return nil, fmt.Errorf("%w: seq flag %d", ErrMapObjectCorrupt, flag)
		}
		r.SeqNum, r.SeqNums = first, seqs
		runs = append(runs, r)
		prevEnd, prevObj, prevSeq = r.end(), r.ObjectID, first
	}
	if len(p) != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes", ErrMapObjectCorrupt, len(p))
	}
	return runs, nil
}

func appendRef(out []byte, r mapObjRef) []byte {
	out = binary.AppendUvarint(out, r.Epoch)
	out = binary.AppendUvarint(out, r.ID)
	out = binary.AppendUvarint(out, uint64(r.Size))
	return binary.BigEndian.AppendUint32(out, r.CRC)
}

func encodeMapHead(h mapHead) []byte {
	out := appendRef(nil, h.Base)
	out = binary.AppendUvarint(out, safecast.IntToUint64(len(h.Deltas)))
	for _, d := range h.Deltas {
		out = appendRef(out, d)
	}
	return out
}

func decodeMapHead(body []byte) (mapHead, error) {
	p := body
	ref := func() (mapObjRef, error) {
		var r mapObjRef
		var vals [3]uint64
		for i := range vals {
			v, n := binary.Uvarint(p)
			if n <= 0 {
				return r, fmt.Errorf("%w: truncated head", ErrMapObjectCorrupt)
			}
			vals[i], p = v, p[n:]
		}
		if len(p) < 4 || vals[2] > 0xFFFFFFFF {
			return r, fmt.Errorf("%w: truncated head", ErrMapObjectCorrupt)
		}
		r = mapObjRef{Epoch: vals[0], ID: vals[1], Size: safecast.Uint64ToUint32(vals[2]), CRC: binary.BigEndian.Uint32(p)}
		p = p[4:]
		return r, nil
	}
	var h mapHead
	var err error
	if h.Base, err = ref(); err != nil {
		return h, err
	}
	count, n := binary.Uvarint(p)
	if n <= 0 || count > safecast.IntToUint64(len(p)) {
		return h, fmt.Errorf("%w: delta count", ErrMapObjectCorrupt)
	}
	p = p[n:]
	h.Deltas = make([]mapObjRef, 0, count)
	for range count {
		d, derr := ref()
		if derr != nil {
			return h, derr
		}
		h.Deltas = append(h.Deltas, d)
	}
	if len(p) != 0 {
		return h, fmt.Errorf("%w: trailing head bytes", ErrMapObjectCorrupt)
	}
	return h, nil
}

// mapChainState is a format-2 volume's view of its committed map. Fields are
// guarded by VB.checkpointMu unless noted.
type mapChainState struct {
	head       mapHead
	headN      uint64
	objN       uint64
	claimed    bool
	pending    []BlockLookup
	baseBytes  uint64
	deltaBytes uint64
	lastBase   time.Time

	// baseBusy is set while a background base is written.
	baseBusy atomic.Bool
	// deleteBusy is set while deferred deletes are processed.
	deleteBusy atomic.Bool
	fenced     atomic.Bool

	deletes []deferredMapDelete

	// afterBase and afterDeletes, when set by a test, run when a background
	// base or delete pass finishes.
	afterBase    func()
	afterDeletes func()

	// chunkSeen records when chunk GC first found each zero-reference chunk.
	// Guarded by BlocksToObject.mu.
	chunkSeen map[uint64]time.Time
}

type deferredMapDelete struct {
	key        string
	eligibleAt time.Time
}

func (vb *VB) mapNow() time.Time {
	if vb.mapClock != nil {
		return vb.mapClock()
	}
	return time.Now()
}

func (vb *VB) mapDeleteDeferral() time.Duration {
	if vb.MapDeleteDeferral > 0 {
		return vb.MapDeleteDeferral
	}
	return DefaultMapDeleteDeferral
}

// IsMapDelta reports whether this volume persists its map as format 2.
func (vb *VB) IsMapDelta() bool { return vb.MapFormat == MapFormatDelta }

// setMapDelta switches the in-memory volume to format 2, so installs record
// their runs for the next delta.
func (vb *VB) setMapDelta() {
	vb.MapFormat = MapFormatDelta
	vb.BlocksToObject.mu.Lock()
	vb.BlocksToObject.trackChanges = true
	vb.BlocksToObject.mu.Unlock()
}

func (vb *VB) writeMapKey(ctx context.Context, key string, data []byte) error {
	backoff := vb.checkpointBackoff()
	var err error
	for attempt := range mapCommitAttempts {
		if err = vb.Backend.WriteKeyCtx(ctx, key, data); err == nil {
			return nil
		}
		if attempt < mapCommitAttempts-1 {
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
			backoff *= 2
		}
	}
	return err
}

func (vb *VB) putMapObject(ctx context.Context, prefix string, kind byte, body []byte) (mapObjRef, error) {
	ref := mapObjRef{Epoch: vb.LeaseEpoch, ID: vb.mapChain.objN}
	vb.mapChain.objN++
	data := frameMapObject(kind, vb.VolumeName, ref.Epoch, ref.ID, body)
	ref.Size = safecast.IntToUint32(len(data))
	ref.CRC = crc32.Checksum(data, crc32cTable)
	return ref, vb.writeMapKey(ctx, ref.key(prefix), data)
}

// putMapHead writes the next head of this launch's epoch. Its number is
// consumed whether or not the write succeeds, so no name is ever reused.
func (vb *VB) putMapHead(ctx context.Context, h mapHead) error {
	n := vb.mapChain.headN
	vb.mapChain.headN++
	data := frameMapObject(mapKindHead, vb.VolumeName, vb.LeaseEpoch, n, encodeMapHead(h))
	return vb.writeMapKey(ctx, mapObjectKey(mapHeadPrefix, vb.LeaseEpoch, n), data)
}

// InitMapDeltaCtx makes a new volume format 2 under its creation epoch: the
// fence stub at both v1 keys, an empty base, and the first head, each durable
// before the next. The caller writes VBState afterwards.
func (vb *VB) InitMapDeltaCtx(ctx context.Context) error {
	if vb.LeaseEpoch == 0 {
		return ErrMapNoLeaseEpoch
	}
	stub := MapFenceStub()
	empty := []byte{}
	if err := vb.Backend.WriteCtx(ctx, types.FileTypeBlockCheckpointLive, 0, &empty, &stub); err != nil {
		return fmt.Errorf("map: write live fence stub: %w", err)
	}
	if err := vb.Backend.WriteCtx(ctx, types.FileTypeBlockCheckpoint, vb.BlockToObjectWAL.WallNum.Load(), &empty, &stub); err != nil {
		return fmt.Errorf("map: write numbered fence stub: %w", err)
	}
	vb.checkpointMu.Lock()
	defer vb.checkpointMu.Unlock()
	vb.setMapDelta()
	vb.BlocksToObject.mu.RLock()
	runs := vb.mapRunsLocked()
	vb.BlocksToObject.mu.RUnlock()
	base, err := vb.putMapObject(ctx, mapBasePrefix, mapKindBase, encodeMapRuns(runs))
	if err != nil {
		return fmt.Errorf("map: write first base: %w", err)
	}
	h := mapHead{Base: base}
	if err := vb.putMapHead(ctx, h); err != nil {
		return fmt.Errorf("map: write first head: %w", err)
	}
	vb.mapChain.head = h
	vb.mapChain.baseBytes = uint64(base.Size)
	vb.mapChain.lastBase = vb.mapNow()
	vb.mapChain.claimed = true
	return nil
}

func (vb *VB) mapRunsLocked() []BlockLookup {
	runs := make([]BlockLookup, 0, vb.BlocksToObject.lookup.len())
	vb.BlocksToObject.lookup.scan(func(bl BlockLookup) bool {
		runs = append(runs, bl)
		return true
	})
	return runs
}

// listMapHeads returns every committed head's (epoch, n), unordered.
func (vb *VB) listMapHeads(ctx context.Context) ([][2]uint64, error) {
	// Listed by directory, which every backend supports, then filtered.
	keys, err := vb.Backend.ListObjectsCtx(ctx, vb.VolumeName+"/map/")
	if err != nil {
		return nil, fmt.Errorf("map: list heads: %w", err)
	}
	heads := make([][2]uint64, 0, len(keys))
	for _, k := range keys {
		if !strings.Contains(k, "/"+mapHeadPrefix) {
			continue
		}
		if e, n, ok := parseMapObjectKey(k, mapHeadPrefix); ok {
			heads = append(heads, [2]uint64{e, n})
		}
	}
	return heads, nil
}

func (vb *VB) readMapObject(ctx context.Context, prefix string, kind byte, ref mapObjRef) ([]byte, error) {
	data, err := vb.Backend.ReadKeyCtx(ctx, ref.key(prefix))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s absent", ErrMapObjectCorrupt, ref.key(prefix))
		}
		return nil, err
	}
	if safecast.IntToUint32(len(data)) != ref.Size || crc32.Checksum(data, crc32cTable) != ref.CRC {
		return nil, fmt.Errorf("%w: %s digest", ErrMapObjectCorrupt, ref.key(prefix))
	}
	return unframeMapObject(data, kind, vb.VolumeName, ref.Epoch, ref.ID)
}

// loadMapChainCtx loads the map the newest head names. A missing or corrupt
// object fails the open: an older head is never used instead, because the
// WAL and chunks only the newer map protects may already be gone.
func (vb *VB) loadMapChainCtx(ctx context.Context) error {
	heads, err := vb.listMapHeads(ctx)
	if err != nil {
		return err
	}
	if len(heads) == 0 {
		return ErrMapHeadMissing
	}
	newest := heads[0]
	for _, h := range heads[1:] {
		if h[0] > newest[0] || (h[0] == newest[0] && h[1] > newest[1]) {
			newest = h
		}
	}
	// A single-key read tries every replica, so it can find a head a lagging
	// listing missed.
	for {
		_, perr := vb.Backend.ReadKeyCtx(ctx, mapObjectKey(mapHeadPrefix, newest[0], newest[1]+1))
		if perr != nil {
			if errors.Is(perr, os.ErrNotExist) {
				break
			}
			return fmt.Errorf("map: probe head: %w", perr)
		}
		newest[1]++
	}
	if vb.LeaseEpoch != 0 && newest[0] >= vb.LeaseEpoch {
		return fmt.Errorf("%w: newest %d, launch %d", ErrMapEpochNotNewer, newest[0], vb.LeaseEpoch)
	}

	headData, err := vb.Backend.ReadKeyCtx(ctx, mapObjectKey(mapHeadPrefix, newest[0], newest[1]))
	if err != nil {
		return fmt.Errorf("map: read head %d.%d: %w", newest[0], newest[1], err)
	}
	body, err := unframeMapObject(headData, mapKindHead, vb.VolumeName, newest[0], newest[1])
	if err != nil {
		return err
	}
	h, err := decodeMapHead(body)
	if err != nil {
		return err
	}
	baseBody, err := vb.readMapObject(ctx, mapBasePrefix, mapKindBase, h.Base)
	if err != nil {
		return err
	}
	baseRuns, err := decodeMapRuns(baseBody)
	if err != nil {
		return err
	}
	deltas := make([][]BlockLookup, 0, len(h.Deltas))
	var deltaBytes uint64
	for _, ref := range h.Deltas {
		db, derr := vb.readMapObject(ctx, mapDeltaPrefix, mapKindDelta, ref)
		if derr != nil {
			return derr
		}
		runs, derr := decodeMapRuns(db)
		if derr != nil {
			return derr
		}
		deltas = append(deltas, runs)
		deltaBytes += uint64(ref.Size)
	}

	vb.checkpointMu.Lock()
	defer vb.checkpointMu.Unlock()
	vb.BlocksToObject.mu.Lock()
	defer vb.BlocksToObject.mu.Unlock()
	vb.BlocksToObject.lookup.clear()
	for _, r := range baseRuns {
		vb.BlocksToObject.lookup.set(r)
	}
	stride := vb.blockStride()
	for _, runs := range deltas {
		for _, r := range runs {
			vb.BlocksToObject.insertCoalescedLocked(r, stride, nil)
		}
	}
	vb.finishMapLoadLocked()
	vb.MapFormat = MapFormatDelta
	vb.BlocksToObject.trackChanges = true
	vb.BlocksToObject.changes = nil
	vb.BlocksToObject.dirty.Store(false)
	vb.mapChain.head = h
	vb.mapChain.baseBytes = uint64(h.Base.Size)
	vb.mapChain.deltaBytes = deltaBytes
	vb.mapChain.lastBase = vb.mapNow()
	vb.mapChain.pending = nil
	return nil
}

// finishMapLoadLocked rebuilds what a full map replacement invalidates: GC
// refcounts, the block store, and the ObjectNum and SeqNum floors.
// Caller holds BlocksToObject.mu for writing.
func (vb *VB) finishMapLoadLocked() {
	if vb.GCEnabled {
		vb.gcRefcount = make(map[uint64]uint64)
	}
	var maxObjectID, maxSeqNum uint64
	haveObject := false
	vb.BlocksToObject.lookup.scan(func(entry BlockLookup) bool {
		if vb.GCEnabled {
			vb.gcRefcount[entry.ObjectID]++
		}
		if !haveObject || entry.ObjectID > maxObjectID {
			maxObjectID, haveObject = entry.ObjectID, true
		}
		for i := range int(entry.NumBlocks) {
			maxSeqNum = max(maxSeqNum, entry.seqNumAt(i))
		}
		if vb.UseBlockStore && vb.BlockStore != nil {
			blocks := make([]uint64, entry.NumBlocks)
			seqNums := make([]uint64, entry.NumBlocks)
			for i := range blocks {
				blocks[i] = entry.StartBlock + uint64(i)
				seqNums[i] = entry.seqNumAt(i)
			}
			vb.BlockStore.SetPersistedRange(blocks, entry.ObjectID, entry.ObjectOffset, vb.blockStride(), seqNums)
		}
		return true
	})
	if haveObject {
		advanceAtomicTo(&vb.ObjectNum, maxObjectID+1)
	}
	advanceAtomicTo(&vb.SeqNum, maxSeqNum)
}

// ClaimMapCtx commits this launch's first head over the loaded map, before
// any guest IO, so it supersedes every head an older writer can still issue.
func (vb *VB) ClaimMapCtx(ctx context.Context) error {
	if !vb.IsMapDelta() {
		return nil
	}
	if vb.LeaseEpoch == 0 {
		return ErrMapNoLeaseEpoch
	}
	if err := vb.verifyMapFenceStubs(ctx); err != nil {
		return err
	}
	vb.checkpointMu.Lock()
	defer vb.checkpointMu.Unlock()
	if vb.mapChain.claimed {
		return nil
	}
	if err := vb.putMapHead(ctx, vb.mapChain.head); err != nil {
		return fmt.Errorf("map: claim head: %w", err)
	}
	vb.mapChain.claimed = true
	return nil
}

// verifyMapFenceStubs fails the open if either v1 key no longer holds the
// stub: a late v1 write has replaced it, and an older build could read it.
func (vb *VB) verifyMapFenceStubs(ctx context.Context) error {
	for _, k := range []struct {
		ft types.FileType
		id uint64
	}{{types.FileTypeBlockCheckpointLive, 0}, {types.FileTypeBlockCheckpoint, vb.BlockToObjectWAL.WallNum.Load()}} {
		data, err := vb.Backend.ReadCtx(ctx, k.ft, k.id, 0, 0)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("map: read fence stub: %w", err)
		}
		if err != nil || !IsMapFenceStub(data) {
			return fmt.Errorf("%w: %s", ErrMapFenceStubAbsent, types.GetFilePath(k.ft, k.id, vb.VolumeName))
		}
	}
	return nil
}

// persistMapCtx makes the in-memory map durable in the volume's format.
func (vb *VB) persistMapCtx(ctx context.Context) error {
	if !vb.IsMapDelta() {
		return vb.SaveLiveCheckpointCtx(ctx)
	}
	vb.checkpointMu.Lock()
	defer vb.checkpointMu.Unlock()
	return vb.commitMapDeltaLocked(ctx)
}

func (vb *VB) mapBaseDueLocked() bool {
	c := &vb.mapChain
	if len(c.head.Deltas) == 0 {
		return false
	}
	if c.deltaBytes >= max(c.baseBytes, mapBaseMinTriggerBytes) || len(c.head.Deltas) >= mapBaseMaxDeltas {
		return true
	}
	return vb.mapNow().Sub(c.lastBase) >= mapBaseMaxAge
}

// commitMapDeltaLocked writes the runs installed since the last commit as a
// delta, then the head naming it. A failed or ambiguous write commits
// nothing: the batch merges into the next commit under new names.
// Caller holds checkpointMu.
func (vb *VB) commitMapDeltaLocked(ctx context.Context) (err error) {
	if vb.mapChain.fenced.Load() {
		return ErrMapFenced
	}
	if vb.LeaseEpoch == 0 {
		return ErrMapNoLeaseEpoch
	}
	if !vb.mapChain.claimed {
		return ErrMapNotClaimed
	}

	var snap *extentIndex[BlockLookup]
	vb.BlocksToObject.mu.Lock()
	if vb.BlocksToObject.replaced {
		runs := vb.mapRunsLocked()
		vb.BlocksToObject.changes = nil
		vb.BlocksToObject.dirty.Store(false)
		vb.BlocksToObject.mu.Unlock()
		return vb.commitMapBaseLocked(ctx, runs)
	}
	vb.BlocksToObject.dirty.Store(false)
	changes := vb.BlocksToObject.changes
	vb.BlocksToObject.changes = nil
	if len(changes) > 0 || len(vb.mapChain.pending) > 0 {
		if !vb.mapChain.baseBusy.Load() && vb.mapBaseDueLocked() {
			// The map exactly as this delta commits it, taken in the same
			// critical section as the swap.
			snap = vb.BlocksToObject.lookup.copy()
		}
	}
	vb.BlocksToObject.mu.Unlock()

	batch := append(vb.mapChain.pending, changes...)
	vb.mapChain.pending = nil
	committed := false
	defer func() {
		if !committed {
			vb.mapChain.pending = batch
			vb.BlocksToObject.dirty.Store(true)
		}
	}()
	if len(batch) == 0 {
		committed = true
		return nil
	}

	start := time.Now()
	defer func() {
		outcome := "success"
		if err != nil {
			outcome = "error"
		}
		recordWALOp(context.Background(), "checkpoint", vb.VolumeName, outcome, time.Since(start))
	}()

	ref, err := vb.putMapObject(ctx, mapDeltaPrefix, mapKindDelta, encodeMapRuns(batch))
	if err != nil {
		return fmt.Errorf("map: write delta: %w", err)
	}
	next := vb.mapChain.head.clone()
	next.Deltas = append(next.Deltas, ref)
	if err = vb.putMapHead(ctx, next); err != nil {
		return fmt.Errorf("map: write head: %w", err)
	}
	committed = true
	vb.mapChain.head = next
	vb.mapChain.deltaBytes += uint64(ref.Size)
	if snap != nil && vb.mapChain.baseBusy.CompareAndSwap(false, true) {
		go vb.writeMapBase(context.WithoutCancel(ctx), snap, ref)
	}
	vb.maybeProcessMapDeletes()
	return nil
}

// commitMapBaseLocked commits runs as a base with no deltas above it, for a
// map replaced wholesale. Caller holds checkpointMu.
func (vb *VB) commitMapBaseLocked(ctx context.Context, runs []BlockLookup) error {
	committed := false
	defer func() {
		if !committed {
			vb.BlocksToObject.dirty.Store(true)
		}
	}()
	base, err := vb.putMapObject(ctx, mapBasePrefix, mapKindBase, encodeMapRuns(runs))
	if err != nil {
		return fmt.Errorf("map: write base: %w", err)
	}
	next := mapHead{Base: base}
	if err := vb.putMapHead(ctx, next); err != nil {
		return fmt.Errorf("map: write head: %w", err)
	}
	committed = true
	prev := vb.mapChain.head
	vb.mapChain.head = next
	vb.mapChain.pending = nil
	vb.mapChain.baseBytes = uint64(base.Size)
	vb.mapChain.deltaBytes = 0
	vb.mapChain.lastBase = vb.mapNow()
	vb.BlocksToObject.mu.Lock()
	vb.BlocksToObject.replaced = false
	vb.BlocksToObject.mu.Unlock()
	vb.scheduleMapPruneLocked(ctx, next, prev)
	return nil
}

// writeMapBase writes the whole map as delta covered commits it, then
// publishes a head naming that base and every delta committed above it.
func (vb *VB) writeMapBase(parent context.Context, snap *extentIndex[BlockLookup], covered mapObjRef) {
	defer func() {
		vb.mapChain.baseBusy.Store(false)
		if vb.mapChain.afterBase != nil {
			vb.mapChain.afterBase()
		}
	}()
	ctx, cancel := context.WithTimeout(parent, 10*time.Minute)
	defer cancel()

	runs := make([]BlockLookup, 0, snap.len())
	snap.scan(func(bl BlockLookup) bool {
		runs = append(runs, bl)
		return true
	})
	body := encodeMapRuns(runs)

	// The base is written outside checkpointMu, so drains keep committing;
	// only the head that publishes it is composed and written under it.
	vb.checkpointMu.Lock()
	ref := mapObjRef{Epoch: vb.LeaseEpoch, ID: vb.mapChain.objN}
	vb.mapChain.objN++
	vb.checkpointMu.Unlock()
	data := frameMapObject(mapKindBase, vb.VolumeName, ref.Epoch, ref.ID, body)
	ref.Size = safecast.IntToUint32(len(data))
	ref.CRC = crc32.Checksum(data, crc32cTable)
	if err := vb.writeMapKey(ctx, ref.key(mapBasePrefix), data); err != nil {
		vb.logger().Warn("map: base write failed, will retry at the next trigger", "volume", vb.VolumeName, "err", err)
		return
	}
	base := ref

	vb.checkpointMu.Lock()
	defer vb.checkpointMu.Unlock()
	if vb.mapChain.fenced.Load() {
		return
	}
	idx := slices.Index(vb.mapChain.head.Deltas, covered)
	if idx < 0 {
		return
	}
	prev := vb.mapChain.head
	next := mapHead{Base: base, Deltas: slices.Clone(prev.Deltas[idx+1:])}
	if err := vb.putMapHead(ctx, next); err != nil {
		vb.logger().Warn("map: base head write failed", "volume", vb.VolumeName, "err", err)
		return
	}
	vb.mapChain.head = next
	vb.mapChain.baseBytes = uint64(base.Size)
	vb.mapChain.deltaBytes = 0
	for _, d := range next.Deltas {
		vb.mapChain.deltaBytes += uint64(d.Size)
	}
	vb.mapChain.lastBase = vb.mapNow()
	vb.scheduleMapPruneLocked(ctx, next, prev)
}

// scheduleMapPruneLocked queues for deferred deletion every map object that
// neither the newest head nor the one before it names: one generation back
// stays readable. Caller holds checkpointMu.
func (vb *VB) scheduleMapPruneLocked(ctx context.Context, newest, prev mapHead) {
	keep := map[string]bool{}
	for _, h := range []mapHead{newest, prev} {
		keep[h.Base.key(mapBasePrefix)] = true
		for _, d := range h.Deltas {
			keep[d.key(mapDeltaPrefix)] = true
		}
	}
	keys, err := vb.Backend.ListObjectsCtx(ctx, vb.VolumeName+"/map/")
	if err != nil {
		vb.logger().Warn("map: prune listing failed, will retry at the next base", "volume", vb.VolumeName, "err", err)
		return
	}
	lastHead := vb.mapChain.headN - 1
	eligible := vb.mapNow().Add(vb.mapDeleteDeferral())
	for _, full := range keys {
		rel := strings.TrimPrefix(full, vb.VolumeName+"/")
		if keep[rel] {
			continue
		}
		if e, n, ok := parseMapObjectKey(rel, mapHeadPrefix); ok {
			if e > vb.LeaseEpoch || (e == vb.LeaseEpoch && n+1 >= lastHead) {
				continue
			}
		} else if e, _, ok := parseMapObjectKey(rel, mapDeltaPrefix); ok && e > vb.LeaseEpoch {
			continue
		} else if e, _, ok := parseMapObjectKey(rel, mapBasePrefix); ok && e > vb.LeaseEpoch {
			continue
		}
		vb.mapChain.deletes = append(vb.mapChain.deletes, deferredMapDelete{key: rel, eligibleAt: eligible})
	}
}

// higherEpochHeadExists reports whether a later launch has committed. A
// writer that finds one deletes nothing and fences itself.
func (vb *VB) higherEpochHeadExists(ctx context.Context) (bool, error) {
	heads, err := vb.listMapHeads(ctx)
	if err != nil {
		return false, err
	}
	for _, h := range heads {
		if h[0] > vb.LeaseEpoch {
			if vb.mapChain.fenced.CompareAndSwap(false, true) {
				vb.logger().Error("map: a newer lease holder has committed; fencing this writer",
					"volume", vb.VolumeName, "epoch", vb.LeaseEpoch, "newer", h[0])
			}
			return true, nil
		}
	}
	return false, nil
}

// maybeProcessMapDeletes runs any deferred map deletes that have come due,
// off the caller's path. Caller holds checkpointMu.
func (vb *VB) maybeProcessMapDeletes() {
	now := vb.mapNow()
	var due []string
	rest := vb.mapChain.deletes[:0]
	for _, d := range vb.mapChain.deletes {
		if !now.Before(d.eligibleAt) {
			due = append(due, d.key)
		} else {
			rest = append(rest, d)
		}
	}
	if len(due) == 0 {
		vb.mapChain.deletes = rest
		return
	}
	if !vb.mapChain.deleteBusy.CompareAndSwap(false, true) {
		return
	}
	vb.mapChain.deletes = rest
	go func() {
		defer func() {
			vb.mapChain.deleteBusy.Store(false)
			if vb.mapChain.afterDeletes != nil {
				vb.mapChain.afterDeletes()
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		vb.deleteMapKeys(ctx, due)
	}()
}

// deleteMapKeys deletes keys after checking that no later launch has
// committed; a failed delete is requeued.
func (vb *VB) deleteMapKeys(ctx context.Context, keys []string) {
	newer, err := vb.higherEpochHeadExists(ctx)
	if err != nil || newer {
		if err != nil {
			vb.requeueMapDeletes(keys)
		}
		return
	}
	var failed []string
	for _, k := range keys {
		if derr := vb.Backend.DeleteKeyCtx(ctx, k); derr != nil && !errors.Is(derr, os.ErrNotExist) {
			failed = append(failed, k)
		}
	}
	vb.requeueMapDeletes(failed)
}

func (vb *VB) requeueMapDeletes(keys []string) {
	if len(keys) == 0 {
		return
	}
	vb.checkpointMu.Lock()
	defer vb.checkpointMu.Unlock()
	now := vb.mapNow()
	for _, k := range keys {
		vb.mapChain.deletes = append(vb.mapChain.deletes, deferredMapDelete{key: k, eligibleAt: now})
	}
}

// mapChunkCandidatesLocked filters chunk GC candidates for a format-2 volume
// to those first seen at zero references at least the deferral ago.
// Caller holds BlocksToObject.mu.
func (vb *VB) mapChunkCandidatesLocked(candidates []uint64) []uint64 {
	if vb.mapChain.chunkSeen == nil {
		vb.mapChain.chunkSeen = make(map[uint64]time.Time)
	}
	now := vb.mapNow()
	live := make(map[uint64]bool, len(candidates))
	out := candidates[:0]
	for _, id := range candidates {
		live[id] = true
		seen, ok := vb.mapChain.chunkSeen[id]
		if !ok {
			vb.mapChain.chunkSeen[id] = now
			continue
		}
		if now.Sub(seen) >= vb.mapDeleteDeferral() {
			out = append(out, id)
		}
	}
	for id := range vb.mapChain.chunkSeen {
		if !live[id] {
			delete(vb.mapChain.chunkSeen, id)
		}
	}
	return out
}
