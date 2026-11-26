package viperblock

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"time"

	"github.com/mulgadc/bluebottle/pkg/safecast"
	"github.com/mulgadc/viperblock/types"
)

// A format-1 volume converts to format 2 while it runs, by its own writer:
// a background base from a copy-on-write snapshot, then a flip at a later
// ordinary drain. VBState is the authority for the format; a stub at the
// live key under a format-1 VBState is a flip the loader completes.

type mapConvPhase uint8

const (
	convIdle mapConvPhase = iota
	// convBase: the base and its head are being written in the background.
	convBase
	// convReady: the base and its head are durable; the next ordinary
	// drain flips.
	convReady
)

// mapConvRetry is how long a conversion that failed waits before the next
// ordinary drain starts it again.
const mapConvRetry = time.Minute

// mapConvMaxFlipRuns bounds what a flip encodes inside a drain: a batch that
// outgrew it while the base was written restarts the conversion instead.
const mapConvMaxFlipRuns = 1 << 16

type mapConversion struct {
	phase   mapConvPhase
	started time.Time
	retryAt time.Time
	// pending is set from the flip's first stub write until both stubs and
	// VBState naming format 2 are acknowledged. Until then every commit
	// retries them and fails, so no WAL is reclaimed meanwhile.
	pending bool
	// repair is set when the v1 loader found the stub under a VBState that
	// says format 1, so the claim rewrites VBState.
	repair bool

	// afterBase, when set by a test, runs when a conversion base finishes.
	afterBase func()
	// stopBeforeFlip, when set by a test, fails the flip at a step.
	stopBeforeFlip func(step string) error
}

// errConvStep is returned by a test hook to stop a flip at a step.
var errConvStep = errors.New("map: conversion stopped at a step")

func (vb *VB) persistedMapFormat() uint8 {
	if vb.IsMapDelta() {
		return MapFormatDelta
	}
	return vb.MapFormat
}

// persistFormatOneCtx persists a format-1 volume's map. An ordinary drain
// also flips a ready conversion, or starts one.
func (vb *VB) persistFormatOneCtx(ctx context.Context, drain bool) error {
	if drain {
		vb.checkpointMu.Lock()
		if vb.mapChain.conv.phase == convReady && vb.convertibleLocked() {
			err := vb.flipMapLocked(ctx)
			vb.checkpointMu.Unlock()
			if !errors.Is(err, errConvAbandoned) {
				return err
			}
		} else {
			vb.checkpointMu.Unlock()
		}
	}
	if err := vb.SaveLiveCheckpointCtx(ctx); err != nil {
		return err
	}
	if drain {
		vb.maybeStartConversion()
	}
	return nil
}

// errConvAbandoned reports a flip that gave up before anything irrevocable,
// leaving the volume format 1 for this drain.
var errConvAbandoned = errors.New("map: conversion abandoned")

// convertibleLocked reports whether this engine may convert now: it holds a
// lease epoch, is not stopping and is not fenced. Caller holds checkpointMu.
func (vb *VB) convertibleLocked() bool {
	return vb.LeaseEpoch != 0 && !vb.mapChain.closing.Load() && !vb.mapChain.fenced.Load()
}

// maybeStartConversion snapshots the map and writes it as a base in the
// background, recording every install from that instant for the flip.
func (vb *VB) maybeStartConversion() {
	vb.checkpointMu.Lock()
	defer vb.checkpointMu.Unlock()
	c := &vb.mapChain.conv
	if vb.IsMapDelta() || c.phase != convIdle || !vb.convertibleLocked() || vb.mapNow().Before(c.retryAt) {
		return
	}
	vb.BlocksToObject.mu.Lock()
	if vb.BlocksToObject.replaced {
		vb.BlocksToObject.mu.Unlock()
		return
	}
	snap := vb.BlocksToObject.lookup.copy()
	vb.BlocksToObject.trackChanges = true
	vb.BlocksToObject.changes = nil
	vb.BlocksToObject.mu.Unlock()

	c.phase = convBase
	c.started = vb.mapNow()
	// A reconnect in the same process continues the earlier engine's
	// numbering, so a head an abandoned conversion wrote is never reused.
	vb.mapChain.headN.Store(max(vb.mapChain.headN.Load(), vb.ResumeHeadN))
	vb.mapChain.objN.Store(max(vb.mapChain.objN.Load(), vb.ResumeObjN))
	vb.mapChain.bg.Go(func() {
		bgCtx, cancel := vb.mapBackgroundCtx()
		defer cancel()
		vb.writeConversionBase(bgCtx, snap)
	})
}

// writeConversionBase writes the snapshot as a base and the head naming it,
// outside checkpointMu, so drains keep committing format 1 meanwhile.
func (vb *VB) writeConversionBase(parent context.Context, snap *extentIndex[BlockLookup]) {
	defer func() {
		if fn := vb.mapChain.conv.afterBase; fn != nil {
			fn()
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

	vb.checkpointMu.Lock()
	ref := mapObjRef{Epoch: vb.LeaseEpoch, ID: vb.mapChain.objN.Add(1) - 1}
	vb.checkpointMu.Unlock()
	data := frameMapObject(mapKindBase, vb.VolumeName, ref.Epoch, ref.ID, body)
	ref.Size = safecast.IntToUint32(len(data))
	ref.CRC = crc32.Checksum(data, crc32cTable)
	werr := vb.writeMapKey(ctx, ref.key(mapBasePrefix), data)

	vb.checkpointMu.Lock()
	defer vb.checkpointMu.Unlock()
	if werr == nil && vb.convertibleLocked() && ctx.Err() == nil {
		h := mapHead{Creation: vb.LeaseEpoch, Base: ref}
		if werr = vb.putMapHead(ctx, h); werr == nil {
			vb.mapChain.head = h
			vb.mapChain.baseBytes = uint64(ref.Size)
			vb.mapChain.deltaBytes = 0
			vb.mapChain.lastBase = vb.mapNow()
			vb.mapChain.conv.phase = convReady
			return
		}
	}
	if werr == nil {
		werr = errors.New("engine stopping")
	}
	vb.logger().Warn("map: conversion base failed; the volume stays format 1", "volume", vb.VolumeName, "step", "base", "err", werr)
	vb.abandonConversionLocked()
}

// abandonConversionLocked returns a conversion to idle and stops recording
// installs. What it wrote is orphaned, and pruned by a later base.
// Caller holds checkpointMu.
func (vb *VB) abandonConversionLocked() {
	c := &vb.mapChain.conv
	c.phase = convIdle
	c.retryAt = vb.mapNow().Add(mapConvRetry)
	vb.mapChain.head = mapHead{}
	vb.BlocksToObject.mu.Lock()
	vb.BlocksToObject.trackChanges = false
	vb.BlocksToObject.changes = nil
	vb.BlocksToObject.mu.Unlock()
}

// flipMapLocked commits the installs since the base as a delta and head,
// then VBState naming format 2: from that write the volume is format 2.
// Caller holds checkpointMu.
func (vb *VB) flipMapLocked(ctx context.Context) error {
	c := &vb.mapChain.conv
	step := func(name string) error {
		if c.stopBeforeFlip != nil {
			return c.stopBeforeFlip(name)
		}
		return nil
	}

	vb.BlocksToObject.mu.Lock()
	if vb.BlocksToObject.replaced {
		vb.BlocksToObject.mu.Unlock()
		vb.abandonConversionLocked()
		return errConvAbandoned
	}
	if len(vb.BlocksToObject.changes) > mapConvMaxFlipRuns {
		vb.BlocksToObject.mu.Unlock()
		vb.logger().Info("map: conversion batch outgrew the flip bound; restarting it", "volume", vb.VolumeName, "step", "flip")
		vb.abandonConversionLocked()
		c.retryAt = time.Time{}
		return errConvAbandoned
	}
	batch := vb.BlocksToObject.changes
	vb.BlocksToObject.changes = nil
	vb.BlocksToObject.dirty.Store(false)
	vb.BlocksToObject.mu.Unlock()

	// Until VBState is written nothing is irrevocable: a failure puts the
	// batch back and fails the drain, which keeps its WAL.
	undo := func(err error) error {
		vb.BlocksToObject.mu.Lock()
		vb.BlocksToObject.changes = append(batch, vb.BlocksToObject.changes...)
		vb.BlocksToObject.dirty.Store(true)
		vb.BlocksToObject.mu.Unlock()
		vb.logger().Warn("map: conversion flip failed before VBState; the volume stays format 1", "volume", vb.VolumeName, "err", err)
		return err
	}
	next := vb.mapChain.head.clone()
	if len(batch) > 0 {
		if err := step("delta"); err != nil {
			return undo(err)
		}
		ref, err := vb.putMapObject(ctx, mapDeltaPrefix, mapKindDelta, encodeMapRuns(batch))
		if err != nil {
			return undo(fmt.Errorf("map: conversion delta: %w", err))
		}
		next.Deltas = append(next.Deltas, ref)
		if err := step("head"); err != nil {
			return undo(err)
		}
		if err := vb.putMapHead(ctx, next); err != nil {
			return undo(fmt.Errorf("map: conversion head: %w", err))
		}
		vb.mapChain.deltaBytes += uint64(ref.Size)
	}
	if err := step("stub"); err != nil {
		return undo(err)
	}

	// From the first stub write the engine is format 2 whatever the outcome,
	// because an unacknowledged write may still land: it never writes a v1
	// checkpoint again. The stubs precede VBState, so a crash between them
	// leaves a stub under a format-1 VBState, which the loader completes.
	vb.mapChain.head = next
	vb.mapChain.claimed = true
	vb.MapCreationEpoch = vb.LeaseEpoch
	vb.BlockToObjectWAL.WallNum.Add(1)
	vb.setMapDelta()
	c.phase = convIdle
	c.pending = true
	err := vb.finishConversionLocked(ctx)
	vb.logger().Info("map converted to format 2", "volume", vb.VolumeName, "epoch", vb.LeaseEpoch,
		"runs", len(batch), "base_bytes", vb.mapChain.baseBytes, "duration", vb.mapNow().Sub(c.started),
		"committed", !c.pending)
	return err
}

// finishConversionLocked writes what a flip still owes, each retried and
// failing the commit until acknowledged: the numbered stub, the live stub,
// then VBState naming format 2. Caller holds checkpointMu.
func (vb *VB) finishConversionLocked(ctx context.Context) error {
	c := &vb.mapChain.conv
	if !c.pending {
		return nil
	}
	stub := MapFenceStub()
	empty := []byte{}
	writes := []struct {
		step  string
		write func() error
	}{
		{"numbered stub", func() error {
			return vb.Backend.WriteCtx(ctx, types.FileTypeBlockCheckpoint, vb.BlockToObjectWAL.WallNum.Load(), &empty, &stub)
		}},
		{"live stub", func() error {
			return vb.Backend.WriteCtx(ctx, types.FileTypeBlockCheckpointLive, 0, &empty, &stub)
		}},
		{"state", func() error { return vb.SaveStateCtx(ctx) }},
	}
	for _, w := range writes {
		var err error
		for attempt := range mapCommitAttempts {
			if err = w.write(); err == nil {
				break
			}
			if attempt < mapCommitAttempts-1 {
				select {
				case <-time.After(vb.checkpointBackoff()):
				case <-ctx.Done():
					err = ctx.Err()
				}
			}
		}
		if err != nil {
			vb.logger().Error("map: conversion not yet committed; keeping WAL and retrying", "volume", vb.VolumeName, "step", w.step, "err", err)
			return fmt.Errorf("map: conversion %s: %w", w.step, err)
		}
	}
	c.pending = false
	return nil
}

// repairConvertedStateLocked rewrites VBState after the v1 loader found the
// stub under a VBState that says format 1. Caller holds checkpointMu.
func (vb *VB) repairConvertedStateLocked(ctx context.Context) error {
	c := &vb.mapChain.conv
	if !c.repair {
		return nil
	}
	c.repair = false
	vb.logger().Warn("map: VBState said format 1 over a converted map; rewriting it", "volume", vb.VolumeName, "step", "resume")
	vb.BlockToObjectWAL.WallNum.Add(1)
	c.pending = true
	return vb.finishConversionLocked(ctx)
}
