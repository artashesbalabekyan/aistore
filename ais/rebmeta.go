// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"unsafe"

	"github.com/artashesbalabekyan/aistore/3rdparty/glog"
	"github.com/artashesbalabekyan/aistore/api/apc"
	"github.com/artashesbalabekyan/aistore/cluster/meta"
	"github.com/artashesbalabekyan/aistore/cmn"
	"github.com/artashesbalabekyan/aistore/cmn/atomic"
	"github.com/artashesbalabekyan/aistore/cmn/cos"
	"github.com/artashesbalabekyan/aistore/cmn/debug"
	"github.com/artashesbalabekyan/aistore/cmn/fname"
	"github.com/artashesbalabekyan/aistore/cmn/jsp"
	"github.com/artashesbalabekyan/aistore/memsys"
	"github.com/artashesbalabekyan/aistore/nl"
	"github.com/artashesbalabekyan/aistore/xact"
)

// Rebalance metadata is distributed to start rebalance. We must do it:
// - when new target(s) joins the cluster - classical case
// - at startup when cluster starts with unfinished rebalance (was aborted)
// - when we unregister a target and some bucket(s) use EC - we must redistribute
//   the slices
// - on bucket rename:
//    1. bucket is renamed (and the paths of the objects change)
//    2. rebalance must be started to redistribute the objects to the targets
//       depending on HRW
// - when requested by the user - `ais job start rebalance` or via HTTP API

type (
	// rebMD is revs (see metasync) which is distributed by primary proxy to
	// the targets. It is distributed when some kind of rebalance is required.
	rebMD struct {
		meta.RMD
	}

	// rmdOwner is used to keep the information about the rebalances. Currently
	// it keeps the Version of the latest rebalance.
	rmdOwner struct {
		sync.Mutex
		rmd atomic.Pointer
		// global local atomic state
		interrupted atomic.Bool // when joining target reports interrupted rebalance
		starting    atomic.Bool // when starting up
	}

	rmdModifier struct {
		pre   func(ctx *rmdModifier, clone *rebMD)
		final func(ctx *rmdModifier, clone *rebMD)

		prev  *rebMD // pre-modification rmd
		cur   *rebMD // the cloned and modified `prev`
		rebID string // cluster-wide UUID

		p       *proxy
		smapCtx *smapModifier
		wait    bool
	}
)

// interface guard
var _ revs = (*rebMD)(nil)

// as revs
func (*rebMD) tag() string       { return revsRMDTag }
func (r *rebMD) version() int64  { return r.Version }
func (r *rebMD) marshal() []byte { return cos.MustMarshal(r) }
func (*rebMD) jit(p *proxy) revs { return p.owner.rmd.get() }
func (*rebMD) sgl() *memsys.SGL  { return nil }

func (r *rebMD) inc() { r.Version++ }

func (r *rebMD) clone() *rebMD {
	dst := &rebMD{}
	cos.CopyStruct(dst, r)
	return dst
}

func (r *rebMD) String() string {
	if r == nil {
		return "RMD <nil>"
	}
	if len(r.TargetIDs) == 0 && r.Resilver == "" {
		return fmt.Sprintf("RMD v%d", r.Version)
	}
	if r.Resilver == "" {
		return fmt.Sprintf("RMD v%d(%v)", r.Version, r.TargetIDs)
	}
	return fmt.Sprintf("RMD v%d(%v, %s)", r.Version, r.TargetIDs, r.Resilver)
}

func newRMDOwner() *rmdOwner {
	rmdo := &rmdOwner{}
	rmdo.put(&rebMD{})
	return rmdo
}

func (*rmdOwner) persist(rmd *rebMD) error {
	rmdPathName := filepath.Join(cmn.GCO.Get().ConfigDir, fname.Rmd)
	return jsp.SaveMeta(rmdPathName, rmd, nil /*wto*/)
}

func (r *rmdOwner) load() {
	rmd := &rebMD{}
	_, err := jsp.LoadMeta(filepath.Join(cmn.GCO.Get().ConfigDir, fname.Rmd), rmd)
	if err == nil {
		r.put(rmd)
		return
	}
	if !os.IsNotExist(err) {
		glog.Errorf("failed to load rmd: %v", err)
	}
}

func (r *rmdOwner) put(rmd *rebMD) { r.rmd.Store(unsafe.Pointer(rmd)) }
func (r *rmdOwner) get() *rebMD    { return (*rebMD)(r.rmd.Load()) }

func (r *rmdOwner) modify(ctx *rmdModifier) (clone *rebMD, err error) {
	r.Lock()
	clone, err = r.do(ctx)
	r.Unlock()
	if err == nil && ctx.final != nil {
		ctx.final(ctx, clone)
	}
	return
}

func (r *rmdOwner) do(ctx *rmdModifier) (clone *rebMD, err error) {
	ctx.prev = r.get()
	clone = ctx.prev.clone()
	clone.TargetIDs = nil
	clone.Resilver = ""
	ctx.pre(ctx, clone) // `pre` callback

	if err = r.persist(clone); err == nil {
		r.put(clone)
	}
	ctx.cur = clone
	ctx.rebID = xact.RebID2S(clone.Version) // new rebID
	return
}

/////////////////
// rmdModifier //
/////////////////

func rmdInc(_ *rmdModifier, clone *rebMD) { clone.inc() }

// via `rmdModifier.final`
func rmdSync(m *rmdModifier, clone *rebMD) {
	debug.Assert(m.cur == clone)
	m.listen(nil)
	msg := &aisMsg{ActMsg: apc.ActMsg{Action: apc.ActRebalance}, UUID: m.rebID} // user-requested rebalance
	wg := m.p.metasyncer.sync(revsPair{m.cur, msg})
	if m.wait {
		wg.Wait()
	}
}

// see `receiveRMD` (upon termination, notify IC)
func (m *rmdModifier) listen(cb func(nl nl.Listener)) {
	nl := xact.NewXactNL(m.rebID, apc.ActRebalance, &m.smapCtx.smap.Smap, nil)
	nl.SetOwner(equalIC)

	nl.F = m.log
	if cb != nil {
		nl.F = cb
	}
	err := m.p.notifs.add(nl)
	debug.AssertNoErr(err)
}

// default
func (m *rmdModifier) log(nl nl.Listener) {
	debug.Assert(nl.UUID() == m.rebID)
	var (
		err  = nl.Err()
		abrt = nl.Aborted()
		name = "rebalance[" + nl.UUID() + "] "
	)
	switch {
	case err == nil && !abrt:
		glog.Infoln(name + "done")
	case abrt:
		glog.Warningln(name + "aborted")
	default:
		glog.Errorf("%s failed: %v", name, err)
	}
}

// deactivate or remove node from the cluster (as per msg.Action)
func (m *rmdModifier) rmNode(nl nl.Listener) {
	m.log(nl)
	if err, abrt := nl.Err(), nl.Aborted(); err != nil || abrt {
		return
	}
	si := m.smapCtx.smap.GetNode(m.smapCtx.sid)
	debug.Assert(si.IsTarget())
	if _, err := m.p.rmNodeFinal(m.smapCtx.msg, si, m.smapCtx); err != nil {
		glog.Error(err)
	}
}
