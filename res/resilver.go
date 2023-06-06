// Package res provides local volume resilvering upon mountpath-attach and similar
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package res

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/artashesbalabekyan/aistore/3rdparty/glog"
	"github.com/artashesbalabekyan/aistore/api/apc"
	"github.com/artashesbalabekyan/aistore/cluster"
	"github.com/artashesbalabekyan/aistore/cmn"
	"github.com/artashesbalabekyan/aistore/cmn/atomic"
	"github.com/artashesbalabekyan/aistore/cmn/cos"
	"github.com/artashesbalabekyan/aistore/cmn/debug"
	"github.com/artashesbalabekyan/aistore/cmn/fname"
	"github.com/artashesbalabekyan/aistore/cmn/mono"
	"github.com/artashesbalabekyan/aistore/fs"
	"github.com/artashesbalabekyan/aistore/fs/mpather"
	"github.com/artashesbalabekyan/aistore/memsys"
	"github.com/artashesbalabekyan/aistore/xact"
	"github.com/artashesbalabekyan/aistore/xact/xreg"
	"github.com/artashesbalabekyan/aistore/xact/xs"
)

const timedDuration = 4 * time.Second // see also: timedDuration in tgtgfn.go

type (
	Res struct {
		t cluster.Target
		// last or current resilver's time interval
		begin atomic.Int64
		end   atomic.Int64
	}
	Args struct {
		UUID              string
		Notif             *xact.NotifXact
		Rmi               *fs.Mountpath
		Action            string
		PostDD            func(rmi *fs.Mountpath, action string, xres *xs.Resilver, err error)
		SkipGlobMisplaced bool
		SingleRmiJogger   bool
	}
	joggerCtx struct {
		xres *xs.Resilver
		t    cluster.Target
	}
)

func New(t cluster.Target) *Res {
	return &Res{t: t}
}

func (res *Res) IsActive(multiplier int64) (yes bool) {
	begin := res.begin.Load()
	if begin == 0 {
		return
	}
	now := mono.NanoTime()
	if now-begin < multiplier*int64(timedDuration) {
		yes = true
	} else {
		end := res.end.Load()
		yes = end == 0 || time.Duration(now-end) < timedDuration
	}
	return
}

func (res *Res) _begin() {
	res.begin.Store(mono.NanoTime())
	res.end.Store(0)
}

func (res *Res) _end() {
	res.end.Store(mono.NanoTime())
}

func (res *Res) RunResilver(args Args) {
	res._begin()
	defer res._end()
	if fatalErr, writeErr := fs.PersistMarker(fname.ResilverMarker); fatalErr != nil || writeErr != nil {
		glog.Errorf("FATAL: %v, WRITE: %v", fatalErr, writeErr)
		return
	}
	availablePaths, _ := fs.Get()
	if len(availablePaths) < 1 {
		glog.Error(cmn.ErrNoMountpaths)
		return
	}
	xres := xreg.RenewResilver(args.UUID).(*xs.Resilver)
	if args.Notif != nil {
		args.Notif.Xact = xres
		xres.AddNotif(args.Notif)
	}

	// jogger group
	var (
		jg        *mpather.Jgroup
		slab, err = res.t.PageMM().GetSlab(memsys.MaxPageSlabSize)
		jctx      = &joggerCtx{xres: xres, t: res.t}

		opts = &mpather.JgroupOpts{
			T:                     res.t,
			CTs:                   []string{fs.ObjectType, fs.ECSliceType},
			VisitObj:              jctx.visitObj,
			VisitCT:               jctx.visitCT,
			Slab:                  slab,
			SkipGloballyMisplaced: args.SkipGlobMisplaced,
		}
	)
	debug.AssertNoErr(err)
	debug.Assert(args.PostDD == nil || (args.Action == apc.ActMountpathDetach || args.Action == apc.ActMountpathDisable))

	if args.SingleRmiJogger {
		jg = mpather.NewJoggerGroup(opts, args.Rmi.Path)
		glog.Infof("%s, action %q, jogger->(%q)", xres.Name(), args.Action, args.Rmi)
	} else {
		jg = mpather.NewJoggerGroup(opts)
		if args.Rmi != nil {
			glog.Infof("%s, action %q, rmi %s, num %d", xres.Name(), args.Action, args.Rmi, jg.Num())
		} else {
			glog.Infof("%s, num %d", xres.Name(), jg.Num())
		}
	}

	// run and block waiting
	res.end.Store(0)
	jg.Run()
	err = res.wait(jg, xres)

	// callback to, finally, detach-disable
	if args.PostDD != nil {
		args.PostDD(args.Rmi, args.Action, xres, err)
	}
	xres.Finish(err)
}

// Wait for an abort or for resilvering joggers to finish.
func (res *Res) wait(jg *mpather.Jgroup, xres *xs.Resilver) (err error) {
	tsi := res.t.Snode()
	for {
		select {
		case errCause := <-xres.ChanAbort():
			if err = jg.Stop(); err != nil {
				glog.Errorf("%s: %s aborted (cause %v), traversal err %v", tsi, xres, errCause, err)
			} else {
				glog.Infof("%s: %s aborted (cause %v)", tsi, xres, errCause)
			}
			return cmn.NewErrAborted(xres.Name(), "", errCause)
		case <-jg.ListenFinished():
			if err = fs.RemoveMarker(fname.ResilverMarker); err == nil {
				glog.Infof("%s: %s removed marker ok", tsi, xres)
			}
			return
		}
	}
}

// Copies a slice and its metafile (if exists) to the current mpath. At the
// end does proper cleanup: removes ether source files(on success), or
// destination files(on copy failure)
func _mvSlice(ct *cluster.CT, buf []byte) {
	uname := ct.Bck().MakeUname(ct.ObjectName())
	destMpath, _, err := cluster.HrwMpath(uname)
	if err != nil {
		glog.Warning(err)
		return
	}
	if destMpath.Path == ct.Mountpath().Path {
		return
	}

	destFQN := destMpath.MakePathFQN(ct.Bucket(), fs.ECSliceType, ct.ObjectName())
	srcMetaFQN, destMetaFQN, err := _moveECMeta(ct, ct.Mountpath(), destMpath, buf)
	if err != nil {
		return
	}
	// Slice without metafile - skip it as unusable, let LRU clean it up
	if srcMetaFQN == "" {
		return
	}
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("Resilver moving %q -> %q", ct.FQN(), destFQN)
	}
	if _, _, err = cos.CopyFile(ct.FQN(), destFQN, buf, cos.ChecksumNone); err != nil {
		glog.Errorf("Failed to copy %q -> %q: %v. Rolling back", ct.FQN(), destFQN, err)
		if err = os.Remove(destMetaFQN); err != nil {
			glog.Warningf("Failed to cleanup metafile copy %q: %v", destMetaFQN, err)
		}
	}
	errMeta := os.Remove(srcMetaFQN)
	errSlice := os.Remove(ct.FQN())
	if errMeta != nil || errSlice != nil {
		glog.Warningf("Failed to cleanup %q: %v, %v", ct.FQN(), errSlice, errMeta)
	}
}

// Copies EC metafile to correct mpath. It returns FQNs of the source and
// destination for a caller to do proper cleanup. Empty values means: either
// the source FQN does not exist(err==nil), or copying failed
func _moveECMeta(ct *cluster.CT, srcMpath, dstMpath *fs.Mountpath, buf []byte) (string, string, error) {
	src := srcMpath.MakePathFQN(ct.Bucket(), fs.ECMetaType, ct.ObjectName())
	// If metafile does not exist it may mean that EC has not processed the
	// object yet (e.g, EC was enabled after the bucket was filled), or
	// the metafile has gone
	if err := cos.Stat(src); os.IsNotExist(err) {
		return "", "", nil
	}
	dst := dstMpath.MakePathFQN(ct.Bucket(), fs.ECMetaType, ct.ObjectName())
	_, _, err := cos.CopyFile(src, dst, buf, cos.ChecksumNone)
	if err == nil {
		return src, dst, nil
	}
	if os.IsNotExist(err) {
		err = nil
	}
	return "", "", err
}

// TODO: revisit EC bits and check for OOS preemptively
// NOTE: not deleting extra copies - delegating to `storage cleanup`
func (jg *joggerCtx) visitObj(lom *cluster.LOM, buf []byte) (errHrw error) {
	const maxRetries = 3
	var (
		orig   = lom
		hlom   *cluster.LOM
		xname  = jg.xres.Name()
		size   int64
		copied bool
	)
	if !lom.TryLock(true) { // NOTE: skipping busy
		time.Sleep(time.Second >> 1)
		if !lom.TryLock(true) {
			return
		}
	}
	// cleanup
	defer func() {
		lom = orig
		lom.Unlock(true)
		if copied && errHrw == nil {
			jg.xres.ObjsAdd(1, size)
		}
	}()

	// 1. fix EC metafile
	var metaOldPath, metaNewPath string
	if !lom.IsHRW() && lom.Bprops().EC.Enabled {
		// copy metafile
		newMpath, _, errEc := cluster.ResolveFQN(lom.HrwFQN)
		if errEc != nil {
			glog.Warningf("%s: %s %v", xname, lom, errEc)
			return nil
		}
		ct := cluster.NewCTFromLOM(lom, fs.ObjectType)
		metaOldPath, metaNewPath, errEc = _moveECMeta(ct, lom.Mountpath(), newMpath.Mountpath, buf)
		if errEc != nil {
			glog.Warningf("%s: failed to copy EC metafile %s %q -> %q: %v",
				xname, lom, lom.Mountpath().Path, newMpath.Mountpath.Path, errEc)
			return nil
		}
	}

	if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
		return nil
	}
	size = lom.SizeBytes()
	// 2. fix hrw location; fail and subsequently abort if unsuccessful
	var retries int
	mi, isHrw := lom.ToMpath()
	if mi == nil {
		goto ret // nothing to do
	}
redo:
	if isHrw {
		// cannot have it associated with a non-hrw mp; TODO: !lom.WritePolicy().IsImmediate()
		lom.Uncache(true)

		hlom, errHrw = jg.fixHrw(lom, mi, buf)
		if errHrw != nil {
			if !os.IsNotExist(errHrw) && !strings.Contains(errHrw.Error(), "does not exist") {
				glog.Errorf("%s: failed to restore %s, errHrw: %v", xname, lom, errHrw)
			}
			// EC cleanup and return
			if metaNewPath != "" {
				if errHrw = os.Remove(metaNewPath); errHrw != nil {
					glog.Warningf("%s: nested (%s %s: %v)", xname, lom, metaNewPath, errHrw)
				}
			}
			return
		}
		lom = hlom
		copied = true
	}

	// 3. fix copies
	for {
		mi, isHrw := lom.ToMpath()
		if mi == nil {
			break
		}
		if isHrw {
			// redo hlom in an unlikely event
			retries++
			if retries > maxRetries {
				hmi := "???"
				if hlom != nil && hlom.Mountpath() != nil {
					hmi = hlom.Mountpath().String()
				}
				errHrw = fmt.Errorf("%s: hrw mountpaths keep changing (%s(%s) => %s => %s ...)",
					xname, orig, orig.Mountpath(), hmi, mi)
				glog.Error(errHrw)
				return
			}
			copied = false
			lom, hlom = orig, nil
			time.Sleep(cmn.Timeout.CplaneOperation() / 2)
			goto redo
		}
		err := lom.Copy(mi, buf)
		if err == nil {
			copied = true
			continue
		}
		if cos.IsErrOOS(err) {
			err = cmn.NewErrAborted(xname, "visit-obj", err)
		} else if !os.IsNotExist(err) && !strings.Contains(err.Error(), "does not exist") {
			glog.Warningf("%s: failed to copy %s to %s, err: %v", xname, lom, mi, err)
		}
		break
	}
ret:
	// EC: remove old metafile
	if metaOldPath != "" {
		if err := os.Remove(metaOldPath); err != nil {
			glog.Warningf("%s: failed to cleanup %s old metafile %q: %v", xname, lom, metaOldPath, err)
		}
	}
	return nil
}

func (*joggerCtx) fixHrw(lom *cluster.LOM, mi *fs.Mountpath, buf []byte) (hlom *cluster.LOM, err error) {
	if err = lom.Copy(mi, buf); err != nil {
		return
	}
	hrwFQN := mi.MakePathFQN(lom.Bucket(), fs.ObjectType, lom.ObjName)
	hlom = &cluster.LOM{}
	if err = hlom.InitFQN(hrwFQN, lom.Bucket()); err != nil {
		return
	}
	debug.Assert(hlom.Mountpath().Path == mi.Path)

	// reload; cache iff write-policy != immediate
	err = hlom.Load(!hlom.WritePolicy().IsImmediate() /*cache it*/, true /*locked*/)
	return
}

func (*joggerCtx) visitCT(ct *cluster.CT, buf []byte) (err error) {
	debug.Assert(ct.ContentType() == fs.ECSliceType)
	if !ct.Bck().Props.EC.Enabled {
		// Since `%ec` directory is inside a bucket, it is safe to skip
		// the entire `%ec` directory when EC is disabled for the bucket.
		return filepath.SkipDir
	}
	_mvSlice(ct, buf)
	return nil
}
