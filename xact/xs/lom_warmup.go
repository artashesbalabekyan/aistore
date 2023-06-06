// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"sync"

	"github.com/artashesbalabekyan/aistore/3rdparty/glog"
	"github.com/artashesbalabekyan/aistore/api/apc"
	"github.com/artashesbalabekyan/aistore/cluster"
	"github.com/artashesbalabekyan/aistore/cluster/meta"
	"github.com/artashesbalabekyan/aistore/fs"
	"github.com/artashesbalabekyan/aistore/fs/mpather"
	"github.com/artashesbalabekyan/aistore/xact"
	"github.com/artashesbalabekyan/aistore/xact/xreg"
)

type (
	llcFactory struct {
		xreg.RenewBase
		xctn *xactLLC
	}
	xactLLC struct {
		xact.BckJog
	}
)

// interface guard
var (
	_ cluster.Xact   = (*xactLLC)(nil)
	_ xreg.Renewable = (*llcFactory)(nil)
)

////////////////
// llcFactory //
////////////////

func (*llcFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	p := &llcFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}}
	p.Bck = bck
	return p
}

func (p *llcFactory) Start() error {
	xctn := newXactLLC(p.T, p.UUID(), p.Bck)
	p.xctn = xctn
	go xctn.Run(nil)
	return nil
}

func (*llcFactory) Kind() string        { return apc.ActLoadLomCache }
func (p *llcFactory) Get() cluster.Xact { return p.xctn }

func (*llcFactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) { return xreg.WprUse, nil }

/////////////
// xactLLC //
/////////////

func newXactLLC(t cluster.Target, uuid string, bck *meta.Bck) (r *xactLLC) {
	r = &xactLLC{}
	mpopts := &mpather.JgroupOpts{
		T:        t,
		CTs:      []string{fs.ObjectType},
		VisitObj: func(*cluster.LOM, []byte) error { return nil },
		DoLoad:   mpather.Load,
	}
	mpopts.Bck.Copy(bck.Bucket())
	r.BckJog.Init(uuid, apc.ActLoadLomCache, bck, mpopts)
	return
}

func (r *xactLLC) Run(*sync.WaitGroup) {
	r.BckJog.Run()
	glog.Infoln(r.Name())
	err := r.BckJog.Wait()
	r.Finish(err)
}

func (r *xactLLC) Snap() (snap *cluster.Snap) {
	snap = &cluster.Snap{}
	r.ToSnap(snap)

	snap.IdleX = r.IsIdle()
	return
}
